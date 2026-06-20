//! Incremental-strategy inference (Phase 2 of the intelligent-sync design).
//!
//! Looks at a source table's schema/constraints and classifies how it can be
//! synced incrementally, with a confidence and a *tier* that controls how much
//! validation is required before the strategy is trusted:
//!
//! - `Trusted`  — the watermark is DB-enforced (a `DEFAULT now()`/`CURRENT_TIMESTAMP`
//!   column on a table with an UPDATE trigger). Safe to activate immediately.
//! - `Probation` — a conventional `updated_at`-style watermark that is *not* proven
//!   DB-enforced. Plausible but must pass shadow validation first (Phase 2c).
//! - `Audited`   — a riskier inference (non-conventional timestamp, or APPEND via a
//!   monotonic id assuming inserts-only). Kept under permanent low-rate audit.
//!
//! Only the SQL probes here touch the database; [`classify`] is a pure function so
//! the decision logic is unit-tested without a database.

use sqlx::{PgPool, Row};

use crate::error::TaskError;
use crate::incremental::detect_primary_keys;

/// Same conventional "updated at" names the legacy detector used.
const WATERMARK_COLUMN_CANDIDATES: &[&str] = &[
    "updated_at",
    "last_update_time",
    "last_updated",
    "last_modified",
    "modified_at",
    "update_time",
    "gmt_modified",
    "updated",
    "mtime",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncMode {
    Full,
    Upsert,
    Append,
}

impl IncMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncMode::Full => "full",
            IncMode::Upsert => "upsert",
            IncMode::Append => "append",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncTier {
    Trusted,
    Probation,
    Audited,
}

impl IncTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            IncTier::Trusted => "trusted",
            IncTier::Probation => "probation",
            IncTier::Audited => "audited",
        }
    }

    /// Initial sync status for a freshly-inferred strategy: a trusted strategy is
    /// safe to use right away; everything else must be validated first.
    pub fn initial_status(&self) -> &'static str {
        match self {
            IncTier::Trusted => "active",
            IncTier::Probation | IncTier::Audited => "suggested",
        }
    }
}

/// One timestamp column with the facts that bear on watermark trust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampColumn {
    pub name: String,
    pub not_null: bool,
    /// Column default is `now()` / `CURRENT_TIMESTAMP`.
    pub has_default_now: bool,
}

/// The schema facts feeding [`classify`]. Gathered by [`gather_facts`].
#[derive(Debug, Clone, Default)]
pub struct TableFacts {
    pub pk_columns: Vec<String>,
    pub timestamp_columns: Vec<TimestampColumn>,
    /// The table has a row-level UPDATE trigger (so updates are intercepted —
    /// evidence that an `updated_at` column is actually maintained).
    pub has_update_trigger: bool,
    /// Single-column PK that is an identity/serial integer (monotonic), if any.
    pub monotonic_id_pk: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalStrategy {
    pub mode: IncMode,
    pub watermark_column: Option<String>,
    pub pk_columns: Vec<String>,
    /// 0..1.
    pub confidence_pct: u8,
    pub tier: IncTier,
    pub reason: String,
}

impl IncrementalStrategy {
    fn full(reason: impl Into<String>) -> Self {
        Self {
            mode: IncMode::Full,
            watermark_column: None,
            pk_columns: vec![],
            confidence_pct: 0,
            tier: IncTier::Audited,
            reason: reason.into(),
        }
    }
}

fn is_candidate_name(name: &str) -> bool {
    WATERMARK_COLUMN_CANDIDATES
        .iter()
        .any(|c| name.eq_ignore_ascii_case(c))
}

/// Pure classification of a sync strategy from schema facts. See module docs for
/// the tiering rules. Conservative on purpose: only a DB-enforced watermark is
/// `Trusted`; anything inferred is `Probation`/`Audited` pending validation.
pub fn classify(facts: &TableFacts) -> IncrementalStrategy {
    let has_pk = !facts.pk_columns.is_empty();

    // 1. UPSERT via a conventionally-named timestamp + a primary key.
    if has_pk {
        if let Some(col) = facts
            .timestamp_columns
            .iter()
            .find(|c| is_candidate_name(&c.name))
        {
            let db_enforced = col.has_default_now && facts.has_update_trigger;
            let mut confidence = 60u8;
            let mut why = format!("PK + conventional watermark '{}'", col.name);
            if col.not_null {
                confidence += 15;
                why.push_str(", NOT NULL");
            }
            if col.has_default_now {
                confidence += 10;
                why.push_str(", DEFAULT now()");
            }
            if facts.has_update_trigger {
                confidence += 10;
                why.push_str(", UPDATE trigger");
            }
            let tier = if db_enforced {
                why.push_str(" → DB-enforced");
                IncTier::Trusted
            } else {
                IncTier::Probation
            };
            return IncrementalStrategy {
                mode: IncMode::Upsert,
                watermark_column: Some(col.name.clone()),
                pk_columns: facts.pk_columns.clone(),
                confidence_pct: confidence.min(95),
                tier,
                reason: why,
            };
        }

        // 2. UPSERT via a non-conventional timestamp column: plausible but the
        // name gives no "updated" semantics, so always validate.
        if let Some(col) = facts.timestamp_columns.first() {
            let mut confidence = 30u8;
            if col.not_null {
                confidence += 10;
            }
            return IncrementalStrategy {
                mode: IncMode::Upsert,
                watermark_column: Some(col.name.clone()),
                pk_columns: facts.pk_columns.clone(),
                confidence_pct: confidence,
                tier: IncTier::Audited,
                reason: format!(
                    "PK + non-conventional timestamp '{}'; needs validation",
                    col.name
                ),
            };
        }
    }

    // 3. APPEND via a monotonic identity/serial PK (assumes insert-only).
    if let Some(id) = &facts.monotonic_id_pk {
        return IncrementalStrategy {
            mode: IncMode::Append,
            watermark_column: Some(id.clone()),
            pk_columns: facts.pk_columns.clone(),
            confidence_pct: 40,
            tier: IncTier::Audited,
            reason: format!(
                "monotonic id PK '{}'; assumes insert-only, needs validation",
                id
            ),
        };
    }

    // 4. No usable signal: full refresh.
    IncrementalStrategy::full(if has_pk {
        "PK present but no watermark/monotonic-id column"
    } else {
        "no primary key or watermark column"
    })
}

/// Reads the schema facts for a table from the source PostgreSQL catalog.
pub async fn gather_facts(
    pool: &PgPool,
    qualified_table: &str,
) -> Result<TableFacts, TaskError> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), qualified_table.to_string()),
    };

    let pk_columns = detect_primary_keys(pool, qualified_table).await?;

    // Timestamp columns with nullability + default-now flags.
    let ts_rows = sqlx::query(
        "SELECT column_name, is_nullable, COALESCE(column_default, '') AS col_default \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
           AND data_type IN ('timestamp without time zone', 'timestamp with time zone')",
    )
    .bind(&schema)
    .bind(&table)
    .fetch_all(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(format!("timestamp columns query failed: {e}")))?;

    let timestamp_columns = ts_rows
        .into_iter()
        .map(|r| {
            let default: String = r.try_get::<String, _>("col_default").unwrap_or_default();
            let d = default.to_lowercase();
            TimestampColumn {
                name: r.get::<String, _>("column_name"),
                not_null: r
                    .try_get::<String, _>("is_nullable")
                    .map(|n| n.eq_ignore_ascii_case("NO"))
                    .unwrap_or(false),
                has_default_now: d.contains("now()") || d.contains("current_timestamp"),
            }
        })
        .collect::<Vec<_>>();

    // Any row-level UPDATE trigger on the table.
    let has_update_trigger = sqlx::query(
        "SELECT 1 FROM information_schema.triggers \
         WHERE event_object_schema = $1 AND event_object_table = $2 \
           AND event_manipulation = 'UPDATE' LIMIT 1",
    )
    .bind(&schema)
    .bind(&table)
    .fetch_optional(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(format!("trigger query failed: {e}")))?
    .is_some();

    // Single-column integer PK that is an identity/serial column → monotonic.
    let monotonic_id_pk = if pk_columns.len() == 1 {
        let col = &pk_columns[0];
        let row = sqlx::query(
            "SELECT a.attidentity::text AS ident, t.typname AS typ, \
                    pg_get_serial_sequence($1, $2) AS seq \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = $1::regclass AND a.attname = $2",
        )
        .bind(qualified_table)
        .bind(col)
        .fetch_optional(pool)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("identity PK query failed: {e}")))?;

        row.and_then(|r| {
            let typ: String = r.try_get("typ").unwrap_or_default();
            let ident: String = r.try_get("ident").unwrap_or_default();
            let seq: Option<String> = r.try_get("seq").ok().flatten();
            let is_int = matches!(typ.as_str(), "int2" | "int4" | "int8");
            let is_monotonic = !ident.trim().is_empty() || seq.is_some();
            (is_int && is_monotonic).then(|| col.clone())
        })
    } else {
        None
    };

    Ok(TableFacts {
        pk_columns,
        timestamp_columns,
        has_update_trigger,
        monotonic_id_pk,
    })
}

/// Probes the source and classifies a table's incremental strategy.
pub async fn infer_strategy(
    pool: &PgPool,
    qualified_table: &str,
) -> Result<IncrementalStrategy, TaskError> {
    let facts = gather_facts(pool, qualified_table).await?;
    Ok(classify(&facts))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(name: &str, not_null: bool, default_now: bool) -> TimestampColumn {
        TimestampColumn {
            name: name.to_string(),
            not_null,
            has_default_now: default_now,
        }
    }

    #[test]
    fn db_enforced_updated_at_is_trusted_upsert() {
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            timestamp_columns: vec![ts("updated_at", true, true)],
            has_update_trigger: true,
            monotonic_id_pk: None,
        };
        let s = classify(&facts);
        assert_eq!(s.mode, IncMode::Upsert);
        assert_eq!(s.tier, IncTier::Trusted);
        assert_eq!(s.watermark_column.as_deref(), Some("updated_at"));
        assert_eq!(s.tier.initial_status(), "active");
    }

    #[test]
    fn conventional_but_not_enforced_is_probation() {
        // updated_at present, but no DEFAULT now() and no UPDATE trigger.
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            timestamp_columns: vec![ts("updated_at", true, false)],
            has_update_trigger: false,
            monotonic_id_pk: None,
        };
        let s = classify(&facts);
        assert_eq!(s.mode, IncMode::Upsert);
        assert_eq!(s.tier, IncTier::Probation);
        assert_eq!(s.tier.initial_status(), "suggested");
    }

    #[test]
    fn non_conventional_timestamp_is_audited() {
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            timestamp_columns: vec![ts("event_ts", true, false)],
            has_update_trigger: false,
            monotonic_id_pk: None,
        };
        let s = classify(&facts);
        assert_eq!(s.mode, IncMode::Upsert);
        assert_eq!(s.tier, IncTier::Audited);
        assert_eq!(s.watermark_column.as_deref(), Some("event_ts"));
    }

    #[test]
    fn monotonic_id_without_timestamp_is_append_audited() {
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            timestamp_columns: vec![],
            has_update_trigger: false,
            monotonic_id_pk: Some("id".into()),
        };
        let s = classify(&facts);
        assert_eq!(s.mode, IncMode::Append);
        assert_eq!(s.tier, IncTier::Audited);
        assert_eq!(s.watermark_column.as_deref(), Some("id"));
    }

    #[test]
    fn candidate_timestamp_preferred_over_append() {
        // Both a conventional watermark and a monotonic id exist → prefer UPSERT.
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            timestamp_columns: vec![ts("updated_at", true, true)],
            has_update_trigger: true,
            monotonic_id_pk: Some("id".into()),
        };
        assert_eq!(classify(&facts).mode, IncMode::Upsert);
    }

    #[test]
    fn no_pk_no_watermark_is_full() {
        let facts = TableFacts::default();
        let s = classify(&facts);
        assert_eq!(s.mode, IncMode::Full);
        assert_eq!(s.confidence_pct, 0);
    }

    #[test]
    fn pk_only_no_signal_is_full() {
        let facts = TableFacts {
            pk_columns: vec!["id".into()],
            ..Default::default()
        };
        assert_eq!(classify(&facts).mode, IncMode::Full);
    }

    fn test_dsn() -> String {
        std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".into())
    }

    // Exercises every SQL probe in `gather_facts` against a real PostgreSQL:
    // a DB-enforced upsert table, an identity-PK append table, and a no-key
    // full-refresh table.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN"]
    async fn gather_facts_classifies_real_tables() {
        let pool = PgPool::connect(&test_dsn()).await.unwrap();

        sqlx::query("DROP TABLE IF EXISTS it_upsert, it_append, it_full CASCADE")
            .execute(&pool)
            .await
            .unwrap();

        // DB-enforced watermark: DEFAULT now() + a BEFORE UPDATE trigger.
        for stmt in [
            "CREATE TABLE it_upsert (id int PRIMARY KEY, v text, \
             updated_at timestamptz NOT NULL DEFAULT now())",
            "CREATE OR REPLACE FUNCTION it_touch() RETURNS trigger AS $$ \
             BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql",
            "CREATE TRIGGER it_upsert_touch BEFORE UPDATE ON it_upsert \
             FOR EACH ROW EXECUTE FUNCTION it_touch()",
            "INSERT INTO it_upsert(id, v) SELECT g, 'x' FROM generate_series(1, 100) g",
            "CREATE TABLE it_append (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)",
            "INSERT INTO it_append(v) SELECT 'y' FROM generate_series(1, 50)",
            "CREATE TABLE it_full (v text)",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }

        let upsert = classify(&gather_facts(&pool, "public.it_upsert").await.unwrap());
        assert_eq!(upsert.mode, IncMode::Upsert, "{upsert:?}");
        assert_eq!(upsert.tier, IncTier::Trusted, "{upsert:?}");
        assert_eq!(upsert.watermark_column.as_deref(), Some("updated_at"));

        let append = classify(&gather_facts(&pool, "public.it_append").await.unwrap());
        assert_eq!(append.mode, IncMode::Append, "{append:?}");
        assert_eq!(append.tier, IncTier::Audited, "{append:?}");
        assert_eq!(append.watermark_column.as_deref(), Some("id"));

        let full = classify(&gather_facts(&pool, "public.it_full").await.unwrap());
        assert_eq!(full.mode, IncMode::Full, "{full:?}");

        sqlx::query("DROP TABLE IF EXISTS it_upsert, it_append, it_full CASCADE")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION IF EXISTS it_touch() CASCADE")
            .execute(&pool)
            .await
            .unwrap();
    }
}
