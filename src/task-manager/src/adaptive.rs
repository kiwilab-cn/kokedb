//! Adaptive refresh scheduling (Phase 1 of the intelligent-sync design).
//!
//! Two entry points, both driven from the per-catalog scheduler:
//! - [`reevaluate_catalog`] recomputes each cached table's refresh cadence from
//!   observed signals and persists it to `system.table_sync_policy`.
//! - [`tick_refresh_due`] enqueues only the tables whose cadence has elapsed.

use std::sync::Arc;

use kokedb_common::cache_policy::CachePolicy;
use kokedb_common::env::get_env_as;
use kokedb_meta::catalog_list::{
    PostgreSQLMetaCatalogProviderList, TableIncPolicy, TableSyncPolicy,
};
use log::{info, warn};
use sqlx::postgres::PgPool;

use crate::cache_sync_task::{refresh_single_table, select_tables_for_policy};
use crate::error::TaskError;
use crate::incremental_infer::{classify, gather_facts, IncTier};
use crate::llm::{self, LlmConfig, TableStats, TableSummary};
use crate::table_signals::{bucket, collect_remote_signals, score, ScoreWeights};
use crate::task_manager::TaskManager;

/// A stable fingerprint of the schema facts that drive inference, used as the
/// LLM cache key: re-inference reuses a prior LLM decision only while this is
/// unchanged. DefaultHasher is deterministic (fixed keys), so it is stable
/// across process restarts.
fn schema_fingerprint(facts: &crate::incremental_infer::TableFacts) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut pk = facts.pk_columns.clone();
    pk.sort();
    pk.hash(&mut h);
    let mut ts: Vec<_> = facts
        .timestamp_columns
        .iter()
        .map(|c| (c.name.clone(), c.not_null, c.has_default_now))
        .collect();
    ts.sort();
    ts.hash(&mut h);
    facts.has_update_trigger.hash(&mut h);
    facts.monotonic_id_pk.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Splits a `schema.table` (or bare `table`) into `(schema, table)`, defaulting
/// the schema to `public` like the rest of the sync path.
fn split_schema_table(name: &str) -> (String, String) {
    match name.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), name.to_string()),
    }
}

/// Recomputes and persists the adaptive refresh policy for every table the
/// catalog's cache policy selects. Best-effort per table: a table whose signals
/// cannot be read keeps its previous policy (or the seeded default).
pub async fn reevaluate_catalog(
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    catalog: &str,
    dsn: &str,
    cache_policy: &CachePolicy,
) -> Result<(), TaskError> {
    let tables = select_tables_for_policy(catalog, dsn, cache_policy).await?;
    if tables.is_empty() {
        return Ok(());
    }

    // MySQL sources don't have the PostgreSQL-specific signal queries used for
    // adaptive cadence/incremental inference yet, so seed each selected table
    // with a default full-sync policy (normal cadence) and skip the rest.
    if kokedb_common::source::is_mysql(dsn) {
        for table in &tables {
            let (schema, tbl) = split_schema_table(table);
            let policy = TableSyncPolicy {
                refresh_bucket: "normal".to_string(),
                refresh_interval_sec: 900,
                est_row_count: None,
                est_size_bytes: None,
                churn_per_hour: None,
                access_per_day: None,
                score: None,
            };
            if let Err(e) = meta
                .upsert_table_sync_policy(catalog, &schema, &tbl, &policy)
                .await
            {
                warn!("Failed to persist default MySQL policy for {catalog}.{table}: {e}");
            }
        }
        info!(
            "Adaptive reeval for MySQL catalog '{catalog}': seeded {} default policies",
            tables.len()
        );
        return Ok(());
    }

    let access = meta.get_access_per_day(catalog).await.unwrap_or_default();
    let weights = ScoreWeights::from_env();

    let pool = PgPool::connect(dsn)
        .await
        .map_err(|e| TaskError::DatabaseError(format!("connect source for reeval failed: {e}")))?;

    let mut updated = 0usize;
    for table in &tables {
        let access_per_day = access.get(table).copied().unwrap_or(0.0) as f64;
        let signals = match collect_remote_signals(&pool, table, access_per_day).await {
            Ok(s) => s,
            Err(e) => {
                warn!("Skipping reeval for {catalog}.{table}: {e}");
                continue;
            }
        };
        let b = bucket(&signals, &weights);
        let (schema, tbl) = split_schema_table(table);
        let policy = TableSyncPolicy {
            refresh_bucket: b.as_str().to_string(),
            refresh_interval_sec: b.interval_sec(),
            est_row_count: Some(signals.est_rows as i64),
            est_size_bytes: Some(signals.est_size_bytes as i64),
            churn_per_hour: Some(signals.churn_per_hour as f32),
            access_per_day: Some(signals.access_per_day as f32),
            score: Some(score(&signals, &weights) as f32),
        };
        if let Err(e) = meta
            .upsert_table_sync_policy(catalog, &schema, &tbl, &policy)
            .await
        {
            warn!("Failed to persist policy for {catalog}.{table}: {e}");
        } else {
            updated += 1;
        }

        // Infer the incremental strategy (rule-based, optionally LLM-assisted)
        // and persist it. The upsert preserves any existing lifecycle status.
        if get_env_as("KOKEDB_INFER_INCREMENTAL", true) {
            if let Err(e) =
                infer_and_persist(&meta, &pool, catalog, &schema, &tbl, table, &signals).await
            {
                warn!("Incremental inference failed for {catalog}.{table}: {e}");
            }
        }
    }
    pool.close().await;

    info!("Adaptive reeval for catalog '{catalog}': updated {updated}/{} table policies", tables.len());
    Ok(())
}

/// Infers a table's incremental strategy (rule-based, with an optional LLM
/// assist when the rules are unsure) and persists it. A prior LLM decision that
/// is already in the validation pipeline is left untouched.
async fn infer_and_persist(
    meta: &PostgreSQLMetaCatalogProviderList,
    pool: &PgPool,
    catalog: &str,
    schema: &str,
    tbl: &str,
    qualified_table: &str,
    signals: &crate::table_signals::TableSignals,
) -> Result<(), TaskError> {
    let facts = gather_facts(pool, qualified_table).await?;
    let schema_hash = schema_fingerprint(&facts);

    // Don't overwrite a user override. Reuse a prior LLM recommendation that is
    // already in the validation pipeline *only while the schema is unchanged* —
    // a schema change re-triggers inference (and a fresh LLM call if needed).
    if let Ok(Some(existing)) = meta.get_table_inc_policy(catalog, schema, tbl).await {
        if existing.source == "user"
            || (existing.source == "llm"
                && existing.inc_status != "none"
                && existing.schema_hash.as_deref() == Some(schema_hash.as_str()))
        {
            return Ok(());
        }
    }

    let rule = classify(&facts);

    // Build the policy from the rule by default.
    let mut inc = TableIncPolicy {
        inc_mode: rule.mode.as_str().to_string(),
        inc_status: rule.tier.initial_status().to_string(),
        inc_tier: rule.tier.as_str().to_string(),
        watermark_column: rule.watermark_column.clone(),
        pk_columns: (!rule.pk_columns.is_empty()).then(|| rule.pk_columns.join(",")),
        source: "rule".to_string(),
        confidence: Some(rule.confidence_pct as f32 / 100.0),
        reason: Some(rule.reason.clone()),
        audit_passes: 0,
        divergence_count: 0,
        schema_hash: Some(schema_hash.clone()),
    };

    // When the rule is unsure and an LLM is configured, ask it and prefer a valid
    // recommendation. An LLM strategy is never trusted directly — it starts as
    // `suggested`/`audited` and must pass shadow validation like any inference.
    let unsure = (rule.confidence_pct as f32 / 100.0) < llm::confidence_threshold();
    if unsure {
        if let Some(cfg) = LlmConfig::from_env() {
            let columns = llm::gather_columns(pool, qualified_table).await.unwrap_or_default();
            let summary = TableSummary {
                table: qualified_table.to_string(),
                columns,
                primary_key: facts.pk_columns.clone(),
                has_update_trigger: facts.has_update_trigger,
                stats: Some(TableStats {
                    est_rows: signals.est_rows as i64,
                    est_size_bytes: signals.est_size_bytes as i64,
                    churn_per_hour: signals.churn_per_hour,
                    timestamp_columns: facts
                        .timestamp_columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }),
            };
            if let Some(rec) = llm::recommend(&cfg, &summary).await {
                inc.inc_mode = rec.inc_mode.clone();
                inc.watermark_column = rec.watermark_column.clone();
                inc.pk_columns = (!rec.pk_columns.is_empty()).then(|| rec.pk_columns.join(","));
                // Inferred by a model → always audited, must be validated.
                inc.inc_tier = IncTier::Audited.as_str().to_string();
                inc.inc_status = if rec.inc_mode == "full" { "none" } else { "suggested" }
                    .to_string();
                inc.source = "llm".to_string();
                inc.confidence = Some(rec.confidence);
                inc.reason = Some(format!("[llm] {}", rec.reason));
            }
        }
    }

    meta.upsert_table_inc_policy(catalog, schema, tbl, &inc)
        .await
        .map_err(|e| TaskError::MetaReqeustError(format!("persist inc policy failed: {e}")))?;
    Ok(())
}

/// Enqueues a sync for every table in the catalog whose refresh interval has
/// elapsed, skipping tables that already have an in-flight sync task.
pub async fn tick_refresh_due(
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    catalog: &str,
    dsn: &str,
    task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let due = meta.get_due_refresh_tables(catalog).await.map_err(|e| {
        TaskError::MetaReqeustError(format!("Failed to query due tables: {e}"))
    })?;

    let mut queued = 0usize;
    for table in due {
        // Avoid piling up duplicate tasks when a sync runs longer than the tick.
        if task_manager.has_inflight_table_task(catalog, &table) {
            continue;
        }
        match refresh_single_table(catalog, dsn, &table, task_manager.clone()).await {
            Ok(_) => queued += 1,
            Err(e) => warn!("Failed to enqueue adaptive refresh for {catalog}.{table}: {e}"),
        }
    }
    if queued > 0 {
        info!("Adaptive tick for catalog '{catalog}': queued {queued} due tables");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    fn test_dsn() -> String {
        std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".into())
    }

    // End-to-end: reevaluate_catalog must select the listed tables, score a
    // cadence, infer an incremental strategy, and persist both into
    // system.table_sync_policy.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN + PG_META_DSN"]
    async fn reevaluate_persists_cadence_and_strategy() {
        let pool = PgPool::connect(&test_dsn()).await.unwrap();
        for stmt in [
            "DROP TABLE IF EXISTS it_e2e_upsert, it_e2e_full CASCADE",
            "CREATE TABLE it_e2e_upsert (id int PRIMARY KEY, v text, \
             updated_at timestamptz NOT NULL DEFAULT now())",
            "INSERT INTO it_e2e_upsert(id,v) SELECT g,'x' FROM generate_series(1,200) g",
            "CREATE TABLE it_e2e_full (v text)",
            "ANALYZE it_e2e_upsert",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }

        // Ensure the meta schema exists (init_db via the meta client), then use a
        // separate pool for raw assertion/cleanup queries (local_pool is private).
        let meta = Arc::new(PostgreSQLMetaCatalogProviderList::new().await.unwrap());
        meta.init_db().await.unwrap();
        let meta_dsn = std::env::var("PG_META_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/kokedb".into());
        let meta_pool = PgPool::connect(&meta_dsn).await.unwrap();
        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = 'it_e2e'")
            .execute(&meta_pool)
            .await
            .unwrap();

        let policy = CachePolicy::Select {
            table_set: "public.it_e2e_upsert,public.it_e2e_full".into(),
        };
        reevaluate_catalog(meta.clone(), "it_e2e", &test_dsn(), &policy)
            .await
            .unwrap();

        let rows = sqlx::query(
            "SELECT table_name, refresh_bucket, refresh_interval_sec, inc_mode, inc_status, \
                    est_row_count FROM system.table_sync_policy \
             WHERE catalog = 'it_e2e' ORDER BY table_name",
        )
        .fetch_all(&meta_pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "both tables should have a policy row");

        let by_name = |n: &str| {
            rows.iter()
                .find(|r| r.get::<String, _>("table_name") == n)
                .unwrap_or_else(|| panic!("missing {n}"))
        };
        let upsert = by_name("it_e2e_upsert");
        assert_eq!(upsert.get::<String, _>("inc_mode"), "upsert");
        assert!(upsert.get::<i32, _>("refresh_interval_sec") > 0);
        assert!(upsert.get::<Option<i64>, _>("est_row_count").unwrap_or(0) >= 1);

        let full = by_name("it_e2e_full");
        assert_eq!(full.get::<String, _>("inc_mode"), "full");

        sqlx::query("DELETE FROM system.table_sync_policy WHERE catalog = 'it_e2e'")
            .execute(&meta_pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS it_e2e_upsert, it_e2e_full CASCADE")
            .execute(&pool)
            .await
            .unwrap();
    }
}
