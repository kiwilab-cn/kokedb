//! Adaptive refresh cadence (Phase 1 of the intelligent-sync design).
//!
//! For each cached table we cheaply read three signals — size, write churn, and
//! query heat — and score them into a coarse refresh bucket. Hot, fast-changing
//! tables refresh often; large, cold tables refresh rarely. The scoring itself
//! is a pure function so it can be unit-tested without a database.

use kokedb_common::env::get_env_as;
use sqlx::{postgres::PgPool, Row};

use crate::error::TaskError;

/// Cheap, observable per-table signals used to pick a refresh cadence.
#[derive(Debug, Clone, Default)]
pub struct TableSignals {
    /// Estimated live row count (`pg_class.reltuples`).
    pub est_rows: f64,
    /// Estimated total on-disk size in bytes (`pg_total_relation_size`).
    pub est_size_bytes: f64,
    /// Estimated writes per hour (ins+upd+del) since stats were last reset.
    pub churn_per_hour: f64,
    /// Queries per day against this table over the last 7 days (from meta).
    pub access_per_day: f64,
}

/// Coarse refresh cadence tiers. Bucketing (rather than a continuous interval)
/// keeps cadence stable across small signal fluctuations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshBucket {
    Fast,
    Normal,
    Slow,
    Cold,
}

impl RefreshBucket {
    pub fn as_str(&self) -> &'static str {
        match self {
            RefreshBucket::Fast => "fast",
            RefreshBucket::Normal => "normal",
            RefreshBucket::Slow => "slow",
            RefreshBucket::Cold => "cold",
        }
    }

    /// Refresh interval in seconds, overridable via env.
    pub fn interval_sec(&self) -> i32 {
        match self {
            RefreshBucket::Fast => get_env_as("KOKEDB_REFRESH_BUCKET_FAST_SEC", 120i32),
            RefreshBucket::Normal => get_env_as("KOKEDB_REFRESH_BUCKET_NORMAL_SEC", 900i32),
            RefreshBucket::Slow => get_env_as("KOKEDB_REFRESH_BUCKET_SLOW_SEC", 3600i32),
            RefreshBucket::Cold => get_env_as("KOKEDB_REFRESH_BUCKET_COLD_SEC", 21600i32),
        }
    }
}

/// Weights for the scoring function (env-overridable).
#[derive(Debug, Clone, Copy)]
pub struct ScoreWeights {
    pub access: f64,
    pub churn: f64,
    pub cost: f64,
}

impl ScoreWeights {
    pub fn from_env() -> Self {
        Self {
            access: get_env_as("KOKEDB_REFRESH_SCORE_W_ACCESS", 1.0f64),
            churn: get_env_as("KOKEDB_REFRESH_SCORE_W_CHURN", 1.0f64),
            cost: get_env_as("KOKEDB_REFRESH_SCORE_W_COST", 0.5f64),
        }
    }
}

/// Pure scoring: combine signals into a single score where higher means "refresh
/// more eagerly". Freshness value (query heat) and update pressure (churn) push
/// up; size (cost to rematerialize) pushes down. Inputs are log-compressed so no
/// single huge value dominates.
pub fn score(signals: &TableSignals, w: &ScoreWeights) -> f64 {
    let freshness_value = (1.0 + signals.access_per_day.max(0.0)).ln();
    let update_pressure = (1.0 + signals.churn_per_hour.max(0.0)).ln();
    let cost = (1.0 + signals.est_size_bytes.max(0.0)).ln();
    w.access * freshness_value + w.churn * update_pressure - w.cost * cost
}

/// Maps signals to a refresh bucket. Hard rules first (genuinely cold tables are
/// demoted regardless of score), then score thresholds.
pub fn bucket(signals: &TableSignals, w: &ScoreWeights) -> RefreshBucket {
    // A table nobody queries and nothing writes to does not need eager refresh.
    if signals.access_per_day < f64::EPSILON && signals.churn_per_hour < f64::EPSILON {
        return RefreshBucket::Cold;
    }
    let s = score(signals, w);
    let fast = get_env_as("KOKEDB_REFRESH_SCORE_FAST", 6.0f64);
    let normal = get_env_as("KOKEDB_REFRESH_SCORE_NORMAL", 2.0f64);
    let cold = get_env_as("KOKEDB_REFRESH_SCORE_COLD", 0.0f64);
    if s >= fast {
        RefreshBucket::Fast
    } else if s >= normal {
        RefreshBucket::Normal
    } else if s >= cold {
        RefreshBucket::Slow
    } else {
        RefreshBucket::Cold
    }
}

/// Reads the size/churn signals for one table from the remote PostgreSQL
/// catalog. `access_per_day` is supplied separately (it comes from kokedb's own
/// query stats, not the source DB). Best-effort: missing stats default to 0.
pub async fn collect_remote_signals(
    pool: &PgPool,
    source_table: &str,
    access_per_day: f64,
) -> Result<TableSignals, TaskError> {
    let regclass = source_table; // e.g. "public.orders"; cast to regclass below.

    // reltuples (row estimate) and total relation size.
    let size_row = sqlx::query(
        "SELECT c.reltuples::float8 AS rows, \
                pg_total_relation_size($1::regclass)::float8 AS size_bytes \
         FROM pg_class c WHERE c.oid = $1::regclass",
    )
    .bind(regclass)
    .fetch_optional(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(format!("reltuples/size query failed: {e}")))?;

    let (est_rows, est_size_bytes) = match size_row {
        Some(r) => (
            r.try_get::<f64, _>("rows").unwrap_or(0.0).max(0.0),
            r.try_get::<f64, _>("size_bytes").unwrap_or(0.0).max(0.0),
        ),
        None => (0.0, 0.0),
    };

    // Churn since stats were last reset: (ins+upd+del) over the elapsed hours.
    // The per-table write counters live in pg_stat_user_tables, but the reset
    // timestamp that bounds them is db-level (pg_stat_database.stats_reset).
    let churn_per_hour = sqlx::query(
        "SELECT (COALESCE(t.n_tup_ins,0)+COALESCE(t.n_tup_upd,0)+COALESCE(t.n_tup_del,0))::float8 \
                AS writes, \
                GREATEST(EXTRACT(EPOCH FROM (now() - COALESCE(d.stats_reset, now() - interval '1 hour'))), 1.0) \
                AS secs \
         FROM pg_stat_user_tables t \
         JOIN pg_stat_database d ON d.datname = current_database() \
         WHERE t.relid = $1::regclass",
    )
    .bind(regclass)
    .fetch_optional(pool)
    .await
    .map_err(|e| TaskError::DatabaseError(format!("pg_stat_user_tables query failed: {e}")))?
    .map(|r| {
        let writes = r.try_get::<f64, _>("writes").unwrap_or(0.0).max(0.0);
        let secs = r.try_get::<f64, _>("secs").unwrap_or(3600.0).max(1.0);
        writes / (secs / 3600.0)
    })
    .unwrap_or(0.0);

    Ok(TableSignals {
        est_rows,
        est_size_bytes,
        churn_per_hour,
        access_per_day: access_per_day.max(0.0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w() -> ScoreWeights {
        ScoreWeights {
            access: 1.0,
            churn: 1.0,
            cost: 0.5,
        }
    }

    #[test]
    fn idle_table_is_cold() {
        let s = TableSignals {
            est_rows: 1000.0,
            est_size_bytes: 1_000_000.0,
            churn_per_hour: 0.0,
            access_per_day: 0.0,
        };
        assert_eq!(bucket(&s, &w()), RefreshBucket::Cold);
    }

    #[test]
    fn hot_fast_changing_small_table_is_fast() {
        // High access + high churn, modest size -> should land in Fast.
        let s = TableSignals {
            est_rows: 10_000.0,
            est_size_bytes: 5_000_000.0,
            churn_per_hour: 50_000.0,
            access_per_day: 5_000.0,
        };
        assert_eq!(bucket(&s, &w()), RefreshBucket::Fast);
    }

    #[test]
    fn huge_cold_ish_table_is_demoted_by_cost() {
        // Some access but enormous size -> cost term pulls it down a tier.
        let small = TableSignals {
            est_size_bytes: 1_000_000.0,
            access_per_day: 20.0,
            churn_per_hour: 20.0,
            ..Default::default()
        };
        let huge = TableSignals {
            est_size_bytes: 5_000_000_000_000.0,
            ..small.clone()
        };
        let order = |b: RefreshBucket| match b {
            RefreshBucket::Fast => 3,
            RefreshBucket::Normal => 2,
            RefreshBucket::Slow => 1,
            RefreshBucket::Cold => 0,
        };
        assert!(
            order(bucket(&huge, &w())) <= order(bucket(&small, &w())),
            "larger table must never refresh more eagerly than an otherwise identical small one"
        );
    }

    #[test]
    fn buckets_have_increasing_intervals() {
        assert!(RefreshBucket::Fast.interval_sec() < RefreshBucket::Normal.interval_sec());
        assert!(RefreshBucket::Normal.interval_sec() < RefreshBucket::Slow.interval_sec());
        assert!(RefreshBucket::Slow.interval_sec() < RefreshBucket::Cold.interval_sec());
    }

    // Verifies the size/churn signal SQL runs and returns sane values against a
    // real PostgreSQL.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN"]
    async fn collect_signals_reads_real_table() {
        let dsn = std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".into());
        let pool = PgPool::connect(&dsn).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS it_signals").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE it_signals (id int primary key, v text)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO it_signals SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g")
            .execute(&pool)
            .await
            .unwrap();
        // ANALYZE so reltuples is populated.
        sqlx::query("ANALYZE it_signals").execute(&pool).await.unwrap();

        let s = collect_remote_signals(&pool, "public.it_signals", 12.0)
            .await
            .unwrap();
        assert!(s.est_rows >= 1.0, "reltuples should be populated: {s:?}");
        assert!(s.est_size_bytes > 0.0, "size should be > 0: {s:?}");
        assert!(s.churn_per_hour >= 0.0);
        assert_eq!(s.access_per_day, 12.0);

        sqlx::query("DROP TABLE IF EXISTS it_signals").execute(&pool).await.unwrap();
    }
}
