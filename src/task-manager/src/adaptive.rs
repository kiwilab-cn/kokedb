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
use crate::incremental_infer::infer_strategy;
use crate::table_signals::{bucket, collect_remote_signals, score, ScoreWeights};
use crate::task_manager::TaskManager;

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
    catalog: &str,
    dsn: &str,
    cache_policy: &CachePolicy,
) -> Result<(), TaskError> {
    let meta = PostgreSQLMetaCatalogProviderList::new().await.map_err(|e| {
        TaskError::MetaReqeustError(format!("Failed to create meta client: {e}"))
    })?;

    let tables = select_tables_for_policy(catalog, dsn, cache_policy).await?;
    if tables.is_empty() {
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

        // Infer the incremental strategy and persist it. The upsert preserves any
        // existing lifecycle status, so a trusted strategy activates immediately
        // while probation/audited ones wait for validation (Phase 2c).
        if get_env_as("KOKEDB_INFER_INCREMENTAL", true) {
            match infer_strategy(&pool, table).await {
                Ok(s) => {
                    let inc = TableIncPolicy {
                        inc_mode: s.mode.as_str().to_string(),
                        inc_status: s.tier.initial_status().to_string(),
                        inc_tier: s.tier.as_str().to_string(),
                        watermark_column: s.watermark_column.clone(),
                        pk_columns: (!s.pk_columns.is_empty())
                            .then(|| s.pk_columns.join(",")),
                        source: "rule".to_string(),
                        confidence: Some(s.confidence_pct as f32 / 100.0),
                        reason: Some(s.reason.clone()),
                    };
                    if let Err(e) = meta
                        .upsert_table_inc_policy(catalog, &schema, &tbl, &inc)
                        .await
                    {
                        warn!("Failed to persist inc policy for {catalog}.{table}: {e}");
                    }
                }
                Err(e) => warn!("Incremental inference failed for {catalog}.{table}: {e}"),
            }
        }
    }
    pool.close().await;

    info!("Adaptive reeval for catalog '{catalog}': updated {updated}/{} table policies", tables.len());
    Ok(())
}

/// Enqueues a sync for every table in the catalog whose refresh interval has
/// elapsed, skipping tables that already have an in-flight sync task.
pub async fn tick_refresh_due(
    catalog: &str,
    dsn: &str,
    task_manager: Arc<TaskManager>,
) -> Result<(), TaskError> {
    let meta = PostgreSQLMetaCatalogProviderList::new().await.map_err(|e| {
        TaskError::MetaReqeustError(format!("Failed to create meta client: {e}"))
    })?;

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
