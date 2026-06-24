use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use kokedb_cache::result_cache::ResultCache;
use kokedb_common::{
    env::get_env_as,
    file::{
        cleanup_old_directories, ensure_dir_exists, get_remote_catalog_parent_local_path, TempPath,
    },
};
use kokedb_meta::{
    catalog_list::{PostgreSQLMetaCatalogProviderList, TableSyncState},
    schema::SchemaTable,
};
use log::{error, info};
use sqlx::PgPool;

use crate::{
    error::TaskError,
    incremental,
    read_postgres::{convert_postgres_to_parquet, PostgresToParquetConverter},
    task_manager::CacheTableTaskConfig,
};

/// After this many consecutive incremental merges, do a full refresh to
/// reconcile source-side hard deletes (which timestamp watermarks cannot see).
fn force_full_every() -> i32 {
    get_env_as("KOKEDB_FORCE_FULL_EVERY", 10i32)
}

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        cache: ResultCache,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError>;
}

/// Proactively recomputes and re-caches query results. Implemented in the query
/// layer (which owns the execution path) and injected here, so the task manager
/// does not depend on the query crate.
#[async_trait::async_trait]
pub trait ResultRefresher: Send + Sync {
    /// Re-execute and re-cache the queries behind the given result-cache keys.
    async fn refresh(&self, cache_keys: Vec<u128>);
}

/// A settable slot for the [`ResultRefresher`], filled after the query layer is
/// constructed (breaks the executor <-> query-context construction cycle).
pub type ResultRefresherSlot = Arc<Mutex<Option<Arc<dyn ResultRefresher>>>>;

pub struct DataSyncExecutor {
    refresher: ResultRefresherSlot,
}

impl DataSyncExecutor {
    pub fn new(refresher: ResultRefresherSlot) -> Self {
        Self { refresher }
    }
}

/// The sync strategy chosen for one run, decided from detected metadata and the
/// persisted sync state.
enum SyncPlan {
    /// Full snapshot rebuild (default / fallback).
    Full {
        watermark_column: Option<String>,
        pk_columns: Vec<String>,
    },
    /// Incremental: fetch rows changed since `last_watermark` and merge.
    Incremental {
        watermark_column: String,
        pk_columns: Vec<String>,
        last_watermark: String,
        old_path: String,
    },
    /// The source has not changed since the last sync — keep the snapshot as-is.
    SkipUnchanged {
        watermark_column: String,
        pk_columns: Vec<String>,
        last_watermark: String,
    },
}

#[async_trait::async_trait]
impl TaskExecutor for DataSyncExecutor {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        cache: ResultCache,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError> {
        // Record a cache_job row around the sync (best-effort observability).
        let (job_schema, job_table) = config
            .local_table
            .split_once('.')
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or_else(|| ("public".to_string(), config.local_table.clone()));
        let job_catalog = config.catalog_name.clone();
        let job = match PostgreSQLMetaCatalogProviderList::new().await {
            Ok(m) => {
                let id = m
                    .insert_cache_job(&job_catalog, &job_schema, &job_table)
                    .await
                    .ok();
                Some((m, id))
            }
            Err(_) => None,
        };

        let result = self.run_sync(config, cache, progress_callback).await;

        if let Some((m, Some(id))) = job {
            let (status, err) = match &result {
                Ok(_) => ("success", None),
                Err(e) => ("failed", Some(e.to_string())),
            };
            let _ = m.complete_cache_job(id, status, err).await;
        }
        result
    }
}

impl DataSyncExecutor {
    async fn run_sync(
        &self,
        config: CacheTableTaskConfig,
        cache: ResultCache,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError> {
        // Report coarse progress at the natural milestones of a sync.
        let report = |p: f32| {
            if let Some(cb) = progress_callback.as_ref() {
                cb(p);
            }
        };

        info!("Received task: {:?}", &config);
        report(0.05);

        let dsn = config.dsn;
        let catalog = &config.catalog_name;
        let source_table = &config.source_table;
        let local_table = &config.local_table;
        let table_base_path = format!(
            "{}/{}/{}",
            get_remote_catalog_parent_local_path(),
            catalog,
            local_table.replace('.', "/"),
        );
        let local_path = format!("{}/{}", &table_base_path, uuid::Uuid::new_v4());
        // Remove the snapshot dir if the sync fails before it is registered in the
        // meta store (disarmed once save_table_schema commits it). Prevents
        // orphaned snapshots on partial failure.
        let mut snapshot_guard = TempPath::new(&local_path);

        let (schema, table) = local_table
            .split_once('.')
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or(("public".to_string(), local_table.to_string()));

        let meta = PostgreSQLMetaCatalogProviderList::new()
            .await
            .map_err(|_x| {
                TaskError::DatabaseError("Failed to connect meta postgresql server.".to_string())
            })?;

        // Catalog-level Hive partition column (e.g. `with properties(partition_by="dt")`).
        let partition_by = meta.get_catalog_partition_by(catalog).await.ok().flatten();

        // Decide full vs incremental vs skip from detected metadata + state.
        let plan = plan_sync(&dsn, source_table, catalog, &schema, &table, &meta).await?;
        // Partitioned tables are always rebuilt fully (incremental + partitioned
        // merge is not supported); reuse the columns already detected.
        let plan = if partition_by.is_some() {
            match plan {
                SyncPlan::Incremental {
                    watermark_column,
                    pk_columns,
                    ..
                }
                | SyncPlan::SkipUnchanged {
                    watermark_column,
                    pk_columns,
                    ..
                } => SyncPlan::Full {
                    watermark_column: Some(watermark_column),
                    pk_columns,
                },
                full => full,
            }
        } else {
            plan
        };
        report(0.1);

        // Tracks the partition column actually used (persisted after the schema).
        let mut partition_used: Option<String> = None;

        // Materialize the new snapshot (unless skipping) and compute the state
        // to persist.
        let (arrow_schema, new_state) = match plan {
            SyncPlan::SkipUnchanged {
                watermark_column,
                pk_columns,
                last_watermark,
            } => {
                info!(
                    "Source {}.{}.{} unchanged since last sync; keeping snapshot.",
                    catalog, schema, table
                );
                // Refresh liveness without touching the snapshot or caches.
                let state = TableSyncState {
                    watermark_column: Some(watermark_column),
                    pk_columns: Some(pk_columns.join(",")),
                    last_watermark: Some(last_watermark),
                    sync_mode: "incremental".to_string(),
                    incremental_runs: meta
                        .get_table_sync_state(catalog, &schema, &table)
                        .await
                        .ok()
                        .flatten()
                        .map(|s| s.incremental_runs)
                        .unwrap_or(0),
                };
                let _ = meta
                    .upsert_table_sync_state(catalog, &schema, &table, &state)
                    .await;
                report(1.0);
                return Ok(());
            }
            SyncPlan::Full {
                watermark_column,
                pk_columns,
            } => {
                // Cluster the snapshot by the watermark (or PK) column so the
                // parquet files/row-groups prune well for selective predicates.
                let sort_column = watermark_column
                    .clone()
                    .or_else(|| pk_columns.first().cloned());
                let arrow_schema = match partition_by.as_deref() {
                    // Hive-partition the snapshot when the catalog declares a
                    // partition column and this table actually has it.
                    Some(col) => {
                        let flat = format!("/tmp/kokedb-flat/{}", uuid::Uuid::new_v4());
                        // Cleaned up on any exit from this arm (incl. early error).
                        let _flat_guard = TempPath::new(&flat);
                        let schema = convert_postgres_to_parquet(
                            &dsn,
                            source_table,
                            &flat,
                            sort_column.as_deref(),
                        )
                        .await
                        .map_err(|x| {
                            TaskError::ExecutionFailed(format!(
                                "Failed to write postgresql table to parquet with error: {}",
                                x
                            ))
                        })?;
                        if schema.field_with_name(col).is_ok() {
                            ensure_dir_exists(&local_path).await?;
                            incremental::write_partitioned(
                                &flat,
                                &local_path,
                                col,
                                schema.clone(),
                            )
                            .await?;
                            let _ = tokio::fs::remove_dir_all(&flat).await;
                            partition_used = Some(col.to_string());
                        } else {
                            // Table lacks the partition column: keep it flat.
                            tokio::fs::rename(&flat, &local_path).await.map_err(|e| {
                                TaskError::WriteParquetError(format!(
                                    "Failed to move flat snapshot: {e}"
                                ))
                            })?;
                        }
                        schema
                    }
                    None => convert_postgres_to_parquet(
                        &dsn,
                        source_table,
                        &local_path,
                        sort_column.as_deref(),
                    )
                    .await
                    .map_err(|x| {
                        TaskError::ExecutionFailed(format!(
                            "Failed to write postgresql table to parquet with error: {}",
                            x
                        ))
                    })?,
                };

                let last_watermark = match &watermark_column {
                    Some(col) => {
                        let pool = connect_source(&dsn).await?;
                        let wm = incremental::max_watermark(&pool, source_table, col).await?;
                        pool.close().await;
                        wm
                    }
                    None => None,
                };

                let state = TableSyncState {
                    watermark_column,
                    pk_columns: (!pk_columns.is_empty()).then(|| pk_columns.join(",")),
                    last_watermark,
                    sync_mode: "full".to_string(),
                    incremental_runs: 0,
                };
                (arrow_schema, state)
            }
            SyncPlan::Incremental {
                watermark_column,
                pk_columns,
                last_watermark,
                old_path,
            } => {
                let (arrow_schema, new_watermark) = run_incremental(
                    &dsn,
                    source_table,
                    &local_path,
                    &old_path,
                    &watermark_column,
                    &last_watermark,
                    &pk_columns,
                )
                .await?;

                let prev_runs = meta
                    .get_table_sync_state(catalog, &schema, &table)
                    .await
                    .ok()
                    .flatten()
                    .map(|s| s.incremental_runs)
                    .unwrap_or(0);

                let state = TableSyncState {
                    watermark_column: Some(watermark_column),
                    pk_columns: Some(pk_columns.join(",")),
                    last_watermark: new_watermark.or(Some(last_watermark)),
                    sync_mode: "incremental".to_string(),
                    incremental_runs: prev_runs + 1,
                };
                (arrow_schema, state)
            }
        };
        report(0.8);

        let schema_info = SchemaTable {
            catalog: catalog.as_str(),
            schema: schema.as_str(),
            table: table.as_str(),
            arrow_schema: arrow_schema.clone(),
            local_path: &local_path,
        };

        meta.save_table_schema(&schema_info).await.map_err(|_x| {
            TaskError::DatabaseError(
                "Failed to save table schema to meta postgresql server.".to_string(),
            )
        })?;
        // The snapshot is now registered; keep it even if later steps fail.
        snapshot_guard.disarm();

        // Record (or clear) the Hive partition column for the query path.
        if let Err(e) = meta
            .set_table_partition_column(catalog, &schema, &table, partition_used.as_deref())
            .await
        {
            error!("Failed to persist partition column for {}: {}", schema_info, e);
        }

        if let Err(e) = meta
            .upsert_table_sync_state(catalog, &schema, &table, &new_state)
            .await
        {
            error!("Failed to persist sync state for {}: {}", schema_info, e);
        }
        report(0.85);

        // Invalidate cached query results that referenced this table.
        let table_cache_key_list = meta.get_table_cache_key(&schema_info).await.map_err(|_x| {
            TaskError::DatabaseError(
                "Failed to read table cache keys from meta postgresql server.".to_string(),
            )
        })?;

        if let Some(cache_key_list) = table_cache_key_list {
            // Invalidate first so a failed refresh never leaves stale results.
            for cache_key in &cache_key_list {
                if cache.delete(*cache_key).await.is_err() {
                    error!("Failed to delete table: {} key from cache.", schema_info);
                }
            }
            // Proactively recompute and re-cache the affected queries, if a
            // refresher has been wired in (best-effort; lazy recompute otherwise).
            let refresher = self
                .refresher
                .lock()
                .ok()
                .and_then(|guard| guard.clone());
            if let Some(refresher) = refresher {
                refresher.refresh(cache_key_list).await;
            }
        }
        report(0.95);

        let cache_versions = get_env_as("CACHE_KEEP_NUM", 3usize);
        if let Err(x) =
            cleanup_old_directories(&table_base_path, cache_versions, Some(&local_path)).await
        {
            error!(
                "Failed to clean table path: {} with error: {}",
                &table_base_path,
                x.to_string()
            );
        }

        report(1.0);
        info!(
            "Success sync ({}) table {}.{}.{} data to path {}",
            new_state.sync_mode, &catalog, &schema, &table, &local_path
        );

        Ok(())
    }
}

async fn connect_source(dsn: &str) -> Result<PgPool, TaskError> {
    PgPool::connect(dsn).await.map_err(|e| {
        TaskError::DatabaseError(format!("Failed to connect source postgresql: {e}"))
    })
}

/// Decides the sync strategy for this run.
async fn plan_sync(
    dsn: &str,
    source_table: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    meta: &PostgreSQLMetaCatalogProviderList,
) -> Result<SyncPlan, TaskError> {
    let pool = connect_source(dsn).await?;
    let pk_columns = incremental::detect_primary_keys(&pool, source_table).await?;
    let watermark_column = incremental::detect_watermark_column(&pool, source_table).await?;

    // Phase 2: an explicit inferred policy overrides live detection. An `active`
    // strategy supplies the watermark/pk to use; a `rejected` one (validation
    // found incremental unsafe) forces full. Otherwise (no policy yet, or still
    // `suggested`) we keep the legacy live-detection behavior — no regression.
    let inc_policy = meta
        .get_table_inc_policy(catalog, schema, table)
        .await
        .ok()
        .flatten();
    let (pk_columns, watermark_column, policy_forces_full) = match &inc_policy {
        Some(p)
            if p.inc_status == "active" && matches!(p.inc_mode.as_str(), "upsert" | "append") =>
        {
            let pk = p
                .pk_columns
                .as_ref()
                .map(|s| s.split(',').map(|x| x.to_string()).collect::<Vec<_>>())
                .filter(|v| !v.is_empty())
                .unwrap_or(pk_columns);
            let wm = p.watermark_column.clone().or(watermark_column);
            (pk, wm, false)
        }
        Some(p) if p.inc_status == "rejected" => (pk_columns, watermark_column, true),
        _ => (pk_columns, watermark_column, false),
    };

    let sync_state = meta
        .get_table_sync_state(catalog, schema, table)
        .await
        .ok()
        .flatten()
        .unwrap_or_default();

    // The previous snapshot path (empty string if the table was never synced).
    let old_path = meta
        .get_table_schema(catalog, schema, table)
        .await
        .map(|(_, path)| path)
        .unwrap_or_default();

    let force_full = sync_state.incremental_runs >= force_full_every();

    let can_incremental = !pk_columns.is_empty()
        && watermark_column.is_some()
        && !old_path.is_empty()
        && sync_state.last_watermark.is_some()
        && !force_full
        && !policy_forces_full;

    if !can_incremental {
        pool.close().await;
        return Ok(SyncPlan::Full {
            watermark_column,
            pk_columns,
        });
    }

    let wm_col = watermark_column.unwrap_or_default();
    let last_watermark = sync_state.last_watermark.unwrap_or_default();
    let changed =
        incremental::has_changes_since(&pool, source_table, &wm_col, &last_watermark).await?;
    pool.close().await;

    if changed {
        Ok(SyncPlan::Incremental {
            watermark_column: wm_col,
            pk_columns,
            last_watermark,
            old_path,
        })
    } else {
        Ok(SyncPlan::SkipUnchanged {
            watermark_column: wm_col,
            pk_columns,
            last_watermark,
        })
    }
}

/// Fetches the delta since `last_watermark`, merges it with the previous
/// snapshot into `new_local_path`, and returns the snapshot schema plus the new
/// max watermark.
async fn run_incremental(
    dsn: &str,
    source_table: &str,
    new_local_path: &str,
    old_path: &str,
    watermark_column: &str,
    last_watermark: &str,
    pk_columns: &[String],
) -> Result<(Arc<Schema>, Option<String>), TaskError> {
    let delta_path = format!("/tmp/kokedb-delta/{}", uuid::Uuid::new_v4());
    let _delta_guard = TempPath::new(&delta_path);
    // Ensure both directories exist so the merge can register them even if the
    // delta turns out empty.
    ensure_dir_exists(&delta_path).await?;
    ensure_dir_exists(new_local_path).await?;

    let where_clause = incremental::incremental_where(watermark_column, last_watermark);
    let batch_size = get_env_as("KOKEDB_READ_TABLE_BATCH_SIZE", 300000usize);
    let converter = PostgresToParquetConverter::new(dsn)
        .await?
        .with_batch_size(batch_size);
    let arrow_schema = converter
        .convert_table_to_parquet_where(
            source_table,
            &delta_path,
            Some(&where_clause),
            Some(watermark_column),
        )
        .await?;

    incremental::merge_snapshot(
        old_path,
        &delta_path,
        new_local_path,
        pk_columns,
        arrow_schema.clone(),
        Some(watermark_column),
    )
    .await?;

    let _ = tokio::fs::remove_dir_all(&delta_path).await;

    // The new watermark is the current source max (read fresh).
    let pool = connect_source(dsn).await?;
    let new_watermark = incremental::max_watermark(&pool, source_table, watermark_column).await?;
    pool.close().await;

    Ok((arrow_schema, new_watermark))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use kokedb_cache::result_cache::ResultCache;

    fn test_dsn() -> String {
        std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".to_string())
    }

    async fn count_rows(path: &str) -> i64 {
        let ctx = SessionContext::new();
        ctx.register_parquet("t", path, ParquetReadOptions::default())
            .await
            .unwrap();
        let df = ctx.sql("SELECT COUNT(*) AS c FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        col.value(0)
    }

    async fn val_of(path: &str, id: i32) -> Option<String> {
        let ctx = SessionContext::new();
        ctx.register_parquet("t", path, ParquetReadOptions::default())
            .await
            .unwrap();
        let df = ctx
            .sql(&format!("SELECT val FROM t WHERE id = {id}"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return None;
        }
        // Use a type-agnostic formatter: DataFusion may decode parquet strings
        // as Utf8 or Utf8View depending on version/config.
        use arrow::util::display::{ArrayFormatter, FormatOptions};
        let fmt =
            ArrayFormatter::try_new(batches[0].column(0), &FormatOptions::default()).unwrap();
        Some(fmt.value(0).to_string())
    }

    // Verifies full sync, then a watermark-driven incremental merge that applies
    // an update (dedup by PK) and an insert, producing a correct snapshot.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via `make integration-test`"]
    async fn incremental_sync_merges_updates_and_inserts() {
        let dsn = test_dsn();
        let meta = PostgreSQLMetaCatalogProviderList::new().await.unwrap();
        meta.init_db().await.unwrap();

        // Reset persisted meta state for this table so the run is deterministic
        // regardless of previous runs (sync state + snapshot live in the meta DB,
        // which is a different database than the source under KOKEDB_TEST_DSN).
        let meta_dsn = std::env::var("PG_META_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/kokedb".to_string());
        let meta_pool = PgPool::connect(&meta_dsn).await.unwrap();
        sqlx::query("DELETE FROM system.table_sync_state WHERE table_name = 'inc_test';")
            .execute(&meta_pool)
            .await
            .ok();
        sqlx::query("DELETE FROM system.table_arrow_schema WHERE table_name = 'inc_test';")
            .execute(&meta_pool)
            .await
            .ok();
        meta_pool.close().await;

        let pool = PgPool::connect(&dsn).await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS public.inc_test;")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE public.inc_test (id int PRIMARY KEY, val text, \
             updated_at timestamptz NOT NULL DEFAULT now());",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO public.inc_test (id, val, updated_at) VALUES \
             (1,'a', now()), (2,'b', now()), (3,'c', now());",
        )
        .execute(&pool)
        .await
        .unwrap();

        let cache = ResultCache::local(64, 64).await.unwrap();
        let exec = DataSyncExecutor::new(Arc::new(Mutex::new(None)));
        let config = CacheTableTaskConfig::new(
            "kokedb".to_string(),
            dsn.clone(),
            "public.inc_test".to_string(),
            "public.inc_test".to_string(),
        );

        // 1. Full sync → snapshot has 3 rows.
        exec.execute(config.clone(), cache.clone(), None).await.unwrap();
        let (_, path1) = meta
            .get_table_schema("kokedb", "public", "inc_test")
            .await
            .unwrap();
        assert_eq!(count_rows(&path1).await, 3, "full sync should load 3 rows");

        // Confirm sync state recorded a watermark for the next incremental run.
        let state = meta
            .get_table_sync_state("kokedb", "public", "inc_test")
            .await
            .unwrap()
            .expect("sync state persisted");
        assert!(state.last_watermark.is_some());

        // 2. Update id=2 and insert id=4 with strictly newer watermarks.
        sqlx::query(
            "UPDATE public.inc_test SET val='B', updated_at = now() + interval '10 seconds' WHERE id=2;",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO public.inc_test (id, val, updated_at) VALUES \
             (4,'d', now() + interval '10 seconds');",
        )
        .execute(&pool)
        .await
        .unwrap();

        // 3. Second sync → incremental merge.
        exec.execute(config, cache, None).await.unwrap();
        let (_, path2) = meta
            .get_table_schema("kokedb", "public", "inc_test")
            .await
            .unwrap();
        assert_ne!(path1, path2, "a new snapshot dir should be produced");

        let state2 = meta
            .get_table_sync_state("kokedb", "public", "inc_test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state2.sync_mode, "incremental");

        assert_eq!(
            count_rows(&path2).await,
            4,
            "merged snapshot should have 4 rows (3 + 1 insert, update deduped)"
        );
        assert_eq!(val_of(&path2, 2).await.as_deref(), Some("B"), "update applied");
        assert_eq!(val_of(&path2, 4).await.as_deref(), Some("d"), "insert applied");

        pool.close().await;
    }
}
