use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use kokedb_common::cache_policy::{parse_cache_policy, CachePolicy};
use kokedb_common::env::get_env_as;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_task_manager::adaptive;
use kokedb_task_manager::cache_sync_task::{
    execute_catalog_sync_task, refresh_single_table, select_tables_for_policy,
};
use kokedb_task_manager::shadow_validate;
use kokedb_task_manager::task_manager::TaskManager;
use kokedb_task_manager::error::TaskError;
use log::{error, info, warn};
use tokio_cron_scheduler::Job;

use crate::display::{
    CacheJobDisplay, CachePolicyDisplay, RefreshCacheDisplay, TableMetadataDisplay,
};
use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::CreateCatalogOptions;
use crate::utils::match_pattern;

impl CatalogManager {
    pub fn default_catalog(&self) -> CatalogResult<Arc<str>> {
        Ok(self.state()?.default_catalog.clone())
    }

    /// Sets the default catalog for the current session.
    /// An error is returned if the catalog does not exist.
    pub fn set_default_catalog(&self, catalog: impl Into<Arc<str>>) -> CatalogResult<()> {
        let catalog = catalog.into();
        let mut state = self.state()?;
        if !state.catalog_names().contains(&catalog) {
            return Err(CatalogError::NotFound("catalog", catalog.to_string()));
        }
        state.default_catalog = catalog;
        Ok(())
    }

    pub fn list_catalogs(&self, pattern: Option<&str>) -> CatalogResult<Vec<Arc<str>>> {
        Ok(self
            .state()?
            .catalog_names()
            .iter()
            .filter(|name| match_pattern(name.as_ref(), pattern))
            .cloned()
            .collect::<Vec<_>>())
    }

    /// Lists the cache policy of every registered catalog (backs `SHOW CACHE POLICIES`).
    pub async fn list_cache_policies(&self) -> CatalogResult<Vec<CachePolicyDisplay>> {
        // Clone the Arc out of the guard so the mutex is released before awaiting.
        let dynamic_catalog_list = self.state()?.dynamic_catalog_list.clone();
        let rows = dynamic_catalog_list
            .list_cache_policies()
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to list cache policies: {e}"))
            })?;
        Ok(rows
            .into_iter()
            .map(|(catalog, db_type, cache_policy)| CachePolicyDisplay {
                catalog,
                db_type,
                cache_policy,
            })
            .collect())
    }

    /// Enqueues a sync for a single cached table (backs `REFRESH CACHE FROM
    /// TABLE`). The `table` parts are `[catalog, db, table]`, `[db, table]`, or
    /// `[table]`; missing parts fall back to the session's default catalog and
    /// database. Returns the queued task id without waiting for the sync.
    pub async fn refresh_table(&self, table: Vec<String>) -> CatalogResult<RefreshCacheDisplay> {
        let (catalog, source_table) = match table.as_slice() {
            [c, db, t] => (c.clone(), format!("{db}.{t}")),
            [db, t] => (self.default_catalog()?.to_string(), format!("{db}.{t}")),
            [t] => {
                let db = self.default_database()?.head;
                (self.default_catalog()?.to_string(), format!("{db}.{t}"))
            }
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "expected catalog.database.table, got {} name parts",
                    table.len()
                )))
            }
        };

        // Look up the catalog's DSN; fail fast if the catalog is unknown.
        let dynamic_catalog_list = self.state()?.dynamic_catalog_list.clone();
        let catalog_info = dynamic_catalog_list
            .get_catalog(&catalog)
            .await
            .map_err(|e| CatalogError::NotFound("catalog", format!("{catalog}: {e}")))?;

        let task_manager = self.state()?.catalog_task_manager.clone();
        let task_id =
            refresh_single_table(&catalog, &catalog_info.dsn, &source_table, task_manager)
                .await
                .map_err(|e| {
                    CatalogError::External(format!("Failed to enqueue refresh task: {e}"))
                })?;

        info!(
            "Queued manual refresh for {catalog}.{source_table} as task {task_id}"
        );
        Ok(RefreshCacheDisplay {
            table: format!("{catalog}.{source_table}"),
            task_id,
            status: "QUEUED".to_string(),
        })
    }

    /// Enqueues a sync for every table the catalog's policy selects (backs
    /// `REFRESH CACHE FROM CATALOG`). Returns one row per queued table.
    pub async fn refresh_catalog(&self, catalog: &str) -> CatalogResult<Vec<RefreshCacheDisplay>> {
        let meta = self.state()?.dynamic_catalog_list.clone();
        let info = meta
            .get_catalog(catalog)
            .await
            .map_err(|e| CatalogError::NotFound("catalog", format!("{catalog}: {e}")))?;
        let policy_str = meta
            .get_catalog_cache_policy(catalog)
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to read cache policy: {e}")))?;
        let policy = CachePolicy::from_string(&policy_str).map_err(|e| {
            CatalogError::Internal(format!("Failed to parse cache policy: {e:?}"))
        })?;

        let tables = select_tables_for_policy(catalog, &info.dsn, &policy)
            .await
            .map_err(|e| CatalogError::External(format!("Failed to select tables: {e}")))?;

        let task_manager = self.state()?.catalog_task_manager.clone();
        let mut rows = Vec::with_capacity(tables.len());
        for table in tables {
            match refresh_single_table(catalog, &info.dsn, &table, task_manager.clone()).await {
                Ok(task_id) => rows.push(RefreshCacheDisplay {
                    table: format!("{catalog}.{table}"),
                    task_id,
                    status: "QUEUED".to_string(),
                }),
                Err(e) => warn!("Failed to enqueue refresh for {catalog}.{table}: {e}"),
            }
        }
        info!("Queued {} table refreshes for catalog '{catalog}'", rows.len());
        Ok(rows)
    }

    /// Pauses or resumes a table's scheduled refresh
    /// (`PAUSE/RESUME CACHE SCHEDULE FOR TABLE`). Name parts default to the
    /// session catalog/database.
    pub async fn set_table_paused(
        &self,
        table: Vec<String>,
        paused: bool,
    ) -> CatalogResult<()> {
        let (catalog, schema_name, table_name) = match table.as_slice() {
            [c, db, t] => (c.clone(), db.clone(), t.clone()),
            [db, t] => (self.default_catalog()?.to_string(), db.clone(), t.clone()),
            [t] => (
                self.default_catalog()?.to_string(),
                self.default_database()?.head.to_string(),
                t.clone(),
            ),
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "expected catalog.database.table, got {} name parts",
                    table.len()
                )))
            }
        };
        let meta = self.state()?.dynamic_catalog_list.clone();
        meta.set_table_paused(&catalog, &schema_name, &table_name, paused)
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to set paused: {e}")))?;
        info!(
            "{} refresh for {catalog}.{schema_name}.{table_name}",
            if paused { "Paused" } else { "Resumed" }
        );
        Ok(())
    }

    /// Reads a table's inferred sync metadata for `SHOW TABLE METADATA`. The
    /// `table` parts default like [`refresh_table`].
    pub async fn show_table_metadata(
        &self,
        table: Vec<String>,
    ) -> CatalogResult<TableMetadataDisplay> {
        let (catalog, schema_name, table_name) = match table.as_slice() {
            [c, db, t] => (c.clone(), db.clone(), t.clone()),
            [db, t] => (self.default_catalog()?.to_string(), db.clone(), t.clone()),
            [t] => (
                self.default_catalog()?.to_string(),
                self.default_database()?.head.to_string(),
                t.clone(),
            ),
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "expected catalog.database.table, got {} name parts",
                    table.len()
                )))
            }
        };

        let meta = self.state()?.dynamic_catalog_list.clone();
        let m = meta
            .get_table_metadata(&catalog, &schema_name, &table_name)
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to read table metadata: {e}")))?
            .ok_or_else(|| {
                CatalogError::NotFound(
                    "table metadata",
                    format!("{catalog}.{schema_name}.{table_name} (not yet evaluated)"),
                )
            })?;

        let partition_by = meta
            .get_table_partition_column(&catalog, &schema_name, &table_name)
            .await
            .ok()
            .flatten();

        Ok(TableMetadataDisplay {
            table: format!("{catalog}.{schema_name}.{table_name}"),
            inc_mode: m.inc_mode,
            inc_status: m.inc_status,
            inc_tier: m.inc_tier,
            source: m.source,
            confidence: m.confidence,
            watermark_column: m.watermark_column,
            pk_columns: m.pk_columns,
            partition_by,
            refresh_bucket: m.refresh_bucket,
            refresh_interval_sec: m.refresh_interval_sec,
            est_row_count: m.est_row_count,
            est_size_bytes: m.est_size_bytes,
            churn_per_hour: m.churn_per_hour,
            access_per_day: m.access_per_day,
            audit_passes: m.audit_passes,
            divergence_count: m.divergence_count,
            reason: m.reason,
        })
    }

    /// Applies a user override of a table's incremental strategy
    /// (`ALTER TABLE ... SET CACHE POLICY WITH (...)`). Supported keys:
    /// `inc_mode` (full|upsert|append), `watermark_column`, `pk_columns`. A
    /// user-set strategy is trusted (activated without shadow validation) and is
    /// sticky — periodic re-evaluation will not overwrite it.
    pub async fn set_table_cache_policy(
        &self,
        table: Vec<String>,
        options: Vec<(String, String)>,
    ) -> CatalogResult<()> {
        let (catalog, schema_name, table_name) = match table.as_slice() {
            [c, db, t] => (c.clone(), db.clone(), t.clone()),
            [db, t] => (self.default_catalog()?.to_string(), db.clone(), t.clone()),
            [t] => (
                self.default_catalog()?.to_string(),
                self.default_database()?.head.to_string(),
                t.clone(),
            ),
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "expected catalog.database.table, got {} name parts",
                    table.len()
                )))
            }
        };

        let opts: std::collections::HashMap<String, String> = options
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();

        let inc_mode = opts
            .get("inc_mode")
            .map(|s| s.to_lowercase())
            .ok_or_else(|| {
                CatalogError::InvalidArgument("missing required option `inc_mode`".to_string())
            })?;
        let watermark = opts.get("watermark_column").cloned();
        let pk = opts.get("pk_columns").cloned();

        match inc_mode.as_str() {
            "full" => {}
            "upsert" => {
                if watermark.is_none() || pk.is_none() {
                    return Err(CatalogError::InvalidArgument(
                        "inc_mode=upsert requires `watermark_column` and `pk_columns`".to_string(),
                    ));
                }
            }
            "append" => {
                if watermark.is_none() {
                    return Err(CatalogError::InvalidArgument(
                        "inc_mode=append requires `watermark_column`".to_string(),
                    ));
                }
            }
            other => {
                return Err(CatalogError::InvalidArgument(format!(
                    "invalid inc_mode `{other}` (expected full|upsert|append)"
                )))
            }
        }

        let meta = self.state()?.dynamic_catalog_list.clone();
        meta.set_user_inc_policy(&catalog, &schema_name, &table_name, &inc_mode, watermark, pk)
            .await
            .map_err(|e| {
                CatalogError::Internal(format!("Failed to set cache policy: {e}"))
            })?;

        info!("User set cache policy for {catalog}.{schema_name}.{table_name}: inc_mode={inc_mode}");
        Ok(())
    }

    /// Returns recent table-sync runs (`SHOW CACHE JOBS`), optionally scoped to a
    /// `FROM TABLE catalog.db.table` or `FROM CATALOG name` filter.
    pub async fn show_cache_jobs(
        &self,
        table: Option<Vec<String>>,
        catalog: Option<String>,
    ) -> CatalogResult<Vec<CacheJobDisplay>> {
        // Resolve the filter: a table scope pins all three parts; a catalog scope
        // pins just the catalog; otherwise everything (newest first).
        let (cat, schema, tbl) = match (&table, &catalog) {
            (Some(parts), _) => match parts.as_slice() {
                [c, db, t] => (Some(c.clone()), Some(db.clone()), Some(t.clone())),
                [db, t] => (
                    Some(self.default_catalog()?.to_string()),
                    Some(db.clone()),
                    Some(t.clone()),
                ),
                [t] => (
                    Some(self.default_catalog()?.to_string()),
                    Some(self.default_database()?.head.to_string()),
                    Some(t.clone()),
                ),
                _ => {
                    return Err(CatalogError::InvalidArgument(
                        "expected catalog.database.table".to_string(),
                    ))
                }
            },
            (None, Some(c)) => (Some(c.clone()), None, None),
            (None, None) => (None, None, None),
        };

        let limit = get_env_as("KOKEDB_SHOW_JOBS_LIMIT", 50i64);
        let meta = self.state()?.dynamic_catalog_list.clone();
        let jobs = meta
            .get_cache_jobs(cat.as_deref(), schema.as_deref(), tbl.as_deref(), limit)
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to read cache jobs: {e}")))?;

        let fmt = |t: chrono::DateTime<chrono::Utc>| t.format("%Y-%m-%d %H:%M:%S").to_string();
        Ok(jobs
            .into_iter()
            .map(|j| CacheJobDisplay {
                id: j.id,
                catalog: j.catalog,
                schema: j.schema_name,
                table: j.table_name,
                status: j.status,
                started_at: fmt(j.started_at),
                ended_at: j.ended_at.map(fmt),
                duration_ms: j
                    .ended_at
                    .map(|e| (e - j.started_at).num_milliseconds()),
                error: j.error_message,
            })
            .collect())
    }

    /// Changes a catalog's cache policy (`ALTER CATALOG ... SET CACHE POLICY`)
    /// and re-applies it so the new selection is picked up.
    pub async fn set_catalog_cache_policy(
        &self,
        catalog: &str,
        options: Vec<(String, String)>,
    ) -> CatalogResult<()> {
        let policy = parse_cache_policy(options)
            .map_err(|e| CatalogError::InvalidArgument(format!("invalid cache policy: {e:?}")))?;

        let meta = self.state()?.dynamic_catalog_list.clone();
        let updated = meta
            .update_catalog_cache_policy(catalog, &policy.to_string())
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to update cache policy: {e}")))?;
        if updated == 0 {
            return Err(CatalogError::NotFound("catalog", catalog.to_string()));
        }

        // Re-apply: re-select tables, recompute cadence/inference (best-effort).
        if let Ok(info) = meta.get_catalog(catalog).await {
            if let Err(e) = adaptive::reevaluate_catalog(catalog, &info.dsn, &policy).await {
                warn!("Re-apply after ALTER CATALOG '{}' failed: {}", catalog, e);
            }
        }
        info!("Set cache policy for catalog '{catalog}': {}", policy.to_string());
        Ok(())
    }

    /// Unloads a single table from the cache: clears its cache metadata (so it
    /// resolves to the live remote source again), invalidates affected query
    /// results, and removes the on-disk snapshot. Best-effort.
    async fn evict_one_table(
        &self,
        meta: &Arc<PostgreSQLMetaCatalogProviderList>,
        task_manager: &Arc<TaskManager>,
        catalog: &str,
        schema: &str,
        table: &str,
    ) {
        match meta.evict_table_cache(catalog, schema, table).await {
            Ok((path, keys)) => {
                task_manager.invalidate_cache_keys(&keys).await;
                if let Some(dir) = path
                    .as_deref()
                    .and_then(|p| std::path::Path::new(p).parent())
                {
                    if let Err(e) = tokio::fs::remove_dir_all(dir).await {
                        warn!("Failed to remove snapshot dir {}: {}", dir.display(), e);
                    }
                }
                info!("Evicted {catalog}.{schema}.{table} from cache");
            }
            Err(e) => warn!("Failed to evict {catalog}.{schema}.{table}: {e}"),
        }
    }

    /// Sets a per-database cache override (`ALTER DATABASE ... SET CACHE POLICY`).
    /// `mode='none'` disables caching for the database (and drops its table
    /// schedules); `mode='all'` caches every table in it. Re-applies afterwards.
    pub async fn set_database_cache_policy(
        &self,
        database: Vec<String>,
        options: Vec<(String, String)>,
    ) -> CatalogResult<()> {
        let (catalog, schema) = match database.as_slice() {
            [c, db] => (c.clone(), db.clone()),
            [db] => (self.default_catalog()?.to_string(), db.clone()),
            _ => {
                return Err(CatalogError::InvalidArgument(format!(
                    "expected catalog.database, got {} name parts",
                    database.len()
                )))
            }
        };

        let opts: std::collections::HashMap<String, String> = options
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
            .collect();
        let mode = opts.get("mode").cloned().ok_or_else(|| {
            CatalogError::InvalidArgument("missing required option `mode`".to_string())
        })?;
        if mode != "all" && mode != "none" {
            return Err(CatalogError::InvalidArgument(format!(
                "invalid mode `{mode}` (expected all|none)"
            )));
        }

        let meta = self.state()?.dynamic_catalog_list.clone();
        meta.upsert_database_policy(&catalog, &schema, &mode)
            .await
            .map_err(|e| CatalogError::Internal(format!("Failed to set database policy: {e}")))?;
        if mode == "none" {
            // Unload the database's cached tables: drop their snapshots + cache
            // metadata so queries fall back to the live remote source, and reclaim
            // the on-disk snapshots. Then stop scheduling any refreshes.
            let task_manager = self.state()?.catalog_task_manager.clone();
            let cached = meta
                .list_cached_tables_in_schema(&catalog, &schema)
                .await
                .unwrap_or_default();
            for t in &cached {
                self.evict_one_table(&meta, &task_manager, &catalog, &schema, t).await;
            }
            let _ = meta.delete_schema_table_policies(&catalog, &schema).await;
        }

        // Re-apply the (now inheritance-aware) selection.
        if let (Ok(info), Ok(policy_str)) = (
            meta.get_catalog(&catalog).await,
            meta.get_catalog_cache_policy(&catalog).await,
        ) {
            if let Ok(policy) = CachePolicy::from_string(&policy_str) {
                if let Err(e) = adaptive::reevaluate_catalog(&catalog, &info.dsn, &policy).await {
                    warn!("Re-apply after ALTER DATABASE '{}.{}' failed: {}", catalog, schema, e);
                }
            }
        }
        info!("Set database policy for '{catalog}.{schema}': mode={mode}");
        Ok(())
    }

    pub fn create_catalog(
        &self,
        catalog: impl Into<Arc<str>>,
        options: CreateCatalogOptions,
    ) -> CatalogResult<Arc<str>> {
        //TODO: check dsn is valid.
        let catalog = catalog.into();
        let dsn = options.dsn;
        let comment = options.comment;
        let properties = options.properties;
        let db_type = options.db_type.to_string();

        let ret = self
            .state()?
            .dynamic_catalog_list
            .create_catalog(&catalog, &dsn, &db_type, comment, properties);

        if ret.is_err() {
            return Err(CatalogError::External(format!(
                "Failed to save catalog to postgresql with error: {:?}",
                ret.err()
            )));
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let dsn = dsn.clone();
                let catalog = catalog.clone();
                self.create_catalog_scheduler_job(&dsn, &catalog)
                    .await
                    .map_err(|x| {
                        TaskError::Internal(format!(
                            "Failed to added scheduler job with error:{:?}",
                            x
                        ))
                    })?;

                Ok(())
            })
        })
        .map_err(|e: TaskError| {
            CatalogError::External(format!(
                "Failed to create cache table task withe error:{}",
                e
            ))
        })?;

        Ok(catalog)
    }

    pub async fn create_catalog_scheduler_job(
        &self,
        dsn: &str,
        catalog: &str,
    ) -> CatalogResult<uuid::Uuid> {
        let schedule_interval_min: u32 = get_env_as("KOKEDB_CACHE_JOB_INTERVAL", 30u32); // minutes

        let state = self
            .state()
            .map_err(|e| CatalogError::Internal(format!("Failed to get state: {}", e)))?;

        let catalog_task_manager = state.catalog_task_manager.clone();
        let cache_policy = state
            .dynamic_catalog_list
            .get_catalog_cache_policy(catalog)
            .await
            .map_err(|e| {
                CatalogError::Internal(format!(
                    "Failed to get catalog cache policy with error: {}",
                    e
                ))
            })?;
        let cache_policy = CachePolicy::from_string(&cache_policy).map_err(|x| {
            CatalogError::Internal(format!(
                "Failed to parse cache_policy: {} with error: {:?}",
                &cache_policy, x
            ))
        })?;

        if let Err(e) = execute_catalog_sync_task(
            &dsn,
            &catalog,
            catalog_task_manager.clone(),
            cache_policy.clone(),
        )
        .await
        {
            error!(
                "Catalog sync task failed for catalog '{}' with DSN '{}': {}",
                &catalog, &dsn, e
            );
        } else {
            info!(
                "Catalog first sync task successed for catalog '{}' with DSN '{}'",
                &catalog, &dsn
            )
        }

        let adaptive = get_env_as("KOKEDB_ADAPTIVE_REFRESH", true);
        let job_dsn = dsn.to_string();
        let job_catalog = catalog.to_string();

        let job = if adaptive {
            // Adaptive mode: seed per-table policies now, then run a per-minute
            // tick that enqueues only tables whose cadence has elapsed, and
            // re-evaluate cadences (and discover new tables) periodically.
            if let Err(e) =
                adaptive::reevaluate_catalog(&catalog, &dsn, &cache_policy).await
            {
                warn!("Initial adaptive reeval failed for '{}': {}", catalog, e);
            }
            let tick_min: u32 = get_env_as("KOKEDB_TICK_MIN", 1u32).max(1);
            let reeval_min: u32 =
                get_env_as("KOKEDB_REEVALUATE_INTERVAL_MIN", 60u32).max(tick_min);
            let cron_expr = format!("0 */{} * * * *", tick_min);
            let ticks = Arc::new(AtomicU64::new(0));

            Job::new_async(cron_expr.as_str(), move |_uuid, _l| {
                let dsn = job_dsn.clone();
                let catalog = job_catalog.clone();
                let task_manager = catalog_task_manager.clone();
                let cache_policy = cache_policy.clone();
                let ticks = ticks.clone();
                Box::pin(async move {
                    // Enqueue tables that are due this tick.
                    if let Err(e) =
                        adaptive::tick_refresh_due(&catalog, &dsn, task_manager).await
                    {
                        error!("Adaptive tick failed for '{}': {}", catalog, e);
                    }
                    // Shadow-validate audit-due tables (low budget per tick).
                    if get_env_as("KOKEDB_SHADOW_VALIDATE", true) {
                        if let Err(e) = shadow_validate::sweep_audits(&catalog, &dsn).await {
                            error!("Shadow audit sweep failed for '{}': {}", catalog, e);
                        }
                    }
                    // Re-evaluate cadences every `reeval_min` minutes.
                    let elapsed_min =
                        (ticks.fetch_add(1, AtomicOrdering::Relaxed) + 1) * tick_min as u64;
                    if elapsed_min % reeval_min as u64 == 0 {
                        if let Err(e) =
                            adaptive::reevaluate_catalog(&catalog, &dsn, &cache_policy).await
                        {
                            error!("Adaptive reeval failed for '{}': {}", catalog, e);
                        }
                    }
                })
            })
        } else {
            // Legacy mode: a single periodic full-catalog sync.
            let cron_expr = format!("0 */{} * * * *", schedule_interval_min);
            Job::new_async(cron_expr.as_str(), move |_uuid, _l| {
                let dsn = job_dsn.clone();
                let catalog = job_catalog.clone();
                let catalog_task_manager = catalog_task_manager.clone();
                let cache_policy = cache_policy.clone();
                Box::pin(async move {
                    if let Err(e) = execute_catalog_sync_task(
                        &dsn,
                        &catalog,
                        catalog_task_manager,
                        cache_policy,
                    )
                    .await
                    {
                        error!(
                            "Catalog sync task failed for catalog '{}' with DSN '{}': {}",
                            &catalog, &dsn, e
                        );
                    }
                })
            })
        }
        .map_err(|e| {
            CatalogError::External(format!(
                "Failed to create scheduler job for catalog '{}': {}",
                catalog, e
            ))
        })?;

        let job_uuid = state.catalog_task_scheduler.add(job).await.map_err(|e| {
            CatalogError::External(format!(
                "Failed to add scheduler job for catalog '{}': {}",
                catalog, e
            ))
        })?;

        // Remember the job id so `DROP CATALOG` can stop it later.
        if let Ok(mut jobs) = self.scheduler_jobs.lock() {
            jobs.insert(catalog.to_string(), job_uuid);
        }

        info!(
            "Successfully added scheduled sync job for catalog '{}' (UUID: {})",
            catalog, job_uuid
        );

        Ok(job_uuid)
    }

    /// Drops a catalog: stops its background sync job, removes its registration
    /// from the meta store, and evicts it from the provider cache.
    pub async fn drop_catalog(&self, catalog: &str, if_exists: bool) -> CatalogResult<()> {
        // Stop the scheduled sync job, if we have its id.
        let job_id = self
            .scheduler_jobs
            .lock()
            .ok()
            .and_then(|mut jobs| jobs.remove(catalog));
        if let Some(job_id) = job_id {
            let scheduler = self.state()?.catalog_task_scheduler.clone();
            if let Err(e) = scheduler.remove(&job_id).await {
                error!(
                    "Failed to remove scheduler job for catalog '{}': {}",
                    catalog, e
                );
            }
        }

        // Remove the registration (and provider cache entry) from the meta store.
        let dynamic_catalog_list = self.state()?.dynamic_catalog_list.clone();
        let deleted = dynamic_catalog_list
            .delete_catalog(catalog)
            .await
            .map_err(|e| {
                CatalogError::External(format!("Failed to drop catalog '{}': {}", catalog, e))
            })?;

        if !deleted && !if_exists {
            return Err(CatalogError::NotFound("catalog", catalog.to_string()));
        }

        info!("Dropped catalog '{}'", catalog);
        Ok(())
    }
}
