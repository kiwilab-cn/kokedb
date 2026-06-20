use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use kokedb_common::cache_policy::CachePolicy;
use kokedb_common::env::get_env_as;
use kokedb_task_manager::adaptive;
use kokedb_task_manager::cache_sync_task::{execute_catalog_sync_task, refresh_single_table};
use kokedb_task_manager::shadow_validate;
use kokedb_task_manager::error::TaskError;
use log::{error, info, warn};
use tokio_cron_scheduler::Job;

use crate::display::{CachePolicyDisplay, RefreshCacheDisplay};
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
