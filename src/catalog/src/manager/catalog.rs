use std::sync::Arc;

use kokedb_common::cache_policy::CachePolicy;
use kokedb_common::env::get_env_as;
use kokedb_task_manager::cache_sync_task::execute_catalog_sync_task;
use kokedb_task_manager::error::TaskError;
use log::{error, info};
use tokio_cron_scheduler::Job;

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
        let schedule_interval_min: u32 = get_env_as("KOKEDB_CACHE_JOB_INTERVAL", 60u32); // 1 hour

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

        let job_dsn = dsn.to_string();
        let job_catalog = catalog.to_string();
        let cron_expr = format!("0 */{} * * * *", schedule_interval_min);

        let job = Job::new_async(cron_expr, move |_uuid, _l| {
            let dsn = job_dsn.clone();
            let catalog = job_catalog.clone();
            let catalog_task_manager = catalog_task_manager.clone();
            let cache_policy = cache_policy.clone();

            Box::pin(async move {
                if let Err(e) =
                    execute_catalog_sync_task(&dsn, &catalog, catalog_task_manager, cache_policy)
                        .await
                {
                    error!(
                        "Catalog sync task failed for catalog '{}' with DSN '{}': {}",
                        &catalog, &dsn, e
                    );
                }
            })
        })
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

        info!(
            "Successfully added scheduled sync job for catalog '{}' (UUID: {})",
            catalog, job_uuid
        );

        Ok(job_uuid)
    }
}
