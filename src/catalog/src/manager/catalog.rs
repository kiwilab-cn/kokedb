use std::sync::Arc;

use kokedb_task_manager::error::TaskError;
use kokedb_task_manager::postgres_table_analyzer::get_postgres_top_tables;
use kokedb_task_manager::task::CacheTableTaskConfig;
use log::warn;

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

        let ret = self
            .state()?
            .dynamic_catalog_list
            .create_catalog(&catalog, &dsn);

        if ret.is_err() {
            return Err(CatalogError::External(format!(
                "Failed to save catalog to postgresql with error: {:?}",
                ret.err()
            )));
        }

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let dsn = dsn.clone();
                let postgres_topk_table = get_postgres_top_tables(&dsn, 10).await?;

                for table in postgres_topk_table {
                    let config = CacheTableTaskConfig::new(
                        catalog.to_string(),
                        dsn.clone(),
                        table.clone(),
                        table.clone(),
                    );

                    let state = self
                        .state()
                        .map_err(|e| TaskError::Internal(e.to_string()))?;

                    let ret = state.catalog_task_manager.add_task(config).await;
                    if ret.is_err() {
                        warn!(
                            "Failed to add cache dsn: {} table: {} task with error: {:?}",
                            &dsn,
                            &table,
                            ret.err()
                        );
                    }
                }
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
}
