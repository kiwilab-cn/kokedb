use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::catalog::CatalogProviderList;
use kokedb_common_datafusion::extension::SessionExtension;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_task_manager::task_manager::TaskManager;
use log::info;
use tokio_cron_scheduler::JobScheduler;

use crate::datafusion_catalog_adapter::DataFusionCatalogAdapter;
use crate::error::{CatalogError, CatalogResult};
use crate::provider::{CatalogProvider, Namespace};
use crate::temp_view::TemporaryViewManager;

pub mod catalog;
pub mod database;
pub mod function;
pub mod table;
pub mod view;

/// A manager for all catalogs registered with the session.
/// Each catalog has a name and a corresponding [`CatalogProvider`] instance.
pub struct CatalogManager {
    state: Arc<Mutex<CatalogManagerState>>,
    pub(super) temporary_views: TemporaryViewManager,
}

pub(super) struct CatalogManagerState {
    pub(super) catalogs: HashMap<Arc<str>, Arc<dyn CatalogProvider>>,
    pub(super) dynamic_catalog_list: Arc<PostgreSQLMetaCatalogProviderList>,
    pub(super) catalog_task_manager: Arc<TaskManager>,
    pub(super) catalog_task_scheduler: Arc<JobScheduler>,
    pub(super) default_catalog: Arc<str>,
    pub(super) default_database: Namespace,
    pub(super) global_temporary_database: Namespace,
}

pub struct CatalogManagerOptions {
    pub catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
    pub dynamic_catalog_list: Arc<PostgreSQLMetaCatalogProviderList>,
    pub catalog_task_manager: Arc<TaskManager>,
    pub catalog_task_scheduler: Arc<JobScheduler>,
    pub default_catalog: String,
    pub default_database: Vec<String>,
    pub global_temporary_database: Vec<String>,
}

impl CatalogManager {
    pub fn new(options: CatalogManagerOptions) -> CatalogResult<Self> {
        let catalogs = options
            .catalogs
            .into_iter()
            .map(|(name, provider)| (name.into(), provider))
            .collect::<HashMap<_, _>>();
        if !catalogs.contains_key(options.default_catalog.as_str()) {
            return Err(CatalogError::NotFound(
                "catalog",
                options.default_catalog.clone(),
            ));
        }
        // We do not validate the existence of the default database here,
        // since it requires an async method call to the catalog provider.
        // Even if the default database is valid now, it may be dropped externally later.
        let state = CatalogManagerState {
            catalogs,
            default_catalog: options.default_catalog.into(),
            default_database: options.default_database.try_into()?,
            global_temporary_database: options.global_temporary_database.try_into()?,
            dynamic_catalog_list: options.dynamic_catalog_list,
            catalog_task_manager: options.catalog_task_manager.clone(),
            catalog_task_scheduler: options.catalog_task_scheduler.clone(),
        };
        Ok(CatalogManager {
            state: Arc::new(Mutex::new(state)),
            temporary_views: Default::default(),
        })
    }

    pub async fn init_catalog_job(&self) -> CatalogResult<()> {
        let catalogs = {
            self.state()?
                .dynamic_catalog_list
                .load_catalog_info()
                .await
                .map_err(|x| {
                    CatalogError::Internal(format!("Failed to get catalog list with error: {}", x))
                })?
        };

        for to_init_catalog in catalogs {
            let catalog = to_init_catalog.name.as_str();
            let dsn = to_init_catalog.dsn.as_str();
            self.create_catalog_scheduler_job(dsn, catalog)
                .await
                .map_err(|x| {
                    CatalogError::Internal(format!(
                        "Failed to create catalog: {} and dsn: {} scheduler job with error:{}",
                        catalog, dsn, x
                    ))
                })?;
            info!("Success added catalog:{} scheduler job.", catalog);
        }

        Ok(())
    }

    pub(super) fn state(&self) -> CatalogResult<MutexGuard<'_, CatalogManagerState>> {
        self.state
            .lock()
            .map_err(|e| CatalogError::Internal(e.to_string()))
    }

    pub(super) fn resolve_default_database(
        &self,
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        let catalog = state.default_catalog.clone();
        let database = state.default_database.clone();
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_database<T: AsRef<str>>(
        &self,
        database: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace)> {
        let state = self.state()?;
        let (catalog, database) = state.resolve_database_reference(database)?;
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_optional_database<T: AsRef<str>>(
        &self,
        database: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Option<Namespace>)> {
        let state = self.state()?;
        let (catalog, database) = state.resolve_optional_database_reference(database)?;
        Ok((state.get_catalog(&catalog)?, database))
    }

    pub(super) fn resolve_object<T: AsRef<str>>(
        &self,
        object: &[T],
    ) -> CatalogResult<(Arc<dyn CatalogProvider>, Namespace, Arc<str>)> {
        let state = self.state()?;
        let (catalog, database, table) = state.resolve_object_reference(object)?;
        Ok((state.get_catalog(&catalog)?, database, table))
    }
}

impl CatalogManagerState {
    pub fn resolve_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty database reference".to_string(),
            )),
            [head, tail @ ..] if self.catalog_names().contains(&Arc::from(head.as_ref())) => {
                let catalog = head.as_ref().into();
                let database = tail.try_into()?;
                Ok((catalog, database))
            }
            x => {
                let catalog = self.default_catalog.clone();
                let database = x.try_into()?;
                Ok((catalog, database))
            }
        }
    }

    pub fn resolve_optional_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Option<Namespace>)> {
        match reference {
            [] => {
                let catalog = self.default_catalog.clone();
                Ok((catalog, None))
            }
            [name] if self.catalog_names().contains(&Arc::from(name.as_ref())) => {
                let catalog = name.as_ref().into();
                Ok((catalog, None))
            }
            x => {
                let catalog = self.default_catalog.clone();
                let database = x.try_into()?;
                Ok((catalog, Some(database)))
            }
        }
    }

    pub fn resolve_object_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty object reference".to_string(),
            )),
            [name] => {
                let table = name.as_ref().into();
                let catalog = self.default_catalog.clone();
                let database = self.default_database.clone();
                Ok((catalog, database, table))
            }
            [x @ .., last] => {
                let table = last.as_ref().into();
                let (catalog, database) = self.resolve_database_reference(x)?;
                Ok((catalog, database, table))
            }
        }
    }

    pub fn is_global_temporary_view_database<T: AsRef<str>>(&self, reference: &[T]) -> bool {
        match reference {
            [] => false,
            x => self.global_temporary_database == x,
        }
    }

    pub fn _list_catalog(&self) -> HashMap<Arc<str>, Arc<dyn CatalogProvider>> {
        let mut catalogs: HashMap<String, Arc<dyn CatalogProvider>> = self
            .catalogs
            .iter()
            .map(|(k, v)| (k.to_string(), Arc::clone(v)))
            .collect();

        let catalog_list = self.dynamic_catalog_list.clone();
        for catalog_name in catalog_list.catalog_names() {
            if let Some(catalog) = catalog_list.catalog(&catalog_name) {
                let catalog_adapter = DataFusionCatalogAdapter::new(catalog, catalog_name.clone());
                catalogs.insert(catalog_name.clone(), Arc::new(catalog_adapter));
            }
        }

        catalogs
            .into_iter()
            .map(|(k, v)| (Arc::from(k), v))
            .collect()
    }

    pub fn catalog_names(&self) -> Vec<Arc<str>> {
        let mut catalog_names = self
            .catalogs
            .iter()
            .map(|(k, _)| k.to_string())
            .collect::<Vec<String>>();

        let dynamic_catalog_names = self.dynamic_catalog_list.catalog_names();

        catalog_names.extend(dynamic_catalog_names);
        catalog_names
            .iter()
            .map(|x| Arc::from(x.as_str()))
            .collect::<Vec<Arc<str>>>()
    }

    pub fn get_catalog(&self, catalog_name: &str) -> CatalogResult<Arc<dyn CatalogProvider>> {
        let catalog_list = self.dynamic_catalog_list.clone();

        if let Some(catalog) = catalog_list.catalog(catalog_name) {
            let catalog_adapter = DataFusionCatalogAdapter::new(catalog, catalog_name.to_string());
            return Ok(Arc::new(catalog_adapter));
        }

        self.catalogs
            .get(catalog_name)
            .map(Arc::clone)
            .ok_or_else(|| CatalogError::NotFound("catalog", catalog_name.to_string()))
    }
}

impl SessionExtension for CatalogManager {
    fn name() -> &'static str {
        "catalog manager"
    }
}
