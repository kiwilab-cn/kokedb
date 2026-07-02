use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::catalog::CatalogProviderList;
use kokedb_cache::result_cache::ResultCache;
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
pub mod user;
pub mod view;

/// A manager for all catalogs registered with the session.
/// Each catalog has a name and a corresponding [`CatalogProvider`] instance.
pub struct CatalogManager {
    state: Arc<Mutex<CatalogManagerState>>,
    pub result_cache: ResultCache,
    pub(super) temporary_views: TemporaryViewManager,
    /// Maps a catalog name to its scheduled sync job id, so `DROP CATALOG` can
    /// stop the background job it started.
    pub(super) scheduler_jobs: Arc<Mutex<HashMap<String, uuid::Uuid>>>,
}

pub(super) struct CatalogManagerState {
    pub(super) catalogs: HashMap<Arc<str>, Arc<dyn CatalogProvider>>,
    pub dynamic_catalog_list: Arc<PostgreSQLMetaCatalogProviderList>,
    pub(super) catalog_task_manager: Arc<TaskManager>,
    pub(super) catalog_task_scheduler: Arc<JobScheduler>,
    pub(super) default_catalog: Arc<str>,
    pub(super) default_database: Namespace,
    pub(super) global_temporary_database: Namespace,
    /// Per-session catalog allow-list. `None` means unrestricted (superuser or
    /// authentication disabled); `Some(set)` scopes the session to those
    /// dynamic catalogs only. The static default catalog is always visible.
    pub(super) allowed_catalogs: Option<Arc<std::collections::HashSet<String>>>,
}

pub struct CatalogManagerOptions {
    pub catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
    pub dynamic_catalog_list: Arc<PostgreSQLMetaCatalogProviderList>,
    pub catalog_task_manager: Arc<TaskManager>,
    pub catalog_task_scheduler: Arc<JobScheduler>,
    /// Shared map of catalog name -> scheduled sync job id. Shared across all
    /// per-connection managers since the scheduler itself is a singleton.
    pub scheduler_jobs: Arc<Mutex<HashMap<String, uuid::Uuid>>>,
    pub result_cache: ResultCache,
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
            catalog_task_manager: options.catalog_task_manager.clone(),
            dynamic_catalog_list: options.dynamic_catalog_list,
            catalog_task_scheduler: options.catalog_task_scheduler.clone(),
            allowed_catalogs: None,
        };
        Ok(CatalogManager {
            state: Arc::new(Mutex::new(state)),
            temporary_views: Default::default(),
            result_cache: options.result_cache.clone(),
            scheduler_jobs: options.scheduler_jobs,
        })
    }

    /// Scopes this session to a catalog allow-list (set after authentication).
    /// `None` leaves the session unrestricted (superuser / auth disabled).
    pub fn set_acl(&self, allowed_catalogs: Option<Arc<std::collections::HashSet<String>>>) {
        if let Ok(mut state) = self.state.lock() {
            state.allowed_catalogs = allowed_catalogs;
        }
    }

    /// Whether this session has a catalog allow-list applied. Restricted
    /// sessions must bypass the shared, user-agnostic result cache.
    pub fn is_restricted(&self) -> bool {
        self.state
            .lock()
            .map(|s| s.allowed_catalogs.is_some())
            .unwrap_or(false)
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

    /// Whether the session's ACL permits access to a dynamic catalog. Static
    /// catalogs (e.g. the default catalog) are always allowed.
    pub(super) fn acl_allows(&self, name: &str) -> bool {
        match &self.allowed_catalogs {
            None => true,
            Some(set) => set.contains(name),
        }
    }

    pub fn catalog_names(&self) -> Vec<Arc<str>> {
        // Static catalogs first, then ACL-filtered dynamic ones, deduplicated
        // (a name registered both ways must not appear twice) and without the
        // Arc<str> -> String -> Arc<str> round-trip this used to do.
        let mut seen: std::collections::HashSet<Arc<str>> = std::collections::HashSet::new();
        let mut names: Vec<Arc<str>> = Vec::new();
        for name in self.catalogs.keys() {
            if seen.insert(Arc::clone(name)) {
                names.push(Arc::clone(name));
            }
        }
        for name in self.dynamic_catalog_list.catalog_names() {
            if !self.acl_allows(&name) {
                continue;
            }
            let name: Arc<str> = Arc::from(name.as_str());
            if seen.insert(Arc::clone(&name)) {
                names.push(name);
            }
        }
        names
    }

    pub fn get_catalog(&self, catalog_name: &str) -> CatalogResult<Arc<dyn CatalogProvider>> {
        let catalog_list = self.dynamic_catalog_list.clone();

        if self.acl_allows(catalog_name) {
            if let Some(catalog) = catalog_list.catalog(catalog_name) {
                let catalog_adapter = DataFusionCatalogAdapter::new(
                    catalog,
                    catalog_name.to_string(),
                    self.dynamic_catalog_list.clone(),
                );
                return Ok(Arc::new(catalog_adapter));
            }
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
