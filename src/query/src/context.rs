use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::{
    execution::{runtime_env::RuntimeEnv, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::plan_datafusion_err;
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_catalog::{
    manager::{CatalogManager, CatalogManagerOptions},
    provider::CatalogProvider,
};
use kokedb_common_datafusion::extension::SessionExtensionAccessor;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_task_manager::task_manager::TaskManager;
use tokio_cron_scheduler::JobScheduler;

use crate::mem_catalog::MemoryCatalogProvider;

const DEFAULT_CATALOG: &str = "kokedb";

/// Process-wide services shared by every client connection: the meta store, the
/// background sync task manager, the cron scheduler, and the shared sync-job
/// registry. These are created once; each connection then gets its own
/// lightweight [`SessionContext`] (and its own [`CatalogManager`] default
/// catalog/database) that references these shared handles.
#[derive(Clone)]
pub struct SharedServices {
    runtime: Arc<RuntimeEnv>,
    catalog_list: Arc<PostgreSQLMetaCatalogProviderList>,
    task_manager: Arc<TaskManager>,
    task_scheduler: Arc<JobScheduler>,
    scheduler_jobs: Arc<Mutex<HashMap<String, uuid::Uuid>>>,
    result_cache: LruResultCache,
}

/// Initializes the process-wide services exactly once: connects to the meta
/// store, creates the schema, starts the task manager + scheduler, and registers
/// the periodic catalog-sync jobs.
pub async fn init_shared_services(
    result_cache: LruResultCache,
) -> Result<SharedServices, Box<dyn std::error::Error>> {
    let runtime = Arc::new(RuntimeEnv::default());

    let catalog_list = Arc::new(PostgreSQLMetaCatalogProviderList::new().await?);
    catalog_list.init_db().await?;

    let task_manager = Arc::new(TaskManager::new(result_cache.clone()).await?);
    let task_scheduler = Arc::new(JobScheduler::new().await?);
    task_scheduler.start().await?;

    let shared = SharedServices {
        runtime,
        catalog_list,
        task_manager,
        task_scheduler,
        scheduler_jobs: Arc::new(Mutex::new(HashMap::new())),
        result_cache,
    };

    // Register the periodic per-catalog sync jobs once, using a throwaway
    // manager. The jobs land in the shared scheduler + shared job registry, so
    // per-connection managers see them too.
    let bootstrap = build_catalog_manager(&shared)?;
    bootstrap
        .init_catalog_job()
        .await
        .map_err(|e| plan_datafusion_err!("Failed to init catalog jobs: {e}"))?;

    Ok(shared)
}

/// Builds a per-connection [`SessionContext`]. It has its own [`CatalogManager`]
/// (hence its own default catalog/database for `USE`), but shares the meta
/// store, task manager, scheduler, job registry, and result cache.
pub fn create_session_context(
    shared: &SharedServices,
) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let catalog_manager = build_catalog_manager(shared)?;

    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(false)
        .with_information_schema(false)
        .with_extension(Arc::new(catalog_manager));

    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(shared.runtime.clone())
        .with_default_features()
        .build();
    Ok(SessionContext::new_with_state(state_builder))
}

/// Sets the connection's default catalog (`USE <name>`). Returns an error if
/// the catalog does not exist. Because each connection owns its own
/// [`CatalogManager`], this only affects the calling connection.
pub fn use_database(ctx: &SessionContext, name: &str) -> Result<(), String> {
    let manager = ctx
        .extension::<CatalogManager>()
        .map_err(|e| format!("catalog manager unavailable: {e}"))?;
    manager.set_default_catalog(name).map_err(|e| e.to_string())
}

fn build_catalog_manager(
    shared: &SharedServices,
) -> Result<CatalogManager, Box<dyn std::error::Error>> {
    let default_catalog = DEFAULT_CATALOG.to_string();
    let default_database = vec!["public".to_string()];
    let default_global_database = vec!["global".to_string()];

    let provider = MemoryCatalogProvider::new(
        default_catalog.clone(),
        default_database.clone().try_into()?,
        Some("default memory database".to_string()),
    );
    let mut catalogs: HashMap<String, Arc<dyn CatalogProvider>> = HashMap::new();
    catalogs.insert(default_catalog.clone(), Arc::new(provider));

    let options = CatalogManagerOptions {
        catalogs,
        default_catalog,
        default_database,
        global_temporary_database: default_global_database,
        dynamic_catalog_list: shared.catalog_list.clone(),
        catalog_task_manager: shared.task_manager.clone(),
        catalog_task_scheduler: shared.task_scheduler.clone(),
        scheduler_jobs: shared.scheduler_jobs.clone(),
        result_cache: shared.result_cache.clone(),
    };

    CatalogManager::new(options)
        .map_err(|e| plan_datafusion_err!("Failed to create catalog manager: {e}").into())
}
