use std::{collections::HashMap, sync::Arc};

use datafusion::{
    execution::{runtime_env::RuntimeEnv, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::plan_datafusion_err;
use kokedb_catalog::{
    manager::{CatalogManager, CatalogManagerOptions},
    provider::CatalogProvider,
};
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_task_manager::task_manager::TaskManager;
use tokio_cron_scheduler::JobScheduler;

use crate::mem_catalog::MemoryCatalogProvider;

pub async fn create_session_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let runtime = Arc::new(RuntimeEnv::default());
    let default_catalog = "kokedb".to_string();
    let default_database = vec!["public".to_string()];
    let default_global_database = vec!["global".to_string()];

    let provider = MemoryCatalogProvider::new(
        default_catalog.clone(),
        default_database.clone().try_into()?,
        Some("default memory database".to_string()),
    );
    let mut catalogs: HashMap<String, Arc<dyn CatalogProvider>> = HashMap::new();
    catalogs.insert(default_catalog.clone(), Arc::new(provider));

    let catalog_list = Arc::new(PostgreSQLMetaCatalogProviderList::new().await.unwrap());
    catalog_list.init_db().await?;

    let task_manager = TaskManager::new().await?;
    let task_scheduler = JobScheduler::new().await?;
    task_scheduler.start().await?;

    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: default_catalog.clone(),
        default_database: default_database.clone(),
        global_temporary_database: default_global_database,
        dynamic_catalog_list: catalog_list.clone(),
        catalog_task_manager: Arc::new(task_manager),
        catalog_task_scheduler: Arc::new(task_scheduler),
    };

    let catalog_manager = CatalogManager::new(options)
        .map_err(|e| plan_datafusion_err!("Failed to create catalog manager: {e}"))?;
    catalog_manager
        .init_catalog_job()
        .await
        .map_err(|e| plan_datafusion_err!("Failed to init catalog job: {e}"))?;

    let config = SessionConfig::new()
        .with_create_default_catalog_and_schema(false)
        .with_information_schema(false)
        .with_extension(Arc::new(catalog_manager));

    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state_builder);
    Ok(ctx)
}
