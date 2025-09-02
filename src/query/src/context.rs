use std::sync::Arc;

use datafusion::{
    execution::{runtime_env::RuntimeEnv, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::plan_datafusion_err;
use kokedb_catalog::manager::CatalogManager;

use crate::catalog::PostgreSQLMetaCatalogProviderList;

pub async fn create_session_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let local_dsn = std::env::var("PG_META_DSN")
        .unwrap_or("postgresql://postgres:123456@192.168.0.227:25432/kokedb".to_string());
    let catalog_list = Arc::new(
        PostgreSQLMetaCatalogProviderList::new(&local_dsn)
            .await
            .unwrap(),
    );

    let runtime = Arc::new(RuntimeEnv::default());

    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: config.catalog.default_catalog.clone(),
        default_database: config.catalog.default_database.clone(),
        global_temporary_database: config.catalog.global_temporary_database.clone(),
    };
    CatalogManager::new(options)
        .map_err(|e| plan_datafusion_err!("failed to create catalog manager: {e}"));

    let mut session_config = SessionConfig::new()
        // We do not use the DataFusion catalog and schema since we manage catalogs ourselves.
        .with_create_default_catalog_and_schema(false)
        .with_information_schema(false)
        .with_extension(Arc::new(create_catalog_manager(&options.config)?));

    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_catalog_list(catalog_list)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state_builder);
    Ok(ctx)
}
