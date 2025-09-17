use kokedb_common::file::get_remote_catalog_parent_local_path;
use kokedb_meta::{catalog_list::PostgreSQLMetaCatalogProviderList, schema::SchemaTable};
use log::info;

use crate::{
    error::TaskError, read_postgres::convert_postgres_to_parquet,
    task_manager::CacheTableTaskConfig,
};

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError>;
}

pub struct DataSyncExecutor;

#[async_trait::async_trait]
impl TaskExecutor for DataSyncExecutor {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        _progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError> {
        info!("Received task: {:?}", &config);
        let dsn = config.dsn;
        let catalog = &config.catalog_name;
        let source_table = &config.source_table;
        let local_table = &config.local_table;

        let local_path = format!(
            "{}/{}/{}/{}",
            get_remote_catalog_parent_local_path(),
            catalog,
            local_table.replace('.', "/"),
            uuid::Uuid::new_v4()
        );

        let (schema, table) = local_table
            .split_once('.')
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or(("public".to_string(), local_table.to_string()));

        let arrow_schema = convert_postgres_to_parquet(&dsn, &source_table, &local_path)
            .await
            .map_err(|x| {
                TaskError::ExecutionFailed(format!(
                    "Failed to write postgresql table to parquet with error: {}",
                    x
                ))
            })?;

        let postgresql_catalog = PostgreSQLMetaCatalogProviderList::new()
            .await
            .map_err(|_x| {
                TaskError::DatabaseError("Failed to connect meta postgresql server.".to_string())
            })?;

        let schema_info = SchemaTable {
            catalog: catalog.as_str(),
            schema: schema.as_str(),
            table: table.as_str(),
            arrow_schema: arrow_schema.clone(),
            local_path: &local_path,
        };

        postgresql_catalog
            .save_table_schema(&schema_info)
            .map_err(|_x| {
                TaskError::DatabaseError(
                    "Failed to save table schema to meta postgresql server.".to_string(),
                )
            })?;
        Ok(())
    }
}
