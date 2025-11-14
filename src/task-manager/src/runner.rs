use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::{
    env::get_env_as,
    file::{cleanup_old_directories, get_remote_catalog_parent_local_path},
};
use kokedb_meta::{catalog_list::PostgreSQLMetaCatalogProviderList, schema::SchemaTable};
use log::{error, info};

use crate::{
    error::TaskError, read_postgres::convert_postgres_to_parquet,
    task_manager::CacheTableTaskConfig,
};

#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        cache: LruResultCache,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError>;
}

pub struct DataSyncExecutor;

#[async_trait::async_trait]
impl TaskExecutor for DataSyncExecutor {
    async fn execute(
        &self,
        config: CacheTableTaskConfig,
        cache: LruResultCache,
        _progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError> {
        info!("Received task: {:?}", &config);
        let dsn = config.dsn;
        let catalog = &config.catalog_name;
        let source_table = &config.source_table;
        let local_table = &config.local_table;
        let table_base_path = format!(
            "{}/{}/{}",
            get_remote_catalog_parent_local_path(),
            catalog,
            local_table.replace('.', "/"),
        );

        let local_path = format!("{}/{}", &table_base_path, uuid::Uuid::new_v4());

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
            .await
            .map_err(|_x| {
                TaskError::DatabaseError(
                    "Failed to save table schema to meta postgresql server.".to_string(),
                )
            })?;

        // find all cache key link with the table.
        let table_cache_key_list = postgresql_catalog
            .get_table_cache_key(&schema_info)
            .await
            .map_err(|_x| {
                TaskError::DatabaseError(
                    "Failed to save table schema to meta postgresql server.".to_string(),
                )
            })?;

        if let Some(cache_key_list) = table_cache_key_list {
            for cache_key in cache_key_list {
                let ret = cache.delete(cache_key).await;
                if ret.is_err() {
                    error!("Failed to delete table: {} key from cache.", schema_info);
                }
            }
        }

        let cache_versions = get_env_as("CACHE_KEEP_NUM", 3usize);
        if let Err(x) =
            cleanup_old_directories(&table_base_path, cache_versions, Some(&local_path)).await
        {
            error!(
                "Failed to clean table path: {} with error: {}",
                &table_base_path,
                x.to_string()
            );
        }

        Ok(())
    }
}
