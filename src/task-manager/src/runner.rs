use log::info;

use crate::{
    error::TaskError, read_postgres::convert_postgres_to_parquet, task::CacheTableTaskConfig,
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
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError> {
        info!("Received task: {:?}", &config);
        let dsn = config.dsn;
        let catalog = config.catalog_name;
        let source_table = config.source_table;
        let local_table = config.local_table;

        let local_path = format!("{}/{}/{}", "/tmp", catalog, local_table.replace('.', "/"));

        convert_postgres_to_parquet(&dsn, &source_table, &local_path)
            .await
            .map_err(|x| {
                TaskError::ExecutionFailed(format!(
                    "Failed to write postgresql table to parquet with error: {}",
                    x
                ))
            })?;

        Ok(())
    }
}
