#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync {
    async fn execute(
        &self,
        config: DataSourceConfig,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> Result<(), TaskError>;
}

pub struct DataSyncExecutor;

#[async_trait::async_trait]
impl TaskExecutor for DataSyncExecutor {
    async fn execute(
        &self,
        config: DataSourceConfig,
        progress_callback: Option<Box<dyn Fn(f32) + Send + Sync>>,
    ) -> impl Future<Output = Result<(), TaskError>> {
    }
}
