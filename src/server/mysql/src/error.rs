use thiserror::Error;

pub type MysqlResult<T> = Result<T, MysqlServerError>;

#[derive(Debug, Error)]
pub enum MysqlServerError {
    #[error("Failed generate plan: {0}")]
    GeneratePlanError(String),
    #[error("Failed to transform plan: {0}")]
    TransformPhyPlanError(String),
    #[error("Failed to execute datafusion plan: {0}")]
    ExecutePhyPlanError(String),
    #[error("Create mysql result writer error: {0}")]
    CreateMysqlResultWriterError(String),
    #[error("Failed to compact mysql result error: {0}")]
    CompactExecuteResultError(String),
    #[error("Failed to write mysql result error: {0}")]
    WriteMysqlResultError(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("external error: {0}")]
    External(String),
}
