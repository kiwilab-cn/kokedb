use thiserror::Error;

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("Query meta db error: {0}")]
    MetaReqeustError(String),
    #[error("Interval error: {0}")]
    Internal(String),
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    #[error("Task execution failed: {0}")]
    ExecutionFailed(String),
    #[error("Invalid DSN format: {0}")]
    InvalidDsn(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Resource exhausted")]
    ResourceExhausted,
    #[error("Thread pool error: {0}")]
    ThreadPoolError(String),
    #[error("Task queue full")]
    QueueFull,
    #[error("Invalid task argument: {0}")]
    InvalideTaskArgment(String),
}
