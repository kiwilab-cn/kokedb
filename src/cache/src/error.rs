use thiserror::Error;

pub type CacheResult<T> = Result<T, CacheError>;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("external error: {0}")]
    External(String),
    #[error("foyer error: {0}")]
    FoyerError(String),
}
