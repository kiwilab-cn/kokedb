use std::io;

use kokedb_query::error::QueryError;
use opensrv_mysql::ErrorKind;
use thiserror::Error;

pub type MysqlResult<T> = Result<T, MysqlServerError>;

#[derive(Debug, Error)]
pub enum MysqlServerError {
    #[error("Create mysql result writer error: {0}")]
    CreateMysqlResultWriterError(String),
    #[error("Failed to write mysql result error: {0}")]
    WriteMysqlResultError(String),
    #[error("error in SQL parser: {0}")]
    SqlParserError(String),
    #[error("missing argument: {0}")]
    MissingArgument(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not implemented: {0}")]
    NotImplemented(String),
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("create context error: {0}")]
    CreateContextError(String),
    #[error("create sql plan error: {0}")]
    CreatePlanError(String),
    #[error("datafusion error: {0}")]
    DataFusionError(String),
    #[error("arrow error: {0}")]
    ArrowError(String),
    #[error("context error: {0}")]
    ContextError(String),
    #[error("external error: {0}")]
    ExternalError(String),
    #[error("io error: {0}")]
    IoError(String),
    #[error("object store error: {0}")]
    ObjectStoreError(String),
    #[error("parquet store error: {0}")]
    ParquetError(String),
    #[error("save sql stats error: {0}")]
    SaveSqlStatsError(String),
}

impl From<QueryError> for MysqlServerError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::SqlParserError(mesg) => MysqlServerError::SqlParserError(mesg),
            QueryError::MissingArgument(mesg) => MysqlServerError::MissingArgument(mesg),
            QueryError::InvalidArgument(mesg) => MysqlServerError::InvalidArgument(mesg),
            QueryError::NotImplemented(mesg) => MysqlServerError::NotImplemented(mesg),
            QueryError::NotSupported(mesg) => MysqlServerError::NotSupported(mesg),
            QueryError::InternalError(mesg) => MysqlServerError::InternalError(mesg),
            QueryError::CreateContextError(mesg) => MysqlServerError::CreateContextError(mesg),
            QueryError::CreatePlanError(mesg) => MysqlServerError::CreatePlanError(mesg),
            QueryError::DataFusionError(mesg) => MysqlServerError::DataFusionError(mesg),
            QueryError::ArrowError(mesg) => MysqlServerError::ArrowError(mesg),
            QueryError::ContextError(mesg) => MysqlServerError::ContextError(mesg),
            QueryError::ExternalError(mesg) => MysqlServerError::ExternalError(mesg),
            QueryError::IoError(mesg) => MysqlServerError::IoError(mesg),
            QueryError::ObjectStoreError(mesg) => MysqlServerError::ObjectStoreError(mesg),
            QueryError::ParquetError(mesg) => MysqlServerError::ParquetError(mesg),
            QueryError::SaveSqlStatsError(mesg) => MysqlServerError::SaveSqlStatsError(mesg),
        }
    }
}

impl From<io::Error> for MysqlServerError {
    fn from(error: io::Error) -> Self {
        let msg = format!("{} ({})", error.kind(), &error);

        MysqlServerError::WriteMysqlResultError(msg)
    }
}

pub fn to_mysql_error(error: &QueryError) -> (ErrorKind, String) {
    let mesg = error.to_string();

    let kind = ErrorKind::ER_QUERY_INTERRUPTED;

    (kind, mesg)
}
