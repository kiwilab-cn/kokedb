use datafusion_common::DataFusionError;
use kokedb_plan::error::PlanError;
use kokedb_sql_analyzer::error::SqlError;
use thiserror::Error;

pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug, Error)]
pub enum QueryError {
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
}

impl From<SqlError> for QueryError {
    fn from(error: SqlError) -> Self {
        match error {
            SqlError::SqlParserError(mesg) => QueryError::SqlParserError(mesg),
            SqlError::MissingArgument(mesg) => QueryError::MissingArgument(mesg),
            SqlError::InvalidArgument(mesg) => QueryError::InvalidArgument(mesg),
            SqlError::NotImplemented(mesg) => QueryError::NotImplemented(mesg),
            SqlError::NotSupported(mesg) => QueryError::NotSupported(mesg),
            SqlError::InternalError(mesg) => QueryError::InternalError(mesg),
        }
    }
}

impl From<PlanError> for QueryError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::DataFusionError(mesg) => QueryError::CreatePlanError(mesg.to_string()),
            PlanError::ArrowError(mesg) => QueryError::CreatePlanError(mesg.to_string()),
            PlanError::MissingArgument(mesg) => QueryError::MissingArgument(mesg),
            PlanError::InvalidArgument(mesg) => QueryError::InvalidArgument(mesg),
            PlanError::NotImplemented(mesg) => QueryError::NotImplemented(mesg),
            PlanError::NotSupported(mesg) => QueryError::NotSupported(mesg),
            PlanError::InternalError(mesg) => QueryError::InternalError(mesg),
            PlanError::AnalysisError(mesg) => QueryError::CreatePlanError(mesg),
            PlanError::DeltaTableError(mesg) => QueryError::CreatePlanError(mesg),
        }
    }
}

impl From<DataFusionError> for QueryError {
    fn from(error: DataFusionError) -> Self {
        match error {
            DataFusionError::ArrowError(arrow_error, _) => {
                QueryError::ArrowError(arrow_error.to_string())
            }
            DataFusionError::ParquetError(parquet_error) => {
                QueryError::ParquetError(parquet_error.to_string())
            }
            DataFusionError::AvroError(error) => QueryError::DataFusionError(error.to_string()),
            DataFusionError::ObjectStore(error) => QueryError::ObjectStoreError(error.to_string()),
            DataFusionError::IoError(error) => QueryError::IoError(error.to_string()),
            DataFusionError::SQL(parser_error, _) => {
                QueryError::SqlParserError(parser_error.to_string())
            }
            DataFusionError::NotImplemented(mesg) => QueryError::NotImplemented(mesg),
            DataFusionError::Internal(mesg) => QueryError::InternalError(mesg),
            DataFusionError::Plan(mesg) => QueryError::CreatePlanError(mesg),
            DataFusionError::Configuration(mesg) => QueryError::DataFusionError(mesg),
            DataFusionError::SchemaError(schema_error, _) => {
                QueryError::DataFusionError(schema_error.to_string())
            }
            DataFusionError::Execution(mesg) => QueryError::DataFusionError(mesg),
            DataFusionError::ExecutionJoin(join_error) => {
                QueryError::DataFusionError(join_error.to_string())
            }
            DataFusionError::ResourcesExhausted(mesg) => QueryError::DataFusionError(mesg),
            DataFusionError::External(error) => QueryError::ExternalError(error.to_string()),
            DataFusionError::Context(_, data_fusion_error) => {
                QueryError::ContextError(data_fusion_error.to_string())
            }
            DataFusionError::Substrait(mesg) => QueryError::DataFusionError(mesg),
            DataFusionError::Diagnostic(_diagnostic, data_fusion_error) => {
                QueryError::DataFusionError(data_fusion_error.to_string())
            }
            DataFusionError::Collection(data_fusion_errors) => QueryError::DataFusionError(
                data_fusion_errors
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            DataFusionError::Shared(data_fusion_error) => {
                QueryError::DataFusionError(data_fusion_error.to_string())
            }
        }
    }
}
