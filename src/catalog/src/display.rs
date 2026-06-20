use serde::{Deserialize, Serialize};

use crate::provider::{DatabaseStatus, FunctionStatus, TableColumnStatus, TableStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmptyDisplay {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleValueDisplay<T> {
    pub value: T,
}

/// One row of `SHOW CACHE POLICIES` output. The cache policy is independent of
/// the catalog display dialect, so it is represented with a concrete struct
/// rather than an associated type on [`CatalogDisplay`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePolicyDisplay {
    pub catalog: String,
    pub db_type: String,
    pub cache_policy: String,
}

/// One row of `REFRESH CACHE FROM TABLE` output: the resolved table, the queued
/// sync task id, and the resulting status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshCacheDisplay {
    pub table: String,
    pub task_id: String,
    pub status: String,
}

/// One row of `SHOW TABLE METADATA`: the inferred sync decision, its validation
/// lifecycle, and the observed signals that drove it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadataDisplay {
    pub table: String,
    pub inc_mode: String,
    pub inc_status: String,
    pub inc_tier: String,
    pub source: String,
    pub confidence: Option<f32>,
    pub watermark_column: Option<String>,
    pub pk_columns: Option<String>,
    pub partition_by: Option<String>,
    pub refresh_bucket: String,
    pub refresh_interval_sec: i32,
    pub est_row_count: Option<i64>,
    pub est_size_bytes: Option<i64>,
    pub churn_per_hour: Option<f32>,
    pub access_per_day: Option<f32>,
    pub audit_passes: i32,
    pub divergence_count: i32,
    pub reason: Option<String>,
}

/// One row of `SHOW CACHE JOBS`: a single table-sync run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheJobDisplay {
    pub id: i64,
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub status: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
}

/// A trait for displaying catalog information in a structured format.
/// This is useful for defining output schemas and producing outputs
/// for various SQL catalog commands.
pub trait CatalogDisplay {
    type Catalog: Serialize + for<'de> Deserialize<'de>;
    type Database: Serialize + for<'de> Deserialize<'de>;
    type Table: Serialize + for<'de> Deserialize<'de>;
    type TableColumn: Serialize + for<'de> Deserialize<'de>;
    type Function: Serialize + for<'de> Deserialize<'de>;

    fn catalog(name: String) -> Self::Catalog;

    fn database(status: DatabaseStatus) -> Self::Database;

    fn table(status: TableStatus) -> Self::Table;

    fn table_column(status: TableColumnStatus) -> Self::TableColumn;

    fn function(status: FunctionStatus) -> Self::Function;
}
