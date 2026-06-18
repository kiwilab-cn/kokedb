use serde::{Deserialize, Serialize};

use crate::provider::{DatabaseStatus, TableColumnStatus, TableStatus};

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
}
