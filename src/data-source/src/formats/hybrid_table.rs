use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use log::warn;

use crate::formats::remote_table::{PostgreSQLConfig, PostgreSQLTableProvider};

/// A cached table that can serve a scan from its parquet snapshot (the fast
/// default) OR from the live PostgreSQL source, decided per scan from the
/// pushed-down filters. This is opt-in cost-based routing: it sends a scan to
/// the source only when that is very likely cheaper than scanning the snapshot
/// — namely a point lookup (equality on a primary-key column) against a large
/// table, where PostgreSQL answers from its index while the columnar snapshot
/// would be scanned. Everything else stays on the snapshot.
///
/// Routing inputs (table size, PK columns) come from the meta store, so a
/// cache-served query pays no live-source cost; PostgreSQL is contacted only
/// when a scan actually routes live, and any failure there falls back to the
/// snapshot (correctness is never at stake — both are valid reads).
pub struct HybridTableProvider {
    /// The parquet-snapshot provider (the default path).
    cached: Arc<dyn TableProvider>,
    /// Connection details for the live source, used only when routing live.
    pg_config: PostgreSQLConfig,
    /// Primary-key columns; an equality on any of these is an index point read.
    pk_columns: HashSet<String>,
    /// Estimated source row count; routing live is only worth it above this.
    est_rows: Option<usize>,
    /// Minimum table size (rows) before live routing is even considered.
    min_rows: usize,
}

impl std::fmt::Debug for HybridTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridTableProvider")
            .field("table", &self.pg_config.table_name)
            .field("est_rows", &self.est_rows)
            .field("min_rows", &self.min_rows)
            .finish()
    }
}

impl HybridTableProvider {
    pub fn new(
        cached: Arc<dyn TableProvider>,
        pg_config: PostgreSQLConfig,
        pk_columns: Vec<String>,
        est_rows: Option<usize>,
        min_rows: usize,
    ) -> Self {
        Self {
            cached,
            pg_config,
            pk_columns: pk_columns.into_iter().collect(),
            est_rows,
            min_rows,
        }
    }

    /// A scan routes live only for a large table with an equality predicate on a
    /// primary-key column (a sargable point lookup). Small/unsized tables and
    /// non-point scans always use the snapshot.
    fn should_route_live(&self, filters: &[Expr]) -> bool {
        let big = self.est_rows.map(|n| n >= self.min_rows).unwrap_or(false);
        if !big || self.pk_columns.is_empty() {
            return false;
        }
        filters.iter().any(|f| pk_equality(f, &self.pk_columns))
    }
}

#[async_trait::async_trait]
impl TableProvider for HybridTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.cached.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Defer to the snapshot provider's pushdown semantics: equality filters are
    /// pushed (for parquet pruning), so they also reach `scan` for the routing
    /// decision, and the live path re-translates whatever it can.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.cached.supports_filters_pushdown(filters)
    }

    /// The snapshot's statistics drive planning (the default path); the live
    /// path is a per-scan optimization, not a different table size.
    fn statistics(&self) -> Option<Statistics> {
        self.cached.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.should_route_live(filters) {
            match PostgreSQLTableProvider::new(self.pg_config.clone()).await {
                Ok(remote) => return remote.scan(state, projection, filters, limit).await,
                Err(e) => warn!(
                    "Cost-based routing chose the live source for {} but it is \
                     unavailable ({e}); serving the cached snapshot instead.",
                    self.pg_config.table_name
                ),
            }
        }
        self.cached.scan(state, projection, filters, limit).await
    }
}

/// Whether `expr` contains an equality `pk_col = <literal>` (either operand
/// order), recursing through `AND`. Only equality on a PK column counts.
fn pk_equality(expr: &Expr, pk: &HashSet<String>) -> bool {
    let Expr::BinaryExpr(b) = expr else {
        return false;
    };
    match b.op {
        Operator::Eq => {
            col_eq_literal(&b.left, &b.right, pk) || col_eq_literal(&b.right, &b.left, pk)
        }
        Operator::And => pk_equality(&b.left, pk) || pk_equality(&b.right, pk),
        _ => false,
    }
}

fn col_eq_literal(maybe_col: &Expr, maybe_lit: &Expr, pk: &HashSet<String>) -> bool {
    matches!(maybe_col, Expr::Column(c) if pk.contains(&c.name))
        && matches!(maybe_lit, Expr::Literal(_, _))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    fn pk() -> HashSet<String> {
        ["id".to_string()].into_iter().collect()
    }

    #[test]
    fn detects_pk_equality_either_order_and_under_and() {
        assert!(pk_equality(&col("id").eq(lit(7i64)), &pk()));
        assert!(pk_equality(&lit(7i64).eq(col("id")), &pk()));
        assert!(pk_equality(
            &col("id").eq(lit(7i64)).and(col("v").gt(lit(1i64))),
            &pk()
        ));
    }

    #[test]
    fn ignores_non_pk_or_non_equality() {
        // Equality on a non-PK column.
        assert!(!pk_equality(&col("v").eq(lit(7i64)), &pk()));
        // Range on the PK column is not a point lookup.
        assert!(!pk_equality(&col("id").gt(lit(7i64)), &pk()));
        // Column = column (no literal) is not a point lookup.
        assert!(!pk_equality(&col("id").eq(col("v")), &pk()));
    }
}
