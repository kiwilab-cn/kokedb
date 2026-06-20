use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_datafusion_err, Column, Constraints, DFSchema, Result, Statistics};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::utils::conjunction;
use kokedb_common_datafusion::utils::{rename_physical_plan, rename_schema};

/// Pushes `filters` (expressed in the inner provider's column names) into the
/// physical `scan` plan so a parquet `DataSourceExec` gains its pruning
/// predicate. Returns the plan unchanged if the filters cannot be bound or
/// pushed; the caller's `FilterExec` still enforces correctness.
fn push_filters_into_scan(
    state: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    filters: &[Expr],
) -> Arc<dyn ExecutionPlan> {
    if filters.is_empty() {
        return plan;
    }
    let Ok(df_schema) = DFSchema::try_from(plan.schema()) else {
        return plan;
    };
    let physical: Option<Vec<_>> = filters
        .iter()
        .map(|f| state.create_physical_expr(f.clone(), &df_schema).ok())
        .collect();
    let Some(physical) = physical else {
        return plan;
    };
    let predicate = conjunction(physical);
    let Ok(filter_exec) = FilterExec::try_new(predicate, Arc::clone(&plan)) else {
        return plan;
    };
    let pushed = match FilterPushdown::new().optimize(Arc::new(filter_exec), state.config_options())
    {
        Ok(p) => p,
        Err(_) => return plan,
    };
    // Drop the synthetic `FilterExec` we wrapped with; the predicate now lives in
    // the scan and the caller re-applies the exact filter. If pushdown left a
    // `FilterExec` on top, unwrap to its child; otherwise use the pushed plan.
    if pushed.as_any().is::<FilterExec>() {
        pushed.children().first().map(|c| Arc::clone(c)).unwrap_or(plan)
    } else {
        pushed
    }
}

#[derive(Clone)]
pub(crate) struct RenameTableProvider {
    inner: Arc<dyn TableProvider>,
    /// A map from the new name to the old name.
    names: HashMap<String, String>,
    /// The schema for the renamed table.
    schema: SchemaRef,
}

impl Debug for RenameTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenameTableProvider")
            .field("names", &self.names)
            .finish()
    }
}

impl RenameTableProvider {
    pub fn try_new(inner: Arc<dyn TableProvider>, names: Vec<String>) -> Result<Self> {
        let schema = rename_schema(&inner.schema(), &names)?;
        let names = names
            .iter()
            .zip(inner.schema().fields.iter())
            .map(|(n, f)| (n.clone(), f.name().clone()))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            inner,
            names,
            schema,
        })
    }

    fn to_inner_expr(&self, expr: &Expr) -> Result<Expr> {
        let rewrite = |e: Expr| -> Result<Transformed<Expr>> {
            if let Expr::Column(Column {
                name,
                relation: _,
                spans,
            }) = e
            {
                let name = self
                    .names
                    .get(&name)
                    .ok_or_else(|| plan_datafusion_err!("column {name} not found"))?
                    .clone();
                // Drop the outer relation qualifier: the inner provider's schema
                // is unqualified, so a qualified column (e.g. `t.ts`) fails to
                // bind when the inner table builds its parquet pruning predicate,
                // silently disabling row-group pruning.
                Ok(Transformed::yes(Expr::Column(Column {
                    name,
                    relation: None,
                    spans,
                })))
            } else {
                Ok(Transformed::no(e))
            }
        };
        expr.clone().transform(rewrite).data()
    }
}

#[async_trait]
impl TableProvider for RenameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.names
            .get(column)
            .and_then(|column| self.inner.get_column_default(column))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = filters
            .iter()
            .map(|f| self.to_inner_expr(f))
            .collect::<Result<Vec<_>>>()?;
        let names = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let plan = self.inner.scan(state, projection, &filters, limit).await?;
        // DataFusion injects the parquet pruning predicate into the scan via the
        // physical `FilterPushdown` rule, which fuses a parent `FilterExec` into
        // the `DataSourceExec`. The rename projection we add below sits between
        // the query's `FilterExec` and the scan and blocks that rule, so we push
        // the (already column-renamed) filters into the inner scan here, before
        // wrapping. This restores row-group pruning for predicates on cached
        // tables. Best-effort: any filter that cannot be bound is skipped, since
        // the query's own `FilterExec` still guarantees correctness.
        let plan = push_filters_into_scan(state, plan, &filters);
        if let Some(projection) = projection {
            let names = projection
                .iter()
                .map(|i| names[*i].clone())
                .collect::<Vec<_>>();
            rename_physical_plan(plan, &names)
        } else {
            rename_physical_plan(plan, &names)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let filters = filters
            .iter()
            .map(|f| self.to_inner_expr(f))
            .collect::<Result<Vec<_>>>()?;
        let filters = filters.iter().collect::<Vec<_>>();
        self.inner.supports_filters_pushdown(&filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let names = self
            .inner
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let input = rename_physical_plan(input, &names)?;
        self.inner.insert_into(state, input, insert_op).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;

    // A predicate on a column read through `RenameTableProvider` must still reach
    // the parquet scan as a pruning predicate. The wrapper inserts a projection
    // between the query's `FilterExec` and the scan, which would otherwise block
    // DataFusion's physical filter pushdown and silently disable row-group
    // pruning (a large regression for time-range scans on cached tables).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rename_provider_preserves_parquet_pruning() {
        let dir = std::env::temp_dir().join("kokedb-test-rename-prune");
        let dir = dir.to_string_lossy();
        let _ = std::fs::remove_dir_all(dir.as_ref());

        let ctx = SessionContext::new();
        ctx.sql(
            "SELECT value AS id, arrow_cast(value, 'Timestamp(Second, Some(\"+00:00\"))') AS ts, \
             CAST(value AS DOUBLE) AS v FROM range(0, 1000000) ORDER BY value",
        )
        .await
        .unwrap()
        .write_parquet(dir.as_ref(), DataFrameWriteOptions::new(), None)
        .await
        .unwrap();

        let cfg = ListingTableConfig::new(ListingTableUrl::parse(dir.as_ref()).unwrap())
            .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())))
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        let inner: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(cfg).unwrap());
        let renamed =
            RenameTableProvider::try_new(inner, vec!["c0".into(), "c1".into(), "c2".into()])
                .unwrap();
        ctx.register_table("t", Arc::new(renamed)).unwrap();

        let plan = ctx
            .sql("EXPLAIN SELECT count(*) FROM t WHERE c1 > '1970-01-12 00:00:00'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rendered = arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(
            rendered.contains("pruning_predicate="),
            "expected a parquet pruning predicate through RenameTableProvider, got:\n{rendered}"
        );

        // Sanity: the predicate still computes the correct result.
        let n = ctx
            .sql("SELECT count(*) FROM t WHERE c1 > '1970-01-12 00:00:00'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let got = n[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        // ts == row index in seconds; '1970-01-12 00:00:00' == 950400s.
        assert_eq!(got, 1_000_000 - 950_400 - 1);

        let _ = std::fs::remove_dir_all(dir.as_ref());
    }
}
