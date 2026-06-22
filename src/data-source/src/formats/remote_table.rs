use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::*;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, StreamExt};
use kokedb_common::table::postgresql::{get_postgresql_table_schema, rows_to_record_batch};
use sqlx::postgres::PgPool;

#[derive(Debug, Clone)]
pub struct PostgreSQLConfig {
    pub connection_string: String,
    pub table_name: String,
    pub schema_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PostgreSQLTableProvider {
    config: PostgreSQLConfig,
    schema: SchemaRef,
    pool: Arc<PgPool>,
}

impl PostgreSQLTableProvider {
    pub async fn new(config: PostgreSQLConfig) -> Result<Self> {
        let pool = PgPool::connect(&config.connection_string)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema = Self::infer_schema(&pool, &config).await?;

        Ok(Self {
            config,
            schema: Arc::new(schema),
            pool: Arc::new(pool),
        })
    }

    async fn infer_schema(pool: &PgPool, config: &PostgreSQLConfig) -> Result<Schema> {
        let schema_name = config.schema_name.as_deref().unwrap_or("public");
        let table_name = &config.table_name;

        get_postgresql_table_schema(pool, schema_name, table_name)
            .await
            .map_err(|x| DataFusionError::External(Box::new(x)))
    }
}

#[async_trait::async_trait]
impl TableProvider for PostgreSQLTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Translatable filters are pushed to PostgreSQL as `Inexact`: DataFusion
    /// keeps its own `FilterExec` on top, so correctness never depends on our
    /// SQL translation being perfect — pushdown is a pure row-reduction
    /// optimization. Filters we can't translate stay `Unsupported`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_sql(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(indices) => {
                let projected_fields: Vec<Field> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(projected_fields))
            }
            None => self.schema.clone(),
        };

        Ok(Arc::new(PostgreSQLExec::new(
            self.config.clone(),
            self.pool.clone(),
            projected_schema,
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }
}

struct PostgreSQLExec {
    config: PostgreSQLConfig,
    pool: Arc<PgPool>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl PostgreSQLExec {
    fn new(
        config: PostgreSQLConfig,
        pool: Arc<PgPool>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            config,
            pool,
            projected_schema,
            projection,
            filters,
            limit,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn build_query(&self) -> String {
        let full_table_name = match &self.config.schema_name {
            Some(schema) => format!("\"{}\".\"{}\"", schema, self.config.table_name),
            None => format!("\"{}\"", self.config.table_name),
        };

        let columns = match &self.projection {
            // An empty projection (e.g. `count(*)`) needs no columns — select a
            // constant so PostgreSQL still returns one row per source row.
            Some(p) if p.is_empty() => "1".to_string(),
            Some(projection) => projection
                .iter()
                .map(|&i| format!("\"{}\"", self.projected_schema.field(i).name()))
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };

        let mut query = format!("SELECT {} FROM {}", columns, full_table_name);

        if !self.filters.is_empty() {
            let where_clauses: Vec<String> = self
                .filters
                .iter()
                .filter_map(expr_to_sql)
                .collect();

            if !where_clauses.is_empty() {
                query.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
            }
        }

        // Safe to push: DataFusion only supplies a scan limit when nothing
        // below it changes row cardinality (it never pushes a limit past a
        // re-applied filter), so fetching only `n` rows from PostgreSQL cannot
        // drop rows a downstream operator still needs.
        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        query
    }
}

/// Translates a DataFusion filter `Expr` into a PostgreSQL boolean SQL
/// fragment. Returns `None` for anything not confidently translatable; the
/// caller treats pushdown as `Inexact`, so an untranslated filter is simply
/// re-applied by DataFusion rather than producing a wrong result.
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            let left = expr_to_sql(&binary_expr.left)?;
            let right = expr_to_sql(&binary_expr.right)?;
            let op = match binary_expr.op {
                datafusion::logical_expr::Operator::Eq => "=",
                datafusion::logical_expr::Operator::NotEq => "!=",
                datafusion::logical_expr::Operator::Lt => "<",
                datafusion::logical_expr::Operator::LtEq => "<=",
                datafusion::logical_expr::Operator::Gt => ">",
                datafusion::logical_expr::Operator::GtEq => ">=",
                datafusion::logical_expr::Operator::And => "AND",
                datafusion::logical_expr::Operator::Or => "OR",
                datafusion::logical_expr::Operator::LikeMatch => "LIKE",
                datafusion::logical_expr::Operator::NotLikeMatch => "NOT LIKE",
                _ => return None,
            };
            Some(format!("({} {} {})", left, op, right))
        }
        Expr::Column(col) => Some(format!("\"{}\"", col.name)),
        Expr::IsNull(inner) => Some(format!("({} IS NULL)", expr_to_sql(inner)?)),
        Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", expr_to_sql(inner)?)),
        Expr::Not(inner) => Some(format!("(NOT {})", expr_to_sql(inner)?)),
        Expr::Between(between) => {
            let what = expr_to_sql(&between.expr)?;
            let low = expr_to_sql(&between.low)?;
            let high = expr_to_sql(&between.high)?;
            let kw = if between.negated {
                "NOT BETWEEN"
            } else {
                "BETWEEN"
            };
            Some(format!("({} {} {} AND {})", what, kw, low, high))
        }
        Expr::InList(in_list) => {
            let what = expr_to_sql(&in_list.expr)?;
            let items: Option<Vec<String>> = in_list.list.iter().map(expr_to_sql).collect();
            let items = items?;
            if items.is_empty() {
                return None;
            }
            let kw = if in_list.negated { "NOT IN" } else { "IN" };
            Some(format!("({} {} ({}))", what, kw, items.join(", ")))
        }
        Expr::Literal(scalar_value, None) => match scalar_value {
            datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                Some(format!("'{}'", s.replace("'", "''")))
            }
            datafusion::scalar::ScalarValue::Int32(Some(i)) => Some(i.to_string()),
            datafusion::scalar::ScalarValue::Int64(Some(i)) => Some(i.to_string()),
            datafusion::scalar::ScalarValue::Float32(Some(f)) => Some(f.to_string()),
            datafusion::scalar::ScalarValue::Float64(Some(f)) => Some(f.to_string()),
            datafusion::scalar::ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
            _ => None,
        },
        _ => None,
    }
}

impl std::fmt::Debug for PostgreSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgreSQLExec")
    }
}

impl DisplayAs for PostgreSQLExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PostgreSQLExec: table={}", self.config.table_name)
    }
}

impl ExecutionPlan for PostgreSQLExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let query = self.build_query();
        let pool = self.pool.clone();
        let schema = self.projected_schema.clone();

        Ok(Box::pin(PostgreSQLStream::new(query, pool, schema)))
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn name(&self) -> &str {
        "PostgreSQLExec"
    }
}

/// Builds a record batch from source rows. Handles the zero-column case (a
/// `count(*)`-style scan with an empty projection): the batch carries no columns
/// but an explicit row count, which `RecordBatch::try_new` cannot infer.
fn rows_to_batch(rows: &[sqlx::postgres::PgRow], schema: &SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
            .map_err(DataFusionError::from)
    } else {
        rows_to_record_batch(rows, schema).map_err(|e| {
            DataFusionError::Internal(format!("Failed to build record batch: {e}"))
        })
    }
}

struct PostgreSQLStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl PostgreSQLStream {
    fn new(query: String, pool: Arc<PgPool>, schema: SchemaRef) -> Self {
        let schema_for_stream = schema.clone();

        let stream = async_stream::stream! {
            let mut rows_stream = sqlx::query(&query).fetch(&*pool);

            let mut batch_rows = Vec::new();
            const BATCH_SIZE: usize = 1000;

            while let Some(row_result) = rows_stream.next().await {
                match row_result {
                    Ok(row) => {
                        batch_rows.push(row);

                        if batch_rows.len() >= BATCH_SIZE {
                            match rows_to_batch(&batch_rows, &schema_for_stream) {
                                Ok(batch) => {
                                    yield Ok(batch);
                                    batch_rows.clear();
                                }
                                Err(e) => {
                                    yield Err(e);
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(DataFusionError::External(Box::new(e)));
                        return;
                    }
                }
            }

            if !batch_rows.is_empty() {
                match rows_to_batch(&batch_rows, &schema_for_stream) {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => yield Err(e),
                }
            }
        };

        Self {
            schema,
            stream: Box::pin(stream),
        }
    }
}

impl RecordBatchStream for PostgreSQLStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for PostgreSQLStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use log::info;

    // Filter translation is DB-free and the core correctness surface of
    // pushdown, so it is exercised directly here.
    #[test]
    fn expr_to_sql_translates_supported_filters() {
        assert_eq!(
            expr_to_sql(&col("id").gt(lit(10i64))).as_deref(),
            Some("(\"id\" > 10)")
        );
        assert_eq!(
            expr_to_sql(&col("name").eq(lit("o'brien"))).as_deref(),
            Some("(\"name\" = 'o''brien')")
        );
        assert_eq!(
            expr_to_sql(&col("id").is_null()).as_deref(),
            Some("(\"id\" IS NULL)")
        );
        assert_eq!(
            expr_to_sql(&col("id").is_not_null()).as_deref(),
            Some("(\"id\" IS NOT NULL)")
        );
        assert_eq!(
            expr_to_sql(&col("id").between(lit(1i64), lit(5i64))).as_deref(),
            Some("(\"id\" BETWEEN 1 AND 5)")
        );
        assert_eq!(
            expr_to_sql(&col("id").in_list(vec![lit(1i64), lit(2i64), lit(3i64)], false))
                .as_deref(),
            Some("(\"id\" IN (1, 2, 3))")
        );
        assert_eq!(
            expr_to_sql(&col("id").in_list(vec![lit(1i64)], true)).as_deref(),
            Some("(\"id\" NOT IN (1))")
        );
        // AND of two simple comparisons.
        assert_eq!(
            expr_to_sql(&col("a").gt(lit(1i64)).and(col("b").lt(lit(9i64)))).as_deref(),
            Some("((\"a\" > 1) AND (\"b\" < 9))")
        );
    }

    #[test]
    fn expr_to_sql_returns_none_for_untranslatable() {
        // A scalar function we don't translate must yield None so the caller
        // leaves it to DataFusion's FilterExec instead of emitting bad SQL.
        let unsupported = col("ts").eq(now());
        assert!(expr_to_sql(&unsupported).is_none());
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via `make integration-test`"]
    async fn test_postgresql_table_provider() -> Result<()> {
        let connection_string = std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".to_string());
        let config = PostgreSQLConfig {
            connection_string,
            table_name: "newtable".to_string(),
            schema_name: Some("test".to_string()),
        };

        let provider = PostgreSQLTableProvider::new(config).await?;

        let ctx = SessionContext::new();
        ctx.register_table("remote_table", Arc::new(provider))?;

        let df = ctx
            .sql("SELECT * FROM remote_table where column3 > 10")
            .await?;

        let results = df.collect().await?;
        info!("Results: {:?}", results);

        Ok(())
    }

    // Regression: `count(*)` over a remote (uncached) table — a zero-column
    // projection — previously failed with "must either specify a row count or at
    // least one column". 1500 rows exercises both the 1000-row chunk path and the
    // final partial batch.
    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via KOKEDB_TEST_DSN"]
    async fn count_star_over_remote_table() {
        use arrow::array::Int64Array;
        let dsn = std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".to_string());
        let pool = PgPool::connect(&dsn).await.unwrap();
        for stmt in [
            "DROP TABLE IF EXISTS it_remote_cnt",
            "CREATE TABLE it_remote_cnt (id int, v int)",
            "INSERT INTO it_remote_cnt SELECT g, g*2 FROM generate_series(1, 1500) g",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }

        let provider = PostgreSQLTableProvider::new(PostgreSQLConfig {
            connection_string: dsn.clone(),
            table_name: "it_remote_cnt".to_string(),
            schema_name: Some("public".to_string()),
        })
        .await
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let scalar = |batches: Vec<RecordBatch>| {
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };

        let total = ctx.sql("SELECT count(*) FROM t").await.unwrap().collect().await.unwrap();
        assert_eq!(scalar(total), 1500, "count(*) over remote table");

        let filtered = ctx
            .sql("SELECT count(*) FROM t WHERE id > 1495")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(scalar(filtered), 5, "filtered count(*) over remote table");

        sqlx::query("DROP TABLE IF EXISTS it_remote_cnt").execute(&pool).await.unwrap();
    }
}
