use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::datatypes::*;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, StreamExt};
use kokedb_common::env::get_env_as;
use kokedb_common::table::mysql::{get_mysql_table_schema, mysql_rows_to_record_batch};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

#[derive(Debug, Clone)]
pub struct MysqlConfig {
    pub connection_string: String,
    pub table_name: String,
    /// The MySQL database (schema) the table lives in.
    pub schema_name: Option<String>,
}

async fn connect_source(dsn: &str) -> Result<MySqlPool> {
    let secs = get_env_as("KOKEDB_SOURCE_CONNECT_TIMEOUT_SECS", 10u64);
    let fut = MySqlPoolOptions::new()
        .acquire_timeout(Duration::from_secs(secs))
        .connect(dsn);
    match tokio::time::timeout(Duration::from_secs(secs), fut).await {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(e)) => Err(DataFusionError::External(Box::new(e))),
        Err(_) => Err(DataFusionError::Execution(format!(
            "source database is unavailable: connection timed out after {secs}s"
        ))),
    }
}

#[derive(Debug, Clone)]
pub struct MySqlTableProvider {
    config: MysqlConfig,
    schema: SchemaRef,
    pool: Arc<MySqlPool>,
    estimated_rows: Option<usize>,
}

impl MySqlTableProvider {
    pub async fn new(config: MysqlConfig) -> Result<Self> {
        let pool = connect_source(&config.connection_string).await?;
        let db = Self::database(&pool, &config).await?;
        let schema = get_mysql_table_schema(&pool, &db, &config.table_name)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let estimated_rows = Self::estimate_row_count(&pool, &db, &config.table_name).await;
        Ok(Self {
            config,
            schema: Arc::new(schema),
            pool: Arc::new(pool),
            estimated_rows,
        })
    }

    async fn database(pool: &MySqlPool, config: &MysqlConfig) -> Result<String> {
        if let Some(s) = config.schema_name.as_deref().filter(|s| !s.is_empty()) {
            return Ok(s.to_string());
        }
        sqlx::query_scalar::<_, Option<String>>("SELECT DATABASE()")
            .fetch_one(pool)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| DataFusionError::Execution("no MySQL database selected".to_string()))
    }

    /// MySQL's cached row estimate from information_schema (no table scan).
    async fn estimate_row_count(pool: &MySqlPool, db: &str, table: &str) -> Option<usize> {
        let n: Option<i64> = sqlx::query_scalar(
            "SELECT table_rows FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
        )
        .bind(db)
        .bind(table)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        n.filter(|v| *v >= 0).map(|v| v as usize)
    }
}

#[async_trait::async_trait]
impl TableProvider for MySqlTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        let mut stats = Statistics::new_unknown(&self.schema);
        if let Some(rows) = self.estimated_rows {
            stats.num_rows = Precision::Inexact(rows);
        }
        Some(stats)
    }

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
            Some(indices) => Arc::new(Schema::new(
                indices.iter().map(|&i| self.schema.field(i).clone()).collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };
        Ok(Arc::new(MysqlExec::new(
            self.config.clone(),
            self.pool.clone(),
            projected_schema,
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.estimated_rows,
        )))
    }
}

struct MysqlExec {
    config: MysqlConfig,
    pool: Arc<MySqlPool>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    estimated_rows: Option<usize>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl MysqlExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: MysqlConfig,
        pool: Arc<MySqlPool>,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        estimated_rows: Option<usize>,
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
            estimated_rows,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn build_query(&self) -> String {
        let table = match &self.config.schema_name {
            Some(s) if !s.is_empty() => format!("{}.{}", quote_ident(s), quote_ident(&self.config.table_name)),
            _ => quote_ident(&self.config.table_name),
        };
        let columns = match &self.projection {
            Some(p) if p.is_empty() => "1".to_string(),
            Some(projection) => projection
                .iter()
                .map(|&i| quote_ident(self.projected_schema.field(i).name()))
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };
        let mut query = format!("SELECT {} FROM {}", columns, table);
        if !self.filters.is_empty() {
            let where_clauses: Vec<String> = self.filters.iter().filter_map(expr_to_sql).collect();
            if !where_clauses.is_empty() {
                query.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
            }
        }
        if let Some(limit) = self.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        query
    }
}

impl std::fmt::Debug for MysqlExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MysqlExec: table={}", self.config.table_name)
    }
}

impl DisplayAs for MysqlExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MysqlExec: table={}", self.config.table_name)
    }
}

impl ExecutionPlan for MysqlExec {
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
        Ok(Box::pin(MysqlStream::new(
            self.build_query(),
            self.pool.clone(),
            self.projected_schema.clone(),
        )))
    }
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn statistics(&self) -> Result<Statistics> {
        let mut stats = Statistics::new_unknown(&self.projected_schema);
        if let Some(rows) = self.estimated_rows {
            stats.num_rows = Precision::Inexact(rows);
        }
        Ok(stats)
    }
    fn name(&self) -> &str {
        "MysqlExec"
    }
}

fn rows_to_batch(rows: &[sqlx::mysql::MySqlRow], schema: &SchemaRef) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
            .map_err(DataFusionError::from)
    } else {
        mysql_rows_to_record_batch(rows, schema)
            .map_err(|e| DataFusionError::Internal(format!("Failed to build record batch: {e}")))
    }
}

struct MysqlStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl MysqlStream {
    fn new(query: String, pool: Arc<MySqlPool>, schema: SchemaRef) -> Self {
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
                                Ok(batch) => { yield Ok(batch); batch_rows.clear(); }
                                Err(e) => { yield Err(e); return; }
                            }
                        }
                    }
                    Err(e) => { yield Err(DataFusionError::External(Box::new(e))); return; }
                }
            }
            if !batch_rows.is_empty() {
                match rows_to_batch(&batch_rows, &schema_for_stream) {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => yield Err(e),
                }
            }
        };
        Self { schema, stream: Box::pin(stream) }
    }
}

impl RecordBatchStream for MysqlStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MysqlStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Translates a DataFusion filter into a MySQL boolean SQL fragment, or `None`
/// for anything not confidently translatable (left to DataFusion's FilterExec).
fn expr_to_sql(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(b) => {
            let left = expr_to_sql(&b.left)?;
            let right = expr_to_sql(&b.right)?;
            let op = match b.op {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::LikeMatch => "LIKE",
                Operator::NotLikeMatch => "NOT LIKE",
                _ => return None,
            };
            Some(format!("({} {} {})", left, op, right))
        }
        Expr::Column(col) => Some(quote_ident(&col.name)),
        Expr::IsNull(inner) => Some(format!("({} IS NULL)", expr_to_sql(inner)?)),
        Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", expr_to_sql(inner)?)),
        Expr::Not(inner) => Some(format!("(NOT {})", expr_to_sql(inner)?)),
        Expr::Between(b) => {
            let what = expr_to_sql(&b.expr)?;
            let low = expr_to_sql(&b.low)?;
            let high = expr_to_sql(&b.high)?;
            let kw = if b.negated { "NOT BETWEEN" } else { "BETWEEN" };
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
        Expr::Literal(scalar, None) => match scalar {
            datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                // Escape both backslash and quote for default MySQL string mode.
                Some(format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")))
            }
            datafusion::scalar::ScalarValue::Int32(Some(i)) => Some(i.to_string()),
            datafusion::scalar::ScalarValue::Int64(Some(i)) => Some(i.to_string()),
            datafusion::scalar::ScalarValue::Float32(Some(f)) => Some(f.to_string()),
            datafusion::scalar::ScalarValue::Float64(Some(f)) => Some(f.to_string()),
            datafusion::scalar::ScalarValue::Boolean(Some(b)) => {
                Some(if *b { "1".to_string() } else { "0".to_string() })
            }
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};

    #[test]
    fn expr_to_sql_uses_backtick_idents_and_escapes_strings() {
        assert_eq!(expr_to_sql(&col("id").gt(lit(10i64))).as_deref(), Some("(`id` > 10)"));
        assert_eq!(
            expr_to_sql(&col("name").eq(lit("o'brien"))).as_deref(),
            Some("(`name` = 'o''brien')")
        );
        assert_eq!(
            expr_to_sql(&col("id").in_list(vec![lit(1i64), lit(2i64)], false)).as_deref(),
            Some("(`id` IN (1, 2))")
        );
        assert!(expr_to_sql(&col("ts").eq(datafusion::prelude::now())).is_none());
    }
}
