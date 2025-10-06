use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
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
        )))
    }
}

struct PostgreSQLExec {
    config: PostgreSQLConfig,
    pool: Arc<PgPool>,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
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
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn build_query(&self) -> String {
        let full_table_name = match &self.config.schema_name {
            Some(schema) => format!("\"{}\".\"{}\"", schema, self.config.table_name),
            None => format!("\"{}\"", self.config.table_name),
        };

        let columns = if let Some(ref projection) = self.projection {
            projection
                .iter()
                .map(|&i| format!("\"{}\"", self.projected_schema.field(i).name()))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        let mut query = format!("SELECT {} FROM {}", columns, full_table_name);

        if !self.filters.is_empty() {
            let where_clauses: Vec<String> = self
                .filters
                .iter()
                .filter_map(|filter| self.expr_to_sql(filter))
                .collect();

            if !where_clauses.is_empty() {
                query.push_str(&format!(" WHERE {}", where_clauses.join(" AND ")));
            }
        }

        query
    }

    fn expr_to_sql(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                let left = self.expr_to_sql(&binary_expr.left)?;
                let right = self.expr_to_sql(&binary_expr.right)?;
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
        todo!()
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
                            match rows_to_record_batch(&batch_rows, &schema_for_stream) {
                                Ok(batch) => {
                                    yield Ok(batch);
                                    batch_rows.clear();
                                }
                                Err(e) => {
                                    yield Err(DataFusionError::Internal(format!("Failed to transaction record batch with error: {}", e)));
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
                match rows_to_record_batch(&batch_rows, &schema_for_stream) {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => yield Err(DataFusionError::Internal(format!("Failed to transaction record_batch with error: {}", e))),
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

    #[tokio::test]
    async fn test_postgresql_table_provider() -> Result<()> {
        let config = PostgreSQLConfig {
            connection_string: "postgresql://postgres:123456@192.168.0.227:25432/postgres"
                .to_string(),
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
}
