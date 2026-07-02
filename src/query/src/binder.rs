use std::sync::Arc;

use datafusion::{
    execution::{context::TaskContext, SendableRecordBatchStream},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, execute_stream,
        sorts::sort_preserving_merge::SortPreservingMergeExec, ExecutionPlan,
        ExecutionPlanProperties,
    },
    prelude::SessionContext,
};
use kokedb_common::{hash::get_plan_hash, spec::Plan};
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_plan::{config::PlanConfig, resolve_and_execute_plan};
use kokedb_sql_analyzer::{parser::parse_one_statement, statement::from_ast_statement};

use crate::error::{QueryError, QueryResult};

pub fn parser(sql: &str) -> QueryResult<Plan> {
    let tree = parse_one_statement(sql)?;
    let plan = from_ast_statement(tree)?;
    Ok(plan)
}

pub async fn query(
    ctx: Arc<SessionContext>,
    plan: &Plan,
    key: u128,
    max_staleness_secs: Option<u64>,
) -> Result<SendableRecordBatchStream, QueryError> {
    let plan_config = PlanConfig {
        max_staleness_secs,
        ..PlanConfig::default()
    };
    let df_plan =
        resolve_and_execute_plan(&ctx, Arc::new(plan_config), plan.clone(), key).await?;
    let batches = execute_preserving_order(df_plan, ctx.task_ctx())?;

    Ok(batches)
}

/// Executes a physical plan's partitions in parallel and returns a single
/// row stream for the MySQL writer.
///
/// `execute_stream` always merges multiple partitions with
/// `CoalescePartitionsExec`, which is *order-dropping*: it emits batches in
/// whatever order partitions produce them. When the plan carries an output
/// ordering (e.g. `ORDER BY`), we instead merge with `SortPreservingMergeExec`
/// so the parallel partitions are k-way merged back into the correct order.
/// Single-partition plans take the direct path (no merge wrapper).
fn execute_preserving_order(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream, QueryError> {
    let partitions = plan.output_partitioning().partition_count();
    if partitions <= 1 {
        // Handles the 0- and 1-partition cases without a merge operator.
        return Ok(execute_stream(plan, task_ctx)?);
    }

    let merged: Arc<dyn ExecutionPlan> = match plan.output_ordering() {
        Some(ordering) => Arc::new(SortPreservingMergeExec::new(ordering.clone(), plan)),
        None => Arc::new(CoalescePartitionsExec::new(plan)),
    };
    Ok(merged.execute(0, task_ctx)?)
}

pub async fn save_sql_history(
    meta: &PostgreSQLMetaCatalogProviderList,
    sql: &str,
    plan: &Plan,
    cost: u64,
) -> Result<bool, QueryError> {
    // Only queries are recorded: history exists to power proactive re-caching
    // of query results, which never applies to commands — and command text can
    // carry secrets (`CREATE USER ... password=...`, CREATE CATALOG DSNs) that
    // must not be persisted into system.sql_stats.
    if !matches!(plan, Plan::Query(_)) {
        return Ok(false);
    }
    let key = get_plan_hash(plan)?;

    let ret = meta.save_sql_stats(sql, key, cost).await.map_err(|x| {
        QueryError::SaveSqlStatsError(format!("Failed to save sql stats with error: {:?}", x))
    })?;

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn part(vals: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals.to_vec()))]).unwrap()
    }

    // Four unordered partitions; target_partitions(4) keeps the scan parallel.
    async fn ctx_with_4_partitions() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let partitions = vec![
            vec![part(&[7, 3, 9])],
            vec![part(&[1, 8, 4])],
            vec![part(&[6, 2])],
            vec![part(&[5, 10, 0])],
        ];
        let table = MemTable::try_new(schema, partitions).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    async fn run(ctx: &SessionContext, sql: &str) -> (Arc<dyn ExecutionPlan>, Vec<i32>) {
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let stream = execute_preserving_order(plan.clone(), ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = {
            use futures::TryStreamExt;
            stream.try_collect().await.unwrap()
        };
        let vals: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        (plan, vals)
    }

    // ORDER BY output must come back fully sorted through our merge path.
    #[tokio::test]
    async fn preserves_order_for_order_by() {
        let ctx = ctx_with_4_partitions().await;
        let (_plan, vals) = run(&ctx, "SELECT v FROM t ORDER BY v").await;
        assert_eq!(vals, (0..=10).collect::<Vec<_>>());
    }

    // A bare scan stays multi-partition (exercises the parallel coalesce branch);
    // every row must survive even though order is unspecified.
    #[tokio::test]
    async fn coalesces_all_rows_for_unordered_scan() {
        let ctx = ctx_with_4_partitions().await;
        let (plan, mut vals) = run(&ctx, "SELECT v FROM t").await;
        assert!(
            plan.output_partitioning().partition_count() > 1,
            "scan should stay multi-partition so the >1 merge branch is exercised"
        );
        vals.sort();
        assert_eq!(vals, (0..=10).collect::<Vec<_>>());
    }

    // Sanity: the helper also drives single-partition plans (the <=1 path).
    #[tokio::test]
    async fn handles_single_partition_aggregate() {
        let ctx = ctx_with_4_partitions().await;
        let plan = ctx
            .sql("SELECT count(*) AS c FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let out = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(out[0].num_rows(), 1);
    }
}
