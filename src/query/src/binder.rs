use std::sync::Arc;

use datafusion::{
    execution::SendableRecordBatchStream, physical_plan::execute_stream, prelude::SessionContext,
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
) -> Result<SendableRecordBatchStream, QueryError> {
    let default_plan_config = PlanConfig::default();
    let df_plan =
        resolve_and_execute_plan(&ctx, Arc::new(default_plan_config), plan.clone()).await?;
    // TODO: execute_stream_partitioned maybe better.
    let batches = execute_stream(df_plan, ctx.task_ctx())?;

    Ok(batches)
}

pub async fn save_sql_history(sql: &str, plan: &Plan, cost: u64) -> Result<bool, QueryError> {
    let key = get_plan_hash(plan)?;

    let meta_client = PostgreSQLMetaCatalogProviderList::new().await?;
    let ret = meta_client
        .save_sql_stats(sql, key, cost)
        .await
        .map_err(|x| {
            QueryError::SaveSqlStatsError(format!("Failed to save sql stats with error: {:?}", x))
        })?;

    Ok(ret)
}
