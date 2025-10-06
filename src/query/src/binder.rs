use std::sync::Arc;

use datafusion::{
    arrow::array::RecordBatch,
    physical_plan::{common::collect, execute_stream},
    prelude::SessionContext,
};
use kokedb_common::spec::Plan;
use kokedb_plan::{config::PlanConfig, resolve_and_execute_plan};
use kokedb_sql_analyzer::{parser::parse_one_statement, statement::from_ast_statement};

use crate::error::{QueryError, QueryResult};

pub fn parser(sql: &str) -> QueryResult<Plan> {
    let tree = parse_one_statement(sql)?;
    let plan = from_ast_statement(tree)?;
    Ok(plan)
}

pub async fn query(ctx: Arc<SessionContext>, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
    let plan = parser(sql)?;

    let default_plan_config = PlanConfig::default();

    let df_plan = resolve_and_execute_plan(&ctx, Arc::new(default_plan_config), plan).await?;

    let batches = execute_stream(df_plan, ctx.task_ctx())?;

    let batches = collect(batches).await?;
    Ok(batches)
}
