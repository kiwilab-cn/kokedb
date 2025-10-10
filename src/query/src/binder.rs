use std::sync::Arc;

use datafusion::{
    arrow::array::RecordBatch,
    physical_plan::{common::collect, execute_stream},
    prelude::SessionContext,
};
use kokedb_common::spec::Plan;
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_plan::{config::PlanConfig, resolve_and_execute_plan};
use kokedb_sql_analyzer::{parser::parse_one_statement, statement::from_ast_statement};
use log::error;
use xxhash_rust::xxh3;

use crate::error::{QueryError, QueryResult};

pub fn parser(sql: &str) -> QueryResult<Plan> {
    let tree = parse_one_statement(sql)?;
    let plan = from_ast_statement(tree)?;
    Ok(plan)
}

pub async fn query(ctx: Arc<SessionContext>, sql: &str) -> Result<Vec<RecordBatch>, QueryError> {
    let instant = std::time::Instant::now();

    let plan = parser(sql)?;
    let default_plan_config = PlanConfig::default();
    let df_plan =
        resolve_and_execute_plan(&ctx, Arc::new(default_plan_config), plan.clone()).await?;
    let batches = execute_stream(df_plan, ctx.task_ctx())?;
    let batches = collect(batches).await?;

    let cost = instant.elapsed().as_millis() as u64;

    let ret = save_sql_history(sql, &plan, cost).await;
    if ret.is_err() {
        error!(
            "Failed to store sql execute info to meta db with error: {:?}",
            ret.err().unwrap()
        );
    }

    Ok(batches)
}

async fn save_sql_history(sql: &str, plan: &Plan, cost: u64) -> Result<bool, QueryError> {
    let plan_bytes = serde_json::to_vec(&plan).map_err(|x| {
        QueryError::InternalError(format!("Failed to serde plan to json with error: {:?}", x))
    })?;

    let key = xxh3::xxh3_64(&plan_bytes);

    let meta_client = PostgreSQLMetaCatalogProviderList::new()
        .await
        .map_err(|_x| {
            QueryError::InternalError("Failed to connect meta postgresql server.".to_string())
        })?;
    let ret = meta_client
        .save_sql_stats(sql, key, cost)
        .await
        .map_err(|x| {
            QueryError::SaveSqlStatsError(format!("Failed to save sql stats with error: {:?}", x))
        })?;

    Ok(ret)
}
