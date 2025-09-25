use kokedb_common::spec::Plan;
use kokedb_sql_analyzer::{parser::parse_one_statement, statement::from_ast_statement};

use crate::error::{QueryError, QueryResult};

pub fn plan_sql(sql: &str) -> QueryResult<Plan> {
    println!("=====begin===");
    let tree = parse_one_statement(sql).map_err(|x| {
        QueryError::SqlParserError(format!("Failed to parse sql statement with error: {}", x))
    })?;
    println!("---->>>>{:?}", &tree);
    let plan = from_ast_statement(tree).map_err(|x| {
        QueryError::CreatePlanError(format!("Failed to create plan with error: {}", x))
    })?;
    Ok(plan)
}
