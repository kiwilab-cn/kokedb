use kokedb_common::spec::Plan;
use kokedb_sql_analyzer::{parser::parse_one_statement, statement::from_ast_statement};

use crate::error::QueryResult;

pub fn sql_parser(sql: &str) -> QueryResult<Plan> {
    println!("=====begin===");
    let tree = parse_one_statement(sql)?;
    println!("---->>>>{:?}", &tree);
    let plan = from_ast_statement(tree)?;
    Ok(plan)
}
