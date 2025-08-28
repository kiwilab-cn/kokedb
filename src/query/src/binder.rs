use kokedb_common::spec::Plan;
use kokedb_sql_analyzer::{
    error::SqlResult, parser::parse_one_statement, statement::from_ast_statement,
};

pub fn plan_sql(sql: &str) -> SqlResult<Plan> {
    println!("=====begin===");
    let tree = parse_one_statement(sql)?;
    println!("---->>>>{:?}", &tree);
    let plan = from_ast_statement(tree)?;
    Ok(plan)
}
