use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod command;
mod constraint;
mod data_type;
mod expression;
mod function;
mod literal;
pub mod plan;
mod query;
mod schema;
mod state;
mod tree;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    cache_key: u64,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>, cache_key: u64) -> Self {
        Self {
            ctx,
            config,
            cache_key,
        }
    }
}
