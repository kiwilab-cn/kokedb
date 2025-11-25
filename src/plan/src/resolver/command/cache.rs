use datafusion_expr::LogicalPlan;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_cache_show_policies(&self) -> PlanResult<LogicalPlan> {}
}
