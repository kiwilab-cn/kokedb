use datafusion_expr::LogicalPlan;
use kokedb_catalog::command::CatalogCommand;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_cache_show_policies(&self) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::ListCachePolicies)
    }
}
