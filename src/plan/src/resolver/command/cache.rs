use datafusion_expr::LogicalPlan;
use kokedb_catalog::command::CatalogCommand;
use kokedb_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_cache_show_policies(&self) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::ListCachePolicies)
    }

    pub(in super::super) fn resolve_cache_refresh(
        &self,
        table: spec::ObjectName,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::RefreshCache {
            table: table.into(),
        })
    }
}
