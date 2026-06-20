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

    pub(in super::super) fn resolve_show_table_metadata(
        &self,
        table: spec::ObjectName,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::ShowTableMetadata {
            table: table.into(),
        })
    }

    pub(in super::super) fn resolve_alter_table_cache_policy(
        &self,
        table: spec::ObjectName,
        options: Vec<(String, String)>,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::AlterTableCachePolicy {
            table: table.into(),
            options,
        })
    }

    pub(in super::super) fn resolve_show_cache_jobs(
        &self,
        table: Option<spec::ObjectName>,
        catalog: Option<spec::ObjectName>,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::ShowCacheJobs {
            table: table.map(|t| t.into()),
            catalog: catalog.map(|c| <Vec<String>>::from(c).join(".")),
        })
    }
}
