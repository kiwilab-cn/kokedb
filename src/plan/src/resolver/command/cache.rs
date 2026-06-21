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

    pub(in super::super) fn resolve_cache_refresh_catalog(
        &self,
        catalog: spec::ObjectName,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::RefreshCacheCatalog {
            catalog: <Vec<String>>::from(catalog).join("."),
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

    pub(in super::super) fn resolve_alter_catalog_cache_policy(
        &self,
        catalog: spec::ObjectName,
        options: Vec<(String, String)>,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::AlterCatalogCachePolicy {
            catalog: <Vec<String>>::from(catalog).join("."),
            options,
        })
    }

    pub(in super::super) fn resolve_alter_database_cache_policy(
        &self,
        database: spec::ObjectName,
        options: Vec<(String, String)>,
    ) -> PlanResult<LogicalPlan> {
        self.resolve_catalog_command(CatalogCommand::AlterDatabaseCachePolicy {
            database: database.into(),
            options,
        })
    }
}
