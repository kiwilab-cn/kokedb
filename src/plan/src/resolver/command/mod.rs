use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use kokedb_catalog::command::CatalogCommand;
use kokedb_catalog::provider::{DropDatabaseOptions, DropTableOptions};
use kokedb_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::extension::logical::CatalogCommandNode;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use kokedb_catalog::provider::RemoteDatabaseType;

mod cache;
mod catalog;
mod explain;
mod function;
mod insert;
mod show;
mod variable;
mod write;
mod write_v1;
mod write_v2;

/// kokedb caches read-only snapshots of source tables; mutating a cached table
/// here would corrupt the snapshot (and never reach the source). DML must go to
/// the source database, which the cache then picks up on its next sync. This is
/// the single, intentional rejection message for all table-targeting DML.
pub(crate) fn write_not_supported(op: &str) -> PlanError {
    PlanError::unsupported(format!(
        "{op} is not supported: kokedb is a read-only query accelerator. \
         Run the write against the source database directly; the cached \
         snapshot refreshes automatically."
    ))
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_plan(
        &self,
        plan: spec::CommandPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::CommandNode;

        match plan.node {
            CommandNode::ShowString(show) => self.resolve_command_show_string(show, state).await,
            CommandNode::HtmlString(html) => self.resolve_command_html_string(html, state).await,
            CommandNode::CurrentDatabase => {
                self.resolve_catalog_command(CatalogCommand::CurrentDatabase)
            }
            CommandNode::SetCurrentDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentDatabase {
                    database: database.into(),
                })
            }
            CommandNode::ListDatabases { qualifier, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListDatabases {
                    qualifier: qualifier.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListViews { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListViews {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListFunctions { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListFunctions {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: table.into(),
                })
            }
            CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: database.into(),
                })
            }
            CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: table.into(),
                })
            }
            CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: function.into(),
                })
            }
            CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: database.into(),
                })
            }
            CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: table.into(),
                })
            }
            CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: function.into(),
                })
            }
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition, state)
                    .await
            }
            CommandNode::CreateTableAsSelect {
                table,
                definition,
                query,
            } => {
                self.resolve_catalog_create_table_as_select(table, definition, *query, state)
                    .await
            }
            CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_drop_view(view, if_exists).await
            }
            CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => {
                self.resolve_catalog_drop_temporary_view(view, is_global, if_exists)
                    .await
            }
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: database.into(),
                options: DropDatabaseOptions { if_exists, cascade },
            }),
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: function.into(),
                if_exists,
                is_temporary,
            }),
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: table.into(),
                options: DropTableOptions { if_exists, purge },
            }),
            CommandNode::RecoverPartitions { .. } => {
                Err(PlanError::todo("PlanNode::RecoverPartitions"))
            }
            CommandNode::IsCached { .. } => Err(PlanError::todo("PlanNode::IsCached")),
            CommandNode::CacheTable { .. } => Err(PlanError::todo("PlanNode::CacheTable")),
            CommandNode::UncacheTable { .. } => Err(PlanError::todo("PlanNode::UncacheTable")),
            CommandNode::ClearCache => Err(PlanError::todo("PlanNode::ClearCache")),
            CommandNode::RefreshTable { .. } => Err(PlanError::todo("PlanNode::RefreshTable")),
            CommandNode::RefreshByPath { .. } => Err(PlanError::todo("PlanNode::RefreshByPath")),
            CommandNode::CurrentCatalog => {
                self.resolve_catalog_command(CatalogCommand::CurrentCatalog)
            }
            CommandNode::SetCurrentCatalog { catalog } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentCatalog {
                    catalog: catalog.into(),
                })
            }
            CommandNode::ListCatalogs { pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListCatalogs { pattern })
            }
            CommandNode::CreateCatalog {
                catalog,
                definition,
            } => {
                let dsn = definition.dsn;
                let db_type = if dsn.starts_with("postgresql://") {
                    RemoteDatabaseType::PostgreSQL
                } else if dsn.starts_with("mysql://") {
                    RemoteDatabaseType::Mysql
                } else {
                    RemoteDatabaseType::Oracle
                };

                let options = kokedb_catalog::provider::CreateCatalogOptions {
                    db_type,
                    dsn,
                    comment: definition.comment,
                    properties: definition.properties,
                };
                let catalog = String::from(catalog);
                self.resolve_catalog_command(CatalogCommand::CreateCatalog { catalog, options })
            }
            CommandNode::DropCatalog {
                catalog,
                if_exists,
            } => self.resolve_catalog_command(CatalogCommand::DropCatalog {
                catalog: String::from(catalog),
                if_exists,
            }),
            CommandNode::CreateDatabase {
                database,
                definition,
            } => self.resolve_catalog_create_database(database, definition),
            CommandNode::RegisterFunction(function) => {
                self.resolve_catalog_register_function(function, state)
            }
            CommandNode::RegisterTableFunction(function) => {
                self.resolve_catalog_register_table_function(function, state)
            }
            CommandNode::RefreshFunction { .. } => {
                Err(PlanError::todo("CommandNode::RefreshFunction"))
            }
            CommandNode::CreateView { view, definition } => {
                self.resolve_catalog_create_view(view, definition, state)
                    .await
            }
            CommandNode::CreateTemporaryView {
                view,
                is_global,
                definition,
            } => {
                self.resolve_catalog_create_temporary_view(view, is_global, definition, state)
                    .await
            }
            CommandNode::Write(write) => self.resolve_command_write(write, state).await,
            CommandNode::WriteTo(write_to) => self.resolve_command_write_to(write_to, state).await,
            CommandNode::Explain { mode, input } => {
                self.resolve_command_explain(*input, mode, state).await
            }
            CommandNode::InsertOverwriteDirectory {
                input,
                local,
                location,
                file_format,
                row_format,
                options,
            } => {
                self.resolve_command_insert_overwrite_directory(
                    *input,
                    local,
                    location,
                    file_format,
                    row_format,
                    options,
                    state,
                )
                .await
            }
            CommandNode::InsertInto {
                input,
                table,
                mode,
                partition,
                if_not_exists,
            } => {
                self.resolve_command_insert_into(
                    *input,
                    table,
                    mode,
                    partition,
                    if_not_exists,
                    state,
                )
                .await
            }
            CommandNode::MergeInto { .. } => Err(write_not_supported("MERGE")),
            CommandNode::SetVariable { variable, value } => {
                self.resolve_command_set_variable(variable, value).await
            }
            CommandNode::Update { .. } => Err(write_not_supported("UPDATE")),
            CommandNode::Delete { .. } => Err(write_not_supported("DELETE")),
            CommandNode::AlterTable { .. } => Err(PlanError::todo("CommandNode::AlterTable")),
            CommandNode::AlterView { .. } => Err(PlanError::todo("CommandNode::AlterView")),
            CommandNode::LoadData { .. } => Err(PlanError::todo("CommandNode::LoadData")),
            CommandNode::AnalyzeTable { .. } => Err(PlanError::todo("CommandNode::AnalyzeTable")),
            CommandNode::AnalyzeTables { .. } => Err(PlanError::todo("CommandNode::AnalyzeTables")),
            CommandNode::DescribeQuery { .. } => Err(PlanError::todo("CommandNode::DescribeQuery")),
            CommandNode::DescribeFunction { .. } => {
                Err(PlanError::todo("CommandNode::DescribeFunction"))
            }
            CommandNode::DescribeCatalog { .. } => {
                Err(PlanError::todo("CommandNode::DescribeCatalog"))
            }
            CommandNode::DescribeDatabase { .. } => {
                Err(PlanError::todo("CommandNode::DescribeDatabase"))
            }
            CommandNode::DescribeTable { .. } => Err(PlanError::todo("CommandNode::DescribeTable")),
            CommandNode::CommentOnCatalog { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnCatalog"))
            }
            CommandNode::CommentOnDatabase { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnDatabase"))
            }
            CommandNode::CommentOnTable { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnTable"))
            }
            CommandNode::CommentOnColumn { .. } => {
                Err(PlanError::todo("CommandNode::CommentOnColumn"))
            }
            CommandNode::ListCachePolicies => self.resolve_cache_show_policies(),
            CommandNode::RefreshCache { table } => self.resolve_cache_refresh(table),
            CommandNode::RefreshCacheCatalog { catalog } => {
                self.resolve_cache_refresh_catalog(catalog)
            }
            CommandNode::ShowTableMetadata { table } => self.resolve_show_table_metadata(table),
            CommandNode::AlterTableCachePolicy { table, options } => {
                self.resolve_alter_table_cache_policy(table, options)
            }
            CommandNode::ShowCacheJobs { table, catalog } => {
                self.resolve_show_cache_jobs(table, catalog)
            }
            CommandNode::SetTablePaused { table, paused } => {
                self.resolve_set_table_paused(table, paused)
            }
            CommandNode::ShowCacheSchedule { table, catalog } => {
                self.resolve_show_cache_schedule(table, catalog)
            }
            CommandNode::DiagnoseCache { catalog } => self.resolve_diagnose_cache(catalog),
            CommandNode::AlterCatalogCachePolicy { catalog, options } => {
                self.resolve_alter_catalog_cache_policy(catalog, options)
            }
            CommandNode::AlterDatabaseCachePolicy { database, options } => {
                self.resolve_alter_database_cache_policy(database, options)
            }
            CommandNode::CreateUser { username, options } => {
                self.resolve_catalog_command(CatalogCommand::CreateUser {
                    username: username.into(),
                    options,
                })
            }
            CommandNode::DropUser {
                username,
                if_exists,
            } => self.resolve_catalog_command(CatalogCommand::DropUser {
                username: username.into(),
                if_exists,
            }),
            CommandNode::ListUsers { pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListUsers { pattern })
            }
            CommandNode::GrantScope { scope, username } => {
                self.resolve_catalog_command(CatalogCommand::GrantScope {
                    scope: scope.into(),
                    username: username.into(),
                })
            }
            CommandNode::RevokeScope { scope, username } => {
                self.resolve_catalog_command(CatalogCommand::RevokeScope {
                    scope: scope.into(),
                    username: username.into(),
                })
            }
            CommandNode::CreateRowPolicy {
                table,
                username,
                filter,
            } => self.resolve_catalog_command(CatalogCommand::CreateRowPolicy {
                table: table.into(),
                username: username.into(),
                filter,
            }),
            CommandNode::DropRowPolicy { table, username } => {
                self.resolve_catalog_command(CatalogCommand::DropRowPolicy {
                    table: table.into(),
                    username: username.into(),
                })
            }
            CommandNode::ListRowPolicies => {
                self.resolve_catalog_command(CatalogCommand::ListRowPolicies)
            }
            CommandNode::CreateColumnPolicy {
                table,
                username,
                columns,
            } => self.resolve_catalog_command(CatalogCommand::CreateColumnPolicy {
                table: table.into(),
                username: username.into(),
                columns,
            }),
            CommandNode::DropColumnPolicy { table, username } => {
                self.resolve_catalog_command(CatalogCommand::DropColumnPolicy {
                    table: table.into(),
                    username: username.into(),
                })
            }
            CommandNode::ListColumnPolicies => {
                self.resolve_catalog_command(CatalogCommand::ListColumnPolicies)
            }
        }
    }

    fn resolve_catalog_command(&self, command: CatalogCommand) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(command, self.config.clone())?),
        }))
    }
}
