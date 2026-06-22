use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use datafusion_expr::ScalarUDF;
use serde::Serialize;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_arrow;

use crate::display::{
    CacheJobDisplay, CachePolicyDisplay, CacheScheduleDisplay, CatalogDisplay, DiagnoseDisplay,
    EmptyDisplay, RefreshCacheDisplay, SingleValueDisplay, TableMetadataDisplay,
};
use crate::error::{CatalogError, CatalogResult};
use crate::manager::CatalogManager;
use crate::provider::{
    CreateCatalogOptions, CreateDatabaseOptions, CreateTableOptions, CreateTemporaryViewOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropTemporaryViewOptions,
    DropViewOptions, FunctionStatus,
};
use crate::utils::{match_pattern, quote_namespace_if_needed};

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum CatalogCommand {
    CurrentCatalog,
    SetCurrentCatalog {
        catalog: String,
    },
    CreateCatalog {
        catalog: String,
        options: CreateCatalogOptions,
    },
    ListCatalogs {
        pattern: Option<String>,
    },
    DropCatalog {
        catalog: String,
        if_exists: bool,
    },
    ListCachePolicies,
    RefreshCache {
        table: Vec<String>,
    },
    ShowTableMetadata {
        table: Vec<String>,
    },
    RefreshCacheCatalog {
        catalog: String,
    },
    SetTablePaused {
        table: Vec<String>,
        paused: bool,
    },
    ShowCacheSchedule {
        table: Option<Vec<String>>,
        catalog: Option<String>,
    },
    DiagnoseCache {
        catalog: Option<String>,
    },
    AlterTableCachePolicy {
        table: Vec<String>,
        options: Vec<(String, String)>,
    },
    ShowCacheJobs {
        table: Option<Vec<String>>,
        catalog: Option<String>,
    },
    AlterCatalogCachePolicy {
        catalog: String,
        options: Vec<(String, String)>,
    },
    AlterDatabaseCachePolicy {
        database: Vec<String>,
        options: Vec<(String, String)>,
    },
    CurrentDatabase,
    SetCurrentDatabase {
        database: Vec<String>,
    },
    CreateDatabase {
        database: Vec<String>,
        options: CreateDatabaseOptions,
    },
    DatabaseExists {
        database: Vec<String>,
    },
    GetDatabase {
        database: Vec<String>,
    },
    ListDatabases {
        qualifier: Vec<String>,
        pattern: Option<String>,
    },
    DropDatabase {
        database: Vec<String>,
        options: DropDatabaseOptions,
    },
    CreateTable {
        table: Vec<String>,
        options: CreateTableOptions,
    },
    TableExists {
        table: Vec<String>,
    },
    GetTable {
        table: Vec<String>,
    },
    ListTables {
        database: Vec<String>,
        pattern: Option<String>,
    },
    ListViews {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropTable {
        table: Vec<String>,
        options: DropTableOptions,
    },
    ListColumns {
        table: Vec<String>,
    },
    FunctionExists {
        function: Vec<String>,
    },
    GetFunction {
        function: Vec<String>,
    },
    ListFunctions {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropFunction {
        function: Vec<String>,
        if_exists: bool,
        is_temporary: bool,
    },
    RegisterFunction {
        udf: ScalarUDF,
    },
    #[allow(unused)]
    RegisterTableFunction {
        name: String,
        // We have to be explicit about the UDTF types we support.
        // We cannot use `Arc<dyn TableFunctionImpl>` because it does not implement `Eq` and `Hash`.
        udtf: CatalogTableFunction,
    },
    DropTemporaryView {
        view: String,
        is_global: bool,
        options: DropTemporaryViewOptions,
    },
    DropView {
        view: Vec<String>,
        options: DropViewOptions,
    },
    CreateTemporaryView {
        view: String,
        is_global: bool,
        options: CreateTemporaryViewOptions,
    },
    CreateView {
        view: Vec<String>,
        options: CreateViewOptions,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub enum CatalogTableFunction {
    // We do not support any kind of table functions yet.
    // PySpark UDTF is registered as a scalar UDF.
}

impl CatalogCommand {
    pub fn name(&self) -> &str {
        match self {
            CatalogCommand::CurrentCatalog => "CurrentCatalog",
            CatalogCommand::SetCurrentCatalog { .. } => "SetCurrentCatalog",
            CatalogCommand::ListCatalogs { .. } => "ListCatalogs",
            CatalogCommand::DropCatalog { .. } => "DropCatalog",
            CatalogCommand::ListCachePolicies => "ListCachePolicies",
            CatalogCommand::RefreshCache { .. } => "RefreshCache",
            CatalogCommand::RefreshCacheCatalog { .. } => "RefreshCacheCatalog",
            CatalogCommand::SetTablePaused { .. } => "SetTablePaused",
            CatalogCommand::ShowCacheSchedule { .. } => "ShowCacheSchedule",
            CatalogCommand::DiagnoseCache { .. } => "DiagnoseCache",
            CatalogCommand::ShowTableMetadata { .. } => "ShowTableMetadata",
            CatalogCommand::AlterTableCachePolicy { .. } => "AlterTableCachePolicy",
            CatalogCommand::ShowCacheJobs { .. } => "ShowCacheJobs",
            CatalogCommand::AlterCatalogCachePolicy { .. } => "AlterCatalogCachePolicy",
            CatalogCommand::AlterDatabaseCachePolicy { .. } => "AlterDatabaseCachePolicy",
            CatalogCommand::CurrentDatabase => "CurrentDatabase",
            CatalogCommand::CreateCatalog { .. } => "CreateCatalog",
            CatalogCommand::SetCurrentDatabase { .. } => "SetCurrentDatabase",
            CatalogCommand::CreateDatabase { .. } => "CreateDatabase",
            CatalogCommand::DatabaseExists { .. } => "DatabaseExists",
            CatalogCommand::GetDatabase { .. } => "GetDatabase",
            CatalogCommand::ListDatabases { .. } => "ListDatabases",
            CatalogCommand::DropDatabase { .. } => "DropDatabase",
            CatalogCommand::CreateTable { .. } => "CreateTable",
            CatalogCommand::TableExists { .. } => "TableExists",
            CatalogCommand::GetTable { .. } => "GetTable",
            CatalogCommand::ListTables { .. } => "ListTables",
            CatalogCommand::ListViews { .. } => "ListViews",
            CatalogCommand::DropTable { .. } => "DropTable",
            CatalogCommand::ListColumns { .. } => "ListColumns",
            CatalogCommand::FunctionExists { .. } => "FunctionExists",
            CatalogCommand::GetFunction { .. } => "GetFunction",
            CatalogCommand::ListFunctions { .. } => "ListFunctions",
            CatalogCommand::RegisterFunction { .. } => "RegisterFunction",
            CatalogCommand::RegisterTableFunction { .. } => "RegisterTableFunction",
            CatalogCommand::DropFunction { .. } => "DropFunction",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropView { .. } => "DropView",
            CatalogCommand::CreateTemporaryView { .. } => "CreateTemporaryView",
            CatalogCommand::CreateView { .. } => "CreateView",
        }
    }

    pub fn schema<D: CatalogDisplay>(&self) -> CatalogResult<SchemaRef> {
        let fields = match self {
            CatalogCommand::ListCatalogs { .. } => {
                Vec::<FieldRef>::from_type::<D::Catalog>(TracingOptions::default())
            }
            CatalogCommand::ListCachePolicies => {
                Vec::<FieldRef>::from_type::<CachePolicyDisplay>(TracingOptions::default())
            }
            CatalogCommand::RefreshCache { .. } | CatalogCommand::RefreshCacheCatalog { .. } => {
                Vec::<FieldRef>::from_type::<RefreshCacheDisplay>(TracingOptions::default())
            }
            CatalogCommand::ShowTableMetadata { .. } => {
                Vec::<FieldRef>::from_type::<TableMetadataDisplay>(TracingOptions::default())
            }
            CatalogCommand::ShowCacheJobs { .. } => {
                Vec::<FieldRef>::from_type::<CacheJobDisplay>(TracingOptions::default())
            }
            CatalogCommand::ShowCacheSchedule { .. } => {
                Vec::<FieldRef>::from_type::<CacheScheduleDisplay>(TracingOptions::default())
            }
            CatalogCommand::DiagnoseCache { .. } => {
                Vec::<FieldRef>::from_type::<DiagnoseDisplay>(TracingOptions::default())
            }
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                Vec::<FieldRef>::from_type::<D::Database>(TracingOptions::default())
            }
            CatalogCommand::GetTable { .. }
            | CatalogCommand::ListTables { .. }
            | CatalogCommand::ListViews { .. } => {
                Vec::<FieldRef>::from_type::<D::Table>(TracingOptions::default())
            }
            CatalogCommand::ListColumns { .. } => {
                Vec::<FieldRef>::from_type::<D::TableColumn>(TracingOptions::default())
            }
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                Vec::<FieldRef>::from_type::<D::Function>(TracingOptions::default())
            }
            CatalogCommand::SetCurrentCatalog { .. }
            | CatalogCommand::SetCurrentDatabase { .. }
            | CatalogCommand::RegisterFunction { .. }
            | CatalogCommand::RegisterTableFunction { .. } => {
                Vec::<FieldRef>::from_type::<EmptyDisplay>(TracingOptions::default())
            }
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                Vec::<FieldRef>::from_type::<SingleValueDisplay<String>>(TracingOptions::default())
            }
            CatalogCommand::DatabaseExists { .. }
            | CatalogCommand::TableExists { .. }
            | CatalogCommand::FunctionExists { .. }
            | CatalogCommand::CreateCatalog { .. }
            | CatalogCommand::CreateDatabase { .. }
            | CatalogCommand::CreateTable { .. }
            | CatalogCommand::CreateTemporaryView { .. }
            | CatalogCommand::CreateView { .. }
            | CatalogCommand::DropDatabase { .. }
            | CatalogCommand::DropTable { .. }
            | CatalogCommand::DropFunction { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropView { .. }
            | CatalogCommand::AlterTableCachePolicy { .. }
            | CatalogCommand::AlterCatalogCachePolicy { .. }
            | CatalogCommand::AlterDatabaseCachePolicy { .. }
            | CatalogCommand::SetTablePaused { .. }
            | CatalogCommand::DropCatalog { .. } => {
                Vec::<FieldRef>::from_type::<SingleValueDisplay<bool>>(TracingOptions::default())
            }
        }
        .map_err(|e| {
            CatalogError::Internal(format!("failed to build catalog command schema: {e}"))
        })?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub async fn execute<D: CatalogDisplay>(
        self,
        ctx: &SessionContext,
        manager: &CatalogManager,
    ) -> CatalogResult<RecordBatch> {
        // TODO: make sure we return the same schema as Spark for each command
        // TODO: `ctx` will not be needed if `CatalogManager` manages functions internally.
        let schema = self.schema::<D>()?;
        match self {
            CatalogCommand::CurrentCatalog => {
                let value = manager.default_catalog()?;
                let rows = vec![SingleValueDisplay { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::SetCurrentCatalog { catalog } => {
                manager.set_default_catalog(catalog)?;
                let rows: Vec<EmptyDisplay> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateCatalog { catalog, options } => {
                manager.create_catalog(catalog, options)?;

                let rows: Vec<EmptyDisplay> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListCatalogs { pattern } => {
                let rows = manager
                    .list_catalogs(pattern.as_deref())?
                    .into_iter()
                    .map(|x| D::catalog(x.to_string()))
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListCachePolicies => {
                let rows = manager.list_cache_policies().await?;
                build_record_batch(schema, &rows)
            }
            CatalogCommand::RefreshCache { table } => {
                let row = manager.refresh_table(table).await?;
                build_record_batch(schema, &[row])
            }
            CatalogCommand::RefreshCacheCatalog { catalog } => {
                let rows = manager.refresh_catalog(&catalog).await?;
                build_record_batch(schema, &rows)
            }
            CatalogCommand::SetTablePaused { table, paused } => {
                manager.set_table_paused(table, paused).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ShowTableMetadata { table } => {
                let row = manager.show_table_metadata(table).await?;
                build_record_batch(schema, &[row])
            }
            CatalogCommand::AlterTableCachePolicy { table, options } => {
                manager.set_table_cache_policy(table, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ShowCacheJobs { table, catalog } => {
                let rows = manager.show_cache_jobs(table, catalog).await?;
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ShowCacheSchedule { table, catalog } => {
                let rows = manager.show_cache_schedule(table, catalog).await?;
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DiagnoseCache { catalog } => {
                let rows = manager.diagnose_cache(catalog).await?;
                build_record_batch(schema, &rows)
            }
            CatalogCommand::AlterCatalogCachePolicy { catalog, options } => {
                manager.set_catalog_cache_policy(&catalog, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::AlterDatabaseCachePolicy { database, options } => {
                manager.set_database_cache_policy(database, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropCatalog { catalog, if_exists } => {
                manager.drop_catalog(&catalog, if_exists).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CurrentDatabase => {
                let value = manager.default_database()?;
                let value = quote_namespace_if_needed(&value);
                let rows = vec![SingleValueDisplay { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::SetCurrentDatabase { database } => {
                manager.set_default_database(database).await?;
                let rows: Vec<EmptyDisplay> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateDatabase { database, options } => {
                manager.create_database(&database, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = match manager.get_database(&database).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                let rows = vec![SingleValueDisplay { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match manager.get_database(&database).await {
                    Ok(x) => vec![D::database(x)],
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListDatabases { qualifier, pattern } => {
                let rows = match manager.list_databases(&qualifier, pattern.as_deref()).await {
                    Ok(rows) => rows.into_iter().map(D::database).collect::<Vec<_>>(),
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropDatabase { database, options } => {
                manager.drop_database(&database, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateTable { table, options } => {
                manager.create_table(&table, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::TableExists { table } => {
                let value = match manager.get_table_or_view(&table).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                let rows = vec![SingleValueDisplay { value }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::GetTable { table } => {
                // We are supposed to return an error if the table or view does not exist.
                let table = manager.get_table_or_view(&table).await?;
                let table = D::table(table);
                build_record_batch(schema, &[table])
            }
            CatalogCommand::ListTables { database, pattern } => {
                let rows = manager
                    .list_tables_and_temporary_views(&database, pattern.as_deref())
                    .await?
                    .into_iter()
                    .map(D::table)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListViews { database, pattern } => {
                let rows = manager
                    .list_views_and_temporary_views(&database, pattern.as_deref())
                    .await?
                    .into_iter()
                    .map(D::table)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropTable { table, options } => {
                manager.drop_table(&table, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::ListColumns { table } => {
                let rows = manager
                    .get_table_or_view(&table)
                    .await?
                    .kind
                    .columns()
                    .into_iter()
                    .map(D::table_column)
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::FunctionExists { function } => {
                let target = function.last().cloned().unwrap_or_default();
                let exists = session_function_names(ctx)
                    .iter()
                    .any(|n| n.eq_ignore_ascii_case(&target));
                let rows = vec![SingleValueDisplay { value: exists }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::GetFunction { function } => {
                let target = function.last().cloned().unwrap_or_default();
                match session_function_names(ctx)
                    .into_iter()
                    .find(|n| n.eq_ignore_ascii_case(&target))
                {
                    Some(name) => {
                        let rows = vec![D::function(function_status(name))];
                        build_record_batch(schema, &rows)
                    }
                    None => Err(CatalogError::NotFound("function", target)),
                }
            }
            CatalogCommand::ListFunctions { pattern, .. } => {
                let rows = session_function_names(ctx)
                    .into_iter()
                    .filter(|n| match_pattern(n, pattern.as_deref()))
                    .map(|name| D::function(function_status(name)))
                    .collect::<Vec<_>>();
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => {
                manager
                    .deregister_function(ctx, &function, if_exists, is_temporary)
                    .await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::RegisterFunction { udf } => {
                manager.register_function(ctx, udf)?;
                let rows: Vec<EmptyDisplay> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::RegisterTableFunction { name, udtf } => {
                manager.register_table_function(ctx, name, udtf)?;
                let rows: Vec<EmptyDisplay> = vec![];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropTemporaryView {
                view,
                is_global,
                options,
            } => {
                if is_global {
                    manager.drop_global_temporary_view(&view, options).await?;
                } else {
                    manager.drop_temporary_view(&view, options).await?;
                }
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::DropView { view, options } => {
                manager.drop_maybe_temporary_view(&view, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateTemporaryView {
                view,
                is_global,
                options,
            } => {
                if is_global {
                    manager.create_global_temporary_view(&view, options).await?;
                } else {
                    manager.create_temporary_view(&view, options).await?;
                }
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
            CatalogCommand::CreateView { view, options } => {
                manager.create_view(&view, options).await?;
                let rows = vec![SingleValueDisplay { value: true }];
                build_record_batch(schema, &rows)
            }
        }
    }
}

/// Distinct, sorted names of all functions available in the session
/// (built-in + registered scalar / aggregate / window functions).
fn session_function_names(ctx: &SessionContext) -> Vec<String> {
    let state = ctx.state();
    let mut names: Vec<String> = state
        .scalar_functions()
        .keys()
        .chain(state.aggregate_functions().keys())
        .chain(state.window_functions().keys())
        .cloned()
        .collect();
    names.sort();
    names.dedup();
    names
}

fn function_status(name: String) -> FunctionStatus {
    FunctionStatus {
        catalog: None,
        namespace: None,
        description: None,
        class_name: name.clone(),
        name,
        is_temporary: false,
    }
}

fn build_record_batch<T: Serialize>(schema: SchemaRef, items: &[T]) -> CatalogResult<RecordBatch> {
    let arrays = to_arrow(schema.fields().deref(), items)
        .map_err(|e| CatalogError::Internal(format!("failed to create record batch: {e}")))?;
    // We must specify the row count if the schema has no fields.
    let options = RecordBatchOptions::new().with_row_count(Some(items.len()));
    let batch = RecordBatch::try_new_with_options(schema, arrays, &options)
        .map_err(|e| CatalogError::Internal(format!("failed to create record batch: {e}")))?;
    Ok(batch)
}
