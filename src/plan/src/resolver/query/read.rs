use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::provider_as_source;
use datafusion_common::DFSchema;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{LogicalPlan, TableScan, UNNAMED_TABLE};
use kokedb_catalog::manager::CatalogManager;
use kokedb_catalog::provider::TableKind;
use kokedb_common::spec;
use kokedb_common_datafusion::datasource::SourceInfo;
use kokedb_common_datafusion::extension::SessionExtensionAccessor;
use kokedb_common_datafusion::utils::rename_logical_plan;
use kokedb_common::env::get_env_as;
use kokedb_data_source::default_registry;
use kokedb_data_source::formats::hybrid_table::HybridTableProvider;
use kokedb_data_source::formats::remote_table::{PostgreSQLConfig, PostgreSQLTableProvider};
use kokedb_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;
use log::error;

use crate::error::{PlanError, PlanResult};
use crate::extension::source::rename::RenameTableProvider;
use crate::function::{get_built_in_table_function, is_built_in_generator_function};
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_read_named_table(
        &self,
        table: spec::ReadNamedTable,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadNamedTable {
            name,
            temporal,
            sample,
            options,
        } = table;
        if temporal.is_some() {
            return Err(PlanError::todo("read table AS OF clause"));
        }
        if sample.is_some() {
            return Err(PlanError::todo("read table TABLESAMPLE clause"));
        }

        let table_reference = self.resolve_table_reference(&name)?;
        if let Some(cte) = state.get_cte(&table_reference) {
            return Ok(cte.clone());
        }

        let reference: Vec<String> = name.clone().into();
        let ret = self
            .ctx
            .extension::<CatalogManager>()?
            .save_hash_key(&reference, self.cache_key)
            .await;
        if ret.is_err() {
            error!(
                "Failed to save table:{:?} hash key with error: {:?}",
                &reference,
                ret.err()
            );
        }

        let status = self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&reference)
            .await?;
        let plan = match status.kind {
            TableKind::Table {
                catalog: _,
                database,
                columns,
                comment: _,
                constraints,
                format,
                location,
                partition_by,
                sort_by,
                bucket_by,
                options: table_options,
                properties: _,
                dsn,
                is_cached,
            } => {
                let schema = Schema::new(columns.iter().map(|x| x.field()).collect::<Vec<_>>());
                let constraints = self.resolve_catalog_table_constraints(constraints, &schema)?;
                let info = SourceInfo {
                    paths: location.clone().map(|x| vec![x]).unwrap_or_default(),
                    schema: Some(schema),
                    constraints,
                    partition_by,
                    bucket_by: bucket_by.map(|x| x.into()),
                    sort_order: sort_by.into_iter().map(|x| x.into()).collect(),
                    // TODO: detect duplicated keys in each set of options
                    options: vec![
                        table_options.into_iter().collect(),
                        options.into_iter().collect(),
                    ],
                };

                // Route to the live source when the table isn't cached, or when
                // it IS cached but the caller demanded freshness this query
                // (`max_staleness_secs`) and the snapshot is older than that.
                let read_remote = if !is_cached {
                    dsn.is_some()
                } else {
                    match self.config.max_staleness_secs {
                        Some(max) if dsn.is_some() => {
                            self.cached_snapshot_is_stale(&reference, max).await
                        }
                        _ => false,
                    }
                };

                let schema_name = if database.is_empty() {
                    None
                } else {
                    Some(database.first().unwrap().clone())
                };
                let table_provider: Arc<dyn datafusion::catalog::TableProvider> = if read_remote {
                    let config = PostgreSQLConfig {
                        connection_string: dsn.clone().unwrap(),
                        table_name: table_reference.table().to_string(),
                        schema_name,
                    };
                    let remote_table = PostgreSQLTableProvider::new(config).await?;
                    Arc::new(remote_table)
                } else {
                    let cached = default_registry()
                        .get_format(&format)?
                        .create_provider(&self.ctx.state(), info)
                        .await?;
                    // Opt-in cost-based routing: a big, primary-keyed cached table
                    // is wrapped so a point-lookup scan can go to the live source.
                    if is_cached && dsn.is_some() && get_env_as("KOKEDB_COST_BASED_ROUTING", false)
                    {
                        self.maybe_cost_routing_provider(
                            cached,
                            &reference,
                            dsn.unwrap(),
                            schema_name,
                            table_reference.table(),
                        )
                        .await
                    } else {
                        cached
                    }
                };

                let names = state.register_fields(table_provider.schema().fields());
                let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                LogicalPlan::TableScan(TableScan::try_new(
                    table_reference,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?)
            }
            TableKind::View { .. } => return Err(PlanError::todo("read view")),
            TableKind::TemporaryView { plan, .. } | TableKind::GlobalTemporaryView { plan, .. } => {
                let names = state.register_fields(plan.schema().inner().fields());
                rename_logical_plan(plan.as_ref().clone(), &names)?
            }
        };
        Ok(plan)
    }

    /// Wraps a cached table provider so point-lookup scans can route to the live
    /// source (opt-in cost-based routing). Only wraps when the table is large
    /// enough and has primary-key columns to route on; otherwise returns the
    /// snapshot provider unchanged so nothing is paid for tables that can't
    /// benefit.
    async fn maybe_cost_routing_provider(
        &self,
        cached: Arc<dyn datafusion::catalog::TableProvider>,
        reference: &[String],
        dsn: String,
        schema_name: Option<String>,
        table_name: &str,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        let Ok(manager) = self.ctx.extension::<CatalogManager>() else {
            return cached;
        };
        let (est_rows, pk_columns) = manager
            .get_table_routing_info(reference)
            .await
            .unwrap_or((None, Vec::new()));
        let min_rows = get_env_as("KOKEDB_COST_ROUTING_MIN_ROWS", 100_000usize);
        if pk_columns.is_empty() || est_rows.map(|n| n < min_rows).unwrap_or(true) {
            return cached;
        }
        let pg_config = PostgreSQLConfig {
            connection_string: dsn,
            table_name: table_name.to_string(),
            schema_name,
        };
        Arc::new(HybridTableProvider::new(
            cached, pg_config, pk_columns, est_rows, min_rows,
        ))
    }

    /// Whether a cached table's snapshot is older than `max_secs` and should be
    /// read live instead. A never-synced table counts as stale (no trustworthy
    /// snapshot). If the timestamp can't be read (e.g. meta hiccup) we keep
    /// serving the snapshot rather than stampede the source.
    async fn cached_snapshot_is_stale(&self, reference: &[String], max_secs: u64) -> bool {
        let manager = match self.ctx.extension::<CatalogManager>() {
            Ok(m) => m,
            Err(_) => return false,
        };
        match manager.get_table_last_sync_at(reference).await {
            Ok(Some(last_sync_at)) => {
                let age = chrono::Utc::now().signed_duration_since(last_sync_at);
                age.num_seconds() > max_secs as i64
            }
            Ok(None) => true,
            Err(e) => {
                error!("Freshness check failed for {reference:?}, serving snapshot: {e}");
                false
            }
        }
    }

    pub(super) async fn resolve_query_read_udtf(
        &self,
        udtf: spec::ReadUdtf,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::ReadUdtf {
            name,
            arguments,
            named_arguments,
            options,
        } = udtf;
        if !options.is_empty() {
            return Err(PlanError::todo("ReadType::UDTF options"));
        }
        let Ok(function_name) = <Vec<String>>::from(name).one() else {
            return Err(PlanError::unsupported("qualified table function name"));
        };
        let canonical_function_name = function_name.to_ascii_lowercase();
        if is_built_in_generator_function(&canonical_function_name) {
            let expr = spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(function_name),
                arguments,
                named_arguments,
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            });
            self.resolve_query_project(None, vec![expr], state).await
        } else {
            let udf = self.ctx.udf(&canonical_function_name).ok();
            if let Some(f) = udf
                .as_ref()
                .and_then(|x| x.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>())
            {
                if f.eval_type().is_table_function() {
                    let udtf = PythonUdtf {
                        python_version: f.python_version().to_string(),
                        eval_type: f.eval_type(),
                        command: f.command().to_vec(),
                        return_type: f.output_type().clone(),
                    };
                    let input = self.resolve_query_empty(true)?;
                    let arguments = self
                        .resolve_named_expressions(arguments, input.schema(), state)
                        .await?;
                    self.resolve_python_udtf_plan(
                        udtf,
                        &function_name,
                        input,
                        arguments,
                        None,
                        None,
                        f.deterministic(),
                        state,
                    )
                } else {
                    Err(PlanError::invalid(format!(
                        "user-defined function is not a table function: {function_name}"
                    )))
                }
            } else {
                let schema = Arc::new(DFSchema::empty());
                let arguments = self.resolve_expressions(arguments, &schema, state).await?;
                let table_function =
                    if let Ok(f) = self.ctx.table_function(&canonical_function_name) {
                        f
                    } else if let Ok(f) = get_built_in_table_function(&canonical_function_name) {
                        f
                    } else {
                        return Err(PlanError::unsupported(format!(
                            "unknown table function: {function_name}"
                        )));
                    };
                let table_provider = table_function.create_table_provider(&arguments)?;
                let names = state.register_fields(table_provider.schema().fields());
                let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                Ok(LogicalPlan::TableScan(TableScan::try_new(
                    function_name,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?))
            }
        }
    }

    pub(super) async fn resolve_query_read_data_source(
        &self,
        source: spec::ReadDataSource,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::ReadDataSource {
            format,
            schema,
            options,
            paths,
            predicates,
        } = source;
        if !predicates.is_empty() {
            return Err(PlanError::todo("data source predicates"));
        }
        let Some(format) = format else {
            return Err(PlanError::invalid("missing data source format"));
        };
        let schema = match schema {
            Some(schema) => Some(self.resolve_schema(schema, state)?),
            None => None,
        };
        let info = SourceInfo {
            paths,
            schema,
            // TODO: detect duplicated keys in the set of options
            constraints: Default::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![options.into_iter().collect()],
        };
        let table_provider = default_registry()
            .get_format(&format)?
            .create_provider(&self.ctx.state(), info)
            .await?;
        let names = state.register_fields(table_provider.schema().fields());
        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
        Ok(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(Arc::new(table_provider)),
            None,
            vec![],
            None,
        )?))
    }
}
