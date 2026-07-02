//! PostgreSQL wire handlers bridging to kokedb's protocol-agnostic query path.
//!
//! Both the simple and the extended (Parse/Bind/Execute) query protocols are
//! supported. The extended protocol is implemented by textually substituting
//! bound `$N` parameters into the SQL — mirroring the MySQL server's prepared
//! path — then running it through the same plan/result-cache pipeline. Result
//! metadata for `Describe` is obtained by planning the statement (without
//! collecting any rows) and reading the output schema.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::{stream, Sink, StreamExt};
use kokedb_cache::result_cache::ResultCache;
use kokedb_cache::singleflight::{Claim, OwnerGuard, Singleflight};
use kokedb_common::hash::get_plan_hash;
use kokedb_common::spec::Plan;
use kokedb_query::binder::{parser, query};
use kokedb_query::context::{create_session_context, set_session_acl, SharedServices};
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::auth::{acl_from_client, KokedbStartupHandler};
use crate::encode::{encode_batch, fields_for_schema, fields_with_format};

/// Catalog allow-list for a session (`None` = unrestricted).
type Acl = Option<Arc<std::collections::HashSet<String>>>;

/// How long a singleflight waiter sleeps before re-checking the cache, as a
/// safety net against a lost wakeup (mirrors the MySQL server).
const SINGLEFLIGHT_WAIT_SECS: u64 = 10;

/// Bundles the handlers for one server.
///
/// IMPORTANT: `clone()` is a *per-connection* operation, not a shallow copy —
/// it shares the process-wide services (meta, caches, singleflight, startup
/// handler) but gives the connection a fresh query handler with its own lazy
/// [`SessionContext`], so per-session state (catalog ACL, default catalog)
/// never leaks between connections. Hand every `process_socket` its own clone.
pub struct KokedbHandlers {
    handler: Arc<KokedbQueryHandler>,
    startup: Arc<KokedbStartupHandler>,
}

impl Clone for KokedbHandlers {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::new(KokedbQueryHandler {
                shared: self.handler.shared.clone(),
                parser: self.handler.parser.clone(),
                sf: self.handler.sf.clone(),
                session: Mutex::new(None),
            }),
            startup: self.startup.clone(),
        }
    }
}

impl KokedbHandlers {
    pub fn new(shared: SharedServices) -> Self {
        Self {
            handler: Arc::new(KokedbQueryHandler {
                shared: shared.clone(),
                parser: Arc::new(NoopQueryParser::new()),
                sf: Singleflight::new(),
                session: Mutex::new(None),
            }),
            startup: Arc::new(KokedbStartupHandler::new(shared)),
        }
    }
}

impl PgWireServerHandlers for KokedbHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup.clone()
    }
}

pub struct KokedbQueryHandler {
    shared: SharedServices,
    parser: Arc<NoopQueryParser>,
    /// Process-wide cache-miss de-duplication (shared across connections via
    /// `KokedbHandlers::clone`).
    sf: Arc<Singleflight>,
    /// This connection's session context, built lazily on the first statement
    /// and reused for the rest of the connection. Building one is expensive
    /// (`SessionStateBuilder::with_default_features` registers every UDF), so
    /// it must not happen per query.
    session: Mutex<Option<Arc<SessionContext>>>,
}

fn pg_err(msg: impl std::fmt::Display) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        msg.to_string(),
    )))
}

/// Extracts a `/*+ max_staleness(N) */` hint, mirroring the MySQL server so the
/// freshness contract works for PostgreSQL clients too.
fn parse_staleness_hint(sql: &str) -> Option<u64> {
    let lower = sql.to_ascii_lowercase();
    let key = "max_staleness(";
    let start = lower.find(key)? + key.len();
    let rel_end = sql[start..].find(')')?;
    sql[start..start + rel_end].trim().parse::<u64>().ok()
}

/// Escapes a string for use as a single-quoted SQL literal.
fn quote_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Replaces `$1`, `$2`, … placeholders in `sql` with the literals produced by
/// `lit(idx)` (1-based as written, 0-based to the callback). Placeholders inside
/// single-quoted strings or double-quoted identifiers are left untouched.
fn substitute_placeholders(
    sql: &str,
    mut lit: impl FnMut(usize) -> PgWireResult<String>,
) -> PgWireResult<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '\'' if !in_dquote => {
                in_squote = !in_squote;
                out.push(c);
                i += 1;
            }
            '"' if !in_squote => {
                in_dquote = !in_dquote;
                out.push(c);
                i += 1;
            }
            '$' if !in_squote && !in_dquote && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let n: usize = sql[i + 1..j].parse().map_err(pg_err)?;
                if n == 0 {
                    return Err(pg_err("invalid parameter $0"));
                }
                out.push_str(&lit(n - 1)?);
                i = j;
            }
            _ => {
                // Push the whole UTF-8 char, not just one byte.
                let ch = sql[i..].chars().next().unwrap_or(c);
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    Ok(out)
}

/// Highest `$N` placeholder index that appears in `sql` outside string literals
/// and quoted identifiers (i.e. the number of parameters). Returns 0 if none.
fn placeholder_count(sql: &str) -> usize {
    let mut max = 0usize;
    let _ = substitute_placeholders(sql, |idx| {
        max = max.max(idx + 1);
        Ok(String::new())
    });
    max
}

/// SQL literal for one bound parameter, decoding from its wire type.
fn param_literal(portal: &Portal<String>, idx: usize) -> PgWireResult<String> {
    // A NULL parameter has no bytes.
    if portal.parameters.get(idx).map(|p| p.is_none()).unwrap_or(true) {
        return Ok("NULL".to_string());
    }
    let ty = portal
        .statement
        .parameter_types
        .get(idx)
        .and_then(|t| t.clone())
        .unwrap_or(Type::UNKNOWN);

    let opt = |v: Option<String>| v.unwrap_or_else(|| "NULL".to_string());
    if ty == Type::BOOL {
        Ok(opt(portal.parameter::<bool>(idx, &ty)?.map(|b| b.to_string())))
    } else if ty == Type::INT2 {
        Ok(opt(portal.parameter::<i16>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::INT4 {
        Ok(opt(portal.parameter::<i32>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::INT8 {
        Ok(opt(portal.parameter::<i64>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::FLOAT4 {
        Ok(opt(portal.parameter::<f32>(idx, &ty)?.map(|v| v.to_string())))
    } else if ty == Type::FLOAT8 {
        Ok(opt(portal.parameter::<f64>(idx, &ty)?.map(|v| v.to_string())))
    } else {
        // Text, varchar, unknown, and anything else: decode as a string and
        // emit a quoted literal. Works for text-format params from any type.
        Ok(opt(portal
            .parameter::<String>(idx, &ty)?
            .map(|s| quote_literal(&s))))
    }
}

/// SQL `CAST(NULL AS <type>)` literal for a parameter at describe time, when no
/// value has been bound yet. The cast preserves the result schema's types.
fn null_literal_for_type(ty: &Type) -> String {
    let sql_ty = if *ty == Type::BOOL {
        "boolean"
    } else if *ty == Type::INT2 {
        "smallint"
    } else if *ty == Type::INT4 {
        "integer"
    } else if *ty == Type::INT8 {
        "bigint"
    } else if *ty == Type::FLOAT4 {
        "real"
    } else if *ty == Type::FLOAT8 {
        "double precision"
    } else if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::UNKNOWN {
        "text"
    } else {
        return "NULL".to_string();
    };
    format!("CAST(NULL AS {sql_ty})")
}

impl KokedbQueryHandler {
    /// Parses + hashes the SQL, going through the shared plan cache. The Arc is
    /// handed out as-is: cloning the inner `Plan` would deep-copy the whole
    /// expression tree on every hit.
    fn prepare(&self, sql: &str) -> PgWireResult<Arc<(Plan, u128)>> {
        let plan_cache = self.shared.plan_cache();
        if let Some(hit) = plan_cache.get(sql) {
            return Ok(hit);
        }
        let plan = parser(sql).map_err(pg_err)?;
        let key = get_plan_hash(&plan).map_err(pg_err)?;
        let entry = Arc::new((plan, key));
        plan_cache.insert(sql, entry.clone());
        Ok(entry)
    }

    /// This connection's session context, built on first use and reused across
    /// statements. The ACL is (cheaply) re-applied per statement because auth
    /// completes after the handler is constructed.
    fn session_ctx(&self, acl: &Acl) -> PgWireResult<Arc<SessionContext>> {
        let ctx = {
            let mut guard = self.session.lock().unwrap_or_else(|e| e.into_inner());
            match guard.as_ref() {
                Some(ctx) => ctx.clone(),
                None => {
                    let ctx = Arc::new(create_session_context(&self.shared).map_err(pg_err)?);
                    *guard = Some(ctx.clone());
                    ctx
                }
            }
        };
        set_session_acl(&ctx, acl.clone()).map_err(pg_err)?;
        Ok(ctx)
    }

    /// Runs one statement through the kokedb query path and returns its result
    /// stream. Query results flow through the shared result cache with
    /// singleflight de-duplication, mirroring the MySQL server:
    ///
    /// * cache hit  -> decoded stream straight from the cache;
    /// * cache miss -> the first caller ("owner") executes and tees batches
    ///   into the cache as they stream out; concurrent callers wait for the
    ///   owner instead of re-executing against the source;
    /// * restricted (ACL-scoped) sessions bypass the shared cache entirely —
    ///   a hit would skip catalog-ACL enforcement, which happens at planning.
    async fn execute(&self, sql: &str, acl: &Acl) -> PgWireResult<SendableRecordBatchStream> {
        let max_staleness = parse_staleness_hint(sql).filter(|&s| s > 0);
        let entry = self.prepare(sql)?;
        let (plan, cache_key) = (&entry.0, entry.1);

        let cacheable =
            matches!(plan, Plan::Query(_)) && max_staleness.is_none() && acl.is_none();
        let cache = self.shared.result_cache();

        if !cacheable {
            let ctx = self.session_ctx(acl)?;
            let stream = query(ctx, plan, cache_key, max_staleness)
                .await
                .map_err(pg_err)?;
            return Ok(stream);
        }

        loop {
            if let Some(stream) = cache.try_get(cache_key).await {
                return Ok(stream);
            }
            match self.sf.claim(cache_key) {
                Claim::Owner(guard) => {
                    let ctx = self.session_ctx(acl)?;
                    let stream = query(ctx, plan, cache_key, max_staleness)
                        .await
                        .map_err(pg_err)?;
                    return Ok(tee_into_cache(stream, cache, cache_key, guard));
                }
                Claim::Waiter(notify) => {
                    // Register the wakeup before re-checking the cache to avoid
                    // a lost notification, then await it (bounded by a safety
                    // timeout); the loop re-checks the cache either way.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if let Some(stream) = cache.try_get(cache_key).await {
                        return Ok(stream);
                    }
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(std::time::Duration::from_secs(
                            SINGLEFLIGHT_WAIT_SECS,
                        )) => {}
                    }
                }
            }
        }
    }

    /// Plans the statement and returns its output schema without executing it.
    /// Non-query statements have no row description, so an empty schema is
    /// returned for them.
    async fn output_schema(&self, sql: &str, acl: &Acl) -> PgWireResult<SchemaRef> {
        let entry = self.prepare(sql)?;
        if !matches!(entry.0, Plan::Query(_)) {
            return Ok(Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        }
        let ctx = self.session_ctx(acl)?;
        let stream = query(ctx, &entry.0, entry.1, None).await.map_err(pg_err)?;
        Ok(stream.schema())
    }

    /// Runs one statement and shapes the wire response. Rows are encoded
    /// lazily, batch by batch, as the client drains the response — the result
    /// set is never materialized in full.
    async fn execute_to_response(
        &self,
        sql: &str,
        acl: &Acl,
        format: &Format,
    ) -> PgWireResult<Response> {
        let mut stream = self.execute(sql, acl).await?;
        let schema = stream.schema();

        // A schemaless result is a command (e.g. CREATE CATALOG): drive it to
        // completion here so its side effects happen before the tag is sent
        // (and before any following statement in a multi-statement query).
        if schema.fields().is_empty() {
            while let Some(item) = stream.next().await {
                item.map_err(pg_err)?;
            }
            return Ok(Response::Execution(Tag::new("OK")));
        }

        let fields = fields_with_format(&schema, format);
        let encode_fields = fields.clone();
        let row_stream = stream.flat_map(move |batch_res| {
            let rows: Vec<PgWireResult<pgwire::messages::data::DataRow>> = match batch_res {
                Ok(batch) => match encode_batch(&batch, &encode_fields) {
                    Ok(rows) => rows.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                },
                Err(e) => vec![Err(pg_err(e))],
            };
            stream::iter(rows)
        });
        Ok(Response::Query(QueryResponse::new(fields, row_stream)))
    }
}

/// Wraps a live result stream so that, on successful completion, the collected
/// batches are inserted into the result cache and the singleflight owner guard
/// is released. Collecting is cheap (`RecordBatch` clones are shallow — the
/// column buffers are shared). If the client disconnects mid-stream or the
/// query errors, the guard is dropped without caching, waking waiters so one
/// of them can take over.
fn tee_into_cache(
    stream: SendableRecordBatchStream,
    cache: ResultCache,
    key: u128,
    guard: OwnerGuard,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let teed = async_stream::try_stream! {
        let mut inner = stream;
        let mut collected: Vec<RecordBatch> = Vec::new();
        while let Some(item) = inner.next().await {
            let batch = item?;
            collected.push(batch.clone());
            yield batch;
        }
        if !collected.is_empty() {
            if let Err(e) = cache.insert(key, &collected).await {
                log::warn!("Failed to cache result for key {key}: {e}");
            }
        }
        drop(guard);
    };
    Box::pin(RecordBatchStreamAdapter::new(schema, teed))
}

/// Splits a simple-query payload into individual statements at top-level `;`,
/// honouring single-quoted strings and double-quoted identifiers. Empty
/// fragments (e.g. a trailing `;`) are dropped.
fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    for (i, &b) in bytes.iter().enumerate() {
        match b as char {
            '\'' if !in_dquote => in_squote = !in_squote,
            '"' if !in_squote => in_dquote = !in_dquote,
            ';' if !in_squote && !in_dquote => {
                let stmt = sql[start..i].trim();
                if !stmt.is_empty() {
                    out.push(stmt);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let tail = sql[start..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    out
}

#[async_trait]
impl SimpleQueryHandler for KokedbQueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let acl = acl_from_client(client);
        // The simple protocol allows several `;`-separated statements in one
        // message, each answered with its own response; an empty payload gets
        // the dedicated EmptyQueryResponse.
        let statements = split_statements(query);
        if statements.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let mut responses = Vec::with_capacity(statements.len());
        for stmt in statements {
            responses.push(
                self.execute_to_response(stmt, &acl, &Format::UnifiedText)
                    .await?,
            );
        }
        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for KokedbQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let acl = acl_from_client(client);
        let sql = substitute_placeholders(&portal.statement.statement, |idx| {
            param_literal(portal, idx)
        })?;
        self.execute_to_response(&sql, &acl, &portal.result_column_format)
            .await
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Report one type per `$N` placeholder so the client binds the right
        // number of parameters. We can't infer types from kokedb's custom plan
        // IR, so we honour the client-supplied OID (JDBC, asyncpg, libpq and
        // `prepare_typed` all send them) and fall back to TEXT otherwise.
        let n = placeholder_count(&target.statement).max(target.parameter_types.len());
        let param_types: Vec<Type> = (0..n)
            .map(|i| {
                target
                    .parameter_types
                    .get(i)
                    .and_then(|t| t.clone())
                    .unwrap_or(Type::TEXT)
            })
            .collect();
        // Substitute placeholders with typed NULLs so the statement plans and we
        // can read the result schema without binding real values.
        let sql = substitute_placeholders(&target.statement, |idx| {
            Ok(null_literal_for_type(
                param_types.get(idx).unwrap_or(&Type::UNKNOWN),
            ))
        })?;
        let acl = acl_from_client(client);
        let schema = self.output_schema(&sql, &acl).await?;
        let fields = fields_for_schema(&schema);
        Ok(DescribeStatementResponse::new(
            param_types,
            (*fields).clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let acl = acl_from_client(client);
        let sql = substitute_placeholders(&portal.statement.statement, |idx| {
            param_literal(portal, idx)
        })?;
        let schema = self.output_schema(&sql, &acl).await?;
        let fields = fields_with_format(&schema, &portal.result_column_format);
        Ok(DescribePortalResponse::new((*fields).clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_statements_at_top_level_semicolons() {
        assert_eq!(split_statements("SELECT 1; SELECT 2"), vec!["SELECT 1", "SELECT 2"]);
        // Trailing semicolon leaves no empty fragment.
        assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
        // Semicolons inside string literals / quoted identifiers don't split.
        assert_eq!(
            split_statements("SELECT 'a;b'; SELECT \"c;d\""),
            vec!["SELECT 'a;b'", "SELECT \"c;d\""]
        );
        // Empty / whitespace-only payloads yield no statements.
        assert!(split_statements("   ;  ; ").is_empty());
        assert!(split_statements("").is_empty());
    }

    #[test]
    fn substitutes_numbered_placeholders() {
        let out =
            substitute_placeholders("SELECT * FROM t WHERE a = $1 AND b = $2", |i| Ok(format!("P{i}")))
                .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = P0 AND b = P1");
    }

    #[test]
    fn leaves_placeholders_inside_quotes() {
        let out = substitute_placeholders("SELECT '$1', \"$2\", $1", |i| Ok(format!("X{i}"))).unwrap();
        assert_eq!(out, "SELECT '$1', \"$2\", X0");
    }

    #[test]
    fn counts_placeholders() {
        assert_eq!(placeholder_count("SELECT $1, $2 WHERE x = $1"), 2);
        assert_eq!(placeholder_count("SELECT '$3', $1"), 1);
        assert_eq!(placeholder_count("SELECT 1"), 0);
    }

    #[test]
    fn rejects_zero_placeholder() {
        assert!(substitute_placeholders("SELECT $0", |i| Ok(format!("{i}"))).is_err());
    }

    #[test]
    fn quote_literal_escapes_quotes() {
        assert_eq!(quote_literal("a'b"), "'a''b'");
    }

    #[test]
    fn parses_staleness_hint_value() {
        assert_eq!(parse_staleness_hint("SELECT /*+ max_staleness(30) */ 1"), Some(30));
        assert_eq!(parse_staleness_hint("SELECT 1"), None);
    }

    #[test]
    fn null_cast_for_known_types() {
        assert_eq!(null_literal_for_type(&Type::INT4), "CAST(NULL AS integer)");
        assert_eq!(null_literal_for_type(&Type::TEXT), "CAST(NULL AS text)");
        assert_eq!(null_literal_for_type(&Type::TIMESTAMP), "NULL");
    }
}
