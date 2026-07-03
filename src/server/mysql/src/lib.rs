pub mod column;
pub mod error;
pub mod metrics;
pub mod row;
pub use kokedb_cache::singleflight;

use std::collections::HashMap;
use std::sync::Arc;

use crate::{column::compact_columns, error::MysqlServerError, row::compact_batch_rows};
use datafusion::{
    arrow::array::RecordBatch, execution::RecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, prelude::SessionContext,
};
use futures::StreamExt;
use kokedb_cache::result_cache::ResultCache;
use kokedb_common::{hash::get_plan_hash, spec::Plan};
use kokedb_meta::catalog_list::PostgreSQLMetaCatalogProviderList;
use kokedb_query::{
    binder::{parser, query, save_sql_history},
    context::{create_session_context, set_session_acl, use_database, SharedServices},
    plan_cache::PlanCache,
};
use log::{error, info, warn};
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

/// A prepared statement bound to a single client connection.
#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    param_count: usize,
}

struct CoreContex {
    ctx: Arc<SessionContext>,
    cache: ResultCache,
    /// Process-wide single-flight registry for cache-miss de-duplication.
    sf: Arc<singleflight::Singleflight>,
    /// Shared meta-store handle (reuses the process-wide pool) for recording
    /// SQL execution history.
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    /// Shared SQL-text -> parsed-plan cache (process-wide).
    plan_cache: PlanCache,
    /// Per-connection freshness requirement (seconds) set via
    /// `SET kokedb_max_staleness = N`. 0/None means serve cache as-is.
    session_max_staleness: Option<u64>,
    /// Per-connection prepared statement registry, keyed by statement id.
    prepared: HashMap<u32, PreparedStatement>,
    /// Monotonic statement id generator for this connection.
    next_stmt_id: u32,
}

impl CoreContex {
    fn new(
        ctx: Arc<SessionContext>,
        cache: ResultCache,
        sf: Arc<singleflight::Singleflight>,
        meta: Arc<PostgreSQLMetaCatalogProviderList>,
        plan_cache: PlanCache,
    ) -> Self {
        Self {
            ctx,
            cache,
            sf,
            meta,
            plan_cache,
            session_max_staleness: None,
            prepared: HashMap::new(),
            next_stmt_id: 1,
        }
    }
}

// Type aliases for clarity
type CacheKey = u128;
type BatchStream = std::pin::Pin<Box<dyn RecordBatchStream + Send>>;
type Cache = ResultCache;
type Context = datafusion::prelude::SessionContext;

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for CoreContex {
    type Error = MysqlServerError;

    /// Validates the client's credentials with `mysql_native_password` against
    /// the `system.app_user` table, and scopes the connection to the user's
    /// allowed catalogs. The check is fully dynamic — `CREATE USER` takes
    /// effect on the next connection, no restart needed: while the table is
    /// empty every client is accepted (open access); once any account exists,
    /// only known users get in.
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let Ok(username) = std::str::from_utf8(username) else {
            warn!("Auth failed: non-UTF8 username");
            return false;
        };
        let user = match self.meta.get_app_user(username).await {
            Ok(Some(user)) => user,
            Ok(None) => {
                // Unknown user: allowed only while authentication is disabled
                // (no accounts defined at all).
                return match self.meta.count_app_users().await {
                    Ok(0) => true,
                    Ok(_) => {
                        warn!("Auth failed: unknown user '{username}'");
                        false
                    }
                    Err(e) => {
                        error!("Auth failed: meta lookup error: {e}");
                        false
                    }
                };
            }
            Err(e) => {
                error!("Auth failed: meta lookup error: {e}");
                return false;
            }
        };
        if !kokedb_common::auth::verify_mysql_native(&user.auth_digest, salt, auth_data) {
            warn!("Auth failed: bad password for user '{username}'");
            return false;
        }
        // Scope the connection to the user's grants (None = unrestricted).
        // Row policies only apply to restricted sessions, so they are loaded
        // only when the user carries an allow-list.
        let acl = user.allowed_catalogs.map(Arc::new);
        let (policies, column_policies) = if acl.is_some() {
            let rows = match self.meta.list_row_policies(username).await {
                Ok(rows) => rows,
                Err(e) => {
                    // Fail-closed: rejecting the login is safer than admitting
                    // a session whose row filters could not be loaded.
                    error!("Auth failed: could not load row policies for '{username}': {e}");
                    return false;
                }
            };
            let cols = match self.meta.list_column_policies(username).await {
                Ok(cols) => cols,
                Err(e) => {
                    error!("Auth failed: could not load column policies for '{username}': {e}");
                    return false;
                }
            };
            (
                kokedb_query::context::parse_row_policies(rows),
                kokedb_query::context::parse_column_policies(cols),
            )
        } else {
            (Vec::new(), Vec::new())
        };
        if let Err(e) = set_session_acl(&self.ctx, acl, policies, column_policies) {
            error!("Failed to apply catalog ACL for '{username}': {e}");
            return false;
        }
        true
    }

    /// Prepare a statement: validate the SQL, count its `?` placeholders, and
    /// register it under a fresh statement id so `on_execute` can run it later.
    ///
    /// Result column metadata is reported lazily (empty here) because resolving
    /// it requires the parameter values, which are only known at execute time.
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        let param_count = count_param_placeholders(query);

        let stmt_id = self.next_stmt_id;
        self.next_stmt_id = self.next_stmt_id.wrapping_add(1).max(1);

        self.prepared.insert(
            stmt_id,
            PreparedStatement {
                sql: query.to_string(),
                param_count,
            },
        );

        // One generic placeholder descriptor per parameter.
        let params: Vec<Column> = (0..param_count).map(|_| generic_param_column()).collect();

        info.reply(stmt_id, &params, &[])
            .await
            .map_err(|x| MysqlServerError::InternalError(x.to_string()))
    }

    /// Execute a previously prepared statement by substituting the bound
    /// parameters into the SQL text and running it through the shared query path.
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        let stmt = match self.prepared.get(&id) {
            Some(stmt) => stmt.clone(),
            None => {
                let msg = format!("Unknown prepared statement id: {}", id);
                return send_error_to_client(results, ErrorKind::ER_UNKNOWN_STMT_HANDLER, msg).await;
            }
        };

        // Decode bound parameters into SQL literals, in positional order.
        let mut literals = Vec::with_capacity(stmt.param_count);
        for param in params {
            match param_to_sql_literal(param.value) {
                Ok(literal) => literals.push(literal),
                Err(msg) => {
                    return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, msg).await;
                }
            }
        }

        let sql = match substitute_params(&stmt.sql, &literals) {
            Ok(sql) => sql,
            Err(msg) => {
                return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, msg).await;
            }
        };

        run_query(
            self.ctx.clone(),
            self.cache.clone(),
            self.sf.clone(),
            self.meta.clone(),
            self.plan_cache.clone(),
            self.session_max_staleness,
            &sql,
            results,
        )
        .await
    }

    async fn on_close(&mut self, stmt: u32) {
        self.prepared.remove(&stmt);
    }

    /// Handles `USE <database>` (COM_INIT_DB): sets this connection's default
    /// catalog. Errors if the catalog does not exist.
    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        match use_database(&self.ctx, database) {
            Ok(()) => {
                info!("Connection switched default catalog to '{}'", database);
                writer
                    .ok()
                    .await
                    .map_err(|e| MysqlServerError::InternalError(e.to_string()))
            }
            Err(msg) => {
                warn!("USE {} failed: {}", database, msg);
                writer
                    .error(ErrorKind::ER_BAD_DB_ERROR, msg.as_bytes())
                    .await
                    .map_err(|e| MysqlServerError::InternalError(e.to_string()))
            }
        }
    }

    /*
     * sql --> cache hit --> yes --> return cache result
     *              |
     *                   --> no  --> execute query  --> return result
     *                                   |
     *                                      --> save to cache
     *
     * sync table cache task  --> table not used  --> cache table data from remote
     *                                  |
     *                                            --> execute cached query --> delete all cached result abount the table
     */
    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        // `SET kokedb_max_staleness = N` is a kokedb session knob, not a real
        // query: store it on the connection and acknowledge.
        if let Some(secs) = parse_set_max_staleness(sql) {
            self.session_max_staleness = if secs == 0 { None } else { Some(secs) };
            return reply_ok(results, "kokedb_max_staleness set").await;
        }
        run_query(
            self.ctx.clone(),
            self.cache.clone(),
            self.sf.clone(),
            self.meta.clone(),
            self.plan_cache.clone(),
            self.session_max_staleness,
            sql,
            results,
        )
        .await
    }
}

/// Shared query execution path used by both text queries (`on_query`) and
/// prepared statement execution (`on_execute`).
async fn run_query<W: AsyncWrite + Send + Unpin>(
    ctx: Arc<SessionContext>,
    cache: Cache,
    sf: Arc<singleflight::Singleflight>,
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    plan_cache: PlanCache,
    session_max_staleness: Option<u64>,
    sql: &str,
    results: QueryResultWriter<'_, W>,
) -> Result<(), MysqlServerError> {
    let instant = std::time::Instant::now();
    metrics::inc_queries();

    // A per-query `/*+ max_staleness(N) */` hint overrides the session value.
    // 0 (either source) means "no freshness requirement", treated as None.
    let max_staleness = parse_staleness_hint(sql)
        .or(session_max_staleness)
        .filter(|&s| s > 0);

    // Step 1: Parse SQL and generate plan (shared Arc from the plan cache).
    let plan_entry = match parse_sql_and_get_plan(&plan_cache, sql) {
        Ok(result) => result,
        Err((error_kind, error_msg)) => {
            return send_error_to_client(results, error_kind, error_msg).await;
        }
    };
    let (plan, cache_key) = (&plan_entry.0, plan_entry.1);

    // Steps 2+3: produce the stream and pull its first batch, bounded by the
    // optional query timeout. Both happen before we start writing to the client
    // (Step 5), so on timeout we can still return a clean error packet. This is
    // where a hung source or a runaway query would otherwise block forever.
    let restricted = kokedb_query::context::session_is_restricted(&ctx);
    let produce_first = async {
        let mut stream =
            get_batch_stream(&cache, &sf, ctx, plan, cache_key, max_staleness, restricted).await?;
        let first = stream.next().await;
        Ok::<_, String>((stream, first))
    };
    let (mut batch_stream, first) = match apply_query_timeout(produce_first).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(error_msg)) => {
            return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, error_msg).await;
        }
        Err(secs) => {
            return send_error_to_client(
                results,
                ErrorKind::ER_QUERY_INTERRUPTED,
                format!("query exceeded the {secs}s timeout (KOKEDB_QUERY_TIMEOUT_SECS)"),
            )
            .await;
        }
    };

    let first_batch = match first {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => {
            let error_msg = format!("Error reading first batch: {}", e);
            return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, error_msg).await;
        }
        None => {
            // Empty result set
            return handle_empty_result(results, meta, sql, plan_entry.clone(), instant).await;
        }
    };

    // Step 4: Prepare columns from schema
    let columns = match prepare_columns_from_batch(&first_batch) {
        Ok(cols) => cols,
        Err(error_msg) => {
            return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, error_msg).await;
        }
    };

    // ============ Critical boundary: cannot use results.error() after start() ============

    // Step 5: Start writing results
    let mut writer = results.start(&columns).await.map_err(|e| {
        let error_msg = format!("Failed to create result writer: {}", e);
        error!("{}", error_msg);
        MysqlServerError::CreateMysqlResultWriterError(error_msg)
    })?;

    // Step 6: Write all batches to client
    write_batches_to_client(&mut writer, &first_batch, &mut batch_stream).await?;

    // Step 7: Finalize query execution
    finalize_query(writer, meta, sql, plan_entry.clone(), instant).await?;

    Ok(())
}

async fn send_error_to_client<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
    error_kind: ErrorKind,
    error_msg: String,
) -> Result<(), MysqlServerError> {
    error!("{}", error_msg);
    metrics::inc_query_error();
    results
        .error(error_kind, error_msg.as_bytes())
        .await
        .map_err(|io_err| MysqlServerError::WriteMysqlResultError(io_err.to_string()))?;
    Ok(())
}

/// Builds a generic string parameter descriptor for prepared statement metadata.
fn generic_param_column() -> Column {
    Column {
        table: String::new(),
        column: "?".to_string(),
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }
}

/// Counts `?` placeholders in `sql`, ignoring any that appear inside single
/// quotes, double quotes, or backtick-quoted identifiers.
fn count_param_placeholders(sql: &str) -> usize {
    let mut count = 0;
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                if c == q {
                    // A doubled quote is an escaped literal quote, not a terminator.
                    if chars.peek() == Some(&q) {
                        chars.next();
                    } else {
                        quote = None;
                    }
                } else if c == '\\' && q != '`' {
                    // Skip the escaped character inside string literals.
                    chars.next();
                }
            }
            None => match c {
                '\'' | '"' | '`' => quote = Some(c),
                '?' => count += 1,
                _ => {}
            },
        }
    }

    count
}

/// Substitutes positional `?` placeholders in `sql` with the provided literals,
/// skipping any `?` inside quoted strings/identifiers. Returns an error if the
/// number of placeholders does not match the number of literals.
fn substitute_params(sql: &str, literals: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len() + literals.len() * 8);
    let mut quote: Option<char> = None;
    let mut next_param = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    if chars.peek() == Some(&q) {
                        out.push(q);
                        chars.next();
                    } else {
                        quote = None;
                    }
                } else if c == '\\' && q != '`' {
                    if let Some(escaped) = chars.next() {
                        out.push(escaped);
                    }
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    out.push(c);
                }
                '?' => {
                    let literal = literals.get(next_param).ok_or_else(|| {
                        format!(
                            "Too few parameters supplied: expected more than {}",
                            literals.len()
                        )
                    })?;
                    out.push_str(literal);
                    next_param += 1;
                }
                _ => out.push(c),
            },
        }
    }

    if next_param != literals.len() {
        return Err(format!(
            "Parameter count mismatch: statement has {} placeholders but {} were supplied",
            next_param,
            literals.len()
        ));
    }

    Ok(out)
}

/// Converts a bound MySQL parameter value into a SQL literal suitable for
/// textual substitution into the statement.
fn param_to_sql_literal(value: Value<'_>) -> Result<String, String> {
    match value.into_inner() {
        ValueInner::NULL => Ok("NULL".to_string()),
        ValueInner::Int(i) => Ok(i.to_string()),
        ValueInner::UInt(u) => Ok(u.to_string()),
        ValueInner::Double(d) => {
            if d.is_finite() {
                Ok(d.to_string())
            } else {
                // NaN/Infinity have no SQL literal form.
                Ok("NULL".to_string())
            }
        }
        ValueInner::Bytes(b) => {
            let s = String::from_utf8_lossy(b);
            Ok(quote_sql_string(&s))
        }
        ValueInner::Date(_) | ValueInner::Datetime(_) => {
            // `Value` is `Copy`, so it is still usable after `into_inner` above.
            let dt = to_naive_datetime(value)
                .map_err(|e| format!("Failed to decode date/datetime parameter: {}", e))?;
            Ok(format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f")))
        }
        ValueInner::Time(_) => Err("TIME parameter binding is not supported yet".to_string()),
    }
}

/// Quotes and escapes a string as a single-quoted SQL string literal.
fn quote_sql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

// Try to retrieve result from cache. Single lookup: `contains()` + `get()`
// would cost two round-trips on the Redis backend and race with eviction.
async fn try_get_from_cache(cache: &Cache, cache_key: CacheKey) -> Option<BatchStream> {
    match cache.try_get(cache_key).await {
        Some(stream) => {
            info!(
                "Cache hit with key: {}, retrieving result from cache.",
                cache_key
            );
            Some(stream)
        }
        None => {
            info!("Cache miss for key: {}, querying from kokedb.", cache_key);
            None
        }
    }
}

// Parse SQL and generate execution plan. Consults the process-wide plan cache
// first: parsing + plan-hashing are pure functions of the SQL text, so a hit
// skips both on the hot path (including result-cache hits, which still need the
// cache key before they can look anything up).
fn parse_sql_and_get_plan(
    plan_cache: &PlanCache,
    sql: &str,
) -> Result<Arc<(Plan, CacheKey)>, (ErrorKind, String)> {
    if let Some(hit) = plan_cache.get(sql) {
        // Hand the Arc straight out: cloning the Plan here would deep-copy the
        // whole expression tree on every cache hit.
        return Ok(hit);
    }

    let plan = parser(sql).map_err(|e| {
        (
            ErrorKind::ER_PARSE_ERROR,
            format!("SQL parsing error: {}", e),
        )
    })?;

    let cache_key = get_plan_hash(&plan).map_err(|e| {
        (
            ErrorKind::ER_UNKNOWN_ERROR,
            format!("Failed to generate cache key: {}", e),
        )
    })?;

    let entry = Arc::new((plan, cache_key));
    plan_cache.insert(sql, entry.clone());
    Ok(entry)
}

fn should_cache_plan(plan: &Plan) -> bool {
    matches!(plan, Plan::Query(_))
}

/// Recognizes `SET kokedb_max_staleness = N` (optionally `SET SESSION ...`,
/// trailing `;`, quoted value) and returns N seconds. Any other statement
/// returns None so it flows through the normal query path.
fn parse_set_max_staleness(sql: &str) -> Option<u64> {
    let s = sql.trim().trim_end_matches(';').trim();
    let lower = s.to_ascii_lowercase();
    let rest = lower.strip_prefix("set ")?.trim_start();
    let rest = rest.strip_prefix("session ").unwrap_or(rest).trim_start();
    let after = rest.strip_prefix("kokedb_max_staleness")?.trim_start();
    let value = after.strip_prefix('=')?.trim();
    let value = value.trim_matches(|c| c == '\'' || c == '"').trim();
    value.parse::<u64>().ok()
}

/// Bounds a query's time-to-first-batch by `KOKEDB_QUERY_TIMEOUT_SECS` (0 =
/// disabled, the default). On timeout returns `Err(secs)` so the caller can
/// report it; otherwise returns the future's value. Applied before any result
/// is written, so a clean error packet is still possible.
async fn apply_query_timeout<F, T>(fut: F) -> Result<T, u64>
where
    F: std::future::Future<Output = T>,
{
    let secs = kokedb_common::env::get_env_as("KOKEDB_QUERY_TIMEOUT_SECS", 0u64);
    if secs == 0 {
        return Ok(fut.await);
    }
    match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
        Ok(v) => Ok(v),
        Err(_) => Err(secs),
    }
}

/// Extracts the seconds from a `/*+ max_staleness(N) */` query hint, if present.
fn parse_staleness_hint(sql: &str) -> Option<u64> {
    let lower = sql.to_ascii_lowercase();
    let key = "max_staleness(";
    let start = lower.find(key)? + key.len();
    let rel_end = sql[start..].find(')')?;
    sql[start..start + rel_end].trim().parse::<u64>().ok()
}

/// Sends an empty-result OK packet with an info message (used for accepted
/// session-control statements like `SET kokedb_max_staleness`).
async fn reply_ok<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
    info: &str,
) -> Result<(), MysqlServerError> {
    let writer = results
        .start(&[])
        .await
        .map_err(|e| MysqlServerError::CreateMysqlResultWriterError(e.to_string()))?;
    writer
        .finish_with_info(info)
        .await
        .map_err(|e| MysqlServerError::WriteMysqlResultError(e.to_string()))
}

fn spawn_cache_task(
    cache: Cache,
    cache_key: CacheKey,
    rx: tokio::sync::mpsc::UnboundedReceiver<RecordBatch>,
    // Held until the cache write completes; dropping it wakes single-flight
    // waiters so they can read the freshly-cached result.
    owner: Option<singleflight::OwnerGuard>,
) {
    tokio::spawn(async move {
        let _owner = owner;
        let mut collected_batches = Vec::new();
        let mut receiver = rx;

        while let Some(batch) = receiver.recv().await {
            collected_batches.push(batch);
        }

        if collected_batches.is_empty() {
            info!("No batches to cache");
            return;
        }

        info!(
            "Collected {} batches, inserting to cache",
            collected_batches.len()
        );
        match cache.insert(cache_key, &collected_batches).await {
            Ok(()) => info!("Successfully cached query results"),
            Err(e) => error!("Failed to cache query results: {}", e),
        }
    });
}

// Execute query and setup caching
async fn execute_query_with_cache(
    ctx: Arc<Context>,
    plan: &Plan,
    cache_key: CacheKey,
    cache: Cache,
    owner: Option<singleflight::OwnerGuard>,
    max_staleness: Option<u64>,
) -> Result<BatchStream, String> {
    info!("Not hitted cache, execute query from kokedb");
    // On error the `owner` guard drops here, releasing any single-flight waiters.
    let query_stream = query(ctx, plan, cache_key, max_staleness)
        .await
        .map_err(|e| format!("Query execution error: {}", e))?;

    // Don't cache a result produced under a freshness requirement: it may have
    // read tables live, and the cache has no notion of result age.
    if !should_cache_plan(plan) || max_staleness.is_some() {
        return Ok(query_stream);
    }

    let schema = query_stream.schema();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn background task to collect and cache results
    spawn_cache_task(cache, cache_key, rx, owner);

    // Adapt stream to send batches to caching task
    let adapted_stream = query_stream.map(move |batch_result| {
        if let Ok(ref batch) = batch_result {
            let _ = tx.send(batch.clone());
        }
        batch_result
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        adapted_stream,
    )))
}

// How long a waiter blocks for the in-flight owner before falling back to
// re-checking the cache itself (safety net against a lost wakeup).
const SINGLEFLIGHT_WAIT_SECS: u64 = 10;

// Get or create batch stream from cache or query.
//
// For cacheable plans this single-flights concurrent misses on the same key:
// the first caller executes while the rest wait and then serve from the cache.
async fn get_batch_stream(
    cache: &Cache,
    sf: &Arc<singleflight::Singleflight>,
    ctx: Arc<Context>,
    plan: &Plan,
    cache_key: CacheKey,
    max_staleness: Option<u64>,
    restricted: bool,
) -> Result<BatchStream, String> {
    // A freshness-demanding query must not read a possibly-stale cached result
    // (cached results carry no age), nor pollute the cache with a result whose
    // tables were routed live. Bypass the result cache entirely; the staleness
    // value flows on into per-table routing.
    //
    // Restricted (ACL-scoped) sessions also bypass the shared, user-agnostic
    // cache: a hit would skip catalog-ACL enforcement, which only happens during
    // planning. Their queries always plan, which errors on a denied catalog.
    if !should_cache_plan(plan) || max_staleness.is_some() || restricted {
        // Commands, non-cacheable plans, and fresh-required queries bypass cache.
        return execute_query_with_cache(ctx, plan, cache_key, cache.clone(), None, max_staleness)
            .await;
    }

    loop {
        if let Some(stream) = try_get_from_cache(cache, cache_key).await {
            metrics::inc_cache_hit();
            return Ok(stream);
        }

        match sf.claim(cache_key) {
            singleflight::Claim::Owner(guard) => {
                // We own this key: execute and populate the cache. The guard is
                // moved into the cache-writer task so waiters are released once
                // the result is cached (or immediately if execution fails).
                metrics::inc_cache_miss();
                return execute_query_with_cache(
                    ctx,
                    plan,
                    cache_key,
                    cache.clone(),
                    Some(guard),
                    max_staleness,
                )
                .await;
            }
            singleflight::Claim::Waiter(notify) => {
                // Register the wakeup before re-checking the cache to avoid a
                // lost notification, then await it (bounded by a safety timeout).
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(stream) = try_get_from_cache(cache, cache_key).await {
                    metrics::inc_cache_hit();
                    return Ok(stream);
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(std::time::Duration::from_secs(SINGLEFLIGHT_WAIT_SECS)) => {}
                }
                // Loop: re-check the cache (normally a hit now).
            }
        }
    }
}

// Handle empty result set
async fn handle_empty_result<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    sql: &str,
    plan_entry: Arc<(Plan, CacheKey)>,
    instant: std::time::Instant,
) -> Result<(), MysqlServerError> {
    let writer = results.start(&[]).await.map_err(|e| {
        error!("Failed to create result writer for empty set: {}", e);
        MysqlServerError::CreateMysqlResultWriterError(e.to_string())
    })?;

    writer
        .finish_with_info("Query executed successfully")
        .await
        .map_err(|e| {
            error!("Failed to finish empty result: {}", e);
            MysqlServerError::WriteMysqlResultError(e.to_string())
        })?;

    spawn_save_sql_history(meta, sql.to_string(), plan_entry, instant);
    Ok(())
}

/// Records SQL execution history off the hot path. The client already has its
/// full result by the time this runs, so a slow meta store must not delay the
/// connection becoming ready for its next query.
fn spawn_save_sql_history(
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    sql: String,
    plan_entry: Arc<(Plan, CacheKey)>,
    instant: std::time::Instant,
) {
    let cost = instant.elapsed().as_millis() as u64;
    tokio::spawn(async move {
        if let Err(e) = save_sql_history(&meta, &sql, &plan_entry.0, cost).await {
            error!("Failed to store sql execute info: {:?}", e);
        }
    });
}

// Prepare columns from first batch schema
fn prepare_columns_from_batch(batch: &RecordBatch) -> Result<Vec<Column>, String> {
    let schema = batch.schema();
    compact_columns(schema).map_err(|e| format!("Failed to compact columns: {}", e))
}

// Write all batches to MySQL client
async fn write_batches_to_client<W: AsyncWrite + Unpin>(
    writer: &mut RowWriter<'_, W>,
    first_batch: &RecordBatch,
    batch_stream: &mut BatchStream,
) -> Result<(), MysqlServerError> {
    // Write first batch
    write_batch_to_mysql(writer, first_batch)
        .await
        .map_err(|e| {
            let error_msg = format!("Failed to write first batch to MySQL: {}", e);
            error!("{}", error_msg);
            MysqlServerError::WriteMysqlResultError(error_msg)
        })?;

    // Write remaining batches
    while let Some(batch_result) = batch_stream.next().await {
        let batch = batch_result.map_err(|e| {
            let error_msg = format!("Error reading batch from stream: {}", e);
            error!("{}", error_msg);
            MysqlServerError::DataFusionError(error_msg)
        })?;

        write_batch_to_mysql(writer, &batch).await.map_err(|e| {
            let error_msg = format!("Failed to write batch to MySQL: {}", e);
            error!("{}", error_msg);
            MysqlServerError::WriteMysqlResultError(error_msg)
        })?;
    }

    Ok(())
}

// Finalize query execution
async fn finalize_query<W: AsyncWrite + Unpin>(
    writer: RowWriter<'_, W>,
    meta: Arc<PostgreSQLMetaCatalogProviderList>,
    sql: &str,
    plan_entry: Arc<(Plan, CacheKey)>,
    instant: std::time::Instant,
) -> Result<(), MysqlServerError> {
    writer
        .finish_with_info("Query executed successfully")
        .await
        .map_err(|e| {
            let error_msg = format!("Failed to finish result writer: {}", e);
            error!("{}", error_msg);
            MysqlServerError::WriteMysqlResultError(error_msg)
        })?;

    spawn_save_sql_history(meta, sql.to_string(), plan_entry, instant);
    Ok(())
}

async fn write_batch_to_mysql<'a, W: AsyncWrite + Unpin>(
    writer: &mut RowWriter<'a, W>,
    batch: &RecordBatch,
) -> Result<(), MysqlServerError> {
    let rows = compact_batch_rows(batch)?;
    for row in rows {
        writer
            .write_row(row.iter().map(|s| s.as_str()))
            .await
            .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_set_max_staleness() {
        assert_eq!(parse_set_max_staleness("SET kokedb_max_staleness = 60"), Some(60));
        assert_eq!(parse_set_max_staleness("set  kokedb_max_staleness=0;"), Some(0));
        assert_eq!(
            parse_set_max_staleness("SET SESSION kokedb_max_staleness = '120'"),
            Some(120)
        );
        // Unrelated SET statements must pass through (None).
        assert_eq!(parse_set_max_staleness("SET autocommit = 1"), None);
        assert_eq!(parse_set_max_staleness("SET NAMES utf8mb4"), None);
        assert_eq!(parse_set_max_staleness("SELECT 1"), None);
    }

    #[tokio::test]
    async fn query_timeout_disabled_passes_through_and_enabled_trips() {
        // Disabled (default) -> the future runs to completion.
        std::env::remove_var("KOKEDB_QUERY_TIMEOUT_SECS");
        assert_eq!(apply_query_timeout(async { 7u8 }).await, Ok(7));

        // Enabled -> a slow future trips the timeout and reports the bound.
        std::env::set_var("KOKEDB_QUERY_TIMEOUT_SECS", "1");
        let slow = async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            7u8
        };
        assert_eq!(apply_query_timeout(slow).await, Err(1));
        std::env::remove_var("KOKEDB_QUERY_TIMEOUT_SECS");
    }

    #[test]
    fn parses_staleness_hint() {
        assert_eq!(
            parse_staleness_hint("SELECT /*+ max_staleness(30) */ * FROM t"),
            Some(30)
        );
        assert_eq!(
            parse_staleness_hint("select /*+ MAX_STALENESS( 5 ) */ a from t"),
            Some(5)
        );
        // No hint -> None (default cache behavior).
        assert_eq!(parse_staleness_hint("SELECT * FROM t"), None);
    }

    #[test]
    fn counts_placeholders_outside_quotes() {
        assert_eq!(count_param_placeholders("SELECT * FROM t WHERE a = ?"), 1);
        assert_eq!(
            count_param_placeholders("SELECT * FROM t WHERE a = ? AND b = ?"),
            2
        );
        // `?` inside string / identifier quotes must be ignored.
        assert_eq!(count_param_placeholders("SELECT '?' AS x WHERE a = ?"), 1);
        assert_eq!(count_param_placeholders("SELECT \"a?b\", `c?d` FROM t"), 0);
        assert_eq!(count_param_placeholders("SELECT 'it''s ? here' FROM t"), 0);
    }

    #[test]
    fn substitutes_positional_params() {
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        let out = substitute_params(sql, &["1".into(), "'x'".into()]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'x'");
    }

    #[test]
    fn substitution_preserves_quoted_question_marks() {
        let sql = "SELECT '?' , ? FROM t";
        let out = substitute_params(sql, &["42".into()]).unwrap();
        assert_eq!(out, "SELECT '?' , 42 FROM t");
    }

    #[test]
    fn substitution_rejects_count_mismatch() {
        let sql = "SELECT * FROM t WHERE a = ?";
        assert!(substitute_params(sql, &[]).is_err());
        assert!(substitute_params(sql, &["1".into(), "2".into()]).is_err());
    }

    #[test]
    fn quotes_and_escapes_strings() {
        assert_eq!(quote_sql_string("abc"), "'abc'");
        assert_eq!(quote_sql_string("a'b"), "'a''b'");
        assert_eq!(quote_sql_string("a\\b"), "'a\\\\b'");
    }

    // mysql_native_password verification lives in kokedb_common::auth and is
    // tested there.
}

/// Runs the MySQL wire server on `bind_addr` using already-initialized shared
/// services, so it can share one `SharedServices` (and its single sync
/// scheduler) with other front-ends in a unified process.
pub async fn serve(shared: SharedServices, bind_addr: String) -> Result<(), MysqlServerError> {
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("kokedb MySQL server listening on {}", bind_addr);

    // Auth is dynamic: enforced whenever `system.app_user` is non-empty,
    // re-checked per connection so CREATE USER takes effect without a restart.
    if shared.meta().count_app_users().await.unwrap_or(0) > 0 {
        info!("MySQL authentication enabled (system.app_user)");
    } else {
        warn!(
            "MySQL authentication disabled (no rows in system.app_user; \
             seed via KOKEDB_ADMIN_USER/PASSWORD or CREATE USER)"
        );
    }

    let result_cache = shared.result_cache();
    let singleflight = singleflight::Singleflight::new();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let cache = result_cache.clone();
        let sf = singleflight.clone();
        let ctx = match create_session_context(&shared) {
            Ok(ctx) => Arc::new(ctx),
            Err(e) => {
                error!("Failed to create session context for connection: {}", e);
                continue;
            }
        };
        let meta = shared.meta();
        let plan_cache = shared.plan_cache();
        tokio::spawn(async move {
            metrics::METRICS
                .active_connections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ = AsyncMysqlIntermediary::run_on(
                CoreContex::new(ctx, cache, sf, meta, plan_cache),
                r,
                w,
            )
            .await;
            metrics::METRICS
                .active_connections
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }
}
