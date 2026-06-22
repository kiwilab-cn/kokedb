pub mod column;
pub mod error;
pub mod metrics;
pub mod row;
pub mod singleflight;

use std::collections::HashMap;
use std::sync::Arc;

use crate::{column::compact_columns, error::MysqlServerError, row::compact_batch_rows};
use datafusion::{
    arrow::array::RecordBatch, execution::RecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, prelude::SessionContext,
};
use futures::StreamExt;
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::{hash::get_plan_hash, opentelemetry::init_logger, spec::Plan};
use kokedb_query::{
    binder::{parser, query, save_sql_history},
    context::{create_session_context, init_shared_services, use_database},
};
use log::{error, info, warn};
use opensrv_mysql::*;
use sha1::{Digest, Sha1};
use tokio::{io::AsyncWrite, net::TcpListener};

/// Optional MySQL authentication credentials, read once from the environment.
/// When `None`, authentication is disabled (any client is accepted).
#[derive(Clone)]
struct AuthConfig {
    user: String,
    password: String,
}

impl AuthConfig {
    /// Reads credentials from `KOKEDB_MYSQL_USER` (default `root`) and
    /// `KOKEDB_MYSQL_PASSWORD`. Auth is only enforced when a password is set.
    fn from_env() -> Option<Self> {
        match std::env::var("KOKEDB_MYSQL_PASSWORD") {
            Ok(password) if !password.is_empty() => Some(Self {
                user: std::env::var("KOKEDB_MYSQL_USER").unwrap_or_else(|_| "root".to_string()),
                password,
            }),
            _ => None,
        }
    }

    /// Verifies a `mysql_native_password` response:
    ///   token == SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
    fn verify_native_password(&self, salt: &[u8], token: &[u8]) -> bool {
        if token.len() != 20 {
            return false;
        }
        let hash1 = Sha1::digest(self.password.as_bytes());
        let hash2 = Sha1::digest(hash1);
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(hash2);
        let scramble = hasher.finalize();
        // recovered = scramble XOR token should equal hash1 for a valid password.
        scramble
            .iter()
            .zip(token.iter())
            .map(|(s, t)| s ^ t)
            .eq(hash1.iter().copied())
    }
}

/// A prepared statement bound to a single client connection.
#[derive(Clone)]
struct PreparedStatement {
    sql: String,
    param_count: usize,
}

struct CoreContex {
    ctx: Arc<SessionContext>,
    cache: LruResultCache,
    /// Optional auth credentials shared across connections (read once at startup).
    auth: Option<Arc<AuthConfig>>,
    /// Process-wide single-flight registry for cache-miss de-duplication.
    sf: Arc<singleflight::Singleflight>,
    /// Per-connection prepared statement registry, keyed by statement id.
    prepared: HashMap<u32, PreparedStatement>,
    /// Monotonic statement id generator for this connection.
    next_stmt_id: u32,
}

impl CoreContex {
    fn new(
        ctx: Arc<SessionContext>,
        cache: LruResultCache,
        auth: Option<Arc<AuthConfig>>,
        sf: Arc<singleflight::Singleflight>,
    ) -> Self {
        Self {
            ctx,
            cache,
            auth,
            sf,
            prepared: HashMap::new(),
            next_stmt_id: 1,
        }
    }
}

// Type aliases for clarity
type CacheKey = u128;
type BatchStream = std::pin::Pin<Box<dyn RecordBatchStream + Send>>;
type Cache = LruResultCache;
type Context = datafusion::prelude::SessionContext;

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for CoreContex {
    type Error = MysqlServerError;

    /// Validates the client's credentials with `mysql_native_password`. When no
    /// `KOKEDB_MYSQL_PASSWORD` is configured, all clients are accepted.
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        match &self.auth {
            None => true,
            Some(cfg) => {
                if username != cfg.user.as_bytes() {
                    warn!("Auth failed: unknown user");
                    return false;
                }
                let ok = cfg.verify_native_password(salt, auth_data);
                if !ok {
                    warn!("Auth failed: bad password for user");
                }
                ok
            }
        }
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

        run_query(self.ctx.clone(), self.cache.clone(), self.sf.clone(), &sql, results).await
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
        run_query(self.ctx.clone(), self.cache.clone(), self.sf.clone(), sql, results).await
    }
}

/// Shared query execution path used by both text queries (`on_query`) and
/// prepared statement execution (`on_execute`).
async fn run_query<W: AsyncWrite + Send + Unpin>(
    ctx: Arc<SessionContext>,
    cache: Cache,
    sf: Arc<singleflight::Singleflight>,
    sql: &str,
    results: QueryResultWriter<'_, W>,
) -> Result<(), MysqlServerError> {
    let instant = std::time::Instant::now();
    metrics::inc_queries();

    // Step 1: Parse SQL and generate plan
    let (plan, cache_key) = match parse_sql_and_get_plan(sql) {
        Ok(result) => result,
        Err((error_kind, error_msg)) => {
            return send_error_to_client(results, error_kind, error_msg).await;
        }
    };

    // Step 2: Get batch stream from cache or query
    let mut batch_stream = match get_batch_stream(&cache, &sf, ctx, &plan, cache_key).await {
        Ok(stream) => stream,
        Err(error_msg) => {
            return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, error_msg).await;
        }
    };

    // Step 3: Get first batch
    let first_batch = match batch_stream.next().await {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => {
            let error_msg = format!("Error reading first batch: {}", e);
            return send_error_to_client(results, ErrorKind::ER_UNKNOWN_ERROR, error_msg).await;
        }
        None => {
            // Empty result set
            return handle_empty_result(results, sql, &plan, instant).await;
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
    finalize_query(writer, sql, &plan, instant).await?;

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

// Try to retrieve result from cache
async fn try_get_from_cache(cache: &Cache, cache_key: CacheKey) -> Option<BatchStream> {
    if !cache.inner.contains(&cache_key) {
        info!("Cache miss for key: {}, querying from kokedb.", cache_key);
        return None;
    }

    match cache.get(cache_key).await {
        Ok(stream) => {
            info!(
                "Cache hit with key: {}, retrieving result from cache.",
                cache_key
            );
            Some(stream)
        }
        Err(e) => {
            warn!(
                "Cache retrieval failed for key: {}, error: {}, falling back to query.",
                cache_key, e
            );
            None
        }
    }
}

// Parse SQL and generate execution plan
fn parse_sql_and_get_plan(sql: &str) -> Result<(Plan, CacheKey), (ErrorKind, String)> {
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

    Ok((plan, cache_key))
}

fn should_cache_plan(plan: &Plan) -> bool {
    matches!(plan, Plan::Query(_))
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
) -> Result<BatchStream, String> {
    info!("Not hitted cache, execute query from kokedb");
    // On error the `owner` guard drops here, releasing any single-flight waiters.
    let query_stream = query(ctx, plan, cache_key)
        .await
        .map_err(|e| format!("Query execution error: {}", e))?;

    if !should_cache_plan(plan) {
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
) -> Result<BatchStream, String> {
    if !should_cache_plan(plan) {
        // Commands and non-cacheable plans bypass the cache entirely.
        return execute_query_with_cache(ctx, plan, cache_key, cache.clone(), None).await;
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
                return execute_query_with_cache(ctx, plan, cache_key, cache.clone(), Some(guard))
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
    sql: &str,
    plan: &Plan,
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

    let cost = instant.elapsed().as_millis() as u64;
    if let Err(e) = save_sql_history(sql, plan, cost).await {
        error!("Failed to store sql execute info: {:?}", e);
    }

    Ok(())
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
    sql: &str,
    plan: &Plan,
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

    let cost = instant.elapsed().as_millis() as u64;
    if let Err(e) = save_sql_history(sql, plan, cost).await {
        error!("Failed to store sql execute info: {:?}", e);
    }

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

    /// Computes the `mysql_native_password` token a client would send.
    fn client_token(password: &str, salt: &[u8]) -> Vec<u8> {
        let hash1 = Sha1::digest(password.as_bytes());
        let hash2 = Sha1::digest(hash1);
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(hash2);
        let scramble = hasher.finalize();
        scramble
            .iter()
            .zip(hash1.iter())
            .map(|(s, h)| s ^ h)
            .collect()
    }

    #[test]
    fn native_password_accepts_correct_and_rejects_wrong() {
        let cfg = AuthConfig {
            user: "root".to_string(),
            password: "s3cret".to_string(),
        };
        let salt = b"01234567890123456789";
        let good = client_token("s3cret", salt);
        assert!(cfg.verify_native_password(salt, &good));

        let bad = client_token("wrong", salt);
        assert!(!cfg.verify_native_password(salt, &bad));
        // Malformed (wrong length) tokens are rejected.
        assert!(!cfg.verify_native_password(salt, b"short"));
    }
}

#[tokio::main]
async fn main() -> Result<(), MysqlServerError> {
    init_logger().unwrap();

    let bind_addr = std::env::var("KOKEDB_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3306".to_string());
    let listener = TcpListener::bind(&bind_addr).await.unwrap();
    info!("kokedb MySQL server listening on {}", bind_addr);

    let auth = AuthConfig::from_env().map(Arc::new);
    if auth.is_some() {
        info!("MySQL authentication enabled");
    } else {
        warn!("MySQL authentication disabled (set KOKEDB_MYSQL_PASSWORD to enable)");
    }

    // Prometheus metrics endpoint (best-effort; non-fatal if it can't bind).
    let metrics_addr =
        std::env::var("KOKEDB_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9090".to_string());
    if !metrics_addr.is_empty() {
        tokio::spawn(metrics::serve_metrics(metrics_addr));
    }

    let result_cache = LruResultCache::new(2000, 40000).await?;
    // Heavy services (meta store, task manager, scheduler, sync jobs) are created
    // once and shared; each connection gets its own lightweight session context.
    let shared = init_shared_services(result_cache.clone()).await.unwrap();
    let singleflight = singleflight::Singleflight::new();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let cache = result_cache.clone();
        let auth = auth.clone();
        let sf = singleflight.clone();
        let ctx = match create_session_context(&shared) {
            Ok(ctx) => Arc::new(ctx),
            Err(e) => {
                error!("Failed to create session context for connection: {}", e);
                continue;
            }
        };
        tokio::spawn(async move {
            metrics::METRICS
                .active_connections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let _ =
                AsyncMysqlIntermediary::run_on(CoreContex::new(ctx, cache, auth, sf), r, w).await;
            metrics::METRICS
                .active_connections
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }
}
