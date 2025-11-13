pub mod column;
pub mod error;
pub mod row;

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
    context::create_session_context,
};
use log::{error, info, warn};
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

#[derive(Clone)]
struct CoreContex {
    ctx: Arc<SessionContext>,
    cache: LruResultCache,
}

// Type aliases for clarity
type CacheKey = u64;
type BatchStream = std::pin::Pin<Box<dyn RecordBatchStream + Send>>;
type Cache = LruResultCache;
type Context = datafusion::prelude::SessionContext;

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for CoreContex {
    type Error = MysqlServerError;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        info.reply(42, &[], &[])
            .await
            .map_err(|x| MysqlServerError::InternalError(x.to_string()))
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        results
            .completed(OkResponse::default())
            .await
            .map_err(|x| MysqlServerError::InternalError(x.to_string()))
    }

    async fn on_close(&mut self, _: u32) {}

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
        let instant = std::time::Instant::now();
        let ctx = self.ctx.clone();
        let cache = self.cache.clone();

        // Step 1: Parse SQL and generate plan
        let (plan, cache_key) = match parse_sql_and_get_plan(sql) {
            Ok(result) => result,
            Err((error_kind, error_msg)) => {
                return send_error_to_client(results, error_kind, error_msg).await;
            }
        };

        // Step 2: Get batch stream from cache or query
        let mut batch_stream = match get_batch_stream(&cache, ctx, &plan, cache_key).await {
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
}

async fn send_error_to_client<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
    error_kind: ErrorKind,
    error_msg: String,
) -> Result<(), MysqlServerError> {
    error!("{}", error_msg);
    results
        .error(error_kind, error_msg.as_bytes())
        .await
        .map_err(|io_err| MysqlServerError::WriteMysqlResultError(io_err.to_string()))?;
    Ok(())
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

fn should_cache_plan(plan: &Plan) -> bool {
    matches!(plan, Plan::Query(_))
}

fn spawn_cache_task(
    cache: Cache,
    cache_key: CacheKey,
    rx: tokio::sync::mpsc::UnboundedReceiver<RecordBatch>,
) {
    tokio::spawn(async move {
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
) -> Result<BatchStream, String> {
    info!("Not hitted cache, execute query from kokedb");
    let query_stream = query(ctx, plan, cache_key)
        .await
        .map_err(|e| format!("Query execution error: {}", e))?;

    if !should_cache_plan(plan) {
        return Ok(query_stream);
    }

    let schema = query_stream.schema();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn background task to collect and cache results
    let _ret = spawn_cache_task(cache, cache_key, rx);

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

// Get or create batch stream from cache or query
async fn get_batch_stream(
    cache: &Cache,
    ctx: Arc<Context>,
    plan: &Plan,
    cache_key: CacheKey,
) -> Result<BatchStream, String> {
    // Try cache first, command plan not need cache.
    if should_cache_plan(plan) {
        if let Some(stream) = try_get_from_cache(cache, cache_key).await {
            return Ok(stream);
        }
    }

    // Cache miss or error, execute query
    execute_query_with_cache(ctx, plan, cache_key, cache.clone()).await
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

#[tokio::main]
async fn main() -> Result<(), MysqlServerError> {
    init_logger().unwrap();
    let listener = TcpListener::bind("0.0.0.0:3306").await.unwrap();
    let result_cache = LruResultCache::new(2000, 40000).await?;
    let ctx = create_session_context(result_cache.clone()).await.unwrap();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let ctx = Arc::new(ctx.clone());
        let cache = result_cache.clone();
        tokio::spawn(async move {
            AsyncMysqlIntermediary::run_on(CoreContex { ctx, cache }, r, w).await
        });
    }
}
