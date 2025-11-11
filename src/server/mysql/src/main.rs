pub mod column;
pub mod error;
pub mod row;

use std::sync::Arc;

use crate::{column::compact_columns, error::MysqlServerError, row::compact_batch_rows};
use datafusion::{
    arrow::array::RecordBatch, physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
};
use futures::StreamExt;
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::{hash::get_plan_hash, opentelemetry::init_logger};
use kokedb_query::{
    binder::{parser, query, save_sql_history},
    context::create_session_context,
};
use log::{error, info};
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

#[derive(Clone)]
struct CoreContex {
    ctx: Arc<SessionContext>,
    cache: LruResultCache,
}

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

        let plan = parser(sql)?;
        let cache_key = get_plan_hash(&plan)?;

        let mut batch_stream = if cache.inner.contains(&cache_key) {
            cache
                .get(cache_key)
                .await
                .map_err(|e| MysqlServerError::from(e))?
        } else {
            let query_stream = query(ctx, &plan, cache_key)
                .await
                .map_err(|e| MysqlServerError::from(e))?;
            let schema = query_stream.schema();

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

            let cache_clone = cache.clone();
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
                match cache_clone.insert(cache_key, &collected_batches).await {
                    Ok(()) => info!("Successfully cached query results"),
                    Err(e) => error!("Failed to cache query results: {}", e),
                }
            });

            let adapted_stream = query_stream.map(move |batch_result| {
                if let Ok(ref batch) = batch_result {
                    let _ = tx.send(batch.clone());
                }
                batch_result
            });

            Box::pin(RecordBatchStreamAdapter::new(schema, adapted_stream))
        };

        let first_batch = match batch_stream.next().await {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(MysqlServerError::DataFusionError(e.to_string())),
            None => {
                let writer = results
                    .start(&[])
                    .await
                    .map_err(|x| MysqlServerError::CreateMysqlResultWriterError(x.to_string()))?;
                writer
                    .finish_with_info("Query executed successfully")
                    .await
                    .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))?;

                let cost = instant.elapsed().as_millis() as u64;
                if let Err(e) = save_sql_history(sql, &plan, cost).await {
                    error!("Failed to store sql execute info: {:?}", e);
                }
                return Ok(());
            }
        };

        let schema = first_batch.schema();
        let columns = compact_columns(schema)?;

        let mut writer = results
            .start(&columns)
            .await
            .map_err(|x| MysqlServerError::CreateMysqlResultWriterError(x.to_string()))?;

        write_batch_to_mysql(&mut writer, &first_batch).await?;

        while let Some(batch_result) = batch_stream.next().await {
            let batch =
                batch_result.map_err(|e| MysqlServerError::DataFusionError(e.to_string()))?;

            write_batch_to_mysql(&mut writer, &batch).await?;
        }

        writer
            .finish_with_info("Query executed successfully")
            .await
            .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))?;

        let cost = instant.elapsed().as_millis() as u64;
        if let Err(e) = save_sql_history(sql, &plan, cost).await {
            error!("Failed to store sql execute info: {:?}", e);
        }

        Ok(())
    }
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
