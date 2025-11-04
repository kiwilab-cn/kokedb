pub mod column;
pub mod error;
pub mod row;

use std::sync::Arc;

use datafusion::{physical_plan::common::collect, prelude::SessionContext};
use kokedb_cache::foyer_hybrid::LruResultCache;
use kokedb_common::{hash::get_plan_hash, opentelemetry::init_logger};
use kokedb_query::{
    binder::{parser, query, save_sql_history},
    context::create_session_context,
};
use log::error;
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

use crate::{
    column::compact_columns,
    error::{to_mysql_error, MysqlServerError},
    row::compact_rows,
};

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
     * sql --> cache hit --> return cache result
     *              |
     *                   --> execute query  --> return result
     *                              |
     *                                      --> save to cache
     *
     * sync table cache task  --> table not used  --> cache table data from remote
     *                                  |
     *                                            --> execute cached query --> update cached result
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
        let query_result_stream = if cache.inner.contains(&cache_key) {
            cache.get(cache_key).await
        } else {
            query(ctx, &plan).await
        };

        if query_result_stream.is_err() {
            let error = query_result_stream.err().unwrap();
            let (kind, error_mesg) = to_mysql_error(&error);
            return results
                .error(kind, error_mesg.as_bytes())
                .await
                .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()));
        }

        let batches = query_result_stream.unwrap();

        tokio::spawn(async move {
            info!("Create insert result to hybrid cache thread.");
            if let Ok(batches) = collect(query_result_stream).await {
                let result = cache.insert(cache_key, &batches).await;
                match result {
                    Ok(()) => {
                        info!("Success to save result to hybrid cache.")
                    }
                    Err(e) => {
                        error!("Failed to save result to hybrid cache with error:{}", e)
                    }
                }
            } else {
                error!("Failed to collect query result stream.")
            }
        });

        let schema = batches[0].schema();

        let columns = compact_columns(schema)?;

        let mut writer = results
            .start(&columns)
            .await
            .map_err(|x| MysqlServerError::CreateMysqlResultWriterError(x.to_string()))?;

        let rows: Vec<Vec<String>> = compact_rows(batches)?;
        for row in rows {
            writer
                .write_row(row.iter().map(|s| s.as_str()))
                .await
                .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))?;
        }

        let _ret = writer
            .finish_with_info("Query executed successfully")
            .await
            .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))?;

        //TODO: check need change to running in thread?
        let cost = instant.elapsed().as_millis() as u64;
        let ret = save_sql_history(sql, &plan, cost).await;
        if ret.is_err() {
            error!(
                "Failed to store sql execute info to meta db with error: {:?}",
                ret.err().unwrap()
            );
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), MysqlServerError> {
    init_logger().unwrap();
    let listener = TcpListener::bind("0.0.0.0:3306").await.unwrap();
    let result_cache = LruResultCache::new(2000, 40000).await?;
    let ctx = create_session_context(result_cache).await.unwrap();

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
