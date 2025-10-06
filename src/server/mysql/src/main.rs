pub mod column;
pub mod error;
pub mod row;

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use kokedb_common::opentelemetry::init_logger;
use kokedb_query::{binder::query, context::create_session_context};
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

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MysqlServerError> {
        let ctx = self.ctx.clone();

        let query_result = query(ctx, sql).await;
        if query_result.is_err() {
            let error = query_result.err().unwrap();
            let (kind, error_mesg) = to_mysql_error(&error);
            return results
                .error(kind, error_mesg.as_bytes())
                .await
                .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()));
        }

        let batches = query_result.unwrap();
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

        writer
            .finish_with_info("Query executed successfully")
            .await
            .map_err(|x| MysqlServerError::WriteMysqlResultError(x.to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<(), MysqlServerError> {
    init_logger().unwrap();
    let listener = TcpListener::bind("0.0.0.0:3306").await.unwrap();
    let ctx = create_session_context().await.unwrap();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let ctx = Arc::new(ctx.clone());
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(CoreContex { ctx }, r, w).await });
    }
}
