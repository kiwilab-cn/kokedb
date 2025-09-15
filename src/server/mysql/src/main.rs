pub mod column;
pub mod row;

use std::{io, sync::Arc};

use datafusion::{
    physical_plan::{common::collect, execute_stream},
    prelude::SessionContext,
};
use kokedb_common::opentelemetry::init_logger;
use kokedb_plan::{config::PlanConfig, resolve_and_execute_plan};
use kokedb_query::{binder::*, context::create_session_context};
use kokedb_task_manager::task::TaskManager;
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

use crate::{column::compact_columns, row::compact_rows};

#[derive(Clone)]
struct CoreContex {
    ctx: Arc<SessionContext>,
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for CoreContex {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("sql: {}", sql);
        // TODO: remove unwrap.
        let plan = plan_sql(sql).unwrap();
        let ctx = self.ctx.clone();
        let default_plan_config = PlanConfig::default();

        let df_plan = resolve_and_execute_plan(&ctx, Arc::new(default_plan_config), plan)
            .await
            .unwrap();
        let batches = execute_stream(df_plan, ctx.task_ctx())?;
        let batches = collect(batches).await?;

        let schema = batches[0].schema();

        let columns = compact_columns(schema)?;

        let mut writer = results.start(&columns).await?;

        let rows: Vec<Vec<String>> = compact_rows(batches)?;
        for row in rows {
            writer.write_row(row.iter().map(|s| s.as_str())).await?;
        }

        writer.finish_with_info("Query executed successfully").await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger()?;
    let listener = TcpListener::bind("0.0.0.0:3306").await.unwrap();
    let ctx = create_session_context().await.unwrap();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let ctx = Arc::new(ctx.clone());
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(CoreContex { ctx }, r, w).await });
    }
}
