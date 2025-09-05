use std::{io, sync::Arc};

use datafusion::{
    physical_plan::{common::collect, execute_stream},
    prelude::SessionContext,
};
use kokedb_plan::{config::PlanConfig, resolve_and_execute_plan};
use kokedb_query::{binder::*, context::create_session_context};
use opensrv_mysql::*;
use tokio::{io::AsyncWrite, net::TcpListener};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use opensrv_mysql::{Column, ColumnType};

#[derive(Clone)]
struct Backend {
    ctx: Arc<SessionContext>,
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
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

        let plan = plan_sql(sql).unwrap();
        let ctx = self.ctx.clone();
        let default_plan_config = PlanConfig::default();

        let df_plan = resolve_and_execute_plan(&ctx, Arc::new(default_plan_config), plan)
            .await
            .unwrap();
        let batches = execute_stream(df_plan, ctx.task_ctx())?;
        let batches = collect(batches).await?;

        let schema = batches[0].schema();
        let columns: Vec<Column> = schema
            .fields()
            .iter()
            .map(|field| Column {
                table: String::new(),
                column: field.name().to_string(),
                coltype: match field.data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                        ColumnType::MYSQL_TYPE_LONG
                    }
                    DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
                    DataType::Float32 | DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
                    DataType::Utf8 => ColumnType::MYSQL_TYPE_STRING,
                    _ => ColumnType::MYSQL_TYPE_STRING, // 默认使用字符串类型
                },
                colflags: if field.is_nullable() {
                    ColumnFlags::empty()
                } else {
                    ColumnFlags::NOT_NULL_FLAG
                },
            })
            .collect();

        let mut writer = results.start(&columns).await?;

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row: Vec<_> = (0..batch.num_columns())
                    .map(|col_idx| {
                        let col = batch.column(col_idx);
                        match col.data_type() {
                            DataType::Int32 => col
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                                .map(|arr| arr.value(row_idx).to_string())
                                .unwrap_or_default(),
                            DataType::Int64 => col
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                                .map(|arr| arr.value(row_idx).to_string())
                                .unwrap_or_default(),
                            DataType::Float64 => col
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                                .map(|arr| arr.value(row_idx).to_string())
                                .unwrap_or_default(),
                            DataType::Utf8 => col
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                                .map(|arr| arr.value(row_idx).to_string())
                                .unwrap_or_default(),
                            DataType::LargeUtf8 => col
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                                .map(|arr| arr.value(row_idx).to_string())
                                .unwrap_or_default(),
                            _ => String::new(),
                        }
                    })
                    .collect();

                writer.write_row(row.iter().map(|s| s.as_str())).await?;
            }
        }

        writer.finish_with_info("Query executed successfully").await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3306").await.unwrap();
    let ctx = create_session_context().await.unwrap();

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let ctx = Arc::new(ctx.clone());
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(Backend { ctx }, r, w).await });
    }
}
