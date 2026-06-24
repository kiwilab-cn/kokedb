use anyhow::Context;
use arrow::datatypes::Schema;
use datafusion::parquet;
use futures_util::TryStreamExt;
use kokedb_common::{
    env::get_env_as,
    file::ensure_dir_exists,
    table::mysql::{get_mysql_table_schema, mysql_rows_to_record_batch},
};
use log::{error, info, warn};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::TaskError;

/// The MySQL counterpart of `PostgresToParquetConverter`: streams a table from
/// MySQL and writes it as parquet, used by the full-sync path.
pub struct MysqlToParquetConverter {
    pool: MySqlPool,
    batch_size: usize,
    row_group_size: usize,
    compression: parquet::basic::Compression,
}

impl MysqlToParquetConverter {
    pub async fn new(dsn: &str) -> Result<Self, TaskError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(dsn)
            .await
            .map_err(|x| {
                TaskError::DatabaseError(format!("Failed to connect to MySQL: {x}"))
            })?;
        Ok(Self {
            pool,
            batch_size: 10000,
            row_group_size: 65536,
            compression: parquet::basic::Compression::SNAPPY,
        })
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    async fn write_parquet_file(
        &self,
        output_path: &str,
        rows: &[MySqlRow],
        props: &WriterProperties,
        arrow_schema: Arc<Schema>,
    ) -> Result<(), TaskError> {
        if rows.is_empty() {
            warn!("Found empty MySQL rows when writing parquet.");
            return Ok(());
        }
        ensure_dir_exists(output_path).await?;
        let name = Uuid::new_v4().to_string()[..8].to_string();
        let file_name = format!("{}/{}.parquet", output_path, name);

        let batch = mysql_rows_to_record_batch(rows, &arrow_schema)
            .map_err(|x| TaskError::RecordBatchError(format!("MySQL row -> batch failed: {x}")))?;

        let file = File::create(&file_name)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.clone()))
            .context("Failed to create Parquet writer")
            .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;
        writer
            .write(&batch)
            .context(format!("Failed to write batch to: {}", &file_name))
            .map_err(|e| {
                error!("Failed to write MySQL batch to parquet: {e:?}");
                TaskError::WriteParquetError(e.to_string())
            })?;
        writer
            .close()
            .context(format!("Failed to close writer for: {}", &file_name))
            .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;
        info!("Successfully wrote parquet file: {}", &file_name);
        Ok(())
    }

    /// Streams `table_name` (`db.table` or bare table) to parquet under
    /// `output_path`, optionally sorted by `sort_column` for prune-friendly files.
    pub async fn convert_table_to_parquet_where(
        &self,
        table_name: &str,
        output_path: &str,
        sort_column: Option<&str>,
    ) -> Result<Arc<Schema>, TaskError> {
        let arrow_schema = Arc::new(self.get_table_schema(table_name).await?);
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .build();

        let order_sql = match sort_column {
            Some(col) => format!(" ORDER BY {}", quote_ident(col)),
            None => String::new(),
        };
        let query = format!("SELECT * FROM {}{}", quote_table(table_name), order_sql);

        let mut stream = sqlx::query(&query).fetch(&self.pool);
        let mut rows: Vec<MySqlRow> = Vec::with_capacity(self.batch_size);
        while let Some(row) = stream
            .try_next()
            .await
            .map_err(|x| TaskError::DatabaseError(x.to_string()))?
        {
            rows.push(row);
            if rows.len() >= self.batch_size {
                self.write_parquet_file(output_path, &rows, &props, arrow_schema.clone())
                    .await?;
                rows.clear();
            }
        }
        if !rows.is_empty() {
            self.write_parquet_file(output_path, &rows, &props, arrow_schema.clone())
                .await?;
        }
        info!("Successfully converted MySQL table {} to {}", table_name, output_path);
        Ok(arrow_schema)
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<Schema, TaskError> {
        let (db, table) = split_db_table(table_name);
        // No database in the qualified name -> fall back to the connection's
        // default schema (DATABASE()).
        let db = if db.is_empty() {
            sqlx::query_scalar::<_, Option<String>>("SELECT DATABASE()")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| TaskError::DatabaseError(e.to_string()))?
                .unwrap_or_default()
        } else {
            db.to_string()
        };
        get_mysql_table_schema(&self.pool, &db, table)
            .await
            .map_err(|x| TaskError::Internal(format!("Failed to get MySQL arrow schema: {x}")))
    }
}

fn split_db_table(name: &str) -> (&str, &str) {
    match name.split_once('.') {
        Some((db, t)) => (db, t),
        None => ("", name),
    }
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn quote_table(name: &str) -> String {
    let (db, t) = split_db_table(name);
    if db.is_empty() {
        quote_ident(t)
    } else {
        format!("{}.{}", quote_ident(db), quote_ident(t))
    }
}

/// Dispatches a full table sync to the right connector by DSN scheme.
pub async fn convert_remote_to_parquet(
    dsn: &str,
    remote_table: &str,
    output_path: &str,
    sort_column: Option<&str>,
) -> Result<Arc<Schema>, TaskError> {
    match kokedb_common::source::source_kind(dsn) {
        kokedb_common::source::SourceKind::Mysql => {
            convert_mysql_to_parquet(dsn, remote_table, output_path, sort_column).await
        }
        kokedb_common::source::SourceKind::Postgres => {
            crate::read_postgres::convert_postgres_to_parquet(
                dsn,
                remote_table,
                output_path,
                sort_column,
            )
            .await
        }
    }
}

/// Full-sync entry point mirroring `convert_postgres_to_parquet`.
pub async fn convert_mysql_to_parquet(
    dsn: &str,
    remote_table: &str,
    output_path: &str,
    sort_column: Option<&str>,
) -> Result<Arc<Schema>, TaskError> {
    let batch_size = get_env_as("KOKEDB_READ_TABLE_BATCH_SIZE", 300000usize);
    MysqlToParquetConverter::new(dsn)
        .await?
        .with_batch_size(batch_size)
        .convert_table_to_parquet_where(remote_table, output_path, sort_column)
        .await
}
