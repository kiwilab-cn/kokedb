use anyhow::Context;
use arrow::datatypes::*;
use datafusion::parquet;
use futures_util::TryStreamExt;
use kokedb_common::{
    env::get_env_as,
    file::ensure_dir_exists,
    table::postgresql::{get_postgresql_table_schema, rows_to_record_batch},
};
use log::{error, info, warn};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::TaskError;

pub struct PostgresToParquetConverter {
    pool: PgPool,
    batch_size: usize,
    /// Parquet row-group size. Kept smaller than `batch_size` so a written file
    /// holds several row groups, giving DataFusion finer-grained statistics
    /// pruning for selective predicates on the sort column.
    row_group_size: usize,
    compression: parquet::basic::Compression,
}

impl PostgresToParquetConverter {
    pub async fn new(dsn: &str) -> Result<Self, TaskError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(dsn)
            .await
            .map_err(|x| {
                TaskError::DatabaseError(format!(
                    "Failed to create connection pool to postgresql with error: {}",
                    x
                ))
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

    pub fn with_row_group_size(mut self, row_group_size: usize) -> Self {
        self.row_group_size = row_group_size;
        self
    }

    pub fn with_compression(mut self, compression: parquet::basic::Compression) -> Self {
        self.compression = compression;
        self
    }

    async fn write_parquet_file(
        &self,
        output_path: &str,
        pg_rows: &Vec<PgRow>,
        props: &WriterProperties,
        arrow_schema: Arc<Schema>,
    ) -> Result<(), TaskError> {
        if pg_rows.is_empty() {
            warn!("Found empty pg_rows when write parquet.");
            return Ok(());
        }

        let _ = ensure_dir_exists(output_path).await?;
        let random_parquet_name = Uuid::new_v4().to_string()[..8].to_string();
        let parquet_file_name = format!("{}/{}.parquet", output_path, random_parquet_name);

        let batch = rows_to_record_batch(&pg_rows, &arrow_schema).map_err(|x| {
            TaskError::RecordBatchError(format!(
                "Failed to translate pgrow to record batch with error: {}",
                x
            ))
        })?;
        info!(
            "RecordBatch created: {} rows, {} columns to file: {}",
            batch.num_rows(),
            batch.num_columns(),
            &parquet_file_name,
        );

        let file = File::create(parquet_file_name.clone())?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.clone()))
            .context("Failed to create Parquet writer")
            .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;

        writer
            .write(&batch)
            .context(format!("Failed to write batch to: {}", &parquet_file_name))
            .map_err(|e| {
                error!("Failed to write batch to parquet file with error: {:?}", e);
                TaskError::WriteParquetError(e.to_string())
            })?;

        writer
            .close()
            .context(format!(
                "Failed to close writer for: {}",
                &parquet_file_name
            ))
            .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;

        info!("Successfully wrote parquet file: {}", &parquet_file_name);
        Ok(())
    }

    pub async fn convert_table_to_parquet(
        &self,
        table_name: &str,
        output_path: &str,
    ) -> Result<Arc<Schema>, TaskError> {
        self.convert_table_to_parquet_where(table_name, output_path, None, None)
            .await
    }

    /// Like [`convert_table_to_parquet`], but with an optional `WHERE` clause
    /// (incremental delta fetch) and an optional sort column. Sorting clusters
    /// the snapshot so each parquet file/row-group has a tight value range,
    /// letting DataFusion prune them for selective predicates on that column.
    pub async fn convert_table_to_parquet_where(
        &self,
        table_name: &str,
        output_path: &str,
        where_clause: Option<&str>,
        sort_column: Option<&str>,
    ) -> Result<Arc<Schema>, TaskError> {
        let schema = self.get_table_schema(table_name).await?;
        let arrow_schema = Arc::new(schema);

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .build();

        let where_sql = match where_clause {
            Some(clause) => format!(" WHERE {}", clause),
            None => String::new(),
        };
        let order_sql = match sort_column {
            // Identifier is quoted; it comes from PostgreSQL catalog metadata.
            Some(col) => format!(" ORDER BY \"{}\"", col.replace('"', "\"\"")),
            None => String::new(),
        };
        let query = format!("SELECT * FROM {}{}{}", table_name, where_sql, order_sql);

        let mut rows = sqlx::query(&query).fetch(&self.pool);
        let mut pg_rows = Vec::with_capacity(self.batch_size);

        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|x| TaskError::DatabaseError(x.to_string()))?
        {
            pg_rows.push(row);
            if pg_rows.len() >= self.batch_size {
                self.write_parquet_file(output_path, &pg_rows, &props, arrow_schema.clone())
                    .await
                    .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;

                pg_rows.clear();
            }
        }

        if !pg_rows.is_empty() {
            self.write_parquet_file(output_path, &pg_rows, &props, arrow_schema.clone())
                .await
                .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;
        }

        info!(
            "Successfully converted table {} to {}",
            table_name, output_path
        );
        Ok(arrow_schema)
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<Schema, TaskError> {
        let parts: Vec<&str> = table_name.split('.').collect();
        let (schema_name, table_name) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", table_name)
        };

        get_postgresql_table_schema(&self.pool, schema_name, table_name)
            .await
            .map_err(|x| {
                TaskError::Internal(format!("Failed to get arrow schema with error:{}", x))
            })
    }
}

pub async fn convert_postgres_to_parquet(
    dsn: &str,
    remote_table: &str,
    output_path: &str,
    sort_column: Option<&str>,
) -> Result<Arc<Schema>, TaskError> {
    // Write fewer, larger files (default 300k rows each) so per-file overhead is
    // small and pruning operates over multiple row groups per file.
    let batch_size = get_env_as("KOKEDB_READ_TABLE_BATCH_SIZE", 300000usize);
    let converter = PostgresToParquetConverter::new(dsn)
        .await?
        .with_batch_size(batch_size);
    let schema = converter
        .convert_table_to_parquet_where(remote_table, output_path, None, sort_column)
        .await
        .map_err(|x| TaskError::WriteParquetError(x.to_string()))?;

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    #[ignore = "requires PostgreSQL; run via `make integration-test`"]
    async fn test_conversion() -> Result<(), TaskError> {
        let dsn = std::env::var("KOKEDB_TEST_DSN")
            .unwrap_or_else(|_| "postgresql://postgres:123456@127.0.0.1:25432/postgres".to_string());
        let dsn = dsn.as_str();
        let table_name = "public.test1";
        let output_path = "/tmp/test1/";

        convert_postgres_to_parquet(dsn, table_name, output_path, None).await?;

        assert!(Path::new(output_path).exists());
        Ok(())
    }
}
