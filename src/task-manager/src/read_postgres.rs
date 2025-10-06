use anyhow::{Context, Result};
use arrow::datatypes::*;
use datafusion::parquet;
use futures_util::TryStreamExt;
use kokedb_common::{
    file::ensure_dir_exists,
    table::postgresql::{get_postgresql_table_schema, rows_to_record_batch},
};
use log::{info, warn};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::TaskError;

pub struct PostgresToParquetConverter {
    pool: PgPool,
    batch_size: usize,
    compression: parquet::basic::Compression,
}

impl PostgresToParquetConverter {
    pub async fn new(dsn: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(dsn)
            .await?;

        Ok(Self {
            pool,
            batch_size: 10000,
            compression: parquet::basic::Compression::SNAPPY,
        })
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
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
    ) -> Result<()> {
        if pg_rows.is_empty() {
            warn!("Found empty pg_rows when write parquet.");
            return Ok(());
        }

        let _ = ensure_dir_exists(output_path)?;
        let random_parquet_name = Uuid::new_v4().to_string()[..8].to_string();
        let parquet_file_name = format!("{}/{}.parquet", output_path, random_parquet_name);

        let file = File::create(parquet_file_name)?;
        let batch = rows_to_record_batch(&pg_rows, &arrow_schema)?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props.clone()))
            .context("Failed to create Parquet writer")?;

        writer
            .write(&batch)
            .context("Failed to write batch to Parquet")?;

        writer.close().context("Failed to close Parquet writer")?;

        Ok(())
    }

    pub async fn convert_table_to_parquet(
        &self,
        table_name: &str,
        output_path: &str,
    ) -> Result<Arc<Schema>> {
        let schema = self.get_table_schema(table_name).await?;
        let arrow_schema = Arc::new(schema);

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.batch_size)
            .build();

        let query = format!("SELECT * FROM {}", table_name);

        let mut rows = sqlx::query(&query).fetch(&self.pool);
        let mut pg_rows = Vec::with_capacity(self.batch_size);

        while let Some(row) = rows.try_next().await? {
            pg_rows.push(row);
            if pg_rows.len() >= self.batch_size {
                self.write_parquet_file(output_path, &pg_rows, &props, arrow_schema.clone())
                    .await?;

                pg_rows.clear();
            }
        }

        if !pg_rows.is_empty() {
            self.write_parquet_file(output_path, &pg_rows, &props, arrow_schema.clone())
                .await?;
        }

        info!("Successfully converted to {}", output_path);
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
) -> Result<Arc<Schema>> {
    let converter = PostgresToParquetConverter::new(dsn).await?;
    let schema = converter
        .convert_table_to_parquet(remote_table, output_path)
        .await?;

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn test_conversion() -> Result<()> {
        let dsn = "postgresql://postgres:123456@192.168.0.227:25432/postgres";
        let table_name = "public.test1";
        let output_path = "/tmp/test1/";

        convert_postgres_to_parquet(dsn, table_name, output_path).await?;

        assert!(Path::new(output_path).exists());
        Ok(())
    }
}
