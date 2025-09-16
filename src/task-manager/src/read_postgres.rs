use anyhow::{Context, Result};
use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::parquet;
use futures_util::TryStreamExt;
use kokedb_common::file::ensure_dir_exists;
use log::warn;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rust_decimal::Decimal;
use sqlx::{
    postgres::{PgPool, PgPoolOptions, PgRow},
    Column, Row, TypeInfo, ValueRef,
};
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

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
        let batch = self.convert_rows_to_record_batch(pg_rows).await?;
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

        println!("Successfully converted to {}", output_path);
        Ok(arrow_schema)
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<Schema> {
        let parts: Vec<&str> = table_name.split('.').collect();
        let (schema_name, table_name) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", table_name)
        };

        let query = r#"
            SELECT 
                column_name,
                data_type,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2 
            ORDER BY ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(schema_name)
            .bind(table_name)
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch table schema")?;

        let mut fields = Vec::new();

        for row in rows {
            let column_name: String = row.get("column_name");
            let data_type: String = row.get("data_type");
            let is_nullable: String = row.get("is_nullable");
            let nullable = is_nullable == "YES";

            let arrow_type = self.pg_type_to_arrow_type(&data_type, &row)?;

            fields.push(Field::new(column_name, arrow_type, nullable));
        }

        Ok(Schema::new(fields))
    }

    fn pg_type_to_arrow_type(&self, pg_type: &str, row: &PgRow) -> Result<DataType> {
        match pg_type {
            "smallint" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "bigint" => Ok(DataType::Int64),
            "real" => Ok(DataType::Float32),
            "double precision" => Ok(DataType::Float64),
            "boolean" => Ok(DataType::Boolean),
            "text" | "varchar" | "character varying" => Ok(DataType::Utf8),
            "character" | "char" => {
                let length: Option<i32> = row.try_get("character_maximum_length").ok();
                match length {
                    Some(len) => Ok(DataType::FixedSizeBinary(len)),
                    None => Ok(DataType::Utf8),
                }
            }
            "uuid" => Ok(DataType::Utf8),
            "date" => Ok(DataType::Date32),
            "timestamp" | "timestamp without time zone" => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            "timestamp with time zone" | "timestamptz" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            "time" | "time without time zone" => Ok(DataType::Time64(TimeUnit::Microsecond)),
            "bytea" => Ok(DataType::Binary),
            "json" | "jsonb" => Ok(DataType::Utf8),
            "numeric" | "decimal" => {
                let precision: Option<i32> = row.try_get("numeric_precision").ok();
                let scale: Option<i32> = row.try_get("numeric_scale").ok();
                match (precision, scale) {
                    (Some(p), Some(s)) => Ok(DataType::Decimal128(p as u8, s as i8)),
                    _ => Ok(DataType::Utf8),
                }
            }
            "array" => Ok(DataType::Utf8),
            _ => {
                println!(
                    "Warning: Unsupported PostgreSQL type '{}', using String",
                    pg_type
                );
                Ok(DataType::Utf8)
            }
        }
    }

    async fn convert_rows_to_record_batch(&self, rows: &Vec<PgRow>) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Err(anyhow::anyhow!("No rows to convert"));
        }

        let columns = rows[0].columns();
        let mut arrow_arrays: Vec<ArrayRef> = Vec::new();
        let mut fields = Vec::new();

        for column in columns {
            let column_name = column.name();
            let type_info = column.type_info();

            let (arrow_array, field) = self
                .convert_column_data(&rows, column_name, type_info)
                .await?;
            arrow_arrays.push(arrow_array);
            fields.push(field);
        }

        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, arrow_arrays).context("Failed to create RecordBatch")
    }

    async fn convert_column_data(
        &self,
        rows: &[PgRow],
        column_name: &str,
        type_info: &sqlx::postgres::PgTypeInfo,
    ) -> Result<(ArrayRef, Field)> {
        let type_name = type_info.name();

        match type_name {
            "INT2" => {
                let values: Vec<Option<i16>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<i16>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = Int16Array::from(values);
                let field = Field::new(column_name, DataType::Int16, true);
                Ok((Arc::new(array), field))
            }
            "INT4" => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<i32>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = Int32Array::from(values);
                let field = Field::new(column_name, DataType::Int32, true);
                Ok((Arc::new(array), field))
            }
            "INT8" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<i64>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = Int64Array::from(values);
                let field = Field::new(column_name, DataType::Int64, true);
                Ok((Arc::new(array), field))
            }
            "FLOAT4" => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<f32>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = Float32Array::from(values);
                let field = Field::new(column_name, DataType::Float32, true);
                Ok((Arc::new(array), field))
            }
            "FLOAT8" => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<f64>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = Float64Array::from(values);
                let field = Field::new(column_name, DataType::Float64, true);
                Ok((Arc::new(array), field))
            }
            "BOOL" => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|row| row.try_get::<Option<bool>, _>(column_name).unwrap_or(None))
                    .collect();
                let array = BooleanArray::from(values);
                let field = Field::new(column_name, DataType::Boolean, true);
                Ok((Arc::new(array), field))
            }
            "TEXT" | "VARCHAR" | "BPCHAR" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<String>, _>(column_name)
                            .unwrap_or(None)
                    })
                    .collect();
                let array = StringArray::from(values);
                let field = Field::new(column_name, DataType::Utf8, true);
                Ok((Arc::new(array), field))
            }
            "UUID" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<Uuid>, _>(column_name)
                            .unwrap_or(None)
                            .map(|u| u.to_string())
                    })
                    .collect();
                let array = StringArray::from(values);
                let field = Field::new(column_name, DataType::Utf8, true);
                Ok((Arc::new(array), field))
            }
            "DATE" => {
                let values: Vec<Option<i32>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<NaiveDate>, _>(column_name)
                            .unwrap_or(None)
                            .map(|d| {
                                (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                            })
                    })
                    .collect();
                let array = Date32Array::from(values);
                let field = Field::new(column_name, DataType::Date32, true);
                Ok((Arc::new(array), field))
            }
            "TIMESTAMP" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<NaiveDateTime>, _>(column_name)
                            .unwrap_or(None)
                            .map(|dt| dt.and_utc().timestamp_micros())
                    })
                    .collect();
                let array = TimestampMicrosecondArray::from(values);
                let field = Field::new(
                    column_name,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                );
                Ok((Arc::new(array), field))
            }
            "TIMESTAMPTZ" => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<DateTime<Utc>>, _>(column_name)
                            .unwrap_or(None)
                            .map(|dt| dt.timestamp_micros())
                    })
                    .collect();
                let array = TimestampMicrosecondArray::from(values);
                let field = Field::new(
                    column_name,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                );
                Ok((Arc::new(array), field))
            }
            "NUMERIC" => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<Decimal>, _>(column_name)
                            .unwrap_or(None)
                            .map(|d| d.to_string())
                    })
                    .collect();
                let array = StringArray::from(values);
                let field = Field::new(column_name, DataType::Utf8, true);
                Ok((Arc::new(array), field))
            }
            "BYTEA" => {
                let byte_vecs: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|row| {
                        row.try_get::<Option<Vec<u8>>, _>(column_name)
                            .unwrap_or(None)
                    })
                    .collect();

                let values: Vec<Option<&[u8]>> =
                    byte_vecs.iter().map(|opt_vec| opt_vec.as_deref()).collect();

                let array = BinaryArray::from_opt_vec(values);
                let field = Field::new(column_name, DataType::Binary, true);
                Ok((Arc::new(array), field))
            }
            _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        if let Ok(string_val) = row.try_get::<Option<String>, _>(column_name) {
                            string_val
                        } else if let Ok(raw_value) = row.try_get_raw(column_name) {
                            if raw_value.type_info().name() == "NULL" {
                                None
                            } else {
                                if let Ok(val) = row.try_get::<Option<i64>, _>(column_name) {
                                    val.map(|v| v.to_string())
                                } else if let Ok(val) = row.try_get::<Option<f64>, _>(column_name) {
                                    val.map(|v| v.to_string())
                                } else if let Ok(val) = row.try_get::<Option<bool>, _>(column_name)
                                {
                                    val.map(|v| v.to_string())
                                } else {
                                    Some(format!(
                                        "Unsupported type: {}",
                                        raw_value.type_info().name()
                                    ))
                                }
                            }
                        } else {
                            None
                        }
                    })
                    .collect();
                let array = StringArray::from(values);
                let field = Field::new(column_name, DataType::Utf8, true);
                Ok((Arc::new(array), field))
            }
        }
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
