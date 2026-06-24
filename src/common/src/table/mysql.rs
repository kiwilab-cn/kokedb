//! MySQL source connector: schema inference and row → Arrow decoding, the
//! MySQL counterpart of [`super::postgresql`]. Types are mapped so each Arrow
//! column's decode type matches what sqlx yields natively (e.g. unsigned MySQL
//! ints map to Arrow unsigned ints), avoiding lossy casts. Uncommon types are
//! rejected rather than silently mis-decoded.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use sqlx::mysql::{MySqlPool, MySqlRow};
use sqlx::{Decode, Row, Type};

use crate::error::CommonError;

fn internal(msg: impl Into<String>) -> CommonError {
    CommonError::InternalError(msg.into())
}

/// Maps a MySQL `COLUMN_TYPE` (e.g. `int(11) unsigned`, `tinyint(1)`,
/// `decimal(10,2)`, `varchar(255)`) to an Arrow type.
pub fn mysql_type_to_arrow(column_type: &str) -> Result<DataType, CommonError> {
    let ct = column_type.trim().to_ascii_lowercase();
    let unsigned = ct.contains("unsigned");
    let base = ct.split(['(', ' ']).next().unwrap_or(&ct);

    let dt = match base {
        // tinyint(1) is the conventional boolean.
        "tinyint" if ct.starts_with("tinyint(1)") => DataType::Boolean,
        "bool" | "boolean" => DataType::Boolean,
        "tinyint" => uint_or_int(unsigned, DataType::UInt8, DataType::Int8),
        "smallint" => uint_or_int(unsigned, DataType::UInt16, DataType::Int16),
        "mediumint" => uint_or_int(unsigned, DataType::UInt32, DataType::Int32),
        "int" | "integer" => uint_or_int(unsigned, DataType::UInt32, DataType::Int32),
        "bigint" => uint_or_int(unsigned, DataType::UInt64, DataType::Int64),
        "year" => DataType::Int16,
        "float" => DataType::Float32,
        "double" | "real" => DataType::Float64,
        "decimal" | "numeric" => {
            let (p, s) = parse_decimal_params(&ct).unwrap_or((10, 0));
            DataType::Decimal128(p, s)
        }
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "time" => DataType::Time64(TimeUnit::Microsecond),
        "char" | "varchar" | "tinytext" | "text" | "enum" | "set" | "json" => DataType::Utf8,
        "mediumtext" | "longtext" => DataType::LargeUtf8,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            DataType::Binary
        }
        other => {
            return Err(internal(format!(
                "unsupported MySQL column type: {other} (from `{column_type}`)"
            )))
        }
    };
    Ok(dt)
}

fn uint_or_int(unsigned: bool, u: DataType, i: DataType) -> DataType {
    if unsigned {
        u
    } else {
        i
    }
}

fn parse_decimal_params(ct: &str) -> Option<(u8, i8)> {
    let inner = ct.split_once('(')?.1;
    let inner = inner.split_once(')')?.0;
    let mut parts = inner.split(',');
    let p: u8 = parts.next()?.trim().parse().ok()?;
    let s: i8 = parts.next().unwrap_or("0").trim().parse().ok()?;
    Some((p.clamp(1, 38), s.clamp(0, 38)))
}

/// Reads a MySQL table's column schema (database = `schema_name`).
pub async fn get_mysql_table_schema(
    pool: &MySqlPool,
    schema_name: &str,
    table_name: &str,
) -> Result<Schema, CommonError> {
    // information_schema text columns come back as BLOB over the wire, so cast
    // them to CHAR to decode as strings.
    let rows = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS name, \
                CAST(COLUMN_TYPE AS CHAR) AS ctype, \
                CAST(IS_NULLABLE AS CHAR) AS nullable \
         FROM information_schema.columns \
         WHERE table_schema = ? AND table_name = ? \
         ORDER BY ordinal_position",
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(pool)
    .await
    .map_err(|e| internal(format!("failed to read MySQL schema: {e}")))?;

    if rows.is_empty() {
        return Err(internal(format!(
            "MySQL table `{schema_name}`.`{table_name}` not found or has no columns"
        )));
    }

    let mut fields = Vec::with_capacity(rows.len());
    for row in &rows {
        let name: String = row.try_get("name").map_err(|e| internal(e.to_string()))?;
        let ctype: String = row.try_get("ctype").map_err(|e| internal(e.to_string()))?;
        let nullable: String = row.try_get("nullable").map_err(|e| internal(e.to_string()))?;
        let dt = mysql_type_to_arrow(&ctype)?;
        fields.push(Field::new(name, dt, nullable.eq_ignore_ascii_case("yes")));
    }
    Ok(Schema::new(fields))
}

fn col<T>(rows: &[MySqlRow], idx: usize) -> Result<Vec<Option<T>>, CommonError>
where
    T: for<'r> Decode<'r, sqlx::MySql> + Type<sqlx::MySql>,
{
    rows.iter()
        .map(|r| {
            r.try_get::<Option<T>, _>(idx)
                .map_err(|e| internal(format!("MySQL decode of column {idx} failed: {e}")))
        })
        .collect()
}

const UNIX_EPOCH_FROM_CE: i32 = 719_163; // NaiveDate(1970,1,1).num_days_from_ce()

/// Builds an Arrow array for one column from MySQL rows, dispatching on the
/// column's (already inferred) Arrow type.
pub fn build_array_from_mysql_rows(
    rows: &[MySqlRow],
    idx: usize,
    field: &Field,
) -> Result<ArrayRef, CommonError> {
    let arr: ArrayRef = match field.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(col::<bool>(rows, idx)?)),
        DataType::Int8 => Arc::new(Int8Array::from(col::<i8>(rows, idx)?)),
        DataType::UInt8 => Arc::new(UInt8Array::from(col::<u8>(rows, idx)?)),
        DataType::Int16 => Arc::new(Int16Array::from(col::<i16>(rows, idx)?)),
        DataType::UInt16 => Arc::new(UInt16Array::from(col::<u16>(rows, idx)?)),
        DataType::Int32 => Arc::new(Int32Array::from(col::<i32>(rows, idx)?)),
        DataType::UInt32 => Arc::new(UInt32Array::from(col::<u32>(rows, idx)?)),
        DataType::Int64 => Arc::new(Int64Array::from(col::<i64>(rows, idx)?)),
        DataType::UInt64 => Arc::new(UInt64Array::from(col::<u64>(rows, idx)?)),
        DataType::Float32 => Arc::new(Float32Array::from(col::<f32>(rows, idx)?)),
        DataType::Float64 => Arc::new(Float64Array::from(col::<f64>(rows, idx)?)),
        DataType::Utf8 => Arc::new(StringArray::from(col::<String>(rows, idx)?)),
        DataType::LargeUtf8 => Arc::new(arrow::array::LargeStringArray::from(
            col::<String>(rows, idx)?,
        )),
        DataType::Binary => {
            let vals = col::<Vec<u8>>(rows, idx)?;
            Arc::new(BinaryArray::from_iter(
                vals.iter().map(|v| v.as_deref()),
            ))
        }
        DataType::Date32 => {
            let vals = col::<NaiveDate>(rows, idx)?;
            Arc::new(Date32Array::from(
                vals.into_iter()
                    .map(|d| d.map(|d| d.num_days_from_ce() - UNIX_EPOCH_FROM_CE))
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let vals = col::<NaiveDateTime>(rows, idx)?;
            Arc::new(TimestampMicrosecondArray::from(
                vals.into_iter()
                    .map(|d| d.map(|d| d.and_utc().timestamp_micros()))
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let vals = col::<NaiveTime>(rows, idx)?;
            Arc::new(Time64MicrosecondArray::from(
                vals.into_iter()
                    .map(|t| {
                        t.map(|t| {
                            t.num_seconds_from_midnight() as i64 * 1_000_000
                                + (t.nanosecond() as i64) / 1_000
                        })
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Decimal128(precision, scale) => {
            let vals = col::<rust_decimal::Decimal>(rows, idx)?;
            let mut builder =
                Decimal128Array::builder(vals.len()).with_data_type(field.data_type().clone());
            for v in vals {
                match v {
                    Some(mut d) => {
                        d.rescale(*scale as u32);
                        builder.append_value(d.mantissa());
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish().with_precision_and_scale(*precision, *scale).map_err(
                |e| internal(format!("decimal column {idx}: {e}")),
            )?)
        }
        other => {
            return Err(internal(format!(
                "no MySQL decoder for Arrow type {other:?} (column {idx})"
            )))
        }
    };
    Ok(arr)
}

/// Builds a record batch from MySQL rows for the given (inferred) schema.
pub fn mysql_rows_to_record_batch(
    rows: &[MySqlRow],
    schema: &Schema,
) -> Result<RecordBatch, CommonError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        columns.push(build_array_from_mysql_rows(rows, idx, field)?);
    }
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| internal(format!("failed to build MySQL record batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_mysql_types() {
        assert_eq!(mysql_type_to_arrow("tinyint(1)").unwrap(), DataType::Boolean);
        assert_eq!(mysql_type_to_arrow("tinyint").unwrap(), DataType::Int8);
        assert_eq!(
            mysql_type_to_arrow("int(11) unsigned").unwrap(),
            DataType::UInt32
        );
        assert_eq!(mysql_type_to_arrow("bigint").unwrap(), DataType::Int64);
        assert_eq!(
            mysql_type_to_arrow("bigint unsigned").unwrap(),
            DataType::UInt64
        );
        assert_eq!(
            mysql_type_to_arrow("decimal(10,2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(mysql_type_to_arrow("varchar(255)").unwrap(), DataType::Utf8);
        assert_eq!(
            mysql_type_to_arrow("longtext").unwrap(),
            DataType::LargeUtf8
        );
        assert_eq!(mysql_type_to_arrow("date").unwrap(), DataType::Date32);
        assert_eq!(
            mysql_type_to_arrow("datetime").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(mysql_type_to_arrow("blob").unwrap(), DataType::Binary);
        assert!(mysql_type_to_arrow("geometry").is_err());
    }
}
