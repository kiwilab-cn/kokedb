use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Decimal128Array, FixedSizeBinaryArray, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    IntervalMonthDayNanoArray, LargeStringArray, ListBuilder, RecordBatch, StringArray,
    StringBuilder, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::Schema;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use rust_decimal::Decimal;
use sqlx::types::Uuid;
use sqlx::PgPool;

use crate::error::CommonError;

use rust_decimal::prelude::ToPrimitive;
use sqlx::postgres::PgRow;
use sqlx::Row;
use sqlx::ValueRef;
use std::sync::Arc;

pub fn pg_type_to_arrow_schema(pg_type: &str) -> Result<DataType, CommonError> {
    let normalized_type = pg_type.to_lowercase();

    if normalized_type.ends_with("[]") {
        let element_type = &normalized_type[..normalized_type.len() - 2];
        let element_data_type = convert_pg_type_to_arrow(element_type, None)?;
        return Ok(DataType::List(Arc::new(Field::new(
            "item",
            element_data_type,
            true,
        ))));
    }

    convert_pg_type_to_arrow(&normalized_type, None)
}

pub fn pg_type_to_arrow(
    pg_type: &str,
    row: &PgRow,
    column_index: usize,
) -> Result<DataType, CommonError> {
    let normalized_type = pg_type.to_lowercase();

    if normalized_type.ends_with("[]") {
        let element_type = &normalized_type[..normalized_type.len() - 2];
        let element_data_type = convert_pg_type_to_arrow(element_type, Some((row, column_index)))?;
        return Ok(DataType::List(Arc::new(Field::new(
            "item",
            element_data_type,
            true,
        ))));
    }

    convert_pg_type_to_arrow(&normalized_type, Some((row, column_index)))
}

fn convert_pg_type_to_arrow(
    pg_type: &str,
    row_data: Option<(&PgRow, usize)>,
) -> Result<DataType, CommonError> {
    let normalized_type = pg_type.to_lowercase();
    let base_type = normalized_type
        .split('(')
        .next()
        .unwrap_or(&normalized_type)
        .trim();

    match base_type {
        "smallint" | "int2" => Ok(DataType::Int16),
        "integer" | "int4" => Ok(DataType::Int32),
        "bigint" | "int8" => Ok(DataType::Int64),
        "real" | "float4" => Ok(DataType::Float32),
        "double precision" | "float8" => Ok(DataType::Float64),
        "numeric" | "decimal" => {
            if let Some((row, column_index)) = row_data {
                if let Ok(value) = row.try_get::<rust_decimal::Decimal, _>(column_index) {
                    let scale = value.scale() as i8;
                    let value_str = value.to_string();
                    let total_digits =
                        value_str.chars().filter(|c| c.is_ascii_digit()).count() as u8;
                    let precision = std::cmp::min(38, std::cmp::max(scale as u8 + 1, total_digits));
                    return Ok(DataType::Decimal128(precision, scale));
                }
            }

            if let Some(params) = extract_numeric_params(&normalized_type) {
                let precision = std::cmp::min(38, params.0);
                let scale = std::cmp::min(37, std::cmp::max(-84, params.1));
                Ok(DataType::Decimal128(precision, scale))
            } else {
                Ok(DataType::Utf8)
            }
        }
        "smallserial" => Ok(DataType::Int16),
        "serial" => Ok(DataType::Int32),
        "bigserial" => Ok(DataType::Int64),
        "text" | "varchar" | "character varying" => Ok(DataType::Utf8),
        "char" | "character" => {
            if let Some(len) = extract_char_length(&normalized_type) {
                if len <= 32 {
                    Ok(DataType::Utf8)
                } else {
                    Ok(DataType::LargeUtf8)
                }
            } else {
                Ok(DataType::Utf8)
            }
        }

        "bytea" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" | "timestamp without time zone" => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "timestamp with time zone" | "timestamptz" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "time" | "time without time zone" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "time with time zone" | "timetz" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => Ok(DataType::Utf8),
        "cidr" | "inet" | "macaddr" | "macaddr8" => Ok(DataType::Utf8),
        "bit" | "bit varying" => {
            if let Some(length) = extract_bit_length(&normalized_type) {
                Ok(DataType::FixedSizeBinary((length + 7) / 8))
            } else {
                Ok(DataType::Binary)
            }
        }

        "tsvector" | "tsquery" => Ok(DataType::Utf8),
        "uuid" => Ok(DataType::Utf8),
        "xml" => Ok(DataType::Utf8),
        "json" | "jsonb" => Ok(DataType::Utf8),
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            Ok(DataType::Utf8)
        }
        "oid" | "regproc" | "regprocedure" | "regoper" | "regoperator" | "regclass" | "regtype"
        | "regrole" | "regnamespace" | "regconfig" | "regdictionary" => Ok(DataType::UInt32),
        "money" => Ok(DataType::Decimal128(19, 4)),
        _ => {
            if let Some((row, column_index)) = row_data {
                if let Ok(raw_value) = row.try_get_raw(column_index) {
                    if !raw_value.is_null() {}
                }
            }
            Ok(DataType::Utf8)
        }
    }
}

fn extract_numeric_params(type_str: &str) -> Option<(u8, i8)> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let params_str = &type_str[start + 1..end];
            let params: Vec<&str> = params_str.split(',').collect();

            if params.len() >= 2 {
                if let (Ok(precision), Ok(scale)) = (
                    params[0].trim().parse::<u16>(),
                    params[1].trim().parse::<i16>(),
                ) {
                    let capped_precision = std::cmp::min(38, precision) as u8;
                    let capped_scale = std::cmp::min(127, std::cmp::max(-128, scale)) as i8;
                    return Some((capped_precision, capped_scale));
                }
            } else if params.len() == 1 {
                if let Ok(precision) = params[0].trim().parse::<u16>() {
                    let capped_precision = std::cmp::min(38, precision) as u8;
                    return Some((capped_precision, 0));
                }
            }
        }
    }
    None
}

fn extract_char_length(type_str: &str) -> Option<i32> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let length_str = &type_str[start + 1..end];
            return length_str.trim().parse::<i32>().ok();
        }
    }
    None
}

fn extract_bit_length(type_str: &str) -> Option<i32> {
    if let Some(start) = type_str.find('(') {
        if let Some(end) = type_str.find(')') {
            let length_str = &type_str[start + 1..end];
            return length_str.trim().parse::<i32>().ok();
        }
    }
    None
}

pub async fn get_postgresql_table_schema(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
) -> Result<Schema, CommonError> {
    let query = format!(
        "SELECT 
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            character_octet_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            interval_type,
            interval_precision,
            udt_name,  -- User-defined type name (更准确的类型信息)
            CASE 
                WHEN data_type = 'ARRAY' THEN 
                    REPLACE(udt_name, '_', '') || '[]'
                WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL THEN
                    'numeric(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
                WHEN data_type = 'character varying' AND character_maximum_length IS NOT NULL THEN
                    'varchar(' || character_maximum_length || ')'
                WHEN data_type = 'character' AND character_maximum_length IS NOT NULL THEN
                    'char(' || character_maximum_length || ')'
                WHEN data_type = 'bit' AND character_maximum_length IS NOT NULL THEN
                    'bit(' || character_maximum_length || ')'
                WHEN data_type = 'bit varying' AND character_maximum_length IS NOT NULL THEN
                    'bit varying(' || character_maximum_length || ')'
                ELSE data_type
            END as complete_data_type
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND table_schema = $2
        ORDER BY ordinal_position"
    );

    let rows = sqlx::query(&query)
        .bind(table_name)
        .bind(schema_name)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            CommonError::InternalError(format!("Failed to execute query sql with error:{}", e))
        })?;

    let mut fields = Vec::new();
    for row in rows {
        let column_name: String = row.get("column_name");
        let complete_data_type: String = row.get("complete_data_type");
        let is_nullable: String = row.get("is_nullable");

        let arrow_type = pg_type_to_arrow_schema(&complete_data_type).map_err(|x| {
            CommonError::InternalError(format!(
                "Failed to transaction type: {} to arrow with error: {:?}",
                &complete_data_type, x
            ))
        })?;
        let nullable = is_nullable == "YES";
        fields.push(Field::new(column_name, arrow_type, nullable));
    }
    Ok(Schema::new(fields))
}

pub fn build_array_from_rows(
    rows: &[PgRow],
    col_idx: usize,
    field: &Field,
) -> Result<ArrayRef, CommonError> {
    match field.data_type() {
        DataType::Int16 => {
            let values: Vec<Option<i16>> = rows
                .iter()
                .map(|row| row.try_get::<Option<i16>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(Int16Array::from(values)))
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| row.try_get::<Option<i32>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.try_get::<Option<i64>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = rows
                .iter()
                .map(|row| {
                    if let Ok(val) = row.try_get::<Option<i64>, _>(col_idx) {
                        val.and_then(|v| u32::try_from(v).ok())
                    } else if let Ok(val) = row.try_get::<Option<i32>, _>(col_idx) {
                        val.and_then(|v| u32::try_from(v).ok())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(UInt32Array::from(values)))
        }
        DataType::Float32 => {
            let values: Vec<Option<f32>> = rows
                .iter()
                .map(|row| row.try_get::<Option<f32>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(Float32Array::from(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.try_get::<Option<f64>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        DataType::Decimal128(precision, scale) => {
            let values: Result<Vec<Option<i128>>, CommonError> = rows
                .iter()
                .map(|row| match row.try_get::<Option<Decimal>, _>(col_idx) {
                    Ok(Some(decimal)) => {
                        let scaled = decimal * Decimal::from(10_i128.pow(*scale as u32));
                        let value = scaled.to_i128().ok_or_else(|| {
                            CommonError::InternalError(format!(
                                "Decimal out of i128 range: {}",
                                scaled
                            ))
                        })?;

                        Ok(Some(value))
                    }
                    Ok(None) => Ok(None),
                    Err(_) => Ok(None),
                })
                .collect();

            match values {
                Ok(decimal_values) => {
                    let array = Decimal128Array::from(decimal_values)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(|e| {
                            CommonError::InternalError(format!(
                                "Decimal128 array creation failed: {}",
                                e
                            ))
                        })?;
                    Ok(Arc::new(array))
                }
                Err(e) => Err(CommonError::InternalError(format!(
                    "Failed to process decimal values: {}",
                    e
                ))),
            }
        }

        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.try_get::<Option<bool>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }

        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    if let Ok(value) = row.try_get::<Option<String>, _>(col_idx) {
                        value
                    } else if let Ok(uuid_val) = row.try_get::<Option<Uuid>, _>(col_idx) {
                        uuid_val.map(|u| u.to_string())
                    } else {
                        match row.try_get_raw(col_idx) {
                            Ok(raw) if !raw.is_null() => {
                                let bytes = raw.as_bytes().unwrap();
                                if let Ok(s) = std::str::from_utf8(bytes) {
                                    Some(s.to_string())
                                } else {
                                    Some(format!("0x{}", hex::encode(bytes)))
                                }
                            }
                            _ => None,
                        }
                    }
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::LargeUtf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| row.try_get::<Option<String>, _>(col_idx).unwrap_or(None))
                .collect();
            Ok(Arc::new(LargeStringArray::from(values)))
        }

        DataType::Binary => {
            let values: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|row| row.try_get::<Option<Vec<u8>>, _>(col_idx).unwrap_or(None))
                .collect();

            let mut byte_data = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            let mut null_buffer = Vec::with_capacity(values.len());

            offsets.push(0i32);
            for value in values {
                match value {
                    Some(bytes) => {
                        byte_data.extend_from_slice(&bytes);
                        offsets.push(byte_data.len() as i32);
                        null_buffer.push(true);
                    }
                    None => {
                        offsets.push(byte_data.len() as i32);
                        null_buffer.push(false);
                    }
                }
            }

            let array = BinaryArray::new(
                arrow::buffer::OffsetBuffer::new(offsets.into()),
                Buffer::from(byte_data),
                Some(NullBuffer::from(null_buffer)),
            );
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(size) => {
            let values: Vec<Option<Vec<u8>>> = rows
                .iter()
                .map(|row| {
                    if let Ok(Some(bytes)) = row.try_get::<Option<Vec<u8>>, _>(col_idx) {
                        if bytes.len() == *size as usize {
                            Some(bytes)
                        } else {
                            let mut fixed_bytes = vec![0u8; *size as usize];
                            let copy_len = std::cmp::min(bytes.len(), *size as usize);
                            fixed_bytes[..copy_len].copy_from_slice(&bytes[..copy_len]);
                            Some(fixed_bytes)
                        }
                    } else {
                        None
                    }
                })
                .collect();

            let mut byte_data = Vec::with_capacity(rows.len() * (*size as usize));
            let mut null_buffer = Vec::with_capacity(rows.len());

            for value in values {
                match value {
                    Some(bytes) => {
                        byte_data.extend_from_slice(&bytes);
                        null_buffer.push(true);
                    }
                    None => {
                        byte_data.extend(vec![0u8; *size as usize]);
                        null_buffer.push(false);
                    }
                }
            }

            let array = FixedSizeBinaryArray::new(
                *size,
                Buffer::from(byte_data),
                Some(NullBuffer::from(null_buffer)),
            );
            Ok(Arc::new(array))
        }

        DataType::Date32 => {
            let values: Vec<Option<i32>> = rows
                .iter()
                .map(|row| {
                    if let Ok(date_val) = row.try_get::<Option<chrono::NaiveDate>, _>(col_idx) {
                        date_val.map(|d| {
                            (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
                                as i32
                        })
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    if let Ok(time_val) = row.try_get::<Option<chrono::NaiveTime>, _>(col_idx) {
                        time_val.map(|t| {
                            (t - chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                                .num_microseconds()
                                .unwrap_or(0)
                        })
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Arc::new(Time64MicrosecondArray::from(values)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| {
                    if let Ok(ts_val) = row.try_get::<Option<chrono::NaiveDateTime>, _>(col_idx) {
                        ts_val.map(|ts| ts.and_utc().timestamp_micros())
                    } else if let Ok(ts_val) =
                        row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(col_idx)
                    {
                        ts_val.map(|ts| ts.timestamp_micros())
                    } else {
                        None
                    }
                })
                .collect();

            match tz {
                Some(_) => Ok(Arc::new(
                    TimestampMicrosecondArray::from(values).with_timezone_opt(tz.clone()),
                )),
                None => Ok(Arc::new(TimestampMicrosecondArray::from(values))),
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values: Vec<Option<i128>> = rows
                .iter()
                .map(|row| {
                    match row.try_get::<Option<sqlx::postgres::types::PgInterval>, _>(col_idx) {
                        Ok(Some(interval)) => {
                            let nanos = (interval.microseconds as i128) * 1000;
                            let months = interval.months as i128;
                            let days = interval.days as i128;
                            let value = (nanos << 64) | (months << 32) | days;
                            Some(value)
                        }
                        Ok(None) => None,
                        Err(_) => None,
                    }
                })
                .collect();

            let primitive_values: Vec<i128> = values.iter().map(|v| v.unwrap_or(0)).collect();
            let null_buffer: Vec<bool> = values.iter().map(|v| v.is_some()).collect();

            let array = IntervalMonthDayNanoArray::new(
                ScalarBuffer::from(Buffer::from_vec(primitive_values)),
                Some(NullBuffer::from(null_buffer)),
            );
            Ok(Arc::new(array))
        }
        DataType::List(field) => {
            let child_type = field.data_type();
            let values: Vec<Option<Vec<serde_json::Value>>> = rows
                .iter()
                .map(|row| {
                    if let Ok(array_val) = row.try_get::<Option<serde_json::Value>, _>(col_idx) {
                        array_val.and_then(|val| {
                            if let serde_json::Value::Array(arr) = val {
                                Some(arr)
                            } else {
                                None
                            }
                        })
                    } else {
                        None
                    }
                })
                .collect();

            // Create the appropriate builder and process all values
            let array = match child_type {
                DataType::Int16 => {
                    let mut list_builder = ListBuilder::new(Int16Builder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(num) = v.as_i64() {
                                        list_builder.values().append_value(num as i16);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Int32 => {
                    let mut list_builder = ListBuilder::new(Int32Builder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(num) = v.as_i64() {
                                        list_builder.values().append_value(num as i32);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Int64 => {
                    let mut list_builder = ListBuilder::new(Int64Builder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(num) = v.as_i64() {
                                        list_builder.values().append_value(num);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Float32 => {
                    let mut list_builder = ListBuilder::new(Float32Builder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(num) = v.as_f64() {
                                        list_builder.values().append_value(num as f32);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Float64 => {
                    let mut list_builder = ListBuilder::new(Float64Builder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(num) = v.as_f64() {
                                        list_builder.values().append_value(num);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Utf8 => {
                    let mut list_builder = ListBuilder::new(StringBuilder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(s) = v.as_str() {
                                        list_builder.values().append_value(s);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Binary => {
                    let mut list_builder = ListBuilder::new(BinaryBuilder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(s) = v.as_str() {
                                        // Assuming binary elements are represented as strings; convert to bytes
                                        list_builder.values().append_value(s.as_bytes());
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                DataType::Boolean => {
                    let mut list_builder = ListBuilder::new(BooleanBuilder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(b) = v.as_bool() {
                                        list_builder.values().append_value(b);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
                _ => {
                    // Fallback to String for unsupported types
                    let mut list_builder = ListBuilder::new(StringBuilder::new());
                    for opt_vec in values {
                        match opt_vec {
                            Some(vec) => {
                                for v in vec {
                                    if let Some(s) = v.as_str() {
                                        list_builder.values().append_value(s);
                                    } else {
                                        list_builder.values().append_null();
                                    }
                                }
                                list_builder.append(true);
                            }
                            None => {
                                list_builder.append_null();
                            }
                        }
                    }
                    Arc::new(list_builder.finish()) as ArrayRef
                }
            };

            Ok(array)
        }
        _ => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| match row.try_get_raw(col_idx) {
                    Ok(raw) if !raw.is_null() => {
                        let bytes = raw.as_bytes().unwrap();
                        if let Ok(s) = std::str::from_utf8(bytes) {
                            Some(s.to_string())
                        } else {
                            Some(format!("0x{}", hex::encode(bytes)))
                        }
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

pub fn rows_to_record_batch(rows: &[PgRow], schema: &Schema) -> Result<RecordBatch, CommonError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    let mut columns = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = build_array_from_rows(rows, col_idx, field).map_err(|x| {
            CommonError::InternalError(format!("Failed to build rows array with error: {}", x))
        })?;
        columns.push(array);
    }

    let ret = RecordBatch::try_new(Arc::new(schema.clone()), columns).map_err(|x| {
        CommonError::InternalError(format!("Failed to new record batch with error: {}", x))
    })?;

    Ok(ret)
}
