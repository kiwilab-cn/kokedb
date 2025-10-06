use std::{io, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Schema};
use opensrv_mysql::{Column, ColumnFlags, ColumnType};

pub fn compact_columns(schema: Arc<Schema>) -> io::Result<Vec<Column>> {
    let columns: Vec<Column> = schema
        .fields()
        .iter()
        .map(|field| Column {
            table: String::new(),
            column: field.name().to_string(),
            coltype: match field.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
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
    Ok(columns)
}
