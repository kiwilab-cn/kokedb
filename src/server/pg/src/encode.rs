//! Arrow → PostgreSQL wire encoding, shared by the simple and extended query
//! protocols.
//!
//! Each column is mapped to its closest PostgreSQL type OID and the values are
//! encoded *natively* (i32, f64, &str, …). Encoding the native value — rather
//! than a pre-rendered string — lets `pgwire` honour whatever wire format the
//! client asked for in its `Bind` message (text *or* binary), which the
//! extended protocol requires (e.g. tokio-postgres requests binary results).
//!
//! Types we don't map natively (timestamps, decimals, dates, …) are surfaced as
//! `TEXT` and rendered with arrow's display formatter. Declaring them as `TEXT`
//! keeps the OID and the encoding consistent in both wire formats, since a
//! `TEXT` column carries the same UTF-8 bytes whether text or binary.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, UInt16Array, UInt32Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

/// Maps an Arrow data type to the PostgreSQL type advertised on the wire. Types
/// without a native binary encoding fall back to `TEXT`.
pub fn pg_type_for(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => Type::INT2,
        DataType::Int32 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT,
        DataType::Binary | DataType::LargeBinary => Type::BYTEA,
        _ => Type::TEXT,
    }
}

/// Builds one [`FieldInfo`] per column, all in text format (simple query
/// protocol always uses text).
pub fn fields_for_schema(schema: &SchemaRef) -> Arc<Vec<FieldInfo>> {
    fields_with_format(schema, &Format::UnifiedText)
}

/// Builds one [`FieldInfo`] per column, choosing each column's wire format from
/// `format` (the client's requested result formats in the extended protocol).
pub fn fields_with_format(schema: &SchemaRef, format: &Format) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    pg_type_for(f.data_type()),
                    format.format_for(i),
                )
            })
            .collect(),
    )
}

fn api_err(e: impl std::error::Error + Send + Sync + 'static) -> PgWireError {
    PgWireError::ApiError(Box::new(e))
}

/// Encodes a record batch into wire `DataRow`s using the given field set. The
/// per-column wire format (text/binary) is taken from each `FieldInfo`.
pub fn encode_batch(batch: &RecordBatch, fields: &Arc<Vec<FieldInfo>>) -> PgWireResult<Vec<DataRow>> {
    let opts = FormatOptions::default();
    // Fallback text formatters, used for the columns surfaced as TEXT.
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
        .collect::<Result<_, _>>()
        .map_err(api_err)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for (col, fmt) in batch.columns().iter().zip(&formatters) {
            encode_cell(&mut encoder, col.as_ref(), r, fmt)?;
        }
        rows.push(encoder.take_row());
    }
    Ok(rows)
}

/// Downcasts `array` to its concrete type and encodes the value at `row` as the
/// native PostgreSQL representation; unmapped types are rendered as text.
fn encode_cell(
    encoder: &mut DataRowEncoder,
    array: &dyn Array,
    row: usize,
    fallback: &ArrayFormatter,
) -> PgWireResult<()> {
    if array.is_null(row) {
        return encoder.encode_field(&Option::<&str>::None);
    }
    macro_rules! enc {
        ($ty:ty) => {{
            let a = array.as_any().downcast_ref::<$ty>().expect("array type");
            encoder.encode_field(&a.value(row))
        }};
        ($ty:ty, $as:ty) => {{
            let a = array.as_any().downcast_ref::<$ty>().expect("array type");
            encoder.encode_field(&(a.value(row) as $as))
        }};
    }
    match array.data_type() {
        DataType::Boolean => enc!(BooleanArray),
        DataType::Int8 => enc!(Int8Array, i16),
        DataType::Int16 => enc!(Int16Array),
        DataType::Int32 => enc!(Int32Array),
        DataType::Int64 => enc!(Int64Array),
        DataType::UInt8 => enc!(UInt8Array, i16),
        DataType::UInt16 => enc!(UInt16Array, i32),
        DataType::UInt32 => enc!(UInt32Array, i64),
        DataType::Float32 => enc!(Float32Array),
        DataType::Float64 => enc!(Float64Array),
        DataType::Utf8 => enc!(StringArray),
        DataType::LargeUtf8 => enc!(LargeStringArray),
        DataType::Utf8View => enc!(StringViewArray),
        DataType::Binary => enc!(BinaryArray),
        DataType::LargeBinary => enc!(LargeBinaryArray),
        _ => encoder.encode_field(&fallback.value(row).to_string()),
    }
}
