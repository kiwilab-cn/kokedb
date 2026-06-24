//! Arrow → PostgreSQL wire encoding for the simple query protocol.
//!
//! Every column is surfaced as `TEXT`, with values rendered by arrow's display
//! formatter. This is correct for all Arrow types (dates, decimals, timestamps
//! format as PostgreSQL clients expect) and keeps the encoder small; reporting
//! precise per-type OIDs (so numeric clients see real types) is a follow-up.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;

/// One TEXT [`FieldInfo`] per column, preserving names and order.
pub fn fields_for_schema(schema: &SchemaRef) -> Arc<Vec<FieldInfo>> {
    Arc::new(
        schema
            .fields()
            .iter()
            .map(|f| FieldInfo::new(f.name().clone(), None, None, Type::TEXT, FieldFormat::Text))
            .collect(),
    )
}

/// Encodes a record batch into wire `DataRow`s using the given field set.
pub fn encode_batch(
    batch: &RecordBatch,
    fields: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<Vec<DataRow>> {
    let opts = FormatOptions::default();
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &opts))
        .collect::<Result<_, _>>()
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for (col, fmt) in batch.columns().iter().zip(&formatters) {
            if col.is_null(r) {
                encoder.encode_field(&Option::<&str>::None)?;
            } else {
                encoder.encode_field(&Some(fmt.value(r).to_string()))?;
            }
        }
        rows.push(encoder.take_row());
    }
    Ok(rows)
}
