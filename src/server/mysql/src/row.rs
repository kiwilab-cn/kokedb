use std::io;

use datafusion::arrow::{array::RecordBatch, datatypes::DataType};

pub fn compact_rows(batches: Vec<RecordBatch>) -> io::Result<Vec<Vec<String>>> {
    let mut result = vec![];
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
            result.push(row);
        }
    }

    Ok(result)
}
