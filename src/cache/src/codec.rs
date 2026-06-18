use std::{io::Cursor, sync::Arc};

use arrow::{
    array::RecordBatch,
    ipc::{reader::FileReader, writer::FileWriter},
};
use datafusion::{error::DataFusionError, execution::SendableRecordBatchStream};
use futures::{stream, StreamExt};
use zstd::{decode_all, encode_all};

use crate::error::CacheError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

pub struct RecordBatchCodec;

impl RecordBatchCodec {
    pub fn encode(batches: &[RecordBatch], compression_level: i32) -> Result<Vec<u8>, CacheError> {
        let schema = match batches.first() {
            Some(batch) => batch.schema(),
            None => return Ok(vec![]),
        };

        let mut buffer = Vec::new();
        let mut writer = FileWriter::try_new(&mut buffer, &schema).map_err(|x| {
            CacheError::Internal(format!("Failed to new FileWriter with error: {x}"))
        })?;

        for batch in batches {
            writer.write(batch).map_err(|x| {
                CacheError::Internal(format!("Failed to new FileWriter with error: {x}"))
            })?;
        }

        writer.finish().map_err(|x| {
            CacheError::Internal(format!("Failed to finish FileWriter with error: {x}"))
        })?;

        drop(writer);

        let compressed = encode_all(buffer.as_slice(), compression_level).map_err(|x| {
            CacheError::Internal(format!("Failed to compress buffer with error: {x}"))
        })?;

        Ok(compressed)
    }

    pub fn decode(data: &[u8]) -> Result<SendableRecordBatchStream, CacheError> {
        if data.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                stream::empty(),
            )));
        }

        let decompressed = decode_all(data).map_err(|x| {
            CacheError::Internal(format!("Failed to decompress buffer with error: {x}"))
        })?;

        let cursor = Cursor::new(decompressed);
        let reader = FileReader::try_new(cursor, None).map_err(|x| {
            CacheError::Internal(format!("Failed to new FileReader with error: {x}"))
        })?;

        let schema = reader.schema();
        let stream = stream::iter(reader).map(|result| {
            result.map_err(|e| {
                DataFusionError::Internal(format!("Failed to read batch with error: {e}"))
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
