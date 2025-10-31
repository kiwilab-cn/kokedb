use std::{io::Cursor, pin::Pin};

use arrow::{
    array::RecordBatch,
    ipc::{reader::FileReader, writer::FileWriter},
};
use futures::{stream, Stream, StreamExt};
use zstd::{decode_all, encode_all};

use crate::error::CacheError;

pub struct RecordBatchCodec;

impl RecordBatchCodec {
    pub fn encode(batches: &[RecordBatch], compression_level: i32) -> Result<Vec<u8>, CacheError> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        let mut buffer = Vec::new();
        let schema = batches.first().unwrap().schema();
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

    pub fn decode(
        data: &[u8],
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch, CacheError>> + Send>>, CacheError>
    {
        if data.is_empty() {
            return Ok(stream::iter(vec![]).boxed());
        }

        let decompressed = decode_all(data).map_err(|x| {
            CacheError::Internal(format!("Failed to decompress buffer with error: {x}"))
        })?;

        let cursor = Cursor::new(decompressed);
        let reader = FileReader::try_new(cursor, None).map_err(|x| {
            CacheError::Internal(format!("Failed to new FileReader with error: {x}"))
        })?;

        let stream = stream::iter(reader)
            .map(|result| {
                result.map_err(|e| {
                    CacheError::Internal(format!("Failed to read batch with error: {e}"))
                })
            })
            .boxed();

        Ok(stream)
    }
}
