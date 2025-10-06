use std::{io::Cursor, sync::Arc};

use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{ArrowError, Schema};

#[derive(Debug, Clone)]
pub struct SchemaTable<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub arrow_schema: Arc<Schema>,
    pub local_path: &'a str,
}

pub fn schema_to_binary(schema: Arc<Schema>) -> Result<Vec<u8>, ArrowError> {
    let mut buffer = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buffer, &schema)?;
        writer.finish()?;
    }
    Ok(buffer)
}

pub fn binary_to_schema(schema_bin: &[u8]) -> Result<Arc<Schema>, ArrowError> {
    let cursor = Cursor::new(schema_bin);
    let reader = FileReader::try_new(cursor, None)?;
    let schema = reader.schema();
    Ok(schema)
}
