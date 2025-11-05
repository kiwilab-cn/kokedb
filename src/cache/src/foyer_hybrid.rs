use std::{path::Path, pin::Pin, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::{error::DataFusionError, execution::SendableRecordBatchStream};
use foyer::{
    BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    RuntimeOptions, TokioRuntimeOptions,
};
use futures::Stream;

use crate::{codec::RecordBatchCodec, error::CacheError};

#[derive(Clone)]
pub struct LruResultCache {
    pub inner: Arc<HybridCache<u64, Vec<u8>>>,
}

impl LruResultCache {
    pub async fn new(memory_size: usize, disk_size: usize) -> Result<Self, CacheError> {
        let cache_dir =
            std::env::var("RESULT_CACHE_DIR").unwrap_or("/tmp/kokedb-cache".to_string());
        let cache_path = Path::new(&cache_dir);

        let device = FsDeviceBuilder::new(cache_path)
            .with_capacity(disk_size * 1024 * 1024)
            .build()
            .map_err(|x| {
                CacheError::FoyerError(format!("Failed to build fsdevice with error: {x}"))
            })?;

        let hybrid_cache: HybridCache<u64, Vec<u8>> = HybridCacheBuilder::new()
            .memory(memory_size * 1024 * 1024)
            .storage()
            .with_engine_config(BlockEngineBuilder::new(device))
            .with_compression(foyer::Compression::Lz4)
            .with_runtime_options(RuntimeOptions::Separated {
                read_runtime_options: TokioRuntimeOptions {
                    worker_threads: 4,
                    max_blocking_threads: 8,
                },
                write_runtime_options: TokioRuntimeOptions {
                    worker_threads: 4,
                    max_blocking_threads: 8,
                },
            })
            .build()
            .await
            .map_err(|x| {
                CacheError::FoyerError(format!("Failed to build hybrid cache with error: {x}"))
            })?;

        Ok(Self {
            inner: Arc::new(hybrid_cache),
        })
    }

    pub async fn insert(&self, key: u64, batches: &[RecordBatch]) -> Result<(), CacheError> {
        let encode = RecordBatchCodec::encode(batches, 3)?;
        let _ret = self.inner.insert(key, encode);

        Ok(())
    }

    pub async fn delete(&self, key: u64) -> Result<(), CacheError> {
        let _ret = self.inner.remove(&key);
        Ok(())
    }

    pub async fn get(&self, key: u64) -> Result<SendableRecordBatchStream, CacheError> {
        if let Some(entry) = self.inner.get(&key).await.map_err(|x| {
            CacheError::Internal(format!("Failed to get cache key: {} with error: {x}", &key))
        })? {
            let stream = RecordBatchCodec::decode(entry.value())?;
            Ok(stream)
        } else {
            Err(CacheError::NotFound("CacheValue key", key.to_string()))
        }
    }
}
