use std::{path::Path, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use foyer::{
    BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    RuntimeOptions, TokioRuntimeOptions,
};

use crate::{codec::RecordBatchCodec, error::CacheError};

/// Default zstd compression level used when `RESULT_CACHE_ZSTD_LEVEL` is unset.
/// Level 3 balances ratio and CPU cost for cached Arrow IPC payloads.
const DEFAULT_ZSTD_LEVEL: i32 = 3;

#[derive(Clone)]
pub struct LruResultCache {
    pub inner: Arc<HybridCache<u128, Vec<u8>>>,
    compression_level: i32,
}

impl LruResultCache {
    pub async fn new(memory_size: usize, disk_size: usize) -> Result<Self, CacheError> {
        let cache_dir =
            std::env::var("RESULT_CACHE_DIR").unwrap_or("/tmp/kokedb-cache".to_string());
        let cache_path = Path::new(&cache_dir);

        let compression_level = std::env::var("RESULT_CACHE_ZSTD_LEVEL")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(DEFAULT_ZSTD_LEVEL);

        let device = FsDeviceBuilder::new(cache_path)
            .with_capacity(disk_size * 1024 * 1024)
            .build()
            .map_err(|x| {
                CacheError::FoyerError(format!("Failed to build fsdevice with error: {x}"))
            })?;

        let hybrid_cache: HybridCache<u128, Vec<u8>> = HybridCacheBuilder::new()
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
            compression_level,
        })
    }

    pub async fn insert(&self, key: u128, batches: &[RecordBatch]) -> Result<(), CacheError> {
        let encode = RecordBatchCodec::encode(batches, self.compression_level)?;
        let _ret = self.inner.insert(key, encode);

        Ok(())
    }

    pub async fn delete(&self, key: u128) -> Result<(), CacheError> {
        let _ret = self.inner.remove(&key);
        Ok(())
    }

    pub async fn get(&self, key: u128) -> Result<SendableRecordBatchStream, CacheError> {
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
