use arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;

use crate::error::CacheError;
use crate::foyer_hybrid::LruResultCache;
use crate::redis_cache::RedisResultCache;

/// Default zstd level for the Redis backend, matching the local cache.
const DEFAULT_ZSTD_LEVEL: i32 = 3;
/// Default Redis safety-net expiry: one day.
const DEFAULT_TTL_SECS: u64 = 86_400;

/// The process's result cache. Defaults to the node-local hybrid (memory+disk)
/// cache; can be switched to a shared Redis backend so a result cached by one
/// node serves every node and survives restarts. Both backends speak the same
/// encoding and the same `insert/get/delete/contains` surface, so the rest of
/// the system is backend-agnostic.
#[derive(Clone)]
pub enum ResultCache {
    Local(LruResultCache),
    Redis(RedisResultCache),
}

impl ResultCache {
    /// The node-local hybrid cache (used directly by tests).
    pub async fn local(memory_mb: usize, disk_mb: usize) -> Result<Self, CacheError> {
        Ok(Self::Local(LruResultCache::new(memory_mb, disk_mb).await?))
    }

    /// Selects the backend from the environment:
    ///   KOKEDB_RESULT_CACHE_BACKEND = `local` (default) | `redis`
    /// For `redis`, KOKEDB_REDIS_URL is required; KOKEDB_RESULT_CACHE_TTL_SECS
    /// (default 1 day, 0 = no expiry) and KOKEDB_RESULT_CACHE_PREFIX
    /// (default `kokedb:rc:`) are optional. `memory_mb`/`disk_mb` size the local
    /// backend and are ignored by Redis.
    pub async fn from_env(memory_mb: usize, disk_mb: usize) -> Result<Self, CacheError> {
        let backend = std::env::var("KOKEDB_RESULT_CACHE_BACKEND").unwrap_or_default();
        if backend.eq_ignore_ascii_case("redis") {
            let url = std::env::var("KOKEDB_REDIS_URL").map_err(|_| {
                CacheError::InvalidArgument(
                    "KOKEDB_REDIS_URL must be set when KOKEDB_RESULT_CACHE_BACKEND=redis".into(),
                )
            })?;
            let ttl_secs = std::env::var("KOKEDB_RESULT_CACHE_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_TTL_SECS);
            let prefix = std::env::var("KOKEDB_RESULT_CACHE_PREFIX")
                .unwrap_or_else(|_| "kokedb:rc:".to_string());
            let level = std::env::var("RESULT_CACHE_ZSTD_LEVEL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_ZSTD_LEVEL);
            // Don't log the URL — it may carry credentials.
            log::info!("Result cache backend: redis (shared), ttl={ttl_secs}s");
            Ok(Self::Redis(
                RedisResultCache::new(&url, ttl_secs, prefix, level).await?,
            ))
        } else {
            log::info!("Result cache backend: local hybrid (memory+disk)");
            Self::local(memory_mb, disk_mb).await
        }
    }

    pub async fn insert(&self, key: u128, batches: &[RecordBatch]) -> Result<(), CacheError> {
        match self {
            Self::Local(c) => c.insert(key, batches).await,
            Self::Redis(c) => c.insert(key, batches).await,
        }
    }

    pub async fn get(&self, key: u128) -> Result<SendableRecordBatchStream, CacheError> {
        match self {
            Self::Local(c) => c.get(key).await,
            Self::Redis(c) => c.get(key).await,
        }
    }

    pub async fn delete(&self, key: u128) -> Result<(), CacheError> {
        match self {
            Self::Local(c) => c.delete(key).await,
            Self::Redis(c) => c.delete(key).await,
        }
    }

    /// Whether the key is present, without decoding its value.
    pub async fn contains(&self, key: u128) -> bool {
        match self {
            Self::Local(c) => c.inner.contains(&key),
            Self::Redis(c) => c.contains(key).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_env_defaults_to_local_and_requires_url_for_redis() {
        // Unset (default) -> local hybrid cache.
        std::env::remove_var("KOKEDB_RESULT_CACHE_BACKEND");
        assert!(matches!(
            ResultCache::from_env(8, 8).await.unwrap(),
            ResultCache::Local(_)
        ));

        // redis backend without a URL is a clear configuration error.
        std::env::set_var("KOKEDB_RESULT_CACHE_BACKEND", "redis");
        std::env::remove_var("KOKEDB_REDIS_URL");
        assert!(
            ResultCache::from_env(8, 8).await.is_err(),
            "redis backend without KOKEDB_REDIS_URL must error"
        );
        std::env::remove_var("KOKEDB_RESULT_CACHE_BACKEND");
    }
}
