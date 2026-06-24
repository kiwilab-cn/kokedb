use arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

use crate::codec::RecordBatchCodec;
use crate::error::CacheError;

/// A result cache backed by a shared Redis (or Redis-compatible) server, so a
/// result cached by one kokedb node serves every node, and the cache survives
/// process restarts.
///
/// There is deliberately no node-local tier in front of Redis: a local copy
/// could not be invalidated when another node evicts the key, so Redis is the
/// single source of truth and every read/write is one round trip. Values use
/// the same zstd-compressed Arrow IPC encoding as the local cache, so the two
/// backends are interchangeable.
#[derive(Clone)]
pub struct RedisResultCache {
    conn: ConnectionManager,
    compression_level: i32,
    /// Safety-net expiry (seconds); 0 means no expiry. Explicit invalidation on
    /// table change still deletes keys regardless.
    ttl_secs: u64,
    prefix: String,
}

impl RedisResultCache {
    pub async fn new(
        url: &str,
        ttl_secs: u64,
        prefix: String,
        compression_level: i32,
    ) -> Result<Self, CacheError> {
        let client = redis::Client::open(url)
            .map_err(|e| CacheError::External(format!("invalid redis url: {e}")))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| CacheError::External(format!("failed to connect to redis: {e}")))?;
        Ok(Self {
            conn,
            compression_level,
            ttl_secs,
            prefix,
        })
    }

    fn redis_key(&self, key: u128) -> String {
        redis_key(&self.prefix, key)
    }

    pub async fn insert(&self, key: u128, batches: &[RecordBatch]) -> Result<(), CacheError> {
        let bytes = RecordBatchCodec::encode(batches, self.compression_level)?;
        let mut conn = self.conn.clone();
        let rkey = self.redis_key(key);
        let res: redis::RedisResult<()> = if self.ttl_secs == 0 {
            conn.set(rkey, bytes).await
        } else {
            conn.set_ex(rkey, bytes, self.ttl_secs).await
        };
        res.map_err(|e| CacheError::External(format!("redis set failed: {e}")))
    }

    pub async fn get(&self, key: u128) -> Result<SendableRecordBatchStream, CacheError> {
        let mut conn = self.conn.clone();
        let rkey = self.redis_key(key);
        let bytes: Option<Vec<u8>> = conn
            .get(&rkey)
            .await
            .map_err(|e| CacheError::External(format!("redis get failed: {e}")))?;
        match bytes {
            Some(b) => RecordBatchCodec::decode(&b),
            None => Err(CacheError::NotFound("CacheValue key", key.to_string())),
        }
    }

    pub async fn delete(&self, key: u128) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        let rkey = self.redis_key(key);
        let res: redis::RedisResult<()> = conn.del(rkey).await;
        res.map_err(|e| CacheError::External(format!("redis del failed: {e}")))
    }

    pub async fn contains(&self, key: u128) -> bool {
        let mut conn = self.conn.clone();
        let rkey = self.redis_key(key);
        conn.exists(&rkey).await.unwrap_or(false)
    }
}

/// 128-bit plan hash rendered as a fixed-width hex key under the namespace
/// prefix, so keys are stable and collision-free across nodes.
fn redis_key(prefix: &str, key: u128) -> String {
    format!("{}{:032x}", prefix, key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_key_is_prefixed_fixed_width_hex() {
        let key = redis_key("kokedb:rc:", 0xABCD);
        assert_eq!(key, "kokedb:rc:0000000000000000000000000000abcd");
        assert_eq!(key.len(), "kokedb:rc:".len() + 32);
    }

    // A result inserted by one cache handle must be visible to a separate handle
    // (a different "node"), proving the cache is shared. DB-free of Postgres but
    // needs a Redis server.
    #[tokio::test]
    #[ignore = "requires Redis; run with KOKEDB_REDIS_URL (default redis://127.0.0.1:6379)"]
    async fn shared_across_handles_round_trip() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use futures::TryStreamExt;
        use std::sync::Arc;

        let url = std::env::var("KOKEDB_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let prefix = "kokedb:test:".to_string();
        let node_a = RedisResultCache::new(&url, 60, prefix.clone(), 3).await.unwrap();
        let node_b = RedisResultCache::new(&url, 60, prefix, 3).await.unwrap();

        let key = 0xDEAD_BEEF_u128;
        node_a.delete(key).await.unwrap();
        assert!(!node_b.contains(key).await, "key must start absent");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        // Node A writes; Node B must see and decode it.
        node_a.insert(key, std::slice::from_ref(&batch)).await.unwrap();
        assert!(node_b.contains(key).await, "node B should see node A's write");
        let got: Vec<RecordBatch> = node_b.get(key).await.unwrap().try_collect().await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].num_rows(), 3, "decoded batch must round-trip");

        // Eviction by either node is visible to both.
        node_b.delete(key).await.unwrap();
        assert!(!node_a.contains(key).await, "node A should see node B's delete");
    }
}
