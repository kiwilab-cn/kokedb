use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use kokedb_common::spec::Plan;
use lru::LruCache;

/// Process-wide cache of parsed-and-hashed query plans, keyed by the raw SQL
/// text.
///
/// Parsing (`parser`) and plan hashing (`get_plan_hash`) are pure functions of
/// the SQL string — they touch no catalog or per-connection session state — so
/// a cached `(Plan, hash)` never goes stale and is safe to share across every
/// connection. The only management concern is memory: prepared-statement
/// parameter substitution bakes literals into the SQL text, so distinct
/// parameter values yield distinct keys and the key space is effectively
/// unbounded. The cache is therefore a true LRU: hot statement texts stay
/// resident while one-off parameterized variants age out (the previous
/// arbitrary-victim eviction could drop a hot plan instead).
///
/// A single mutex around the LRU is deliberate: every operation is a few
/// pointer moves (values are `Arc`s), so the critical section is nanoseconds —
/// far cheaper than the parse + hash it saves.
///
/// This sits in front of, not in place of, the result cache: it shortens the
/// pre-execution path (parse + JSON-serialize-and-hash) that runs on *every*
/// query, including result-cache hits.
#[derive(Clone)]
pub struct PlanCache {
    entries: Arc<Mutex<LruCache<String, Arc<(Plan, u128)>>>>,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity.max(1)).unwrap_or(NonZeroUsize::MIN);
        Self {
            entries: Arc::new(Mutex::new(LruCache::new(capacity))),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruCache<String, Arc<(Plan, u128)>>> {
        // Recover from a poisoned lock instead of panicking: the cache is
        // advisory bookkeeping and remains structurally valid.
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Looks up a plan by SQL text, marking it most-recently-used.
    pub fn get(&self, sql: &str) -> Option<Arc<(Plan, u128)>> {
        self.lock().get(sql).cloned()
    }

    pub fn insert(&self, sql: &str, value: Arc<(Plan, u128)>) {
        self.lock().put(sql.to_string(), value);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy() -> Arc<(Plan, u128)> {
        // Parse a minimal statement to obtain a real Plan without depending on
        // any catalog/session state.
        let plan = crate::binder::parser("SELECT 1").expect("parse SELECT 1");
        Arc::new((plan, 42))
    }

    #[test]
    fn caches_and_returns_by_sql_text() {
        let cache = PlanCache::new(8);
        assert!(cache.get("SELECT 1").is_none());
        cache.insert("SELECT 1", dummy());
        let hit = cache.get("SELECT 1").expect("should hit");
        assert_eq!(hit.1, 42);
    }

    #[test]
    fn stays_within_capacity() {
        let cache = PlanCache::new(4);
        for i in 0..50 {
            cache.insert(&format!("SELECT {i}"), dummy());
        }
        assert!(cache.len() <= 4, "cache exceeded capacity: {}", cache.len());
    }

    #[test]
    fn evicts_least_recently_used_not_hot_entries() {
        let cache = PlanCache::new(2);
        cache.insert("hot", dummy());
        cache.insert("cold", dummy());
        // Touch "hot" so "cold" is the LRU victim when a third entry arrives.
        assert!(cache.get("hot").is_some());
        cache.insert("new", dummy());
        assert!(cache.get("hot").is_some(), "hot entry must survive eviction");
        assert!(cache.get("cold").is_none(), "cold entry should be evicted");
        assert!(cache.get("new").is_some());
    }
}
