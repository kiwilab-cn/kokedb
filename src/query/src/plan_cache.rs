use std::sync::Arc;

use dashmap::DashMap;
use kokedb_common::spec::Plan;

/// Process-wide cache of parsed-and-hashed query plans, keyed by the raw SQL
/// text.
///
/// Parsing (`parser`) and plan hashing (`get_plan_hash`) are pure functions of
/// the SQL string — they touch no catalog or per-connection session state — so
/// a cached `(Plan, hash)` never goes stale and is safe to share across every
/// connection. The only management concern is memory: prepared-statement
/// parameter substitution bakes literals into the SQL text, so distinct
/// parameter values yield distinct keys and the key space is effectively
/// unbounded. The cache is therefore size-capped with best-effort eviction.
///
/// This sits in front of, not in place of, the result cache: it shortens the
/// pre-execution path (parse + JSON-serialize-and-hash) that runs on *every*
/// query, including result-cache hits.
#[derive(Clone)]
pub struct PlanCache {
    entries: Arc<DashMap<String, Arc<(Plan, u128)>>>,
    capacity: usize,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            capacity: capacity.max(1),
        }
    }

    pub fn get(&self, sql: &str) -> Option<Arc<(Plan, u128)>> {
        self.entries.get(sql).map(|e| e.value().clone())
    }

    pub fn insert(&self, sql: &str, value: Arc<(Plan, u128)>) {
        // Best-effort bound: drop one arbitrary entry before inserting into a
        // full cache. Exact LRU isn't worth the bookkeeping for a hint cache —
        // a wrongly-evicted entry just costs one re-parse.
        if self.entries.len() >= self.capacity && !self.entries.contains_key(sql) {
            // Materialize the victim key in its own statement so the DashMap
            // shard guard from `iter()` is dropped BEFORE `remove()` — holding
            // it across remove() would deadlock on the same shard.
            let victim = self.entries.iter().next().map(|e| e.key().clone());
            if let Some(victim) = victim {
                self.entries.remove(&victim);
            }
        }
        self.entries.insert(sql.to_string(), value);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kokedb_common::spec::Plan;

    // A trivially-constructed Plan stand-in: Plan must be Clone for the cache to
    // hand out owned copies. We use the real parser path in integration, but
    // here we only need *a* Plan value, so build one via serde from the empty
    // variant is overkill — instead assert the bound/eviction behavior with a
    // shared dummy.
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
}
