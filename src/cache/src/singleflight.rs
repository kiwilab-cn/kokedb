//! Single-flight de-duplication of in-flight cache-miss executions.
//!
//! When many identical queries hit a cold result-cache key at once (e.g. a
//! dashboard loaded by many users), only the first ("owner") executes; the
//! others ("waiters") block until the owner has populated the cache and then
//! serve from it — instead of all of them re-executing against the source.
//!
//! Shared process-wide (keyed by plan hash) so it de-duplicates across
//! connections, not just within one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

pub struct Singleflight {
    inflight: Mutex<HashMap<u128, Arc<Notify>>>,
}

pub enum Claim {
    /// This caller must execute the query; dropping the guard wakes waiters.
    Owner(OwnerGuard),
    /// Another caller is executing; await this, then re-check the cache.
    Waiter(Arc<Notify>),
}

/// Held by the owner for the duration of execution + cache population. Its
/// `Drop` removes the in-flight entry and wakes all waiters — so waiters are
/// released whether the owner succeeded, errored, or panicked.
pub struct OwnerGuard {
    sf: Arc<Singleflight>,
    key: u128,
}

impl Drop for OwnerGuard {
    fn drop(&mut self) {
        let mut map = self.sf.lock();
        if let Some(notify) = map.remove(&self.key) {
            notify.notify_waiters();
        }
    }
}

impl Singleflight {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inflight: Mutex::new(HashMap::new()),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u128, Arc<Notify>>> {
        // Recover from a poisoned lock instead of panicking; the map is simple
        // shared bookkeeping and a poisoned state is still usable.
        self.inflight.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Claim ownership of `key`, or get a handle to await the current owner.
    pub fn claim(self: &Arc<Self>, key: u128) -> Claim {
        let mut map = self.lock();
        if let Some(notify) = map.get(&key) {
            Claim::Waiter(notify.clone())
        } else {
            map.insert(key, Arc::new(Notify::new()));
            Claim::Owner(OwnerGuard {
                sf: self.clone(),
                key,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_owner_then_waiter_then_reclaim() {
        let sf = Singleflight::new();
        // First claim owns; a concurrent claim on the same key waits.
        let guard = match sf.claim(7) {
            Claim::Owner(g) => g,
            _ => panic!("first claim should own"),
        };
        assert!(matches!(sf.claim(7), Claim::Waiter(_)));
        // Releasing the owner lets the next claim own again.
        drop(guard);
        assert!(matches!(sf.claim(7), Claim::Owner(_)));
    }

    #[tokio::test]
    async fn dropping_owner_wakes_waiter() {
        let sf = Singleflight::new();
        let guard = match sf.claim(9) {
            Claim::Owner(g) => g,
            _ => panic!("owner"),
        };
        let notify = match sf.claim(9) {
            Claim::Waiter(n) => n,
            _ => panic!("waiter"),
        };
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(1), notified)
            .await
            .expect("waiter should be woken when the owner is dropped");
    }
}
