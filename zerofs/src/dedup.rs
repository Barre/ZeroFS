//! Idempotency cache: maps a client-supplied op id to the result of applying that
//! op once, so a retried non-idempotent op (mkdir, rename, unlink, ...) returns
//! the original result instead of applying twice. Under HA it is also fed from
//! the replication stream, so a promoted standby dedups retries across failover.
//!
//! In-memory and bounded (oldest-first eviction): state need only outlive a
//! client's retry window. Eviction is never a correctness risk, only a coverage
//! one (an evicted id's retry re-evaluates the op).

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Safety-net poll interval for a single-flight waiter, in case a wakeup is
/// missed; the waiter re-checks the cache each iteration.
const SINGLE_FLIGHT_POLL: Duration = Duration::from_millis(20);

/// Client-generated op id. All-zero means "no id supplied": no deduplication.
pub type OpId = [u8; 16];

/// Whether an op id was actually supplied (non-zero).
pub fn has_op_id(op_id: &OpId) -> bool {
    op_id.iter().any(|&b| b != 0)
}

/// Bounded map from op id to cached reply payload. The payload is opaque bytes;
/// its encoding is the caller's concern.
pub struct DedupCache {
    inner: Mutex<Inner>,
    capacity: usize,
}

struct Inner {
    results: HashMap<OpId, Bytes>,
    /// Insertion order, for oldest-first eviction once at capacity.
    order: VecDeque<OpId>,
    /// Op-ids currently being applied (single-flight): a second caller with the
    /// same id waits on the `Notify`.
    inflight: HashMap<OpId, Arc<Notify>>,
}

impl DedupCache {
    /// Holds at most `capacity` entries (clamped to >= 1).
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                results: HashMap::new(),
                order: VecDeque::new(),
                inflight: HashMap::new(),
            }),
            capacity: capacity.max(1),
        }
    }

    /// Single-flight gate: at most one apply in progress per id, so two concurrent
    /// retries cannot both apply. `Some(guard)` to the one caller that runs the op
    /// (the guard releases the slot on drop); `None` if already applied or just
    /// completed by a caller we waited for (the caller then hits the dedup check).
    pub async fn begin(self: &Arc<Self>, op_id: OpId) -> Option<InFlightGuard> {
        if !has_op_id(&op_id) {
            return None;
        }
        loop {
            let notify = {
                let mut inner = self.inner.lock().unwrap();
                if inner.results.contains_key(&op_id) {
                    return None;
                }
                match inner.inflight.get(&op_id) {
                    Some(n) => Arc::clone(n),
                    None => {
                        inner.inflight.insert(op_id, Arc::new(Notify::new()));
                        return Some(InFlightGuard {
                            op_id,
                            cache: Arc::clone(self),
                        });
                    }
                }
            };
            // Timeout guards against a missed wakeup (re-check the cache each iteration).
            let _ = tokio::time::timeout(SINGLE_FLIGHT_POLL, notify.notified()).await;
        }
    }

    /// The cached reply if `op_id` was already applied; `None` for an absent,
    /// first-time, or evicted id.
    pub fn get(&self, op_id: &OpId) -> Option<Bytes> {
        if !has_op_id(op_id) {
            return None;
        }
        self.inner.lock().unwrap().results.get(op_id).cloned()
    }

    /// Cache `op_id`'s reply. An absent (all-zero) id is a no-op; an already-present
    /// id refreshes its reply without changing eviction position or growing.
    pub fn record(&self, op_id: OpId, result: Bytes) {
        if !has_op_id(&op_id) {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.results.insert(op_id, result).is_none() {
            inner.order.push_back(op_id);
            while inner.order.len() > self.capacity {
                if let Some(evicted) = inner.order.pop_front() {
                    inner.results.remove(&evicted);
                }
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().results.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Releases a single-flight slot (claimed via [`DedupCache::begin`]) on drop,
/// waking waiters on the same op.
pub struct InFlightGuard {
    op_id: OpId,
    cache: Arc<DedupCache>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut inner = self.cache.inner.lock().unwrap();
        if let Some(n) = inner.inflight.remove(&self.op_id) {
            n.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> OpId {
        let mut x = [0u8; 16];
        x[0] = n;
        x
    }

    #[test]
    fn records_and_returns_the_cached_reply() {
        let cache = DedupCache::new(8);
        assert_eq!(cache.get(&id(1)), None, "unseen op is not deduplicated");
        cache.record(id(1), Bytes::from_static(b"qid-1"));
        assert_eq!(
            cache.get(&id(1)).as_deref(),
            Some(&b"qid-1"[..]),
            "a seen op returns its cached reply"
        );
        assert_eq!(cache.get(&id(2)), None);
    }

    #[test]
    fn absent_op_id_is_never_deduplicated() {
        let cache = DedupCache::new(8);
        let absent = [0u8; 16];
        cache.record(absent, Bytes::from_static(b"x"));
        assert_eq!(cache.get(&absent), None, "the all-zero id must never dedup");
        assert!(cache.is_empty());
    }

    #[test]
    fn evicts_oldest_first_at_capacity() {
        let cache = DedupCache::new(2);
        cache.record(id(1), Bytes::from_static(b"a"));
        cache.record(id(2), Bytes::from_static(b"b"));
        cache.record(id(3), Bytes::from_static(b"c")); // evicts id(1)
        assert_eq!(cache.get(&id(1)), None, "oldest entry evicted at capacity");
        assert!(cache.get(&id(2)).is_some());
        assert!(cache.get(&id(3)).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn re_recording_same_id_does_not_grow() {
        let cache = DedupCache::new(8);
        cache.record(id(1), Bytes::from_static(b"first"));
        cache.record(id(1), Bytes::from_static(b"second"));
        assert_eq!(cache.len(), 1, "re-recording an id must not duplicate it");
        assert_eq!(cache.get(&id(1)).as_deref(), Some(&b"second"[..]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_serializes_concurrent_ops() {
        let cache = Arc::new(DedupCache::new(8));
        let g1 = cache
            .begin(id(1))
            .await
            .expect("first caller claims the op-id");
        // A concurrent caller for the same id must block until the first finishes.
        let c2 = cache.clone();
        let waiter = tokio::spawn(async move { c2.begin(id(1)).await.is_none() });
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !waiter.is_finished(),
            "a concurrent begin must wait for the in-flight op"
        );
        cache.record(id(1), Bytes::from_static(b"r"));
        drop(g1);
        // After completion the waiter sees the op already applied (None).
        assert!(
            waiter.await.unwrap(),
            "after the in-flight op completed, the waiter must see it done"
        );
        assert!(cache.begin(id(2)).await.is_some());
    }
}
