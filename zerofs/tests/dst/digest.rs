//! Per-world event digest: a stable hash of the ordered event stream plus a
//! readable trace, so one seed replays identically (see `same_seed_same_digest`).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct Digest {
    hasher: Arc<Mutex<DefaultHasher>>,
    trace: Arc<Mutex<Vec<String>>>,
    /// Virtual-time base for trace timestamps (diagnostic only; the hash
    /// covers the events themselves). Set once per world.
    base: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl Digest {
    pub(crate) fn event(&self, e: impl Hash + std::fmt::Debug) {
        e.hash(&mut *self.hasher.lock().unwrap());
        let mut base = self.base.lock().unwrap();
        let base = base.get_or_insert_with(tokio::time::Instant::now);
        let t = base.elapsed().as_millis();
        self.trace.lock().unwrap().push(format!("[{t}] {e:?}"));
    }
    pub(crate) fn finish(&self) -> (u64, Vec<String>) {
        (
            self.hasher.lock().unwrap().clone().finish(),
            std::mem::take(&mut *self.trace.lock().unwrap()),
        )
    }
}
