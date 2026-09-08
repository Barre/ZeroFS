use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

/// Changes that can alter the result of a counter scan.
pub(crate) struct Activity {
    generation: AtomicU64,
    scanned: AtomicU64,
    changed: Notify,
}

impl Default for Activity {
    fn default() -> Self {
        Self {
            // The first cycle must scan the state loaded at startup.
            generation: AtomicU64::new(1),
            scanned: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }
}

impl Activity {
    pub(crate) fn record(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.changed.notify_one();
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn acknowledge(&self, generation: u64) {
        self.scanned.fetch_max(generation, Ordering::Release);
    }

    pub(crate) fn pending(&self) -> bool {
        self.scanned.load(Ordering::Acquire) < self.generation()
    }

    pub(crate) async fn notified(&self) {
        self.changed.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::Activity;

    #[test]
    fn acknowledging_a_snapshot_keeps_later_activity_pending() {
        let activity = Activity::default();
        let covered = activity.generation();

        activity.record();
        activity.acknowledge(covered);

        assert!(activity.pending());
        activity.acknowledge(activity.generation());
        assert!(!activity.pending());
    }
}
