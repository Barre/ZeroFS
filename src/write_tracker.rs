use crate::inode::InodeId;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks in-flight writes to ensure file sizes only increase during concurrent writes
#[derive(Clone)]
pub struct WriteTracker {
    active_writes: Arc<DashMap<InodeId, Arc<AtomicU64>>>,
}

impl WriteTracker {
    pub fn new() -> Self {
        Self {
            active_writes: Arc::new(DashMap::new()),
        }
    }

    /// Start tracking a write operation and return a guard
    /// The guard will automatically clean up when dropped
    pub fn track_write(
        &self,
        inode_id: InodeId,
        current_size: u64,
        offset: u64,
        length: u64,
    ) -> WriteGuard {
        let end_offset = offset + length;

        let high_water_mark = self
            .active_writes
            .entry(inode_id)
            .or_insert_with(|| Arc::new(AtomicU64::new(current_size)))
            .clone();

        high_water_mark.fetch_max(current_size, Ordering::Relaxed);
        high_water_mark.fetch_max(end_offset, Ordering::Relaxed);

        WriteGuard {
            inode_id,
            high_water_mark,
            active_writes: self.active_writes.clone(),
        }
    }
}

/// RAII guard that tracks a write operation
pub struct WriteGuard {
    inode_id: InodeId,
    high_water_mark: Arc<AtomicU64>,
    active_writes: Arc<DashMap<InodeId, Arc<AtomicU64>>>,
}

impl WriteGuard {
    /// Get the current maximum size for this inode (considering all active writes)
    pub fn get_max_size(&self) -> u64 {
        self.high_water_mark.load(Ordering::Acquire)
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Clean up the entry if this was the last active write
        // The Arc will handle the actual cleanup, but we remove from map when no writers remain
        if Arc::strong_count(&self.high_water_mark) <= 2 {
            // One ref in map, one in this guard = last writer
            self.active_writes
                .remove_if(&self.inode_id, |_, v| Arc::strong_count(v) <= 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_tracker_max_size() {
        let tracker = WriteTracker::new();

        // Simulate concurrent writes to a file that starts at size 500
        let guard1 = tracker.track_write(1, 500, 0, 1000); // Write to 0-1000
        let guard2 = tracker.track_write(1, 500, 1000, 1000); // Write to 1000-2000
        let guard3 = tracker.track_write(1, 500, 500, 100); // Write to 500-600

        // All should see the max size (2000)
        assert_eq!(guard1.get_max_size(), 2000);
        assert_eq!(guard2.get_max_size(), 2000);
        assert_eq!(guard3.get_max_size(), 2000);
    }

    #[test]
    fn test_write_tracker_cleanup() {
        let tracker = WriteTracker::new();

        {
            let _guard = tracker.track_write(1, 0, 0, 1000);
            assert_eq!(tracker.active_writes.len(), 1);
        }

        // After guard is dropped, entry should be cleaned up
        assert_eq!(tracker.active_writes.len(), 0);
    }

    #[test]
    fn test_write_tracker_preserves_size() {
        let tracker = WriteTracker::new();

        // File currently has size 2000
        let guard = tracker.track_write(1, 2000, 0, 500);

        // Should preserve the larger size
        assert_eq!(guard.get_max_size(), 2000);
    }
}
