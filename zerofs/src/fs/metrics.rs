use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use num_format::{Locale, ToFormattedString};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

const MIB_IN_BYTES: f64 = 1024.0 * 1024.0;

struct PreviousSnapshot {
    total_operations: u64,
    bytes_read: u64,
    bytes_written: u64,
    read_operations: u64,
    write_operations: u64,
    timestamp: Instant,
}

pub struct FileSystemStats {
    // File operations
    pub files_created: AtomicU64,
    pub files_deleted: AtomicU64,
    pub files_renamed: AtomicU64,
    pub directories_created: AtomicU64,
    pub directories_deleted: AtomicU64,
    pub directories_renamed: AtomicU64,
    pub links_created: AtomicU64,
    pub links_deleted: AtomicU64,
    pub links_renamed: AtomicU64,

    // Read/Write operations
    pub read_operations: AtomicU64,
    pub write_operations: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,

    // Tombstone cleanup
    pub tombstones_created: AtomicU64,
    pub tombstones_processed: AtomicU64,
    pub tombstone_cleanup_extents_deleted: AtomicU64,
    pub tombstone_cleanup_runs: AtomicU64,

    // Performance
    pub total_operations: AtomicU64,

    // Internal state for rate calculation
    last_snapshot: std::sync::Mutex<PreviousSnapshot>,
}

impl Default for FileSystemStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemStats {
    pub fn new() -> Self {
        Self {
            files_created: AtomicU64::new(0),
            files_deleted: AtomicU64::new(0),
            files_renamed: AtomicU64::new(0),
            directories_created: AtomicU64::new(0),
            directories_deleted: AtomicU64::new(0),
            directories_renamed: AtomicU64::new(0),
            links_created: AtomicU64::new(0),
            links_deleted: AtomicU64::new(0),
            links_renamed: AtomicU64::new(0),
            read_operations: AtomicU64::new(0),
            write_operations: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            tombstones_created: AtomicU64::new(0),
            tombstones_processed: AtomicU64::new(0),
            tombstone_cleanup_extents_deleted: AtomicU64::new(0),
            tombstone_cleanup_runs: AtomicU64::new(0),
            total_operations: AtomicU64::new(0),
            last_snapshot: std::sync::Mutex::new(PreviousSnapshot {
                total_operations: 0,
                bytes_read: 0,
                bytes_written: 0,
                read_operations: 0,
                write_operations: 0,
                timestamp: Instant::now(),
            }),
        }
    }

    pub fn report(&self) -> String {
        // Load current values
        let files_created = self.files_created.load(Ordering::Relaxed);
        let files_deleted = self.files_deleted.load(Ordering::Relaxed);
        let files_renamed = self.files_renamed.load(Ordering::Relaxed);
        let dirs_created = self.directories_created.load(Ordering::Relaxed);
        let dirs_deleted = self.directories_deleted.load(Ordering::Relaxed);
        let dirs_renamed = self.directories_renamed.load(Ordering::Relaxed);
        let links_created = self.links_created.load(Ordering::Relaxed);
        let links_deleted = self.links_deleted.load(Ordering::Relaxed);
        let links_renamed = self.links_renamed.load(Ordering::Relaxed);

        let read_ops = self.read_operations.load(Ordering::Relaxed);
        let write_ops = self.write_operations.load(Ordering::Relaxed);
        let bytes_read = self.bytes_read.load(Ordering::Relaxed);
        let bytes_written = self.bytes_written.load(Ordering::Relaxed);

        let tombstones_created = self.tombstones_created.load(Ordering::Relaxed);
        let tombstones_processed = self.tombstones_processed.load(Ordering::Relaxed);
        let cleaned_extents = self
            .tombstone_cleanup_extents_deleted
            .load(Ordering::Relaxed);
        let tombstone_cleanup_runs = self.tombstone_cleanup_runs.load(Ordering::Relaxed);

        let total_ops = self.total_operations.load(Ordering::Relaxed);

        let mut snapshot = self.last_snapshot.lock().unwrap();
        let interval_secs = snapshot.timestamp.elapsed().as_secs_f64();

        let ops_per_sec = if interval_secs > 0.0 {
            (total_ops - snapshot.total_operations) as f64 / interval_secs
        } else {
            0.0
        };

        let read_ops_per_sec = if interval_secs > 0.0 {
            (read_ops - snapshot.read_operations) as f64 / interval_secs
        } else {
            0.0
        };

        let write_ops_per_sec = if interval_secs > 0.0 {
            (write_ops - snapshot.write_operations) as f64 / interval_secs
        } else {
            0.0
        };

        let mb_read_per_sec = if interval_secs > 0.0 {
            (bytes_read - snapshot.bytes_read) as f64 / interval_secs / MIB_IN_BYTES
        } else {
            0.0
        };

        let mb_written_per_sec = if interval_secs > 0.0 {
            (bytes_written - snapshot.bytes_written) as f64 / interval_secs / MIB_IN_BYTES
        } else {
            0.0
        };

        *snapshot = PreviousSnapshot {
            total_operations: total_ops,
            bytes_read,
            bytes_written,
            read_operations: read_ops,
            write_operations: write_ops,
            timestamp: Instant::now(),
        };

        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table.set_header(vec![
            Cell::new("ZeroFS Statistics")
                .fg(Color::Cyan)
                .add_attribute(Attribute::Bold),
            Cell::new("Value")
                .fg(Color::Cyan)
                .add_attribute(Attribute::Bold),
        ]);

        // File Operations section
        table.add_row(vec![
            Cell::new("File Operations (total)")
                .fg(Color::Yellow)
                .add_attribute(Attribute::Bold),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("  Files"),
            Cell::new(format!(
                "Created: {} | Deleted: {} | Renamed: {}",
                files_created.to_formatted_string(&Locale::en),
                files_deleted.to_formatted_string(&Locale::en),
                files_renamed.to_formatted_string(&Locale::en)
            )),
        ]);
        table.add_row(vec![
            Cell::new("  Directories"),
            Cell::new(format!(
                "Created: {} | Deleted: {} | Renamed: {}",
                dirs_created.to_formatted_string(&Locale::en),
                dirs_deleted.to_formatted_string(&Locale::en),
                dirs_renamed.to_formatted_string(&Locale::en)
            )),
        ]);
        table.add_row(vec![
            Cell::new("  Links"),
            Cell::new(format!(
                "Created: {} | Deleted: {} | Renamed: {}",
                links_created.to_formatted_string(&Locale::en),
                links_deleted.to_formatted_string(&Locale::en),
                links_renamed.to_formatted_string(&Locale::en)
            )),
        ]);

        table.add_row(vec![
            Cell::new("I/O Performance (per second)")
                .fg(Color::Yellow)
                .add_attribute(Attribute::Bold),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("  Read"),
            Cell::new(format!(
                "{read_ops_per_sec:.1} ops/s ({mb_read_per_sec:.2} MB/s)"
            ))
            .fg(Color::Green),
        ]);
        table.add_row(vec![
            Cell::new("  Write"),
            Cell::new(format!(
                "{write_ops_per_sec:.1} ops/s ({mb_written_per_sec:.2} MB/s)"
            ))
            .fg(Color::Blue),
        ]);
        table.add_row(vec![
            Cell::new("  All Operations"),
            Cell::new(format!(
                "{ops_per_sec:.1} ops/s (includes create/delete/list/etc.)",
            ))
            .fg(Color::Magenta)
            .add_attribute(Attribute::Bold),
        ]);

        table.add_row(vec![
            Cell::new("Tombstone Cleanup (total)")
                .fg(Color::Yellow)
                .add_attribute(Attribute::Bold),
            Cell::new(""),
        ]);
        table.add_row(vec![
            Cell::new("  Tombstones"),
            Cell::new(format!(
                "{} created, {} processed",
                tombstones_created.to_formatted_string(&Locale::en),
                tombstones_processed.to_formatted_string(&Locale::en)
            )),
        ]);
        table.add_row(vec![
            Cell::new("  Extents deleted"),
            Cell::new(format!(
                "{} (in {} runs)",
                cleaned_extents.to_formatted_string(&Locale::en),
                tombstone_cleanup_runs.to_formatted_string(&Locale::en)
            )),
        ]);

        table.to_string()
    }

    pub fn output_report_debug(&self) {
        tracing::debug!("\n{}", self.report());
    }
}

/// Tracked segment-frame footprint. `ExtentStore::sample_footprint` seeds it;
/// committed counter deltas maintain the same values after open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentFootprint {
    pub segment_count: u64,
    pub appended_bytes: u64,
    pub live_bytes: u64,
    pub reclaimable_bytes: u64,
}

/// Signed tracked-footprint change carried through one committed batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SegmentFootprintDelta {
    segments: i64,
    appended_bytes: i64,
    live_bytes: i64,
}

impl SegmentFootprintDelta {
    pub(crate) const fn new(segments: i64, appended_bytes: i64, live_bytes: i64) -> Self {
        Self {
            segments,
            appended_bytes,
            live_bytes,
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.segments = self.segments.saturating_add(other.segments);
        self.appended_bytes = self.appended_bytes.saturating_add(other.appended_bytes);
        self.live_bytes = self.live_bytes.saturating_add(other.live_bytes);
    }
}

/// Telemetry recorded when one reclaim cycle ends.
#[derive(Default)]
pub(crate) struct SegmentReclaimCycle {
    // Delete-horizon gauges (footprint is maintained incrementally,
    // see `apply_footprint_delta`, not recorded from the reclaim cycle).
    pub(crate) awaiting_delete: u64,
    pub(crate) awaiting_delete_bytes: u64,
    /// Reclamation and repacking paused by a persistent checkpoint pin.
    pub(crate) checkpoint_pinned: bool,

    // Work done by this cycle (accumulated into counters).
    pub(crate) segments_deleted: u64,
    pub(crate) deleted_bytes: u64,
    pub(crate) repack_sources: u64,
    pub(crate) frames_relocated: u64,
    pub(crate) repack_jobs: u64,
}

/// Segment-reclamation metrics, bridged to Prometheus by `crate::prometheus`.
///
/// Counters accumulate across cycles; cycle gauges hold the most recent cycle's
/// state, while executor gauges are updated directly by concurrent RAII
/// guards. Each field is independent telemetry, so `Relaxed` is enough.
#[derive(Default)]
pub struct SegmentReclaimStats {
    // Counters
    pub cycles: AtomicU64,
    pub segments_deleted: AtomicU64,
    pub deleted_bytes: AtomicU64,
    pub repack_sources: AtomicU64,
    pub frames_relocated: AtomicU64,
    pub repack_jobs: AtomicU64,
    pub orphans_reclaimed: AtomicU64,

    // Footprint gauges seeded at open and maintained by committed deltas.
    pub segment_count: AtomicU64,
    pub appended_bytes: AtomicU64,
    pub live_bytes: AtomicU64,

    // Gauges from the latest cycle.
    pub awaiting_delete: AtomicU64,
    pub awaiting_delete_bytes: AtomicU64,
    pub checkpoint_pinned: AtomicBool,

    // Live executor state sampled by the monitor and Prometheus exporter.
    pub active_repacks: AtomicU64,
    pub active_fetches: AtomicU64,
    pub active_puts: AtomicU64,
    pub active_deletes: AtomicU64,
    pub repack_memory_reserved_bytes: AtomicU64,
    pub repack_memory_budget_bytes: AtomicU64,
}

impl SegmentReclaimStats {
    /// Sample each footprint gauge once and derive reclaimable bytes from
    /// those values. Concurrent commits can still straddle the atomic loads.
    pub fn footprint(&self) -> SegmentFootprint {
        use Ordering::Relaxed;
        let segment_count = self.segment_count.load(Relaxed);
        let appended_bytes = self.appended_bytes.load(Relaxed);
        let live_bytes = self.live_bytes.load(Relaxed);
        SegmentFootprint {
            segment_count,
            appended_bytes,
            live_bytes,
            reclaimable_bytes: appended_bytes.saturating_sub(live_bytes),
        }
    }

    pub(crate) fn record_cycle(&self, cycle: &SegmentReclaimCycle) {
        use Ordering::Relaxed;
        self.cycles.fetch_add(1, Relaxed);
        self.segments_deleted
            .fetch_add(cycle.segments_deleted, Relaxed);
        self.deleted_bytes.fetch_add(cycle.deleted_bytes, Relaxed);
        self.repack_sources.fetch_add(cycle.repack_sources, Relaxed);
        self.frames_relocated
            .fetch_add(cycle.frames_relocated, Relaxed);
        self.repack_jobs.fetch_add(cycle.repack_jobs, Relaxed);

        // The commit worker owns footprint gauges. Publishing the scan's
        // snapshot here would overwrite updates committed during the cycle.
        self.awaiting_delete.store(cycle.awaiting_delete, Relaxed);
        self.awaiting_delete_bytes
            .store(cycle.awaiting_delete_bytes, Relaxed);

        self.checkpoint_pinned
            .store(cycle.checkpoint_pinned, Relaxed);
    }

    pub(crate) fn record_orphans_reclaimed(&self, n: u64) {
        self.orphans_reclaimed.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn begin_repack(&self) -> SegmentReclaimActivityGuard<'_> {
        self.begin_activity(&self.active_repacks, 1)
    }

    pub(crate) fn reserve_repack_memory(&self, bytes: u64) -> SegmentReclaimActivityGuard<'_> {
        self.begin_activity(&self.repack_memory_reserved_bytes, bytes)
    }

    pub(crate) fn begin_fetch(&self) -> SegmentReclaimActivityGuard<'_> {
        self.begin_activity(&self.active_fetches, 1)
    }

    pub(crate) fn begin_put(&self) -> SegmentReclaimActivityGuard<'_> {
        self.begin_activity(&self.active_puts, 1)
    }

    pub(crate) fn begin_delete(&self) -> SegmentReclaimActivityGuard<'_> {
        self.begin_activity(&self.active_deletes, 1)
    }

    fn begin_activity<'a>(
        &'a self,
        counter: &'a AtomicU64,
        amount: u64,
    ) -> SegmentReclaimActivityGuard<'a> {
        counter.fetch_add(amount, Ordering::Relaxed);
        SegmentReclaimActivityGuard { counter, amount }
    }

    /// Seed the footprint gauges from a one-time scan at store open. After this,
    /// they are maintained incrementally by [`Self::apply_footprint_delta`].
    pub(crate) fn seed_footprint(&self, f: &SegmentFootprint) {
        use Ordering::Relaxed;
        self.segment_count.store(f.segment_count, Relaxed);
        self.appended_bytes.store(f.appended_bytes, Relaxed);
        self.live_bytes.store(f.live_bytes, Relaxed);
    }

    /// Fold one committed batch's net counter change into the footprint gauges.
    /// The commit worker supplies the exact staged and counter-deletion deltas,
    /// so the gauges track writes and deletes in real time without a scan.
    pub(crate) fn apply_footprint_delta(&self, delta: SegmentFootprintDelta) {
        apply_i64(&self.segment_count, delta.segments);
        apply_i64(&self.appended_bytes, delta.appended_bytes);
        apply_i64(&self.live_bytes, delta.live_bytes);
    }
}

/// Cancellation-safe accounting for live segment-reclamation work.
pub(crate) struct SegmentReclaimActivityGuard<'a> {
    counter: &'a AtomicU64,
    amount: u64,
}

impl Drop for SegmentReclaimActivityGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.amount, Ordering::Relaxed);
    }
}

fn apply_i64(a: &AtomicU64, d: i64) {
    use Ordering::Relaxed;
    if d >= 0 {
        a.fetch_add(d as u64, Relaxed);
    } else {
        a.fetch_sub(d.unsigned_abs(), Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footprint_derives_reclaimable_bytes_from_sampled_gauges() {
        let stats = SegmentReclaimStats::default();
        stats.seed_footprint(&SegmentFootprint {
            segment_count: 2,
            appended_bytes: 100,
            live_bytes: 80,
            reclaimable_bytes: 20,
        });
        assert_eq!(stats.footprint().reclaimable_bytes, 20);

        stats.apply_footprint_delta(SegmentFootprintDelta::new(0, 30, -10));
        assert_eq!(
            stats.footprint(),
            SegmentFootprint {
                segment_count: 2,
                appended_bytes: 130,
                live_bytes: 70,
                reclaimable_bytes: 60,
            }
        );
        stats.apply_footprint_delta(SegmentFootprintDelta::new(-1, -60, 0));
        assert_eq!(stats.footprint().reclaimable_bytes, 0);

        // A sample can straddle a commit; never wrap when live exceeds appended.
        stats.live_bytes.store(80, Ordering::Relaxed);
        assert_eq!(stats.footprint().reclaimable_bytes, 0);
    }
}
