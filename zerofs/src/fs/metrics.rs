use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use num_format::{Locale, ToFormattedString};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
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

    // Garbage collection
    pub tombstones_created: AtomicU64,
    pub tombstones_processed: AtomicU64,
    pub gc_extents_deleted: AtomicU64,
    pub gc_runs: AtomicU64,

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
            gc_extents_deleted: AtomicU64::new(0),
            gc_runs: AtomicU64::new(0),
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
        let gc_extents = self.gc_extents_deleted.load(Ordering::Relaxed);
        let gc_runs = self.gc_runs.load(Ordering::Relaxed);

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
            Cell::new("Garbage Collection (total)")
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
                gc_extents.to_formatted_string(&Locale::en),
                gc_runs.to_formatted_string(&Locale::en)
            )),
        ]);

        table.to_string()
    }

    pub fn output_report_debug(&self) {
        tracing::debug!("\n{}", self.report());
    }
}

/// Whole-store segment footprint. `ExtentStore::sample_footprint` computes it
/// authoritatively; the monitor gauges maintain the same values incrementally
/// after open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentFootprint {
    pub segment_count: u64,
    pub appended_bytes: u64,
    pub live_bytes: u64,
    pub reclaimable_bytes: u64,
}

/// One reclaim pass's numbers, mirroring the `segment GC:` summary log line.
/// Handed to [`SegmentGcStats::record_pass`] so the metric names live next to
/// the exporter, not the reclaim path.
#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentGcPass {
    // Delete-horizon / backlog gauges (footprint is maintained incrementally,
    // see `apply_footprint_delta`, not recorded from the pass scan).
    pub awaiting_delete: u64,
    pub awaiting_delete_bytes: u64,
    pub candidate_backlog: u64,
    pub chains_deferred: u64,
    pub saturated: bool,
    /// Reclaim + compaction paused by a persistent checkpoint pin.
    pub pinned: bool,

    // Work done this pass (accumulated into counters).
    pub segments_deleted: u64,
    pub deleted_bytes: u64,
    pub segments_compacted: u64,
    pub segments_packed: u64,
    pub frames_relocated: u64,
    pub compaction_freed_bytes: u64,
    pub batches: u64,
    pub tail_scrubbed: u64,
    pub chains_packed: u64,
    pub chains_assembled: u64,
    pub nominations: u64,
    pub nominations_dropped: u64,
    pub hot_seams: u64,
}

/// Segment-GC / reclaim metrics, bridged to Prometheus by `crate::prometheus`.
///
/// Counters accumulate across passes; gauges hold the most recent pass's
/// whole-store footprint (a gauge is meaningful between passes, a counter is
/// not). The reclaim task is the sole writer, so `Relaxed` is enough.
#[derive(Default)]
pub struct SegmentGcStats {
    // Counters
    pub passes: AtomicU64,
    pub segments_deleted: AtomicU64,
    pub deleted_bytes: AtomicU64,
    pub segments_compacted: AtomicU64,
    pub segments_packed: AtomicU64,
    pub frames_relocated: AtomicU64,
    pub compaction_freed_bytes: AtomicU64,
    pub batches: AtomicU64,
    pub tail_scrubbed: AtomicU64,
    pub chains_packed: AtomicU64,
    pub nominations: AtomicU64,
    pub nominations_dropped: AtomicU64,
    pub hot_seams: AtomicU64,
    pub orphans_reclaimed: AtomicU64,

    // Gauges (last pass)
    pub segment_count: AtomicU64,
    pub appended_bytes: AtomicU64,
    pub live_bytes: AtomicU64,
    pub reclaimable_bytes: AtomicU64,
    pub awaiting_delete: AtomicU64,
    pub awaiting_delete_bytes: AtomicU64,
    pub candidate_backlog: AtomicU64,
    pub chains_deferred: AtomicU64,
    pub saturated: AtomicU64,

    // Last-pass activity + cadence posture, for the `monitor` status line.
    pub last_deleted: AtomicU64,
    pub last_deleted_bytes: AtomicU64,
    pub last_frames_relocated: AtomicU64,
    pub last_chains_packed: AtomicU64,
    pub last_chains_assembled: AtomicU64,
    pub last_hot_seams: AtomicU64,
    pub pinned: AtomicBool,
    /// False until the first reclaim pass populates the gauges above.
    pub has_run: AtomicBool,
    /// Cadence tier of the last plan: 0 none, 1 base, 2 drain, 3 fast.
    pub tier: AtomicU8,
    pub read_directed: AtomicBool,
    /// The GC planner's own plain-language reason for the current cadence.
    pub reason: Mutex<String>,
}

impl SegmentGcStats {
    pub fn record_pass(&self, p: &SegmentGcPass) {
        use Ordering::Relaxed;
        self.passes.fetch_add(1, Relaxed);
        self.segments_deleted.fetch_add(p.segments_deleted, Relaxed);
        self.deleted_bytes.fetch_add(p.deleted_bytes, Relaxed);
        self.segments_compacted
            .fetch_add(p.segments_compacted, Relaxed);
        self.segments_packed.fetch_add(p.segments_packed, Relaxed);
        self.frames_relocated.fetch_add(p.frames_relocated, Relaxed);
        self.compaction_freed_bytes
            .fetch_add(p.compaction_freed_bytes, Relaxed);
        self.batches.fetch_add(p.batches, Relaxed);
        self.tail_scrubbed.fetch_add(p.tail_scrubbed, Relaxed);
        self.chains_packed.fetch_add(p.chains_packed, Relaxed);
        self.nominations.fetch_add(p.nominations, Relaxed);
        self.nominations_dropped
            .fetch_add(p.nominations_dropped, Relaxed);
        self.hot_seams.fetch_add(p.hot_seams, Relaxed);

        // segment_count / appended / live / reclaimable are NOT stored here:
        // they are maintained incrementally off the commit path (seeded once at
        // open, see `apply_footprint_delta`). Writing the pass's start-of-scan
        // snapshot would clobber deltas applied during the pass (e.g. this
        // pass's own deletions). The reclaim-owned gauges below still stand.
        self.awaiting_delete.store(p.awaiting_delete, Relaxed);
        self.awaiting_delete_bytes
            .store(p.awaiting_delete_bytes, Relaxed);
        self.candidate_backlog.store(p.candidate_backlog, Relaxed);
        self.chains_deferred.store(p.chains_deferred, Relaxed);
        self.saturated.store(p.saturated as u64, Relaxed);

        self.last_deleted.store(p.segments_deleted, Relaxed);
        self.last_deleted_bytes.store(p.deleted_bytes, Relaxed);
        self.last_frames_relocated
            .store(p.frames_relocated, Relaxed);
        self.last_chains_packed.store(p.chains_packed, Relaxed);
        self.last_chains_assembled
            .store(p.chains_assembled, Relaxed);
        self.last_hot_seams.store(p.hot_seams, Relaxed);
        self.pinned.store(p.pinned, Relaxed);
        self.has_run.store(true, Relaxed);
    }

    pub fn record_orphans_reclaimed(&self, n: u64) {
        self.orphans_reclaimed.fetch_add(n, Ordering::Relaxed);
    }

    /// Record the cadence planner's decision (tier + reason) for `monitor`.
    /// `tier`: 1 base, 2 drain, 3 fast.
    pub fn record_plan(&self, tier: u8, read_directed: bool, reason: &str) {
        self.tier.store(tier, Ordering::Relaxed);
        self.read_directed.store(read_directed, Ordering::Relaxed);
        if let Ok(mut r) = self.reason.lock() {
            r.clear();
            r.push_str(reason);
        }
    }

    /// Seed the footprint gauges from a one-time scan at store open. After this,
    /// they are maintained incrementally by [`Self::apply_footprint_delta`].
    pub fn seed_footprint(&self, f: &SegmentFootprint) {
        use Ordering::Relaxed;
        self.segment_count.store(f.segment_count, Relaxed);
        self.appended_bytes.store(f.appended_bytes, Relaxed);
        self.live_bytes.store(f.live_bytes, Relaxed);
        self.reclaimable_bytes.store(f.reclaimable_bytes, Relaxed);
        self.has_run.store(true, Relaxed);
    }

    /// Fold one commit's (or one reclaim deletion's) net counter change into the
    /// footprint gauges. Called off the commit path with the exact deltas
    /// `stage_seg_deltas` computed, so the gauges track writes and deletes in
    /// real time without a scan. Deletions pass negative `d_segments`/`d_appended`.
    pub fn apply_footprint_delta(&self, d_segments: i64, d_appended: i64, d_live: i64) {
        apply_i64(&self.segment_count, d_segments);
        apply_i64(&self.appended_bytes, d_appended);
        apply_i64(&self.live_bytes, d_live);
        self.recompute_reclaimable();
        self.has_run.store(true, Ordering::Relaxed);
    }

    /// reclaimable = appended - live, kept as its own gauge so the Prometheus
    /// and RPC readers stay unchanged. Not atomic with the two summands, but a
    /// transient off-by-a-batch on a display gauge is harmless.
    fn recompute_reclaimable(&self) {
        use Ordering::Relaxed;
        let appended = self.appended_bytes.load(Relaxed);
        let live = self.live_bytes.load(Relaxed);
        self.reclaimable_bytes
            .store(appended.saturating_sub(live), Relaxed);
    }
}

/// Creation always precedes deletion, so it never actually underflows, but clamp anyway rather than wrap.
fn apply_i64(a: &AtomicU64, d: i64) {
    use Ordering::Relaxed;
    if d >= 0 {
        a.fetch_add(d as u64, Relaxed);
    } else {
        let _ = a.fetch_update(Relaxed, Relaxed, |cur| {
            Some(cur.saturating_sub(d.unsigned_abs()))
        });
    }
}
