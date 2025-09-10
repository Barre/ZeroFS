use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use num_format::{Locale, ToFormattedString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const MB_IN_BYTES: f64 = 1_048_576.0;

struct PreviousSnapshot {
    total_operations: u64,
    bytes_read: u64,
    bytes_written: u64,
    read_operations: u64,
    write_operations: u64,
    cache_hits: u64,
    cache_misses: u64,
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
    pub gc_chunks_deleted: AtomicU64,
    pub gc_runs: AtomicU64,

    // Cache statistics
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,

    // Performance
    pub total_operations: AtomicU64,

    // Internal state for rate calculation
    last_snapshot: std::sync::Mutex<PreviousSnapshot>,
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
            gc_chunks_deleted: AtomicU64::new(0),
            gc_runs: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            total_operations: AtomicU64::new(0),
            last_snapshot: std::sync::Mutex::new(PreviousSnapshot {
                total_operations: 0,
                bytes_read: 0,
                bytes_written: 0,
                read_operations: 0,
                write_operations: 0,
                cache_hits: 0,
                cache_misses: 0,
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
        let gc_chunks = self.gc_chunks_deleted.load(Ordering::Relaxed);
        let gc_runs = self.gc_runs.load(Ordering::Relaxed);

        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
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
            (bytes_read - snapshot.bytes_read) as f64 / interval_secs / MB_IN_BYTES
        } else {
            0.0
        };

        let mb_written_per_sec = if interval_secs > 0.0 {
            (bytes_written - snapshot.bytes_written) as f64 / interval_secs / MB_IN_BYTES
        } else {
            0.0
        };

        // Calculate interval cache rate
        let interval_cache_hits = cache_hits - snapshot.cache_hits;
        let interval_cache_misses = cache_misses - snapshot.cache_misses;
        let interval_cache_rate = if interval_cache_hits + interval_cache_misses > 0 {
            (interval_cache_hits as f64 / (interval_cache_hits + interval_cache_misses) as f64)
                * 100.0
        } else {
            0.0
        };

        *snapshot = PreviousSnapshot {
            total_operations: total_ops,
            bytes_read,
            bytes_written,
            read_operations: read_ops,
            write_operations: write_ops,
            cache_hits,
            cache_misses,
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
            Cell::new("  Chunks deleted"),
            Cell::new(format!(
                "{} (in {} runs)",
                gc_chunks.to_formatted_string(&Locale::en),
                gc_runs.to_formatted_string(&Locale::en)
            )),
        ]);

        // Cache Performance section
        table.add_row(vec![
            Cell::new("Cache Performance (5s interval)")
                .fg(Color::Yellow)
                .add_attribute(Attribute::Bold),
            Cell::new(""),
        ]);

        let cache_color = if interval_cache_rate > 90.0 {
            Color::Green
        } else if interval_cache_rate > 70.0 {
            Color::Yellow
        } else {
            Color::Red
        };

        table.add_row(vec![
            Cell::new("  Hit rate"),
            Cell::new(format!(
                "{:.1}% ({} hits, {} misses)",
                interval_cache_rate,
                interval_cache_hits.to_formatted_string(&Locale::en),
                interval_cache_misses.to_formatted_string(&Locale::en)
            ))
            .fg(cache_color),
        ]);

        table.to_string()
    }

    pub fn output_report_debug(&self) {
        tracing::debug!("\n{}", self.report());
    }
}
