//! Persistent extent data store.
//!
//! The per-extent `b"extent"` key holds a small [`FrameLoc`](crate::segment::FrameLoc) pointer; the extent
//! bytes themselves live outside the LSM, in immutable `segments/` objects
//! written via [`SegmentStore`]. Writes are read-modify-write over full extents,
//! with sparse holes, all-zero elision, and a tail cache for sequential appends.
//!
//! Writes append sealed frames to an in-RAM open segment and commit the extent
//! pointer eagerly (no PUT on the write path). The open segment is PUT in the
//! background when it crosses a size threshold, and synchronously by the flush
//! path (the fsync barrier) before the metadata it references is made durable —
//! so a durable manifest never points at an un-PUT segment.

mod compact;
mod read;
mod reclaim;
mod select;
#[cfg(test)]
mod test_util;
mod write;

pub(crate) use reclaim::QUIESCENT_AFTER_DEFAULT;
pub use reclaim::{ChainOutcome, PassOutcome, PassStatus};

use crate::db::{Db, Transaction};
use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::metrics::{SegmentFootprint, SegmentGcStats};
use crate::fs::{EXTENT_SIZE, FsError};
use crate::segment::Segid;
use crate::segment_store::SegmentStore;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use foyer::{Cache, CacheBuilder};
use futures::stream::StreamExt;
use read::{READ_AHEAD_MAX_CONCURRENT, READ_AHEAD_TRACK_BYTES};
use select::{NominationSet, PairStats};
use slatedb::config::WriteOptions;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::error;
use write::{MAX_INFLIGHT_SEALS, OpenSegment, SEAL_THRESHOLD, TAIL_CACHE_BYTES};

pub(super) const PARALLEL_EXTENT_OPS: usize = 20;

pub(super) const ZERO_EXTENT: &[u8] = &[0u8; EXTENT_SIZE];

/// What a `write` leaves for the tail cache. Applied by the caller only after
/// the transaction commits, so the cache never runs ahead of durable state.
pub enum TailUpdate {
    Set { extent_idx: u64, data: Bytes },
    Clear,
    Keep,
}

/// Human-readable byte size for log lines, e.g. "3.1 GiB". Display-only.
pub(crate) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

#[derive(Clone)]
pub struct ExtentStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    segments: Arc<SegmentStore>,
    /// Same per-inode write lock the foreground path uses, so the coalescer's
    /// conditional swap can't be clobbered by a concurrent write.
    lock_manager: Arc<KeyedLockManager<InodeId>>,
    codec: Arc<FrameCodec>,
    open: Arc<Mutex<OpenSegment>>,
    /// Finalized bytes of segments whose PUT is in flight (or failed and pending a
    /// re-PUT). Reads consult these before the object store. Ordered so the
    /// barrier's re-PUT sequence is deterministic (seal order).
    sealing: Arc<Mutex<BTreeMap<Segid, Bytes>>>,
    /// Permits = max in-flight seals; acquiring all is the fsync drain barrier.
    seal_sem: Arc<Semaphore>,
    /// Deadline after which each currently-dead segment may be deleted:
    /// recorded the first pass it's seen dead, from the latest expiry of the
    /// checkpoints active then, so reclamation outlasts anything that could
    /// still reference it.
    delete_at: Arc<Mutex<HashMap<Segid, DateTime<Utc>>>>,
    /// Read-nominated compaction hints (see [`NominationSet`]): pushed by the
    /// demand-read path, drained by each reclaim pass to prioritize selection.
    nominations: Arc<Mutex<NominationSet>>,
    /// Whether reads nominate at all. Set once when segment GC starts.
    nominations_enabled: Arc<AtomicBool>,
    /// Crossing-pair heat (see [`PairStats`]): bumped by demand reads that
    /// fetch adjacent file data across two on-store segments, read by each
    /// reclaim pass to form chain-compaction selections.
    pair_stats: Arc<Mutex<PairStats>>,
    /// Monotone reclaim-pass counter: the clock for pair-stat episode dedup
    /// (staleness is wall-clock). Bumped once per pass, pinned passes included.
    gc_round: Arc<std::sync::atomic::AtomicU64>,
    /// Write-quiescence tracking: the (epoch, cutoff) the previous pass saw
    /// and the monotonic instant it was first seen unchanged. Touched only by
    /// the single segment-gc task.
    quiescence: Arc<Mutex<(u64, u64, Instant)>>,
    /// Per-inode copy of the most-recently-written (extent_idx, full extent), so a
    /// sequential append splices into it rather than re-decoding the buffered/sealed
    /// frame. Eviction only ever costs a re-fetch.
    tail_cache: Cache<InodeId, (u64, Bytes)>,
    /// Per-inode logical read-ahead state: (last_read_end, prefetched_to, seq_run).
    read_ahead: Cache<InodeId, (u64, u64, u32)>,
    /// Global bound on concurrent read-ahead fetches.
    prefetch_sem: Arc<Semaphore>,
    /// Buffer size that triggers a background seal (i.e. the segment object size).
    /// Defaults to the const; tests lower it so seal paths don't allocate 256 MiB.
    /// Shared across clones so a test can retune the instance the fs holds.
    seal_threshold: Arc<std::sync::atomic::AtomicUsize>,
    /// Weak handle to the commit worker, injected post-construction (the worker owns
    /// an `ExtentStore` clone, so a strong handle would cycle). Set in production;
    /// unset in the extent unit tests, where `commit_via_coordinator` is the sole
    /// segcount writer and commits directly.
    coordinator: Arc<std::sync::OnceLock<crate::fs::write_coordinator::WeakWriteCoordinator>>,
    /// Reclaim/compaction counters and footprint gauges, bridged to Prometheus.
    /// Written only by the segment-GC task (see `reclaim_segments_gated`).
    segment_gc_stats: Arc<SegmentGcStats>,
}

impl ExtentStore {
    pub fn new(
        db: Arc<Db>,
        key_codec: Arc<KeyCodec>,
        segments: Arc<SegmentStore>,
        lock_manager: Arc<KeyedLockManager<InodeId>>,
    ) -> Self {
        let tail_cache = CacheBuilder::new(TAIL_CACHE_BYTES)
            .with_weighter(|_id: &InodeId, (_idx, data): &(u64, Bytes)| data.len())
            .build();
        let read_ahead = CacheBuilder::new(READ_AHEAD_TRACK_BYTES)
            .with_weighter(|_: &InodeId, _: &(u64, u64, u32)| 24)
            .build();
        let codec = segments.codec();
        let open = Arc::new(Mutex::new(OpenSegment {
            segid: segments.next_segid(),
            buf: Vec::new(),
            dir: Vec::new(),
        }));
        Self {
            db,
            key_codec,
            segments,
            lock_manager,
            codec,
            open,
            sealing: Arc::new(Mutex::new(BTreeMap::new())),
            seal_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_SEALS)),
            delete_at: Arc::new(Mutex::new(HashMap::new())),
            nominations: Arc::new(Mutex::new(NominationSet::default())),
            nominations_enabled: Arc::new(AtomicBool::new(false)),
            pair_stats: Arc::new(Mutex::new(PairStats::default())),
            gc_round: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            quiescence: Arc::new(Mutex::new((0, 0, Instant::now()))),
            tail_cache,
            read_ahead,
            prefetch_sem: Arc::new(Semaphore::new(READ_AHEAD_MAX_CONCURRENT)),
            seal_threshold: Arc::new(std::sync::atomic::AtomicUsize::new(SEAL_THRESHOLD)),
            coordinator: Arc::new(std::sync::OnceLock::new()),
            segment_gc_stats: Arc::new(SegmentGcStats::default()),
        }
    }

    /// Reclaim/compaction metrics holder, for the Prometheus bridge.
    pub fn segment_gc_stats(&self) -> Arc<SegmentGcStats> {
        Arc::clone(&self.segment_gc_stats)
    }

    /// One-time footprint scan: sums the segcount rows into the aggregate the
    /// monitor gauges track. Used to seed those gauges at open (after which they
    /// are maintained incrementally off the commit path) and as the ground data
    /// the incremental path is tested against.
    pub async fn sample_footprint(&self) -> Result<SegmentFootprint, FsError> {
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut stream = self.db.scan(sc_start..sc_end).await.map_err(|e| {
            error!("segment footprint scan failed: {}", e);
            FsError::IoError
        })?;
        let (mut segment_count, mut live_bytes, mut appended_bytes) = (0u64, 0u64, 0u64);
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if self.key_codec.parse_segcount_key(&key).is_none() {
                continue;
            }
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                continue;
            };
            segment_count += 1;
            live_bytes += live;
            appended_bytes += total;
        }
        Ok(SegmentFootprint {
            segment_count,
            appended_bytes,
            live_bytes,
            reclaimable_bytes: appended_bytes.saturating_sub(live_bytes),
        })
    }

    /// Seed the monitor footprint gauges from a one-time scan. Call at store
    /// open, before writes begin, so the incremental deltas start from the
    /// existing on-store footprint.
    pub async fn seed_footprint(&self) -> Result<(), FsError> {
        let f = self.sample_footprint().await?;
        self.segment_gc_stats.seed_footprint(&f);
        Ok(())
    }

    /// Bytes held in RAM, not yet PUT to the object store: the open write
    /// buffer plus any sealed segments whose PUT is still in flight. This is
    /// the write-back buffer, the recently-written data a crash would lose
    /// without a flush. Read fresh (it is volatile); cheap in-memory lengths.
    pub fn unflushed_bytes(&self) -> u64 {
        let open = self.open.lock().unwrap().buf.len() as u64;
        let sealing: u64 = self
            .sealing
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum();
        open + sealing
    }

    /// Inject the commit worker's weak handle so this store's GC/compaction
    /// seg-delta txns route through the single writer. Idempotent.
    pub fn set_coordinator(&self, coord: crate::fs::write_coordinator::WeakWriteCoordinator) {
        let _ = self.coordinator.set(coord);
    }

    /// Enable read-path compaction nominations.
    pub fn enable_nominations(&self) {
        self.nominations_enabled.store(true, Ordering::Relaxed);
    }

    /// No seal PUT in flight or pending re-PUT. Fast passes must not queue
    /// the barrier's all-permits drain behind a seal burst.
    pub fn seals_quiet(&self) -> bool {
        self.seal_sem.available_permits() == MAX_INFLIGHT_SEALS
            && self.sealing.lock().unwrap().is_empty()
    }

    /// Commit a txn that may carry seg-count deltas. In production this hands off to
    /// the commit worker (the sole segcount writer); with no coordinator (unit tests)
    /// we are the only writer, so materialize the deltas and commit directly.
    async fn commit_via_coordinator(&self, mut txn: Transaction) -> Result<(), FsError> {
        if let Some(coord) = self.coordinator.get() {
            return coord.commit(txn).await;
        }
        let deltas = txn.take_seg_deltas();
        let mut batch = txn.into_inner();
        let (_, footprint_delta) =
            crate::fs::write_coordinator::stage_seg_deltas(&self.db, deltas, &mut batch).await?;
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;
        // Committed: fold the batch's net footprint into the monitor gauges,
        // mirroring the write coordinator's apply on its own path.
        self.segment_gc_stats.apply_footprint_delta(
            footprint_delta.d_segments,
            footprint_delta.d_appended,
            footprint_delta.d_live,
        );
        Ok(())
    }

    /// Test-only: lower the seal threshold so seal-path tests don't build a full
    /// 256 MiB segment.
    #[cfg(test)]
    fn with_seal_threshold(self, n: usize) -> Self {
        self.set_seal_threshold(n);
        self
    }

    /// Retune the seal threshold on a live store (shared across clones). For the
    /// DST harness, which needs small segments to exercise the seal/reclaim paths.
    #[allow(dead_code)] // used by the DST integration test, not the lib/bin
    pub fn set_seal_threshold(&self, n: usize) {
        self.seal_threshold
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub(super) fn seal_threshold(&self) -> usize {
        self.seal_threshold
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;

    // The footprint gauges are maintained incrementally off the commit path, so
    // they must track writes and overwrites with no reclaim pass, and always
    // agree with an authoritative scan of the same state.
    #[tokio::test]
    async fn footprint_gauges_track_writes_incrementally() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        use std::sync::atomic::Ordering::Relaxed;
        let m = store.segment_gc_stats();
        assert_eq!(m.appended_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 0);

        // Write 3 extents: appended + live grow, all live, no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![7u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let appended = m.appended_bytes.load(Relaxed);
        assert!(appended > 0);
        assert_eq!(m.live_bytes.load(Relaxed), appended, "all live");
        assert_eq!(m.reclaimable_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 1);
        // The incremental gauges match an authoritative scan of the same state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, appended);
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.segment_count, 1);

        // Overwrite extent 0: a new frame is appended and the old one becomes
        // dead weight, visible immediately with no reclaim pass.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert!(
            m.appended_bytes.load(Relaxed) > appended,
            "new frame appended"
        );
        assert!(m.reclaimable_bytes.load(Relaxed) > 0, "old frame now dead");
        assert!(m.live_bytes.load(Relaxed) < m.appended_bytes.load(Relaxed));
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, m.appended_bytes.load(Relaxed));
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.reclaimable_bytes, m.reclaimable_bytes.load(Relaxed));
    }
}
