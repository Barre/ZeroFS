//! Persistent extent data store.
//!
//! The per-extent `b"extent"` key holds a small [`FrameLoc`](crate::segment::FrameLoc) pointer; the extent
//! bytes themselves live outside the LSM, in immutable `segments/` objects
//! written via [`SegmentStore`]. Writes are read-modify-write over full extents,
//! with sparse holes and all-zero elision.
//!
//! Writes append sealed frames to an in-RAM open segment and commit the extent
//! pointer eagerly (no PUT on the write path). The open segment is PUT in the
//! background when it crosses a size threshold, and synchronously by the flush
//! path (the fsync barrier) before the metadata it references is made durable —
//! so a durable manifest never points at an un-PUT segment.

mod read;
#[doc(hidden)]
pub mod reclaim;
#[cfg(test)]
mod test_util;
mod write;

use crate::db::{Db, ExtentRefGuard, Transaction};
use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::metrics::{SegmentFootprint, SegmentReclaimStats};
use crate::fs::write_coordinator::{LockedMutation, WeakWriteCoordinator};
use crate::fs::{EXTENT_SIZE, FsError};
use crate::segment::Segid;
use crate::segment_store::SegmentStore;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use foyer::{Cache, CacheBuilder};
use futures::stream::StreamExt;
use read::{READ_AHEAD_MAX_CONCURRENT, READ_AHEAD_TRACK_BYTES};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::error;
pub(crate) use write::SEAL_THRESHOLD;
use write::{MAX_INFLIGHT_SEALS, OpenSegment};

pub(super) const PARALLEL_EXTENT_OPS: usize = 20;

pub(super) const ZERO_EXTENT: &[u8] = &[0u8; EXTENT_SIZE];

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
    /// Serializes repack pointer swaps with foreground writes to the same inode.
    lock_manager: Arc<KeyedLockManager<InodeId>>,
    codec: Arc<FrameCodec>,
    open: Arc<Mutex<OpenSegment>>,
    /// Writers hold the read side from FrameLoc assignment through commit;
    /// reclamation takes the write side before sealing and choosing its cutoff.
    extent_ref_barrier: Arc<tokio::sync::RwLock<()>>,
    /// Serializes appends through threshold-triggered rotation. The writer that
    /// crosses the threshold keeps this gate while waiting for a seal permit,
    /// so later writers cannot keep extending an overdue open segment.
    append_gate: Arc<tokio::sync::Mutex<()>>,
    /// Finalized bytes of segments whose PUT is in flight (or failed and pending a
    /// re-PUT). Reads consult these before the object store. Ordered so the
    /// barrier's re-PUT sequence is deterministic (seal order).
    sealing: Arc<Mutex<BTreeMap<Segid, Bytes>>>,
    /// Permits = max in-flight seals; acquiring all is the fsync drain barrier.
    seal_sem: Arc<Semaphore>,
    /// Deadline after which each currently-dead segment may be deleted:
    /// recorded the first scan it's seen dead, from the longest recorded
    /// lifetime of the checkpoints listed then, so reclamation outlasts anything
    /// that could still reference it. Deadlines use monotonic time so subsequent
    /// wall-clock adjustments cannot shorten or extend retention.
    delete_at: Arc<Mutex<HashMap<Segid, Instant>>>,
    /// Per-inode logical read-ahead state: (last_read_end, prefetched_to, seq_run).
    read_ahead: Cache<InodeId, (u64, u64, u32)>,
    /// Global bound on concurrent read-ahead fetches.
    prefetch_sem: Arc<Semaphore>,
    /// Buffer size that triggers a background seal (i.e. the segment object size).
    /// Tests and the DST harness lower it at construction time so seal paths do
    /// not allocate 256 MiB.
    seal_threshold: usize,
    /// Commit queue for extent mutations and segment-counter updates.
    write_coordinator: WeakWriteCoordinator,
    /// Serializes counter-based cycles and orphan sweeps. In particular, an
    /// orphan sweep must not observe an as-yet-uncredited repack output.
    segment_reclaim_lock: Arc<tokio::sync::Mutex<()>>,
    /// Global bound for reclaimer point reads and repoint transactions. Job
    /// concurrency controls payload work; this prevents nested metadata
    /// fan-out from multiplying without bound.
    reclaim_metadata_sem: Arc<Semaphore>,
    /// Segment-reclamation counters and footprint gauges, bridged to Prometheus.
    /// Seeded at boot, updated on committed segment deltas, and annotated by
    /// reclaim cycles.
    segment_reclaim_stats: Arc<SegmentReclaimStats>,
    /// Changes that can alter the result of the next durable counter scan.
    reclaim_activity: Arc<reclaim::Activity>,
    #[cfg(any(test, dst))]
    reclaim_clock: Arc<std::sync::OnceLock<Arc<dyn slatedb_common::SystemClock>>>,
}

impl ExtentStore {
    pub(crate) fn new(
        db: Arc<Db>,
        key_codec: Arc<KeyCodec>,
        segments: Arc<SegmentStore>,
        lock_manager: Arc<KeyedLockManager<InodeId>>,
        seal_threshold: usize,
        write_coordinator: WeakWriteCoordinator,
    ) -> Self {
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
            extent_ref_barrier: Arc::new(tokio::sync::RwLock::new(())),
            append_gate: Arc::new(tokio::sync::Mutex::new(())),
            sealing: Arc::new(Mutex::new(BTreeMap::new())),
            seal_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_SEALS)),
            delete_at: Arc::new(Mutex::new(HashMap::new())),
            read_ahead,
            prefetch_sem: Arc::new(Semaphore::new(READ_AHEAD_MAX_CONCURRENT)),
            seal_threshold,
            write_coordinator,
            segment_reclaim_lock: Arc::new(tokio::sync::Mutex::new(())),
            reclaim_metadata_sem: Arc::new(Semaphore::new(PARALLEL_EXTENT_OPS)),
            segment_reclaim_stats: Arc::new(SegmentReclaimStats::default()),
            reclaim_activity: Arc::new(reclaim::Activity::default()),
            #[cfg(any(test, dst))]
            reclaim_clock: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Segment-reclamation metrics holder, for the Prometheus bridge.
    pub fn segment_reclaim_stats(&self) -> Arc<SegmentReclaimStats> {
        Arc::clone(&self.segment_reclaim_stats)
    }

    pub(crate) fn record_reclaim_activity(&self) {
        self.reclaim_activity.record();
    }

    /// Inject wall time for clock-jump tests or share SlateDB's virtual DST clock.
    #[cfg(any(test, dst))]
    pub fn set_reclaim_clock(&self, clock: Arc<dyn slatedb_common::SystemClock>) {
        assert!(
            self.reclaim_clock.set(clock).is_ok(),
            "reclaim clock already set"
        );
    }

    pub(super) fn reclaim_now(&self) -> DateTime<Utc> {
        #[cfg(any(test, dst))]
        if let Some(clock) = self.reclaim_clock.get() {
            return clock.now();
        }
        Utc::now()
    }

    async fn new_extent_ref_guard(&self) -> ExtentRefGuard {
        Arc::new(self.extent_ref_barrier.clone().read_owned().await)
    }

    /// Attach one publication guard before assigning any FrameLoc.
    pub(super) async fn protect_extent_ref(&self, txn: &mut Transaction) {
        if !txn.has_extent_ref_guard() {
            txn.hold_extent_ref_guard(self.new_extent_ref_guard().await);
        }
    }

    /// Sum segment counters to seed footprint gauges at open and verify
    /// incremental updates in tests.
    pub async fn sample_footprint(&self) -> Result<SegmentFootprint, FsError> {
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut stream = self.db.scan(sc_start..sc_end).await.map_err(|e| {
            error!("segment footprint scan failed: {}", e);
            FsError::IoError
        })?;
        let (mut segment_count, mut live_bytes, mut appended_bytes) = (0u64, 0u64, 0u64);
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            let Some((epoch, counter)) = self.key_codec.parse_segcount_key(&key) else {
                continue;
            };
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                error!(
                    "segment footprint scan found a malformed counter for {:?}",
                    Segid::new(epoch, counter)
                );
                return Err(FsError::IoError);
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
    pub(crate) async fn seed_footprint(&self) -> Result<(), FsError> {
        let f = self.sample_footprint().await?;
        self.segment_reclaim_stats.seed_footprint(&f);
        Ok(())
    }

    /// Raw frame bytes held in RAM, not yet PUT to the object store: the open
    /// write buffer plus sealed segments whose PUT is still in flight. This can
    /// include frames superseded by newer buffered writes, so it is not a live-
    /// byte subset. Read fresh (it is volatile); cheap in-memory lengths.
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

    /// No seal PUT is in flight or awaiting a retry.
    #[cfg(test)]
    pub(super) fn seals_quiet(&self) -> bool {
        self.seal_sem.available_permits() == MAX_INFLIGHT_SEALS
            && self.sealing.lock().unwrap().is_empty()
    }

    /// Submit a transaction to the filesystem's only commit path.
    pub(crate) async fn commit_transaction(&self, txn: Transaction) -> Result<(), FsError> {
        self.write_coordinator.commit(txn).await
    }

    /// Submit a transaction together with the inode locks used to stage it.
    pub(crate) async fn commit_locked_transaction(
        &self,
        mutation: LockedMutation,
    ) -> Result<(), FsError> {
        self.write_coordinator.commit_locked(mutation).await
    }

    /// Test-only: lower the seal threshold so seal-path tests don't build a full
    /// 256 MiB segment.
    #[cfg(test)]
    fn with_seal_threshold(mut self, n: usize) -> Self {
        self.seal_threshold = n;
        self
    }

    pub(super) fn seal_threshold(&self) -> usize {
        self.seal_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;

    #[tokio::test]
    async fn concurrent_commits_preserve_counter_deltas() {
        const WRITERS: usize = 64;
        let (store, db) = make().await;
        let segid = Segid::new(7, 99);
        let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS));
        let mut tasks = Vec::with_capacity(WRITERS);
        for _ in 0..WRITERS {
            let store = store.clone();
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                let mut txn = db.new_transaction().unwrap();
                txn.hold_extent_ref_guard(store.new_extent_ref_guard().await);
                store.seg_delta(&mut txn, segid, 1, 1);
                barrier.wait().await;
                store.commit_transaction(txn).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(
            segcount_pair_of(&store, &db, segid).await,
            (WRITERS as u64, WRITERS as u64),
            "the commit worker must not lose read-modify-write deltas"
        );
    }

    // The commit worker maintains footprint gauges incrementally, so they must
    // track writes and overwrites with no reclaim scan and always agree with an
    // authoritative scan of the same state.
    #[tokio::test]
    async fn footprint_gauges_track_writes_incrementally() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        use std::sync::atomic::Ordering::Relaxed;
        store
            .reclaim_activity
            .acknowledge(store.reclaim_activity.generation());
        let m = store.segment_reclaim_stats();
        assert_eq!(m.appended_bytes.load(Relaxed), 0);
        assert_eq!(m.segment_count.load(Relaxed), 0);

        // Write 3 extents: appended + live grow, all live, no reclaim scan.
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
        assert_eq!(m.footprint().reclaimable_bytes, 0);
        assert_eq!(m.segment_count.load(Relaxed), 1);
        assert!(
            !store.reclaim_activity.pending(),
            "fresh appends cannot make a segment reclaimable"
        );
        // The incremental gauges match an authoritative scan of the same state.
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f.appended_bytes, appended);
        assert_eq!(f.live_bytes, m.live_bytes.load(Relaxed));
        assert_eq!(f.segment_count, 1);

        // Overwrite extent 0: a new frame is appended and the old one becomes
        // dead weight, visible immediately with no reclaim scan.
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
        assert!(m.footprint().reclaimable_bytes > 0, "old frame now dead");
        assert!(store.reclaim_activity.pending());
        assert!(m.live_bytes.load(Relaxed) < m.appended_bytes.load(Relaxed));
        let f = store.sample_footprint().await.unwrap();
        assert_eq!(f, m.footprint());
    }
}
