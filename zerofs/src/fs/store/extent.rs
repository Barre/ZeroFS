//! Persistent extent data store.
//!
//! The per-extent `b"extent"` key holds a small [`FrameLoc`] pointer; the extent
//! bytes themselves live outside the LSM, in immutable `segments/` objects
//! written via [`SegmentStore`]. Writes are read-modify-write over full extents,
//! with sparse holes, all-zero elision, and a tail cache for sequential appends.
//!
//! Writes append sealed frames to an in-RAM open segment and commit the extent
//! pointer eagerly (no PUT on the write path). The open segment is PUT in the
//! background when it crosses a size threshold, and synchronously by the flush
//! path (the fsync barrier) before the metadata it references is made durable —
//! so a durable manifest never points at an un-PUT segment.

use crate::db::{Db, Transaction};
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::{EXTENT_SIZE, FsError};
use crate::replication::ReplOp;
use crate::segment::{DirEntry, FrameLoc, Segid};
use crate::segment_store::{SegmentStore, SegmentStoreError};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use foyer::{Cache, CacheBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use slatedb::config::WriteOptions;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

const PARALLEL_EXTENT_OPS: usize = 20;

/// Frames per write batch before pre-compression fans out on rayon; below
/// this the dispatch overhead outweighs the parallelism.
const PARALLEL_COMPRESS_MIN_FRAMES: usize = 8;
const ZERO_EXTENT: &[u8] = &[0u8; EXTENT_SIZE];
const TAIL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Logical (file-offset) read-ahead distance for a confirmed-sequential
/// stream. Follows the file's extents across segments, which the physical
/// (per-object) prefetcher can't do.
const READ_AHEAD_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
/// Consecutive sequential reads before prefetch kicks in, so a one-off read
/// doesn't drag in a whole window.
const READ_AHEAD_MIN_SEQ: u32 = 2;
/// Global cap on concurrent in-flight read-ahead fetches.
const READ_AHEAD_MAX_CONCURRENT: usize = 16;
/// Bound on the per-inode read-ahead state map (LRU-evicted; ~24 B/entry).
const READ_AHEAD_TRACK_BYTES: usize = 4 * 1024 * 1024;

/// Given the per-inode read-ahead state `(last_read_end, prefetched_to, seq_run)`
/// and a read of `[offset, offset+length)`, return the new state and, when a
/// prefetch is warranted, the `[start, end)` file range to fetch.
fn plan_read_ahead(
    (last_end, prefetched_to, seq): (u64, u64, u32),
    offset: u64,
    length: u64,
) -> ((u64, u64, u32), Option<(u64, u64)>) {
    let read_end = offset + length;
    // A jump starts a new (unconfirmed) sequence.
    if offset != last_end {
        return ((read_end, read_end, 1), None);
    }
    let seq = seq.saturating_add(1);
    if seq < READ_AHEAD_MIN_SEQ {
        return ((read_end, read_end, seq), None);
    }
    // Refill only once less than half a window remains ahead: few large
    // fetches, not one tiny fetch per read.
    let ahead = prefetched_to.saturating_sub(read_end);
    if ahead >= READ_AHEAD_WINDOW_BYTES / 2 {
        return ((read_end, prefetched_to, seq), None);
    }
    let target = read_end + READ_AHEAD_WINDOW_BYTES;
    let start = prefetched_to.max(read_end);
    ((read_end, target, seq), Some((start, target)))
}

/// Seal (PUT) the open segment once its packed frames reach this size, bounding
/// the in-RAM buffer between flushes. The seal PUT is concurrent multipart
/// (`SegmentStore::put_segment`), so its fsync-path latency stays bounded
/// despite the size.
const SEAL_THRESHOLD: usize = 256 * 1024 * 1024;

/// Max segments sealing (PUT in flight) concurrently. Bounds the un-PUT RAM in
/// `sealing` to ~this × SEAL_THRESHOLD; acquiring all permits is the fsync
/// drain barrier.
const MAX_INFLIGHT_SEALS: usize = 4;

/// Compaction thresholds. A live segment is a candidate when *fragmented*
/// (live bytes below this percent of total) or *small* (below
/// SMALL_SEGMENT_BYTES). Dense, full-size segments are left alone: re-sealing
/// one 1:1 reclaims nothing.
const FRAG_LIVE_PERCENT: u64 = 50;
const SMALL_SEGMENT_BYTES: u64 = 1 << 20; // 1 MiB

/// Live frames evacuated from candidates are repacked into segments of this
/// target size (the same threshold the open buffer seals at).
const PACK_TARGET_BYTES: u64 = SEAL_THRESHOLD as u64;

/// Per-round compaction bounds, so a large backlog is worked down incrementally.
const MAX_COMPACT_SEGMENTS_PER_ROUND: usize = 64;
const MAX_COMPACT_BYTES_PER_ROUND: u64 = 256 << 20; // 256 MiB (~one PACK_TARGET segment/round)

/// A compaction round must free at least this much dead space to run (unless
/// enough small-segment live bytes have accumulated to pack a dense segment;
/// see `reclaim_segments_gated`). A near-1:1 repack is churn.
const MIN_FREED_BYTES: u64 = SMALL_SEGMENT_BYTES;

/// Cap on buffered compaction candidates, so a huge store can't make the list
/// O(#segments). When reached, the most-fragmented half is kept.
const MAX_CANDIDATES_BUFFERED: usize = 8192;

/// Cap on segments verified + deleted per reclaim pass; each delete costs a
/// dir read plus O(frames) point-lookups. Deferred segments keep their
/// (already-elapsed) deadline and are retried next pass.
const MAX_SEGMENT_DELETES_PER_PASS: usize = 1024;

/// Orphan-sweep age floor: an uncounted segment object must have been PUT at
/// least this long ago before it is deletable. Guards the window where an
/// object exists but its crediting commit is still in flight (a compaction
/// seal PUTs before its repoint commits). Judged from the object's
/// `last_modified`, not process-local state, so a restarted process still
/// reclaims old orphans on its first sweep.
const ORPHAN_MIN_AGE_SECS: i64 = 60 * 60;

/// A coalesced run of contiguous live frames within one source segment, for the
/// compaction gather: (byte_offset, total byte_len, first frame index, the
/// (inode, extent) slots it covers). Read back in one ranged GET.
type CompactRun = (u64, u32, u32, Vec<(InodeId, u64)>);

/// Outcome of checking whether a segment the counter calls dead is truly
/// reclaimable, distinguishing the three cases the caller must handle differently.
enum SegmentDeadVerdict {
    /// Object present, directory read OK, no extent still points here: delete it.
    Reclaim,
    /// Object already gone (NotFound). Nothing to delete; the caller drops the
    /// leaked counter a crash left behind (object deleted, counter-drop uncommitted).
    ObjectAbsent,
    /// A live frame still points here, or a transient read error: keep (fail-closed).
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

/// What a `write` leaves for the tail cache. Applied by the caller only after
/// the transaction commits, so the cache never runs ahead of durable state.
pub enum TailUpdate {
    Set { extent_idx: u64, data: Bytes },
    Clear,
    Keep,
}

/// The in-RAM open segment. Frames are sealed (compressed+encrypted) and appended
/// here at write time, so an extent's location is known and committed eagerly;
/// the segment object is PUT only on flush or when it crosses [`SEAL_THRESHOLD`].
struct OpenSegment {
    segid: Segid,
    buf: Vec<u8>,
    dir: Vec<DirEntry>,
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
    /// re-PUT). Reads consult these before the object store.
    sealing: Arc<Mutex<HashMap<Segid, Bytes>>>,
    /// Permits = max in-flight seals; acquiring all is the fsync drain barrier.
    seal_sem: Arc<Semaphore>,
    /// Deadline after which each currently-dead segment may be deleted:
    /// recorded the first pass it's seen dead, from the latest expiry of the
    /// checkpoints active then, so reclamation outlasts anything that could
    /// still reference it.
    delete_at: Arc<Mutex<HashMap<Segid, DateTime<Utc>>>>,
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
    seal_threshold: usize,
    /// Weak handle to the commit worker, injected post-construction (the worker owns
    /// an `ExtentStore` clone, so a strong handle would cycle). Set in production;
    /// unset in the extent unit tests, where `commit_via_coordinator` is the sole
    /// segcount writer and commits directly.
    coordinator: Arc<std::sync::OnceLock<crate::fs::write_coordinator::WeakWriteCoordinator>>,
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
            sealing: Arc::new(Mutex::new(HashMap::new())),
            seal_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_SEALS)),
            delete_at: Arc::new(Mutex::new(HashMap::new())),
            tail_cache,
            read_ahead,
            prefetch_sem: Arc::new(Semaphore::new(READ_AHEAD_MAX_CONCURRENT)),
            seal_threshold: SEAL_THRESHOLD,
            coordinator: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Inject the commit worker's weak handle so this store's GC/compaction
    /// seg-delta txns route through the single writer. Idempotent.
    pub fn set_coordinator(&self, coord: crate::fs::write_coordinator::WeakWriteCoordinator) {
        let _ = self.coordinator.set(coord);
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
        Ok(())
    }

    /// Test-only: lower the seal threshold so seal-path tests don't build a full
    /// 256 MiB segment.
    #[cfg(test)]
    fn with_seal_threshold(mut self, n: usize) -> Self {
        self.seal_threshold = n;
        self
    }

    fn tail_get(&self, id: InodeId) -> Option<(u64, Bytes)> {
        self.tail_cache.get(&id).map(|e| (*e).clone())
    }
    fn tail_set(&self, id: InodeId, extent_idx: u64, data: Bytes) {
        self.tail_cache.insert(id, (extent_idx, data));
    }
    fn tail_invalidate(&self, id: InodeId) {
        self.tail_cache.remove(&id);
    }

    /// Apply a `write`'s tail-cache effect. Call only after its commit succeeds.
    pub fn apply_tail_update(&self, id: InodeId, update: TailUpdate) {
        match update {
            TailUpdate::Set { extent_idx, data } => self.tail_set(id, extent_idx, data),
            TailUpdate::Clear => self.tail_invalidate(id),
            TailUpdate::Keep => {}
        }
    }

    /// Raw `[len][sealed]` bytes of a frame still resident in RAM (the open buffer
    /// or an in-flight seal), or `None` once its segment is PUT (the standby reads
    /// the shared store directly). Used to ship un-PUT segments' bytes for HA.
    fn read_frame_for_ship(&self, segid: Segid, byte_offset: u64, byte_len: u32) -> Option<Bytes> {
        let start = byte_offset as usize;
        let end = start.checked_add(byte_len as usize)?;
        {
            let open = self.open.lock().unwrap();
            if open.segid == segid {
                return open.buf.get(start..end).map(Bytes::copy_from_slice);
            }
        }
        let sealing = self.sealing.lock().unwrap();
        let bytes = sealing.get(&segid)?;
        (end <= bytes.len()).then(|| bytes.slice(start..end))
    }

    /// Enrich a batch's replication ops: an extent-write `Put` whose segment is
    /// still un-PUT becomes a `PutFrame` carrying the sealed frame bytes, so
    /// the standby can materialize that segment on takeover. Already-PUT
    /// segments stay plain `Put`. Called by the commit worker when replicating.
    pub fn enrich_repl_ops(&self, ops: Vec<ReplOp>) -> Vec<ReplOp> {
        ops.into_iter()
            .map(|op| match op {
                ReplOp::Put(k, v) => {
                    if self.key_codec.parse_extent_key(&k).is_some()
                        && let Some(loc) = FrameLoc::decode(&v)
                        && let Some(frame) =
                            self.read_frame_for_ship(loc.segid, loc.byte_offset, loc.byte_len)
                    {
                        ReplOp::PutFrame(k, v, frame)
                    } else {
                        ReplOp::Put(k, v)
                    }
                }
                other => other,
            })
            .collect()
    }

    /// The full-extent (EXTENT_SIZE) plaintext for `(id, extent)`, or `None` for a
    /// hole. Resolves the extent key's `FrameLoc` then fetches the frame.
    pub async fn get(&self, id: InodeId, extent_idx: u64) -> Result<Option<Bytes>, FsError> {
        let key = self.key_codec.extent_key(id, extent_idx);
        let encoded = match self.db.get_bytes(&key).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to read extent (inode={}, extent={}): {}",
                    id, extent_idx, e
                );
                return Err(FsError::IoError);
            }
        };
        let Some(encoded) = encoded else {
            return Ok(None);
        };
        let loc = FrameLoc::decode(&encoded).ok_or_else(|| {
            error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
            FsError::IoError
        })?;
        if let Some(mut frames) = self.read_frames_in_ram(
            loc.segid,
            loc.byte_offset,
            loc.byte_len,
            loc.frame_index,
            &[(id, extent_idx)],
        )? {
            let frame = frames.pop().expect("one frame");
            Self::validate_extent_frame(id, extent_idx, &frame)?;
            return Ok(Some(frame));
        }
        match self.segments.read_extent(loc, id, extent_idx).await {
            Ok(b) => {
                Self::validate_extent_frame(id, extent_idx, &b)?;
                Ok(Some(b))
            }
            Err(first_err) => {
                // GC compaction repoints an extent to a freshly-sealed segment and
                // then deletes the drained source; a read that resolved the old
                // FrameLoc just before that delete can race it and 404. Re-resolve
                // the extent key once: if the pointer moved, read the new location;
                // if the extent was deleted concurrently (truncate/unlink) it is now
                // a hole; otherwise the error is real.
                let reresolved = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?;
                match Self::decode_reresolved_extent(id, extent_idx, reresolved.as_ref())? {
                    Some(new_loc) if new_loc.segid != loc.segid => {
                        if let Some(mut frames) = self.read_frames_in_ram(
                            new_loc.segid,
                            new_loc.byte_offset,
                            new_loc.byte_len,
                            new_loc.frame_index,
                            &[(id, extent_idx)],
                        )? {
                            let frame = frames.pop().expect("one frame");
                            Self::validate_extent_frame(id, extent_idx, &frame)?;
                            return Ok(Some(frame));
                        }
                        match self.segments.read_extent(new_loc, id, extent_idx).await {
                            Ok(b) => {
                                Self::validate_extent_frame(id, extent_idx, &b)?;
                                Ok(Some(b))
                            }
                            Err(e) => {
                                error!(
                                    "Failed to read frame after repoint retry \
                                     (inode={}, extent={}): {}",
                                    id, extent_idx, e
                                );
                                Err(FsError::IoError)
                            }
                        }
                    }
                    None => Ok(None),
                    _ => {
                        error!(
                            "Failed to read frame (inode={}, extent={}): {}",
                            id, extent_idx, first_err
                        );
                        Err(FsError::IoError)
                    }
                }
            }
        }
    }

    /// Every stored frame decodes to exactly one full [`EXTENT_SIZE`] extent.
    /// Enforce that before callers slice the plaintext, so a bad frame that
    /// still passed AEAD surfaces as EIO instead of a panic.
    fn validate_extent_frame(id: InodeId, extent: u64, data: &Bytes) -> Result<(), FsError> {
        if data.len() != EXTENT_SIZE {
            error!(
                "Extent frame (inode={}, extent={}) decoded to {} bytes, expected {}",
                id,
                extent,
                data.len(),
                EXTENT_SIZE
            );
            return Err(FsError::IoError);
        }
        Ok(())
    }

    /// Classify the re-resolved extent value in `get`'s GC-repoint retry.
    /// An absent key means a concurrent truncate/unlink made the extent a
    /// genuine hole; a present but undecodable value is the same
    /// corrupt-value EIO as the primary path — never a fabricated hole of
    /// zeros.
    fn decode_reresolved_extent(
        id: InodeId,
        extent: u64,
        enc: Option<&Bytes>,
    ) -> Result<Option<FrameLoc>, FsError> {
        match enc {
            None => Ok(None),
            Some(enc) => match FrameLoc::decode(enc) {
                Some(loc) => Ok(Some(loc)),
                None => {
                    error!("Corrupt extent value (inode={}, extent={})", id, extent);
                    Err(FsError::IoError)
                }
            },
        }
    }

    /// Read a contiguous run of frames from the open or an in-flight sealing
    /// buffer (read-your-writes), or `None` if `segid` is already on the object
    /// store. Frame offsets are identical in the buffer and the finalized
    /// segment, so the same slice works for both.
    fn read_frames_in_ram(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Option<Vec<Bytes>>, FsError> {
        // The range comes from a db-stored FrameLoc: bounds-checked, never
        // trusted, so a corrupt value surfaces as EIO instead of an
        // out-of-range panic that would poison the open-segment lock for
        // every later writer.
        fn region(
            buf: &[u8],
            segid: Segid,
            byte_offset: u64,
            byte_len: u32,
        ) -> Result<&[u8], FsError> {
            let start = byte_offset as usize;
            start
                .checked_add(byte_len as usize)
                .and_then(|end| buf.get(start..end))
                .ok_or_else(|| {
                    error!(
                        "Corrupt FrameLoc for in-RAM {segid:?}: {byte_len} bytes at {byte_offset} \
                         exceed the {}-byte buffer",
                        buf.len()
                    );
                    FsError::IoError
                })
        }
        {
            let open = self.open.lock().unwrap();
            if segid == open.segid {
                let frames = crate::segment::read_frames_from_region(
                    &self.codec,
                    region(&open.buf, segid, byte_offset, byte_len)?,
                    segid,
                    first_frame,
                    slots,
                )
                .map_err(|_| FsError::IoError)?;
                return Ok(Some(frames.into_iter().map(Bytes::from).collect()));
            }
        }
        {
            let sealing = self.sealing.lock().unwrap();
            if let Some(bytes) = sealing.get(&segid) {
                let frames = crate::segment::read_frames_from_region(
                    &self.codec,
                    region(bytes.as_ref(), segid, byte_offset, byte_len)?,
                    segid,
                    first_frame,
                    slots,
                )
                .map_err(|_| FsError::IoError)?;
                return Ok(Some(frames.into_iter().map(Bytes::from).collect()));
            }
        }
        Ok(None)
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId, extent_idx: u64) {
        let key = self.key_codec.extent_key(id, extent_idx);
        txn.delete_bytes(&key);
    }

    /// Stage live/total byte deltas for `segid`'s counter onto the txn; the
    /// commit worker folds them into the absolute `(live, total)`. A debit
    /// passes `total_delta == 0` (`total` is monotonic).
    fn seg_delta(&self, txn: &mut Transaction, segid: Segid, live_delta: i64, total_delta: i64) {
        txn.add_seg_delta(
            &self.key_codec.segcount_key(segid.epoch, segid.counter),
            live_delta,
            total_delta,
        );
    }

    pub async fn delete_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        start: u64,
        end: u64,
    ) -> Result<(), FsError> {
        self.tail_invalidate(id);
        if start >= end {
            return Ok(());
        }
        // Debit each removed extent's bytes from its segment's live-byte counter.
        // One forward-map scan (cheaper than a GET per extent); the caller's inode
        // write lock serialises this read-then-delete against a concurrent write to
        // the same extent, so no segment is debited twice for one frame.
        let start_key = self.key_codec.extent_key(id, start);
        let end_key = self.key_codec.extent_key(id, end);
        let mut stream = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if self.key_codec.parse_extent_key(&key).is_some()
                && let Some(loc) = FrameLoc::decode(&value)
            {
                // Delete debit: live only, total untouched (monotonic).
                self.seg_delta(txn, loc.segid, -(loc.byte_len as i64), 0);
            }
        }
        for extent_idx in start..end {
            self.delete(txn, id, extent_idx);
        }
        Ok(())
    }

    /// Append the non-zero extents of `edits` to the open-segment buffer (no PUT)
    /// and stage their extent pointers; stage extent-key deletes for the holes.
    /// Kicks off a background seal when the buffer crosses [`SEAL_THRESHOLD`].
    async fn stage_edits(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        edits: &[(u64, Option<Bytes>)],
    ) -> Result<(), FsError> {
        // Prior FrameLoc per edited extent: an overwrite/hole debits the old
        // segment's live counter (absent = pure append). The caller's inode
        // write lock serialises this against a concurrent write.
        let mut old_debits: Vec<(Segid, u32)> = Vec::with_capacity(edits.len());
        for (extent, _) in edits {
            let key = self.key_codec.extent_key(id, *extent);
            if let Some(enc) = self
                .db
                .get_bytes(&key)
                .await
                .map_err(|_| FsError::IoError)?
                && let Some(loc) = FrameLoc::decode(&enc)
            {
                old_debits.push((loc.segid, loc.byte_len));
            }
        }
        // Compress before taking the open-segment lock: compression is the
        // expensive half of the codec and depends only on the plaintext, while
        // the AEAD binds (segid, frame_index), assigned under the lock. Large
        // batches fan out on rayon; block_in_place needs the multi-thread
        // runtime (tests run current-thread), and small batches stay inline.
        let payloads: Vec<&Bytes> = edits.iter().filter_map(|(_, e)| e.as_ref()).collect();
        let compressed: Vec<Vec<u8>> = if payloads.len() >= PARALLEL_COMPRESS_MIN_FRAMES
            && tokio::runtime::Handle::current().runtime_flavor()
                == tokio::runtime::RuntimeFlavor::MultiThread
        {
            tokio::task::block_in_place(|| {
                use rayon::prelude::*;
                payloads
                    .par_iter()
                    .map(|p| self.codec.compress(p))
                    .collect::<Result<_, _>>()
            })
            .map_err(|_| FsError::IoError)?
        } else {
            payloads
                .iter()
                .map(|p| self.codec.compress(p))
                .collect::<Result<_, _>>()
                .map_err(|_| FsError::IoError)?
        };
        let mut compressed = compressed.into_iter();
        {
            let mut open = self.open.lock().unwrap();
            for (extent, edit) in edits {
                match edit {
                    Some(_) => {
                        let frame_index = open.dir.len() as u32;
                        let segid = open.segid;
                        let sealed = crate::segment::seal_compressed_frame(
                            &self.codec,
                            segid,
                            frame_index,
                            id,
                            *extent,
                            compressed.next().expect("one compressed payload per edit"),
                        )
                        .map_err(|_| FsError::IoError)?;
                        let byte_offset = open.buf.len() as u64;
                        let sealed_len = sealed.len() as u32;
                        open.buf.extend_from_slice(&sealed_len.to_le_bytes());
                        open.buf.extend_from_slice(&sealed);
                        open.dir.push(DirEntry {
                            byte_offset,
                            len: sealed_len,
                            inode: id,
                            extent: *extent,
                        });
                        let loc = FrameLoc {
                            segid,
                            frame_index,
                            byte_offset,
                            byte_len: 4 + sealed_len,
                        };
                        txn.put_bytes(
                            &self.key_codec.extent_key(id, *extent),
                            Bytes::copy_from_slice(&loc.encode()),
                        );
                        // Credit the frame just appended: both live and total.
                        self.seg_delta(txn, segid, loc.byte_len as i64, loc.byte_len as i64);
                    }
                    None => self.delete(txn, id, *extent),
                }
            }
        }
        for (segid, byte_len) in old_debits {
            // Overwrite debit of the superseded frame: live only, total untouched.
            self.seg_delta(txn, segid, -(byte_len as i64), 0);
        }
        let over_threshold = self.open.lock().unwrap().buf.len() >= self.seal_threshold;
        if over_threshold {
            self.spawn_seal().await;
        }
        Ok(())
    }

    /// The durability barrier (called by the flush path before the manifest is
    /// flushed): wait for every in-flight background seal, re-PUT any that failed,
    /// then synchronously seal the current open buffer. After this returns, every
    /// segment referenced by a committed extent is durable on the object store.
    pub async fn seal_open(&self) -> Result<(), FsError> {
        // Acquire all permits: waits for in-flight background seals to finish, and
        // holds new ones off until we release at end of scope.
        let _all = self
            .seal_sem
            .acquire_many(MAX_INFLIGHT_SEALS as u32)
            .await
            .map_err(|_| FsError::IoError)?;

        // Re-PUT any seal whose background attempt failed (still in `sealing`).
        let pending: Vec<(Segid, Bytes)> = {
            let s = self.sealing.lock().unwrap();
            s.iter().map(|(seg, b)| (*seg, b.clone())).collect()
        };
        for (segid, bytes) in pending {
            self.segments
                .put_segment(segid, bytes)
                .await
                .map_err(|_| FsError::IoError)?;
            self.sealing.lock().unwrap().remove(&segid);
        }

        // Synchronously seal the current open buffer. Register it in `sealing`
        // under the open lock and remove it only on success, so the rotated
        // segment is never absent from both maps: a concurrent read would 404
        // the not-yet-PUT object, and a failed PUT would strand it. Mirrors
        // spawn_seal.
        let current = {
            let mut open = self.open.lock().unwrap();
            if open.dir.is_empty() {
                None
            } else {
                let segid = open.segid;
                #[cfg(feature = "failpoints")]
                fail_point!(fp::SEAL_OPEN_FAIL, |_| Err(FsError::IoError));
                // Seal the directory first: on error the open buffer and its
                // committed FrameLocs stay intact for retry, instead of being
                // dropped into a dangling pointer.
                let sealed_dir = crate::segment::seal_directory(&self.codec, segid, &open.dir)
                    .map_err(|_| FsError::IoError)?;
                let k = open.dir.len() as u32;
                let buf = std::mem::take(&mut open.buf);
                open.dir.clear();
                open.segid = self.segments.next_segid();
                debug_assert_ne!(
                    open.segid, segid,
                    "rotated open segid must differ from the sealed one"
                );
                let bytes = Bytes::from(crate::segment::assemble_segment(
                    segid,
                    buf,
                    k,
                    &sealed_dir,
                    segid.counter,
                ));
                self.sealing.lock().unwrap().insert(segid, bytes.clone());
                Some((segid, bytes))
            }
        };
        if let Some((segid, bytes)) = current {
            self.segments
                .put_segment(segid, bytes)
                .await
                .map_err(|_| FsError::IoError)?;
            self.sealing.lock().unwrap().remove(&segid);
        }
        Ok(())
    }

    /// Rotate the open buffer and PUT it in the background (the size-threshold
    /// path). Acquires a permit first, so a writer that outruns the object store
    /// blocks here (backpressure) instead of growing RAM without bound. The
    /// rotated buffer stays readable via `sealing` until its PUT lands.
    async fn spawn_seal(&self) {
        let permit = match Arc::clone(&self.seal_sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let prepared = {
            let mut open = self.open.lock().unwrap();
            if open.dir.is_empty() {
                return;
            }
            let segid = open.segid;
            // Directory first, as in seal_open: an error must leave the open
            // buffer intact for the next seal/flush to retry.
            let sealed_dir = match crate::segment::seal_directory(&self.codec, segid, &open.dir) {
                Ok(s) => s,
                Err(e) => {
                    error!("failed to seal open segment directory {:?}: {}", segid, e);
                    return;
                }
            };
            let k = open.dir.len() as u32;
            let buf = std::mem::take(&mut open.buf);
            open.dir.clear();
            open.segid = self.segments.next_segid();
            debug_assert_ne!(
                open.segid, segid,
                "rotated open segid must differ from the sealed one"
            );
            let bytes = Bytes::from(crate::segment::assemble_segment(
                segid,
                buf,
                k,
                &sealed_dir,
                segid.counter,
            ));
            // Insert into `sealing` while still holding `open`, so the segid is
            // never absent from both maps (a concurrent read would miss it).
            self.sealing.lock().unwrap().insert(segid, bytes.clone());
            Some((segid, bytes))
        };
        let Some((segid, bytes)) = prepared else {
            return;
        };
        let segments = self.segments.clone();
        let sealing = self.sealing.clone();
        crate::task::spawn_named("segment-seal", async move {
            match segments.put_segment(segid, bytes).await {
                Ok(()) => {
                    sealing.lock().unwrap().remove(&segid);
                }
                Err(e) => {
                    error!(
                        "background seal PUT failed for {:?}: {}; retried on flush",
                        segid, e
                    );
                }
            }
            drop(permit);
        });
    }

    /// Read `[offset, offset+length)`, then kick off the bounded,
    /// sequential-only logical read-ahead so the next read lands warm in the
    /// parts cache.
    pub async fn read(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let data = self.read_range(id, offset, length).await?;
        self.trigger_read_ahead(id, offset, length);
        Ok(data)
    }

    /// Sequential-read detection + a bounded forward prefetch. Best-effort: the
    /// per-inode state is racy under concurrent readers of one file, which only
    /// costs a slightly-off prefetch.
    fn trigger_read_ahead(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if length == 0 {
            return None;
        }
        let prev = self.read_ahead.get(&id).map(|e| *e).unwrap_or((0, 0, 0));
        let (state, plan) = plan_read_ahead(prev, offset, length);
        let Some((start, end)) = plan else {
            self.read_ahead.insert(id, state);
            return None;
        };
        // Skip when at the concurrency cap rather than queueing (read-ahead is
        // best-effort). Coverage is committed only once the fetch is spawned:
        // on a skip, `prefetched_to` stays at the true high-water mark
        // (`start`), so the next read re-plans and re-tries the permit.
        let Ok(permit) = Arc::clone(&self.prefetch_sem).try_acquire_owned() else {
            let (read_end, _, seq) = state;
            self.read_ahead.insert(id, (read_end, start, seq));
            return None;
        };
        self.read_ahead.insert(id, state);
        let this = self.clone();
        let read_end = offset + length;
        Some(crate::task::spawn_named("read-ahead", async move {
            let _permit = permit;
            // Cross-segment only: within one segment the per-object prefetcher
            // already reads ahead, and a second interleaved stream would
            // fragment its window ramp into tiny GETs. Prefetch only when the
            // window reaches a segment it can't follow into.
            let cur_ext = read_end.saturating_sub(1) / EXTENT_SIZE as u64;
            let tgt_ext = end.saturating_sub(1) / EXTENT_SIZE as u64;
            let cur_seg = this.segment_at(id, cur_ext).await;
            if cur_seg.is_some() && cur_seg == this.segment_at(id, tgt_ext).await {
                return;
            }
            let _ = this.read_range(id, start, end - start).await;
        }))
    }

    /// The segment an extent's current frame lives in, or `None` for a hole.
    async fn segment_at(&self, id: InodeId, extent: u64) -> Option<Segid> {
        let key = self.key_codec.extent_key(id, extent);
        self.db
            .get_bytes(&key)
            .await
            .ok()
            .flatten()
            .and_then(|b| FrameLoc::decode(&b))
            .map(|loc| loc.segid)
    }

    /// A byte-range read with no read-ahead side effect (the raw path; also what
    /// the read-ahead task calls, so it never recurses).
    async fn read_range(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + length;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end - 1) / EXTENT_SIZE as u64;
        let start_offset = (offset % EXTENT_SIZE as u64) as usize;

        if start_extent == end_extent {
            let extent_end = start_offset + length as usize;
            return Ok(match self.get(id, start_extent).await? {
                Some(data) => data.slice(start_offset..extent_end),
                None => Bytes::copy_from_slice(&ZERO_EXTENT[start_offset..extent_end]),
            });
        }

        let start_key = self.key_codec.extent_key(id, start_extent);
        let end_key = self.key_codec.extent_key(id, end_extent + 1);
        let mut loc_map: HashMap<u64, FrameLoc> = HashMap::new();
        let mut stream = self.db.scan(start_key..end_key).await.map_err(|e| {
            error!("Failed to scan extents (inode={}): {}", id, e);
            FsError::IoError
        })?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|e| {
                error!("Failed to read extent during scan (inode={}): {}", id, e);
                FsError::IoError
            })?;
            if let Some(extent_idx) = self.key_codec.parse_extent_key(&key) {
                // A present-but-undecodable value is the same corrupt-value
                // EIO as the single-extent path (`get`) — skipping it would
                // serve the extent as a fabricated hole of zeros.
                let loc = FrameLoc::decode(&value).ok_or_else(|| {
                    error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
                    FsError::IoError
                })?;
                loc_map.insert(extent_idx, loc);
            }
        }

        // Assemble, coalescing each maximal run of extents that are contiguous in
        // one segment (consecutive frame index + adjacent byte range) into a
        // single ranged GET. This is the read-amplification win: a region written
        // together reads back in one GET instead of one-per-extent.
        let mut result = BytesMut::with_capacity(length as usize);
        let slice = |c: u64| -> (usize, usize) {
            let cs = if c == start_extent { start_offset } else { 0 };
            let ce = if c == end_extent {
                ((end - 1) % EXTENT_SIZE as u64 + 1) as usize
            } else {
                EXTENT_SIZE
            };
            (cs, ce)
        };
        let mut extent = start_extent;
        while extent <= end_extent {
            let Some(first) = loc_map.get(&extent).copied() else {
                // Hole: this extent contributes zeros.
                let (cs, ce) = slice(extent);
                result.extend_from_slice(&ZERO_EXTENT[cs..ce]);
                extent += 1;
                continue;
            };
            let mut n = 1u64;
            let mut total_len = first.byte_len as u64;
            let mut prev = first;
            let mut slots = vec![(id, extent)];
            while extent + n <= end_extent {
                match loc_map.get(&(extent + n)).copied() {
                    Some(loc)
                        if loc.segid == prev.segid
                            && loc.frame_index == prev.frame_index + 1
                            && loc.byte_offset == prev.byte_offset + prev.byte_len as u64 =>
                    {
                        total_len += loc.byte_len as u64;
                        prev = loc;
                        slots.push((id, extent + n));
                        n += 1;
                    }
                    _ => break,
                }
            }
            // Serve the run from RAM (open or in-flight sealing buffer); else GET.
            let frames = match self.read_frames_in_ram(
                first.segid,
                first.byte_offset,
                total_len as u32,
                first.frame_index,
                &slots,
            )? {
                Some(f) => Some(f),
                // A GET error is swallowed: a compaction repoint+delete can 404
                // this run's segment out from under us. Fall back to per-extent
                // reads via `get`, which re-resolves each FrameLoc.
                None => self
                    .segments
                    .read_run(
                        first.segid,
                        first.byte_offset,
                        total_len as u32,
                        first.frame_index,
                        &slots,
                    )
                    .await
                    .ok(),
            };
            match frames {
                Some(frames) => {
                    for (i, frame) in frames.iter().enumerate() {
                        let idx = extent + i as u64;
                        Self::validate_extent_frame(id, idx, frame)?;
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&frame[cs..ce]);
                    }
                }
                None => {
                    for (i, (fid, fext)) in slots.iter().enumerate() {
                        let idx = extent + i as u64;
                        let data = match self.get(*fid, *fext).await? {
                            Some(d) => d,
                            None => Bytes::from_static(ZERO_EXTENT),
                        };
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&data[cs..ce]);
                    }
                }
            }
            extent += n;
        }
        Ok(result.freeze())
    }

    pub async fn write(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        old_size: u64,
    ) -> Result<TailUpdate, FsError> {
        if data.is_empty() {
            return Ok(TailUpdate::Keep);
        }
        let end_offset = offset + data.len() as u64;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        let cached = self.tail_get(id);

        // Read the existing content of any partially-overwritten extent (full
        // overwrites and extents past EOF need no read).
        let existing_extents: HashMap<u64, Bytes> = stream::iter(start_extent..=end_extent)
            .map(|extent_idx| {
                let extent_start = extent_idx * EXTENT_SIZE as u64;
                let extent_end = extent_start + EXTENT_SIZE as u64;
                let will_overwrite_fully = offset <= extent_start && end_offset >= extent_end;
                let beyond_eof = extent_start >= old_size;
                let store = self.clone();
                let cached = cached.clone();
                async move {
                    let data = if will_overwrite_fully || beyond_eof {
                        Bytes::from_static(ZERO_EXTENT)
                    } else if let Some((_, bytes)) = cached.filter(|(ci, _)| *ci == extent_idx) {
                        bytes
                    } else {
                        store
                            .get(id, extent_idx)
                            .await?
                            .unwrap_or_else(|| Bytes::from_static(ZERO_EXTENT))
                    };
                    Ok::<(u64, Bytes), FsError>((extent_idx, data))
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect()
            .await?;

        let cache_tail = end_offset >= old_size && !end_offset.is_multiple_of(EXTENT_SIZE as u64);

        let mut data_offset = 0usize;
        let mut edits: Vec<(u64, Option<Bytes>)> =
            Vec::with_capacity((end_extent - start_extent + 1) as usize);
        let mut tail: Option<Bytes> = None;
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            let write_start = if offset > extent_start {
                (offset - extent_start) as usize
            } else {
                0
            };
            let write_end = if end_offset < extent_end {
                (end_offset - extent_start) as usize
            } else {
                EXTENT_SIZE
            };
            let write_len = write_end - write_start;
            let extent: Bytes = if write_start == 0 && write_end == EXTENT_SIZE {
                data.slice(data_offset..data_offset + write_len)
            } else {
                let mut buf = BytesMut::from(existing_extents[&extent_idx].as_ref());
                buf[write_start..write_end]
                    .copy_from_slice(&data[data_offset..data_offset + write_len]);
                buf.freeze()
            };
            data_offset += write_len;

            if extent.as_ref() == ZERO_EXTENT {
                edits.push((extent_idx, None));
            } else {
                if extent_idx == end_extent && cache_tail {
                    tail = Some(extent.clone());
                }
                edits.push((extent_idx, Some(extent)));
            }
        }

        self.stage_edits(txn, id, &edits).await?;

        Ok(match tail {
            Some(data) => TailUpdate::Set {
                extent_idx: end_extent,
                data,
            },
            None => TailUpdate::Clear,
        })
    }

    pub async fn truncate(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Result<(), FsError> {
        if new_size >= old_size {
            return Ok(());
        }

        let old_extents = old_size.div_ceil(EXTENT_SIZE as u64);
        let new_extents = new_size.div_ceil(EXTENT_SIZE as u64);
        self.delete_range(txn, id, new_extents, old_extents).await?;

        if new_size > 0 {
            let last_extent_idx = new_extents - 1;
            let clear_from = (new_size % EXTENT_SIZE as u64) as usize;
            if clear_from > 0 {
                let existing = self.get(id, last_extent_idx).await?;
                let mut extent =
                    BytesMut::from(existing.as_ref().map(|b| b.as_ref()).unwrap_or(ZERO_EXTENT));
                extent[clear_from..].fill(0);
                let edit = if extent.as_ref() == ZERO_EXTENT {
                    (last_extent_idx, None)
                } else {
                    (last_extent_idx, Some(extent.freeze()))
                };
                self.stage_edits(txn, id, &[edit]).await?;
            }
        }
        Ok(())
    }

    pub async fn zero_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> Result<(), FsError> {
        if length == 0 {
            return Ok(());
        }
        self.tail_invalidate(id);

        let end_offset = offset + length;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        let mut edits: Vec<(u64, Option<Bytes>)> = Vec::new();
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            if extent_start >= file_size {
                continue;
            }
            if offset <= extent_start && end_offset >= extent_end {
                edits.push((extent_idx, None));
            } else if let Some(existing_data) = self.get(id, extent_idx).await? {
                let zero_start = if offset > extent_start {
                    (offset - extent_start) as usize
                } else {
                    0
                };
                let zero_end = if end_offset < extent_end {
                    (end_offset - extent_start) as usize
                } else {
                    EXTENT_SIZE
                };
                let mut extent_data = BytesMut::from(existing_data.as_ref());
                extent_data[zero_start..zero_end].fill(0);
                if extent_data.as_ref() == ZERO_EXTENT {
                    edits.push((extent_idx, None));
                } else {
                    edits.push((extent_idx, Some(extent_data.freeze())));
                }
            }
        }
        self.stage_edits(txn, id, &edits).await
    }

    /// Delete an extent range under the inode's write lock, in its own transaction.
    /// The tombstone GC calls this so its deletes serialize with the compaction
    /// repoint, which takes the same lock: otherwise a repoint that read an extent
    /// live, then lost the race to a concurrent tombstone delete, would re-commit
    /// the extent last-writer-wins (the LSM has no CAS), resurrecting a deleted
    /// inode's extent and pinning the repacked segment forever.
    pub async fn delete_extents(
        &self,
        inode: InodeId,
        start_extent: u64,
        total_extents: u64,
    ) -> Result<(), FsError> {
        let _guard = self.lock_manager.acquire(inode).await;
        let mut txn = self.db.new_transaction()?;
        self.delete_range(&mut txn, inode, start_extent, total_extents)
            .await?;
        self.commit_via_coordinator(txn).await?;
        Ok(())
    }

    /// List the PUT segment ids. Exposed for the failpoints crash tests, which drive
    /// `compact_segments` directly (the reclaim floor won't compact tiny test data).
    #[cfg(feature = "failpoints")]
    #[allow(dead_code)] // used by the failpoints integration test, not the lib/bin
    pub async fn list_segments(&self) -> Result<Vec<Segid>, FsError> {
        self.segments
            .list_segments()
            .await
            .map_err(|_| FsError::IoError)
    }

    /// Reclaim with an immediate horizon (so a dead segment is deleted this pass).
    /// For the failpoints crash tests, which have no `chrono` in scope.
    #[cfg(feature = "failpoints")]
    #[allow(dead_code)] // used by the failpoints integration test, not the lib/bin
    pub async fn reclaim_now(&self) -> Result<(usize, usize), FsError> {
        self.reclaim_segments(chrono::Utc::now(), None).await
    }

    /// Reclaim object-store space: delete fully-dead segments and compact the
    /// most-fragmented ones, driven entirely off the local `segcount` scan.
    /// Returns (segments deleted, frames relocated).
    #[allow(dead_code)] // failpoints + tests
    pub async fn reclaim_segments(
        &self,
        delete_horizon: DateTime<Utc>,
        // Set when a persistent checkpoint pins a view; see the `pinned` gate
        // in reclaim_segments_gated.
        protect_before: Option<DateTime<Utc>>,
    ) -> Result<(usize, usize), FsError> {
        self.reclaim_segments_gated(move || {
            std::future::ready(Ok(Some((delete_horizon, protect_before))))
        })
        .await
    }

    /// As [`Self::reclaim_segments`], but the horizon gate is computed by `gate`
    /// after the durable barrier (seal + flush). That ordering closes a race: a
    /// concurrently-created checkpoint is either visible to `gate` (extending
    /// the horizon) or pins the already-flushed manifest, in which everything
    /// this pass reclaims is unreferenced. Listed before the barrier, it could
    /// pin the pre-flush manifest and a to-be-dead segment with it. `gate`
    /// returning `Ok(None)` skips the pass; `Err` aborts.
    pub async fn reclaim_segments_gated<F, Fut>(&self, gate: F) -> Result<(usize, usize), FsError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<
                Output = Result<Option<(DateTime<Utc>, Option<DateTime<Utc>>)>, FsError>,
            >,
    {
        // The seal makes the flushed manifest reference only PUT segments; the
        // flush makes "dead in the in-memory view" mean "dead in the durable view".
        // Under the flush barrier (write side), same as the flush coordinator, so a
        // concurrent commit can't durably reference the just-rotated open buffer.
        {
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
        }
        tracing::debug!("segment GC: durable barrier done (sealed + flushed), scanning extents");

        // Post-barrier on purpose; see the doc comment for the race this closes.
        let (delete_horizon, protect_before) = match gate().await? {
            Some(v) => v,
            None => return Ok((0, 0)),
        };

        let cur_epoch = self.segments.epoch();
        // Exclusive counter cutoff for eligibility: the still-open segment's
        // own counter, not `next_counter()`. seal_open() just rotated in a
        // fresh open segment that is still accepting frames whose credits may
        // not be visible to the scan below; `next_counter()` would include it,
        // and a mid-pass background seal could then mis-classify that fully
        // live segment as dead.
        let cutoff = self.open.lock().unwrap().segid.counter;

        // A persistent checkpoint pins a view this scan-driven path can't
        // bound by object mtime (it never LISTs). Skip classification entirely
        // while one exists; the scan still runs for the footprint log.
        let pinned = protect_before.is_some();

        // A segment is reclaimable only once it can no longer gain references: it was
        // sealed before this round (older epoch, or below the counter cutoff).
        let eligible =
            |s: &Segid| s.epoch < cur_epoch || (s.epoch == cur_epoch && s.counter < cutoff);

        // The commit worker keeps each counter's `(live, total)` current, so
        // `live == 0` is the dead test and `live/total` the fragmentation
        // ratio — no listing needed. Absent counters never appear in this scan;
        // the slow orphan sweep owns those. Every delete is still
        // directory-verified below.
        //
        // Stream the scan, keeping only dead segids plus a bounded candidate
        // set, so per-pass RAM is O(dead + cap), not O(#segments).
        let (sc_start, sc_end) = self.key_codec.segcount_prefix_range();
        let mut candidates: Vec<(Segid, u64, u64)> = Vec::new(); // (segid, total, live)
        let mut dead: Vec<(Segid, u64)> = Vec::new(); // (segid, total ~= bytes freed)
        let mut scanned = 0usize;
        let mut total_live = 0u64;
        let mut total_appended = 0u64;
        let mut stream = self.db.scan(sc_start..sc_end).await.map_err(|e| {
            error!("GC segcount scan failed: {}", e);
            FsError::IoError
        })?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            let Some((epoch, counter)) = self.key_codec.parse_segcount_key(&key) else {
                continue;
            };
            let Some((live, total)) = KeyCodec::decode_segcount(&value) else {
                continue;
            };
            let segid = Segid::new(epoch, counter);
            scanned += 1;
            total_live += live;
            total_appended += total;
            if !eligible(&segid) {
                continue;
            }
            if pinned {
                continue;
            }
            if live == 0 {
                dead.push((segid, total));
            } else {
                let fragmented = live.saturating_mul(100) < total.saturating_mul(FRAG_LIVE_PERCENT);
                let small = total < SMALL_SEGMENT_BYTES;
                if fragmented || small {
                    candidates.push((segid, total, live));
                    if candidates.len() >= MAX_CANDIDATES_BUFFERED {
                        // Keep the most-fragmented (lowest live fraction) half.
                        candidates
                            .sort_by_key(|&(_, tot, lb)| lb.saturating_mul(1000) / tot.max(1));
                        candidates.truncate(MAX_CANDIDATES_BUFFERED / 2);
                    }
                }
            }
        }
        drop(stream);
        tracing::debug!(
            "segment GC: segcount scan done, {} segments ({} dead, {} candidates)",
            scanned,
            dead.len(),
            candidates.len()
        );

        // Classify dead segments under the dead-tracking lock (no await held): a
        // fully-dead segment accrues its horizon and is deleted only once past it.
        let now = Utc::now();
        let mut deleted_bytes = 0u64;
        let mut awaiting_bytes = 0u64;
        let to_delete = {
            let mut delete_at = self.delete_at.lock().unwrap();
            let mut to_delete: Vec<(Segid, u64)> = Vec::new();
            let mut still_waiting: HashSet<Segid> = HashSet::new();
            for (segid, size) in dead {
                // Record this pass's horizon the first time the segment is seen dead;
                // keep that deadline on later passes so a fresh checkpoint can't push
                // it out.
                let due = *delete_at.entry(segid).or_insert(delete_horizon);
                if now >= due && to_delete.len() < MAX_SEGMENT_DELETES_PER_PASS {
                    to_delete.push((segid, size));
                    delete_at.remove(&segid);
                } else {
                    // Not yet due, or over this pass's budget. Keep the deadline
                    // either way, so a budget-deferred segment retries at once.
                    still_waiting.insert(segid);
                    awaiting_bytes += size;
                }
            }
            // Forget segments no longer dead/listed, so the map can't grow
            // unbounded.
            delete_at.retain(|s, _| still_waiting.contains(s));
            to_delete
        };

        let mut deleted = 0;
        let mut freed_counters: Vec<Segid> = Vec::new();
        for (segid, size) in to_delete {
            // Fail-closed: a segment is deleted only once its directory confirms no
            // frame is still referenced. Sound because an eligible segment can never
            // regain a reference, so "no frame points here" is a permanent verdict.
            match self.verify_segment_reclaimable(segid).await {
                SegmentDeadVerdict::Keep => {
                    // The counter under-counted (a live frame remains) or the read
                    // was transient — leak beats loss.
                    error!(
                        "segment GC: counter calls {segid:?} dead but a live frame remains; \
                         skipping delete (leak, not loss)"
                    );
                    continue;
                }
                SegmentDeadVerdict::ObjectAbsent => {
                    // A crash between deleting the object and committing the
                    // counter drop left the counter behind; drop it so it stops
                    // re-appearing as dead each pass.
                    freed_counters.push(segid);
                    continue;
                }
                SegmentDeadVerdict::Reclaim => {}
            }
            if let Err(e) = self.segments.delete_segment(segid).await {
                // Don't abort the pass: already-deleted segments still need
                // their counters dropped. This one retries next pass.
                error!("segment GC: delete of {segid:?} failed: {e}; skipping (retried next pass)");
                continue;
            }

            #[cfg(feature = "failpoints")]
            fail_point!(fp::RECLAIM_AFTER_SEGMENT_DELETE);

            // Audit line for the irreversible delete; the object key joins
            // against object-store access logs.
            info!(
                "segment GC: deleted dead segment {:?} at {} (~{})",
                segid,
                segid.object_key(),
                human_bytes(size)
            );
            freed_counters.push(segid);
            deleted_bytes += size;
            deleted += 1;
        }
        // Drop the counters of deleted segments, else one segcount key leaks
        // per segment ever created.
        if !freed_counters.is_empty() {
            let mut txn = self.db.new_transaction()?;
            for segid in &freed_counters {
                txn.delete_bytes(&self.key_codec.segcount_key(segid.epoch, segid.counter));
            }
            self.commit_via_coordinator(txn).await?;
        }

        // Compaction: most-fragmented first (lowest live fraction), bounded per
        // round by segment count and bytes.
        candidates.sort_by_key(|&(_, size, live_b)| live_b.saturating_mul(1000) / size.max(1));
        let candidate_backlog = candidates.len();
        let mut selected: Vec<Segid> = Vec::new();
        let mut sel_size: u64 = 0;
        let mut sel_live: u64 = 0;
        for (segid, size, live_b) in candidates {
            if selected.len() >= MAX_COMPACT_SEGMENTS_PER_ROUND
                || sel_live >= MAX_COMPACT_BYTES_PER_ROUND
            {
                break;
            }
            selected.push(segid);
            sel_size += size;
            sel_live += live_b;
        }
        let freed = sel_size.saturating_sub(sel_live);
        let mut compacted_segments = 0;
        let mut packed_segments = 0;
        let mut frames_relocated = 0;
        // Compact only when it pays: either it reclaims real dead space, or enough
        // small dense segments have accumulated to clear a quarter-segment floor
        // (so the packed output isn't itself "small" and re-compacted next pass).
        // The floor scales with the segment size (seal_threshold), so it stays
        // above SMALL_SEGMENT_BYTES by construction.
        if freed >= MIN_FREED_BYTES || sel_live >= self.seal_threshold as u64 / 4 {
            compacted_segments = selected.len();
            (frames_relocated, packed_segments) = self.compact_segments(&selected).await?;
        }

        // One info line per pass: footprint, reclaimable bytes, and what the
        // pass did. `total_appended` excludes segment framing overhead, so it
        // slightly exceeds on-store bytes.
        let reclaimable = total_appended.saturating_sub(total_live);
        let dead_pct = reclaimable
            .saturating_mul(100)
            .checked_div(total_appended)
            .unwrap_or(0);
        let awaiting = self.delete_at.lock().unwrap().len();
        let action = if deleted > 0
            || compacted_segments > 0
            || candidate_backlog > 0
            || awaiting > 0
        {
            format!(
                "deleted {} dead (~{}), compacted {}→{} of {} candidates ({} frames, ~{} to reclaim), {} awaiting (~{})",
                deleted,
                human_bytes(deleted_bytes),
                compacted_segments,
                packed_segments,
                candidate_backlog,
                frames_relocated,
                human_bytes(freed),
                awaiting,
                human_bytes(awaiting_bytes)
            )
        } else {
            "idle".to_string()
        };
        info!(
            "segment GC: {} segments, {} appended ({} live, ~{} reclaimable, {}% dead); {}",
            scanned,
            human_bytes(total_appended),
            human_bytes(total_live),
            human_bytes(reclaimable),
            dead_pct,
            action
        );
        Ok((deleted, frames_relocated))
    }

    /// Sweep orphan segment objects: present on the store with no `segcount`
    /// key (a crash between seal and the crediting commit, or a compaction
    /// whose repoints all lost the CAS). A frame's counter credit commits
    /// atomically with its FrameLoc, so an absent counter means no extent
    /// references the object. This is the only path that LISTs the object
    /// store — hence the slow cadence; the fast reclaim never sees
    /// counter-less objects.
    ///
    /// Candidates pass the same `eligible()` gate as the fast reclaim, an age
    /// gate — only objects `last_modified` before `modified_before` are
    /// deletable, so a PUT whose crediting commit is still in flight is never
    /// swept — and the directory verify. The age gate is stateless (the
    /// object's mtime, no process-local first-seen map), so a restarted
    /// process reclaims old orphans on its first sweep. The caller runs it
    /// after the fast reclaim, never concurrently, so an in-flight compaction
    /// can't leave a not-yet-credited packed segment eligible. Returns the
    /// number of orphans reclaimed.
    pub async fn sweep_orphans(&self, modified_before: DateTime<Utc>) -> Result<usize, FsError> {
        // Durable barrier, as in the fast reclaim: seal the open buffer and flush so
        // the eligibility cutoff and the directory verify observe the durable view.
        {
            let _barrier = self.db.flush_barrier().write_owned().await;
            self.seal_open().await?;
            self.db.flush().await.map_err(|_| FsError::IoError)?;
        }

        let cur_epoch = self.segments.epoch();
        // Same eligibility cutoff as reclaim_segments_gated. A freshly-packed
        // compaction segment always carries a higher counter, so it is never
        // eligible even mid-compaction.
        let cutoff = self.open.lock().unwrap().segid.counter;
        let eligible =
            |s: &Segid| s.epoch < cur_epoch || (s.epoch == cur_epoch && s.counter < cutoff);

        // The one and only list("segments"). Point-get each object's counter
        // as the listing streams: present => the fast loop owns it; absent =>
        // orphan candidate. Cap the buffered set; the rest sweep next cadence.
        let mut orphans: Vec<Segid> = Vec::new();
        let mut scanned = 0usize;
        let stream = self.segments.list_segments_stream();
        futures::pin_mut!(stream);
        while let Some(result) = futures::StreamExt::next(&mut stream).await {
            let (segid, _size, mtime) = result.map_err(|_| FsError::IoError)?;
            scanned += 1;
            if !eligible(&segid) {
                continue;
            }
            // Age gate: an uncounted object younger than the cutoff may be a
            // PUT whose crediting commit is still in flight; it sweeps next
            // cadence once safely old.
            if mtime >= modified_before {
                continue;
            }
            let key = self.key_codec.segcount_key(segid.epoch, segid.counter);
            if self
                .db
                .get_bytes(&key)
                .await
                .map_err(|_| FsError::IoError)?
                .is_some()
            {
                continue;
            }
            orphans.push(segid);
            if orphans.len() >= MAX_SEGMENT_DELETES_PER_PASS {
                break;
            }
        }

        let mut deleted = 0;
        for segid in orphans {
            // Fail-closed, as in the fast reclaim: confirm via the directory
            // before deleting.
            match self.verify_segment_reclaimable(segid).await {
                SegmentDeadVerdict::Reclaim => {}
                // Deleted concurrently; an orphan has no counter to drop.
                SegmentDeadVerdict::ObjectAbsent => continue,
                SegmentDeadVerdict::Keep => {
                    error!(
                        "orphan sweep: {segid:?} has no counter but a live frame remains; \
                         skipping delete (leak, not loss)"
                    );
                    continue;
                }
            }
            if let Err(e) = self.segments.delete_segment(segid).await {
                error!(
                    "orphan sweep: delete of {segid:?} failed: {e}; skipping (retried next sweep)"
                );
                continue;
            }
            info!(
                "orphan sweep: deleted orphan segment {:?} at {}",
                segid,
                segid.object_key()
            );
            deleted += 1;
        }
        info!("orphan sweep: scanned {scanned} segment objects, reclaimed {deleted} orphan(s)");
        Ok(deleted)
    }

    /// Run the slow orphan sweep at most once per `interval`, gated by a
    /// persisted wall-clock timestamp so the cadence survives restarts (an
    /// uptime timer would never fire on a frequently-restarting deployment).
    /// Returns the orphans reclaimed, or `None` when not yet due.
    pub async fn sweep_orphans_if_due(
        &self,
        interval: chrono::Duration,
    ) -> Result<Option<usize>, FsError> {
        let now = Utc::now();
        let last = self
            .db
            .get_bytes(&self.key_codec.last_orphan_sweep_key())
            .await
            .map_err(|_| FsError::IoError)?
            .and_then(|b| KeyCodec::decode_u64(&b))
            .and_then(|secs| DateTime::from_timestamp(secs as i64, 0));
        if let Some(last) = last
            && now < last + interval
        {
            return Ok(None);
        }
        // No manifest or checkpoint ever references an orphan, so a generous
        // in-flight age floor is the only gate needed.
        let deleted = self
            .sweep_orphans(now - chrono::Duration::seconds(ORPHAN_MIN_AGE_SECS))
            .await?;
        // Persist the timestamp after the sweep, so a crash mid-sweep re-runs
        // it sooner rather than skipping a cadence.
        let mut txn = self.db.new_transaction()?;
        txn.put_bytes(
            &self.key_codec.last_orphan_sweep_key(),
            KeyCodec::encode_u64(now.timestamp() as u64),
        );
        self.db
            .write_with_options(
                txn.into_inner(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;
        Ok(Some(deleted))
    }

    /// Confirm a segment the counter calls dead truly holds no live-referenced
    /// frame, by reading its directory and checking each named extent against the
    /// forward map. Fail-closed: a read error, or any frame the forward map still
    /// points here, returns [`SegmentDeadVerdict::Keep`]. Sound because an eligible
    /// segment can never regain a reference, so the verdict is permanent.
    async fn verify_segment_reclaimable(&self, segid: Segid) -> SegmentDeadVerdict {
        let dir = match self.segments.read_directory(segid).await {
            Ok(d) => d,
            Err(SegmentStoreError::NotFound) => return SegmentDeadVerdict::ObjectAbsent,
            // Transient read error: fail-closed, keep the segment.
            Err(_) => return SegmentDeadVerdict::Keep,
        };
        // Unique extents the directory names (an extent can recur across rewrites).
        let want: HashSet<(InodeId, u64)> = dir.iter().map(|e| (e.inode, e.extent)).collect();
        // Any extent whose current FrameLoc still points here means the segment is
        // live; a point-read error is treated as still-referenced (fail-closed).
        let still_referenced = stream::iter(want)
            .map(|(inode, extent)| {
                let store = self.clone();
                async move {
                    let key = store.key_codec.extent_key(inode, extent);
                    match store.db.get_bytes(&key).await {
                        Ok(Some(enc)) => {
                            FrameLoc::decode(&enc).is_some_and(|loc| loc.segid == segid)
                        }
                        Ok(None) => false,
                        Err(_) => true,
                    }
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .any(|referenced| async move { referenced })
            .await;
        if still_referenced {
            SegmentDeadVerdict::Keep
        } else {
            SegmentDeadVerdict::Reclaim
        }
    }

    /// Compact a set of fragmented/small segments: gather their still-live frames
    /// and repack them into fresh ~[`PACK_TARGET_BYTES`] segments, repointing each
    /// relocated extent. The drained sources become fully dead and are deleted by
    /// a later pass once past their horizon, so in-flight reads of the old
    /// locations stay valid. Returns (frames relocated, packed segments created).
    ///
    /// The repoint is conditional and per-inode-locked: it holds the same write
    /// lock the foreground path uses and moves an extent only if it still points
    /// at the source frame, so a concurrent overwrite is never clobbered. The new
    /// segment is durably PUT (by `seal`) before any extent references it, so a
    /// crash between seal and repoint just leaves the source in place.
    pub async fn compact_segments(&self, segids: &[Segid]) -> Result<(usize, usize), FsError> {
        // Gather still-live frames across all sources, tagged with their source
        // segid, bounded by the per-round byte budget.
        let mut frames: Vec<(InodeId, u64, Segid, Bytes)> = Vec::new();
        let mut gathered: u64 = 0;
        // Gather each live (inode, extent) at most once: an extent rewritten K times
        // within one seal window leaves K directory entries resolving to the same
        // current frame, and an extent can appear in more than one source directory.
        let mut seen: HashSet<(InodeId, u64)> = HashSet::new();
        for &segid in segids {
            if gathered >= MAX_COMPACT_BYTES_PER_ROUND {
                break;
            }
            let dir = self
                .segments
                .read_directory(segid)
                .await
                .map_err(|_| FsError::IoError)?;

            // The unique, not-yet-gathered extents this source directory names.
            let mut want: Vec<(InodeId, u64)> = Vec::new();
            let mut local: HashSet<(InodeId, u64)> = HashSet::new();
            for e in &dir {
                let k = (e.inode, e.extent);
                if !seen.contains(&k) && local.insert(k) {
                    want.push(k);
                }
            }

            // Resolve liveness in parallel: an extent is live in this source iff its
            // current FrameLoc still points here (point reads, mostly cache hits). A
            // not-live extent is left un-`seen` so a later source can still claim it.
            let mut live: Vec<(InodeId, u64, FrameLoc)> = stream::iter(want)
                .map(|(inode, extent)| {
                    let store = self.clone();
                    async move {
                        let key = store.key_codec.extent_key(inode, extent);
                        let enc = store
                            .db
                            .get_bytes(&key)
                            .await
                            .map_err(|_| FsError::IoError)?;
                        Ok::<_, FsError>(
                            enc.and_then(|b| FrameLoc::decode(&b))
                                .filter(|loc| loc.segid == segid)
                                .map(|loc| (inode, extent, loc)),
                        )
                    }
                })
                .buffer_unordered(PARALLEL_EXTENT_OPS)
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .flatten()
                .collect();

            // Coalesce contiguous live frames (consecutive index + adjacent bytes)
            // into runs, then read the runs in parallel — one ranged GET per run
            // instead of one per frame.
            live.sort_by_key(|(_, _, loc)| loc.frame_index);
            let mut runs: Vec<CompactRun> = Vec::new();
            for (inode, extent, loc) in live {
                match runs.last_mut() {
                    Some((byte_offset, total_len, first_frame, slots))
                        if *first_frame + slots.len() as u32 == loc.frame_index
                            && *byte_offset + *total_len as u64 == loc.byte_offset =>
                    {
                        *total_len += loc.byte_len;
                        slots.push((inode, extent));
                    }
                    _ => runs.push((
                        loc.byte_offset,
                        loc.byte_len,
                        loc.frame_index,
                        vec![(inode, extent)],
                    )),
                }
            }
            let read: Vec<Vec<(InodeId, u64, Bytes)>> = stream::iter(runs)
                .map(|(byte_offset, total_len, first_frame, slots)| {
                    let store = self.clone();
                    async move {
                        let bytes = store
                            .segments
                            .read_run(segid, byte_offset, total_len, first_frame, &slots)
                            .await
                            .map_err(|_| FsError::IoError)?;
                        Ok::<_, FsError>(
                            slots
                                .into_iter()
                                .zip(bytes)
                                .map(|((inode, extent), b)| (inode, extent, b))
                                .collect::<Vec<_>>(),
                        )
                    }
                })
                .buffer_unordered(PARALLEL_EXTENT_OPS)
                .try_collect::<Vec<_>>()
                .await?;

            for (inode, extent, bytes) in read.into_iter().flatten() {
                seen.insert((inode, extent));
                gathered += bytes.len() as u64;
                frames.push((inode, extent, segid, bytes));
            }
        }

        info!(
            "compaction: gathered {} live frames ({}) from {} source segment(s)",
            frames.len(),
            human_bytes(gathered),
            segids.len(),
        );

        // Group by (inode, extent) so a file's extents land contiguously in the new
        // segment (consecutive frame index + adjacent bytes), collapsing its reads
        // to a single ranged GET instead of one per scattered run. Sequential inode
        // ids also cluster same-directory files into the same/adjacent segments.
        frames.sort_by_key(|(inode, extent, _, _)| (*inode, *extent));

        // Bin-pack into target-sized segments, sealing + repointing each batch. A
        // batch that repoints nothing (every frame overwritten out from under us)
        // left only an orphan segment, so it doesn't count as packed output.
        let mut relocated = 0;
        let mut packed = 0;
        let mut batch: Vec<(InodeId, u64, Bytes)> = Vec::new();
        let mut batch_src: Vec<Segid> = Vec::new();
        let mut batch_bytes: u64 = 0;
        for (inode, extent, src, bytes) in frames {
            batch_bytes += bytes.len() as u64;
            batch.push((inode, extent, bytes));
            batch_src.push(src);
            if batch_bytes >= PACK_TARGET_BYTES {
                let n = self.seal_and_repoint(&batch, &batch_src).await?;
                relocated += n;
                packed += usize::from(n > 0);
                batch.clear();
                batch_src.clear();
                batch_bytes = 0;
            }
        }
        if !batch.is_empty() {
            let n = self.seal_and_repoint(&batch, &batch_src).await?;
            relocated += n;
            packed += usize::from(n > 0);
        }
        Ok((relocated, packed))
    }

    /// Seal one packed batch into a new segment and repoint the extents that still
    /// reference their source frame. `src[i]` is the source segid of `batch[i]`.
    async fn seal_and_repoint(
        &self,
        batch: &[(InodeId, u64, Bytes)],
        src: &[Segid],
    ) -> Result<usize, FsError> {
        let new_locs = self
            .segments
            .seal(batch)
            .await
            .map_err(|_| FsError::IoError)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT);

        // All frames in one seal go to one new segment.
        let new_segid = new_locs.first().map(|(_, _, loc)| loc.segid);

        // Group by inode so each conditional swap is taken under that inode's write
        // lock (excludes a concurrent foreground write to the same extent).
        let mut by_inode: HashMap<InodeId, Vec<(u64, Segid, FrameLoc)>> = HashMap::new();
        for (i, (inode, extent, new_loc)) in new_locs.into_iter().enumerate() {
            by_inode
                .entry(inode)
                .or_default()
                .push((extent, src[i], new_loc));
        }

        let mut swapped = 0;
        for (inode, items) in by_inode {
            let _guard = self.lock_manager.acquire(inode).await;
            let mut txn = self.db.new_transaction()?;
            let mut any = false;
            for (extent, old_segid, new_loc) in items {
                let key = self.key_codec.extent_key(inode, extent);
                if let Some(enc) = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?
                    && let Some(loc) = FrameLoc::decode(&enc)
                    && loc.segid == old_segid
                {
                    txn.put_bytes(&key, Bytes::copy_from_slice(&new_loc.encode()));
                    // Move the bytes only on a won CAS: after a lost race the
                    // relocated frame is dead weight (no live pointer) and must
                    // not be credited. Source debit is live-only; the packed
                    // frame credits both live and total.
                    self.seg_delta(&mut txn, old_segid, -(loc.byte_len as i64), 0);
                    self.seg_delta(
                        &mut txn,
                        new_loc.segid,
                        new_loc.byte_len as i64,
                        new_loc.byte_len as i64,
                    );
                    swapped += 1;
                    any = true;
                }
            }
            if any {
                self.commit_via_coordinator(txn).await?;
            }
        }

        // Nothing repointed: every gathered frame was overwritten before the
        // CAS, leaving a pure orphan (PUT, no pointer, no counter). Delete it
        // now, best-effort; a crash here leaves it for the orphan sweep.
        let orphaned = swapped == 0;
        let mut orphan_note = "";
        if orphaned && let Some(segid) = new_segid {
            orphan_note = match self.segments.delete_segment(segid).await {
                Ok(()) => " (orphan, deleted)",
                Err(e) => {
                    warn!(
                        "compaction: delete of orphaned {segid:?} failed: {e}; \
                         left for the orphan sweep"
                    );
                    " (orphan, delete failed)"
                }
            };
        }
        info!(
            "compaction: packed {:?}: {} frames, {} repointed, {} discarded to concurrent writes{}",
            new_segid,
            batch.len(),
            swapped,
            batch.len() - swapped,
            orphan_note,
        );
        Ok(swapped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::frame_codec::FrameCodec;
    use crate::segment::SEGMENT_INFO;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{ObjectStore, path::Path};
    use slatedb::{BlockTransformer, DbBuilder};

    async fn make() -> (ExtentStore, Arc<Db>) {
        let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
        (store, db)
    }

    async fn make_with_compression(
        compression: CompressionConfig,
    ) -> (ExtentStore, Arc<Db>, Arc<dyn ObjectStore>) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bt: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
        let slatedb = Arc::new(
            DbBuilder::new(Path::from("t"), object_store.clone())
                .with_block_transformer(bt)
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(slatedb, None));
        let store = make_store(object_store.clone(), db.clone(), compression, 7);
        (store, db, object_store)
    }

    /// A store over given backing state, as a restarted process would build:
    /// fresh in-RAM state, and a higher epoch than any earlier incarnation's.
    fn make_store(
        object_store: Arc<dyn ObjectStore>,
        db: Arc<Db>,
        compression: CompressionConfig,
        epoch: u64,
    ) -> ExtentStore {
        let key_codec = Arc::new(KeyCodec::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, compression);
        let segments = Arc::new(SegmentStore::new(object_store, codec, epoch, None));
        ExtentStore::new(db, key_codec, segments, Arc::new(KeyedLockManager::new()))
            .with_seal_threshold(8 * 1024 * 1024)
    }

    async fn commit(store: &ExtentStore, txn: Transaction) {
        // No commit worker in these tests, so commit_via_coordinator takes its
        // fallback path: this test task is the sole segcount writer.
        store.commit_via_coordinator(txn).await.unwrap();
    }

    /// Apply a write through the store and to a byte-array model, asserting the
    /// full file reads back identically.
    async fn write_and_check(
        store: &ExtentStore,
        db: &Db,
        model: &mut Vec<u8>,
        offset: usize,
        bytes: &[u8],
    ) {
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                1,
                offset as u64,
                &Bytes::copy_from_slice(bytes),
                model.len() as u64,
            )
            .await
            .unwrap();
        commit(store, txn).await;
        store.apply_tail_update(1, tu);
        let end = offset + bytes.len();
        if model.len() < end {
            model.resize(end, 0);
        }
        model[offset..end].copy_from_slice(bytes);
        assert_read_matches(store, model).await;
    }

    async fn assert_read_matches(store: &ExtentStore, model: &[u8]) {
        if !model.is_empty() {
            let got = store.read(1, 0, model.len() as u64).await.unwrap();
            assert_eq!(got.as_ref(), model, "read does not match model");
        }
    }

    async fn frameloc_of(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extent: u64,
    ) -> Option<FrameLoc> {
        let key = store.key_codec.extent_key(inode, extent);
        db.get_bytes(&key)
            .await
            .unwrap()
            .and_then(|b| FrameLoc::decode(&b))
    }

    /// The live component of a segment's `(live, total)` counter.
    async fn segcount_of(store: &ExtentStore, db: &Db, segid: Segid) -> u64 {
        segcount_pair_of(store, db, segid).await.0
    }

    /// The full `(live, total)` counter for a segment.
    async fn segcount_pair_of(store: &ExtentStore, db: &Db, segid: Segid) -> (u64, u64) {
        let key = store.key_codec.segcount_key(segid.epoch, segid.counter);
        db.get_bytes(&key)
            .await
            .unwrap()
            .and_then(|b| KeyCodec::decode_segcount(&b))
            .unwrap_or((0, 0))
    }

    /// The counter's ground truth: sum of live frame bytes an inode still points at
    /// in `segid` over `extents`.
    async fn live_bytes(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extents: std::ops::Range<u64>,
        segid: Segid,
    ) -> u64 {
        let mut sum = 0;
        for e in extents {
            if let Some(l) = frameloc_of(store, db, inode, e).await
                && l.segid == segid
            {
                sum += l.byte_len as u64;
            }
        }
        sum
    }

    #[tokio::test]
    async fn segcount_tracks_live_bytes_across_overwrite_and_delete() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Three full extents land in the open segment; the counter equals their bytes.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Overwrite extent 1: old debited, new credited, the stale frame is dead
        // weight the counter does not count.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                EXTENT_SIZE as u64,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(frameloc_of(&store, &db, inode, 1).await.unwrap().segid, seg);
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Delete extent 2: its bytes leave the counter.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 2, 3).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..2, seg).await,
        );

        // Delete the rest: the counter reaches exactly zero.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 0, 2).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(segcount_of(&store, &db, seg).await, 0);
    }

    // `total` is the cumulative-appended-bytes denominator: credited with every frame,
    // never debited. So a delete drops `live` but leaves `total` put, and `total - live`
    // is the segment's dead (reclaimable) bytes.
    #[tokio::test]
    async fn segcount_total_is_monotonic_never_debited() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Two extents into the open segment: fresh, so every appended byte is live.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 2 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        let ext1_len = frameloc_of(&store, &db, inode, 1).await.unwrap().byte_len as u64;
        let (live0, total0) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(live0, total0, "fresh segment: every appended byte is live");

        // Delete extent 1: live loses its bytes; total is never debited.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 1, 2).await.unwrap();
        commit(&store, txn).await;
        let (live1, total1) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(
            total1, total0,
            "total is monotonic: a delete never debits it"
        );
        assert_eq!(live1, live0 - ext1_len, "live drops by the deleted frame");
        assert_eq!(
            total1,
            live1 + ext1_len,
            "total - live is the dead-frame bytes (the fragmentation denominator)"
        );
    }

    // Compaction moves live bytes from the drained sources onto the packed segment.
    #[tokio::test]
    async fn compaction_moves_counter_from_sources_to_packed() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Two sealed segments, one live extent each.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, EXTENT_SIZE, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, 1).await.unwrap().segid;
        assert_ne!(seg_a, seg_b);
        assert!(segcount_of(&store, &db, seg_a).await > 0);
        assert!(segcount_of(&store, &db, seg_b).await > 0);

        store.compact_segments(&[seg_a, seg_b]).await.unwrap();

        // Sources drain to zero; the extents now point at one packed segment that
        // holds exactly their live bytes.
        assert_eq!(segcount_of(&store, &db, seg_a).await, 0);
        assert_eq!(segcount_of(&store, &db, seg_b).await, 0);
        let packed = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert_eq!(frameloc_of(&store, &db, 1, 1).await.unwrap().segid, packed);
        assert_ne!(packed, seg_a);
        assert_eq!(
            segcount_of(&store, &db, packed).await,
            live_bytes(&store, &db, 1, 0..2, packed).await,
        );
    }

    // The fail-closed directory-verify refuses to delete a segment the counter wrongly
    // calls dead (under-count) while a frame is still referenced: leak, never loss.
    #[tokio::test]
    async fn directory_verify_blocks_deleting_an_undercounted_live_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);

        // Simulate an under-count: force live to zero while the extent lives (total
        // is left nonzero, as a real segment's would be).
        let key = store.key_codec.segcount_key(seg.epoch, seg.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, KeyCodec::encode_segcount(0, 1000));
        commit(&store, txn).await;

        // Reclaim sees count 0 but the directory-verify finds extent 0 still points
        // here, so it must not delete the segment.
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            deleted, 0,
            "verify must block deleting a referenced segment"
        );
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[1u8; 1000]);
    }

    // A dead segment whose `segcount` counter is entirely ABSENT (a compaction whose
    // repoints all lost the CAS, or a crash before the crediting commit) is invisible
    // to the scan-driven fast loop: with no counter it never appears in the segcount
    // scan. Reclaiming these absent-counter orphans is the slow listing loop's job
    // (`sweep_orphans`); the fast loop must leave the object (and live data) untouched.
    #[tokio::test]
    async fn fast_reclaim_leaves_a_no_counter_orphan() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite so segment A's frame is dead (extent 0 points elsewhere), seal B.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Drop A's counter entirely so it looks like a failed-repoint orphan.
        let key = store.key_codec.segcount_key(seg.epoch, seg.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;

        // The fast loop scans counters, so A (no counter) is invisible and untouched.
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            deleted, 0,
            "fast loop must not reclaim an absent-counter orphan"
        );
        assert!(
            store.segments.list_segments().await.unwrap().contains(&seg),
            "no-counter orphan A must survive the fast loop (the slow sweep reclaims it)"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The slow sweep reclaims the no-counter orphan the fast loop leaves alone:
    // it lists the objects, point-gets each counter, and deletes those with none
    // (directory-verified first). Live data is untouched.
    #[tokio::test]
    async fn sweep_orphans_reclaims_a_no_counter_orphan() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let orphan = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite + seal so the first segment's frame is dead, then drop its counter
        // so the object carries none (a failed-repoint / crash-before-credit orphan).
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let key = store.key_codec.segcount_key(orphan.epoch, orphan.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );

        let reclaimed = store.sweep_orphans(Utc::now()).await.unwrap();
        assert_eq!(reclaimed, 1, "the no-counter orphan must be swept");
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan),
            "the orphan object must be deleted by the sweep"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The mirror leak: a counter whose OBJECT is gone (a crash between deleting a
    // dead segment's object and committing its counter drop). Because dead
    // candidates come from the counter scan, such a counter re-appears as dead every
    // pass; the fast loop must recognize the object is absent (NotFound) and drop the
    // leaked counter, not get stuck failing the directory verify forever.
    #[tokio::test]
    async fn reclaim_drops_a_counter_whose_object_is_gone() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let dead = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        // Overwrite + seal so `dead`'s frame is superseded (live == 0) but its
        // counter stays.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();

        // Simulate a crash between deleting the object and dropping the counter:
        // delete the object directly, leaving its (live == 0) counter behind.
        store.segments.delete_segment(dead).await.unwrap();
        let key = store.key_codec.segcount_key(dead.epoch, dead.counter);
        assert!(
            store.db.get_bytes(&key).await.unwrap().is_some(),
            "precondition: the leaked counter is present"
        );

        // The fast reclaim sees the counter (live == 0 -> dead), finds the object
        // absent, and drops the leaked counter instead of getting stuck.
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(
            store.db.get_bytes(&key).await.unwrap().is_none(),
            "the leaked counter (object already gone) must be dropped"
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // A normally-counted segment (counter present) is not an orphan: the sweep
    // point-gets its counter, finds it, and leaves the object alone.
    #[tokio::test]
    async fn sweep_orphans_leaves_a_counted_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[7u8; 1000]).await;
        store.seal_open().await.unwrap();
        let seg = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        assert!(segcount_of(&store, &db, seg).await > 0);

        let reclaimed = store.sweep_orphans(Utc::now()).await.unwrap();
        assert_eq!(
            reclaimed, 0,
            "a counted segment must not be swept as an orphan"
        );
        assert!(store.segments.list_segments().await.unwrap().contains(&seg));
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[7u8; 1000]);
    }

    // The sweep's age gate defers a too-young candidate: a cutoff predating the
    // orphan's PUT leaves it in place (its crediting commit could still be in
    // flight); a cutoff past its mtime reclaims it in that same pass.
    #[tokio::test]
    async fn sweep_orphans_waits_for_the_age_floor() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let orphan = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let key = store.key_codec.segcount_key(orphan.epoch, orphan.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;

        // A cutoff older than the orphan's PUT: too young, kept.
        let before_put = Utc::now() - chrono::Duration::seconds(60);
        assert_eq!(store.sweep_orphans(before_put).await.unwrap(), 0);
        assert!(
            store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );

        // A cutoff past its mtime: old enough, reclaimed in one pass.
        assert_eq!(store.sweep_orphans(Utc::now()).await.unwrap(), 1);
        assert!(
            !store
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    // The age gate reads the object's mtime, not process-lifetime state, so a
    // restarted process (fresh ExtentStore over the same backing store) reclaims
    // a pre-existing orphan on its very first sweep. The cutoff is past the
    // orphan's mtime, as production's `now - age floor` is for a day-old orphan.
    #[tokio::test]
    async fn sweep_orphans_reclaims_an_old_orphan_after_restart() {
        let (store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        let orphan = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        let key = store.key_codec.segcount_key(orphan.epoch, orphan.counter);
        let mut txn = db.new_transaction().unwrap();
        txn.delete_bytes(&key);
        commit(&store, txn).await;
        drop(store);

        // The "restarted" store shares the object store and db but none of the
        // in-RAM sweep state, and opens under the next epoch.
        let restarted = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8);
        let cutoff = Utc::now() + chrono::Duration::seconds(60);
        assert_eq!(
            restarted.sweep_orphans(cutoff).await.unwrap(),
            1,
            "a fresh process's first sweep must reclaim an old-enough orphan"
        );
        assert!(
            !restarted
                .segments
                .list_segments()
                .await
                .unwrap()
                .contains(&orphan)
        );
        assert_eq!(
            restarted.read(1, 0, 1000).await.unwrap().as_ref(),
            &[2u8; 1000]
        );
    }

    #[tokio::test]
    async fn single_and_multi_extent_write_read() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, b"hello").await;
        write_and_check(
            &store,
            &db,
            &mut model,
            3,
            &vec![7u8; EXTENT_SIZE * 2 + 100],
        )
        .await;
        write_and_check(&store, &db, &mut model, EXTENT_SIZE - 5, &[9u8; 20]).await;
    }

    #[tokio::test]
    async fn sparse_hole_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Write at a high offset; the gap before it is a hole.
        write_and_check(&store, &db, &mut model, 5 * EXTENT_SIZE, b"tail").await;
        // Read across the hole.
        let got = store.read(1, 0, 100).await.unwrap();
        assert_eq!(got.as_ref(), &vec![0u8; 100][..]);
    }

    #[tokio::test]
    async fn all_zero_write_makes_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; EXTENT_SIZE + 10]).await;
        // Overwrite the first extent with zeros -> becomes a hole (key deleted).
        write_and_check(&store, &db, &mut model, 0, &vec![0u8; EXTENT_SIZE]).await;
        // The extent key for extent 0 must be gone.
        let key = store.key_codec.extent_key(1, 0);
        assert!(db.get_bytes(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncate_shrink_then_regrow_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![5u8; 3 * EXTENT_SIZE]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .truncate(&mut txn, 1, model.len() as u64, 100)
            .await
            .unwrap();
        commit(&store, txn).await;
        model.truncate(100);
        assert_read_matches(&store, &model).await;

        // Regrow via a write past EOF; the gap must read as zeros.
        write_and_check(&store, &db, &mut model, 2 * EXTENT_SIZE, b"z").await;
    }

    #[tokio::test]
    async fn zero_range_punches_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![8u8; 2 * EXTENT_SIZE + 50]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .zero_range(&mut txn, 1, 100, EXTENT_SIZE as u64, model.len() as u64)
            .await
            .unwrap();
        commit(&store, txn).await;
        let end = (100 + EXTENT_SIZE).min(model.len());
        model[100..end].fill(0);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn contiguous_multiextent_read_is_one_ranged_get() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Four extents in ONE write -> one segment, frames contiguous.
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; 4 * EXTENT_SIZE]).await;
        store.seal_open().await.unwrap();

        let before = store.segments.read_calls();
        let got = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        let gets = store.segments.read_calls() - before;

        assert_eq!(got.len(), 4 * EXTENT_SIZE);
        assert_eq!(got.as_ref(), model.as_slice());
        assert_eq!(
            gets, 1,
            "a contiguous 4-extent read must coalesce into one ranged GET"
        );
    }

    #[tokio::test]
    async fn writes_buffer_until_seal_with_read_your_writes() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A write commits its extent but issues no PUT and serves reads from RAM.
        write_and_check(&store, &db, &mut model, 0, &[7u8; 100]).await;
        assert_eq!(
            store.segments.list_segments().await.unwrap().len(),
            0,
            "a write must not PUT a segment"
        );
        assert_eq!(
            store.segments.read_calls(),
            0,
            "the read was served from the open buffer"
        );

        // Sealing PUTs exactly one segment. With no in-RAM segment cache, the read
        // after seal is a ranged GET (in production the object store's parts cache,
        // warmed at seal time, absorbs it; these tests wire up no such cache).
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 100).await.unwrap().as_ref(), &model[..]);
        assert_eq!(
            store.segments.read_calls(),
            1,
            "a read of a sealed segment is one ranged GET"
        );
    }

    // A stored FrameLoc whose byte range lies outside the open buffer (a torn or
    // corrupt LSM value — any 32-byte value decodes) must surface as EIO, not an
    // out-of-range panic that poisons the open-segment lock for every later writer.
    #[tokio::test]
    async fn corrupt_frameloc_into_open_segment_is_eio_not_panic() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &[1u8; 100]).await;

        // Fabricate a pointer at the current open segment with an impossible range.
        let bogus = FrameLoc {
            segid: store.open.lock().unwrap().segid,
            frame_index: 0,
            byte_offset: 1 << 40,
            byte_len: 4096,
        };
        let key = store.key_codec.extent_key(1, 5);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, Bytes::copy_from_slice(&bogus.encode()));
        commit(&store, txn).await;

        assert!(matches!(store.get(1, 5).await, Err(FsError::IoError)));
        // The open-segment lock survived (not poisoned): writes still work.
        write_and_check(&store, &db, &mut model, 0, &[2u8; 100]).await;
    }

    // A corrupt extent value inside a multi-extent scan is the same EIO as the
    // single-extent path — skipping it would serve that extent as fabricated
    // zeros while a single-extent read of the same key errors.
    #[tokio::test]
    async fn multi_extent_read_of_a_corrupt_value_is_eio_not_zeros() {
        let (store, db) = make().await;

        // Plant a torn (undecodable) value at extent 1, then read across
        // extents 0..=2 so the ranged-scan path (not `get`) resolves it.
        let key = store.key_codec.extent_key(1, 1);
        let mut txn = db.new_transaction().unwrap();
        txn.put_bytes(&key, Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]));
        commit(&store, txn).await;

        let r = store.read_range(1, 0, 3 * EXTENT_SIZE as u64).await;
        assert!(matches!(r, Err(FsError::IoError)));
    }

    // In `get`'s GC-repoint retry, a re-resolved value that fails to decode is the
    // same corrupt-value EIO as the primary path — treating it like an absent key
    // would fabricate a hole of zeros from a torn LSM value.
    #[test]
    fn reresolve_distinguishes_a_hole_from_a_corrupt_value() {
        // Absent: a concurrent truncate/unlink made the extent a genuine hole.
        assert!(matches!(
            ExtentStore::decode_reresolved_extent(1, 0, None),
            Ok(None)
        ));
        // Present but undecodable (torn/truncated): EIO, never a hole.
        let torn = Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]);
        assert!(matches!(
            ExtentStore::decode_reresolved_extent(1, 0, Some(&torn)),
            Err(FsError::IoError)
        ));
        // A decodable value passes through for the segid comparison.
        let loc = FrameLoc {
            segid: Segid::new(1, 2),
            frame_index: 3,
            byte_offset: 4,
            byte_len: 5,
        };
        let enc = Bytes::copy_from_slice(&loc.encode());
        assert_eq!(
            ExtentStore::decode_reresolved_extent(1, 0, Some(&enc)).unwrap(),
            Some(loc)
        );
    }

    // Not a correctness test: measures single-stream engine-side write
    // throughput (codec + open-buffer append + seal + commit, no protocol, no
    // inode lock) against an in-memory object store.
    //   cargo test --release --lib -- --ignored --nocapture bench_sequential_write
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_sequential_write_throughput() {
        for (name, compression) in [
            ("lz4", CompressionConfig::Lz4),
            ("zstd(3)", CompressionConfig::Zstd(3)),
        ] {
            for (shape, data) in [
                ("incompressible", incompressible(1, 256 * 1024)),
                ("zeros", vec![0u8; 256 * 1024]),
            ] {
                let (store, db, _object_store) = make_with_compression(compression).await;
                let chunk = data.len();
                let total: usize = 512 * 1024 * 1024;
                let payload = Bytes::from(data);
                let start = std::time::Instant::now();
                let mut size = 0u64;
                for i in 0..(total / chunk) {
                    let mut txn = db.new_transaction().unwrap();
                    let tu = store
                        .write(&mut txn, 1, (i * chunk) as u64, &payload, size)
                        .await
                        .unwrap();
                    commit(&store, txn).await;
                    store.apply_tail_update(1, tu);
                    size += chunk as u64;
                }
                let secs = start.elapsed().as_secs_f64();
                eprintln!(
                    "engine sequential write [{name}, {shape}, 256 KiB ops]: {:.0} MB/s",
                    total as f64 / secs / 1e6
                );
            }
        }
    }

    // A batch of >= PARALLEL_COMPRESS_MIN_FRAMES frames takes the rayon
    // pre-compression fan-out (multi-thread runtimes only; the other tests run
    // current-thread and take the inline branch) and must roundtrip identically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_batch_write_takes_parallel_compression_path() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        let data = incompressible(3, PARALLEL_COMPRESS_MIN_FRAMES * 2 * EXTENT_SIZE);
        write_and_check(&store, &db, &mut model, 0, &data).await;
    }

    #[tokio::test]
    async fn large_write_seals_async_then_drains() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Incompressible bytes, so the sealed buffer actually crosses the threshold
        // (a repeating pattern would compress below it and never seal).
        let n = store.seal_threshold + 4 * EXTENT_SIZE;
        let data = incompressible(0, n);
        // One >threshold write triggers a background seal; the data reads back
        // correctly whether still in flight (sealing buffer) or already sealed.
        write_and_check(&store, &db, &mut model, 0, &data).await;

        // The flush barrier drains every in-flight seal to the object store.
        store.seal_open().await.unwrap();
        assert!(!store.segments.list_segments().await.unwrap().is_empty());
        assert_eq!(
            store.read(1, 0, n as u64).await.unwrap().as_ref(),
            model.as_slice()
        );
    }

    #[tokio::test]
    async fn reclaim_deletes_a_dead_superseded_segment() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Write + seal (segment A), then overwrite + seal (segment B): A is dead
        // (fully superseded).
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(deleted, 1, "the superseded segment must be reclaimed");
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);

        // Live data still reads after GC, and a second reclaim is a no-op.
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
        assert_eq!(store.reclaim_segments(Utc::now(), None).await.unwrap().0, 0);
    }

    #[tokio::test]
    async fn reclaim_waits_for_delete_horizon() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A dead segment: write+seal (A), overwrite+seal (B); A is now unreferenced.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 1000]).await;
        store.seal_open().await.unwrap();
        write_and_check(&store, &db, &mut model, 0, &[2u8; 1000]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // First pass records a future delete horizon for A and holds it.
        let horizon = Utc::now() + chrono::Duration::milliseconds(80);
        assert_eq!(store.reclaim_segments(horizon, None).await.unwrap().0, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Past the recorded horizon the next pass reclaims it (the new arg is
        // irrelevant — A keeps its first-seen deadline).
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(store.reclaim_segments(Utc::now(), None).await.unwrap().0, 1);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    #[tokio::test]
    async fn compact_relocates_live_frames_and_skips_dead() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // One 3-extent write -> one segment S holding extents 0,1,2.
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; 3 * EXTENT_SIZE]).await;
        store.seal_open().await.unwrap();
        let s = store.segments.list_segments().await.unwrap();
        assert_eq!(s.len(), 1);
        let s = s[0];

        // Full-overwrite extent 1, seal -> a new segment; extent 1's frame moves off
        // S, so S is now partially dead (extents 0,2 live, extent 1 dead).
        write_and_check(
            &store,
            &db,
            &mut model,
            EXTENT_SIZE,
            &vec![2u8; EXTENT_SIZE],
        )
        .await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 2);

        // Compact S: extents 0,2 relocate; extent 1 (already moved off S) is skipped.
        let (swapped, packed) = store.compact_segments(&[s]).await.unwrap();
        assert_eq!(
            swapped, 2,
            "two live frames relocated, the dead one skipped"
        );
        assert_eq!(packed, 1, "relocated into one packed segment");

        // S is now fully dead and reclaimable; data is unchanged.
        assert!(store.reclaim_segments(Utc::now(), None).await.unwrap().0 >= 1);
        let n = model.len() as u64;
        assert_eq!(
            store.read(1, 0, n).await.unwrap().as_ref(),
            model.as_slice()
        );
    }

    /// High-entropy (xorshift64), Lz4-incompressible bytes so segment sizes are
    /// predictable in size-threshold tests (the test codec compresses).
    fn incompressible(seed: usize, n: usize) -> Vec<u8> {
        let mut s = (seed as u64) ^ 0x243F_6A88_85A3_08D3;
        (0..n)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 24) as u8
            })
            .collect()
    }

    #[tokio::test]
    async fn compaction_leaves_a_few_tiny_segments_alone() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A handful of tiny, fully-live segments: below the merge floor with no dead
        // space. Packing them would just produce an equally-tiny segment that gets
        // re-compacted next pass — pure churn — so the GC must leave them be.
        for i in 0..5u8 {
            let off = i as usize * EXTENT_SIZE;
            write_and_check(&store, &db, &mut model, off, &[i + 1; 100]).await;
            store.seal_open().await.unwrap();
        }
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);

        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            relocated, 0,
            "tiny dense segments are left alone, not churned"
        );
        assert_eq!(deleted, 0);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 5);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn compaction_packs_small_segments_once_they_clear_the_floor() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Four small (< 1 MiB) but incompressible segments that together exceed the
        // merge floor, so packing yields a segment above the small threshold.
        let seg_bytes = 28 * EXTENT_SIZE; // 896 KiB < SMALL_SEGMENT_BYTES
        let n = 4usize; // ~3.5 MiB total > MIN_FREED_BYTES
        for i in 0..n {
            let off = i * seg_bytes;
            write_and_check(
                &store,
                &db,
                &mut model,
                off,
                &incompressible(off, seg_bytes),
            )
            .await;
            store.seal_open().await.unwrap();
        }
        let before = store.segments.list_segments().await.unwrap().len();
        assert_eq!(before, n);

        // Pass 1 packs them; pass 2 reclaims the drained sources.
        let (_, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(relocated > 0, "accumulated small segments get packed");
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        let after = store.segments.list_segments().await.unwrap().len();
        assert!(
            after < before,
            "packed into fewer segments ({before} -> {after})"
        );
        assert_read_matches(&store, &model).await;

        // The packed output clears the small threshold, so a further pass is a no-op.
        let (_, relocated3) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!(
            relocated3, 0,
            "packed output is not re-compacted (no churn)"
        );
    }

    #[tokio::test]
    async fn persistent_checkpoint_protects_older_segments() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // One write -> segment S; then zero the extent -> its extent is a hole and S
        // is fully dead.
        write_and_check(&store, &db, &mut model, 0, &[1u8; 100]).await;
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        write_and_check(&store, &db, &mut model, 0, &[0u8; 100]).await;
        store.seal_open().await.unwrap();

        // Any persistent-checkpoint pin (protect_before set) blocks both deletion
        // and compaction of S.
        let (deleted, relocated) = store
            .reclaim_segments(Utc::now(), Some(Utc::now() + chrono::Duration::minutes(5)))
            .await
            .unwrap();
        assert_eq!((deleted, relocated), (0, 0), "S is protected by the pin");
        assert!(!store.segments.list_segments().await.unwrap().is_empty());

        // Without the pin, the same dead segment is reclaimed.
        let (deleted, _) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert!(deleted >= 1, "S is reclaimed once no longer protected");
    }

    #[tokio::test]
    async fn compaction_groups_a_files_extents_for_one_get_reads() {
        let (store, db) = make().await;
        let d = |b: u8| Bytes::from(vec![b; EXTENT_SIZE]);
        // Interleave two files' extents in one open buffer -> one segment whose
        // frames are in write order: A0, B0, A1, B1, A2, B2.
        let mut sizes = [0u64, 0u64];
        for extent in 0..3u64 {
            for (i, inode) in [1u64, 2u64].into_iter().enumerate() {
                let mut txn = db.new_transaction().unwrap();
                let tu = store
                    .write(
                        &mut txn,
                        inode,
                        extent * EXTENT_SIZE as u64,
                        &d(inode as u8 * 10 + extent as u8),
                        sizes[i],
                    )
                    .await
                    .unwrap();
                commit(&store, txn).await;
                store.apply_tail_update(inode, tu);
                sizes[i] = (extent + 1) * EXTENT_SIZE as u64;
            }
        }
        store.seal_open().await.unwrap();
        let s = store.segments.list_segments().await.unwrap();
        assert_eq!(s.len(), 1);

        // Compact -> frames regrouped by (inode, extent); drop the drained source.
        store.compact_segments(&s).await.unwrap();
        store.reclaim_segments(Utc::now(), None).await.unwrap();

        // File A's three extents are now contiguous, so the whole-file read is a
        // single ranged GET, not three.
        let before = store.segments.read_calls();
        let got = store.read(1, 0, 3 * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(
            store.segments.read_calls() - before,
            1,
            "a file's extents are contiguous after compaction -> one GET"
        );
        let expect: Vec<u8> = (0..3u8).flat_map(|c| vec![10 + c; EXTENT_SIZE]).collect();
        assert_eq!(got.as_ref(), expect.as_slice());
    }

    #[test]
    fn read_ahead_planner() {
        let w = READ_AHEAD_WINDOW_BYTES;
        // First read of a stream: unconfirmed, no prefetch.
        let (s, p) = plan_read_ahead((0, 0, 0), 0, 1000);
        assert_eq!(s, (1000, 1000, 1));
        assert_eq!(p, None);
        // Second sequential read: confirmed -> prefetch a window ahead.
        let (s, p) = plan_read_ahead(s, 1000, 1000);
        assert_eq!(s, (2000, 2000 + w, 2));
        assert_eq!(p, Some((2000, 2000 + w)));
        // Still deep in the buffered-ahead: no new prefetch.
        let (s, p) = plan_read_ahead(s, 2000, 1000);
        assert_eq!(s, (3000, 2000 + w, 3));
        assert_eq!(p, None);
        // A non-contiguous jump resets to unconfirmed.
        let (s, p) = plan_read_ahead(s, 5_000_000, 1000);
        assert_eq!(s, (5_001_000, 5_001_000, 1));
        assert_eq!(p, None);
        // Less than half a window buffered ahead -> refill.
        let low = (2000 + w / 2 + 1, 2000 + w, 9);
        let (_, p) = plan_read_ahead(low, 2000 + w / 2 + 1, 100);
        assert!(
            p.is_some(),
            "refills when under half a window remains ahead"
        );
    }

    #[tokio::test]
    async fn read_ahead_spawns_only_on_confirmed_sequential() {
        let (store, _db) = make().await;
        // First read of a stream: nothing spawned (could be a one-off).
        assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
        // Second sequential read: a read-ahead task is spawned.
        let h = store.trigger_read_ahead(1, 1000, 1000);
        assert!(h.is_some(), "confirmed sequential -> read-ahead");
        h.unwrap().await.unwrap();
        // A non-contiguous jump: nothing spawned.
        assert!(store.trigger_read_ahead(1, 9_000_000, 1000).is_none());
    }

    #[tokio::test]
    async fn read_ahead_skip_at_cap_keeps_coverage_honest() {
        let (store, _db) = make().await;
        // Hold every permit so the planned prefetch is skipped at the cap.
        let held: Vec<_> = (0..READ_AHEAD_MAX_CONCURRENT)
            .map(|_| Arc::clone(&store.prefetch_sem).try_acquire_owned().unwrap())
            .collect();
        assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
        assert!(
            store.trigger_read_ahead(1, 1000, 1000).is_none(),
            "at the cap -> skip"
        );
        // The skip must not count the window as covered: `prefetched_to`
        // stays at the read end, so the next read re-plans.
        let (_, prefetched_to, _) = store.read_ahead.get(&1).map(|e| *e).unwrap();
        assert_eq!(prefetched_to, 2000, "skipped window recorded as covered");
        drop(held);
        let h = store.trigger_read_ahead(1, 2000, 1000);
        assert!(h.is_some(), "freed permit -> the next read re-triggers");
        h.unwrap().await.unwrap();
    }

    #[tokio::test]
    async fn sequential_append_via_tail_cache() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Many small appends into the same tail extent: exercises the tail cache
        // (each append RMWs the cached tail rather than re-fetching the frame).
        for _ in 0..50 {
            let off = model.len();
            write_and_check(&store, &db, &mut model, off, b"0123456789").await;
        }
        assert_eq!(model.len(), 500);
    }
}
