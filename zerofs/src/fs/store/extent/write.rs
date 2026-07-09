//! Write path: read-modify-write over full extents staged into the in-RAM
//! open segment, background and synchronous sealing (the durability
//! barrier), delete/truncate/zero-range staging with segment-counter
//! debits, and the tail cache for sequential appends.

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

use super::{ExtentStore, PARALLEL_EXTENT_OPS, TailUpdate, ZERO_EXTENT};
use crate::db::Transaction;
use crate::frame_codec::Compressed;
use crate::fs::inode::InodeId;
use crate::fs::{EXTENT_SIZE, FsError};
use crate::replication::ReplOp;
use crate::segment::{DirEntry, FrameLoc, Segid};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

/// Frames per write batch before pre-compression fans out on rayon; below
/// this the dispatch overhead outweighs the parallelism.
const PARALLEL_COMPRESS_MIN_FRAMES: usize = 8;

pub(super) const TAIL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Seal (PUT) the open segment once its packed frames reach this size, bounding
/// the in-RAM buffer between flushes. The seal PUT is concurrent multipart
/// (`SegmentStore::put_segment`), so its fsync-path latency stays bounded
/// despite the size.
pub(super) const SEAL_THRESHOLD: usize = 256 * 1024 * 1024;

/// Max segments sealing (PUT in flight) concurrently. Bounds the un-PUT RAM in
/// `sealing` to ~this × SEAL_THRESHOLD; acquiring all permits is the fsync
/// drain barrier.
pub(super) const MAX_INFLIGHT_SEALS: usize = 4;

/// The in-RAM open segment. Frames are sealed (compressed+encrypted) and appended
/// here at write time, so an extent's location is known and committed eagerly;
/// the segment object is PUT only on flush or when it crosses [`SEAL_THRESHOLD`].
pub(super) struct OpenSegment {
    pub(super) segid: Segid,
    pub(super) buf: Vec<u8>,
    pub(super) dir: Vec<DirEntry>,
}

impl ExtentStore {
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

    /// Stage the extent-key delete only: the segment-counter debit is the
    /// caller's job (see [`Self::delete_range`], which debits as it scans).
    pub fn delete(&self, txn: &mut Transaction, id: InodeId, extent_idx: u64) {
        let key = self.key_codec.extent_key(id, extent_idx);
        txn.delete_bytes(&key);
    }

    /// Stage live/total byte deltas for `segid`'s counter onto the txn; the
    /// commit worker folds them into the absolute `(live, total)`. A debit
    /// passes `total_delta == 0` (`total` is monotonic).
    pub(super) fn seg_delta(
        &self,
        txn: &mut Transaction,
        segid: Segid,
        live_delta: i64,
        total_delta: i64,
    ) {
        debug_assert!(
            total_delta <= 0 || txn.has_segment_publish_guard(),
            "a positive segment credit must remain publication-protected through commit"
        );
        txn.add_seg_delta(
            &self.key_codec.segcount_key(segid.epoch, segid.counter),
            live_delta,
            total_delta,
        );
    }

    /// Stage deletes for extents `[start, end)` with their live-byte debits.
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
        let mut old_debits: Vec<(Segid, u32)> = Vec::with_capacity(edits.len());
        if let (Some(min), Some(max)) = (
            edits.iter().map(|(e, _)| *e).min(),
            edits.iter().map(|(e, _)| *e).max(),
        ) {
            let edited: HashSet<u64> = edits.iter().map(|(e, _)| *e).collect();
            let start_key = self.key_codec.extent_key(id, min);
            let end_key = self.key_codec.extent_key(id, max.saturating_add(1));
            let mut stream = self
                .db
                .scan(start_key..end_key)
                .await
                .map_err(|_| FsError::IoError)?;
            while let Some(result) = stream.next().await {
                let (key, value) = result.map_err(|_| FsError::IoError)?;
                if let Some(extent_idx) = self.key_codec.parse_extent_key(&key)
                    && edited.contains(&extent_idx)
                    && let Some(loc) = FrameLoc::decode(&value)
                {
                    old_debits.push((loc.segid, loc.byte_len));
                }
            }
        }
        // Compress before taking the open-segment lock: compression is the
        // expensive half of the codec and depends only on the plaintext, while
        // the AEAD binds (segid, frame_index), assigned under the lock. Large
        // batches fan out on rayon; block_in_place needs the multi-thread
        // runtime (tests run current-thread), and small batches stay inline.
        let payloads: Vec<&Bytes> = edits.iter().filter_map(|(_, e)| e.as_ref()).collect();
        let compressed: Vec<Compressed> = if payloads.len() >= PARALLEL_COMPRESS_MIN_FRAMES
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
        if edits.iter().any(|(_, edit)| edit.is_some()) {
            // Must precede assignment under `open`: once the lock is released,
            // GC may try to seal this segment and must see this publisher.
            self.protect_segment_publish(txn).await;
        }
        // Keep later writers from appending once this writer discovers that a
        // rotation is due. In particular, this guard stays held while
        // spawn_seal waits for an in-flight-seal permit.
        let _append_guard = self.append_gate.lock().await;
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
        let over_threshold = self.open.lock().unwrap().buf.len() >= self.seal_threshold();
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

    /// Stage a write at `offset` as read-modify-write over full extents;
    /// all-zero extents become holes. Commits nothing itself: the returned
    /// [`TailUpdate`] must be applied only after the txn commits.
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
        let end_offset = offset
            .checked_add(data.len() as u64)
            .ok_or(FsError::InvalidArgument)?;
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

    /// Stage a shrink to `new_size` (growth is a no-op: extension is sparse):
    /// drops extents past the end and zero-fills the partial last one.
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

    /// Stage zeroes over `[offset, offset + length)` capped at `file_size`:
    /// fully-covered extents become holes, partial ones are RMW-zeroed.
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
        let end_offset = offset.checked_add(length).ok_or(FsError::InvalidArgument)?;
        self.tail_invalidate(id);

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
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::config::CompressionConfig;

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

    // The debit scan must find and debit *every* prior frame of a multi-extent
    // overwrite, not just the range endpoints: a missed debit over-counts live
    // bytes (a space leak), a double debit under-counts (GC could drop a live
    // segment). Fresh-write and single-extent-overwrite tests never make the
    // scan return more than one key, so cover the multi-key case explicitly.
    #[tokio::test]
    async fn segcount_debits_every_extent_of_a_multi_extent_overwrite() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Four full extents into the open segment.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 4 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );

        // Overwrite all four in one write: the scan returns four keys, and each
        // superseded frame must be debited. The counter then equals only the
        // four live (current) frames; any missed or extra debit breaks equality.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; 4 * EXTENT_SIZE]),
                4 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );
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
        let n = store.seal_threshold() + 4 * EXTENT_SIZE;
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
    async fn saturated_seals_backpressure_before_later_writers_append() {
        let (store, db) = make().await;
        store.set_seal_threshold(1);

        // Model four slow object-store PUTs by holding every seal permit. The
        // first writer can append, but then has to wait before rotating.
        let permits = store
            .seal_sem
            .clone()
            .acquire_many_owned(MAX_INFLIGHT_SEALS as u32)
            .await
            .unwrap();
        let first = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 100, 0, &Bytes::from_static(b"a"), 0)
                    .await
                    .unwrap();
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.open.lock().unwrap().dir.len() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first writer did not reach the seal-permit wait");

        let second = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 101, 0, &Bytes::from_static(b"b"), 0)
                    .await
                    .unwrap();
            }
        });

        let appended_behind_blocked_seal =
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                loop {
                    if store.open.lock().unwrap().dir.len() > 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
        assert!(
            !appended_behind_blocked_seal,
            "a later writer grew the open segment while rotation was blocked"
        );
        assert!(!second.is_finished());

        drop(permits);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            first.await.unwrap();
            second.await.unwrap();
            while !store.seals_quiet() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writers did not resume after seal permits were released");
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
