//! Read path: FrameLoc resolution, run-coalesced ranged reads with
//! read-your-writes from the in-RAM buffers, and the sequential logical
//! read-ahead. Demand reads also record compaction heat (nominations,
//! crossing pairs).

use super::select::{NOMINATE_MIN_FANOUT, NOMINATE_PER_CALL_CAP, PAIR_BUMPS_PER_CALL, PairStats};
use super::{ExtentStore, ZERO_EXTENT};
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::fs::inode::InodeId;
use crate::fs::{EXTENT_SIZE, FsError};
use crate::segment::{FrameLoc, Segid};
use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::error;

/// Logical (file-offset) read-ahead distance for a confirmed-sequential
/// stream. Follows the file's extents across segments, which the physical
/// (per-object) prefetcher can't do.
const READ_AHEAD_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
/// Consecutive sequential reads before prefetch kicks in, so a one-off read
/// doesn't drag in a whole window.
const READ_AHEAD_MIN_SEQ: u32 = 2;
/// Global cap on concurrent in-flight read-ahead fetches.
pub(super) const READ_AHEAD_MAX_CONCURRENT: usize = 16;
/// Bound on the per-inode read-ahead state map (LRU-evicted; ~24 B/entry).
pub(super) const READ_AHEAD_TRACK_BYTES: usize = 4 * 1024 * 1024;

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

impl ExtentStore {
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

    /// Read `[offset, offset+length)`, then kick off the bounded,
    /// sequential-only logical read-ahead so the next read lands warm in the
    /// parts cache.
    pub async fn read(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let data = self.read_range(id, offset, length, true).await?;
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
            let _ = this.read_range(id, start, end - start, false).await;
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
    /// the read-ahead task calls, so it never recurses). Only demand reads
    /// (`is_demand`) feed compaction nominations, so prefetch traffic can't
    /// nominate.
    async fn read_range(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
        is_demand: bool,
    ) -> Result<Bytes, FsError> {
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
        let mut nominate: Vec<Segid> = Vec::new();
        // Seams this read paid. `prev_nonram` = (segid, end extent) of the
        // previous on-store run; holes and RAM runs break adjacency.
        let mut crossings: Vec<(Segid, Segid)> = Vec::new();
        let mut prev_nonram: Option<(Segid, u64)> = None;
        let track = is_demand && self.nominations_enabled.load(Ordering::Relaxed);
        let mut extent = start_extent;
        while extent <= end_extent {
            let Some(first) = loc_map.get(&extent).copied() else {
                // Hole: this extent contributes zeros.
                let (cs, ce) = slice(extent);
                result.extend_from_slice(&ZERO_EXTENT[cs..ce]);
                prev_nonram = None;
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
                Some(f) => {
                    prev_nonram = None;
                    Some(f)
                }
                None => {
                    // Non-RAM ⇒ PUT complete ⇒ directory durably readable:
                    // safe to nominate.
                    if track {
                        if nominate.len() < NOMINATE_PER_CALL_CAP
                            && !nominate.contains(&first.segid)
                        {
                            nominate.push(first.segid);
                        }
                        // A seam: adjacent file data split across objects, or
                        // (same segid) scattered within one (a self-pair).
                        if let Some((prev_segid, prev_end)) = prev_nonram
                            && prev_end == extent
                            && crossings.len() < PAIR_BUMPS_PER_CALL
                        {
                            let pair = PairStats::key(prev_segid, first.segid);
                            if !crossings.contains(&pair) {
                                crossings.push(pair);
                            }
                        }
                    }
                    prev_nonram = Some((first.segid, extent + n));
                    #[cfg(feature = "failpoints")]
                    {
                        fail_point!(fp::READ_AFTER_RESOLVE_BEFORE_FETCH);
                        fp::widen(fp::READ_AFTER_RESOLVE_BEFORE_FETCH).await;
                    }
                    // A GET error is swallowed: a compaction repoint+delete can 404
                    // this run's segment out from under us. Fall back to per-extent
                    // reads via `get`, which re-resolves each FrameLoc.
                    self.segments
                        .read_run(
                            first.segid,
                            first.byte_offset,
                            total_len as u32,
                            first.frame_index,
                            &slots,
                        )
                        .await
                        .ok()
                }
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
        // Fan-out is the read-amplification signal; pushed as a set so one
        // pass co-packs what was co-read.
        if nominate.len() >= NOMINATE_MIN_FANOUT {
            let mut noms = self.nominations.lock().unwrap();
            for segid in nominate {
                noms.push(segid);
            }
        }
        // Recorded below the nomination floor too: the pair itself is the
        // fan-out; once per pass regardless of read rate.
        if !crossings.is_empty() {
            let round = self.gc_round.load(Ordering::Relaxed);
            let now = Instant::now();
            let mut stats = self.pair_stats.lock().unwrap();
            for (a, b) in crossings {
                stats.bump(a, b, round, now);
            }
        }
        Ok(result.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;

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

        let r = store.read_range(1, 0, 3 * EXTENT_SIZE as u64, true).await;
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

    #[tokio::test]
    async fn crossings_count_seams_not_reads_and_holes_break_adjacency() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Segments A (extents 0-1), B (2-3), C (4-5), all sealed.
        for extent in 0..6u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
            if extent % 2 == 1 {
                store.seal_open().await.unwrap();
            }
        }
        let a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let b = frameloc_of(&store, &db, 1, 2).await.unwrap().segid;
        let c = frameloc_of(&store, &db, 1, 4).await.unwrap().segid;

        // One pass over A|B|C pays each seam once; re-reading in the same GC
        // round adds nothing (burst reads are not episodes).
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        {
            let stats = store.pair_stats.lock().unwrap();
            assert_eq!(stats.map.len(), 2);
            assert_eq!(stats.map[&PairStats::key(a, b)].count, 1);
            assert_eq!(stats.map[&PairStats::key(b, c)].count, 1);
        }

        // A hole between two segments is not a seam: the data isn't adjacent.
        let (store2, db2) = make().await;
        store2.enable_nominations();
        for extent in 0..2u64 {
            write_extent(&store2, &db2, extent, &[1u8; EXTENT_SIZE]).await;
        }
        store2.seal_open().await.unwrap();
        for extent in 3..5u64 {
            // Writing extent 3 with the file at 2 extents leaves extent 2 a hole.
            let mut txn = db2.new_transaction().unwrap();
            let tu = store2
                .write(
                    &mut txn,
                    1,
                    extent * EXTENT_SIZE as u64,
                    &Bytes::from(vec![2u8; EXTENT_SIZE]),
                    2 * EXTENT_SIZE as u64,
                )
                .await
                .unwrap();
            commit(&store2, txn).await;
            store2.apply_tail_update(1, tu);
        }
        store2.seal_open().await.unwrap();
        store2.read(1, 0, 5 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store2.pair_stats.lock().unwrap().map.is_empty());
    }

    #[tokio::test]
    async fn reads_nominate_only_enabled_fanned_out_and_on_store_segments() {
        let (store, db) = make().await;
        // Segment A holds extents 0-1, segment B extents 2-3.
        for extent in 0..4u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
            if extent % 2 == 1 {
                store.seal_open().await.unwrap();
            }
        }
        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, 2).await.unwrap().segid;

        // Disabled (replica / pre-GC shape): a fanned-out read tracks nothing.
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        store.enable_nominations();

        // A read served by one segment is below the fan-out floor.
        store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        // A read fanning out across both nominates both; re-reading dedups.
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
        {
            let noms = store.nominations.lock().unwrap();
            assert_eq!(noms.set.len(), 2);
            assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
        }

        // RAM-served runs never count: extents 4-5 live in the open buffer, so
        // a read across B + open is one on-store segment — below the fan-out
        // floor (RAM runs cost no GETs, so the read isn't suffering).
        store.nominations.lock().unwrap().drain();
        for extent in 4..6u64 {
            write_extent(&store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
        }
        store
            .read(1, 2 * EXTENT_SIZE as u64, 4 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert!(store.nominations.lock().unwrap().set.is_empty());

        // A read across A + B + open clears the floor on the two on-store
        // segments and still never nominates the open segid.
        store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
        {
            let open_segid = store.open.lock().unwrap().segid;
            let noms = store.nominations.lock().unwrap();
            assert_eq!(noms.set.len(), 2);
            assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
            assert!(!noms.set.contains(&open_segid));
        }
    }

    #[tokio::test]
    async fn per_call_cap_bounds_nominations() {
        let (store, db) = make().await;
        store.enable_nominations();
        // More single-extent segments than the per-call cap.
        let n = NOMINATE_PER_CALL_CAP as u64 + 2;
        for extent in 0..n {
            write_extent(&store, &db, extent, &[1u8; EXTENT_SIZE]).await;
            store.seal_open().await.unwrap();
        }
        store.read(1, 0, n * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(
            store.nominations.lock().unwrap().set.len(),
            NOMINATE_PER_CALL_CAP
        );
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
}
