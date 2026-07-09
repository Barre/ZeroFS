//! Compaction mechanism: gather source segments' still-live frames
//! (compressed end to end), cut file-adjacency-preserving batches, seal
//! each into a fresh segment, and conditionally repoint the extents.

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

use super::reclaim::SMALL_SEGMENT_BYTES;
use super::write::SEAL_THRESHOLD;
use super::{ExtentStore, PARALLEL_EXTENT_OPS, human_bytes};
use crate::frame_codec::Compressed;
use crate::fs::FsError;
use crate::fs::inode::InodeId;
use crate::segment::{FrameLoc, Segid};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tracing::{info, warn};

/// Live frames evacuated from candidates are repacked into segments of this
/// target size, in source on-store bytes — the unit candidacy and the
/// selection budget use. Plaintext cuts would split a compressible chain
/// across outputs, manufacturing a fresh seam for the same reads to re-heat.
/// The compaction sealer is one object per batch (no auto-split), so an
/// output may exceed this only by re-framing delta plus a folded sliver.
const PACK_TARGET_BYTES: u64 = SEAL_THRESHOLD as u64;

/// How far a batch cut may back off the target to land on a file-adjacency
/// break instead of splitting a contiguous run (see [`plan_batches`]).
const PACK_CUT_SLACK_BYTES: u64 = 32 << 20;

/// Charged against the gather cap per frame, on top of its payload: the fixed
/// bookkeeping a gathered frame costs regardless of size (frames tuple, `seen`
/// entry, batch metas, source-loc copy, allocation slack), which dominates
/// when near-constant-fill data stores 32 KiB extents as a few hundred bytes.
/// Also bounds frames per round, and with it the largest repoint transaction.
const GATHER_FRAME_OVERHEAD_BYTES: u64 = 200;

/// A coalesced run of contiguous live frames within one source segment, for the
/// compaction gather: (byte_offset, total byte_len, first frame index, the
/// (inode, extent) slots it covers). Read back in one ranged GET.
type CompactRun = (u64, u32, u32, Vec<(InodeId, u64)>);

/// One packed batch awaiting seal: frames and their gathered source locations.
type PackBatch = (Vec<(InodeId, u64, Compressed)>, Vec<FrameLoc>);

/// Batch-cut plan for repacking gathered extents: over `(inode, extent
/// index, source store bytes)` metas, return end-exclusive batch boundaries
/// (strictly increasing, last == len).
///
/// Fills greedily to `target` store bytes, then backtracks up to `slack`
/// bytes to the latest file-adjacency break; if none, defers the whole
/// containing run when it fits a batch by itself.
fn plan_batches(metas: &[(InodeId, u64, u64)], target: u64, slack: u64) -> Vec<usize> {
    let n = metas.len();
    let mut bounds = Vec::new();
    // Whether metas[k - 1] and metas[k] are file-adjacent.
    let adj = |k: usize| {
        let (ia, ea, _) = metas[k - 1];
        let (ib, eb, _) = metas[k];
        ia == ib && ea.checked_add(1) == Some(eb)
    };
    let mut start = 0usize;
    while start < n {
        let mut sum = 0u64;
        let mut i = start;
        while i < n && (i == start || sum.saturating_add(metas[i].2) <= target) {
            sum = sum.saturating_add(metas[i].2);
            i += 1;
        }
        if i == n {
            bounds.push(n);
            break;
        }
        // metas[i] no longer fits. Latest break within the slack window wins.
        let mut cut = None;
        let mut deferred = 0u64;
        let mut j = i;
        while j > start {
            if !adj(j) {
                cut = Some(j);
                break;
            }
            deferred = deferred.saturating_add(metas[j - 1].2);
            if deferred > slack {
                break;
            }
            j -= 1;
        }
        let cut = cut.unwrap_or_else(|| {
            let mut r = i;
            while r > 0 && adj(r) {
                r -= 1;
            }
            let mut e = i + 1;
            while e < n && adj(e) {
                e += 1;
            }
            let run: u64 = metas[r..e].iter().map(|m| m.2).sum();
            // A run that fits a batch defers whole (underfilling here beats
            // a manufactured seam); one that can't fit anywhere hard-cuts.
            if r > start && run <= target { r } else { i }
        });
        bounds.push(cut);
        start = cut;
    }
    bounds
}

impl ExtentStore {
    /// Compact a set of fragmented/small segments: gather their still-live frames
    /// and repack them into fresh ~[`PACK_TARGET_BYTES`] segments, repointing each
    /// relocated extent. The drained sources become fully dead and are deleted by
    /// a later pass once past their horizon, so in-flight reads of the old
    /// locations stay valid.
    ///
    /// `atomic_prefix` lists the member counts of the leading atomic groups of
    /// `segids` (the admitted chain groups, in selection order); every source
    /// past their sum is an implicit singleton group. Pass `&[]` for
    /// all-singleton behavior.
    ///
    /// Returns (frames relocated, packed segments created, sources consumed):
    /// the gather stops between GROUPS at its RAM cap — a started group always
    /// completes, so a chain is never split into a seam-preserving partial
    /// repack — so `consumed` is the fully-gathered, group-granular prefix of
    /// `segids`; always >= the first group's size for a nonempty input,
    /// guaranteeing per-call progress. The caller must not drop the unconsumed
    /// rest from its backlog.
    ///
    /// The repoint is conditional and per-inode-locked: it holds the same write
    /// lock the foreground path uses and moves an extent only if it still points
    /// at the source frame, so a concurrent overwrite is never clobbered. The
    /// new segment is durably PUT (by `seal_compressed`) before any extent
    /// references it, so a crash between seal and repoint just leaves the
    /// source in place.
    pub async fn compact_segments(
        &self,
        segids: &[Segid],
        atomic_prefix: &[usize],
        // Gather cap in stored bytes (config `compact_round_max_mib`); a leading
        // atomic group is still gathered whole, so this bounds RAM to one group.
        // Frames stay compressed end to end (verify + AAD rebind only) and each
        // charges [`GATHER_FRAME_OVERHEAD_BYTES`] besides its payload, so gather
        // memory tracks the budget whatever the data's compression ratio.
        round_bytes: u64,
    ) -> Result<(usize, usize, usize), FsError> {
        debug_assert!(atomic_prefix.iter().sum::<usize>() <= segids.len());
        // Gather still-live frames across all sources, tagged with the exact
        // source FrameLoc the liveness read resolved (the repoint CAS's expected
        // value).
        let mut frames: Vec<(InodeId, u64, FrameLoc, Compressed)> = Vec::new();
        let mut gathered: u64 = 0;
        let mut consumed = 0usize;
        // Gather each live (inode, extent) at most once: an extent rewritten K times
        // within one seal window leaves K directory entries resolving to the same
        // current frame, and an extent can appear in more than one source directory.
        let mut seen: HashSet<(InodeId, u64)> = HashSet::new();
        let mut group_sizes = atomic_prefix
            .iter()
            .copied()
            .filter(|&g| g > 0)
            .chain(std::iter::repeat(1));
        while consumed < segids.len() {
            // Checked before each group, so the first group is always gathered
            // whole and every call makes progress. A group stopped mid-way would
            // 1:1-repack a chain prefix that dissolves no seam, so the cap never
            // splits one; if it trips at a group boundary the whole gather stops
            // (`consumed` stays a prefix).
            if gathered >= round_bytes {
                break;
            }
            let group_end = (consumed + group_sizes.next().unwrap_or(1)).min(segids.len());
            while consumed < group_end {
                self.gather_source(segids[consumed], &mut seen, &mut frames, &mut gathered)
                    .await?;
                consumed += 1;
            }
        }

        info!(
            "compaction: gathered {} live frames ({}) from {} of {} source segment(s)",
            frames.len(),
            human_bytes(gathered),
            consumed,
            segids.len(),
        );

        // Group by (inode, extent) so a file's extents land contiguously in the new
        // segment (consecutive frame index + adjacent bytes), collapsing its reads
        // to a single ranged GET instead of one per scattered run. Sequential inode
        // ids also cluster same-directory files into the same/adjacent segments.
        frames.sort_by_key(|(inode, extent, _, _)| (*inode, *extent));

        // Cut into target-sized batches by SOURCE on-store bytes, never
        // between file-adjacent extents unless a single run alone exceeds the
        // target (see plan_batches). A final sliver under the small threshold
        // folds into its predecessor: sealed alone it would re-enter small
        // candidacy and be rewritten again.
        let metas: Vec<(InodeId, u64, u64)> = frames
            .iter()
            .map(|&(inode, extent, loc, _)| (inode, extent, loc.byte_len as u64))
            .collect();
        let mut bounds = plan_batches(&metas, PACK_TARGET_BYTES, PACK_CUT_SLACK_BYTES);
        if bounds.len() > 1 {
            let last_start = bounds[bounds.len() - 2];
            let last_store: u64 = metas[last_start..].iter().map(|m| m.2).sum();
            if last_store < SMALL_SEGMENT_BYTES {
                bounds.remove(bounds.len() - 2);
            }
        }
        let mut batches: Vec<PackBatch> = Vec::with_capacity(bounds.len());
        let mut iter = frames.into_iter();
        let mut prev = 0usize;
        for &end in &bounds {
            let mut batch = Vec::with_capacity(end - prev);
            let mut batch_src = Vec::with_capacity(end - prev);
            for _ in prev..end {
                let (inode, extent, src, bytes) = iter.next().expect("bounds within frames");
                batch.push((inode, extent, bytes));
                batch_src.push(src);
            }
            batches.push((batch, batch_src));
            prev = end;
        }

        // Seal + repoint each batch. A batch that repoints nothing (every
        // frame overwritten out from under us) left only an orphan segment,
        // so it doesn't count as packed output.
        let mut relocated = 0;
        let mut packed = 0;
        for (batch, batch_src) in batches {
            let n = self.seal_and_repoint(batch, &batch_src).await?;
            relocated += n;
            packed += usize::from(n > 0);
        }
        Ok((relocated, packed, consumed))
    }

    /// Gather one source segment's still-live frames for [`Self::compact_segments`]:
    /// append them to `frames` (tagged with the resolved source [`FrameLoc`]) as
    /// verified, still-compressed payloads, growing `seen` and the stored-byte
    /// `gathered` total.
    async fn gather_source(
        &self,
        segid: Segid,
        seen: &mut HashSet<(InodeId, u64)>,
        frames: &mut Vec<(InodeId, u64, FrameLoc, Compressed)>,
        gathered: &mut u64,
    ) -> Result<(), FsError> {
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
        // Resolved location per slot, carried through to the batch cut and the
        // repoint CAS.
        let loc_of: HashMap<(InodeId, u64), FrameLoc> = live
            .iter()
            .map(|&(inode, extent, loc)| ((inode, extent), loc))
            .collect();
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
        let read: Vec<Vec<(InodeId, u64, Compressed)>> = stream::iter(runs)
            .map(|(byte_offset, total_len, first_frame, slots)| {
                let store = self.clone();
                async move {
                    let payloads = store
                        .segments
                        .read_compressed_run(segid, byte_offset, total_len, first_frame, &slots)
                        .await
                        .map_err(|_| FsError::IoError)?;
                    Ok::<_, FsError>(
                        slots
                            .into_iter()
                            .zip(payloads)
                            .map(|((inode, extent), p)| (inode, extent, p))
                            .collect::<Vec<_>>(),
                    )
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect::<Vec<_>>()
            .await?;

        for (inode, extent, payload) in read.into_iter().flatten() {
            seen.insert((inode, extent));
            // Stored size (the AEAD strips a constant nonce+tag; close enough
            // for a RAM cap and consistent with the selection budget's unit)
            // plus the fixed per-frame bookkeeping.
            *gathered += payload.len() as u64 + GATHER_FRAME_OVERHEAD_BYTES;
            frames.push((inode, extent, loc_of[&(inode, extent)], payload));
        }
        Ok(())
    }

    /// Seal one packed batch into a new segment and repoint the extents that still
    /// reference their source frame. `src[i]` is the gathered source [`FrameLoc`]
    /// of `batch[i]`, the swap's expected value. The batch's payloads are still
    /// compressed; the seal rebinds each frame's AAD without a decompress.
    async fn seal_and_repoint(
        &self,
        batch: Vec<(InodeId, u64, Compressed)>,
        src: &[FrameLoc],
    ) -> Result<usize, FsError> {
        // A packed object is another future FrameLoc source. Although the
        // production GC task is single-threaded, protect it with the same
        // invariant as foreground writes so future concurrent callers cannot
        // make it eligible before its repoints commit.
        let publish_guard = self.new_segment_publish_guard().await;
        let batch_len = batch.len();
        let new_locs = self
            .segments
            .seal_compressed(batch)
            .await
            .map_err(|_| FsError::IoError)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::COMPACT_AFTER_SEAL_BEFORE_REPOINT);

        // All frames in one seal go to one new segment.
        let new_segid = new_locs.first().map(|(_, _, loc)| loc.segid);

        // Group by inode so each conditional swap is taken under that inode's write
        // lock (excludes a concurrent foreground write to the same extent).
        // Ordered so the lock-acquisition sequence is deterministic.
        let mut by_inode: BTreeMap<InodeId, Vec<(u64, FrameLoc, FrameLoc)>> = BTreeMap::new();
        for (i, (inode, extent, new_loc)) in new_locs.into_iter().enumerate() {
            by_inode
                .entry(inode)
                .or_default()
                .push((extent, src[i], new_loc));
        }

        let mut swapped = 0;
        for (inode, items) in by_inode {
            #[cfg(feature = "failpoints")]
            {
                fail_point!(fp::COMPACT_BETWEEN_REPOINTS);
                fp::widen(fp::COMPACT_BETWEEN_REPOINTS).await;
            }
            let _guard = self.lock_manager.acquire(inode).await;
            let mut txn = self.db.new_transaction()?;
            txn.hold_segment_publish_guard(Arc::clone(&publish_guard));
            let mut any = false;
            for (extent, old_loc, new_loc) in items {
                let key = self.key_codec.extent_key(inode, extent);
                if let Some(enc) = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?
                    && let Some(loc) = FrameLoc::decode(&enc)
                    // Full-loc equality, not just the segid: a rewrite staged
                    // into the same source segment whose commit lands between
                    // the gather and this swap moves the pointer to a sibling
                    // frame, and swapping on the segid alone would revert the
                    // extent to the gathered (superseded) frame's bytes.
                    && loc == old_loc
                {
                    txn.put_bytes(&key, Bytes::copy_from_slice(&new_loc.encode()));
                    // Move the bytes only on a won CAS: after a lost race the
                    // relocated frame is dead weight (no live pointer) and must
                    // not be credited. Source debit is live-only; the packed
                    // frame credits both live and total.
                    self.seg_delta(&mut txn, old_loc.segid, -(loc.byte_len as i64), 0);
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
            batch_len,
            swapped,
            batch_len - swapped,
            orphan_note,
        );
        Ok(swapped)
    }
}

#[cfg(test)]
mod tests {
    use super::super::reclaim::{
        ChainOutcome, PassStatus, QUIESCENT_AFTER_DEFAULT, TAIL_SCRUB_DEFAULT_PERCENT,
    };
    use super::super::select::MAX_COMPACT_BYTES_PER_ROUND;
    use super::super::test_util::*;
    use super::*;
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::fs::EXTENT_SIZE;
    use chrono::Utc;
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::time::Duration;

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

        store
            .compact_segments(&[seg_a, seg_b], &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();

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
        let (swapped, packed, consumed) = store
            .compact_segments(&[s], &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();
        assert_eq!(consumed, 1);
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
        store
            .compact_segments(&s, &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();
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

    /// A world whose decompressed size dwarfs the round budget while its
    /// stored size stays tiny: two constant-fill (highly compressible)
    /// segments whose live plaintext alone overflows the budget, then three
    /// incompressible small segments that clear the economics gate on their
    /// own. All five tie on the live fraction, so selection keeps scan
    /// (counter) order and the compressible pair is always gathered first.
    async fn ram_capped_world() -> (ExtentStore, Arc<Db>, Vec<Segid>) {
        let (store, db) = make().await;
        let comp_extents = (MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 32;
        for (inode, fill) in [(1u64, 0x11u8), (2, 0x22)] {
            let mut txn = db.new_transaction().unwrap();
            let tu = store
                .write(
                    &mut txn,
                    inode,
                    0,
                    &Bytes::from(vec![fill; comp_extents * EXTENT_SIZE]),
                    0,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            store.apply_tail_update(inode, tu);
            store.seal_open().await.unwrap();
        }
        for inode in 3u64..6 {
            let mut txn = db.new_transaction().unwrap();
            let tu = store
                .write(
                    &mut txn,
                    inode,
                    0,
                    &Bytes::from(incompressible(inode as usize, 28 * EXTENT_SIZE)),
                    0,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            store.apply_tail_update(inode, tu);
            store.seal_open().await.unwrap();
        }
        let mut segids = Vec::new();
        for inode in 1u64..6 {
            let segid = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
            let (live, total) = segcount_pair_of(&store, &db, segid).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(
                total < SMALL_SEGMENT_BYTES,
                "world setup: every segment must be a small candidate"
            );
            segids.push(segid);
        }
        (store, db, segids)
    }

    /// A world whose plaintext dwarfs the round budget drains in ONE batch:
    /// frames stay compressed through the gather, so its cap tracks stored
    /// bytes (the selection budget's unit) and compressibility no longer
    /// forces mid-pass deferral. (Before the compressed passthrough, these
    /// same sources tripped the then-plaintext cap and needed later batches.)
    #[tokio::test]
    async fn compressible_sources_pack_in_one_batch_under_the_stored_byte_cap() {
        let (store, db, segids) = ram_capped_world().await;
        let comp_extents = (MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 32;
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                QUIESCENT_AFTER_DEFAULT,
                // No multi-batch approval: everything must fit the first batch.
                || false,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.relocated,
            2 * comp_extents + 3 * 28,
            "one batch drained every source: no plaintext-cap deferral"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "fully drained: no backlog left"
        );
        for (i, inode) in (1u64..6).enumerate() {
            assert_ne!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[i],
                "every source drained within one pass"
            );
        }
    }

    /// `compact_segments`' budget is checked between groups: a call whose
    /// budget is exhausted consumes a group-granular prefix (never splitting
    /// an atomic group, always taking at least the first) and leaves the
    /// rest untouched for the caller's backlog, packable by a later call.
    #[tokio::test]
    async fn exhausted_gather_budget_consumes_a_group_prefix_and_never_splits_a_group() {
        let (store, db) = make().await;
        // Four single-extent sources of incompressible data; segids[0..2] form
        // one atomic group (a chain), segids[2..] are singletons.
        let mut segids = Vec::new();
        for inode in 1u64..5 {
            write_chunk_at(
                &store,
                &db,
                inode,
                0,
                Bytes::from(incompressible(inode as usize, EXTENT_SIZE)),
            )
            .await;
            store.seal_open().await.unwrap();
            segids.push(frameloc_of(&store, &db, inode, 0).await.unwrap().segid);
        }

        // A 1-byte budget is exhausted before (and after) every group; the
        // leading group must still gather whole.
        let (relocated, packed, consumed) = store.compact_segments(&segids, &[2], 1).await.unwrap();
        assert_eq!(consumed, 2, "the leading atomic group consumed whole");
        assert_eq!(relocated, 2, "both group members' frames relocated");
        assert_eq!(packed, 1, "the group merged into one packed segment");
        let now_1 = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_2 = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        assert_ne!(now_1, segids[0]);
        assert_eq!(now_1, now_2, "group members merged, not split");
        for inode in 3u64..5 {
            assert_eq!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[(inode - 1) as usize],
                "past-budget sources left in place, not lost"
            );
        }

        // Singletons: the same 1-byte budget consumes exactly the first.
        let rest = &segids[2..];
        let (relocated, _, consumed) = store.compact_segments(rest, &[], 1).await.unwrap();
        assert_eq!(
            (consumed, relocated),
            (1, 1),
            "one singleton per exhausted call"
        );
        assert_ne!(
            frameloc_of(&store, &db, 3, 0).await.unwrap().segid,
            segids[2]
        );
        assert_eq!(
            frameloc_of(&store, &db, 4, 0).await.unwrap().segid,
            segids[3],
            "the unconsumed singleton stays for the caller's backlog"
        );
    }

    /// Relocation is an AAD rebind of the verified still-compressed payload,
    /// never a decompress/recompress: frames sealed under a Zstd codec keep
    /// byte-identical payload sizes when a restarted Lz4-configured store
    /// compacts them (a recompression would re-encode them as lz4), and read
    /// back intact through the auto-detecting open.
    #[tokio::test]
    async fn compaction_passes_compressed_payloads_through_across_codec_configs() {
        let (store, db, object_store) = make_with_compression(CompressionConfig::Zstd(3)).await;
        // Compressible content: zstd and lz4 encodings would differ in size,
        // so byte_len equality across the repack distinguishes passthrough
        // from recompression.
        let data = vec![0x41u8; 2 * EXTENT_SIZE];
        write_and_check(&store, &db, &mut Vec::new(), 0, &data).await;
        store.seal_open().await.unwrap();
        let old_locs = [
            frameloc_of(&store, &db, 1, 0).await.unwrap(),
            frameloc_of(&store, &db, 1, 1).await.unwrap(),
        ];

        // A restarted store, now configured for Lz4, compacts the zstd world.
        let store2 = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8);
        let (relocated, _, _) = store2
            .compact_segments(&[old_locs[0].segid], &[], MAX_COMPACT_BYTES_PER_ROUND)
            .await
            .unwrap();
        assert_eq!(relocated, 2);
        for (extent, old) in old_locs.iter().enumerate() {
            let new = frameloc_of(&store2, &db, 1, extent as u64).await.unwrap();
            assert_ne!(new.segid, old.segid, "relocated");
            assert_eq!(
                new.byte_len, old.byte_len,
                "payload passed through byte-identically (no recompression)"
            );
        }
        assert_eq!(
            store2.read(1, 0, data.len() as u64).await.unwrap().as_ref(),
            data.as_slice(),
            "zstd payloads read back through the lz4-configured codec"
        );
    }

    /// Write `bytes` to `inode` at extent offset `extent_off` (chunks
    /// appended in order, so the prior file size is the offset).
    async fn write_chunk_at(
        store: &ExtentStore,
        db: &Db,
        inode: InodeId,
        extent_off: u64,
        bytes: Bytes,
    ) {
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                extent_off * EXTENT_SIZE as u64,
                &bytes,
                extent_off * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(store, txn).await;
        store.apply_tail_update(inode, tu);
    }

    /// A chain whose HEAD member alone decodes past the round budget must
    /// still gather whole (the group is atomic), not stop at consumed=1: a
    /// 1:1 repack dissolves no seam, spends the reserve, and re-heats
    /// forever on a quiescent store. Constant-fill (compressible) data keeps
    /// the members inside the on-store reserve while their plaintext
    /// overflows the budget (trivially satisfied now that the gather stays
    /// compressed; kept as the atomicity regression); all-zero writes would
    /// elide into holes.
    #[tokio::test]
    async fn chain_with_head_over_ram_cap_packs_whole_in_one_pass() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Segment A: incompressible ballast for density (dense + fully live
        // means never a scan candidate — only the chain path may move it)
        // plus constant fill decoding past the cap alone.
        let ballast = 36u64;
        let comp = (MAX_COMPACT_BYTES_PER_ROUND as usize / EXTENT_SIZE + 64) as u64;
        let ballast_buf = incompressible(1, ballast as usize * EXTENT_SIZE);
        write_chunk_at(&store, &db, 1, 0, Bytes::from(ballast_buf.clone())).await;
        write_chunk_at(
            &store,
            &db,
            1,
            ballast,
            Bytes::from(vec![0x11u8; comp as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        // Segment B: the same file continues into a dense second member.
        let a_extents = ballast + comp;
        let b_extents = 40u64;
        let b_buf = incompressible(2, b_extents as usize * EXTENT_SIZE);
        write_chunk_at(&store, &db, 1, a_extents, Bytes::from(b_buf.clone())).await;
        store.seal_open().await.unwrap();

        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 1, a_extents).await.unwrap().segid;
        assert_ne!(seg_a, seg_b);
        for s in [seg_a, seg_b] {
            let (live, total) = segcount_pair_of(&store, &db, s).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(total >= SMALL_SEGMENT_BYTES, "world setup: dense");
        }

        // Two boundary-crossing reads in distinct GC rounds arm the seam;
        // the negative control pins that one episode packs nothing.
        let boundary = (a_extents - 1) * EXTENT_SIZE as u64;
        store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        let (deleted, relocated) = store.reclaim_segments(Utc::now(), None).await.unwrap();
        assert_eq!((deleted, relocated), (0, 0), "one episode: not hot yet");
        store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;

        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.relocated,
            (a_extents + b_extents) as usize,
            "consumed covers both members: the whole group gathered"
        );
        assert_eq!(
            outcome.status,
            PassStatus::Completed { saturated: false },
            "nothing deferred, nothing pending"
        );
        assert_eq!(
            outcome.chains,
            ChainOutcome {
                assembled: 1,
                packed: 1,
                deferred: 0,
                warm: 0,
                unpackable: 0,
            },
            "consumed-based count: the whole group packed as one chain",
        );

        // Merged: the whole file lives in one fresh segment — the seam and
        // both old members are gone.
        let now_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_b = frameloc_of(&store, &db, 1, a_extents).await.unwrap().segid;
        assert_ne!(now_a, seg_a);
        assert_ne!(now_b, seg_b);
        assert_eq!(now_a, now_b, "co-read data co-located: no more seam");

        // Integrity spot-checks: ballast, fill, and the seam region.
        let got = store.read(1, 0, EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(got.as_ref(), &ballast_buf[..EXTENT_SIZE]);
        let got = store
            .read(1, boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert_eq!(&got[..EXTENT_SIZE], &vec![0x11u8; EXTENT_SIZE][..]);
        assert_eq!(&got[EXTENT_SIZE..], &b_buf[..EXTENT_SIZE]);
    }

    /// Two hot chains, one whose plaintext dwarfs the round budget, both
    /// pack in a single pass: the gather holds compressed payloads, so its
    /// budget tracks stored bytes and compressibility no longer defers the
    /// second group to a later pass. (Before the compressed passthrough the
    /// first chain exhausted the then-plaintext cap here.)
    #[tokio::test]
    async fn two_chains_pack_in_one_pass_under_the_stored_byte_budget() {
        let (store, db) = make().await;
        store.enable_nominations();
        // Chain 1 (inode 1): A1|A2 jointly decode past the cap. A1 carries
        // incompressible ballast so the packed output stays dense (no
        // small-candidate churn muddying the next pass).
        let ballast = 40u64;
        let half = ((MAX_COMPACT_BYTES_PER_ROUND / 2) as usize / EXTENT_SIZE + 64) as u64;
        write_chunk_at(
            &store,
            &db,
            1,
            0,
            Bytes::from(incompressible(1, ballast as usize * EXTENT_SIZE)),
        )
        .await;
        write_chunk_at(
            &store,
            &db,
            1,
            ballast,
            Bytes::from(vec![0x11u8; half as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        let a1_extents = ballast + half;
        write_chunk_at(
            &store,
            &db,
            1,
            a1_extents,
            Bytes::from(vec![0x22u8; half as usize * EXTENT_SIZE]),
        )
        .await;
        store.seal_open().await.unwrap();
        // Chain 2 (inode 2): B1|B2, dense and tiny-plaintext (never scan
        // candidates: only the chain path can move them).
        let b_extents = 40u64;
        write_chunk_at(
            &store,
            &db,
            2,
            0,
            Bytes::from(incompressible(2, b_extents as usize * EXTENT_SIZE)),
        )
        .await;
        store.seal_open().await.unwrap();
        write_chunk_at(
            &store,
            &db,
            2,
            b_extents,
            Bytes::from(incompressible(3, b_extents as usize * EXTENT_SIZE)),
        )
        .await;
        store.seal_open().await.unwrap();

        let a1 = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let a2 = frameloc_of(&store, &db, 1, a1_extents).await.unwrap().segid;
        let b1 = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let b2 = frameloc_of(&store, &db, 2, b_extents).await.unwrap().segid;
        for s in [b1, b2] {
            let (live, total) = segcount_pair_of(&store, &db, s).await;
            assert_eq!(live, total, "world setup: fully live");
            assert!(total >= SMALL_SEGMENT_BYTES, "world setup: dense");
        }

        // Arm both seams across two GC rounds.
        let a_boundary = (a1_extents - 1) * EXTENT_SIZE as u64;
        let b_boundary = (b_extents - 1) * EXTENT_SIZE as u64;
        store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store.reclaim_segments(Utc::now(), None).await.unwrap();
        store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;

        // One pass packs both chains: neither the reserve (their stored live
        // is tiny) nor the gather budget (stored-byte unit) defers anything.
        let outcome = store
            .reclaim_segments_tuned(
                Utc::now(),
                None,
                Some(TAIL_SCRUB_DEFAULT_PERCENT),
                Duration::from_millis(500),
                || false,
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.relocated,
            (a1_extents + half + 2 * b_extents) as usize,
            "both chains' members gathered in one pass"
        );
        assert_eq!(
            outcome.chains,
            ChainOutcome {
                assembled: 2,
                packed: 2,
                deferred: 0,
                warm: 0,
                unpackable: 0,
            },
            "both groups packed, none deferred",
        );
        let now_a1 = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let now_a2 = frameloc_of(&store, &db, 1, a1_extents).await.unwrap().segid;
        assert_ne!(now_a1, a1);
        assert_ne!(now_a2, a2);
        assert_eq!(now_a1, now_a2, "chain 1 merged");
        let now_b1 = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let now_b2 = frameloc_of(&store, &db, 2, b_extents).await.unwrap().segid;
        assert_ne!(now_b1, b1);
        assert_ne!(now_b2, b2);
        assert_eq!(now_b1, now_b2, "chain 2 merged: the seam is gone");

        // Integrity across both seams.
        let got = store
            .read(1, a_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        assert_eq!(&got[..EXTENT_SIZE], &vec![0x11u8; EXTENT_SIZE][..]);
        assert_eq!(&got[EXTENT_SIZE..], &vec![0x22u8; EXTENT_SIZE][..]);
        let got = store
            .read(2, b_boundary, 2 * EXTENT_SIZE as u64)
            .await
            .unwrap();
        let expect_b1 = incompressible(2, b_extents as usize * EXTENT_SIZE);
        let expect_b2 = incompressible(3, b_extents as usize * EXTENT_SIZE);
        assert_eq!(
            &got[..EXTENT_SIZE],
            &expect_b1[(b_extents as usize - 1) * EXTENT_SIZE..]
        );
        assert_eq!(&got[EXTENT_SIZE..], &expect_b2[..EXTENT_SIZE]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // The batch-cut plan: exact partition, seam-safe boundaries, bounded
        // batches, determinism.
        #[test]
        fn plan_batches_invariants(
            raw in prop::collection::vec((0u64..4, 0u64..12, 1u64..2000), 0..200),
            target in 500u64..4000,
            slack in 0u64..1500,
        ) {
            let mut metas: Vec<(InodeId, u64, u64)> = raw;
            metas.sort();
            metas.dedup_by(|a, b| (a.0, a.1) == (b.0, b.1));
            let bounds = plan_batches(&metas, target, slack);
            if metas.is_empty() {
                prop_assert!(bounds.is_empty());
                return Ok(());
            }

            // Partition: strictly increasing boundaries ending at len — every
            // extent in exactly one batch, order preserved, no empty batch.
            prop_assert_eq!(*bounds.last().unwrap(), metas.len());
            let mut prev = 0usize;
            for &b in &bounds {
                prop_assert!(b > prev, "empty or out-of-order batch");
                prev = b;
            }

            let adj = |k: usize| {
                let (ia, ea, _) = metas[k - 1];
                let (ib, eb, _) = metas[k];
                ia == ib && ea.checked_add(1) == Some(eb)
            };

            // Seam-safety: a boundary between file-adjacent extents is legal
            // only when their containing run alone exceeds the target.
            for &b in &bounds[..bounds.len() - 1] {
                if adj(b) {
                    let mut r = b;
                    while r > 0 && adj(r) {
                        r -= 1;
                    }
                    let mut e = b + 1;
                    while e < metas.len() && adj(e) {
                        e += 1;
                    }
                    let run: u64 = metas[r..e].iter().map(|m| m.2).sum();
                    prop_assert!(run > target, "run split although it fits a batch");
                }
            }

            // Bounded batches: only a single extent may exceed the target.
            let mut start = 0usize;
            for &b in &bounds {
                let sum: u64 = metas[start..b].iter().map(|m| m.2).sum();
                prop_assert!(sum <= target || b - start == 1, "over-target multi-extent batch");
                start = b;
            }

            // Determinism.
            prop_assert_eq!(&plan_batches(&metas, target, slack), &bounds);
        }
    }
}
