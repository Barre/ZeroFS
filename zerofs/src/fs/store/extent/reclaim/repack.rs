//! Repack mechanism: gather source segments' still-live frames, sort them for
//! locality, seal one fresh segment, and conditionally repoint the extents.

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

use super::super::{ExtentStore, PARALLEL_EXTENT_OPS, human_bytes};
use crate::db::ExtentRefGuard;
use crate::frame_codec::{Compressed, FrameCodec};
use crate::fs::FsError;
use crate::fs::inode::InodeId;
use crate::fs::write_coordinator::LockedMutation;
use crate::segment::{FrameLoc, MAX_SEGMENT_OBJECT_BYTES, Segid, max_segment_object_size};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tracing::{info, warn};

/// Raw reverse-map directories retained while resolving source liveness.
const PARALLEL_SOURCE_PLANS: usize = 8;

/// One resolved live frame in a source directory.
type PlannedFrame = (InodeId, u64, FrameLoc);

/// One extent to swap: `(extent, expected old location, packed location)`.
type Repoint = (u64, FrameLoc, FrameLoc);

/// Contiguous live frames within one source segment, planned before payload reads.
struct SourceRun {
    byte_offset: u64,
    total_len: u32,
    first_frame: u32,
    frames: Vec<PlannedFrame>,
}

/// Directory + liveness resolution for one source, before any payload GET.
struct SourcePlan {
    segid: Segid,
    runs: Vec<SourceRun>,
    frame_bytes: u64,
    frame_count: u64,
}

pub struct RepackProgress {
    pub(super) relocated: usize,
    /// Sources planned into this job; the caller restores the rest.
    pub(super) consumed: usize,
}

/// Repack live frames into one fresh segment. Drained sources remain readable
/// until a later cycle deletes them. The first source may exceed `job_bytes`;
/// the caller restores any unconsumed suffix. The destination PUT completes
/// before pointer publication.
pub async fn run(
    store: &ExtentStore,
    segids: &[Segid],
    job_bytes: u64,
) -> Result<RepackProgress, FsError> {
    let _activity = store.segment_reclaim_stats.begin_repack();
    let (plans, gathered) = plan_sources(store, segids, job_bytes).await?;
    let consumed = plans.len();
    // Keep the planned stored-byte reservation live through payload reads,
    // packing, and every repoint.
    let _memory = store.segment_reclaim_stats.reserve_repack_memory(gathered);

    let mut frames = gather_runs(store, plans).await?;
    info!(
        "segment repack: gathered {} live frames ({}) from {} of {} source segment(s)",
        frames.len(),
        human_bytes(gathered),
        consumed,
        segids.len(),
    );
    if frames.is_empty() {
        return Ok(RepackProgress {
            relocated: 0,
            consumed,
        });
    }

    // Keep each file's selected extents contiguous for coalesced ranged reads.
    frames.sort_by_key(|(inode, extent, _, _)| (*inode, *extent));
    let mut output = Vec::with_capacity(frames.len());
    let mut source_locs = Vec::with_capacity(frames.len());
    for (inode, extent, source, bytes) in frames {
        output.push((inode, extent, bytes));
        source_locs.push(source);
    }
    let relocated = seal_and_repoint(store, output, &source_locs).await?;
    Ok(RepackProgress {
        relocated,
        consumed,
    })
}

/// Fetch planned payloads with bounded concurrency across this job's sources.
async fn gather_runs(
    store: &ExtentStore,
    plans: Vec<SourcePlan>,
) -> Result<Vec<(InodeId, u64, FrameLoc, Compressed)>, FsError> {
    let reads = plans
        .into_iter()
        .flat_map(|plan| plan.runs.into_iter().map(move |run| (plan.segid, run)));
    stream::iter(reads)
        .map(|(segid, run)| async move {
            let slots: Vec<(InodeId, u64)> = run
                .frames
                .iter()
                .map(|&(inode, extent, _)| (inode, extent))
                .collect();
            let _fetch = store.segment_reclaim_stats.begin_fetch();
            let payloads = store
                .segments
                .read_compressed_run(
                    segid,
                    run.byte_offset,
                    run.total_len,
                    run.first_frame,
                    &slots,
                )
                .await
                .map_err(|_| FsError::IoError)?;
            Ok::<_, FsError>(
                run.frames
                    .into_iter()
                    .zip(payloads)
                    .map(|((inode, extent, loc), payload)| (inode, extent, loc, payload))
                    .collect::<Vec<_>>(),
            )
        })
        .buffer_unordered(PARALLEL_EXTENT_OPS)
        .try_fold(Vec::new(), |mut frames, run| async move {
            frames.extend(run);
            Ok(frames)
        })
        .await
}

/// Stored-byte budget for one job's output. The first source may exceed the
/// soft budget so an oversized segment still makes progress; nothing may push
/// the encoded object past the single-PUT limit.
struct GatherBudget {
    soft_limit: u64,
    gathered: u64,
    frames: u64,
}

enum Admission {
    Admitted,
    /// Does not fit; close the job before this source.
    Full,
    /// A lone source whose encoded bound exceeds the hard limit.
    TooLarge {
        bound: u64,
    },
}

impl GatherBudget {
    fn new(job_bytes: u64) -> Self {
        Self {
            soft_limit: job_bytes.min(MAX_SEGMENT_OBJECT_BYTES),
            gathered: 0,
            frames: 0,
        }
    }

    fn has_room(&self) -> bool {
        self.gathered < self.soft_limit
    }

    fn admit(&mut self, codec: &FrameCodec, plan: &SourcePlan, first: bool) -> Admission {
        let gathered = self.gathered.saturating_add(plan.frame_bytes);
        let frames = self.frames.saturating_add(plan.frame_count);
        let bound = max_segment_object_size(codec, gathered, frames);
        if bound > MAX_SEGMENT_OBJECT_BYTES {
            return if first {
                Admission::TooLarge { bound }
            } else {
                Admission::Full
            };
        }
        if !first && gathered > self.soft_limit {
            return Admission::Full;
        }
        self.gathered = gathered;
        self.frames = frames;
        Admission::Admitted
    }
}

/// Plan the admitted source prefix with bounded directory residency. Dropping
/// the stream cancels plans beyond the first source that did not fit.
async fn plan_sources(
    store: &ExtentStore,
    segids: &[Segid],
    job_bytes: u64,
) -> Result<(Vec<SourcePlan>, u64), FsError> {
    let mut budget = GatherBudget::new(job_bytes);
    let mut plans = Vec::new();
    let sources = stream::iter(segids.iter().copied())
        .map(|segid| async move { plan_source(store, segid).await })
        .buffered(PARALLEL_SOURCE_PLANS);
    futures::pin_mut!(sources);
    while plans.is_empty() || budget.has_room() {
        let Some(plan) = sources.try_next().await? else {
            break;
        };
        match budget.admit(&store.codec, &plan, plans.is_empty()) {
            Admission::Admitted => plans.push(plan),
            Admission::Full => break,
            Admission::TooLarge { bound } => {
                tracing::error!(
                    "segment repack: source {:?} encoded-size upper bound is {}; hard limit is {}",
                    plan.segid,
                    human_bytes(bound),
                    human_bytes(MAX_SEGMENT_OBJECT_BYTES),
                );
                return Err(FsError::IoError);
            }
        }
    }
    Ok((plans, budget.gathered))
}

async fn plan_source(store: &ExtentStore, segid: Segid) -> Result<SourcePlan, FsError> {
    let mut dir = {
        let _fetch = store.segment_reclaim_stats.begin_fetch();
        store
            .segments
            .read_directory(segid)
            .await
            .map_err(|_| FsError::IoError)?
    };

    // A segment can contain several versions of an extent. Resolve each slot
    // once; its current FrameLoc identifies the live frame.
    let mut seen: HashSet<(InodeId, u64)> = HashSet::new();
    dir.retain(|e| seen.insert((e.inode, e.extent)));
    drop(seen);
    let mut live: Vec<PlannedFrame> = stream::iter(dir)
        .map(|e| async move {
            let _permit = store
                .reclaim_metadata_sem
                .acquire()
                .await
                .map_err(|_| FsError::IoError)?;
            let key = store.key_codec.extent_key(e.inode, e.extent);
            let enc = store
                .db
                .get_bytes(&key)
                .await
                .map_err(|_| FsError::IoError)?;
            Ok::<_, FsError>(
                enc.and_then(|b| FrameLoc::decode(&b))
                    .filter(|loc| loc.segid == segid)
                    .map(|loc| (e.inode, e.extent, loc)),
            )
        })
        .buffer_unordered(PARALLEL_EXTENT_OPS)
        .try_filter_map(|frame| async move { Ok(frame) })
        .try_collect()
        .await?;

    live.sort_by_key(|(_, _, loc)| loc.frame_index);
    let frame_bytes = live.iter().map(|(_, _, loc)| loc.byte_len as u64).sum();
    let frame_count = live.len() as u64;
    let mut runs: Vec<SourceRun> = Vec::new();
    for (inode, extent, loc) in live {
        match runs.last_mut() {
            Some(run)
                if run.first_frame + run.frames.len() as u32 == loc.frame_index
                    && run.byte_offset + run.total_len as u64 == loc.byte_offset =>
            {
                run.total_len += loc.byte_len;
                run.frames.push((inode, extent, loc));
            }
            _ => runs.push(SourceRun {
                byte_offset: loc.byte_offset,
                total_len: loc.byte_len,
                first_frame: loc.frame_index,
                frames: vec![(inode, extent, loc)],
            }),
        }
    }
    Ok(SourcePlan {
        segid,
        runs,
        frame_bytes,
        frame_count,
    })
}

/// Seal one output segment and repoint the extents that still reference their
/// source frame. `source_locs[i]` is the expected old location of `output[i]`.
/// Payloads stay compressed; sealing only rebinds each frame's AAD.
async fn seal_and_repoint(
    store: &ExtentStore,
    output: Vec<(InodeId, u64, Compressed)>,
    source_locs: &[FrameLoc],
) -> Result<usize, FsError> {
    // Pin the packed segment from allocation through every repoint. Clones
    // let the coordinator retain the pin if this future is cancelled.
    let extent_ref_guard = store.new_extent_ref_guard().await;
    let frame_count = output.len();
    let new_locs = {
        let _put = store.segment_reclaim_stats.begin_put();
        store
            .segments
            .seal_compressed(output)
            .await
            .map_err(|_| FsError::IoError)?
    };

    #[cfg(feature = "failpoints")]
    {
        fail_point!(fp::REPACK_AFTER_SEAL_BEFORE_REPOINT);
        fp::widen(fp::REPACK_AFTER_SEAL_BEFORE_REPOINT).await;
    }

    // Every nonempty job seals into one segment.
    let new_segid = new_locs
        .first()
        .expect("sealed output has locations")
        .2
        .segid;
    let sealed_total = new_locs.iter().map(|(_, _, loc)| loc.byte_len as u64).sum();

    // Group by inode so each conditional swap is taken under that inode's write
    // lock (excludes a concurrent foreground write to the same extent).
    // Ordered so concurrent scheduling starts deterministically.
    let mut by_inode: BTreeMap<InodeId, Vec<Repoint>> = BTreeMap::new();
    for (i, (inode, extent, new_loc)) in new_locs.into_iter().enumerate() {
        by_inode
            .entry(inode)
            .or_default()
            .push((extent, source_locs[i], new_loc));
    }

    let (mut swapped, remaining) =
        repoint_until_first_credit(store, by_inode, &extent_ref_guard, new_segid, sealed_total)
            .await?;

    // With the total credited, disjoint inode swaps can run in parallel and
    // add only their newly-live bytes.
    swapped += stream::iter(remaining)
        .map(|(inode, items)| {
            let extent_ref_guard = Arc::clone(&extent_ref_guard);
            async move { repoint_inode(store, inode, items, extent_ref_guard, new_segid, 0).await }
        })
        .buffer_unordered(PARALLEL_EXTENT_OPS)
        .try_fold(0usize, |swapped, committed| async move {
            Ok(swapped + committed)
        })
        .await?;

    // Nothing repointed: every gathered frame was overwritten before the
    // CAS, leaving a pure orphan (PUT, no pointer, no counter). Delete it
    // now, best-effort; a crash here leaves it for the orphan sweep.
    let orphan_note = if swapped == 0 {
        let delete_result = {
            let _delete = store.segment_reclaim_stats.begin_delete();
            store.segments.delete_segment(new_segid).await
        };
        match delete_result {
            Ok(()) => " (orphan, deleted)",
            Err(e) => {
                warn!(
                    "segment repack: delete of orphaned {new_segid:?} failed: {e}; \
                     left for the orphan sweep"
                );
                " (orphan, delete failed)"
            }
        }
    } else {
        ""
    };
    info!(
        "segment repack: packed {:?}: {} frames, {} repointed, {} discarded to concurrent writes{}",
        new_segid,
        frame_count,
        swapped,
        frame_count - swapped,
        orphan_note,
    );
    Ok(swapped)
}

/// Repoint inode groups one at a time until one commits, crediting the packed
/// object's complete `total` in that first transaction. Counter deltas add,
/// so exactly one transaction may carry the total, and it must be one that
/// publishes a pointer: that keeps `total` correct when later groups lose
/// their CAS to foreground writes and makes a crash after any published
/// pointer self-describing. Returns the frames swapped and the groups left.
async fn repoint_until_first_credit(
    store: &ExtentStore,
    by_inode: BTreeMap<InodeId, Vec<Repoint>>,
    extent_ref_guard: &ExtentRefGuard,
    new_segid: Segid,
    sealed_total: u64,
) -> Result<(usize, Vec<(InodeId, Vec<Repoint>)>), FsError> {
    let mut groups = by_inode.into_iter();
    for (inode, items) in groups.by_ref() {
        let swapped = repoint_inode(
            store,
            inode,
            items,
            Arc::clone(extent_ref_guard),
            new_segid,
            sealed_total,
        )
        .await?;
        if swapped > 0 {
            return Ok((swapped, groups.collect()));
        }
    }
    Ok((0, Vec::new()))
}

/// Conditionally repoint one inode's gathered frames. Different inodes are
/// independent and run concurrently; the keyed lock retains foreground write
/// serialization, and the commit coordinator remains the sole writer of
/// segment-counter deltas.
async fn repoint_inode(
    store: &ExtentStore,
    inode: InodeId,
    items: Vec<Repoint>,
    extent_ref_guard: ExtentRefGuard,
    destination: Segid,
    total_credit: u64,
) -> Result<usize, FsError> {
    #[cfg(feature = "failpoints")]
    {
        fail_point!(fp::REPACK_BETWEEN_REPOINTS);
        fp::widen(fp::REPACK_BETWEEN_REPOINTS).await;
    }
    let inode_guard = store.lock_manager.acquire(inode).await;
    let _metadata = store
        .reclaim_metadata_sem
        .acquire()
        .await
        .map_err(|_| FsError::IoError)?;
    let mut txn = store.db.new_transaction()?;
    txn.hold_extent_ref_guard(extent_ref_guard);
    let mut swapped = 0;
    for (extent, old_loc, new_loc) in items {
        let key = store.key_codec.extent_key(inode, extent);
        if let Some(enc) = store
            .db
            .get_bytes(&key)
            .await
            .map_err(|_| FsError::IoError)?
            && let Some(loc) = FrameLoc::decode(&enc)
            // Full-loc equality, not just the segid: a rewrite staged into
            // the same source segment can move the pointer to a sibling
            // frame between gather and swap.
            && loc == old_loc
        {
            txn.put_bytes(&key, Bytes::copy_from_slice(&new_loc.encode()));
            store.seg_delta(&mut txn, old_loc.segid, -(loc.byte_len as i64), 0);
            store.seg_delta(&mut txn, new_loc.segid, new_loc.byte_len as i64, 0);
            swapped += 1;
        }
    }
    if swapped == 0 {
        return Ok(0);
    }
    if total_credit > 0 {
        store.seg_delta(&mut txn, destination, 0, total_credit as i64);
    }

    let mutation = LockedMutation::new(txn, inode_guard);
    store.commit_locked_transaction(mutation).await?;
    Ok(swapped)
}

#[cfg(test)]
mod tests {
    use super::super::super::test_util::*;
    use super::super::REPACK_JOB_BYTES;
    use super::*;
    use crate::config::CompressionConfig;
    use crate::db::Db;
    use crate::fs::EXTENT_SIZE;
    use std::time::Duration;

    async fn repack(
        store: &ExtentStore,
        segids: &[Segid],
        job_bytes: u64,
    ) -> Result<(usize, usize), FsError> {
        let progress = run(store, segids, job_bytes).await?;
        Ok((progress.relocated, progress.consumed))
    }

    // Repack moves live bytes from the drained sources onto the packed segment.
    #[tokio::test]
    async fn repack_moves_counter_from_sources_to_packed() {
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

        repack(&store, &[seg_a, seg_b], REPACK_JOB_BYTES)
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
    async fn partially_repointed_output_counts_discarded_frames_as_dead() {
        let (store, db) = make().await;
        committed_write(&store, &db, 1, 0, Bytes::from(vec![1u8; 1000]), 0).await;
        committed_write(&store, &db, 2, 0, Bytes::from(vec![2u8; 1000]), 0).await;
        store.seal_open().await.unwrap();

        let old_a = frameloc_of(&store, &db, 1, 0).await.unwrap();
        let old_b = frameloc_of(&store, &db, 2, 0).await.unwrap();
        let payload_a = store
            .segments
            .read_compressed_run(
                old_a.segid,
                old_a.byte_offset,
                old_a.byte_len,
                old_a.frame_index,
                &[(1, 0)],
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        let payload_b = store
            .segments
            .read_compressed_run(
                old_b.segid,
                old_b.byte_offset,
                old_b.byte_len,
                old_b.frame_index,
                &[(2, 0)],
            )
            .await
            .unwrap()
            .pop()
            .unwrap();

        // Make A's gathered location stale before the conditional repoint;
        // B still swaps, so the packed object survives with A physically dead.
        committed_write(&store, &db, 1, 0, Bytes::from(vec![3u8; 1000]), 1000).await;
        store.seal_open().await.unwrap();
        let swapped = seal_and_repoint(
            &store,
            vec![(1, 0, payload_a), (2, 0, payload_b)],
            &[old_a, old_b],
        )
        .await
        .unwrap();
        assert_eq!(swapped, 1);

        let packed_b = frameloc_of(&store, &db, 2, 0).await.unwrap();
        assert_ne!(packed_b.segid, old_b.segid);
        let physical_total: u64 = store
            .segments
            .read_directory(packed_b.segid)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.len as u64 + crate::segment::LEN_PREFIX as u64)
            .sum();
        assert_eq!(
            segcount_pair_of(&store, &db, packed_b.segid).await,
            (packed_b.byte_len as u64, physical_total),
        );
        assert!(physical_total > packed_b.byte_len as u64);
        assert_eq!(store.read(1, 0, 1000).await.unwrap().as_ref(), &[3u8; 1000]);
        assert_eq!(store.read(2, 0, 1000).await.unwrap().as_ref(), &[2u8; 1000]);
    }

    #[tokio::test]
    async fn repacking_after_a_partial_repoint_converges() {
        const FRAMES: usize = 40; // Each inode contributes more than the 1 MiB payoff floor.
        let (store, db) = make().await;
        let first = Bytes::from(incompressible(1, FRAMES * EXTENT_SIZE));
        let second = Bytes::from(incompressible(2, FRAMES * EXTENT_SIZE));
        committed_write(&store, &db, 1, 0, first.clone(), 0).await;
        committed_write(&store, &db, 2, 0, second.clone(), 0).await;
        store.seal_open().await.unwrap();
        let source = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let (plans, _) = plan_sources(&store, &[source], REPACK_JOB_BYTES)
            .await
            .unwrap();
        let gathered = gather_runs(&store, plans).await.unwrap();

        // Overwrite half the gathered frames before publishing the output.
        let replacement = Bytes::from(incompressible(3, first.len()));
        committed_write(&store, &db, 1, 0, replacement.clone(), first.len() as u64).await;
        store.seal_open().await.unwrap();
        let mut output = Vec::new();
        let mut old_locs = Vec::new();
        for (inode, extent, loc, payload) in gathered {
            output.push((inode, extent, payload));
            old_locs.push(loc);
        }
        assert_eq!(
            seal_and_repoint(&store, output, &old_locs).await.unwrap(),
            FRAMES
        );
        let partial = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let (live, total) = segcount_pair_of(&store, &db, partial).await;
        assert!(total - live >= super::super::SMALL_SEGMENT_BYTES);

        // Once writes stop, one cleanup repack yields a fully-live, stable output.
        let (_, relocated) = reclaim(&store, tokio::time::Instant::now(), false)
            .await
            .unwrap();
        assert_eq!(relocated, FRAMES);
        let stable = frameloc_of(&store, &db, 2, 0).await.unwrap();
        assert_ne!(stable.segid, partial);
        let (live, total) = segcount_pair_of(&store, &db, stable.segid).await;
        assert_eq!(live, total);
        assert!(total >= super::super::SMALL_SEGMENT_BYTES);
        let jobs = store
            .segment_reclaim_stats()
            .repack_jobs
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..3 {
            let (_, relocated) = reclaim(&store, tokio::time::Instant::now(), false)
                .await
                .unwrap();
            assert_eq!(relocated, 0);
            assert_eq!(frameloc_of(&store, &db, 2, 0).await.unwrap(), stable);
            assert_eq!(
                store
                    .segment_reclaim_stats()
                    .repack_jobs
                    .load(std::sync::atomic::Ordering::Relaxed),
                jobs
            );
        }
        assert_eq!(
            store.read(1, 0, replacement.len() as u64).await.unwrap(),
            replacement
        );
        assert_eq!(store.read(2, 0, second.len() as u64).await.unwrap(), second);
    }

    // Holding the reclamation side must stop repack before it creates the target.
    #[tokio::test]
    async fn repack_acquires_reference_guard_before_sealing() {
        let (store, db) = make().await;

        for (inode, byte) in [(1, 1u8), (2, 2)] {
            let mut txn = db.new_transaction().unwrap();
            store
                .write(&mut txn, inode, 0, &Bytes::from(vec![byte; 1000]), 0)
                .await
                .unwrap();
            commit(&store, txn).await;
            store.seal_open().await.unwrap();
        }

        let seg_a = frameloc_of(&store, &db, 1, 0).await.unwrap().segid;
        let seg_b = frameloc_of(&store, &db, 2, 0).await.unwrap().segid;
        let source_count = store.segments.list_segments().await.unwrap().len();

        let reclaim_guard = store.extent_ref_barrier.clone().write_owned().await;

        let repacking_store = store.clone();
        let mut repack = tokio::spawn(async move {
            repack(&repacking_store, &[seg_a, seg_b], REPACK_JOB_BYTES).await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut repack)
                .await
                .is_err(),
            "repack must block on the reference barrier"
        );
        assert_eq!(
            store.segments.list_segments().await.unwrap().len(),
            source_count,
            "repack sealed a packed segment before taking the reference barrier"
        );

        drop(reclaim_guard);
        let (swapped, _) = repack.await.unwrap().unwrap();
        assert_eq!(swapped, 2);
        for (inode, byte) in [(1, 1u8), (2, 2)] {
            assert_eq!(
                store.read(inode, 0, 1000).await.unwrap().as_ref(),
                &[byte; 1000]
            );
        }
    }

    #[tokio::test]
    async fn repack_relocates_live_frames_and_skips_dead() {
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

        // Repack S: extents 0,2 relocate; extent 1 (already moved off S) is skipped.
        let (swapped, consumed) = repack(&store, &[s], REPACK_JOB_BYTES).await.unwrap();
        assert_eq!(consumed, 1);
        assert_eq!(
            swapped, 2,
            "two live frames relocated, the dead one skipped"
        );

        // S is now fully dead and reclaimable; data is unchanged.
        assert!(
            reclaim(&store, tokio::time::Instant::now(), false)
                .await
                .unwrap()
                .0
                >= 1
        );
        let n = model.len() as u64;
        assert_eq!(
            store.read(1, 0, n).await.unwrap().as_ref(),
            model.as_slice()
        );
    }

    #[tokio::test]
    async fn repack_groups_a_files_extents_for_one_get_reads() {
        let (store, db) = make().await;
        let d = |b: u8| Bytes::from(vec![b; EXTENT_SIZE]);
        // Interleave two files' extents in one open buffer -> one segment whose
        // frames are in write order: A0, B0, A1, B1, A2, B2.
        let mut sizes = [0u64, 0u64];
        for extent in 0..3u64 {
            for (i, inode) in [1u64, 2u64].into_iter().enumerate() {
                committed_write(
                    &store,
                    &db,
                    inode,
                    extent * EXTENT_SIZE as u64,
                    d(inode as u8 * 10 + extent as u8),
                    sizes[i],
                )
                .await;
                sizes[i] = (extent + 1) * EXTENT_SIZE as u64;
            }
        }
        store.seal_open().await.unwrap();
        let s = store.segments.list_segments().await.unwrap();
        assert_eq!(s.len(), 1);

        // Repack -> frames regrouped by (inode, extent); drop the drained source.
        repack(&store, &s, REPACK_JOB_BYTES).await.unwrap();
        reclaim(&store, tokio::time::Instant::now(), false)
            .await
            .unwrap();

        // File A's three extents are now contiguous, so the whole-file read is a
        // single ranged GET, not three.
        let before = store.segments.read_calls();
        let got = store.read(1, 0, 3 * EXTENT_SIZE as u64).await.unwrap();
        assert_eq!(
            store.segments.read_calls() - before,
            1,
            "a file's extents are contiguous after repack -> one GET"
        );
        let expect: Vec<u8> = (0..3u8).flat_map(|c| vec![10 + c; EXTENT_SIZE]).collect();
        assert_eq!(got.as_ref(), expect.as_slice());
    }

    /// Compressed gathering charges stored bytes, so plaintext size alone does
    /// not force work into another job.
    #[tokio::test]
    async fn compressible_sources_pack_in_one_job_under_the_stored_byte_cap() {
        let (store, db) = make().await;
        let mut old = Vec::new();
        for (inode, fill) in [(1u64, 0x11u8), (2, 0x22)] {
            write_chunk_at(&store, &db, inode, 0, Bytes::from(vec![fill; EXTENT_SIZE])).await;
            store.seal_open().await.unwrap();
            old.push(frameloc_of(&store, &db, inode, 0).await.unwrap());
        }
        let budget: u64 = old.iter().map(|loc| loc.byte_len as u64).sum();
        assert!(budget < 2 * EXTENT_SIZE as u64, "frames must compress");

        let segids: Vec<Segid> = old.iter().map(|loc| loc.segid).collect();
        let (relocated, consumed) = repack(&store, &segids, budget).await.unwrap();
        assert_eq!((relocated, consumed), (2, 2));
    }

    /// An exhausted budget still consumes the first source for progress, then
    /// leaves the unconsumed suffix untouched for the caller's scan pool.
    #[tokio::test]
    async fn exhausted_gather_budget_consumes_only_the_first_source() {
        let (store, db) = make().await;
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

        let (relocated, consumed) = repack(&store, &segids, 1).await.unwrap();
        assert_eq!((consumed, relocated), (1, 1));
        assert_ne!(
            frameloc_of(&store, &db, 1, 0).await.unwrap().segid,
            segids[0],
            "the first source progresses despite exceeding the budget"
        );
        for inode in 2u64..5 {
            assert_eq!(
                frameloc_of(&store, &db, inode, 0).await.unwrap().segid,
                segids[(inode - 1) as usize],
                "the unconsumed suffix stays in place"
            );
        }
    }

    #[tokio::test]
    async fn later_singleton_must_fit_the_remaining_gather_budget() {
        let (store, db) = make().await;
        write_chunk_at(&store, &db, 1, 0, Bytes::from(vec![1; 1_000])).await;
        store.seal_open().await.unwrap();
        write_chunk_at(
            &store,
            &db,
            2,
            0,
            Bytes::from(incompressible(2, EXTENT_SIZE)),
        )
        .await;
        store.seal_open().await.unwrap();

        let first = frameloc_of(&store, &db, 1, 0).await.unwrap();
        let second = frameloc_of(&store, &db, 2, 0).await.unwrap();
        let budget = first.byte_len as u64 + second.byte_len as u64 - 1;

        let (relocated, consumed) = repack(&store, &[first.segid, second.segid], budget)
            .await
            .unwrap();
        assert_eq!((relocated, consumed), (1, 1));
        assert_eq!(
            frameloc_of(&store, &db, 2, 0).await.unwrap().segid,
            second.segid
        );
    }

    /// Relocation rebinds AAD without recompressing: Zstd frames retain their
    /// encoded lengths when an Lz4-configured restart repacks them.
    #[tokio::test]
    async fn repack_passes_compressed_payloads_through_across_codec_configs() {
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

        // A restarted store, now configured for Lz4, repacks the zstd world.
        drop(store);
        let store2 = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8).await;
        let (relocated, _) = repack(&store2, &[old_locs[0].segid], REPACK_JOB_BYTES)
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
        let offset = extent_off * EXTENT_SIZE as u64;
        committed_write(store, db, inode, offset, bytes, offset).await;
    }
}
