//! Commit coalescer: batched DB writes through a single worker task.
//!
//! Each `commit(txn)` call sends the transaction through an mpsc channel to one
//! worker task. The worker drains all currently-queued messages, merges their
//! ops into a single `WriteBatch`, attaches a `system_counter_key` write if the
//! inode counter advanced since the last emission, submits one
//! `db.write_with_options`, and replies to every caller in the batch.
//!
//! The worker also materializes usage stats: transactions carry signed
//! per-inode deltas, the worker aggregates them per shard across the batch,
//! writes the resulting absolute shard values in the same batch, and publishes
//! them to the in-memory counters once the write succeeds. Being the single
//! writer of those counters is what makes this lock-free.

use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::key_codec::KeyCodec;
use crate::fs::stats::FileSystemGlobalStats;
use crate::fs::store::{ExtentStore, InodeStore};
use crate::replication::ShipOutcome;
use crate::task::spawn_named;
use futures::stream::{self, StreamExt, TryStreamExt};
use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Concurrency for the per-segment `segcount` base reads in [`stage_seg_deltas`].
const PARALLEL_SEGCOUNT_READS: usize = 16;

/// One segment's staged counter update read: (segcount key, `(live_delta,
/// total_delta)` netted for this batch, `(current_live, current_total)` base).
type SegBase = (bytes::Bytes, (i64, i64), (u64, u64));

type Reply = oneshot::Sender<Result<(), FsError>>;

#[derive(Clone)]
pub struct WriteCoordinator {
    sender: mpsc::UnboundedSender<(Transaction, Reply)>,
}

/// Everything the commit worker needs besides its channel.
struct WorkerContext {
    db: Arc<Db>,
    inode_store: InodeStore,
    flush_coordinator: FlushCoordinator,
    key_codec: Arc<KeyCodec>,
    global_stats: Arc<FileSystemGlobalStats>,
    sync_writes: bool,
    /// Present on a leader with a standby: each batch is shipped and durably
    /// acked here before it is applied (commit-then-apply). `None` for a
    /// single-node or a solo (post-failover) node, which applies directly.
    replicator: Option<Arc<crate::replication::Replicator>>,
    /// Idempotency cache: applied op-ids are recorded here atomically with the
    /// commit, so a retry of the same op is recognized as already done.
    dedup: Arc<crate::dedup::DedupCache>,
    /// This leader's durability lineage token, written as the Solo taint on the
    /// first downgrade to Solo (see the taint write in `worker_loop`).
    lineage_token: u64,
    /// The data plane, to attach un-PUT segments' bytes to replicated extent writes
    /// so the standby can materialize them on takeover.
    extent_store: ExtentStore,
}

impl WriteCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Arc<Db>,
        inode_store: InodeStore,
        flush_coordinator: FlushCoordinator,
        key_codec: Arc<KeyCodec>,
        global_stats: Arc<FileSystemGlobalStats>,
        sync_writes: bool,
        replicator: Option<Arc<crate::replication::Replicator>>,
        dedup: Arc<crate::dedup::DedupCache>,
        lineage_token: u64,
        extent_store: ExtentStore,
    ) -> Self {
        // Capture synchronously: the spawned task starts later, by which point
        // callers may already have bumped `next_id`. If we captured inside the
        // task we'd over-shoot and skip the first emit.
        let initial_counter = inode_store.next_id();
        let (sender, receiver) = mpsc::unbounded_channel();
        let ctx = WorkerContext {
            db,
            inode_store,
            flush_coordinator,
            key_codec,
            global_stats,
            sync_writes,
            replicator,
            dedup,
            lineage_token,
            extent_store,
        };
        spawn_named("commit-worker", worker_loop(ctx, receiver, initial_counter));
        Self { sender }
    }

    pub async fn commit(&self, txn: Transaction) -> Result<(), FsError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send((txn, reply_tx))
            .map_err(|_| FsError::IoError)?;
        reply_rx.await.map_err(|_| FsError::IoError)?
    }

    /// A weak handle for the data plane's GC/compaction paths, so their
    /// seg-delta-bearing txns route through this single writer.
    pub fn downgrade(&self) -> WeakWriteCoordinator {
        WeakWriteCoordinator(self.sender.downgrade())
    }
}

/// A weak handle to the commit worker held by `ExtentStore`. Weak (not a sender
/// clone) so it can't keep the worker's receiver alive: the worker owns an
/// `ExtentStore` clone, so a strong sender there would be a cycle the channel
/// could never close.
#[derive(Clone)]
pub struct WeakWriteCoordinator(mpsc::WeakUnboundedSender<(Transaction, Reply)>);

impl WeakWriteCoordinator {
    pub async fn commit(&self, txn: Transaction) -> Result<(), FsError> {
        let sender = self.0.upgrade().ok_or(FsError::IoError)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        sender.send((txn, reply_tx)).map_err(|_| FsError::IoError)?;
        reply_rx.await.map_err(|_| FsError::IoError)?
    }
}

/// The net whole-store footprint change of one staged batch, folded into the
/// monitor's segment gauges once the batch commits. `stage_seg_deltas` has the
/// old and new absolute counter values in hand, so it reports the exact deltas
/// for free, which is what lets the gauges track writes without a scan.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SegFootprintDelta {
    /// New segments first credited in this batch (a counter going from absent to nonzero).
    pub d_segments: i64,
    pub d_appended: i64,
    pub d_live: i64,
}

/// Materialize per-segment `(live, total)` deltas as absolute `segcount`
/// values: aggregate per key, RMW-read the current value, write the new
/// absolute into `batch`, and return the `(key, value)` pairs so a replicating
/// caller can ship them, plus the batch's net footprint delta. `total` only
/// ever credits (a debit carries `total_delta == 0`). Correctness rests on the
/// caller being the sole writer of these keys, so the RMW never races.
pub(crate) async fn stage_seg_deltas(
    db: &Db,
    deltas: impl IntoIterator<Item = (bytes::Bytes, (i64, i64))>,
    batch: &mut WriteBatch,
) -> Result<(Vec<(bytes::Bytes, bytes::Bytes)>, SegFootprintDelta), FsError> {
    // Ordered so the read fan-out below issues in a deterministic sequence.
    let mut agg: BTreeMap<bytes::Bytes, (i64, i64)> = BTreeMap::new();
    for (k, (dl, dt)) in deltas {
        let e = agg.entry(k).or_insert((0, 0));
        e.0 = e.0.saturating_add(dl);
        e.1 = e.1.saturating_add(dt);
    }
    // Read each segment's base concurrently: a large overwrite can touch many
    // old segments, and a serial RMW would be O(#segments) round trips. Still
    // race-free — this worker is the keys' sole writer. A failed read — or a
    // present-but-undecodable value — must fail the batch, not default to 0:
    // an undercounted live segment is one GC can delete (data loss). Only a
    // genuinely absent key starts at 0.
    let bases: Vec<SegBase> = stream::iter(agg)
        .map(|(key, net)| async move {
            match db.get_bytes(&key).await {
                Ok(None) => Ok((key, net, (0, 0))),
                Ok(Some(b)) => KeyCodec::decode_segcount(&b)
                    .map(|base| (key, net, base))
                    .ok_or(FsError::IoError),
                Err(_) => Err(FsError::IoError),
            }
        })
        .buffer_unordered(PARALLEL_SEGCOUNT_READS)
        .try_collect()
        .await?;

    let mut out = Vec::with_capacity(bases.len());
    let mut fd = SegFootprintDelta::default();
    for (key, (net_live, net_total), (cur_live, cur_total)) in bases {
        let live = (cur_live as i128 + net_live as i128).max(0) as u64;
        // `total` is monotonic: clamp to at least its current value.
        let total = (cur_total as i128 + net_total as i128).max(cur_total as i128) as u64;
        // Exact contribution to the whole-store footprint, post-clamp. A
        // counter going from absent/zero to a first credit is a new segment.
        // Saturating so a display gauge can never panic the commit worker.
        fd.d_live = fd.d_live.saturating_add(live as i64 - cur_live as i64);
        fd.d_appended = fd
            .d_appended
            .saturating_add(total as i64 - cur_total as i64);
        fd.d_segments = fd
            .d_segments
            .saturating_add((cur_total == 0 && total > 0) as i64);
        let val = KeyCodec::encode_segcount(live, total);
        batch.put_bytes(key.clone(), val.clone());
        out.push((key, val));
    }
    Ok((out, fd))
}

async fn worker_loop(
    ctx: WorkerContext,
    mut rx: mpsc::UnboundedReceiver<(Transaction, Reply)>,
    initial_counter: u64,
) {
    let mut last_emitted_counter = initial_counter;
    // A lineage is tainted at most once per leader process.
    let mut taint_written = false;
    // Cleared after the first post-solo shipped batch is flushed.
    let mut reconnect_flush_pending = false;

    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(msg) = rx.try_recv() {
            batch.push(msg);
        }

        let replicating = ctx.replicator.is_some();
        let mut merged = WriteBatch::new();
        // Collected only when replicating, to ship the batch to the standby.
        let mut repl_ops: Vec<crate::replication::ReplOp> = Vec::new();
        let mut replies = Vec::with_capacity(batch.len());
        // Pin staged FrameLocs until the merged write resolves.
        let mut extent_ref_guards = Vec::new();
        let mut any_ops = false;
        let mut shard_deltas: HashMap<usize, (i64, i64)> = HashMap::new();
        // Per-segment (live, total) deltas, aggregated per key across the batch.
        let mut seg_map: HashMap<bytes::Bytes, (i64, i64)> = HashMap::new();
        // Op-ids carried by this batch's transactions, recorded on apply.
        let mut batch_op_ids: Vec<crate::dedup::OpId> = Vec::new();
        for (mut txn, reply) in batch {
            if let Some(guard) = txn.take_extent_ref_guard() {
                extent_ref_guards.push(guard);
            }
            any_ops |= !txn.is_empty();
            let oid = txn.op_id();
            if crate::dedup::has_op_id(&oid) {
                batch_op_ids.push(oid);
            }
            for delta in txn.take_stats_deltas() {
                let entry = shard_deltas
                    .entry(ctx.global_stats.shard_of(delta.inode_id))
                    .or_default();
                // Saturating: individual deltas are already clamped to the
                // i64 range, so a batch of multi-EiB deltas could overflow a
                // plain `+=` and panic the singleton worker.
                entry.0 = entry.0.saturating_add(delta.bytes);
                entry.1 = entry.1.saturating_add(delta.inodes);
            }
            // Segment counter deltas: aggregate per key before apply_to consumes the
            // txn (apply_to merges only the ops and drops the deltas).
            for (key, (dl, dt)) in txn.take_seg_deltas() {
                let e = seg_map.entry(key).or_default();
                e.0 = e.0.saturating_add(dl);
                e.1 = e.1.saturating_add(dt);
            }
            if replicating {
                repl_ops.extend(txn.apply_to_collecting(&mut merged));
            } else {
                txn.apply_to(&mut merged);
            }
            replies.push(reply);
        }

        // Emit the inode counter only if `next_id` actually advanced since the
        // last emission. See the module doc for why this value is always a safe
        // upper bound on every inode id in this batch.
        let current = ctx.inode_store.next_id();
        let counter_staged = current > last_emitted_counter;
        if counter_staged {
            let counter_key = ctx.key_codec.system_counter_key();
            let counter_value = KeyCodec::encode_counter(current);
            if replicating {
                repl_ops.push(crate::replication::ReplOp::Put(
                    counter_key.clone(),
                    counter_value.clone(),
                ));
            }
            merged.put_bytes(counter_key, counter_value);
            last_emitted_counter = current;
            any_ops = true;
        }

        let staged: Vec<_> = shard_deltas
            .into_iter()
            .map(|(shard_id, (bytes, inodes))| {
                ctx.global_stats.stage_delta(shard_id, bytes, inodes)
            })
            .collect();
        for shard in &staged {
            if replicating {
                repl_ops.push(crate::replication::ReplOp::Put(
                    shard.key.clone(),
                    shard.value.clone(),
                ));
            }
            merged.put_bytes(shard.key.clone(), shard.value.clone());
            any_ops = true;
        }

        // Segment live-byte counters: this single worker is their sole writer, so an
        // absolute RMW off the DB (memtable-consistent, never a cold 0 for a segment
        // touched this session) is exact and lock-free.
        let (seg_abs, footprint_delta) = match stage_seg_deltas(&ctx.db, seg_map, &mut merged).await
        {
            Ok(v) => v,
            Err(e) => {
                // A segcount base read failed; committing with a guessed base
                // risks data loss (see stage_seg_deltas). Abort the whole batch
                // before shipping or committing. If the dropped batch staged a
                // counter emission, `last_emitted_counter` is now ahead of
                // anything persisted and no later batch would re-emit it
                // (`current > last_emitted_counter` stays false), so a restart
                // could re-allocate ids that already own durable inode
                // records. Burn one id: `next_id` moves past the watermark and
                // the next committed batch emits a covering value.
                if counter_staged {
                    ctx.inode_store.allocate();
                }
                for reply in replies {
                    let _ = reply.send(Err(e));
                }
                continue;
            }
        };
        if !seg_abs.is_empty() {
            any_ops = true;
            if replicating {
                for (key, val) in seg_abs {
                    repl_ops.push(crate::replication::ReplOp::Put(key, val));
                }
            }
        }

        // Attach the sealed bytes of any still-un-PUT segment an extent write
        // points at, so the standby can materialize it on takeover.
        if replicating {
            repl_ops = ctx.extent_store.enrich_repl_ops(repl_ops);
        }

        // Commit-then-apply while connected. Solo batches still commit with
        // single-node durability.
        let ship_outcome = match (any_ops, ctx.replicator.as_ref()) {
            (true, Some(repl)) => Some(repl.ship(&repl_ops, &batch_op_ids).await),
            _ => None,
        };
        // Rejection is deposal evidence. Fail the batch and force a manifest
        // update so fencing closes the database without waiting for its poller.
        let (shipped_seqno, ran_solo) = match ship_outcome {
            Some(ShipOutcome::Deposed) => {
                tracing::error!(
                    "HA: standby rejected a ship: this leader is deposed; failing the \
                     batch and stepping down"
                );
                let _ = ctx.flush_coordinator.flush().await;
                if counter_staged {
                    ctx.inode_store.allocate();
                }
                for reply in replies {
                    let _ = reply.send(Err(FsError::IoError));
                }
                continue;
            }
            Some(ShipOutcome::Shipped(seqno)) => (Some(seqno), false),
            Some(ShipOutcome::Solo) => (None, true),
            None => (None, false),
        };
        if ran_solo {
            reconnect_flush_pending = true;
        }

        // HA provenance stamp, flushed atomically with the batch: (epoch, last
        // acked ship's seqno, solo commits since). On takeover a promoted
        // standby validates its buffered tail against this durable head: prune
        // to the shipped watermark, and drop ships the leader outlived (solo
        // progress after them means replaying would regress newer state). Local
        // only (not in repl_ops); the standby keys its tail by its own seqnos.
        if let Some(repl) = ctx.replicator.as_ref()
            && any_ops
        {
            let (epoch, last_shipped, solo) = repl.stamp();
            merged.put_bytes(
                ctx.key_codec.ha_seqno_key(),
                KeyCodec::encode_ha_stamp(epoch, last_shipped, solo),
            );
        }

        // SlateDB rejects empty WriteBatches. A batch can be empty when every
        // queued txn was a no-op (e.g. a sub-extent trim against a fully sparse
        // region produces no extent writes) and no inode was allocated. Reply
        // Ok without touching the db; there's nothing to make durable either.
        let mut result: Result<(), FsError> = Ok(());

        // First Solo batch: durably taint this lineage before acking any Solo write.
        // A takeover reads the taint and regenerates the lineage token, so these
        // un-shipped writes' later fsync fails honestly instead of matching a
        // carried-forward token. One flush per leader, gated by `taint_written`. If
        // it can't be made durable we fail the batch rather than ack a write we
        // cannot stand behind.
        if !taint_written && ran_solo {
            match ctx
                .db
                .put_with_options(
                    &ctx.key_codec.taint_key(),
                    &KeyCodec::encode_u64(ctx.lineage_token),
                    &slatedb::config::PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(()) => match ctx.flush_coordinator.flush().await {
                    Ok(()) => taint_written = true,
                    Err(e) => result = Err(e),
                },
                Err(_) => result = Err(FsError::IoError),
            }
        }

        if any_ops && result.is_ok() {
            match ctx
                .db
                .write_with_options(
                    merged,
                    &WriteOptions {
                        await_durable: false,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(slatedb_seq) => {
                    // Op-ids recorded atomically with the commit (dedup invariant).
                    for oid in &batch_op_ids {
                        ctx.dedup.record(*oid, bytes::Bytes::new());
                    }
                    if let (Some(repl), Some(batch_seqno)) =
                        (ctx.replicator.as_ref(), shipped_seqno)
                    {
                        repl.mark_applied(batch_seqno, slatedb_seq);
                    }
                }
                Err(_) => result = Err(FsError::IoError),
            }
        }

        // Publish on write success without waiting for the sync flush below:
        // a flush failure means the db is broken (SlateDB retries flushes
        // indefinitely), not that the batch was lost.
        if result.is_ok() {
            for shard in &staged {
                ctx.global_stats.publish(shard);
            }
            // Same durability point as the stats publish: fold this batch's net
            // segment footprint into the monitor gauges now that it is committed.
            ctx.extent_store.segment_gc_stats().apply_footprint_delta(
                footprint_delta.d_segments,
                footprint_delta.d_appended,
                footprint_delta.d_live,
            );
        }

        drop(extent_ref_guards);

        // In sync_writes mode, force a flush after the batch is committed so
        // every caller in the batch only sees Ok once their data is durable in
        // object storage. FlushCoordinator coalesces concurrent flush requests
        // into a single db.flush(). The first shipped batch after a solo episode
        // must flush too: its stamp (solo=0) supersedes the durable solo>0 one,
        // and acking before it is durable would let a crash-then-takeover
        // discard this acked batch with the outlived tail.
        let reconnect_flush = shipped_seqno.is_some() && reconnect_flush_pending;
        if (ctx.sync_writes || reconnect_flush) && result.is_ok() && any_ops {
            result = ctx.flush_coordinator.flush().await;
            if result.is_ok() && reconnect_flush {
                reconnect_flush_pending = false;
            }
        }

        // Same watermark hole as the seg-delta abort above, for the failures
        // below it (lost lease, taint flush): a failed batch may have dropped
        // its staged counter emission. Burning an id when the counter may not
        // have been applied is always safe — over-emission only wastes ids.
        if counter_staged && result.is_err() {
            ctx.inode_store.allocate();
        }
        for reply in replies {
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use bytes::Bytes;

    async fn make_fs() -> ZeroFS {
        ZeroFS::new_in_memory().await.unwrap()
    }

    fn codec() -> KeyCodec {
        KeyCodec::new()
    }

    #[tokio::test]
    async fn commits_single_transaction() {
        let fs = make_fs().await;
        let mut txn = Transaction::new();
        // Use a real codec-built key so the segment extractor accepts it.
        let key = codec().extent_key(1, 0);
        txn.put_bytes(&key, Bytes::from_static(b"value"));
        fs.write_coordinator.commit(txn).await.unwrap();
        let v = fs.db.get_bytes(&key).await.unwrap();
        assert_eq!(v.as_deref(), Some(&b"value"[..]));
    }

    #[tokio::test]
    async fn coalesces_concurrent_commits() {
        let fs = make_fs().await;
        let coord = fs.write_coordinator.clone();
        let codec = codec();
        let mut handles = Vec::new();
        for i in 0u64..32 {
            let c = coord.clone();
            let k = codec.extent_key(1, i);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&k, Bytes::from(vec![1u8; 8]));
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for i in 0u64..32 {
            let v = fs.db.get_bytes(&codec.extent_key(1, i)).await.unwrap();
            assert!(v.is_some());
        }
    }

    #[tokio::test]
    async fn sync_writes_commits_persist() {
        let fs = ZeroFS::new_in_memory_with_sync_writes(true).await.unwrap();
        let coord = fs.write_coordinator.clone();
        let codec = codec();
        let mut handles = Vec::new();
        for i in 0u64..16 {
            let c = coord.clone();
            let k = codec.extent_key(2, i);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&k, Bytes::from(vec![2u8; 8]));
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for i in 0u64..16 {
            let v = fs.db.get_bytes(&codec.extent_key(2, i)).await.unwrap();
            assert!(
                v.is_some(),
                "extent {i} not durable after sync_writes commit"
            );
        }
    }

    #[tokio::test]
    async fn empty_transaction_is_noop_not_fatal() {
        // SlateDB rejects empty WriteBatches with "empty write batch not
        // allowed". A no-op txn (e.g. sub-extent trim on a fully sparse range)
        // must short-circuit before reaching the db.
        let fs = make_fs().await;
        let txn = Transaction::new();
        assert!(txn.is_empty());
        fs.write_coordinator
            .commit(txn)
            .await
            .expect("empty txn should commit as a no-op");
    }

    #[tokio::test]
    async fn counter_emitted_only_when_advanced() {
        let fs = make_fs().await;
        let counter_key = codec().system_counter_key();
        let before = fs.db.get_bytes(&counter_key).await.unwrap();

        // A commit that doesn't allocate any inode.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(3, 0), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_eq!(
            before, after,
            "counter key should not change without allocate"
        );

        // Now allocate and commit; counter must advance on disk.
        let _id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(3, 1), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after_allocate = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_ne!(
            after, after_allocate,
            "counter key should advance after allocate"
        );
    }

    #[tokio::test]
    async fn corrupt_segcount_value_fails_the_batch() {
        let fs = make_fs().await;
        let codec = codec();
        let seg_key = codec.segcount_key(1, 1);
        // Five bytes: neither the 16-byte encoding nor the legacy 8-byte one.
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(7, 0), Bytes::from_static(b"v"));
        txn.add_seg_delta(&seg_key, 5, 5);
        fs.write_coordinator
            .commit(txn)
            .await
            .expect_err("a corrupt segcount base must abort the batch, not default to 0");
    }

    #[tokio::test]
    async fn counter_reemitted_after_aborted_batch() {
        let fs = make_fs().await;
        let codec = codec();
        let seg_key = codec.segcount_key(1, 2);
        // Corrupt segcount base so the seg-delta-bearing batch aborts.
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        // Allocate so the aborting batch stages a counter emission, then lose
        // that batch (and the staged counter put) to the segcount abort.
        let id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(id, 0), Bytes::from_static(b"v"));
        txn.add_seg_delta(&seg_key, 5, 5);
        fs.write_coordinator.commit(txn).await.unwrap_err();

        // A later batch with no new allocation must still emit a counter
        // covering `id` (via the id burned on abort); otherwise a restart
        // would hand out `id` again over this batch's durable records.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(id, 1), Bytes::from_static(b"w"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let persisted = fs
            .db
            .get_bytes(&codec.system_counter_key())
            .await
            .unwrap()
            .map(|b| KeyCodec::decode_counter(&b).unwrap())
            .unwrap_or(0);
        assert!(
            persisted > id,
            "persisted counter {persisted} must cover allocated id {id}"
        );
    }

    #[tokio::test]
    async fn stats_deltas_aggregate_per_shard_across_batches() {
        use crate::fs::STATS_SHARDS;
        use crate::fs::stats::StatsShardData;

        let fs = make_fs().await;
        let coord = fs.write_coordinator.clone();
        let codec = codec();

        // All inode ids congruent to 5 mod 100 map to stats shard 5.
        const SHARD: usize = 5;
        const TASKS: u64 = 32;

        let mut handles = Vec::new();
        for k in 0..TASKS {
            let c = coord.clone();
            let inode_id = SHARD as u64 + 100 * k;
            let key = codec.extent_key(inode_id, 0);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&key, Bytes::from_static(b"x"));
                txn.add_stats_delta(inode_id, ((k + 1) * 10) as i64, 1);
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        // 10 + 20 + ... + 320 = 5280
        let expected_bytes: u64 = (1..=TASKS).map(|k| k * 10).sum();
        assert_eq!(fs.global_stats.get_totals(), (expected_bytes, TASKS));

        let raw = fs
            .db
            .get_bytes(&codec.stats_shard_key(SHARD))
            .await
            .unwrap()
            .expect("shard 5 must be persisted");
        let shard: StatsShardData = bincode::deserialize(&raw).unwrap();
        assert_eq!(
            (shard.used_bytes, shard.used_inodes),
            (expected_bytes, TASKS)
        );

        // No other shard key may have been written.
        for i in 0..STATS_SHARDS {
            if i != SHARD {
                assert!(
                    fs.db
                        .get_bytes(&codec.stats_shard_key(i))
                        .await
                        .unwrap()
                        .is_none(),
                    "shard {i} written without any delta for it"
                );
            }
        }

        // Second wave: negative byte deltas must drain the shard back down,
        // both persisted and in memory.
        let mut handles = Vec::new();
        for k in 0..TASKS {
            let c = coord.clone();
            let inode_id = SHARD as u64 + 100 * k;
            let key = codec.extent_key(inode_id, 1);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&key, Bytes::from_static(b"y"));
                txn.add_stats_delta(inode_id, -(((k + 1) * 10) as i64), 0);
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(fs.global_stats.get_totals(), (0, TASKS));
        let raw = fs
            .db
            .get_bytes(&codec.stats_shard_key(SHARD))
            .await
            .unwrap()
            .unwrap();
        let shard: StatsShardData = bincode::deserialize(&raw).unwrap();
        assert_eq!((shard.used_bytes, shard.used_inodes), (0, TASKS));
    }

    use crate::replication::transport::{ReplicationReceiver, ReplicationSender};
    use crate::replication::{ReplOp, Replicator, TailBuffer};

    async fn spawn_receiver() -> (String, Arc<tokio::sync::Mutex<TailBuffer>>) {
        let buffer = Arc::new(tokio::sync::Mutex::new(TailBuffer::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = ReplicationReceiver::new(
            buffer.clone(),
            Arc::new(crate::dedup::DedupCache::new(64)),
            None,
            "standby-under-test".to_string(),
        )
        .into_server();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (format!("http://{addr}"), buffer)
    }

    async fn connect_sender(endpoint: &str) -> ReplicationSender {
        for _ in 0..100 {
            if let Ok(s) = ReplicationSender::connect(endpoint.to_string()).await {
                return s;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("could not connect to receiver");
    }

    /// A second coordinator over the same fs's stores, with a replicator attached.
    fn replicating_coordinator(fs: &ZeroFS, replicator: Arc<Replicator>) -> WriteCoordinator {
        WriteCoordinator::new(
            fs.db.clone(),
            fs.inode_store.clone(),
            fs.flush_coordinator.clone(),
            Arc::new(KeyCodec::new()),
            fs.global_stats.clone(),
            false,
            Some(replicator),
            fs.dedup.clone(),
            fs.lineage_token,
            fs.extent_store.clone(),
        )
    }

    // A standby's rejection is deposal evidence: a newer writer exists, so the
    // new history cannot contain this batch. It must fail, not be applied and
    // acked by the deposed leader.
    #[tokio::test]
    async fn rejected_ship_fails_the_batch_instead_of_acking() {
        let fs = make_fs().await;
        let (endpoint, _buffer) = spawn_receiver().await;
        // A newer leader (epoch 5) shipped first: epoch-1 ships are rejected.
        let newer = connect_sender(&endpoint).await;
        assert!(
            newer
                .ship(1, &[ReplOp::Put("a".into(), "b".into())], &[], 0, 5)
                .await
                .unwrap()
        );

        let repl = Replicator::new(endpoint.clone(), 1);
        repl.set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);

        let codec = codec();
        let key = codec.extent_key(1, 0);
        let mut txn = Transaction::new();
        txn.put_bytes(&key, Bytes::from_static(b"v"));
        coord
            .commit(txn)
            .await
            .expect_err("a deposed leader must fail the batch, not ack it");
        assert!(
            fs.db.get_bytes(&key).await.unwrap().is_none(),
            "a deposed leader must not apply the rejected batch"
        );

        // Deposal is terminal: later batches fail too.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 1), Bytes::from_static(b"w"));
        coord.commit(txn).await.expect_err("deposal must be sticky");
    }

    // The first shipped batch after a solo episode carries the stamp (solo=0)
    // that supersedes the durable solo>0 one. It must be durable before the ack:
    // a crash before the flush leaves the durable head stamped solo>0, and the
    // takeover replay guard would discard the acked batch with the tail.
    #[tokio::test]
    async fn first_shipped_batch_after_a_solo_episode_is_flushed_before_ack() {
        let fs = make_fs().await;
        let (endpoint, _buffer) = spawn_receiver().await;
        let repl = Replicator::new(endpoint.clone(), 7);
        repl.set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl.clone());
        let codec = codec();

        // Connected write: no forced flush.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 0), Bytes::from_static(b"a"));
        coord.commit(txn).await.unwrap();
        let baseline = fs.flush_coordinator.completed_flush_count();

        // Standby outage: solo writes (the first durably taints the lineage).
        repl.set_sender_for_tests(None).await;
        for i in 1..=2u64 {
            let mut txn = Transaction::new();
            txn.put_bytes(&codec.extent_key(1, i), Bytes::from_static(b"s"));
            coord.commit(txn).await.unwrap();
        }
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 1,
            "the solo episode forces exactly the one-time taint flush"
        );

        // Reconnect: the next shipped batch must flush before its ack.
        repl.set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 3), Bytes::from_static(b"c"));
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "the first post-solo shipped batch must be forced durable before the ack"
        );

        // Steady state again: no flush per shipped batch.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 4), Bytes::from_static(b"d"));
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "steady-state shipped batches must not force a flush"
        );
    }
}
