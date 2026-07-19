//! Batched database writes through a single commit worker.
//!
//! The worker merges queued transactions, inode allocation state, usage
//! counters, segment counters, replication records, and dedup results into one
//! ordered commit.

use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::key_codec::KeyCodec;
use crate::fs::stats::FileSystemGlobalStats;
use crate::fs::store::{ExtentStore, InodeStore};
use crate::replication::ShipOutcome;
use crate::replication::types::SlateDbSeqno;
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

/// Commit worker dependencies.
struct WorkerContext {
    db: Arc<Db>,
    inode_store: InodeStore,
    flush_coordinator: FlushCoordinator,
    key_codec: Arc<KeyCodec>,
    global_stats: Arc<FileSystemGlobalStats>,
    sync_writes: bool,
    /// Replication sequencer for commit-then-apply.
    replicator: Option<crate::replication::Replicator>,
    /// Applied mutation results.
    dedup: Arc<crate::dedup::DedupCache>,
    /// Lineage token stored on the first Solo commit.
    lineage_token: u64,
    /// Data plane used to attach un-PUT segment bytes to replication.
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
        replicator: Option<crate::replication::Replicator>,
        dedup: Arc<crate::dedup::DedupCache>,
        lineage_token: u64,
        extent_store: ExtentStore,
    ) -> Self {
        // Capture before spawning so concurrent allocations cannot advance the
        // worker's initial persisted watermark.
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

    /// Weak commit handle for data-plane GC and compaction.
    pub fn downgrade(&self) -> WeakWriteCoordinator {
        WeakWriteCoordinator(self.sender.downgrade())
    }
}

/// Weak commit handle held by `ExtentStore`; a strong sender would form a cycle.
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

/// Whole-store footprint change for one committed batch.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SegFootprintDelta {
    /// Counters changing from zero to nonzero.
    pub d_segments: i64,
    pub d_appended: i64,
    pub d_live: i64,
}

/// Materialize segment deltas as absolute counters and return their wire values.
/// The commit worker is the sole writer of these keys. `total` is monotonic.
pub(crate) async fn stage_seg_deltas(
    db: &Db,
    deltas: impl IntoIterator<Item = (bytes::Bytes, (i64, i64))>,
    batch: &mut WriteBatch,
) -> Result<(Vec<(bytes::Bytes, bytes::Bytes)>, SegFootprintDelta), FsError> {
    // Preserve deterministic read order.
    let mut agg: BTreeMap<bytes::Bytes, (i64, i64)> = BTreeMap::new();
    for (k, (dl, dt)) in deltas {
        let e = agg.entry(k).or_insert((0, 0));
        e.0 = e.0.saturating_add(dl);
        e.1 = e.1.saturating_add(dt);
    }
    // Missing counters start at zero. Read and decode failures abort the batch;
    // undercounting live bytes can make GC delete referenced data.
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
        // Monitoring deltas use the clamped absolute values and saturating sums.
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
    mut ctx: WorkerContext,
    mut rx: mpsc::UnboundedReceiver<(Transaction, Reply)>,
    initial_counter: u64,
) {
    let mut last_emitted_counter = initial_counter;
    // One durable Solo taint per leader process.
    let mut taint_written = false;
    loop {
        // Base repair shares mutation sequencing. Bias it over queued commits so
        // the flush covers the complete prior receiver prefix.
        let first = match ctx.replicator.as_mut() {
            Some(replicator) => {
                tokio::select! {
                    biased;
                    repair = replicator.next_base_repair() => {
                        let Some(request) = repair else {
                            break;
                        };
                        let required =
                            crate::replication::Replicator::begin_base_repair(replicator);
                        if let Some(required) = required {
                            let through = required.through().get();
                            let receipt = match ctx.flush_coordinator.flush_with_receipt().await {
                                Ok(receipt) => receipt,
                                Err(error) => crate::db::exit_on_write_error(format!(
                                    "HA receiver-base repair through local sequence {through} failed: \
                                     {error}"
                                )),
                            };
                            required.complete(receipt);
                        }
                        let _ = request.send(());
                        continue;
                    }
                    commit = rx.recv() => {
                        let Some(commit) = commit else {
                            break;
                        };
                        commit
                    }
                }
            }
            None => {
                let Some(commit) = rx.recv().await else {
                    break;
                };
                commit
            }
        };
        let mut batch = vec![first];
        while let Ok(msg) = rx.try_recv() {
            batch.push(msg);
        }

        let replicating = ctx.replicator.is_some();
        let mut merged = WriteBatch::new();
        let mut repl_ops: Vec<crate::replication::ReplOp> = Vec::new();
        let mut replies = Vec::with_capacity(batch.len());
        // Pin staged FrameLocs until the merged write resolves.
        let mut extent_ref_guards = Vec::new();
        let mut any_ops = false;
        let mut shard_deltas: HashMap<usize, (i64, i64)> = HashMap::new();
        let mut seg_map: HashMap<bytes::Bytes, (i64, i64)> = HashMap::new();
        let mut batch_dedup_entries: Vec<crate::dedup::DedupEntry> = Vec::new();
        for (mut txn, reply) in batch {
            if let Some(guard) = txn.take_extent_ref_guard() {
                extent_ref_guards.push(guard);
            }
            any_ops |= !txn.is_empty();
            if let Some(entry) = txn.take_dedup_entry() {
                batch_dedup_entries.push(entry);
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
            // `apply_to` consumes operations but not segment deltas.
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

        // Persist the allocation watermark only after it advances.
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

        // The commit worker is the sole segment-counter writer.
        let (seg_abs, footprint_delta) = match stage_seg_deltas(&ctx.db, seg_map, &mut merged).await
        {
            Ok(v) => v,
            Err(e) => {
                // A failed staged counter may have advanced the in-memory inode
                // watermark. Burn one ID so the next commit persists past it.
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

        // Replication carries bytes for referenced segments not yet PUT.
        if replicating {
            repl_ops = ctx.extent_store.enrich_repl_ops(repl_ops);
        }

        // Dedup-only outcomes follow the same ordered replication path.
        let has_logical_work = any_ops || !batch_dedup_entries.is_empty();

        // After Solo operation, the local base must be durable before the first
        // dependent replicated suffix is shipped.
        let mut deposed = false;
        let mut apply_permit = match (has_logical_work, ctx.replicator.as_mut()) {
            (true, Some(repl)) => loop {
                match repl.ship(&repl_ops, &batch_dedup_entries).await {
                    ShipOutcome::Apply(permit) => break Some(permit),
                    ShipOutcome::NeedsBaseFlush(required) => {
                        let through = required.through().get();
                        let receipt = match ctx.flush_coordinator.flush_with_receipt().await {
                            Ok(receipt) => receipt,
                            Err(error) => {
                                // The current batch is unapplied. A base-flush failure
                                // retires the writer before the suffix can ship.
                                crate::db::exit_on_write_error(format!(
                                    "HA replication base through local sequence {through} failed \
                                     to flush before ship retry: {error}"
                                ));
                            }
                        };
                        required.complete(receipt);
                    }
                    ShipOutcome::Deposed => {
                        deposed = true;
                        break None;
                    }
                    ShipOutcome::Poisoned => {
                        crate::db::exit_on_write_error(
                            "HA replication sequencer is poisoned by an unresolved peer copy",
                        );
                    }
                }
            },
            _ => None,
        };
        // Peer rejection proves this batch was not appended or applied locally.
        // Revoke admission before returning CLEAN failures.
        if deposed {
            tracing::error!(
                "HA: standby rejected a ship: this leader is deposed; failing the \
                 batch and stepping down"
            );
            // A stale writer must not flush.
            ctx.db.revoke_lease();
            if counter_staged {
                ctx.inode_store.allocate();
            }
            for reply in replies {
                let _ = reply.send(Err(FsError::LeaderRejectedBeforeApply));
            }
            continue;
        }
        let ran_solo = apply_permit
            .as_ref()
            .is_some_and(|permit| permit.requires_solo_taint());

        // The provenance stamp remains local and independently durable.
        if let Some(permit) = apply_permit.as_ref() {
            merged.put_bytes(
                ctx.key_codec.ha_seqno_key(),
                KeyCodec::encode_ha_stamp(permit.stamp()),
            );
            any_ops = true;
        }

        // SlateDB rejects empty write batches; logical-only work is handled
        // without a database write.
        let mut result: Result<(), FsError> = Ok(());

        // Persist the lineage taint before acknowledging the first Solo write.
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

        let mut local_applied = false;
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
                    local_applied = true;
                    if let Some(permit) = apply_permit.take() {
                        permit.applied(SlateDbSeqno::new(slatedb_seq));
                    }
                }
                Err(_) => result = Err(FsError::IoError),
            }
        } else if result.is_ok() && !batch_dedup_entries.is_empty() {
            // Standalone logical outcomes complete when published to the ledger.
            local_applied = true;
        }
        if local_applied {
            for entry in batch_dedup_entries {
                ctx.dedup.record_entry(entry);
            }
        }

        // A failed local apply with a possible peer copy poisons sequencing.
        if !local_applied
            && let Some(permit) = apply_permit.take()
            && let Err(peer_copy) = permit.failed()
        {
            let error = result.as_ref().err().copied().unwrap_or(FsError::IoError);
            crate::db::exit_on_write_error(format!(
                "HA batch {} may be buffered on the standby but failed local apply: {error}",
                peer_copy.seqno().get()
            ));
        }

        // Publish in-memory counters after local commit.
        if result.is_ok() {
            for shard in &staged {
                ctx.global_stats.publish(shard);
            }
            ctx.extent_store.segment_gc_stats().apply_footprint_delta(
                footprint_delta.d_segments,
                footprint_delta.d_appended,
                footprint_delta.d_live,
            );
        }

        drop(extent_ref_guards);

        // `sync_writes` returns success only after the batch is durable.
        if ctx.sync_writes && result.is_ok() && any_ops {
            result = ctx.flush_coordinator.flush().await;
        }

        // Burn one ID when a failed batch may have dropped its staged watermark.
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

    /// `DST_PANIC_ON_WRITE_ERROR` is process-global, so fatal-path unit tests
    /// must not toggle it concurrently.
    static FATAL_WRITE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct PanicOnFatalWrite(bool);

    impl Drop for PanicOnFatalWrite {
        fn drop(&mut self) {
            crate::db::DST_PANIC_ON_WRITE_ERROR.store(self.0, std::sync::atomic::Ordering::SeqCst);
        }
    }

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
    async fn dedup_only_transaction_publishes_terminal_outcome() {
        let fs = make_fs().await;
        let op_id = [0x5au8; 16];
        let mut txn = Transaction::new();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Error {
                errno: libc::EEXIST as u32,
            },
        );
        assert!(txn.is_empty(), "ledger-only work has no user-data ops");

        fs.write_coordinator.commit(txn).await.unwrap();
        assert!(matches!(
            fs.dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Error { errno })
                if errno == libc::EEXIST as u32
        ));
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

    use crate::replication::transport::{
        ReceiverControl, ReplicationReceiver, ReplicationSender, ShipResult,
    };
    use crate::replication::types::{CoverageFrontier, PruneWatermark, ShipSeqno, WriterEpoch};
    use crate::replication::{ReplOp, Replicator};

    fn writer_epoch(value: u64) -> WriterEpoch {
        WriterEpoch::new(value).expect("test writer epochs are nonzero")
    }

    fn ship_seqno(value: u64) -> ShipSeqno {
        ShipSeqno::new(value).expect("test ship sequence numbers are nonzero")
    }

    fn prune_watermark(value: u64, current: u64) -> PruneWatermark {
        PruneWatermark::for_ship(value, ship_seqno(current))
            .expect("test watermark must precede its ship")
    }

    async fn serve_receiver(receiver: ReplicationReceiver) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = receiver.into_server();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_receiver() -> String {
        serve_receiver(ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "standby-under-test".to_string(),
        ))
        .await
    }

    async fn spawn_receiver_paused_before_append(
        epoch: u64,
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> (String, ReceiverControl) {
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "standby-under-test".to_string(),
        )
        .pause_epoch_before_append(epoch, reached, resume);
        let control = receiver.control();
        (serve_receiver(receiver).await, control)
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

    async fn make_leased_replicating_fs_with_raw(
        lease: Arc<crate::replication::Lease>,
        replicator: Replicator,
    ) -> (ZeroFS, Arc<slatedb::Db>) {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use slatedb::BlockTransformer;

        let test_key = [0u8; 32];
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("ha-apply-failure"),
                object_store.clone(),
            )
            .with_block_transformer(block_transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
        );
        let segment_codec = crate::frame_codec::FrameCodec::new(
            &test_key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        );

        let fs = ZeroFS::new_with_slatedb_and_lease(
            crate::db::SlateDbHandle::ReadWrite(raw_db.clone()),
            u64::MAX,
            None,
            false,
            false,
            Some(lease),
            Some(replicator),
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            crate::object_trace::ObjectTracer::new(),
            object_store,
            segment_codec,
            None,
            None,
        )
        .await
        .unwrap();
        (fs, raw_db)
    }

    /// A second coordinator over the same fs's stores, with a replicator attached.
    fn replicating_coordinator(fs: &ZeroFS, replicator: Replicator) -> WriteCoordinator {
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

    #[tokio::test]
    async fn dedup_only_outcome_ships_and_publishes_on_standby() {
        let standby_dedup = Arc::new(crate::dedup::DedupCache::new());
        let endpoint = serve_receiver(ReplicationReceiver::new(
            standby_dedup.clone(),
            None,
            "dedup-only-standby".to_string(),
        ))
        .await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let fs = make_fs().await;
        let coord = replicating_coordinator(&fs, repl);
        let op_id = [0x6bu8; 16];
        let mut txn = Transaction::new();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Error {
                errno: libc::EEXIST as u32,
            },
        );
        coord.commit(txn).await.unwrap();

        // Watermark 1 publishes sequence 1's retained result.
        assert_eq!(
            connect_sender(&endpoint)
                .await
                .ship(
                    ship_seqno(2),
                    &[],
                    &[],
                    prune_watermark(1, 2),
                    writer_epoch(7),
                )
                .await
                .unwrap(),
            ShipResult::Accepted
        );
        assert!(matches!(
            standby_dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Error { errno })
                if errno == libc::EEXIST as u32
        ));
    }

    #[tokio::test]
    async fn deposal_revokes_without_apply_or_flush() {
        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (replicator, control) = Replicator::new("unused".to_string(), writer_epoch(1));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;
        control.depose().await;

        let key = codec().extent_key(7, 0);
        let flushes_before = fs.flush_coordinator.completed_flush_count();
        let mut txn = Transaction::new();
        txn.put_bytes(&key, Bytes::from_static(b"never-applied"));
        assert_eq!(
            fs.write_coordinator
                .commit(txn)
                .await
                .expect_err("a terminally deposed replicator must reject the batch"),
            FsError::LeaderRejectedBeforeApply
        );

        assert!(!lease.is_valid(), "rejection must close the serving gate");
        assert!(
            raw_db.get(&key).await.unwrap().is_none(),
            "the rejected batch must not reach the local database"
        );
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            flushes_before,
            "deposal must not force-flush a stale database"
        );
    }

    // A standby's rejection is deposal evidence: a newer writer exists, so the
    // new history cannot contain this batch. It must fail, not be applied and
    // acked by the deposed leader.
    #[tokio::test]
    async fn rejected_ship_fails_the_batch_instead_of_acking() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        // A newer leader (epoch 5) shipped first: epoch-1 ships are rejected.
        let newer = connect_sender(&endpoint).await;
        assert_eq!(
            newer
                .ship(
                    ship_seqno(1),
                    &[ReplOp::Put("a".into(), "b".into())],
                    &[],
                    prune_watermark(0, 1),
                    writer_epoch(5),
                )
                .await
                .unwrap(),
            ShipResult::Accepted
        );

        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(1));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);

        let codec = codec();
        let key = codec.extent_key(1, 0);
        let flushes_before = fs.flush_coordinator.completed_flush_count();
        let mut txn = Transaction::new();
        txn.put_bytes(&key, Bytes::from_static(b"v"));
        let error = coord
            .commit(txn)
            .await
            .expect_err("a deposed leader must fail the batch, not ack it");
        assert_eq!(error, FsError::LeaderRejectedBeforeApply);
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            flushes_before,
            "a writer proven stale must not flush before returning the clean failure"
        );
        assert!(
            fs.db.get_bytes(&key).await.unwrap().is_none(),
            "a deposed leader must not apply the rejected batch"
        );

        // Deposal is terminal: later batches fail too.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 1), Bytes::from_static(b"w"));
        assert_eq!(
            coord.commit(txn).await.expect_err("deposal must be sticky"),
            FsError::LeaderRejectedBeforeApply
        );
    }

    /// An acknowledged batch waits through recoverable suspension before local apply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shipped_batch_waits_through_suspension() {
        const EPOCH: u64 = 7;

        let _fatal_test_lock = FATAL_WRITE_TEST_LOCK.lock().await;

        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let (endpoint, control) =
            spawn_receiver_paused_before_append(EPOCH, reached.clone(), resume.clone()).await;
        let (replicator, replication_control) =
            Replicator::new(endpoint.clone(), writer_epoch(EPOCH));
        replication_control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;

        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;
        let codec = codec();
        let first_key = codec.extent_key(90, 0);

        let first_commit = {
            let coordinator = fs.write_coordinator.clone();
            let first_key = first_key.clone();
            tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&first_key, Bytes::from_static(b"first"));
                coordinator.commit(txn).await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("the first ship must reach standby admission");

        // Suspend after peer admission and before peer append.
        lease.suspend_for_tests();
        assert!(!fs.db.permits_successful_response());
        let previous =
            crate::db::DST_PANIC_ON_WRITE_ERROR.swap(true, std::sync::atomic::Ordering::SeqCst);
        let _fatal_guard = PanicOnFatalWrite(previous);
        resume.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let appended = control
                    .inspect_standby_for_tests(|tail, _| !tail.is_empty())
                    .await;
                if appended {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the standby must append the first ship");
        tokio::task::yield_now().await;
        assert!(
            !first_commit.is_finished(),
            "local apply must wait while successful responses are suspended"
        );
        assert!(raw_db.get(&first_key).await.unwrap().is_none());
        assert!(
            raw_db.get(&codec.ha_seqno_key()).await.unwrap().is_none(),
            "a suspended local apply must not publish provenance early"
        );

        assert!(lease.recover_for_tests(std::time::Duration::from_secs(30)));
        tokio::time::timeout(std::time::Duration::from_secs(5), first_commit)
            .await
            .expect("the recovered local apply must complete promptly")
            .expect("the commit caller task must not panic")
            .expect("the acknowledged batch must apply after recovery");

        assert!(
            raw_db.get(&codec.ha_seqno_key()).await.unwrap().is_some(),
            "the recovered local apply must persist its provenance"
        );
        assert_eq!(
            raw_db.get(&first_key).await.unwrap(),
            Some(Bytes::from_static(b"first"))
        );
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| {
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>()
                })
                .await,
            vec![1],
            "the standby must retain the sole acknowledged batch for takeover replay"
        );
    }

    /// A failed Solo-base flush prevents the dependent batch from shipping or applying.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_solo_base_flush_stops_before_reconnect_ship() {
        const EPOCH: u64 = 7;

        let _fatal_test_lock = FATAL_WRITE_TEST_LOCK.lock().await;
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "reconnect-flush-standby".to_string(),
        );
        let receiver_control = receiver.control();
        let endpoint = serve_receiver(receiver).await;
        let (replicator, replication_control) =
            Replicator::new(endpoint.clone(), writer_epoch(EPOCH));
        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;

        // The Solo mutation follows the lineage-taint flush.
        let solo_key = codec().extent_key(91, 0);
        let mut solo = Transaction::new();
        solo.put_bytes(&solo_key, Bytes::from_static(b"solo"));
        fs.write_coordinator.commit(solo).await.unwrap();
        let ha_stamp_key = codec().ha_seqno_key();
        let solo_stamp = fs
            .db
            .get_bytes(&ha_stamp_key)
            .await
            .unwrap()
            .expect("the applied Solo prefix must carry its durable-format HA stamp");
        let requested_before = fs.flush_coordinator.requested_flush_count();

        let barrier = fs.db.flush_barrier().read_owned().await;
        replication_control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let reconnect_key = codec().extent_key(91, 1);
        let op_id = [0x91; 16];
        let reconnect_commit = {
            let coordinator = fs.write_coordinator.clone();
            let reconnect_key = reconnect_key.clone();
            tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&reconnect_key, Bytes::from_static(b"reconnected"));
                txn.set_dedup_result(op_id, crate::dedup::DedupResult::Applied);
                coordinator.commit(txn).await
            })
        };

        // Wait until the worker requests the blocked pre-ship flush.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if fs.flush_coordinator.requested_flush_count() > requested_before {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the reconnect batch must request its pre-ship base flush");
        assert!(
            !reconnect_commit.is_finished(),
            "the reconnect batch must wait behind the Solo-base barrier"
        );
        assert!(
            fs.db.get_bytes(&reconnect_key).await.unwrap().is_none(),
            "the reconnect mutation must not apply before the base flush"
        );
        assert!(
            fs.dedup.get(&op_id).is_none(),
            "an unshipped, unapplied mutation cannot publish its exact result"
        );
        assert!(
            receiver_control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "the reconnect mutation must not reach the standby before the base flush"
        );
        assert_eq!(
            fs.db.get_bytes(&ha_stamp_key).await.unwrap(),
            Some(solo_stamp.clone()),
            "the blocked reconnect must leave the database at the applied Solo prefix"
        );

        let previous =
            crate::db::DST_PANIC_ON_WRITE_ERROR.swap(true, std::sync::atomic::Ordering::SeqCst);
        let _fatal_guard = PanicOnFatalWrite(previous);
        lease.revoke();
        drop(barrier);

        tokio::time::timeout(std::time::Duration::from_secs(5), reconnect_commit)
            .await
            .expect("the failed Solo-base flush must terminate promptly")
            .expect("the commit caller task must not panic")
            .expect_err("fatal commit-worker exit must drop the reply");
        assert!(
            fs.dedup.get(&op_id).is_none(),
            "a failed pre-ship flush leaves the result unpublished"
        );
        assert!(
            receiver_control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "a failed base flush must leave the standby untouched"
        );
        assert_eq!(
            raw_db.get(&ha_stamp_key).await.unwrap(),
            Some(solo_stamp),
            "a failed base flush must not persist provenance for the reconnect batch"
        );

        lease.renew(std::time::Duration::from_secs(30));
        let mut later = Transaction::new();
        later.put_bytes(&codec().extent_key(91, 2), Bytes::from_static(b"later"));
        fs.write_coordinator
            .commit(later)
            .await
            .expect_err("the fatal Solo-base flush must leave the worker dead");
    }

    /// The first post-Solo ship follows a flush of its local base.
    #[tokio::test]
    async fn solo_base_is_flushed_before_the_first_reconnect_ship() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);
        let codec = codec();

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 0), Bytes::from_static(b"a"));
        coord.commit(txn).await.unwrap();
        let baseline = fs.flush_coordinator.completed_flush_count();

        control.set_sender_for_tests(None).await;
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

        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let reconnect_op_id = [0xa7; 16];
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 3), Bytes::from_static(b"c"));
        txn.set_dedup_result(reconnect_op_id, crate::dedup::DedupResult::Applied);
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "the Solo base must be forced durable before the first reconnect ship"
        );
        assert!(matches!(
            fs.dedup.get(&reconnect_op_id),
            Some(crate::dedup::DedupResult::Applied)
        ));

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 4), Bytes::from_static(b"d"));
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "steady-state shipped batches must not force a flush"
        );
    }

    #[tokio::test]
    async fn idle_receiver_repair_wakes_commit_worker() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);

        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(1, 0), Bytes::from_static(b"acked"));
        coord.commit(txn).await.unwrap();
        let baseline = fs.flush_coordinator.completed_flush_count();
        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(ship_seqno(1)), None).unwrap()
        );

        // Request repair while the commit queue is idle.
        control.repair_base().await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 1,
            "an idle receiver replacement must actively repair the acknowledged base"
        );
        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(ship_seqno(1)), Some(ship_seqno(1))).unwrap()
        );
    }

    /// Reconnect-base flush preserves staged extent publication and readability.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_flush_handles_staged_extent() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        let coord = replicating_coordinator(&fs, repl);
        let baseline = fs.flush_coordinator.completed_flush_count();

        let mut solo = Transaction::new();
        let solo_tail = fs
            .extent_store
            .write(&mut solo, 41, 0, &Bytes::from_static(b"solo"), 0)
            .await
            .unwrap();
        coord.commit(solo).await.unwrap();
        fs.extent_store.apply_tail_update(41, solo_tail);

        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let mut reconnect = Transaction::new();
        let reconnect_tail = fs
            .extent_store
            .write(
                &mut reconnect,
                41,
                4,
                &Bytes::from_static(b"-reconnected"),
                4,
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), coord.commit(reconnect))
            .await
            .expect("the base flush must not deadlock on the extent publication guard")
            .unwrap();
        fs.extent_store.apply_tail_update(41, reconnect_tail);

        assert_eq!(
            fs.extent_store.read(41, 0, 16).await.unwrap().as_ref(),
            b"solo-reconnected"
        );
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "the Solo taint and reconnect base each cross one durability barrier"
        );
    }
}
