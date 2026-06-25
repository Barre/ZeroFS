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
use crate::fs::store::InodeStore;
use crate::task::spawn_named;
use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
    /// acked here BEFORE it is applied (commit-then-apply). `None` for a
    /// single-node or a solo (post-failover) node, which applies directly.
    replicator: Option<Arc<crate::replication::Replicator>>,
    /// Idempotency cache: applied op-ids are recorded here atomically with the
    /// commit, so a retry of the same op is recognized as already done.
    dedup: Arc<crate::dedup::DedupCache>,
    /// This leader's durability lineage token, written as the Solo taint on the
    /// first downgrade to Solo (see the taint write in `worker_loop`).
    lineage_token: u64,
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
}

async fn worker_loop(
    ctx: WorkerContext,
    mut rx: mpsc::UnboundedReceiver<(Transaction, Reply)>,
    initial_counter: u64,
) {
    let mut last_emitted_counter = initial_counter;
    // Set once this leader first downgrades to Solo and durably taints its lineage.
    // The current lineage is always untainted at bring-up (we never carry forward a
    // tainted one), so it starts false; once set it stays set for this process's
    // life — a lineage that ever went Solo stays tainted even if it reconnects.
    let mut taint_written = false;

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
        let mut any_ops = false;
        let mut shard_deltas: HashMap<usize, (i64, i64)> = HashMap::new();
        // Op-ids carried by this batch's transactions, recorded on apply.
        let mut batch_op_ids: Vec<crate::dedup::OpId> = Vec::new();
        for (mut txn, reply) in batch {
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
        if current > last_emitted_counter {
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

        // Commit-then-apply: ship and await the standby's durable ack BEFORE
        // applying, so a visible write is always replicated. `ship` returns None
        // when solo (no standby, or just downgraded); a solo write is still valid,
        // just single-node durable until the standby returns.
        let shipped_seqno = if any_ops {
            match ctx.replicator.as_ref() {
                Some(repl) => repl.ship(&repl_ops, &batch_op_ids).await,
                None => None,
            }
        } else {
            None
        };

        // HA: stamp this data db with the highest shipped batch seqno, flushed
        // ATOMICALLY with the batch. On takeover a promoted standby prunes its
        // buffered tail to this watermark, so a batch already flushed here is
        // never replayed (replaying a stale batch would regress monotonic
        // counters and double-count stats against the newer db state). Local
        // only (not in repl_ops); the standby keys its tail by its own seqnos.
        if let (Some(repl), Some(seqno)) = (ctx.replicator.as_ref(), shipped_seqno) {
            merged.put_bytes(
                ctx.key_codec.ha_seqno_key(),
                KeyCodec::encode_ha_seqno(repl.writer_epoch(), seqno),
            );
        }

        // SlateDB rejects empty WriteBatches. A batch can be empty when every
        // queued txn was a no-op (e.g. a sub-chunk trim against a fully sparse
        // region produces no chunk writes) and no inode was allocated. Reply
        // Ok without touching the db; there's nothing to make durable either.
        let mut result: Result<(), FsError> = Ok(());

        // First Solo batch: durably taint this lineage BEFORE acking any Solo write.
        // A takeover reads the taint and regenerates the lineage token, so these
        // un-shipped writes' later fsync fails honestly instead of matching a
        // carried-forward token. One flush per leader, gated by `taint_written`. If
        // it can't be made durable we fail the batch rather than ack a write we
        // cannot stand behind.
        if !taint_written && any_ops && ctx.replicator.is_some() && shipped_seqno.is_none() {
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
        }

        // In sync_writes mode, force a flush after the batch is committed so
        // every caller in the batch only sees Ok once their data is durable in
        // object storage. FlushCoordinator coalesces concurrent flush requests
        // into a single db.flush()
        if ctx.sync_writes && result.is_ok() && any_ops {
            result = ctx.flush_coordinator.flush().await;
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
        // `new_in_memory` creates v2-segmented volumes.
        KeyCodec::new(true)
    }

    #[tokio::test]
    async fn commits_single_transaction() {
        let fs = make_fs().await;
        let mut txn = Transaction::new();
        // Use a real codec-built key so the segment extractor (when v2 is
        // enabled by `new_in_memory`) accepts it.
        let key = codec().chunk_key(1, 0);
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
            let k = codec.chunk_key(1, i);
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
            let v = fs.db.get_bytes(&codec.chunk_key(1, i)).await.unwrap();
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
            let k = codec.chunk_key(2, i);
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
            let v = fs.db.get_bytes(&codec.chunk_key(2, i)).await.unwrap();
            assert!(
                v.is_some(),
                "chunk {i} not durable after sync_writes commit"
            );
        }
    }

    #[tokio::test]
    async fn empty_transaction_is_noop_not_fatal() {
        // SlateDB rejects empty WriteBatches with "empty write batch not
        // allowed". A no-op txn (e.g. sub-chunk trim on a fully sparse range)
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
        txn.put_bytes(&codec().chunk_key(3, 0), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_eq!(
            before, after,
            "counter key should not change without allocate"
        );

        // Now allocate and commit; counter must advance on disk.
        let _id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&codec().chunk_key(3, 1), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after_allocate = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_ne!(
            after, after_allocate,
            "counter key should advance after allocate"
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
            let key = codec.chunk_key(inode_id, 0);
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
            let key = codec.chunk_key(inode_id, 1);
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
}
