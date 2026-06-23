//! Database wrapper for SlateDB.
//!
//! This provides a unified interface for both read-write and read-only database access.
//! Encryption is handled at the SlateDB level via BlockTransformer, so this wrapper
//! just passes through operations.

use crate::fs::errors::FsError;
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions, WriteOptions};
use slatedb::{DbReader, WriteBatch};
use slatedb_common::metrics::DefaultMetricsRecorder;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;

/// Wrapper for SlateDB handle that can be either read-write or read-only.
pub enum SlateDbHandle {
    ReadWrite(Arc<slatedb::Db>),
    ReadOnly(ArcSwap<DbReader>),
}

impl Clone for SlateDbHandle {
    fn clone(&self) -> Self {
        match self {
            SlateDbHandle::ReadWrite(db) => SlateDbHandle::ReadWrite(db.clone()),
            SlateDbHandle::ReadOnly(reader) => {
                SlateDbHandle::ReadOnly(ArcSwap::new(reader.load_full()))
            }
        }
    }
}

impl SlateDbHandle {
    pub fn is_read_only(&self) -> bool {
        matches!(self, SlateDbHandle::ReadOnly(_))
    }
}

/// Fatal handler for SlateDB write errors.
/// After a write failure, the database state is unknown. Exit and let
/// the eventual orchestrator restart the service to rebuild from a known-good state.
pub fn exit_on_write_error(err: impl std::fmt::Display) -> ! {
    tracing::error!("Fatal write error, exiting: {}", err);
    std::process::exit(1)
}

enum TxOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

/// Usage-stats adjustment riding along with a transaction. Deltas commute, so
/// the commit worker can aggregate them per shard across a whole batch and
/// persist one absolute shard value, without any per-operation locking.
pub struct StatsDelta {
    pub inode_id: u64,
    pub bytes: i64,
    pub inodes: i64,
}

/// Transaction for batching database writes.
///
/// Ops are recorded as a flat vector so the commit coordinator can replay
/// several transactions into a single merged `WriteBatch` via [`apply_to`].
pub struct Transaction {
    ops: Vec<TxOp>,
    stats_deltas: Vec<StatsDelta>,
    op_id: crate::dedup::OpId,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            stats_deltas: Vec::new(),
            op_id: [0u8; 16],
        }
    }

    /// Tag with a client idempotency op-id (all-zero = none). The commit worker
    /// records it in the dedup cache and ships it to the standby atomically with
    /// the data, so a retry (here or after failover) is recognized as applied.
    pub fn set_op_id(&mut self, op_id: crate::dedup::OpId) {
        self.op_id = op_id;
    }

    pub fn op_id(&self) -> crate::dedup::OpId {
        self.op_id
    }

    pub fn put_bytes(&mut self, key: &Bytes, value: Bytes) {
        self.ops.push(TxOp::Put(key.clone(), value));
    }

    pub fn delete_bytes(&mut self, key: &Bytes) {
        self.ops.push(TxOp::Delete(key.clone()));
    }

    /// Record a usage-stats adjustment for `inode_id`'s shard, materialized
    /// by the commit worker. No-op deltas are dropped so callers can pass
    /// computed differences unconditionally.
    pub fn add_stats_delta(&mut self, inode_id: u64, bytes: i64, inodes: i64) {
        if bytes != 0 || inodes != 0 {
            self.stats_deltas.push(StatsDelta {
                inode_id,
                bytes,
                inodes,
            });
        }
    }

    pub fn take_stats_deltas(&mut self) -> Vec<StatsDelta> {
        std::mem::take(&mut self.stats_deltas)
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Replay this transaction's ops into `target`. SlateDB's `WriteBatch`
    /// already dedupes per key, so calling this on multiple transactions
    /// produces one merged batch with last-write-wins per key.
    pub fn apply_to(self, target: &mut WriteBatch) {
        for op in self.ops {
            match op {
                TxOp::Put(k, v) => target.put_bytes(k, v),
                TxOp::Delete(k) => target.delete(k),
            }
        }
    }

    /// Like [`apply_to`](Self::apply_to) but also returns the ops as `ReplOp`s
    /// for shipping. In apply order; replaying in seqno-then-op order on the
    /// standby reproduces the merged batch's last-write-wins result.
    pub fn apply_to_collecting(self, target: &mut WriteBatch) -> Vec<crate::replication::ReplOp> {
        use crate::replication::ReplOp;
        let mut ops = Vec::with_capacity(self.ops.len());
        for op in self.ops {
            match op {
                TxOp::Put(k, v) => {
                    target.put_bytes(k.clone(), v.clone());
                    ops.push(ReplOp::Put(k, v));
                }
                TxOp::Delete(k) => {
                    target.delete(k.clone());
                    ops.push(ReplOp::Delete(k));
                }
            }
        }
        ops
    }

    pub fn into_inner(self) -> WriteBatch {
        let mut batch = WriteBatch::new();
        self.apply_to(&mut batch);
        batch
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

/// Database wrapper providing a unified interface for SlateDB operations.
///
/// With BlockTransformer handling encryption at the SlateDB level, this wrapper
/// simply passes through operations without additional encryption/decryption.
pub struct Db {
    inner: SlateDbHandle,
    metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    /// HA leader lease. `Some` only under replication; reads/writes are refused
    /// while invalid so a deposed node never serves stale data. `None` (ungated)
    /// in single-node mode.
    lease: Option<Arc<crate::replication::Lease>>,
    /// Data db status, read directly on the gate path (not a lagging watcher) so a
    /// deposition is seen with no lag: SlateDB closes the db (`CloseReason::Fenced`)
    /// the instant its manifest poll sees a newer writer, and a closed db must
    /// reject ops with the "not leader" signal a failover client re-routes on.
    status: Option<tokio::sync::watch::Receiver<slatedb::DbStatus>>,
}

impl Db {
    pub fn new(
        db: Arc<slatedb::Db>,
        metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    ) -> Self {
        let status = Some(db.subscribe());
        Self {
            inner: SlateDbHandle::ReadWrite(db),
            metrics_recorder,
            lease: None,
            status,
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>) -> Self {
        Self {
            inner: SlateDbHandle::ReadOnly(db_reader),
            metrics_recorder: None,
            lease: None,
            status: None,
        }
    }

    /// Attach the HA leader lease; reads/writes are then refused while it is
    /// invalid. Single-node `Db`s have no lease and are never gated.
    pub fn with_lease(mut self, lease: Arc<crate::replication::Lease>) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Error if a lease is attached and currently invalid.
    #[inline]
    fn check_lease(&self) -> Result<()> {
        if self.is_deposed() {
            return Err(FsError::LeaderLeaseExpired.into());
        }
        Ok(())
    }

    /// Whether this node may serve as leader right now (always true in single-node
    /// mode). The 9P layer checks this before dispatch so a deposed leader answers
    /// with the "not leader" signal a failover client re-routes on, not EIO.
    #[inline]
    pub fn lease_is_valid(&self) -> bool {
        !self.is_deposed()
    }

    /// True once no longer the serving leader: the lease is invalid, or the data
    /// db has been closed (fenced by a takeover).
    #[inline]
    fn is_deposed(&self) -> bool {
        if let Some(lease) = &self.lease
            && !lease.is_valid()
        {
            return true;
        }
        if let Some(status) = &self.status
            && status.borrow().close_reason.is_some()
        {
            return true;
        }
        false
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    pub async fn get_bytes(&self, key: &Bytes) -> Result<Option<Bytes>> {
        self.check_lease()?;
        let read_options = ReadOptions {
            durability_filter: DurabilityLevel::Memory,
            cache_blocks: true,
            ..Default::default()
        };

        let result = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.get_with_options(key, &read_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.get_with_options(key, &read_options).await?
            }
        };

        Ok(result)
    }

    pub async fn scan<R: RangeBounds<Bytes> + Clone + Send + Sync + 'static>(
        &self,
        range: R,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        self.check_lease()?;
        let scan_options = ScanOptions {
            durability_filter: DurabilityLevel::Memory,
            read_ahead_bytes: 4 * 1024 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 4,
            ..Default::default()
        };

        let iter = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.scan_with_options(range, &scan_options).await?,
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.scan_with_options(range, &scan_options).await?
            }
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<(Bytes, Bytes)>>(32);

        tokio::spawn(async move {
            let mut iter = iter;
            while let Ok(Some(kv)) = iter.next().await {
                if tx.send(Ok((kv.key, kv.value))).await.is_err() {
                    break;
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Prefix scan that consults SlateDB SST filters to skip non-matching SSTs.
    ///
    /// `seek_to` lets the caller advance to the first interesting key inside the
    /// prefix without re-fetching the leading blocks; `read_ahead_bytes` controls
    /// SlateDB's read-ahead within the iterator.
    pub async fn scan_prefix(
        &self,
        prefix: Bytes,
        seek_to: Option<Bytes>,
        read_ahead_bytes: usize,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send>>> {
        self.check_lease()?;
        let scan_options = ScanOptions {
            durability_filter: DurabilityLevel::Memory,
            read_ahead_bytes,
            cache_blocks: true,
            max_fetch_tasks: 4,
            ..Default::default()
        };

        let mut iter = match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                db.scan_prefix_with_options(prefix, &scan_options).await?
            }
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader
                    .scan_prefix_with_options(prefix, &scan_options)
                    .await?
            }
        };

        if let Some(key) = seek_to {
            iter.seek(key).await?;
        }

        Ok(Box::pin(futures::stream::unfold(
            iter,
            |mut iter| async move {
                match iter.next().await {
                    Ok(Some(kv)) => Some((Ok((kv.key, kv.value)), iter)),
                    Ok(None) => None,
                    Err(e) => Some((Err(e.into()), iter)),
                }
            },
        )))
    }

    /// Returns the committed batch's SlateDB seqnum, mapped to `durable_seq` to
    /// advance the standby's prune watermark.
    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<u64> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }
        self.check_lease()?;

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => match db.write_with_options(batch, options).await {
                Ok(handle) => Ok(handle.seqnum()),
                Err(e) => exit_on_write_error(e),
            },
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }
    }

    pub fn new_transaction(&self) -> Result<Transaction, FsError> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem);
        }
        Ok(Transaction::new())
    }

    pub async fn put_with_options(
        &self,
        key: &Bytes,
        value: &[u8],
        put_options: &PutOptions,
        write_options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db
                    .put_with_options(key, value, put_options, write_options)
                    .await
                {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.flush().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }
        Ok(())
    }

    pub fn slatedb_metrics(&self) -> Option<Arc<DefaultMetricsRecorder>> {
        self.metrics_recorder.clone()
    }

    pub async fn close(&self) -> Result<()> {
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.close().await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader.close().await?
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod lease_gate_tests {
    use super::*;
    use crate::replication::Lease;
    use std::time::Duration;

    async fn open_inner() -> Arc<slatedb::Db> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        Arc::new(
            slatedb::DbBuilder::new(slatedb::object_store::path::Path::from("data"), store)
                .build()
                .await
                .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_gates_reads_and_writes() {
        let lease = Lease::new();
        let db = Db::new(open_inner().await, None).with_lease(lease.clone());
        let key = Bytes::from_static(b"k");

        // Invalid lease (never renewed): reads are refused.
        assert!(
            db.get_bytes(&key).await.is_err(),
            "read must be refused while the lease is invalid"
        );

        // Refused by the gate before reaching the db, so no write is attempted.
        let mut batch = WriteBatch::new();
        batch.put_bytes(key.clone(), Bytes::from_static(b"v"));
        assert!(
            db.write_with_options(batch, &WriteOptions::default())
                .await
                .is_err(),
            "write must be refused while the lease is invalid"
        );

        // Valid lease: reads serve (the key is absent, so None).
        lease.renew(Duration::from_millis(500));
        assert!(db.get_bytes(&key).await.unwrap().is_none());

        // Revoked: refused again.
        lease.invalidate();
        assert!(
            db.get_bytes(&key).await.is_err(),
            "read must be refused after the lease is revoked"
        );
    }
}
