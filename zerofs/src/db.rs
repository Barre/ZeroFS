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
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            stats_deltas: Vec::new(),
        }
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
}

impl Db {
    pub fn new(
        db: Arc<slatedb::Db>,
        metrics_recorder: Option<Arc<DefaultMetricsRecorder>>,
    ) -> Self {
        Self {
            inner: SlateDbHandle::ReadWrite(db),
            metrics_recorder,
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>) -> Self {
        Self {
            inner: SlateDbHandle::ReadOnly(db_reader),
            metrics_recorder: None,
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    pub async fn get_bytes(&self, key: &Bytes) -> Result<Option<Bytes>> {
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

    pub async fn write_with_options(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<()> {
        if self.is_read_only() {
            return Err(FsError::ReadOnlyFilesystem.into());
        }

        match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                if let Err(e) = db.write_with_options(batch, options).await {
                    exit_on_write_error(e);
                }
            }
            SlateDbHandle::ReadOnly(_) => unreachable!(),
        }

        Ok(())
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
