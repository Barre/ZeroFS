//! Database wrapper for SlateDB.
//!
//! This provides a unified interface for both read-write and read-only database access.
//! Encryption is handled at the SlateDB level via BlockTransformer, so this wrapper
//! just passes through operations.

use crate::fs::errors::FsError;
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use futures::stream::StreamExt;
use slatedb::config::{DurabilityLevel, PutOptions, ReadOptions, ScanOptions, WriteOptions};
use slatedb::{CacheTarget, DbCacheManagerOps, DbReader, WriteBatch};
use slatedb_common::metrics::DefaultMetricsRecorder;
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

/// Outcome of a [`Db::warm_metadata`] pass.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
pub struct WarmStats {
    /// Metadata SSTs the warm fan-out touched.
    pub ssts: usize,
    /// Of those, how many had at least one target fail (counted, not fatal).
    pub failed: usize,
}

/// Tracks which metadata SSTs have already been warmed, so that across manifest
/// changes (L0 flushes and compactions, which swap SSTs in and out) each SST is
/// warmed exactly once. Generic over the id type purely so the diff can be unit
/// tested with plain ids; production instantiates it with slatedb's `SsTableId`.
struct WarmTracker<Id> {
    seen: std::collections::HashSet<Id>,
    last_manifest_id: u64,
}

impl<Id: Eq + std::hash::Hash + Copy> WarmTracker<Id> {
    fn new() -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            last_manifest_id: 0,
        }
    }

    /// Given a manifest id and that manifest's live metadata SST ids, return the
    /// ids not yet warmed (recording them as warmed) and forget ids that are no
    /// longer live. Returns empty when the manifest id is unchanged, so a status
    /// notification that didn't change the manifest (e.g. a durability advance) is
    /// a no-op.
    fn plan(&mut self, manifest_id: u64, current: impl Iterator<Item = Id>) -> Vec<Id> {
        if manifest_id == self.last_manifest_id {
            return Vec::new();
        }
        self.last_manifest_id = manifest_id;
        let current: std::collections::HashSet<Id> = current.collect();
        // Drop retired ids so the set can't grow without bound (and a future SST
        // that reuses a retired id would warm again).
        self.seen.retain(|id| current.contains(id));
        current
            .into_iter()
            .filter(|id| self.seen.insert(*id))
            .collect()
    }
}

/// DST override for [`exit_on_write_error`]: in the simulation the "process"
/// is one instance inside the test, so death becomes a task panic: the
/// instance's workers stop and the test goes on to verify what the crash
/// left behind.
#[doc(hidden)]
pub static DST_PANIC_ON_WRITE_ERROR: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Fatal handler for SlateDB write errors.
/// After a write failure, the database state is unknown. Exit and let
/// the eventual orchestrator restart the service to rebuild from a known-good state.
pub fn exit_on_write_error(err: impl std::fmt::Display) -> ! {
    if DST_PANIC_ON_WRITE_ERROR.load(std::sync::atomic::Ordering::Relaxed) {
        panic!("dst: simulated process death on write error: {err}");
    }
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
    /// Per-segment counter adjustments (segcount key, `(live_delta, total_delta)`),
    /// aggregated by the commit worker into one absolute `(live, total)` per
    /// segment. Same lock-free pattern as `stats_deltas`.
    seg_deltas: Vec<(Bytes, (i64, i64))>,
    /// Shared side of the extent-store publication barrier. A transaction that
    /// has assigned any FrameLoc holds this from before the frame is appended
    /// until the coordinator has made its pointer visible. Segment GC takes
    /// the exclusive side before sealing and choosing its eligibility cutoff,
    /// so a sealed segment can no longer gain a late reference.
    segment_publish_guard: Option<SegmentPublishGuard>,
    op_id: crate::dedup::OpId,
}

/// Type-erased RAII token supplied by the extent store. Keeping this token in
/// the transaction lets the database layer preserve the publisher's lifetime
/// without depending on the extent module's admission-gate implementation.
pub(crate) trait SegmentPublishPermit: Send + Sync {}

impl<T: Send + Sync> SegmentPublishPermit for T {}

pub(crate) type SegmentPublishGuard = Arc<dyn SegmentPublishPermit>;

impl Transaction {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            stats_deltas: Vec::new(),
            seg_deltas: Vec::new(),
            segment_publish_guard: None,
            op_id: [0u8; 16],
        }
    }

    pub(crate) fn has_segment_publish_guard(&self) -> bool {
        self.segment_publish_guard.is_some()
    }

    pub(crate) fn hold_segment_publish_guard(&mut self, guard: SegmentPublishGuard) {
        debug_assert!(
            self.segment_publish_guard.is_none(),
            "a transaction may hold only one segment publication guard"
        );
        self.segment_publish_guard = Some(guard);
    }

    pub(crate) fn take_segment_publish_guard(&mut self) -> Option<SegmentPublishGuard> {
        self.segment_publish_guard.take()
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

    /// Record live/total byte adjustments for a segment's counter (`segcount_key`),
    /// materialized as an absolute `(live, total)` by the commit worker. A frame
    /// write credits both (`+len, +len`); an overwrite/delete debits live only
    /// (`-len, 0`), keeping `total` monotonic. All-zero deltas drop.
    pub fn add_seg_delta(&mut self, segcount_key: &Bytes, live_delta: i64, total_delta: i64) {
        if live_delta != 0 || total_delta != 0 {
            self.seg_deltas
                .push((segcount_key.clone(), (live_delta, total_delta)));
        }
    }

    pub fn take_seg_deltas(&mut self) -> Vec<(Bytes, (i64, i64))> {
        std::mem::take(&mut self.seg_deltas)
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Replay this transaction's ops into `target`. SlateDB's `WriteBatch`
    /// already dedupes per key, so calling this on multiple transactions
    /// produces one merged batch with last-write-wins per key.
    ///
    /// Only `ops` are applied — `seg_deltas` are the WriteCoordinator worker's job
    /// (the sole segcount writer, which drains them first). The assert catches any
    /// path that would commit a seg-delta-bearing txn directly and silently lose it.
    pub fn apply_to(self, target: &mut WriteBatch) {
        debug_assert!(
            self.seg_deltas.is_empty(),
            "seg_deltas would be dropped: commit a seg_delta-bearing txn through the \
             WriteCoordinator, not into_inner/apply_to"
        );
        debug_assert!(
            self.segment_publish_guard.is_none(),
            "segment publication guard would be dropped before commit"
        );
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
        debug_assert!(
            self.seg_deltas.is_empty(),
            "seg_deltas would be dropped: commit a seg_delta-bearing txn through the \
             WriteCoordinator, not apply_to_collecting"
        );
        debug_assert!(
            self.segment_publish_guard.is_none(),
            "segment publication guard would be dropped before commit"
        );
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
    /// Flush barrier. Every commit takes a read lock; the seal+flush durability
    /// sequence (flush coordinator, segment reclaim) takes the write lock. This
    /// keeps the flush from durably capturing a `FrameLoc` whose segment is still
    /// the un-PUT open buffer: a commit that overlaps a flush lands *after* it, so
    /// its pointer is never in the flushed set referencing an un-sealed segment.
    flush_barrier: Arc<tokio::sync::RwLock<()>>,
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
            flush_barrier: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    pub fn new_read_only(db_reader: ArcSwap<DbReader>) -> Self {
        Self {
            inner: SlateDbHandle::ReadOnly(db_reader),
            metrics_recorder: None,
            lease: None,
            status: None,
            flush_barrier: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    /// The flush barrier (see the field). The seal+flush durability sequence holds
    /// the *write* lock across `seal_open()` + `flush()`; commits hold a read lock.
    pub fn flush_barrier(&self) -> Arc<tokio::sync::RwLock<()>> {
        Arc::clone(&self.flush_barrier)
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
        self.get_bytes_at(key, DurabilityLevel::Memory).await
    }

    /// Point read seeing only object-storage-durable data.
    pub async fn get_bytes_durable(&self, key: &Bytes) -> Result<Option<Bytes>> {
        self.get_bytes_at(key, DurabilityLevel::Remote).await
    }

    async fn get_bytes_at(
        &self,
        key: &Bytes,
        durability_filter: DurabilityLevel,
    ) -> Result<Option<Bytes>> {
        self.check_lease()?;
        let read_options = ReadOptions {
            durability_filter,
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

    pub async fn scan<R: slatedb::ByteRangeBounds + Send>(
        &self,
        range: R,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        self.scan_at(range, DurabilityLevel::Memory).await
    }

    /// Scan seeing only object-storage-durable data.
    pub async fn scan_durable<R: slatedb::ByteRangeBounds + Send>(
        &self,
        range: R,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        self.scan_at(range, DurabilityLevel::Remote).await
    }

    async fn scan_at<R: slatedb::ByteRangeBounds + Send>(
        &self,
        range: R,
        durability_filter: DurabilityLevel,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Bytes, Bytes)>> + Send + '_>>> {
        self.check_lease()?;
        let scan_options = ScanOptions {
            durability_filter,
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
        tokio::spawn(forward_scan(iter, tx));
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// Prefix scan that consults SlateDB SST filters to skip non-matching SSTs.
    ///
    /// `seek_to` (a full key within the prefix) is pushed down as the scan's
    /// lower bound, so SlateDB prunes sorted runs entirely below the resume
    /// point at setup instead of opening them and seeking forward.
    /// `read_ahead_bytes` controls SlateDB's read-ahead within the iterator.
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

        // Push the optional resume point down as the subrange's lower bound
        // rather than scanning from the prefix start and seeking forward.
        // SlateDB selects only sorted runs whose key range overlaps the scan
        // range, so a tighter lower bound prunes SSTs that sit entirely below
        // the resume point before they are ever opened. `seek_to` is a full key
        // (`prefix ++ suffix`); the subrange is a prefix-relative suffix, so we
        // strip the prefix. An empty suffix reproduces the full-prefix scan.
        let suffix = match &seek_to {
            Some(key) => {
                debug_assert!(
                    key.starts_with(prefix.as_ref()),
                    "scan_prefix seek_to must fall within the prefix"
                );
                key.slice(prefix.len()..)
            }
            None => Bytes::new(),
        };

        let iter = match &self.inner {
            SlateDbHandle::ReadWrite(db) => {
                db.scan_prefix_with_options(prefix, suffix.., &scan_options)
                    .await?
            }
            SlateDbHandle::ReadOnly(reader_swap) => {
                let reader = reader_swap.load();
                reader
                    .scan_prefix_with_options(prefix, suffix.., &scan_options)
                    .await?
            }
        };

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
        // Read side of the flush barrier: a commit overlapping a seal+flush waits
        // for it, so its pointer lands after the flush, never in the flushed set.
        let _flush_guard = self.flush_barrier.read().await;

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
        let _flush_guard = self.flush_barrier.read().await;

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

    /// Concurrency cap for the warm fan-out. Each task issues at most a couple of
    /// object-store GETs (filter + index), so a small cap drains thousands of
    /// SSTs quickly without crowding the serving path off the object store.
    const WARM_CONCURRENCY: usize = 16;

    /// Cache targets for a metadata warm. Filters + index are always warmed
    /// (small, bounded, and they gate every point lookup); `warm_data` adds the
    /// data blocks (the whole `meta` segment is metadata, so the full range).
    fn warm_targets(warm_data: bool) -> Vec<CacheTarget> {
        let mut targets = vec![CacheTarget::Filters, CacheTarget::Index];
        if warm_data {
            targets.push(CacheTarget::data::<&[u8], _>(..));
        }
        targets
    }

    /// One-shot warm of the whole metadata segment, returning what it touched.
    /// Production keeps the cache warm with [`warm_metadata_watch`](Self::warm_metadata_watch)
    /// instead (which also re-warms after compactions); this primitive exists for
    /// tests that assert the cache effect of a single warm.
    ///
    /// Best-effort and side-effect-only: a per-SST failure is counted, not
    /// propagated. A no-op on a volume with no metadata segment yet (a fresh DB
    /// before its first flush) or without a block cache. Not lease-gated.
    #[cfg(test)]
    pub async fn warm_metadata(&self, warm_data: bool) -> WarmStats {
        let targets = Self::warm_targets(warm_data);

        // Snapshot the metadata segment's SST ids; the manifest borrow ends with
        // the collect (ids are `Copy`), before the fan-out.
        let manifest = match &self.inner {
            SlateDbHandle::ReadWrite(db) => db.manifest(),
            SlateDbHandle::ReadOnly(reader) => reader.load().manifest(),
        };
        let Some(segment) = manifest.segment(crate::fs::key_codec::META_DOMAIN) else {
            tracing::info!("metadata cache warm skipped: no metadata segment on this volume");
            return WarmStats::default();
        };
        let ids: Vec<_> = segment
            .l0()
            .iter()
            .chain(
                segment
                    .compacted()
                    .iter()
                    .flat_map(|run| run.sst_views.iter()),
            )
            .map(|view| view.sst.id)
            .collect();

        let total = ids.len();
        let failed = futures::stream::iter(ids)
            .map(|id| {
                let targets = &targets;
                async move {
                    match &self.inner {
                        SlateDbHandle::ReadWrite(db) => db.warm_sst(id, targets).await,
                        SlateDbHandle::ReadOnly(reader) => {
                            reader.load_full().warm_sst(id, targets).await
                        }
                    }
                }
            })
            .buffer_unordered(Self::WARM_CONCURRENCY)
            .filter(|r| std::future::ready(r.is_err()))
            .count()
            .await;

        WarmStats {
            ssts: total,
            failed,
        }
    }

    /// A fresh status subscription (manifest + durability updates) for the
    /// read-write handle, or `None` for a read-only open (which has no block
    /// cache to keep warm).
    pub fn subscribe_status(&self) -> Option<tokio::sync::watch::Receiver<slatedb::DbStatus>> {
        match &self.inner {
            SlateDbHandle::ReadWrite(db) => Some(db.subscribe()),
            SlateDbHandle::ReadOnly(_) => None,
        }
    }

    /// Keep the metadata block cache warm for the life of the process.
    ///
    /// Warms the meta segment once up front, then re-warms newly-appeared SSTs
    /// whenever the manifest changes. Compactions (and L0 flushes) replace meta
    /// SSTs with cold ones, and the compactor shares no block cache, so without
    /// this the startup warm decays and metadata reads pay the cold object-store
    /// cost again right after every compaction. The set of already-warmed ids is
    /// diffed against each new manifest, so each SST is warmed exactly once and an
    /// unchanged manifest is a no-op. Runs until `shutdown`.
    pub async fn warm_metadata_watch(
        &self,
        warm_data: bool,
        mut status: tokio::sync::watch::Receiver<slatedb::DbStatus>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let targets = Self::warm_targets(warm_data);
        let mut tracker = WarmTracker::new();
        let mut first = true;

        loop {
            let new_ids: Vec<_> = {
                let snapshot = status.borrow();
                let manifest = &snapshot.current_manifest;
                match manifest.segment(crate::fs::key_codec::META_DOMAIN) {
                    None => Vec::new(),
                    Some(segment) => {
                        let live = segment
                            .l0()
                            .iter()
                            .chain(
                                segment
                                    .compacted()
                                    .iter()
                                    .flat_map(|run| run.sst_views.iter()),
                            )
                            .map(|view| view.sst.id);
                        tracker.plan(manifest.id(), live)
                    }
                }
            };

            if !new_ids.is_empty() {
                let count = new_ids.len();
                let failed = futures::stream::iter(new_ids)
                    .map(|id| {
                        let targets = &targets;
                        async move {
                            match &self.inner {
                                SlateDbHandle::ReadWrite(db) => db.warm_sst(id, targets).await,
                                SlateDbHandle::ReadOnly(reader) => {
                                    reader.load_full().warm_sst(id, targets).await
                                }
                            }
                        }
                    })
                    .buffer_unordered(Self::WARM_CONCURRENCY)
                    .filter(|r| std::future::ready(r.is_err()))
                    .count()
                    .await;
                // The first pass is the startup warm (whole segment); later passes
                // are the post-compaction / post-flush deltas.
                if first {
                    tracing::info!("metadata cache warm: {count} SSTs ({failed} failed)");
                } else {
                    tracing::debug!("metadata cache re-warm: {count} new SSTs ({failed} failed)");
                }
            }
            first = false;

            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = status.changed() => {
                    if changed.is_err() {
                        break; // sender dropped: the db is closing
                    }
                }
            }
        }
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

/// The scan forwarder's input
trait ScanSource: Send {
    fn next_kv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<(Bytes, Bytes)>>> + Send;
}

impl ScanSource for slatedb::DbIterator {
    async fn next_kv(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self.next().await?.map(|kv| (kv.key, kv.value)))
    }
}

/// Pump `iter` into `tx` until end-of-range, a dropped consumer, or an error.
async fn forward_scan<S: ScanSource>(
    mut iter: S,
    tx: tokio::sync::mpsc::Sender<Result<(Bytes, Bytes)>>,
) {
    loop {
        match iter.next_kv().await {
            Ok(Some(kv)) => {
                if tx.send(Ok(kv)).await.is_err() {
                    break; // consumer dropped the stream
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod warm_tracker_tests {
    use super::WarmTracker;

    fn sorted(mut v: Vec<u64>) -> Vec<u64> {
        v.sort_unstable();
        v
    }

    // The tracker warms each SST once across the manifest changes a long-running
    // leader sees: the initial open, L0 flushes (add an SST), and compactions
    // (retire several SSTs, add new ones). Only genuinely-new ids are warmed.
    #[test]
    fn warms_new_ssts_once_across_flushes_and_compactions() {
        let mut t = WarmTracker::<u64>::new();

        // Initial manifest: warm everything.
        assert_eq!(
            sorted(t.plan(1, [10, 11, 12].into_iter())),
            vec![10, 11, 12]
        );

        // A status notification that didn't bump the manifest id (e.g. a
        // durability advance): nothing to do.
        assert!(t.plan(1, [10, 11, 12].into_iter()).is_empty());

        // An L0 flush adds one SST: only the new one is warmed.
        assert_eq!(sorted(t.plan(2, [10, 11, 12, 13].into_iter())), vec![13]);

        // A compaction retires 11, 12, 13 into a new run 20 and keeps 10: only the
        // compaction output is cold, so only it is warmed.
        assert_eq!(sorted(t.plan(3, [10, 20].into_iter())), vec![20]);

        // Re-presenting the same set after another change warms nothing.
        assert!(t.plan(4, [10, 20].into_iter()).is_empty());

        // An id that was retired and reappears is treated as new (its blocks are
        // no longer guaranteed cached), so it warms again.
        assert_eq!(sorted(t.plan(5, [10, 20, 11].into_iter())), vec![11]);
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

#[cfg(test)]
mod scan_error_tests {
    use super::*;
    use futures::StreamExt;
    use slatedb::config::PutOptions;
    use std::collections::VecDeque;

    /// A scripted [`ScanSource`]; yields its items then end-of-range.
    struct Scripted(VecDeque<Result<Option<(Bytes, Bytes)>>>);

    impl ScanSource for Scripted {
        async fn next_kv(&mut self) -> Result<Option<(Bytes, Bytes)>> {
            self.0.pop_front().unwrap_or(Ok(None))
        }
    }

    // A mid-scan error must surface as an Err item on the stream, never as a
    // silently truncated clean end: read_range would serve the missing extents
    // as fabricated zeros, and delete_range would skip their segment debits (a
    // permanent leak).
    #[tokio::test]
    async fn forwarder_delivers_a_mid_scan_error_instead_of_truncating() {
        let kv = |k: &str| {
            (
                Bytes::copy_from_slice(k.as_bytes()),
                Bytes::from_static(b"v"),
            )
        };
        let script = VecDeque::from([
            Ok(Some(kv("a"))),
            Ok(Some(kv("b"))),
            Err(anyhow::anyhow!("mid-scan read failure")),
            // Past the error: must never be served.
            Ok(Some(kv("c"))),
        ]);
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(forward_scan(Scripted(script), tx));

        let items: Vec<_> = tokio_stream::wrappers::ReceiverStream::new(rx)
            .collect()
            .await;
        assert!(
            items.len() == 3 && items[2].is_err(),
            "two rows then the delivered error, nothing after; got {} item(s), \
             last ok={:?}",
            items.len(),
            items.last().map(|i| i.is_ok())
        );
        assert!(items[0].is_ok() && items[1].is_ok());
    }

    // The forwarder streams a range wider than the eager prefetch window
    // (max_fetch_tasks x read_ahead_bytes) to completion over a cold reopen —
    // the paged path production scans rarely cross.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_streams_a_multi_window_range_completely() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = slatedb::object_store::path::Path::from("data");
        const ROWS: usize = 8192;
        let value = vec![7u8; 4096];
        let settings = || slatedb::config::Settings {
            compactor_options: None,
            ..Default::default()
        };
        {
            let db = Db::new(
                Arc::new(
                    slatedb::DbBuilder::new(path.clone(), store.clone())
                        .with_settings(settings())
                        .build()
                        .await
                        .unwrap(),
                ),
                None,
            );
            for i in 0..ROWS {
                let key = Bytes::from(format!("k{i:05}"));
                db.put_with_options(
                    &key,
                    &value,
                    &PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
            db.flush().await.unwrap();
            db.close().await.unwrap();
        }
        let db = Db::new(
            Arc::new(
                slatedb::DbBuilder::new(path, store)
                    .with_settings(settings())
                    .build()
                    .await
                    .unwrap(),
            ),
            None,
        );
        let mut stream = db
            .scan(Bytes::new()..Bytes::from_static(&[0xff]))
            .await
            .unwrap();
        let rows = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            let mut rows = 0usize;
            while let Some(item) = stream.next().await {
                item.unwrap();
                rows += 1;
            }
            rows
        })
        .await
        .expect("scan drain hung");
        assert_eq!(rows, ROWS);
    }
}
