//! Shared fixtures for the extent tests: in-memory store construction and
//! model-checked write/read helpers.

use super::ExtentStore;
use super::reclaim::cycle::{self, CyclePolicy, ReclaimOutcome, SegmentProtection};
use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::CompressionConfig;
use crate::config::ReclaimConfig;
use crate::db::{Db, Transaction};
use crate::frame_codec::FrameCodec;
use crate::fs::EXTENT_SIZE;
use crate::fs::FsError;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::fs::stats::FileSystemGlobalStats;
use crate::fs::store::{DirectoryStore, InodeStore};
use crate::fs::write_coordinator::WriteCoordinator;
use crate::segment::{FrameLoc, SEGMENT_INFO, Segid};
use crate::segment_store::SegmentStore;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use slatedb::{BlockTransformer, DbBuilder};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Extent-only tests use the production commit worker without constructing a
/// complete filesystem. The outer handle keeps that worker alive; the inner
/// store holds only its mandatory weak sender, exactly as it does in production.
#[derive(Clone)]
pub(super) struct TestExtentStore {
    inner: ExtentStore,
    _write_coordinator: WriteCoordinator,
}

impl Deref for TestExtentStore {
    type Target = ExtentStore;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestExtentStore {
    pub(super) fn with_seal_threshold(mut self, n: usize) -> Self {
        self.inner = self.inner.with_seal_threshold(n);
        self
    }
}

#[derive(Debug, Default)]
struct InFlightCounter {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl InFlightCounter {
    fn begin(self: &Arc<Self>) -> InFlightGuard {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        InFlightGuard(Arc::clone(self))
    }

    fn reset_peak(&self) {
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
        self.peak.store(0, Ordering::SeqCst);
    }
}

struct InFlightGuard(Arc<InFlightCounter>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Test-only object-store wrapper that makes overlap observable. Tests build
/// SlateDB on the raw inner store and inject this wrapper only into
/// `SegmentStore`, so its counters describe data-plane segment I/O exactly.
#[derive(Debug)]
pub(super) struct InFlightObjectStore {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
    gets: Arc<InFlightCounter>,
    puts: Arc<InFlightCounter>,
    deletes: Arc<InFlightCounter>,
    listed: Arc<AtomicUsize>,
    listed_at_first_delete: Arc<AtomicUsize>,
}

impl InFlightObjectStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            delay,
            gets: Arc::default(),
            puts: Arc::default(),
            deletes: Arc::default(),
            listed: Arc::default(),
            listed_at_first_delete: Arc::new(AtomicUsize::new(usize::MAX)),
        })
    }

    pub(super) fn reset_peaks(&self) {
        self.gets.reset_peak();
        self.puts.reset_peak();
        self.deletes.reset_peak();
    }

    pub(super) fn peak_gets(&self) -> usize {
        self.gets.peak.load(Ordering::SeqCst)
    }

    pub(super) fn peak_puts(&self) -> usize {
        self.puts.peak.load(Ordering::SeqCst)
    }

    pub(super) fn peak_deletes(&self) -> usize {
        self.deletes.peak.load(Ordering::SeqCst)
    }

    pub(super) fn listed_at_first_delete(&self) -> Option<usize> {
        let listed = self.listed_at_first_delete.load(Ordering::SeqCst);
        (listed != usize::MAX).then_some(listed)
    }
}

impl Display for InFlightObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "InFlightObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for InFlightObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        let _guard = self.puts.begin();
        tokio::time::sleep(self.delay).await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        let _guard = self.gets.begin();
        tokio::time::sleep(self.delay).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        let delay = self.delay;
        let deletes = Arc::clone(&self.deletes);
        let listed = Arc::clone(&self.listed);
        let listed_at_first_delete = Arc::clone(&self.listed_at_first_delete);
        locations
            .then(move |location| {
                let inner = Arc::clone(&inner);
                let deletes = Arc::clone(&deletes);
                let listed = Arc::clone(&listed);
                let listed_at_first_delete = Arc::clone(&listed_at_first_delete);
                async move {
                    let location = location?;
                    let _ = listed_at_first_delete.compare_exchange(
                        usize::MAX,
                        listed.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                    let _guard = deletes.begin();
                    tokio::time::sleep(delay).await;
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        let listed = Arc::clone(&self.listed);
        self.inner
            .list(prefix)
            .inspect(move |_| {
                listed.fetch_add(1, Ordering::SeqCst);
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

pub(super) async fn make() -> (TestExtentStore, Arc<Db>) {
    let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
    (store, db)
}

pub(super) async fn make_with_compression(
    compression: CompressionConfig,
) -> (TestExtentStore, Arc<Db>, Arc<dyn ObjectStore>) {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bt: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::try_new_arc(&[0u8; 32], CompressionConfig::default())
            .expect("test key should be lockable");
    let slatedb = Arc::new(
        DbBuilder::new(Path::from("t"), object_store.clone())
            .with_block_transformer(bt)
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let db = Arc::new(Db::new(slatedb, None));
    let store = make_store(object_store.clone(), db.clone(), compression, 7).await;
    (store, db, object_store)
}

/// A store over given backing state with fresh in-memory state at `epoch`.
pub(super) async fn make_store(
    object_store: Arc<dyn ObjectStore>,
    db: Arc<Db>,
    compression: CompressionConfig,
    epoch: u64,
) -> TestExtentStore {
    let key_codec = Arc::new(KeyCodec::new());
    let inode_store = InodeStore::new(Arc::clone(&db), Arc::clone(&key_codec), 2);
    let directory_store = DirectoryStore::new(Arc::clone(&db), Arc::clone(&key_codec));
    let flush_coordinator = FlushCoordinator::new(Arc::clone(&db));
    let global_stats = Arc::new(FileSystemGlobalStats::new(Arc::clone(&key_codec)));
    let dedup = Arc::new(crate::dedup::DedupCache::new());
    let (write_coordinator, pending_write_coordinator) =
        WriteCoordinator::channel(inode_store.next_id());
    let codec = FrameCodec::try_new(&[1u8; 32], SEGMENT_INFO, compression)
        .expect("test key should be lockable");
    let segments = Arc::new(SegmentStore::new(object_store, codec, epoch));
    let inner = ExtentStore::new(
        Arc::clone(&db),
        Arc::clone(&key_codec),
        segments,
        Arc::new(KeyedLockManager::new()),
        8 * 1024 * 1024,
        write_coordinator.downgrade(),
    );
    inner.seed_footprint().await.unwrap();
    pending_write_coordinator.start(
        db,
        inode_store,
        directory_store,
        flush_coordinator,
        key_codec,
        global_stats,
        false,
        None,
        dedup,
        0,
        inner.clone(),
    );
    TestExtentStore {
        inner,
        _write_coordinator: write_coordinator,
    }
}

pub(super) async fn commit(store: &ExtentStore, txn: Transaction) {
    store.commit_transaction(txn).await.unwrap();
}

pub(super) async fn committed_write(
    store: &ExtentStore,
    db: &Db,
    inode: InodeId,
    offset: u64,
    data: Bytes,
    current_size: u64,
) {
    let mut txn = db.new_transaction().unwrap();
    store
        .write(&mut txn, inode, offset, &data, current_size)
        .await
        .unwrap();
    commit(store, txn).await;
}

/// Apply a write through the store and to a byte-array model, asserting the
/// full file reads back identically.
pub(super) async fn write_and_check(
    store: &ExtentStore,
    db: &Db,
    model: &mut Vec<u8>,
    offset: usize,
    bytes: &[u8],
) {
    committed_write(
        store,
        db,
        1,
        offset as u64,
        Bytes::copy_from_slice(bytes),
        model.len() as u64,
    )
    .await;
    let end = offset + bytes.len();
    if model.len() < end {
        model.resize(end, 0);
    }
    model[offset..end].copy_from_slice(bytes);
    assert_read_matches(store, model).await;
}

pub(super) async fn assert_read_matches(store: &ExtentStore, model: &[u8]) {
    if !model.is_empty() {
        let got = store.read(1, 0, model.len() as u64).await.unwrap();
        assert_eq!(got.as_ref(), model, "read does not match model");
    }
}

pub(super) async fn frameloc_of(
    store: &ExtentStore,
    db: &Db,
    inode: InodeId,
    extent: u64,
) -> Option<FrameLoc> {
    let key = store.key_codec.extent_key(inode, extent);
    db.get_bytes(&key)
        .await
        .unwrap()
        .and_then(|b| FrameLoc::decode(&b))
}

/// The live component of a segment's `(live, total)` counter.
pub(super) async fn segcount_of(store: &ExtentStore, db: &Db, segid: Segid) -> u64 {
    segcount_pair_of(store, db, segid).await.0
}

/// The full `(live, total)` counter for a segment.
pub(super) async fn segcount_pair_of(store: &ExtentStore, db: &Db, segid: Segid) -> (u64, u64) {
    let key = store.key_codec.segcount_key(segid.epoch, segid.counter);
    db.get_bytes(&key)
        .await
        .unwrap()
        .and_then(|b| KeyCodec::decode_segcount(&b))
        .unwrap_or((0, 0))
}

/// The counter's ground truth: sum of live frame bytes an inode still points at
/// in `segid` over `extents`.
pub(super) async fn live_bytes(
    store: &ExtentStore,
    db: &Db,
    inode: InodeId,
    extents: std::ops::Range<u64>,
    segid: Segid,
) -> u64 {
    let mut sum = 0;
    for e in extents {
        if let Some(l) = frameloc_of(store, db, inode, e).await
            && l.segid == segid
        {
            sum += l.byte_len as u64;
        }
    }
    sum
}

/// High-entropy (xorshift64), Lz4-incompressible bytes so segment sizes are
/// predictable in size-threshold tests (the test codec compresses).
pub(super) fn incompressible(seed: usize, n: usize) -> Vec<u8> {
    let mut s = (seed as u64) ^ 0x243F_6A88_85A3_08D3;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 24) as u8
        })
        .collect()
}

/// Write `data` as inode 1's extent `extent` (extents written in increasing
/// order, so the prior file size is `extent * EXTENT_SIZE`).
pub(super) async fn write_extent(store: &ExtentStore, db: &Db, extent: u64, data: &[u8]) {
    let offset = extent * EXTENT_SIZE as u64;
    committed_write(store, db, 1, offset, Bytes::copy_from_slice(data), offset).await;
}

/// Reclaim-cycle tuning for tests: production payoff floor, one job slot.
pub(super) fn test_policy() -> CyclePolicy {
    CyclePolicy {
        max_concurrent_repacks: 1,
        ..CyclePolicy::from(&ReclaimConfig::default())
    }
}

/// One reclaim cycle with the delete horizon already reached; returns
/// `(deleted, relocated)`.
pub(super) async fn reclaim(
    store: &ExtentStore,
    delete_horizon: Instant,
    checkpoint_pinned: bool,
) -> Result<(usize, usize), FsError> {
    let outcome = reclaim_tuned(
        store,
        delete_horizon,
        checkpoint_pinned,
        ReclaimConfig::DEFAULT_REPACK_MIN_DEAD_PERCENT,
    )
    .await?;
    Ok((outcome.deleted, outcome.relocated))
}

pub(super) async fn reclaim_tuned(
    store: &ExtentStore,
    delete_horizon: Instant,
    checkpoint_pinned: bool,
    repack_min_dead_percent: u64,
) -> Result<ReclaimOutcome, FsError> {
    let protection = if checkpoint_pinned {
        SegmentProtection::Indefinite
    } else {
        SegmentProtection::Until(delete_horizon)
    };
    cycle::run(
        store,
        move || std::future::ready(Ok(protection)),
        CyclePolicy {
            repack_min_dead_percent,
            ..test_policy()
        },
        &CancellationToken::new(),
    )
    .await
}
