//! Shared fixtures for the extent tests: in-memory store construction and
//! model-checked write/read helpers.

use super::ExtentStore;
use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::CompressionConfig;
use crate::db::{Db, Transaction};
use crate::frame_codec::FrameCodec;
use crate::fs::EXTENT_SIZE;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::lock_manager::KeyedLockManager;
use crate::segment::{FrameLoc, SEGMENT_INFO, Segid};
use crate::segment_store::SegmentStore;
use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{ObjectStore, path::Path};
use slatedb::{BlockTransformer, DbBuilder};
use std::sync::Arc;

pub(super) async fn make() -> (ExtentStore, Arc<Db>) {
    let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
    (store, db)
}

pub(super) async fn make_with_compression(
    compression: CompressionConfig,
) -> (ExtentStore, Arc<Db>, Arc<dyn ObjectStore>) {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bt: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
    let slatedb = Arc::new(
        DbBuilder::new(Path::from("t"), object_store.clone())
            .with_block_transformer(bt)
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let db = Arc::new(Db::new(slatedb, None));
    let store = make_store(object_store.clone(), db.clone(), compression, 7);
    (store, db, object_store)
}

/// A store over given backing state, as a restarted process would build:
/// fresh in-RAM state, and a higher epoch than any earlier incarnation's.
pub(super) fn make_store(
    object_store: Arc<dyn ObjectStore>,
    db: Arc<Db>,
    compression: CompressionConfig,
    epoch: u64,
) -> ExtentStore {
    let key_codec = Arc::new(KeyCodec::new());
    let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, compression);
    let segments = Arc::new(SegmentStore::new(object_store, codec, epoch, None));
    ExtentStore::new(db, key_codec, segments, Arc::new(KeyedLockManager::new()))
        .with_seal_threshold(8 * 1024 * 1024)
}

pub(super) async fn commit(store: &ExtentStore, txn: Transaction) {
    // No commit worker in these tests, so commit_via_coordinator takes its
    // fallback path: this test task is the sole segcount writer.
    store.commit_via_coordinator(txn).await.unwrap();
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
    let mut txn = db.new_transaction().unwrap();
    let tu = store
        .write(
            &mut txn,
            1,
            offset as u64,
            &Bytes::copy_from_slice(bytes),
            model.len() as u64,
        )
        .await
        .unwrap();
    commit(store, txn).await;
    store.apply_tail_update(1, tu);
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
    let mut txn = db.new_transaction().unwrap();
    let tu = store
        .write(
            &mut txn,
            1,
            extent * EXTENT_SIZE as u64,
            &Bytes::copy_from_slice(data),
            extent * EXTENT_SIZE as u64,
        )
        .await
        .unwrap();
    commit(store, txn).await;
    store.apply_tail_update(1, tu);
}
