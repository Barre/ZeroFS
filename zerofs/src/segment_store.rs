//! Object-store-backed segment writer/reader (RFC-0025).
//!
//! Seals a batch of extent frames into one immutable
//! `segments/<shard>/<epoch>/<counter>` object (durable on return) and reads a
//! single extent back by its [`FrameLoc`].
//! The counter is per-instance (reset each process open); epoch namespacing is
//! what keeps two writer terms from colliding on an object key.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use slatedb::object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};

use crate::frame_codec::FrameCodec;
use crate::fs::inode::InodeId;
use crate::segment::{DirEntry, FrameLoc, Segid, SegmentBuilder, SegmentError};

#[derive(Debug, thiserror::Error)]
pub enum SegmentStoreError {
    #[error("segment object store error: {0}")]
    ObjectStore(String),
    /// The segment object does not exist. Distinguished from a transient
    /// `ObjectStore` error so reclamation can tell "the object is genuinely gone"
    /// (trivially dead) from "the store is momentarily unreachable" (fail-closed).
    #[error("segment object not found")]
    NotFound,
    #[error(transparent)]
    Segment(#[from] SegmentError),
}

type Result<T> = std::result::Result<T, SegmentStoreError>;

/// Warm a just-written segment into the object store's read (parts) cache. The
/// production upload streams as multipart, which bypasses the store's single-PUT
/// write-through, so `put_segment` calls this with the bytes it already holds. The
/// hook receives the segment's object path and full bytes and is responsible for
/// applying any object-store prefix. `None` when there is no such cache (tests).
pub type SegmentWarmHook = Arc<dyn Fn(&Path, Bytes) + Send + Sync>;

/// Writes and reads `segments/` objects against an object store.
pub struct SegmentStore {
    object_store: Arc<dyn ObjectStore>,
    codec: Arc<FrameCodec>,
    epoch: u64,
    counter: AtomicU64,
    /// Count of ranged segment GETs issued (a read-amplification metric).
    read_calls: AtomicU64,
    /// Warms the parts cache with a just-written segment (see [`SegmentWarmHook`]).
    /// That cache (RAM + disk, the bulk of the configured budget) is the only
    /// segment cache; same-process read-your-writes and the in-flight-PUT window
    /// are served upstream from the open/sealing buffers before a read reaches here.
    warm: Option<SegmentWarmHook>,
}

impl SegmentStore {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        codec: FrameCodec,
        epoch: u64,
        warm: Option<SegmentWarmHook>,
    ) -> Self {
        Self {
            object_store,
            codec: Arc::new(codec),
            epoch,
            counter: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            warm,
        }
    }

    /// A shared handle to the frame codec (for the open-segment buffer).
    pub fn codec(&self) -> Arc<FrameCodec> {
        Arc::clone(&self.codec)
    }

    /// Allocate the next epoch-namespaced segment id.
    pub fn next_segid(&self) -> Segid {
        Segid::new(self.epoch, self.counter.fetch_add(1, Ordering::Relaxed))
    }

    /// PUT pre-built segment bytes (durable on return). Used by the open-segment
    /// buffer's seal, which builds the bytes itself via `seal_directory` +
    /// `assemble_segment`.
    pub async fn put_segment(&self, segid: Segid, bytes: Bytes) -> Result<()> {
        // BufWriter uploads a small (partial-fsync) seal as a single PUT (which
        // still writes through the parts cache) but streams a full 256 MiB seal
        // as concurrent multipart, so the fsync-path PUT latency stays bounded
        // instead of serializing 256 MiB on one stream.
        let path = Path::from(segid.object_key());
        use tokio::io::AsyncWriteExt;
        let mut w = slatedb::object_store::buffered::BufWriter::new(
            self.object_store.clone(),
            path.clone(),
        )
        .with_max_concurrency(8);
        w.write_all(&bytes)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        w.shutdown()
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        // The multipart path above doesn't write through the parts cache; warm it
        // with the bytes we already hold so a read after this segment ages out of
        // the small RAM cache hits the (disk-backed) parts cache, not the object
        // store. Runs after the upload commits, mirroring the single-PUT path.
        if let Some(warm) = &self.warm {
            warm(&path, bytes);
        }
        Ok(())
    }

    /// Seal `frames` (each `(inode, extent, full-extent plaintext)`) into one new
    /// segment object, durable on return. Returns each frame's location.
    pub async fn seal(
        &self,
        frames: &[(InodeId, u64, Bytes)],
    ) -> Result<Vec<(InodeId, u64, FrameLoc)>> {
        let segid = self.next_segid();
        let mut builder = SegmentBuilder::new(&self.codec, segid);
        let mut locs = Vec::with_capacity(frames.len());
        for (id, extent, data) in frames {
            let byte_offset = builder.byte_len();
            let frame_index = builder.add_frame(*id, *extent, data.as_ref())?;
            let byte_len = (builder.byte_len() - byte_offset) as u32;
            locs.push((
                *id,
                *extent,
                FrameLoc {
                    segid,
                    frame_index,
                    byte_offset,
                    byte_len,
                },
            ));
        }
        let bytes = builder.finish(segid.counter)?;
        self.put_segment(segid, Bytes::from(bytes)).await?;
        Ok(locs)
    }

    /// Read one extent's plaintext via a ranged GET of just its frame.
    pub async fn read_extent(&self, loc: FrameLoc, id: InodeId, extent: u64) -> Result<Bytes> {
        let mut frames = self
            .read_run(
                loc.segid,
                loc.byte_offset,
                loc.byte_len,
                loc.frame_index,
                &[(id, extent)],
            )
            .await?;
        Ok(frames.pop().expect("one frame"))
    }

    /// Read a contiguous run of `slots.len()` frames from `segid` in one ranged
    /// GET over `[byte_offset, byte_offset + byte_len)`, returning each plaintext.
    /// `slots[i]` is the `(inode, extent)` of the frame at `first_frame + i`.
    pub async fn read_run(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Vec<Bytes>> {
        // A ranged GET of just this run's bytes; the parts cache (warmed at seal
        // time) absorbs the re-read and read-after-write cases.
        let path = Path::from(segid.object_key());
        let region = self
            .object_store
            .get_range(&path, byte_offset..byte_offset + byte_len as u64)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        let frames = crate::segment::read_frames_from_region(
            &self.codec,
            &region,
            segid,
            first_frame,
            slots,
        )?;
        Ok(frames.into_iter().map(Bytes::from).collect())
    }
}

/// GC/maintenance primitives.
impl SegmentStore {
    /// This writer's epoch (segids it produces are namespaced under it).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Ranged segment GETs issued so far (read-amplification metric).
    #[cfg(test)]
    pub fn read_calls(&self) -> u64 {
        self.read_calls.load(Ordering::Relaxed)
    }

    /// List every `segments/` object currently present. Production reclaim
    /// streams via [`Self::list_segments_stream`]; this collected form serves
    /// the tests and the failpoints harness.
    #[allow(dead_code)] // used by tests and the failpoints wrapper, not the bin
    pub async fn list_segments(&self) -> Result<Vec<Segid>> {
        use futures::TryStreamExt;
        self.list_segments_stream()
            .map_ok(|(segid, _, _)| segid)
            .try_collect()
            .await
    }

    /// Stream `(Segid, size, last_modified)` for every segment object, so the GC
    /// can classify on the fly instead of buffering the whole listing
    /// (O(#segments)) in RAM. `last_modified` is the object's creation time
    /// (segments are immutable), used to protect anything that could predate a
    /// persistent checkpoint.
    pub fn list_segments_stream(
        &self,
    ) -> impl futures::Stream<Item = Result<(Segid, u64, chrono::DateTime<chrono::Utc>)>> + '_ {
        self.object_store
            .list(Some(&Path::from("segments")))
            .filter_map(|meta| {
                futures::future::ready(match meta {
                    Ok(m) => Segid::from_object_key(m.location.as_ref())
                        .map(|s| Ok((s, m.size, m.last_modified))),
                    Err(e) => Some(Err(SegmentStoreError::ObjectStore(e.to_string()))),
                })
            })
    }

    /// Delete one segment object.
    pub async fn delete_segment(&self, segid: Segid) -> Result<()> {
        self.object_store
            .delete(&Path::from(segid.object_key()))
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))
    }

    /// Read and decrypt a segment's reverse-map directory (which frame backs
    /// which logical block), for the coalescer.
    pub async fn read_directory(&self, segid: Segid) -> Result<Vec<DirEntry>> {
        let path = Path::from(segid.object_key());
        // Fetch just the footer (last FOOTER_LEN bytes) to locate the directory,
        // then a ranged GET of the directory itself — never the whole object.
        let footer_res = self
            .object_store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(crate::segment::FOOTER_LEN as u64)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                slatedb::object_store::Error::NotFound { .. } => SegmentStoreError::NotFound,
                other => SegmentStoreError::ObjectStore(other.to_string()),
            })?;
        let object_size = footer_res.meta.size;
        let footer = footer_res
            .bytes()
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        let meta = crate::segment::parse_footer(&footer, object_size)?;
        // Defense-in-depth: the object at this key must be the segment we asked for.
        // A misdirected read or a key collision returning a different (self-consistent)
        // segment would otherwise feed the wrong directory into the coalescer.
        if meta.segid != segid {
            return Err(crate::segment::SegmentError::SegidMismatch {
                expected: segid,
                found: meta.segid,
            }
            .into());
        }
        let dir_bytes = self
            .object_store
            .get_range(
                &path,
                meta.dir_offset..meta.dir_offset + meta.dir_len as u64,
            )
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        Ok(crate::segment::decode_directory(
            &self.codec,
            &dir_bytes,
            &footer,
            &meta,
        )?)
    }
}

/// A shipped frame, for the HA standby to rebuild an un-PUT segment on takeover.
pub struct ReconFrame {
    pub frame_index: u32,
    pub byte_offset: u64,
    pub byte_len: u32,
    pub inode: InodeId,
    pub extent: u64,
    pub bytes: Bytes,
}

/// Materialize a segment object from shipped frames, unless it is already on the
/// store (HEAD-guard — never overwrite a leader-sealed object, which may include
/// frames the standby never received). Each frame's raw `[len][sealed]` bytes go
/// back at their original `byte_offset`, so the replayed `FrameLoc`s resolve.
/// Returns whether a PUT happened. HA-takeover only.
pub async fn materialize_segment_if_absent(
    object_store: &Arc<dyn ObjectStore>,
    codec: &FrameCodec,
    segid: Segid,
    frames: &[ReconFrame],
) -> Result<bool> {
    if frames.is_empty() {
        return Ok(false);
    }
    let path = Path::from(segid.object_key());
    if object_store.head(&path).await.is_ok() {
        return Ok(false);
    }
    let end = frames
        .iter()
        .map(|f| f.byte_offset + f.byte_len as u64)
        .max()
        .unwrap_or(0) as usize;
    let mut buf = vec![0u8; end];
    for f in frames {
        let start = f.byte_offset as usize;
        let stop = start + f.byte_len as usize;
        if stop > buf.len() || f.bytes.len() != f.byte_len as usize {
            return Err(SegmentStoreError::ObjectStore(format!(
                "shipped frame layout mismatch for {segid:?}"
            )));
        }
        buf[start..stop].copy_from_slice(&f.bytes);
    }
    let mut sorted: Vec<&ReconFrame> = frames.iter().collect();
    sorted.sort_by_key(|f| f.frame_index);
    let dir: Vec<DirEntry> = sorted
        .iter()
        .map(|f| DirEntry {
            byte_offset: f.byte_offset,
            len: f.byte_len.saturating_sub(crate::segment::LEN_PREFIX as u32),
            inode: f.inode,
            extent: f.extent,
        })
        .collect();
    let bytes = crate::segment::finalize_segment(codec, segid, buf, &dir, segid.counter)?;
    object_store
        .put(&path, Bytes::from(bytes).into())
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;
    use crate::segment::SEGMENT_INFO;
    use slatedb::object_store::memory::InMemory;

    fn store() -> SegmentStore {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        SegmentStore::new(os, codec, 5, None)
    }

    #[tokio::test]
    async fn seal_then_read_roundtrips() {
        let store = store();
        let frames = vec![
            (10u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (10, 1, Bytes::from(vec![2u8; 2000])),
            (20, 0, Bytes::from(vec![3u8; 500])),
        ];
        let locs = store.seal(&frames).await.unwrap();
        assert_eq!(locs.len(), 3);
        for ((id, extent, data), (lid, lextent, loc)) in frames.iter().zip(&locs) {
            assert_eq!(id, lid);
            assert_eq!(extent, lextent);
            let got = store.read_extent(*loc, *id, *extent).await.unwrap();
            assert_eq!(&got, data);
        }
    }

    #[tokio::test]
    async fn read_under_wrong_slot_fails() {
        let store = store();
        let locs = store
            .seal(&[(10u64, 0u64, Bytes::from(vec![7u8; 100]))])
            .await
            .unwrap();
        let (_, _, loc) = locs[0];
        assert!(store.read_extent(loc, 999, 999).await.is_err());
    }

    #[tokio::test]
    async fn distinct_seals_get_distinct_segids() {
        let store = store();
        let a = store
            .seal(&[(1, 0, Bytes::from_static(b"a"))])
            .await
            .unwrap();
        let b = store
            .seal(&[(1, 0, Bytes::from_static(b"b"))])
            .await
            .unwrap();
        assert_ne!(a[0].2.segid, b[0].2.segid, "counter must advance per seal");
        assert_eq!(
            store.read_extent(a[0].2, 1, 0).await.unwrap().as_ref(),
            b"a"
        );
        assert_eq!(
            store.read_extent(b[0].2, 1, 0).await.unwrap().as_ref(),
            b"b"
        );
    }

    // Two databases sharing one bucket must not see — or clobber — each other's
    // segments. Both writers start at the same epoch, so their segids collide
    // (segments/<shard>/<e>/<c> is identical); only the per-db path prefix keeps the
    // objects apart. Without it, db_b's seal overwrites db_a's at the shared key.
    #[tokio::test]
    async fn segments_are_isolated_by_db_path_prefix() {
        use slatedb::object_store::prefix::PrefixStore;

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = || FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store_a: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(bucket.clone(), Path::from("db_a")));
        let store_b: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(bucket.clone(), Path::from("db_b")));
        let seg_a = SegmentStore::new(store_a, codec(), 5, None);
        let seg_b = SegmentStore::new(store_b, codec(), 5, None);

        let a = seg_a
            .seal(&[(1, 0, Bytes::from_static(b"aaaa"))])
            .await
            .unwrap();
        let b = seg_b
            .seal(&[(1, 0, Bytes::from_static(b"bbbb"))])
            .await
            .unwrap();
        assert_eq!(
            a[0].2.segid, b[0].2.segid,
            "fresh dbs reuse the same segids"
        );

        // Reads go straight to the object store (no in-RAM segment cache).
        assert_eq!(
            seg_a.read_extent(a[0].2, 1, 0).await.unwrap().as_ref(),
            b"aaaa"
        );
        assert_eq!(
            seg_b.read_extent(b[0].2, 1, 0).await.unwrap().as_ref(),
            b"bbbb"
        );

        // Each db lists only its own segment, not the other's.
        assert_eq!(seg_a.list_segments().await.unwrap().len(), 1);
        assert_eq!(seg_b.list_segments().await.unwrap().len(), 1);
    }

    // HA takeover: the bytes the leader ships (a segment's raw frames) reconstruct
    // a segment on a fresh store that reads back identically — and the HEAD-guard
    // makes a repeat call a no-op (never overwrite a leader-sealed object).
    #[tokio::test]
    async fn shipped_frames_reconstruct_a_readable_segment() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);

        // Leader: seal frames into store A; that object's bytes are what's shipped.
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let seg_a = SegmentStore::new(store_a.clone(), codec(), 9, None);
        let frames = vec![
            (5u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (5, 1, Bytes::from(vec![2u8; 2000])),
            (7, 0, Bytes::from(vec![3u8; 1500])),
        ];
        let locs = seg_a.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let seg_bytes = store_a
            .get(&Path::from(segid.object_key()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // Standby: rebuild ReconFrames from each FrameLoc + the shipped raw bytes.
        let recon: Vec<ReconFrame> = frames
            .iter()
            .zip(&locs)
            .map(|((inode, extent, _), (_, _, loc))| {
                let s = loc.byte_offset as usize;
                let e = s + loc.byte_len as usize;
                ReconFrame {
                    frame_index: loc.frame_index,
                    byte_offset: loc.byte_offset,
                    byte_len: loc.byte_len,
                    inode: *inode,
                    extent: *extent,
                    bytes: seg_bytes.slice(s..e),
                }
            })
            .collect();

        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert!(
            materialize_segment_if_absent(&store_b, &codec(), segid, &recon)
                .await
                .unwrap(),
            "absent on the fresh store -> materialized"
        );
        assert!(
            !materialize_segment_if_absent(&store_b, &codec(), segid, &recon)
                .await
                .unwrap(),
            "HEAD-guard: already present -> no-op"
        );

        let seg_b = SegmentStore::new(store_b, codec(), 9, None);
        for ((inode, extent, data), (_, _, loc)) in frames.iter().zip(&locs) {
            let got = seg_b.read_extent(*loc, *inode, *extent).await.unwrap();
            assert_eq!(&got, data, "reconstructed frame reads back identically");
        }
    }
}
