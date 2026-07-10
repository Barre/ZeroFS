//! Object-store-backed segment writer/reader (RFC-0025).
//!
//! Seals a batch of extent frames into one immutable
//! `segments/<shard>/<epoch>/<counter>` object (durable on return) and reads a
//! single extent back by its [`FrameLoc`].
//! The counter is per-instance (reset each process open); epoch namespacing is
//! what keeps two writer terms from colliding on an object key.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use slatedb::object_store::{
    GetOptions, GetRange, MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    path::Path,
};

use crate::frame_codec::{Compressed, FrameCodec};
use crate::fs::inode::InodeId;
use crate::segment::{
    DirEntry, FOOTER_LEN, FrameLoc, LEN_PREFIX, Segid, SegmentBuilder, SegmentError,
    seal_compressed_batch,
};

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

/// Concurrent per-shard LIST chains inside [`SegmentStore::list_segments_stream`].
/// Bounds in-flight LIST requests and, with them, how much listing a retrying
/// layer below buffers at once.
const LIST_SHARD_CONCURRENCY: usize = 16;

/// Concurrent multipart part uploads per sealing segment. Higher than the
/// throughput-saturating default of 8 because per-seal wall time is fsync tail
/// latency (the durability barrier waits out in-flight seals). Parts are
/// zero-copy slices of the seal bytes, so extra concurrency costs requests in
/// flight, not buffer copies.
const SEAL_UPLOAD_CONCURRENCY: usize = 16;

/// Multipart part size, and the seal size at which `put_segment` switches from
/// a single PUT to multipart.
const SEAL_PART_SIZE: usize = 10 * 1024 * 1024;

/// Warm a just-written segment into the read (parts) cache: the multipart
/// upload bypasses the store's single-PUT write-through, so `put_segment`
/// calls this with the bytes it already holds. The hook applies any
/// object-store prefix itself. `None` when there is no such cache (tests).
pub type SegmentWarmHook = Arc<dyn Fn(&Path, Bytes) + Send + Sync>;

/// Writes and reads `segments/` objects against an object store.
pub struct SegmentStore {
    object_store: Arc<dyn ObjectStore>,
    codec: Arc<FrameCodec>,
    epoch: u64,
    counter: AtomicU64,
    /// Count of ranged segment GETs issued (a read-amplification metric).
    read_calls: AtomicU64,
    /// Warms the parts cache with a just-written segment (see
    /// [`SegmentWarmHook`]); that cache is the only segment cache.
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
        // A small (partial-fsync) seal goes up as a single PUT (which still
        // writes through the parts cache) but a full 256 MiB seal streams as
        // concurrent multipart, so the fsync-path PUT latency stays bounded
        // instead of serializing 256 MiB on one stream.
        let path = Path::from(segid.object_key());
        if bytes.len() < SEAL_PART_SIZE {
            self.object_store
                .put(&path, bytes.clone().into())
                .await
                .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        } else {
            self.put_segment_multipart(&path, &bytes).await?;
        }
        // The multipart path doesn't write through the parts cache; warm it
        // with the bytes in hand, after the upload commits (mirroring the
        // single-PUT path).
        if let Some(warm) = &self.warm {
            warm(&path, bytes);
        }
        Ok(())
    }

    /// Multipart PUT of `bytes` in `SEAL_PART_SIZE` parts, at most
    /// `SEAL_UPLOAD_CONCURRENCY` in flight. Any failure aborts the upload before
    /// the error propagates: parts of an unfinished multipart upload are
    /// invisible to LIST — and so to the orphan sweep — yet billed until
    /// aborted, and each retried seal targets a fresh upload, so leaks would
    /// accrete per failure.
    async fn put_segment_multipart(&self, path: &Path, bytes: &Bytes) -> Result<()> {
        let mut upload = self
            .object_store
            .put_multipart(path)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        // Parts run as spawned tasks, so upload progress never waits on this
        // future being polled.
        let mut parts = tokio::task::JoinSet::new();
        let uploaded = async {
            let mut rest = bytes.clone();
            while !rest.is_empty() {
                while parts.len() >= SEAL_UPLOAD_CONCURRENCY {
                    parts
                        .join_next()
                        .await
                        .expect("parts is non-empty")
                        .expect("part upload panicked")?;
                }
                let part = rest.split_to(rest.len().min(SEAL_PART_SIZE));
                parts.spawn(upload.put_part(part.into()));
            }
            while let Some(part) = parts.join_next().await {
                part.expect("part upload panicked")?;
            }
            upload.complete().await
        }
        .await;
        if let Err(e) = uploaded {
            // Best-effort cleanup: surface the seal error even if the abort
            // itself fails (leaving the parts to the backend's lifecycle rule).
            parts.shutdown().await;
            if let Err(abort_err) = upload.abort().await {
                tracing::warn!("segment seal: aborting failed upload of {path}: {abort_err}");
            }
            return Err(SegmentStoreError::ObjectStore(e.to_string()));
        }
        Ok(())
    }

    /// Seal `frames` (each `(inode, extent, full-extent plaintext)`) into one new
    /// segment object, durable on return. Returns each frame's location.
    #[allow(dead_code)] // production packs via seal_compressed; tests build plaintext worlds here
    pub async fn seal(
        &self,
        frames: &[(InodeId, u64, Bytes)],
    ) -> Result<Vec<(InodeId, u64, FrameLoc)>> {
        let compressed = frames
            .iter()
            .map(|(id, extent, data)| {
                let payload = self.codec.compress(data).map_err(SegmentError::from)?;
                Ok((*id, *extent, payload))
            })
            .collect::<Result<Vec<_>>>()?;
        self.seal_compressed(compressed).await
    }

    /// As [`Self::seal`] for already-compressed payloads (relocated frames read
    /// via [`Self::read_compressed_run`]): each re-seals under its new slot's
    /// AAD, never decompressed, so compaction's memory tracks stored size. A
    /// large batch's seals fan out on rayon ([`seal_compressed_batch`]); the
    /// appends assign offsets in the same order.
    pub async fn seal_compressed(
        &self,
        frames: Vec<(InodeId, u64, Compressed)>,
    ) -> Result<Vec<(InodeId, u64, FrameLoc)>> {
        let segid = self.next_segid();
        let sealed = seal_compressed_batch(&self.codec, segid, frames)?;
        let mut builder = SegmentBuilder::new(&self.codec, segid);
        let mut locs = Vec::with_capacity(sealed.len());
        for (id, extent, body) in sealed {
            let byte_offset = builder.byte_len();
            let frame_index = builder.append_sealed(id, extent, &body);
            let byte_len = (builder.byte_len() - byte_offset) as u32;
            locs.push((
                id,
                extent,
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
        let region = self.read_run_region(segid, byte_offset, byte_len).await?;
        let frames = crate::segment::read_frames_from_region(
            &self.codec,
            &region,
            segid,
            first_frame,
            slots,
        )?;
        Ok(frames.into_iter().map(Bytes::from).collect())
    }

    /// As [`Self::read_run`] but AEAD-verify only, returning still-compressed
    /// payloads for relocation (see [`Self::seal_compressed`]).
    pub async fn read_compressed_run(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Vec<Compressed>> {
        let region = self.read_run_region(segid, byte_offset, byte_len).await?;
        Ok(crate::segment::read_compressed_frames_from_region(
            &self.codec,
            &region,
            segid,
            first_frame,
            slots,
        )?)
    }

    /// One ranged GET of a frame run's bytes; the parts cache (warmed at seal
    /// time) absorbs the re-read and read-after-write cases.
    async fn read_run_region(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
    ) -> Result<Bytes> {
        let path = Path::from(segid.object_key());
        let region = self
            .object_store
            .get_range(&path, byte_offset..byte_offset + byte_len as u64)
            .await
            .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        Ok(region)
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
        // One listing per shard prefix (segments/00 .. segments/ff), flattened
        // with bounded concurrency. Paged LISTs are sequential within a prefix,
        // so per-shard chains parallelize the scan and shrink the unit a
        // retrying layer must buffer per attempt to 1/256th of the keyspace.
        // Shard streams interleave: consumers must not assume key order.
        futures::stream::iter((0..=0xffu8).map(|shard| Path::from(format!("segments/{shard:02x}"))))
            .map(move |prefix| self.object_store.list(Some(&prefix)))
            .flatten_unordered(LIST_SHARD_CONCURRENCY)
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
        // Defense-in-depth: a misdirected read returning a different
        // (self-consistent) segment would feed the wrong directory into the
        // coalescer.
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

/// Confirm that an object which won a concurrent create contains every frame the
/// takeover is about to reference. The existing object may be the old leader's
/// full seal (a superset of the replay tail), but it must agree on both the
/// authenticated directory entry and exact sealed bytes of each required frame.
async fn verify_existing_recon_segment(
    object_store: &Arc<dyn ObjectStore>,
    codec: &FrameCodec,
    segid: Segid,
    frames: &[ReconFrame],
) -> Result<()> {
    let path = Path::from(segid.object_key());
    let footer_result = object_store
        .get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Suffix(FOOTER_LEN as u64)),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let object_size = footer_result.meta.size;
    let footer = footer_result
        .bytes()
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let meta = crate::segment::parse_footer(&footer, object_size)?;
    if meta.segid != segid {
        return Err(SegmentError::SegidMismatch {
            expected: segid,
            found: meta.segid,
        }
        .into());
    }
    let dir_bytes = object_store
        .get_range(
            &path,
            meta.dir_offset..meta.dir_offset + meta.dir_len as u64,
        )
        .await
        .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
    let dir: HashSet<_> = crate::segment::decode_directory(codec, &dir_bytes, &footer, &meta)?
        .into_iter()
        .map(|entry| (entry.byte_offset, entry.len, entry.inode, entry.extent))
        .collect();

    for frame in frames {
        let body_len = frame
            .byte_len
            .checked_sub(LEN_PREFIX as u32)
            .ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "shipped frame is shorter than its length prefix for {segid:?}"
                ))
            })?;
        if !dir.contains(&(frame.byte_offset, body_len, frame.inode, frame.extent)) {
            return Err(SegmentStoreError::ObjectStore(format!(
                "existing segment {segid:?} is missing replayed frame {} for inode {} extent {}",
                frame.frame_index, frame.inode, frame.extent
            )));
        }
    }
    // The conflict path is uncommon but may carry a full segment's worth of
    // replay frames. Verify their ranges with bounded concurrency rather than
    // adding one object-store round trip at a time to takeover latency.
    futures::stream::iter(frames)
        .map(|frame| {
            let path = &path;
            async move {
                let end = frame
                    .byte_offset
                    .checked_add(frame.byte_len as u64)
                    .ok_or_else(|| {
                        SegmentStoreError::ObjectStore(format!(
                            "shipped frame range overflows for {segid:?}"
                        ))
                    })?;
                let existing = object_store
                    .get_range(path, frame.byte_offset..end)
                    .await
                    .map_err(|e| SegmentStoreError::ObjectStore(e.to_string()))?;
                if existing != frame.bytes {
                    return Err(SegmentStoreError::ObjectStore(format!(
                        "existing segment {segid:?} disagrees with replayed frame {} for inode {} extent {}",
                        frame.frame_index, frame.inode, frame.extent
                    )));
                }
                Ok(())
            }
        })
        .buffer_unordered(SEAL_UPLOAD_CONCURRENCY)
        .try_collect::<Vec<()>>()
        .await?;
    Ok(())
}

/// Materialize a segment object from shipped frames unless another complete
/// object already owns the immutable key. The create is atomic: a delayed old
/// leader seal can win, but takeover never overwrites it with a possibly partial
/// reconstruction. Each frame's raw `[len][sealed]` bytes go back at its
/// original `byte_offset`, so the replayed `FrameLoc`s resolve. Returns whether
/// this call created the object. HA-takeover only.
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
    let end = frames.iter().try_fold(0u64, |end, frame| {
        let frame_end = frame
            .byte_offset
            .checked_add(frame.byte_len as u64)
            .ok_or_else(|| {
                SegmentStoreError::ObjectStore(format!(
                    "shipped frame range overflows for {segid:?}"
                ))
            })?;
        Ok::<_, SegmentStoreError>(end.max(frame_end))
    })?;
    let end = usize::try_from(end).map_err(|_| {
        SegmentStoreError::ObjectStore(format!("shipped segment is too large for {segid:?}"))
    })?;
    let mut buf = vec![0u8; end];
    for f in frames {
        let start = f.byte_offset as usize;
        let stop = start + f.byte_len as usize;
        if f.byte_len < LEN_PREFIX as u32
            || stop > buf.len()
            || f.bytes.len() != f.byte_len as usize
        {
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
            len: f.byte_len - LEN_PREFIX as u32,
            inode: f.inode,
            extent: f.extent,
        })
        .collect();
    let bytes = crate::segment::finalize_segment(codec, segid, buf, &dir, segid.counter)?;
    match object_store
        .put_opts(
            &path,
            Bytes::from(bytes).into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            verify_existing_recon_segment(object_store, codec, segid, frames).await?;
            Ok(false)
        }
        Err(e) => Err(SegmentStoreError::ObjectStore(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;
    use crate::segment::SEGMENT_INFO;
    use futures::stream::BoxStream;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        CopyOptions, GetResult, ListResult, ObjectMeta, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, Result as OsResult, UploadPart,
    };
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;

    fn store() -> SegmentStore {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        SegmentStore::new(os, codec, 5, None)
    }

    /// `len` bytes of keyed xorshift noise: incompressible, so a seal built
    /// from it stays past the single-PUT threshold.
    fn noise(seed: u64, len: usize) -> Bytes {
        let mut x = seed | 1;
        let mut v = Vec::with_capacity(len + 8);
        while v.len() < len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.extend_from_slice(&x.to_le_bytes());
        }
        v.truncate(len);
        Bytes::from(v)
    }

    fn recon_frames(
        frames: &[(InodeId, u64, Bytes)],
        locs: &[(InodeId, u64, FrameLoc)],
        segment: &Bytes,
    ) -> Vec<ReconFrame> {
        frames
            .iter()
            .zip(locs)
            .map(|((inode, extent, _), (_, _, loc))| {
                let start = loc.byte_offset as usize;
                let end = start + loc.byte_len as usize;
                ReconFrame {
                    frame_index: loc.frame_index,
                    byte_offset: loc.byte_offset,
                    byte_len: loc.byte_len,
                    inode: *inode,
                    extent: *extent,
                    bytes: segment.slice(start..end),
                }
            })
            .collect()
    }

    /// Wraps `InMemory` to inject multipart or create failures and record the
    /// resulting cleanup behavior.
    #[derive(Debug)]
    struct MultipartFaultStore {
        inner: Arc<dyn ObjectStore>,
        fail_part: Option<usize>,
        fail_complete: bool,
        fail_put: bool,
        fail_after_create: AtomicBool,
        aborted: Arc<AtomicBool>,
    }

    impl MultipartFaultStore {
        fn new(fail_part: Option<usize>, fail_complete: bool) -> (Arc<Self>, Arc<AtomicBool>) {
            let aborted = Arc::new(AtomicBool::new(false));
            (
                Arc::new(Self {
                    inner: Arc::new(InMemory::new()),
                    fail_part,
                    fail_complete,
                    fail_put: false,
                    fail_after_create: AtomicBool::new(false),
                    aborted: aborted.clone(),
                }),
                aborted,
            )
        }

        fn with_create_failure(fail_after_create: bool) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                fail_part: None,
                fail_complete: false,
                fail_put: !fail_after_create,
                fail_after_create: AtomicBool::new(fail_after_create),
                aborted: Arc::new(AtomicBool::new(false)),
            })
        }

        fn injected() -> slatedb::object_store::Error {
            slatedb::object_store::Error::Generic {
                store: "MultipartFaultStore",
                source: "injected object-store fault".into(),
            }
        }
    }

    impl std::fmt::Display for MultipartFaultStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MultipartFaultStore({})", self.inner)
        }
    }

    #[derive(Debug)]
    struct FaultUpload {
        inner: Box<dyn MultipartUpload>,
        fail_part: Option<usize>,
        fail_complete: bool,
        next_part: usize,
        aborted: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for FaultUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let idx = self.next_part;
            self.next_part += 1;
            if self.fail_part == Some(idx) {
                return Box::pin(futures::future::ready(Err(MultipartFaultStore::injected())));
            }
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> OsResult<PutResult> {
            if self.fail_complete {
                return Err(MultipartFaultStore::injected());
            }
            self.inner.complete().await
        }

        async fn abort(&mut self) -> OsResult<()> {
            self.aborted.store(true, Ordering::SeqCst);
            self.inner.abort().await
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MultipartFaultStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if self.fail_put {
                return Err(Self::injected());
            }
            let is_create = matches!(&opts.mode, PutMode::Create);
            let result = self.inner.put_opts(location, payload, opts).await?;
            if is_create && self.fail_after_create.swap(false, Ordering::SeqCst) {
                return Err(Self::injected());
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, opts).await?;
            Ok(Box::new(FaultUpload {
                inner,
                fail_part: self.fail_part,
                fail_complete: self.fail_complete,
                next_part: 0,
                aborted: self.aborted.clone(),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Pauses one conditional create before it reaches the backing store, so a
    /// competing old-leader seal can deterministically win the object key.
    #[derive(Debug)]
    struct CreateGateStore {
        inner: Arc<dyn ObjectStore>,
        entered: Notify,
        release: Notify,
        gate_once: AtomicBool,
    }

    impl CreateGateStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                entered: Notify::new(),
                release: Notify::new(),
                gate_once: AtomicBool::new(true),
            })
        }
    }

    impl std::fmt::Display for CreateGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CreateGateStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CreateGateStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            if matches!(&opts.mode, PutMode::Create) && self.gate_once.swap(false, Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
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

    // A seal past the single-PUT threshold streams as concurrent multipart and
    // must read back byte-identically.
    #[tokio::test]
    async fn multipart_seal_roundtrips() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os.clone(), codec, 5, None);
        let frames: Vec<(u64, u64, Bytes)> =
            (0..4u64).map(|i| (30, i, noise(i + 1, 4 << 20))).collect();
        let locs = store.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let size = os.head(&Path::from(segid.object_key())).await.unwrap().size;
        assert!(
            size as usize > SEAL_PART_SIZE,
            "seal must take the multipart path"
        );
        for ((id, extent, data), (_, _, loc)) in frames.iter().zip(&locs) {
            let got = store.read_extent(*loc, *id, *extent).await.unwrap();
            assert_eq!(&got, data);
        }
    }

    // A part failing mid-multipart must abort the upload on the way out: an
    // unaborted upload's parts are invisible to LIST — and so to the orphan
    // sweep — yet billed until aborted, and every retried seal would strand a
    // fresh batch.
    #[tokio::test]
    async fn failed_multipart_part_aborts_the_upload() {
        let (os, aborted) = MultipartFaultStore::new(Some(1), false);
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os, codec, 5, None);
        let res = store
            .put_segment(store.next_segid(), noise(1, 2 * SEAL_PART_SIZE + 1024))
            .await;
        assert!(res.is_err(), "the seal error must surface");
        assert!(
            aborted.load(Ordering::SeqCst),
            "failed seal must abort its multipart upload"
        );
    }

    // COMPLETE failing after every part landed must abort too: those parts are
    // already durable on the backend, just as invisible and billed.
    #[tokio::test]
    async fn failed_multipart_complete_aborts_the_upload() {
        let (os, aborted) = MultipartFaultStore::new(None, true);
        let codec = FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let store = SegmentStore::new(os, codec, 5, None);
        let res = store
            .put_segment(store.next_segid(), noise(1, 2 * SEAL_PART_SIZE + 1024))
            .await;
        assert!(res.is_err(), "the seal error must surface");
        assert!(
            aborted.load(Ordering::SeqCst),
            "failed COMPLETE must abort its multipart upload"
        );
    }

    // The listing fans out one LIST per shard prefix; every sealed segment must
    // come back exactly once, whichever of the 256 shards its counter lands in.
    #[tokio::test]
    async fn list_segments_covers_all_shards_exactly_once() {
        let store = store();
        let mut expect = Vec::new();
        for i in 0..20u64 {
            let locs = store
                .seal(&[(i, 0, Bytes::from(vec![i as u8; 64]))])
                .await
                .unwrap();
            expect.push(locs[0].2.segid);
        }
        let mut listed = store.list_segments().await.unwrap();
        listed.sort_by_key(|s| (s.epoch, s.counter));
        expect.sort_by_key(|s| (s.epoch, s.counter));
        assert_eq!(listed, expect, "20 seals span 20 shard prefixes");
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

    #[tokio::test]
    async fn materialization_propagates_create_errors() {
        let object_store: Arc<dyn ObjectStore> = MultipartFaultStore::with_create_failure(false);
        let codec = FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let segid = Segid::new(9, 1);
        let frames = [ReconFrame {
            frame_index: 0,
            byte_offset: 0,
            byte_len: 5,
            inode: 1,
            extent: 0,
            bytes: Bytes::from_static(b"\x01\0\0\0x"),
        }];

        let err = materialize_segment_if_absent(&object_store, &codec, segid, &frames)
            .await
            .unwrap_err();
        assert!(matches!(err, SegmentStoreError::ObjectStore(_)));
        assert!(matches!(
            object_store.head(&Path::from(segid.object_key())).await,
            Err(slatedb::object_store::Error::NotFound { .. })
        ));
    }

    // HA takeover: the bytes the leader ships (a segment's raw frames) reconstruct
    // a segment on a fresh store that reads back identically — and atomic create
    // makes a repeat call a verified no-op.
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
        let recon = recon_frames(&frames, &locs, &seg_bytes);

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
            "verified existing object -> no-op"
        );

        let seg_b = SegmentStore::new(store_b, codec(), 9, None);
        for ((inode, extent, data), (_, _, loc)) in frames.iter().zip(&locs) {
            let got = seg_b.read_extent(*loc, *inode, *extent).await.unwrap();
            assert_eq!(&got, data, "reconstructed frame reads back identically");
        }
    }

    // The old leader may already be sealing the full segment when takeover
    // reconstructs a subset from its replication tail. If the full PUT lands
    // first, takeover's create must preserve it rather than overwrite it with
    // the partial object it prepared before discovering the winner.
    #[tokio::test]
    async fn concurrent_full_seal_wins_over_partial_takeover_reconstruction() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let leader_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let leader = SegmentStore::new(leader_os.clone(), codec(), 9, None);
        let frames = vec![
            (5u64, 0u64, Bytes::from(vec![1u8; 1000])),
            (5, 1, Bytes::from(vec![2u8; 2000])),
            (7, 0, Bytes::from(vec![3u8; 1500])),
        ];
        let locs = leader.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let full = leader_os.get(&path).await.unwrap().bytes().await.unwrap();
        let partial_recon = recon_frames(&frames[..2], &locs[..2], &full);

        let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gate = CreateGateStore::new(shared.clone());
        let takeover_store: Arc<dyn ObjectStore> = gate.clone();
        let takeover_codec = codec();
        let takeover =
            materialize_segment_if_absent(&takeover_store, &takeover_codec, segid, &partial_recon);
        let old_leader = async {
            gate.entered.notified().await;
            shared.put(&path, full.clone().into()).await.unwrap();
            gate.release.notify_one();
        };
        let (materialized, ()) = tokio::join!(takeover, old_leader);
        assert!(
            !materialized.unwrap(),
            "the old leader won the immutable segment key"
        );
        assert_eq!(
            shared.get(&path).await.unwrap().bytes().await.unwrap(),
            full,
            "takeover must not replace the full seal with its partial reconstruction"
        );

        let reader = SegmentStore::new(shared, codec(), 9, None);
        for ((inode, extent, expected), (_, _, loc)) in frames.iter().zip(&locs) {
            assert_eq!(
                reader.read_extent(*loc, *inode, *extent).await.unwrap(),
                expected
            );
        }
    }

    // A create can land while its success response is lost. Retrying the same
    // conditional request then returns AlreadyExists; byte verification turns
    // that ambiguous response into success without a second overwrite.
    #[tokio::test]
    async fn lost_create_response_is_verified_as_success() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let source_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = SegmentStore::new(source_os.clone(), codec(), 9, None);
        let frames = vec![(5u64, 0u64, Bytes::from(vec![1u8; 1000]))];
        let locs = source.seal(&frames).await.unwrap();
        let segid = locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let segment = source_os.get(&path).await.unwrap().bytes().await.unwrap();
        let recon = recon_frames(&frames, &locs, &segment);

        let fault = MultipartFaultStore::with_create_failure(true);
        let retrying: Arc<dyn ObjectStore> = Arc::new(
            crate::retrying_object_store::RetryingObjectStore::new(fault.clone()),
        );
        assert!(
            !materialize_segment_if_absent(&retrying, &codec(), segid, &recon)
                .await
                .unwrap(),
            "the retry observes the object created by the attempt whose response was lost"
        );
        let reader = SegmentStore::new(fault.inner.clone(), codec(), 9, None);
        assert_eq!(
            reader
                .read_extent(locs[0].2, frames[0].0, frames[0].1)
                .await
                .unwrap(),
            frames[0].2
        );
    }

    #[tokio::test]
    async fn existing_segment_mismatch_aborts_takeover() {
        let codec = || FrameCodec::new(&[3u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let wanted_os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wanted = SegmentStore::new(wanted_os.clone(), codec(), 9, None);
        let wanted_frames = vec![(5u64, 0u64, Bytes::from(vec![1u8; 1000]))];
        let wanted_locs = wanted.seal(&wanted_frames).await.unwrap();
        let segid = wanted_locs[0].2.segid;
        let path = Path::from(segid.object_key());
        let wanted_bytes = wanted_os.get(&path).await.unwrap().bytes().await.unwrap();
        let recon = recon_frames(&wanted_frames, &wanted_locs, &wanted_bytes);

        // A fresh writer with the same epoch/counter produces the same key but
        // different sealed bytes. Create must not overwrite it, and verification
        // must not let replay publish a pointer into the conflicting object.
        let existing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let conflicting = SegmentStore::new(existing.clone(), codec(), 9, None);
        let conflicting_frames = vec![(5u64, 0u64, Bytes::from(vec![9u8; 1000]))];
        let conflicting_locs = conflicting.seal(&conflicting_frames).await.unwrap();
        assert_eq!(conflicting_locs[0].2.segid, segid);
        let before = existing.get(&path).await.unwrap().bytes().await.unwrap();

        let err = materialize_segment_if_absent(&existing, &codec(), segid, &recon)
            .await
            .unwrap_err();
        assert!(matches!(err, SegmentStoreError::ObjectStore(_)));
        assert_eq!(
            existing.get(&path).await.unwrap().bytes().await.unwrap(),
            before,
            "failed verification must leave the existing object untouched"
        );
    }
}
