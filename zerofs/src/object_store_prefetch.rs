use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use foyer::{Cache, HybridCache};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

pub const DEFAULT_PART_SIZE_BYTES: usize = 128 * 1024;
const HEADS_CAPACITY_ENTRIES: usize = 16 * 1024;
const ACCESS_TRACKER_CAPACITY: usize = 8 * 1024;

const FETCH_WINDOW_MIN: usize = 128 * 1024;
const FETCH_WINDOW_MAX: usize = 8 * 1024 * 1024;
const MAX_STREAMS: usize = 4;

type PartId = usize;

#[derive(Clone, Copy)]
struct Stream {
    last_offset: u64,
    fetch_window: usize,
}

struct AccessHistory {
    streams: [Stream; MAX_STREAMS],
    len: usize,
    stride_limit: u64,
}

impl AccessHistory {
    fn new(part_size: usize) -> Self {
        Self {
            streams: [Stream {
                last_offset: u64::MAX,
                fetch_window: FETCH_WINDOW_MIN,
            }; MAX_STREAMS],
            len: 0,
            stride_limit: part_size as u64 * 4,
        }
    }

    fn record(&mut self, offset: u64) -> usize {
        if let Some(i) = self.find_stream(offset) {
            let s = &mut self.streams[i];
            s.last_offset = offset;
            s.fetch_window = (s.fetch_window * 2).min(FETCH_WINDOW_MAX);
            return s.fetch_window;
        }

        let slot = if self.len < MAX_STREAMS {
            let s = self.len;
            self.len += 1;
            s
        } else {
            self.weakest_slot()
        };

        self.streams[slot] = Stream {
            last_offset: offset,
            fetch_window: FETCH_WINDOW_MIN,
        };
        FETCH_WINDOW_MIN
    }

    fn weakest_slot(&self) -> usize {
        let mut min_idx = 0;
        let mut min_window = usize::MAX;
        for i in 0..self.len {
            if self.streams[i].fetch_window < min_window {
                min_window = self.streams[i].fetch_window;
                min_idx = i;
            }
        }
        min_idx
    }

    fn find_stream(&self, offset: u64) -> Option<usize> {
        let mut best = None;
        let mut best_dist = u64::MAX;
        for i in 0..self.len {
            let s = &self.streams[i];
            if offset >= s.last_offset && offset.saturating_sub(s.last_offset) <= self.stride_limit
            {
                let dist = offset - s.last_offset;
                if dist < best_dist {
                    best = Some(i);
                    best_dist = dist;
                }
            }
        }
        best
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartKey {
    location: String,
    part_id: PartId,
}

impl PartKey {
    fn new(location: &Path, part_id: PartId) -> Self {
        Self {
            location: location.as_ref().to_string(),
            part_id,
        }
    }
}

#[derive(Clone)]
struct CachedHead {
    meta: ObjectMeta,
    attributes: Attributes,
}

pub struct PrefetchingObjectStore {
    inner: Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    parts: HybridCache<PartKey, Bytes>,
    heads: Cache<Path, Arc<CachedHead>>,
    access_tracker: Cache<Path, Arc<Mutex<AccessHistory>>>,
}

impl PrefetchingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, parts: HybridCache<PartKey, Bytes>) -> Self {
        Self::with_options(inner, parts, DEFAULT_PART_SIZE_BYTES)
    }

    pub fn with_options(
        inner: Arc<dyn ObjectStore>,
        parts: HybridCache<PartKey, Bytes>,
        part_size_bytes: usize,
    ) -> Self {
        assert!(
            part_size_bytes > 0 && part_size_bytes.is_multiple_of(1024),
            "part_size_bytes must be a positive multiple of 1024"
        );
        let heads = foyer::CacheBuilder::new(HEADS_CAPACITY_ENTRIES)
            .with_name("zerofs-object-prefetch-heads")
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();
        let access_tracker = foyer::CacheBuilder::new(ACCESS_TRACKER_CAPACITY)
            .with_name("zerofs-object-prefetch-access-tracker")
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();
        Self {
            inner,
            part_size_bytes,
            parts,
            heads,
            access_tracker,
        }
    }

    fn save_head(&self, location: &Path, meta: &ObjectMeta, attrs: &Attributes) {
        self.heads.insert(
            location.clone(),
            Arc::new(CachedHead {
                meta: meta.clone(),
                attributes: attrs.clone(),
            }),
        );
    }

    fn read_head(&self, location: &Path) -> Option<(ObjectMeta, Attributes)> {
        self.heads
            .get(location)
            .map(|entry| (entry.value().meta.clone(), entry.value().attributes.clone()))
    }

    fn save_part(&self, location: &Path, part_id: PartId, bytes: Bytes) {
        self.parts.insert(PartKey::new(location, part_id), bytes);
    }

    #[cfg(test)]
    async fn cached_part(&self, location: &Path, part_id: PartId) -> Option<Bytes> {
        self.parts
            .get(&PartKey::new(location, part_id))
            .await
            .ok()
            .flatten()
            .map(|entry| entry.value().clone())
    }

    fn invalidate(&self, location: &Path) {
        self.heads.remove(location);
    }

    fn record_access(&self, location: &Path, offset: u64) -> usize {
        let entry = self
            .access_tracker
            .get(location)
            .map(|e| e.value().clone())
            .unwrap_or_else(|| {
                let hist = Arc::new(Mutex::new(AccessHistory::new(self.part_size_bytes)));
                self.access_tracker.insert(location.clone(), hist.clone());
                hist
            });
        let mut history = entry.lock().unwrap();
        history.record(offset)
    }

    async fn cached_head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        if let Some((meta, _)) = self.read_head(location) {
            return Ok(meta);
        }
        let result = self
            .inner
            .get_opts(
                location,
                GetOptions {
                    range: None,
                    head: true,
                    ..Default::default()
                },
            )
            .await?;
        let meta = result.meta.clone();
        let _ = self.save_get_result(location, result).await;
        Ok(meta)
    }

    async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        if opts.if_match.is_some()
            || opts.if_none_match.is_some()
            || opts.if_modified_since.is_some()
            || opts.if_unmodified_since.is_some()
            || opts.version.is_some()
            || opts.head
        {
            return self.inner.get_opts(location, opts).await;
        }

        let access_offset = self.range_start_offset(&opts.range);
        let fetch_window = self.record_access(location, access_offset);

        let (meta, attributes) = self
            .maybe_prefetch_range(location, opts.clone(), fetch_window)
            .await?;
        let range = self.canonicalize_range(opts.range.clone(), meta.size)?;
        let parts = self.split_range_into_parts(range.clone());

        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| {
                self.read_part(location.clone(), part_id, range_in_part, fetch_window)
            })
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        mut opts: GetOptions,
        fetch_window: usize,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        if let Some((meta, attrs)) = self.read_head(location) {
            return Ok((meta, attrs));
        }

        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range, fetch_window));
        }

        let get_result = self.inner.get_opts(location, opts).await?;
        let meta = get_result.meta.clone();
        let attrs = get_result.attributes.clone();
        let _ = self.save_get_result(location, get_result).await;
        Ok((meta, attrs))
    }

    async fn save_get_result(
        &self,
        location: &Path,
        result: GetResult,
    ) -> object_store::Result<()> {
        self.save_head(location, &result.meta, &result.attributes);

        let part_size = self.part_size_bytes as u64;
        let aligned_start = result.range.start.is_multiple_of(part_size);
        let aligned_end =
            result.range.end.is_multiple_of(part_size) || result.range.end == result.meta.size;
        if !(aligned_start && aligned_end) {
            return Ok(());
        }

        let start_part: PartId = (result.range.start / part_size)
            .try_into()
            .expect("part number exceeds usize");

        let stream = result.into_stream();
        self.save_parts_stream(location, stream, start_part).await
    }

    async fn save_parts_stream<S>(
        &self,
        location: &Path,
        mut stream: S,
        start_part_number: PartId,
    ) -> object_store::Result<()>
    where
        S: stream::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    {
        let mut buffer = BytesMut::new();
        let mut part_number = start_part_number;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);
            while buffer.len() >= self.part_size_bytes {
                let to_write = buffer.split_to(self.part_size_bytes);
                self.save_part(location, part_number, to_write.freeze());
                part_number += 1;
            }
        }

        if !buffer.is_empty() {
            self.save_part(location, part_number, buffer.freeze());
        }
        Ok(())
    }

    fn split_range_into_parts(&self, range: Range<u64>) -> Vec<(PartId, Range<usize>)> {
        let part_size_u64 = self.part_size_bytes as u64;
        let aligned = self.align_range(&range, self.part_size_bytes);
        let start_part = aligned.start / part_size_u64;
        let end_part = aligned.end / part_size_u64;
        let mut parts: Vec<_> = (start_part..end_part)
            .map(|part_id| {
                (
                    usize::try_from(part_id).expect("part id exceeds usize"),
                    Range {
                        start: 0,
                        end: self.part_size_bytes,
                    },
                )
            })
            .collect();
        if parts.is_empty() {
            return vec![];
        }
        if let Some(first) = parts.first_mut() {
            first.1.start = usize::try_from(range.start % part_size_u64)
                .expect("part_size too large for usize");
        }
        if let Some(last) = parts.last_mut()
            && !range.end.is_multiple_of(part_size_u64)
        {
            last.1.end =
                usize::try_from(range.end % part_size_u64).expect("part_size too large for usize");
        }
        parts
    }

    fn read_part(
        &self,
        location: Path,
        part_id: PartId,
        range_in_part: Range<usize>,
        fetch_window: usize,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let inner = self.inner.clone();
        let part_size_bytes = self.part_size_bytes;
        let parts = self.parts.clone();
        let heads = self.heads.clone();
        Box::pin(async move {
            let key = PartKey::new(&location, part_id);
            if let Ok(Some(entry)) = parts.get(&key).await {
                let bytes = entry.value().clone();
                if range_in_part.end <= bytes.len() {
                    return Ok(bytes.slice(range_in_part));
                }
                parts.remove(&key);
            }

            let extra_parts = fetch_window / part_size_bytes;
            let fetch_start = part_id;
            let fetch_end = fetch_start + extra_parts.max(1);
            let fetch_range = Range {
                start: (fetch_start * part_size_bytes) as u64,
                end: (fetch_end * part_size_bytes) as u64,
            };
            let get_result = inner
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(fetch_range)),
                        ..Default::default()
                    },
                )
                .await?;
            let meta = get_result.meta.clone();
            let attrs = get_result.attributes.clone();
            let all_bytes = get_result.bytes().await?;

            heads.insert(
                location.clone(),
                Arc::new(CachedHead {
                    meta,
                    attributes: attrs,
                }),
            );

            for i in 0..extra_parts.max(1) {
                let start = i * part_size_bytes;
                let end = ((i + 1) * part_size_bytes).min(all_bytes.len());
                if start >= all_bytes.len() {
                    break;
                }
                parts.insert(
                    PartKey::new(&location, fetch_start + i),
                    all_bytes.slice(start..end),
                );
            }

            let bytes = parts
                .get(&key)
                .await
                .ok()
                .flatten()
                .map(|e| e.value().clone())
                .unwrap_or_else(|| all_bytes.slice(0..part_size_bytes.min(all_bytes.len())));

            let end = range_in_part.end.min(bytes.len());
            let start = range_in_part.start.min(end);
            Ok(bytes.slice(start..end))
        })
    }

    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        object_size: u64,
    ) -> object_store::Result<Range<u64>> {
        let (start, end) = match range {
            None => (0, object_size),
            Some(GetRange::Bounded(r)) => {
                if r.start >= object_size {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("range start {} >= size {}", r.start, object_size).into(),
                    });
                }
                if r.start >= r.end {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("inconsistent range {}..{}", r.start, r.end).into(),
                    });
                }
                (r.start, r.end.min(object_size))
            }
            Some(GetRange::Offset(o)) => {
                if o >= object_size {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("offset {} >= size {}", o, object_size).into(),
                    });
                }
                (o, object_size)
            }
            Some(GetRange::Suffix(s)) => (object_size.saturating_sub(s), object_size),
        };
        Ok(Range { start, end })
    }

    fn range_start_offset(&self, range: &Option<GetRange>) -> u64 {
        match range {
            None => 0,
            Some(GetRange::Bounded(r)) => r.start,
            Some(GetRange::Offset(o)) => *o,
            Some(GetRange::Suffix(_)) => u64::MAX,
        }
    }

    fn align_get_range(&self, range: &GetRange, fetch_window: usize) -> GetRange {
        let part = self.part_size_bytes;
        match range {
            GetRange::Bounded(r) => {
                let part_aligned = self.align_range(r, part);
                let expanded = self.align_range(
                    &Range {
                        start: part_aligned.start,
                        end: (r.start + fetch_window as u64).max(part_aligned.end),
                    },
                    part,
                );
                GetRange::Bounded(expanded)
            }
            GetRange::Suffix(s) => {
                let want = (*s).max(fetch_window as u64);
                GetRange::Suffix(self.align_range(&(0..want), part).end)
            }
            GetRange::Offset(o) => GetRange::Offset(*o - *o % part as u64),
        }
    }

    fn align_range(&self, range: &Range<u64>, alignment: usize) -> Range<u64> {
        let alignment = alignment as u64;
        Range {
            start: range.start - range.start % alignment,
            end: range.end.div_ceil(alignment) * alignment,
        }
    }
}

impl Debug for PrefetchingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefetchingObjectStore")
            .field("inner", &self.inner)
            .field("part_size_bytes", &self.part_size_bytes)
            .finish()
    }
}

impl Display for PrefetchingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "PrefetchingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PrefetchingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.cached_get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.cached_head(location).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.invalidate(location);
        Ok(result)
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let result = self.inner.delete(location).await;
        self.invalidate(location);
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let result = self.inner.copy(from, to).await;
        self.invalidate(to);
        result
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let result = self.inner.rename(from, to).await;
        self.invalidate(from);
        self.invalidate(to);
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let result = self.inner.copy_if_not_exists(from, to).await;
        if result.is_ok() {
            self.invalidate(to);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foyer::{BlockEngineConfig, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig};
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    async fn make_store(
        part_size: usize,
        memory_capacity: usize,
        disk_capacity: usize,
    ) -> (PrefetchingObjectStore, Arc<InMemory>, TempDir) {
        let inner = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("test-parts")
            .memory(memory_capacity)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(disk_capacity),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        let store = PrefetchingObjectStore::with_options(inner.clone(), parts, part_size);
        (store, inner, dir)
    }

    const MEM: usize = 4 * 1024 * 1024;
    const DISK: usize = 64 * 1024 * 1024;

    #[tokio::test]
    async fn small_read_caches_full_part() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![7u8; 4096];
        inner.put(&path, body.clone().into()).await.unwrap();

        let r1 = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(10..20)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r1.range, 10..20);
        assert_eq!(&r1.bytes().await.unwrap()[..], &body[10..20]);

        let r2 = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(100..200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r2.bytes().await.unwrap()[..], &body[100..200]);
    }

    #[tokio::test]
    async fn full_object_read_populates_all_parts() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store.get_opts(&path, GetOptions::default()).await.unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[..]);

        for part_id in 0..8 {
            assert!(
                store.cached_part(&path, part_id).await.is_some(),
                "part {part_id} not cached"
            );
        }
    }

    #[tokio::test]
    async fn partial_cache_then_range_fetches_only_misses() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![3u8; 4096];
        inner.put(&path, body.clone().into()).await.unwrap();

        let _ = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..1024)),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let r = store.get_opts(&path, GetOptions::default()).await.unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[..]);
        for part_id in 0..4 {
            assert!(store.cached_part(&path, part_id).await.is_some());
        }
    }

    #[tokio::test]
    async fn put_invalidates_head() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        store.put(&path, vec![1u8; 100].into()).await.unwrap();
        let _ = store.head(&path).await.unwrap();
        store.put(&path, vec![2u8; 200].into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 200);
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &vec![2u8; 200][..]);
    }

    #[tokio::test]
    async fn suffix_range() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![5u8; 10_000];
        store.put(&path, body.clone().into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(100)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(got.len(), 100);
        assert_eq!(&got[..], &body[body.len() - 100..]);
    }

    #[tokio::test]
    async fn offset_range() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        store.put(&path, body.clone().into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Offset(2500)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[2500..]);
    }

    #[tokio::test]
    async fn conditional_get_bypasses_cache() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        inner.put(&path, vec![9u8; 100].into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    if_match: Some("never-matches".into()),
                    ..Default::default()
                },
            )
            .await;
        let _ = r;
    }

    #[test]
    fn window_ramps_up_on_sequential_access() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        assert_eq!(h.record(0), FETCH_WINDOW_MIN);
        assert_eq!(h.record(1024), FETCH_WINDOW_MIN * 2);
        assert_eq!(h.record(2048), FETCH_WINDOW_MIN * 4);
        assert_eq!(h.record(3072), FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(4096), FETCH_WINDOW_MIN * 16);
        assert_eq!(h.record(5120), FETCH_WINDOW_MIN * 32);
        assert_eq!(h.record(6144), FETCH_WINDOW_MIN * 64);
        assert_eq!(h.record(7168), FETCH_WINDOW_MAX);
    }

    #[test]
    fn random_access_starts_new_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        h.record(0);
        h.record(1024);
        h.record(2048);
        assert_eq!(h.record(3072), FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(100_000), FETCH_WINDOW_MIN);
    }

    #[test]
    fn first_access_uses_min_window() {
        let mut h = AccessHistory::new(1024);
        assert_eq!(h.record(5000), FETCH_WINDOW_MIN);
    }

    #[test]
    fn interleaved_streams_dont_interfere() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0);
        h.record(1024);
        assert_eq!(h.record(2048), FETCH_WINDOW_MIN * 4);

        h.record(100_000);
        h.record(101_024);

        assert_eq!(h.record(3072), FETCH_WINDOW_MIN * 8);
    }

    #[test]
    fn random_burst_does_not_evict_ramped_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0);
        h.record(1024);
        h.record(2048);
        assert_eq!(h.record(3072), FETCH_WINDOW_MIN * 8);

        for i in 0..10 {
            assert_eq!(h.record((i + 1) * 100_000), FETCH_WINDOW_MIN);
        }

        assert_eq!(h.record(4096), FETCH_WINDOW_MIN * 16);
    }

    #[tokio::test]
    async fn sequential_reads_ramp_up_prefetch() {
        let part_size = 64 * 1024;
        let seq_reads = 7;
        let max_window_parts = FETCH_WINDOW_MAX / part_size;
        let total_parts = seq_reads + max_window_parts + 4;
        let (store, inner, _dir) = make_store(part_size, 64 * MEM, 4 * DISK).await;
        let path = Path::from("seq");
        let body = vec![0xABu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        for i in 0..seq_reads {
            store.heads.remove(&path);
            let start = (i * part_size) as u64;
            let r = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(start..start + 10)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            r.bytes().await.unwrap();
        }

        store.heads.remove(&path);
        let next_start = (seq_reads * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(next_start..next_start + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let target_part = seq_reads;
        let last_prefetched_part = target_part + max_window_parts - 1;
        assert!(
            store
                .cached_part(&path, last_prefetched_part)
                .await
                .is_some(),
            "part {last_prefetched_part} should be prefetched at max window"
        );
        let beyond_part = last_prefetched_part + 1;
        assert!(
            store.cached_part(&path, beyond_part).await.is_none(),
            "part {beyond_part} should NOT be prefetched (beyond fetch window)"
        );
    }

    #[test]
    fn backward_access_resets_window() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        h.record(0);
        h.record(1024);
        h.record(2048);
        assert_eq!(h.record(3072), FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(1024), FETCH_WINDOW_MIN);
    }

    #[tokio::test]
    async fn read_part_prefetches_window_when_head_cached() {
        let part_size = 1024;
        let window_parts = FETCH_WINDOW_MIN / part_size;
        let total_parts = window_parts * 32;
        let (store, inner, _dir) = make_store(part_size, 16 * MEM, DISK).await;
        let path = Path::from("steady");
        let body: Vec<u8> = (0..part_size * total_parts)
            .map(|i| (i % 251) as u8)
            .collect();
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        assert!(store.read_head(&path).is_some());

        let offset = (window_parts * 4 * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(offset..offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[offset as usize..offset as usize + 10]);

        let target_part = offset as usize / part_size;
        let last_prefetched = target_part + window_parts - 1;
        assert!(
            store.cached_part(&path, last_prefetched).await.is_some(),
            "read_part should prefetch {window_parts} parts when head is cached"
        );
        let beyond = last_prefetched + 1;
        assert!(
            store.cached_part(&path, beyond).await.is_none(),
            "read_part should not prefetch beyond the window"
        );
    }

    #[tokio::test]
    async fn read_part_handles_eof_with_window() {
        let part_size = 1024;
        let total_parts = 4;
        let (store, inner, _dir) = make_store(part_size, MEM, DISK).await;
        let path = Path::from("eof");
        let body = vec![0xEFu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let last_part_offset = ((total_parts - 1) * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(last_part_offset..last_part_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(
            &got[..],
            &body[last_part_offset as usize..last_part_offset as usize + 10]
        );
    }

    #[tokio::test]
    async fn random_reads_stay_at_min_window() {
        let part_size = 1024;
        let min_fetch_parts = FETCH_WINDOW_MIN / part_size;
        let total_parts = min_fetch_parts * 64;
        let (store, inner, _dir) = make_store(part_size, 16 * MEM, DISK).await;
        let path = Path::from("rnd");
        let body = vec![0xCDu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        let gap = min_fetch_parts * 4;
        let random_offsets: Vec<u64> = (0..8).map(|i| (i * gap * part_size) as u64).collect();
        for off in &random_offsets {
            let r = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(*off..*off + 10)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            r.bytes().await.unwrap();
        }

        store.heads.remove(&path);
        let target_part = total_parts / 2;
        let target_offset = (target_part * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(target_offset..target_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        assert!(store.cached_part(&path, target_part).await.is_some());
        let beyond_part = target_part + min_fetch_parts + 1;
        assert!(
            store.cached_part(&path, beyond_part).await.is_none(),
            "part {beyond_part} should not be prefetched under random access"
        );
    }
}
