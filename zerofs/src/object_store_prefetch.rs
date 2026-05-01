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

pub const DEFAULT_PART_SIZE_BYTES: usize = 2 * 1024 * 1024;
const HEADS_CAPACITY_ENTRIES: usize = 16 * 1024;

type PartId = usize;

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
            .build();
        Self {
            inner,
            part_size_bytes,
            parts,
            heads,
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

        let (meta, attributes) = self.maybe_prefetch_range(location, opts.clone()).await?;
        let range = self.canonicalize_range(opts.range.clone(), meta.size)?;
        let parts = self.split_range_into_parts(range.clone());

        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| {
                self.read_part(location.clone(), part_id, range_in_part)
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
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        if let Some((meta, attrs)) = self.read_head(location) {
            return Ok((meta, attrs));
        }

        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range));
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

            let part_range = Range {
                start: (part_id * part_size_bytes) as u64,
                end: ((part_id + 1) * part_size_bytes) as u64,
            };
            let get_result = inner
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(part_range)),
                        ..Default::default()
                    },
                )
                .await?;
            let meta = get_result.meta.clone();
            let attrs = get_result.attributes.clone();
            let bytes = get_result.bytes().await?;

            heads.insert(
                location.clone(),
                Arc::new(CachedHead {
                    meta,
                    attributes: attrs,
                }),
            );
            parts.insert(key, bytes.clone());

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

    fn align_get_range(&self, range: &GetRange) -> GetRange {
        match range {
            GetRange::Bounded(r) => GetRange::Bounded(self.align_range(r, self.part_size_bytes)),
            GetRange::Suffix(s) => {
                GetRange::Suffix(self.align_range(&(0..*s), self.part_size_bytes).end)
            }
            GetRange::Offset(o) => GetRange::Offset(*o - *o % self.part_size_bytes as u64),
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
}
