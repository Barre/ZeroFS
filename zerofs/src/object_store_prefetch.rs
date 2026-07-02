use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use foyer::{Cache, HybridCache};
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{BoxFuture, Shared};
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
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
/// On a proven sequential stream, keep this many full windows fetched ahead of the
/// read so several large GETs stay in flight (a deep read pipeline). At the max
/// window that's `4 × 8 MiB = 32 MiB` outstanding — well inside the parts cache.
const PREFETCH_DEPTH_WINDOWS: usize = 4;

type PartId = usize;

/// A window fetch shared across concurrent readers whose part falls inside it: the
/// first miss launches one GET for the whole window and registers under every part
/// it covers, so an overlapping read joins it instead of issuing its own GET. The
/// result is `(base part id, full window bytes)` (a follower slices out its part),
/// with the error `Arc`-wrapped so the output is `Clone` (required by `Shared`).
type SharedFetch = Shared<BoxFuture<'static, Result<(PartId, Bytes), Arc<object_store::Error>>>>;

#[derive(Clone, Copy)]
struct Stream {
    last_offset: u64,
    last_end: u64,
    fetch_window: usize,
    fetched_until: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordDecision {
    fetch_window: usize,
    async_prefetch: Vec<AsyncPrefetch>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AsyncPrefetch {
    start: u64,
    size: usize,
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
                last_end: u64::MAX,
                fetch_window: FETCH_WINDOW_MIN,
                fetched_until: 0,
            }; MAX_STREAMS],
            len: 0,
            stride_limit: part_size as u64 * 4,
        }
    }

    fn record(&mut self, offset: u64, len: u64) -> RecordDecision {
        if let Some(i) = self.find_stream(offset) {
            let s = &mut self.streams[i];
            s.last_offset = offset;
            s.last_end = offset.saturating_add(len);
            s.fetch_window = (s.fetch_window * 2).min(FETCH_WINDOW_MAX);

            // Refill the prefetch frontier to `PREFETCH_DEPTH_WINDOWS` windows ahead of
            // the read, emitting one AsyncPrefetch per window still missing. Bounded per
            // access by the depth so one record can't fan out unboundedly. Only once the
            // window has ramped past the minimum (a stream is proven sequential) and the
            // frontier is already ahead of the read.
            let mut async_prefetch = Vec::new();
            if s.fetch_window > FETCH_WINDOW_MIN && s.fetched_until > offset {
                let window = s.fetch_window as u64;
                let target = offset + PREFETCH_DEPTH_WINDOWS as u64 * window;
                while s.fetched_until < target && async_prefetch.len() < PREFETCH_DEPTH_WINDOWS {
                    let start = s.fetched_until;
                    async_prefetch.push(AsyncPrefetch {
                        start,
                        size: s.fetch_window,
                    });
                    s.fetched_until = start + window;
                }
            }

            return RecordDecision {
                fetch_window: s.fetch_window,
                async_prefetch,
            };
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
            last_end: offset.saturating_add(len),
            fetch_window: FETCH_WINDOW_MIN,
            fetched_until: offset,
        };

        RecordDecision {
            fetch_window: FETCH_WINDOW_MIN,
            async_prefetch: Vec::new(),
        }
    }

    fn note_fetch(&mut self, offset: u64, fetch_end: u64) {
        if let Some(i) = self.find_stream(offset)
            && fetch_end > self.streams[i].fetched_until
        {
            self.streams[i].fetched_until = fetch_end;
        }
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

    /// Match `offset` to a stream: at or past the stream's last request start, and
    /// within `stride_limit` of where that request ENDED. Measuring the gap from the
    /// end rather than the start keeps large contiguous requests on their stream —
    /// back-to-back 1 MiB coalesced frame runs have a start-to-start stride far
    /// beyond any point-read stride, but a zero end-to-start gap.
    fn find_stream(&self, offset: u64) -> Option<usize> {
        let mut best = None;
        let mut best_dist = u64::MAX;
        for i in 0..self.len {
            let s = &self.streams[i];
            if offset >= s.last_offset && offset.saturating_sub(s.last_end) <= self.stride_limit {
                let dist = offset.saturating_sub(s.last_end);
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
    in_flight: Arc<DashMap<PartKey, ()>>,
    /// Single-flight for the read path: a part being fetched right now maps to the
    /// shared fetch, so a burst of overlapping reads (main read + read-ahead) that
    /// all miss the same part collapse into one GET instead of one GET each.
    fetches: Arc<DashMap<PartKey, SharedFetch>>,
}

struct PrefetchGuard {
    map: Arc<DashMap<PartKey, ()>>,
    key: PartKey,
}

impl Drop for PrefetchGuard {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
}

/// Held by the leader of a single-flight window fetch; clears every part slot it
/// registered on drop, so the slots are released whether the fetch completes or the
/// awaiting read is cancelled. Followers hold `None` — only the leader owns the
/// registration.
struct FetchGuard {
    map: Arc<DashMap<PartKey, SharedFetch>>,
    keys: Vec<PartKey>,
}

impl Drop for FetchGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            self.map.remove(key);
        }
    }
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
            in_flight: Arc::new(DashMap::new()),
            fetches: Arc::new(DashMap::new()),
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

    fn record_access(&self, location: &Path, offset: u64, len: u64) -> RecordDecision {
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
        history.record(offset, len)
    }

    fn note_fetch(&self, location: &Path, offset: u64, fetch_end: u64) {
        if let Some(entry) = self.access_tracker.get(location) {
            let hist = entry.value().clone();
            hist.lock().unwrap().note_fetch(offset, fetch_end);
        }
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
        {
            return self.inner.get_opts(location, opts).await;
        }

        if opts.head {
            let meta = self.cached_head(location).await?;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::empty::<object_store::Result<Bytes>>().boxed(),
                ),
                range: 0..0,
                attributes: Attributes::default(),
                extensions: Default::default(),
                meta,
            });
        }

        let access_offset = self.range_start_offset(&opts.range);
        // Offset/Suffix/None request lengths aren't known until the head is; track
        // them as point accesses.
        let access_len = match &opts.range {
            Some(GetRange::Bounded(r)) => r.end.saturating_sub(r.start),
            _ => 0,
        };
        let decision = self.record_access(location, access_offset, access_len);
        let fetch_window = decision.fetch_window;

        for prefetch in decision.async_prefetch {
            self.spawn_async_prefetch(location.clone(), prefetch);
        }

        let (meta, attributes) = self
            .maybe_prefetch_range(location, opts.clone(), fetch_window, access_offset)
            .await?;
        let range = self.canonicalize_range(opts.range.clone(), meta.size)?;
        // Whichever part leads a window fetch must cover through the request's last
        // part in one GET, so a large coalesced read is never split just because its
        // bounds aren't part-aligned. Each part's window therefore reaches from that
        // part to the request's aligned end, or the ramped fetch window when larger
        // (which is what prefetches ahead of the request).
        let end_part = usize::try_from(range.end.div_ceil(self.part_size_bytes as u64))
            .expect("part id exceeds usize");
        let parts = self.split_range_into_parts(range.clone());

        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| {
                let window = fetch_window.max((end_part - part_id) * self.part_size_bytes);
                self.read_part(
                    location.clone(),
                    part_id,
                    range_in_part,
                    window,
                    access_offset,
                )
            })
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            extensions: Default::default(),
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    fn spawn_async_prefetch(&self, location: Path, prefetch: AsyncPrefetch) {
        let part_size = self.part_size_bytes;
        let part_size_u64 = part_size as u64;

        if !prefetch.start.is_multiple_of(part_size_u64) {
            return;
        }

        // The stream frontier advances in fixed windows with no notion of the
        // object's length, so a sequential read approaching EOF pushes it past
        // the end. A GET starting there is unsatisfiable (HTTP 416), not a
        // useful prefetch; the head is already cached by the stream's earlier
        // reads, so drop the window here. A window merely *ending* past EOF is
        // fine: the backend truncates it.
        if let Some((meta, _)) = self.read_head(&location)
            && prefetch.start >= meta.size
        {
            return;
        }

        let start_part: PartId = match (prefetch.start / part_size_u64).try_into() {
            Ok(p) => p,
            Err(_) => return,
        };

        let key = PartKey::new(&location, start_part);
        // A read is already fetching a window that covers this part — don't launch an
        // overlapping read-ahead GET for it.
        if self.fetches.contains_key(&key) {
            return;
        }
        if self.in_flight.insert(key.clone(), ()).is_some() {
            return;
        }

        let guard = PrefetchGuard {
            map: self.in_flight.clone(),
            key,
        };
        let inner = self.inner.clone();
        let parts_cache = self.parts.clone();
        let access_tracker = self.access_tracker.clone();
        let access_offset = prefetch.start;

        let range = Range {
            start: prefetch.start,
            end: prefetch.start + prefetch.size as u64,
        };

        tokio::spawn(async move {
            let _guard = guard;
            if let Ok(Some(_)) = parts_cache.get(&PartKey::new(&location, start_part)).await {
                return;
            }
            let get_result = match inner
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Bounded(range.clone())),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(r) => r,
                Err(_) => return,
            };
            let actual_end = get_result.range.end;
            let stream = get_result.into_stream();
            if Self::save_parts_stream_static(
                &parts_cache,
                part_size,
                &location,
                stream,
                start_part,
            )
            .await
            .is_ok()
                && let Some(entry) = access_tracker.get(&location)
            {
                let hist = entry.value().clone();
                hist.lock().unwrap().note_fetch(access_offset, actual_end);
            }
        });
    }

    async fn maybe_prefetch_range(
        &self,
        location: &Path,
        mut opts: GetOptions,
        fetch_window: usize,
        access_offset: u64,
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
        let fetch_end = get_result.range.end;
        let _ = self.save_get_result(location, get_result).await;
        self.note_fetch(location, access_offset, fetch_end);
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
        stream: S,
        start_part_number: PartId,
    ) -> object_store::Result<()>
    where
        S: stream::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    {
        Self::save_parts_stream_static(
            &self.parts,
            self.part_size_bytes,
            location,
            stream,
            start_part_number,
        )
        .await
    }

    async fn save_parts_stream_static<S>(
        parts: &HybridCache<PartKey, Bytes>,
        part_size_bytes: usize,
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
            while buffer.len() >= part_size_bytes {
                let to_write = buffer.split_to(part_size_bytes);
                parts.insert(PartKey::new(location, part_number), to_write.freeze());
                part_number += 1;
            }
        }

        if !buffer.is_empty() {
            parts.insert(PartKey::new(location, part_number), buffer.freeze());
        }
        Ok(())
    }

    /// Write-through: populate the parts cache from a just-uploaded object's full
    /// bytes, so the first read after it ages out of the small segment cache is a
    /// cache hit, not an object-store GET. A single-chunk payload (a sealed
    /// segment) slices zero-copy; a multi-chunk payload concatenates once. Inserts
    /// are synchronous (foyer flushes to disk in the background), so this adds no
    /// I/O wait to the put. The final part is partial when the object isn't a
    /// part-size multiple — the read path already handles a partial trailing part.
    fn write_through(&self, location: &Path, payload: PutPayload) {
        let chunks: Vec<Bytes> = payload.into_iter().collect();
        let bytes = match chunks.len() {
            0 => return,
            1 => chunks.into_iter().next().expect("one chunk"),
            _ => {
                let mut buf = BytesMut::with_capacity(chunks.iter().map(Bytes::len).sum());
                for c in &chunks {
                    buf.extend_from_slice(c);
                }
                buf.freeze()
            }
        };
        self.warm_object(location, bytes);
    }

    /// Warm the parts cache with an object's full bytes, sliced into part-sized
    /// entries keyed from part 0 (the same keying the read path uses). For callers
    /// that already hold the bytes but upload via a path that does not write through
    /// (multipart), so the object is cached at write time rather than on the first
    /// post-eviction read.
    pub fn warm_object(&self, location: &Path, bytes: Bytes) {
        let ps = self.part_size_bytes;
        let mut off = 0usize;
        let mut part_id: PartId = 0;
        while off < bytes.len() {
            let end = (off + ps).min(bytes.len());
            self.parts
                .insert(PartKey::new(location, part_id), bytes.slice(off..end));
            off = end;
            part_id += 1;
        }
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
        access_offset: u64,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let inner = self.inner.clone();
        let part_size_bytes = self.part_size_bytes;
        let parts = self.parts.clone();
        let heads = self.heads.clone();
        let access_tracker = self.access_tracker.clone();
        let fetches = self.fetches.clone();
        Box::pin(async move {
            let key = PartKey::new(&location, part_id);
            if let Ok(Some(entry)) = parts.get(&key).await {
                let bytes = entry.value().clone();
                if range_in_part.end <= bytes.len() {
                    return Ok(bytes.slice(range_in_part));
                }
                parts.remove(&key);
            }

            // Single-flight the miss over the whole window it will fetch: overlapping
            // reads (the main read plus the read-ahead layers) whose part lands inside
            // this window join the one GET instead of each issuing their own. The
            // leader registers every part the window covers and holds a `FetchGuard`
            // that clears them on drop, whether the fetch completes or the awaiting
            // read is cancelled, so a dropped read can't strand a slot.
            let extra_parts = (fetch_window / part_size_bytes).max(1);
            let (shared, _guard) = match fetches.entry(key.clone()) {
                dashmap::mapref::entry::Entry::Occupied(e) => (e.get().clone(), None),
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    let fut = Self::fetch_part_window(
                        inner,
                        parts,
                        heads,
                        access_tracker,
                        location.clone(),
                        part_id,
                        part_size_bytes,
                        extra_parts,
                        access_offset,
                    )
                    .boxed()
                    .shared();
                    e.insert(fut.clone());
                    let mut keys = Vec::with_capacity(extra_parts);
                    keys.push(key.clone());
                    for i in 1..extra_parts {
                        let k = PartKey::new(&location, part_id + i);
                        fetches.insert(k.clone(), fut.clone());
                        keys.push(k);
                    }
                    (
                        fut,
                        Some(FetchGuard {
                            map: fetches.clone(),
                            keys,
                        }),
                    )
                }
            };
            let (base, window) = shared.await.map_err(|e| object_store::Error::Generic {
                store: "PrefetchingObjectStore",
                source: format!("shared part fetch failed: {e}").into(),
            })?;
            let off = part_id.saturating_sub(base) * part_size_bytes;
            if off >= window.len() {
                return Ok(Bytes::new());
            }
            let part_end = (off + part_size_bytes).min(window.len());
            let part_bytes = window.slice(off..part_end);
            let end = range_in_part.end.min(part_bytes.len());
            let start = range_in_part.start.min(end);
            Ok(part_bytes.slice(start..end))
        })
    }

    /// Fetch the window of `window_parts` parts starting at `part_id`, cache every
    /// part it spans, and return `(part_id, window bytes)`. `window_parts` comes
    /// from the caller so the GET's span always matches the part slots the caller
    /// registered under the shared fetch. Driven through [`SharedFetch`] so
    /// concurrent readers of any part inside the window collapse onto this one GET
    /// and slice their part from the returned bytes.
    /// `access_offset` is the triggering request's start offset: the stream tracker
    /// keys on request offsets, so the fetch must be credited under it — the aligned
    /// window start would miss the stream entirely for an unaligned read, leaving
    /// `fetched_until` unseeded and the async prefetch pipeline never engaging.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_part_window(
        inner: Arc<dyn ObjectStore>,
        parts: HybridCache<PartKey, Bytes>,
        heads: Cache<Path, Arc<CachedHead>>,
        access_tracker: Cache<Path, Arc<Mutex<AccessHistory>>>,
        location: Path,
        part_id: PartId,
        part_size_bytes: usize,
        window_parts: usize,
        access_offset: u64,
    ) -> Result<(PartId, Bytes), Arc<object_store::Error>> {
        let fetch_start = part_id;
        let fetch_range = Range {
            start: (fetch_start * part_size_bytes) as u64,
            end: ((fetch_start + window_parts) * part_size_bytes) as u64,
        };
        let get_result = inner
            .get_opts(
                &location,
                GetOptions {
                    range: Some(GetRange::Bounded(fetch_range)),
                    ..Default::default()
                },
            )
            .await
            .map_err(Arc::new)?;
        let meta = get_result.meta.clone();
        let attrs = get_result.attributes.clone();
        let actual_end = get_result.range.end;
        let all_bytes = get_result.bytes().await.map_err(Arc::new)?;

        heads.insert(
            location.clone(),
            Arc::new(CachedHead {
                meta,
                attributes: attrs,
            }),
        );
        if let Some(entry) = access_tracker.get(&location) {
            entry
                .value()
                .clone()
                .lock()
                .unwrap()
                .note_fetch(access_offset, actual_end);
        }
        for i in 0..window_parts {
            let start = i * part_size_bytes;
            if start >= all_bytes.len() {
                break;
            }
            let end = ((i + 1) * part_size_bytes).min(all_bytes.len());
            parts.insert(
                PartKey::new(&location, fetch_start + i),
                all_bytes.slice(start..end),
            );
        }
        Ok((fetch_start, all_bytes))
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

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Clone is cheap (Arc-backed) and lets us write the bytes through to the
        // parts cache once the upload commits.
        let cached = payload.clone();
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.invalidate(location);
        self.write_through(location, cached);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        // Invalidate each successfully deleted path's cached head as it passes.
        let heads = self.heads.clone();
        self.inner
            .delete_stream(locations)
            .map(move |res| {
                if let Ok(path) = &res {
                    heads.remove(path);
                }
                res
            })
            .boxed()
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

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let result = self.inner.copy_opts(from, to, options).await;
        self.invalidate(to);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let result = self.inner.rename_opts(from, to, options).await;
        self.invalidate(from);
        self.invalidate(to);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foyer::{BlockEngineConfig, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig};
    use object_store::ObjectStoreExt;
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

    /// Wraps a store, counting `get_opts` calls and delaying each so concurrent
    /// reads overlap in flight (to exercise the single-flight coalescing).
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<InMemory>,
        gets: Arc<std::sync::atomic::AtomicUsize>,
        delay: std::time::Duration,
    }

    impl Display for CountingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(self.delay).await;
            self.inner.get_opts(l, o).await
        }
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    /// Wraps a store, recording each `get_opts` range so a test can assert
    /// what actually reaches the backend.
    #[derive(Debug)]
    struct RangeRecordingStore {
        inner: Arc<InMemory>,
        ranges: Arc<std::sync::Mutex<Vec<GetRange>>>,
    }

    impl Display for RangeRecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RangeRecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RangeRecordingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if let Some(r) = &o.range {
                self.ranges.lock().unwrap().push(r.clone());
            }
            self.inner.get_opts(l, o).await
        }
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    async fn store_over(
        inner: Arc<dyn ObjectStore>,
        part_size: usize,
    ) -> (PrefetchingObjectStore, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("test-parts")
            .memory(MEM)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(DISK),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        (
            PrefetchingObjectStore::with_options(inner, parts, part_size),
            dir,
        )
    }

    // A burst of concurrent reads whose parts fall inside one prefetch window must
    // collapse onto a single object-store GET, not one GET each.
    #[tokio::test]
    async fn overlapping_concurrent_reads_coalesce_into_one_get() {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/seg");
        let body: Vec<u8> = (0..64 * 1024u32).map(|i| i as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::from_millis(40),
        });
        // 1 KiB parts; the first read ramps to a window covering many parts.
        let (store, _dir) = store_over(counting, 1024).await;
        let store = Arc::new(store);

        // Warm the head so the first-touch head GET doesn't skew the count, then
        // launch reads of distinct parts that all land in the leader's window.
        let _ = store.head(&path).await.unwrap();
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        let mut handles = Vec::new();
        for p in 0..16u64 {
            let s = store.clone();
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                let off = p * 1024;
                s.get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(off..off + 1024)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
            }));
        }
        for (p, h) in handles.into_iter().enumerate() {
            let got = h.await.unwrap();
            assert_eq!(&got[..], &body[p * 1024..p * 1024 + 1024]);
        }

        // The first read ramps to a 128 KiB window (128 × 1 KiB parts) covering all
        // 16, so they collapse onto ~one GET; a couple extra tolerates the leader race.
        let n = gets.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            n <= 3,
            "expected coalescing, got {n} GETs for 16 overlapping reads"
        );
        assert!(store.fetches.is_empty(), "fetch slots leaked");
    }

    // A contiguous read larger than the fetch window whose bounds are not
    // part-aligned (every coalesced frame run) must reach the object store as ONE
    // GET: the unaligned tail part must not fall outside the leader's window and
    // trigger a second, overfetching GET.
    #[tokio::test]
    async fn unaligned_large_read_is_one_get() {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/seg");
        let body: Vec<u8> = (0..1024 * 1024u32).map(|i| i as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::ZERO,
        });
        let (store, _dir) = store_over(counting, 1024).await;

        // Warm the head via a small read far from the range under test (a head()
        // against InMemory would return the whole payload and pre-fill the parts
        // cache), then count only the read under test.
        let far = 900 * 1024u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(far..far + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        let (off, len) = (500u64, 200 * 1024u64);
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(off..off + len)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[off as usize..(off + len) as usize]);
        assert_eq!(
            gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "unaligned contiguous read should be a single GET"
        );
    }

    #[tokio::test]
    async fn put_writes_through_to_parts_cache() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("segments/3b/seg");
        // 2.5 parts of 1024 bytes: parts 0 and 1 full, part 2 partial (512).
        let body: Vec<u8> = (0..2560u32).map(|i| i as u8).collect();
        store.put(&path, body.clone().into()).await.unwrap();

        // The put populated every part — a read needs no object-store GET.
        for (part_id, range) in [(0usize, 0..1024usize), (1, 1024..2048), (2, 2048..2560)] {
            let entry = store
                .parts
                .get(&PartKey::new(&path, part_id))
                .await
                .unwrap()
                .expect("part cached by write-through");
            assert_eq!(entry.value().as_ref(), &body[range]);
        }
    }

    #[tokio::test]
    async fn warm_object_populates_parts_cache() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("segments/3b/seg");
        // 2.5 parts: parts 0 and 1 full, part 2 partial (512). No object-store put —
        // warm_object caches from bytes held in hand (the multipart-seal case).
        let body: Vec<u8> = (0..2560u32).map(|i| i as u8).collect();
        store.warm_object(&path, body.clone().into());

        for (part_id, range) in [(0usize, 0..1024usize), (1, 1024..2048), (2, 2048..2560)] {
            let entry = store
                .parts
                .get(&PartKey::new(&path, part_id))
                .await
                .unwrap()
                .expect("part cached by warm_object");
            assert_eq!(entry.value().as_ref(), &body[range]);
        }
    }

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
        assert_eq!(h.record(0, 0).fetch_window, FETCH_WINDOW_MIN);
        assert_eq!(h.record(1024, 0).fetch_window, FETCH_WINDOW_MIN * 2);
        assert_eq!(h.record(2048, 0).fetch_window, FETCH_WINDOW_MIN * 4);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(4096, 0).fetch_window, FETCH_WINDOW_MIN * 16);
        assert_eq!(h.record(5120, 0).fetch_window, FETCH_WINDOW_MIN * 32);
        assert_eq!(h.record(6144, 0).fetch_window, FETCH_WINDOW_MIN * 64);
        assert_eq!(h.record(7168, 0).fetch_window, FETCH_WINDOW_MAX);
    }

    #[test]
    fn large_contiguous_requests_ramp_despite_stride() {
        // Back-to-back 1 MiB coalesced reads: the start-to-start stride is 1 MiB
        // (far beyond any point-read stride limit), but each request starts where
        // the previous one ended, so the stream must keep ramping.
        let mut h = AccessHistory::new(DEFAULT_PART_SIZE_BYTES);
        let m = 1024 * 1024u64;
        assert_eq!(h.record(0, m).fetch_window, FETCH_WINDOW_MIN);
        assert_eq!(h.record(m, m).fetch_window, FETCH_WINDOW_MIN * 2);
        assert_eq!(h.record(2 * m, m).fetch_window, FETCH_WINDOW_MIN * 4);
        assert_eq!(h.record(3 * m, m).fetch_window, FETCH_WINDOW_MIN * 8);
    }

    #[test]
    fn random_access_starts_new_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(100_000, 0).fetch_window, FETCH_WINDOW_MIN);
    }

    #[test]
    fn first_access_uses_min_window() {
        let mut h = AccessHistory::new(1024);
        assert_eq!(h.record(5000, 0).fetch_window, FETCH_WINDOW_MIN);
    }

    #[test]
    fn interleaved_streams_dont_interfere() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.record(1024, 0);
        assert_eq!(h.record(2048, 0).fetch_window, FETCH_WINDOW_MIN * 4);

        h.record(100_000, 0);
        h.record(101_024, 0);

        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
    }

    #[test]
    fn random_burst_does_not_evict_ramped_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);

        for i in 0..10 {
            assert_eq!(
                h.record((i + 1) * 100_000, 0).fetch_window,
                FETCH_WINDOW_MIN
            );
        }

        assert_eq!(h.record(4096, 0).fetch_window, FETCH_WINDOW_MIN * 16);
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
        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(1024, 0).fetch_window, FETCH_WINDOW_MIN);
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

    #[test]
    fn record_emits_async_prefetch_in_trigger_zone() {
        let part_size = 64 * 1024;
        let mut h = AccessHistory::new(part_size);

        let d = h.record(0, 0);
        assert_eq!(d.fetch_window, FETCH_WINDOW_MIN);
        assert!(d.async_prefetch.is_empty());

        h.note_fetch(0, FETCH_WINDOW_MIN as u64);

        let d = h.record((FETCH_WINDOW_MIN / 2) as u64, 0);
        assert_eq!(d.fetch_window, FETCH_WINDOW_MIN * 2);
        // The frontier refills up to PREFETCH_DEPTH_WINDOWS ahead; the first window
        // starts where the read had already fetched to.
        let p = d
            .async_prefetch
            .first()
            .expect("should fire in trigger zone");
        assert_eq!(p.start, FETCH_WINDOW_MIN as u64);
        assert_eq!(p.size, FETCH_WINDOW_MIN * 2);
        assert_eq!(d.async_prefetch.len(), PREFETCH_DEPTH_WINDOWS);
    }

    #[test]
    fn record_no_async_prefetch_when_far_from_fetched_until() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.note_fetch(0, 8 * 1024 * 1024);

        let d = h.record(1024, 0);
        assert!(
            d.async_prefetch.is_empty(),
            "should not trigger when fetched_until is far ahead of offset"
        );
    }

    #[test]
    fn record_no_async_prefetch_at_min_window() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(100_000, 0);
        let d = h.record(100_000, 0);
        assert!(d.async_prefetch.is_empty());
    }

    #[test]
    fn note_fetch_only_advances_fetched_until() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.note_fetch(0, 4096);
        h.note_fetch(0, 2048);
        assert_eq!(h.streams[0].fetched_until, 4096);
    }

    #[tokio::test]
    async fn async_prefetch_fills_cache_ahead_of_consumer() {
        let part_size = 64 * 1024;
        let total_parts = (FETCH_WINDOW_MIN / part_size) * 8;
        let (store, inner, _dir) = make_store(part_size, 4 * MEM, DISK).await;
        let path = Path::from("asyncpf");
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

        let mid_offset = (FETCH_WINDOW_MIN / 2) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(mid_offset..mid_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let next_window_start_part = FETCH_WINDOW_MIN / part_size;
        for _ in 0..100 {
            if store
                .cached_part(&path, next_window_start_part)
                .await
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            store
                .cached_part(&path, next_window_start_part)
                .await
                .is_some(),
            "async prefetch should fill part {next_window_start_part}"
        );
    }

    #[tokio::test]
    async fn spawn_async_prefetch_dedups_via_in_flight() {
        let part_size = 1024;
        let (store, inner, _dir) = make_store(part_size, MEM, DISK).await;
        let path = Path::from("dedup");
        let body = vec![0xAAu8; part_size * 32];
        inner.put(&path, body.clone().into()).await.unwrap();

        let start_part = 5;
        let key = PartKey::new(&path, start_part);
        store.in_flight.insert(key.clone(), ());

        store.spawn_async_prefetch(
            path.clone(),
            AsyncPrefetch {
                start: (start_part * part_size) as u64,
                size: part_size * 4,
            },
        );

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            store.cached_part(&path, start_part).await.is_none(),
            "duplicate prefetch should have been skipped"
        );
        assert!(store.in_flight.contains_key(&key));
    }

    #[tokio::test]
    async fn prefetch_guard_releases_in_flight_on_drop() {
        let part_size = 1024;
        let (store, inner, _dir) = make_store(part_size, MEM, DISK).await;
        let path = Path::from("guard");
        let body = vec![0xBBu8; part_size * 16];
        inner.put(&path, body.clone().into()).await.unwrap();

        let start_part = 2;
        store.spawn_async_prefetch(
            path.clone(),
            AsyncPrefetch {
                start: (start_part * part_size) as u64,
                size: part_size * 4,
            },
        );

        for _ in 0..100 {
            if !store
                .in_flight
                .contains_key(&PartKey::new(&path, start_part))
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            !store
                .in_flight
                .contains_key(&PartKey::new(&path, start_part)),
            "in_flight key should be released after prefetch completes"
        );
    }

    // The async prefetch frontier must stop at EOF. It advances in fixed
    // windows with no notion of the object's length, so a sequential stream
    // near the end would otherwise emit windows starting past EOF — GETs the
    // backend rejects as unsatisfiable (HTTP 416) and that a retrying layer
    // below would re-send forever.
    #[tokio::test]
    async fn async_prefetch_never_fetches_past_eof() {
        let part_size = 1024;
        let len = 2 * FETCH_WINDOW_MIN;
        let inner = Arc::new(InMemory::new());
        let path = Path::from("seq");
        inner.put(&path, vec![7u8; len].into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // Sequential reads: the second ramps the window past the minimum and
        // refills the prefetch frontier, whose later windows start past EOF.
        store.get_range(&path, 0..1024).await.unwrap();
        store.get_range(&path, 1024..2048).await.unwrap();
        store.get_range(&path, 2048..3072).await.unwrap();
        // The prefetches run on spawned tasks; give them time to issue GETs.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let seen = ranges.lock().unwrap().clone();
        assert!(!seen.is_empty());
        for r in &seen {
            if let GetRange::Bounded(b) = r {
                assert!(
                    b.start < len as u64,
                    "GET issued past EOF: {b:?} (object is {len} bytes)"
                );
            }
        }
    }
}
