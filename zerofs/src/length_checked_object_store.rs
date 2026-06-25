use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

/// Backoff cap between re-fetches of a short body.
const SHORT_BODY_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Wraps an [`ObjectStore`] and guarantees the body of every `get` is at least as
/// long as the store claims it is, re-fetching transparently until it is.
///
/// `object_store`'s `GetResult::bytes()` collects the body with `collect_bytes`,
/// which uses the reported range only as a `Vec::with_capacity` hint and never
/// checks that the number of bytes actually received matches it.
#[derive(Debug)]
pub struct LengthCheckedObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl LengthCheckedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl Display for LengthCheckedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "LengthCheckedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LengthCheckedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Head requests carry no body; there is nothing to length-check.
        if options.head {
            return self.inner.get_opts(location, options).await;
        }

        let mut attempt = 0u32;
        loop {
            let result = self.inner.get_opts(location, options.clone()).await?;
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let expected = range.end.saturating_sub(range.start);

            // Draining the body lets us check its length before returning it.
            let bytes = result.bytes().await?;
            if (bytes.len() as u64) >= expected {
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        stream::once(async move { Ok(bytes) }).boxed(),
                    ),
                    meta,
                    range,
                    attributes,
                });
            }

            // Re-fetch (indefinitely) on a short body rather than return a
            // truncated object
            tracing::warn!(
                location = %location,
                expected,
                received = bytes.len(),
                attempt,
                "object store returned a short body; re-fetching"
            );
            // 100, 200, 400, 800ms, then capped
            let backoff =
                Duration::from_millis(100u64 << attempt.min(3)).min(SHORT_BODY_RETRY_MAX_BACKOFF);
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(backoff).await;
        }
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
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
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Inner store that lops `missing` bytes off the body of its first
    /// `truncate_first` get calls (while still reporting the full size/range),
    /// then serves the full body. Simulates a transient partial reply that
    /// clears after a few re-fetches.
    #[derive(Debug)]
    struct ShortBodyStore {
        inner: Arc<InMemory>,
        missing: usize,
        truncate_first: usize,
        calls: AtomicUsize,
    }

    impl Display for ShortBodyStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "ShortBodyStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ShortBodyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let result = self.inner.get_opts(location, options).await?;
            if n >= self.truncate_first {
                return Ok(result);
            }
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let full = result.bytes().await?;
            let short = full.slice(0..full.len().saturating_sub(self.missing));
            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(short) }).boxed()),
                meta,
                range,
                attributes,
            })
        }
        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recovers_after_transient_short_body() {
        let raw = Arc::new(InMemory::new());
        let path = Path::from("obj");
        let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        raw.put(&path, body.clone().into()).await.unwrap();

        // Many fetches come back short before the transient clears — well past any
        // small bound, to show retries are unbounded. (Paused time → no real waits.)
        let short = Arc::new(ShortBodyStore {
            inner: raw.clone(),
            missing: 100,
            truncate_first: 50,
            calls: AtomicUsize::new(0),
        });
        let store = LengthCheckedObjectStore::new(short.clone());

        // The wrapper re-fetches past every short reply and returns the full object.
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], &body[..]);
        assert_eq!(
            short.calls.load(Ordering::SeqCst),
            51,
            "50 short + 1 full fetch"
        );
    }

    #[tokio::test]
    async fn full_body_passes_through() {
        let raw = Arc::new(InMemory::new());
        let path = Path::from("obj");
        let body = vec![9u8; 1024];
        raw.put(&path, body.clone().into()).await.unwrap();

        let store = LengthCheckedObjectStore::new(raw);
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], &body[..]);
    }

    #[tokio::test]
    async fn ranged_read_passes_through() {
        let raw = Arc::new(InMemory::new());
        let path = Path::from("obj");
        let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        raw.put(&path, body.clone().into()).await.unwrap();

        let store = LengthCheckedObjectStore::new(raw);
        let got = store.get_range(&path, 100..200).await.unwrap();
        assert_eq!(&got[..], &body[100..200]);
    }

    // A hard get error must propagate, not be mistaken for a short body and
    // retried forever. (Retrying a hard error is the layer above's job.)
    #[tokio::test]
    async fn propagates_a_hard_get_error() {
        let (faulty, ctl) = crate::fault_store::FaultStore::new(Arc::new(InMemory::new()));
        let path = Path::from("obj");
        faulty.put(&path, b"data".to_vec().into()).await.unwrap();

        ctl.fail_gets(1);
        let store = LengthCheckedObjectStore::new(faulty);
        assert!(store.get(&path).await.is_err());
    }

    // The short-body guard applies to the range length too: a truncated ranged
    // read is re-fetched until the requested window arrives in full.
    #[tokio::test(start_paused = true)]
    async fn retries_a_truncated_ranged_read() {
        let raw = Arc::new(InMemory::new());
        let path = Path::from("obj");
        let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        raw.put(&path, body.clone().into()).await.unwrap();

        let (faulty, ctl) = crate::fault_store::FaultStore::new(raw);
        ctl.truncate_gets(3, 10);
        let store = LengthCheckedObjectStore::new(faulty);

        let got = store.get_range(&path, 100..200).await.unwrap();
        assert_eq!(&got[..], &body[100..200]);
        assert_eq!(ctl.get_count(), 4, "3 truncated + 1 full");
    }
}
