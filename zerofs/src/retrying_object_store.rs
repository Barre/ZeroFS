//! Retrying object-store wrapper for ZeroFS's own I/O, adapted from SlateDB's
//! (crate-private) `RetryingObjectStore`.
//!
//! SlateDB wraps the store we hand it with its own retrying layer, so SST and
//! manifest traffic already rides out transient backend errors. Everything
//! ZeroFS issues directly — segment GETs/PUTs, the GC's LIST/DELETE, checkpoint
//! admin listings — did not, so one flaky response surfaced as an EIO to the
//! client. This wrapper closes that gap: transient errors retry with exponential
//! backoff forever; `NotFound`-class errors return immediately.
//!
//! Differences from SlateDB's version: no conditional-put ULID verification
//! (every PUT ZeroFS issues through this store is an overwrite of an immutable,
//! deterministically-keyed object, so a blind retry is idempotent; the fenced
//! CAS writes that need verification happen inside SlateDB, behind its own
//! wrapper) and backoff sleeps on the tokio clock rather than an injected one.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};

/// Retries transient errors with exponential backoff forever.
#[derive(Debug)]
pub struct RetryingObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl RetryingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    #[inline]
    fn retry_builder() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .without_max_times()
            .with_min_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(1))
    }

    #[inline]
    fn notify(err: &object_store::Error, duration: Duration) {
        tracing::info!(
            "retrying object store operation [error={:?}, delay={:?}]",
            err,
            duration
        );
    }

    #[inline]
    fn should_retry(err: &object_store::Error) -> bool {
        !matches!(
            err,
            object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }
                | object_store::Error::NotFound { .. }
                | object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. }
        ) && !Self::is_unsatisfiable_range(err)
    }

    /// A range the object can never satisfy (start at or past EOF) is
    /// deterministic: retrying re-sends the same doomed request forever.
    /// Every backend surfaces it as `Error::Generic`, so match the message:
    /// "InvalidRange" is the S3/R2/Azure error code, "not satisfiable" the
    /// HTTP 416 reason phrase (GCS), and "StartTooLarge" object_store's own
    /// `InvalidGetRange` variant for the fs/memory backends.
    fn is_unsatisfiable_range(err: &object_store::Error) -> bool {
        let s = format!("{err:?}");
        s.contains("InvalidRange") || s.contains("not satisfiable") || s.contains("StartTooLarge")
    }

    /// Expected byte length of a range read, truncated at the actual object
    /// size (a `GetRange::Bounded` end may exceed it).
    fn expected_range_len(range: &GetRange, file_size: u64) -> u64 {
        match range {
            GetRange::Bounded(r) => r.end.min(file_size).saturating_sub(r.start),
            GetRange::Offset(offset) => file_size.saturating_sub(*offset),
            GetRange::Suffix(suffix) => (*suffix).min(file_size),
        }
    }
}

impl std::fmt::Display for RetryingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RetryingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RetryingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // For range reads, drain the body inside the retry closure so a
        // transient mid-stream error (or a truncated body) retries the entire
        // range; the body bytes would otherwise be read by the caller, outside
        // the retry loop.
        (|| async {
            let options = options.clone();
            let options_range = options.range.clone();
            let result = self.inner.get_opts(location, options).await?;
            let meta = result.meta.clone();

            let Some(requested_range) = options_range else {
                // No range requested: the expected size is unknowable without
                // buffering, so hand the stream through unbuffered.
                return Ok(result);
            };

            let expected_len = Self::expected_range_len(&requested_range, meta.size);
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let extensions = result.extensions.clone();
            let bytes = result.bytes().await?;

            if bytes.len() as u64 != expected_len {
                return Err(object_store::Error::Generic {
                    store: "RetryingObjectStore",
                    source: format!(
                        "range read size check failed: {} bytes read, expected {expected_len} \
                         (requested range truncated at object size {})",
                        bytes.len(),
                        meta.size
                    )
                    .into(),
                });
            }

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                meta,
                range,
                attributes,
                extensions,
            })
        })
        .retry(Self::retry_builder())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        (|| async {
            self.inner
                .put_opts(location, payload.clone(), opts.clone())
                .await
        })
        .retry(Self::retry_builder())
        .notify(Self::notify)
        .when(Self::should_retry)
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Only the initiation is retried; part uploads and completion belong to
        // the returned handle (BufWriter drives those and surfaces their errors
        // to the seal path, which has its own retry).
        (|| async { self.inner.put_multipart_opts(location, opts.clone()).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |loc| {
                let inner = Arc::clone(&inner);
                async move {
                    let loc = loc?;
                    (|| async { inner.delete(&loc).await })
                        .retry(Self::retry_builder())
                        .notify(Self::notify)
                        .when(Self::should_retry)
                        .await?;
                    Ok(loc)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        // A paginated stream can't be retried from the middle, so each attempt
        // collects the whole listing and only a complete one is streamed out.
        stream::once(async move {
            (|| async { inner.list(prefix.as_ref()).try_collect::<Vec<_>>().await })
                .retry(Self::retry_builder())
                .notify(Self::notify)
                .when(Self::should_retry)
                .await
        })
        .map_ok(|entries| stream::iter(entries.into_iter().map(Ok)).boxed())
        .try_flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        stream::once(async move {
            (|| async {
                inner
                    .list_with_offset(prefix.as_ref(), &offset)
                    .try_collect::<Vec<_>>()
                    .await
            })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
        })
        .map_ok(|entries| stream::iter(entries.into_iter().map(Ok)).boxed())
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        (|| async { self.inner.list_with_delimiter(prefix).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        (|| async { self.inner.copy_opts(from, to, options.clone()).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        (|| async { self.inner.rename_opts(from, to, options.clone()).await })
            .retry(Self::retry_builder())
            .notify(Self::notify)
            .when(Self::should_retry)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Fails the first `fail` calls of each op kind with a transient error,
    /// then delegates.
    #[derive(Debug)]
    struct FlakyStore {
        inner: Arc<dyn ObjectStore>,
        fail_gets: AtomicUsize,
        fail_puts: AtomicUsize,
        fail_lists: AtomicUsize,
        gets: AtomicUsize,
        truncate_gets: AtomicUsize,
    }

    impl FlakyStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                fail_gets: AtomicUsize::new(0),
                fail_puts: AtomicUsize::new(0),
                fail_lists: AtomicUsize::new(0),
                gets: AtomicUsize::new(0),
                truncate_gets: AtomicUsize::new(0),
            }
        }

        fn transient() -> object_store::Error {
            object_store::Error::Generic {
                store: "flaky",
                source: "transient".into(),
            }
        }

        fn take(counter: &AtomicUsize) -> bool {
            counter
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }
    }

    impl std::fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if Self::take(&self.fail_gets) {
                return Err(Self::transient());
            }
            let result = self.inner.get_opts(location, options).await?;
            if Self::take(&self.truncate_gets) {
                // Body one byte short of the requested range, as a torn
                // response would be.
                let meta = result.meta.clone();
                let range = result.range.clone();
                let bytes = result.bytes().await?;
                let truncated = bytes.slice(..bytes.len() - 1);
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        stream::once(async move { Ok(truncated) }).boxed(),
                    ),
                    meta,
                    range,
                    attributes: Default::default(),
                    extensions: Default::default(),
                });
            }
            Ok(result)
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if Self::take(&self.fail_puts) {
                return Err(Self::transient());
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            if Self::take(&self.fail_lists) {
                return stream::once(async { Err(Self::transient()) }).boxed();
            }
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    fn seeded() -> (Arc<FlakyStore>, RetryingObjectStore) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let flaky = Arc::new(FlakyStore::new(inner));
        let retrying = RetryingObjectStore::new(flaky.clone());
        (flaky, retrying)
    }

    #[tokio::test]
    async fn ranged_get_retries_transient_errors() {
        let (flaky, retrying) = seeded();
        let path = Path::from("obj");
        retrying
            .put(&path, PutPayload::from_static(b"hello world"))
            .await
            .unwrap();

        flaky.fail_gets.store(2, Ordering::SeqCst);
        let got = retrying.get_range(&path, 0..5).await.unwrap();
        assert_eq!(got, Bytes::from_static(b"hello"));
        // 2 failures + 1 success.
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn truncated_range_body_is_retried() {
        let (flaky, retrying) = seeded();
        let path = Path::from("obj");
        retrying
            .put(&path, PutPayload::from_static(b"hello world"))
            .await
            .unwrap();

        flaky.truncate_gets.store(1, Ordering::SeqCst);
        let got = retrying.get_range(&path, 0..5).await.unwrap();
        assert_eq!(got, Bytes::from_static(b"hello"));
        // 1 truncated attempt (caught by the size check) + 1 clean retry.
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unsatisfiable_range_is_not_retried() {
        let (flaky, retrying) = seeded();
        let path = Path::from("obj");
        retrying
            .put(&path, PutPayload::from_static(b"short"))
            .await
            .unwrap();

        retrying
            .get_range(&path, 100..200)
            .await
            .expect_err("a range starting past EOF can never be satisfied");
        // Deterministic error: exactly one attempt, no retry loop.
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn not_found_is_not_retried() {
        let (flaky, retrying) = seeded();
        let err = retrying
            .get(&Path::from("missing"))
            .await
            .expect_err("object is absent");
        assert!(matches!(err, object_store::Error::NotFound { .. }));
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn put_retries_and_conditional_errors_pass_through() {
        let (flaky, retrying) = seeded();
        let path = Path::from("obj");

        flaky.fail_puts.store(1, Ordering::SeqCst);
        retrying
            .put(&path, PutPayload::from_static(b"v1"))
            .await
            .expect("put succeeds after a transient failure");

        let err = retrying
            .put_opts(
                &path,
                PutPayload::from_static(b"v2"),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect_err("create over an existing object");
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn list_retries_transient_errors() {
        let (flaky, retrying) = seeded();
        for name in ["a", "b", "c"] {
            retrying
                .put(&Path::from(name), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }

        flaky.fail_lists.store(1, Ordering::SeqCst);
        let listed: Vec<_> = retrying.list(None).try_collect().await.unwrap();
        assert_eq!(listed.len(), 3);
    }
}
