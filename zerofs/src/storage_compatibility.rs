use futures::StreamExt;
use object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path,
};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const TEST_FILE_PREFIX: &str = ".zerofs_compatibility_test_";
const RETRY_MIN_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum ProbeError {
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

impl ProbeError {
    fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }

    fn error(&self) -> &anyhow::Error {
        match self {
            Self::Transient(error) | Self::Fatal(error) => error,
        }
    }

    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Transient(error) | Self::Fatal(error) => error,
        }
    }
}

/// Verify the storage provider honors the conditional writes ZeroFS fencing and
/// the SlateDB manifest boundary rely on: both put-if-not-exists
/// (`PutMode::Create`, `If-None-Match`) and put-if-match (`PutMode::Update`,
/// `If-Match`). Some S3-compatible providers implement one but not the other, or none at all.
pub async fn check_if_match_support(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<()> {
    // Clean up any old test files from previous runs (best effort)
    let prefix_path = Path::from(db_path).join(TEST_FILE_PREFIX);
    let mut list = object_store.list(Some(&prefix_path));
    while let Some(Ok(meta)) = list.next().await {
        let _ = object_store.delete(&meta.location).await;
    }

    tracing::info!("Checking storage provider compatibility...");

    let mut retry_delay = RETRY_MIN_DELAY;
    loop {
        match check_if_match_support_once(object_store, db_path).await {
            Ok(()) => break,
            Err(error) if error.is_transient() => {
                tracing::info!(
                    error = ?error.error(),
                    delay = ?retry_delay,
                    "retrying storage provider compatibility check"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = std::cmp::min(retry_delay * 2, RETRY_MAX_DELAY);
            }
            Err(error) => return Err(error.into_error()),
        }
    }

    tracing::info!("Storage provider compatibility check passed");
    Ok(())
}

/// Run one compatibility probe against a fresh object. Retrying the whole probe,
/// rather than an individual conditional PUT, makes an applied request whose
/// response was lost unambiguous: the next attempt uses a new object and ETag.
async fn check_if_match_support_once(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> Result<(), ProbeError> {
    let test_id = Uuid::new_v4();
    let test_path = Path::from(db_path).join(format!("{}{}", TEST_FILE_PREFIX, test_id));
    let result = check_test_object(object_store, &test_path).await;
    let _ = object_store.delete(&test_path).await;
    result
}

async fn check_test_object(
    object_store: &Arc<dyn ObjectStore>,
    test_path: &Path,
) -> Result<(), ProbeError> {
    // Create the object and keep its version for the If-Match checks below.
    let created = object_store
        .put(test_path, "initial".into())
        .await
        .map_err(|e| classify("initial PUT", e))?;
    let version = UpdateVersion::from(created);
    if version.e_tag.is_none() && version.version.is_none() {
        return Err(ProbeError::Fatal(anyhow::anyhow!(
            "Storage provider returned no ETag or version on PUT, so If-Match conditional \
             writes are impossible."
        )));
    }

    // If-None-Match: a second Create must be rejected because the object exists.
    match object_store
        .put_opts(
            test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Err(Error::AlreadyExists { .. }) => {}
        Ok(_) => {
            return Err(ProbeError::Fatal(unsupported(
                "PutMode::Create (If-None-Match) overwrote an existing object instead of failing",
            )));
        }
        Err(error) => return Err(classify("PutMode::Create (If-None-Match)", error)),
    }

    // If-Match with the current version must succeed, moving the object forward.
    match object_store
        .put_opts(
            test_path,
            "updated".into(),
            PutOptions::from(PutMode::Update(version.clone())),
        )
        .await
    {
        Ok(_) => {}
        Err(Error::Precondition { .. }) => {
            return Err(ProbeError::Fatal(unsupported(
                "PutMode::Update (If-Match) rejected the current version",
            )));
        }
        Err(error) => return Err(classify("PutMode::Update (If-Match)", error)),
    }

    // If-Match with the now-stale version must be rejected.
    match object_store
        .put_opts(
            test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Update(version)),
        )
        .await
    {
        Err(Error::Precondition { .. }) => {}
        Ok(_) => {
            return Err(ProbeError::Fatal(unsupported(
                "PutMode::Update (If-Match) accepted a stale version",
            )));
        }
        Err(error) => {
            return Err(classify("PutMode::Update (If-Match) stale check", error));
        }
    }

    Ok(())
}

/// The provider answered a conditional write incorrectly (wrong result, not an
/// error). Points at the `conditional_put` Redis coordinator, which enforces the
/// CAS itself without the provider's precondition headers.
fn unsupported(detail: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "Storage provider does not correctly support the conditional writes ZeroFS requires: {detail}. Use a provider with full conditional-write support, or set a \
         Redis coordinator (`conditional_put`) so ZeroFS enforces the CAS itself."
    )
}

/// Classify a probe operation error. Provider/configuration and conditional
/// failures are fatal; unexpected transport/runtime failures restart the whole
/// probe with a fresh object.
fn classify(op: &str, e: Error) -> ProbeError {
    let unimplemented = matches!(e, Error::NotImplemented { .. }) || {
        let s = e.to_string().to_lowercase();
        s.contains("501") || s.contains("not implemented")
    };
    if unimplemented {
        ProbeError::Fatal(anyhow::anyhow!(
            "Storage provider does not support conditional writes ({op}), required for fencing \
             in ZeroFS. Use a provider that supports them, or set `conditional_put` (Redis).\n\n{e}"
        ))
    } else {
        let retryable = !matches!(
            e,
            Error::AlreadyExists { .. }
                | Error::Precondition { .. }
                | Error::NotModified { .. }
                | Error::NotFound { .. }
                | Error::NotImplemented { .. }
                | Error::NotSupported { .. }
                | Error::PermissionDenied { .. }
                | Error::Unauthenticated { .. }
                | Error::InvalidPath { .. }
                | Error::UnknownConfigurationKey { .. }
        );
        let error = anyhow::anyhow!("Storage provider precondition check failed for {op}: {e}");
        if retryable {
            ProbeError::Transient(error)
        } else {
            ProbeError::Fatal(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutPayload, PutResult, RenameOptions,
    };
    use std::ops::Range;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn conformant_store_passes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        check_if_match_support(&store, "db")
            .await
            .expect("InMemory supports both conditional-write modes");
    }

    /// A store that supports put-if-not-exists but silently ignores `If-Match` on
    /// `Update` (overwrites regardless of version). This is the failure that
    /// passed the old Create-only probe and then livelocked the SlateDB boundary
    /// in production.
    #[derive(Debug, Clone, Copy)]
    enum UpdateBehavior {
        IgnoreIfMatch,
        FailBeforeApplyOnce,
        FailAfterApplyOnce,
    }

    #[derive(Debug)]
    struct TestStore {
        inner: Arc<dyn ObjectStore>,
        behavior: UpdateBehavior,
        failed_once: AtomicBool,
        initial_put_paths: Mutex<Vec<Path>>,
    }

    impl TestStore {
        fn new(behavior: UpdateBehavior) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                behavior,
                failed_once: AtomicBool::new(false),
                initial_put_paths: Mutex::new(Vec::new()),
            }
        }

        fn transient_error() -> Error {
            Error::Generic {
                store: "compatibility-test",
                source: "transient conditional PUT failure".into(),
            }
        }
    }

    impl std::fmt::Display for TestStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if matches!(&opts.mode, PutMode::Overwrite) {
                self.initial_put_paths
                    .lock()
                    .unwrap()
                    .push(location.clone());
            }

            let is_update = matches!(&opts.mode, PutMode::Update(_));
            match self.behavior {
                UpdateBehavior::IgnoreIfMatch if is_update => {
                    opts.mode = PutMode::Overwrite;
                }
                UpdateBehavior::FailBeforeApplyOnce
                    if is_update && !self.failed_once.swap(true, Ordering::SeqCst) =>
                {
                    return Err(Self::transient_error());
                }
                UpdateBehavior::FailAfterApplyOnce
                    if is_update && !self.failed_once.swap(true, Ordering::SeqCst) =>
                {
                    self.inner.put_opts(location, payload, opts).await?;
                    return Err(Self::transient_error());
                }
                _ => {}
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
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
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

    #[tokio::test]
    async fn store_that_ignores_if_match_is_rejected() {
        let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(UpdateBehavior::IgnoreIfMatch));
        let err = check_if_match_support(&store, "db")
            .await
            .expect_err("a store that ignores If-Match must be rejected");
        // Caught at the stale-version check, not silently accepted.
        assert!(err.to_string().to_lowercase().contains("stale"), "{err}");
    }

    async fn assert_transient_update_restarts_probe(behavior: UpdateBehavior) {
        let test_store = Arc::new(TestStore::new(behavior));
        let store: Arc<dyn ObjectStore> = test_store.clone();

        check_if_match_support(&store, "db")
            .await
            .expect("a transient conditional PUT failure should be retried");

        let paths = test_store.initial_put_paths.lock().unwrap();
        assert_eq!(paths.len(), 2, "the full probe should run twice");
        assert_ne!(paths[0], paths[1], "each probe must use a fresh object");
    }

    #[tokio::test]
    async fn transient_update_failure_restarts_probe() {
        assert_transient_update_restarts_probe(UpdateBehavior::FailBeforeApplyOnce).await;
    }

    #[tokio::test]
    async fn lost_update_response_restarts_probe_with_fresh_object() {
        assert_transient_update_restarts_probe(UpdateBehavior::FailAfterApplyOnce).await;
    }
}
