use futures::StreamExt;
use object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path,
};
use std::sync::Arc;
use uuid::Uuid;

const TEST_FILE_PREFIX: &str = ".zerofs_compatibility_test_";

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

    let test_id = Uuid::new_v4();
    let test_path = Path::from(db_path).join(format!("{}{}", TEST_FILE_PREFIX, test_id));

    tracing::info!("Checking storage provider compatibility (conditional writes for fencing)...");

    // Create the object and keep its version for the If-Match checks below.
    let created = object_store
        .put(&test_path, "initial".into())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write test file: {e:#?}"))?;
    let version = UpdateVersion::from(created);
    if version.e_tag.is_none() && version.version.is_none() {
        let _ = object_store.delete(&test_path).await;
        return Err(anyhow::anyhow!(
            "Storage provider returned no ETag or version on PUT, so If-Match conditional \
             writes are impossible."
        ));
    }

    // If-None-Match: a second Create must be rejected because the object exists.
    let create = object_store
        .put_opts(
            &test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Create),
        )
        .await;

    // If-Match with the current version must succeed, moving the object forward.
    let update_current = object_store
        .put_opts(
            &test_path,
            "updated".into(),
            PutOptions::from(PutMode::Update(version.clone())),
        )
        .await;

    // If-Match with the now-stale version must be rejected.
    let update_stale = object_store
        .put_opts(
            &test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Update(version)),
        )
        .await;

    let _ = object_store.delete(&test_path).await;

    // put-if-not-exists: creating over an existing object must fail.
    match create {
        Err(Error::AlreadyExists { .. }) => {}
        Ok(_) => {
            return Err(unsupported(
                "PutMode::Create (If-None-Match) overwrote an existing object instead of failing",
            ));
        }
        Err(e) => return Err(classify("PutMode::Create (If-None-Match)", e)),
    }

    // put-if-match: an update carrying the current version must be accepted...
    match update_current {
        Ok(_) => {}
        Err(Error::Precondition { .. }) => {
            return Err(unsupported(
                "PutMode::Update (If-Match) rejected the current version",
            ));
        }
        Err(e) => return Err(classify("PutMode::Update (If-Match)", e)),
    }

    // ...and one carrying a stale version must be rejected. Accepting it means
    // the CAS is a no-op and fencing is unsafe.
    match update_stale {
        Err(Error::Precondition { .. }) => {}
        Ok(_) => {
            return Err(unsupported(
                "PutMode::Update (If-Match) accepted a stale version",
            ));
        }
        Err(e) => return Err(classify("PutMode::Update (If-Match) stale check", e)),
    }

    tracing::info!("Storage provider compatibility check passed");
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

/// A conditional write returned an error. `NotImplemented` / HTTP 501 means the
/// provider lacks conditional writes; anything else is surfaced verbatim.
fn classify(op: &str, e: Error) -> anyhow::Error {
    let unimplemented = matches!(e, Error::NotImplemented { .. }) || {
        let s = e.to_string().to_lowercase();
        s.contains("501") || s.contains("not implemented")
    };
    if unimplemented {
        anyhow::anyhow!(
            "Storage provider does not support conditional writes ({op}), required for fencing \
             in ZeroFS. Use a provider that supports them, or set `conditional_put` (Redis).\n\n{e}"
        )
    } else {
        anyhow::anyhow!("Storage provider precondition check failed for {op}: {e}")
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
    #[derive(Debug)]
    struct IgnoresIfMatch(Arc<dyn ObjectStore>);

    impl std::fmt::Display for IgnoresIfMatch {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IgnoresIfMatch")
        }
    }

    #[async_trait]
    impl ObjectStore for IgnoresIfMatch {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if matches!(opts.mode, PutMode::Update(_)) {
                opts.mode = PutMode::Overwrite;
            }
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.0.get_opts(location, options).await
        }
        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.0.get_ranges(location, ranges).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(prefix)
        }
        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> object_store::Result<()> {
            self.0.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn store_that_ignores_if_match_is_rejected() {
        let store: Arc<dyn ObjectStore> = Arc::new(IgnoresIfMatch(Arc::new(InMemory::new())));
        let err = check_if_match_support(&store, "db")
            .await
            .expect_err("a store that ignores If-Match must be rejected");
        // Caught at the stale-version check, not silently accepted.
        assert!(err.to_string().to_lowercase().contains("stale"), "{err}");
    }
}
