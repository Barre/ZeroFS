//! Delay SlateDB manifest PUTs until ZeroFS's open segment is durable.
//!
//! SlateDB uploads every SST before writing the manifest object that references it.
//! ZeroFS uses that ordering to upload the SSTs and its open segment concurrently,
//! while blocking the manifest PUT until the segment PUT succeeds.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

/// SlateDB thresholds that prevent size-triggered memtable flushes.
/// Public so integration tests can use the same settings as production.
#[doc(hidden)]
pub const COORDINATED_L0_SST_SIZE_BYTES: usize = usize::MAX - 1;
#[doc(hidden)]
pub const COORDINATED_MAX_UNFLUSHED_BYTES: usize = usize::MAX;

#[derive(Debug, thiserror::Error)]
#[error("manifest PUT blocked because ZeroFS segment sealing did not complete")]
struct SegmentSealIncomplete;

#[derive(Debug, thiserror::Error)]
#[error("multipart writes to SlateDB manifest objects are not supported by ZeroFS")]
struct MultipartManifest;

#[derive(Debug)]
struct PublicationState {
    access: Arc<RwLock<()>>,
    disabled: AtomicBool,
    #[cfg(test)]
    waiting_manifest_writes: AtomicU64,
    #[cfg(test)]
    waiting_notify: Notify,
}

/// Coordinates manifest PUTs made through writer and compactor
/// [`ManifestPublicationStore`] wrappers.
///
/// Dropping an unreleased hold permanently rejects later manifest PUTs for this
/// database instance. Releasing a blocked PUT when the segment PUT failed could
/// make recovered metadata reference a missing segment.
#[derive(Clone, Debug)]
pub struct ManifestPublication {
    state: Arc<PublicationState>,
}

impl ManifestPublication {
    pub fn new() -> Self {
        Self {
            state: Arc::new(PublicationState {
                access: Arc::new(RwLock::new(())),
                disabled: AtomicBool::new(false),
                #[cfg(test)]
                waiting_manifest_writes: AtomicU64::new(0),
                #[cfg(test)]
                waiting_notify: Notify::new(),
            }),
        }
    }

    /// Block writer and compactor manifest PUTs until the hold is released.
    ///
    /// The hold waits for any writer or compactor manifest PUT already in
    /// progress before allowing the caller to start a SlateDB flush.
    pub async fn hold(&self) -> object_store::Result<ManifestPublicationHold> {
        if self.state.disabled.load(Ordering::Acquire) {
            return Err(segment_seal_incomplete());
        }
        let write_guard = Arc::clone(&self.state.access).write_owned().await;
        if self.state.disabled.load(Ordering::Acquire) {
            return Err(segment_seal_incomplete());
        }
        Ok(ManifestPublicationHold {
            state: Arc::clone(&self.state),
            write_guard: Some(write_guard),
        })
    }

    #[cfg(test)]
    pub fn waiting_manifest_writes(&self) -> u64 {
        self.state.waiting_manifest_writes.load(Ordering::Acquire)
    }

    /// Wait until a manifest PUT has actually encountered an active hold.
    /// Used by the real-SlateDB integration test.
    #[cfg(test)]
    pub async fn wait_for_manifest_write_after(&self, previous: u64) {
        loop {
            let notified = self.state.waiting_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.waiting_manifest_writes() > previous {
                return;
            }
            notified.await;
        }
    }
}

impl Default for ManifestPublication {
    fn default() -> Self {
        Self::new()
    }
}

/// Exclusive block on writer and compactor manifest PUTs.
#[must_use = "dropping an unreleased hold permanently disables manifest PUTs"]
pub struct ManifestPublicationHold {
    state: Arc<PublicationState>,
    write_guard: Option<OwnedRwLockWriteGuard<()>>,
}

impl ManifestPublicationHold {
    /// Allow blocked manifest PUTs to continue after the segment PUT succeeds.
    pub fn release(mut self) {
        self.write_guard.take();
    }
}

impl Drop for ManifestPublicationHold {
    fn drop(&mut self) {
        if self.write_guard.is_some() {
            // Blocked and future manifest PUTs fail instead of referencing a
            // segment whose PUT did not complete.
            self.state.disabled.store(true, Ordering::Release);
        }
        self.write_guard.take();
    }
}

/// Intercepts writes to SlateDB's numbered manifest objects.
///
/// Writer and compactor manifest PUTs may run concurrently. A ZeroFS flush
/// blocks both while sealing the open segment. Other object-store operations
/// pass through unchanged.
#[derive(Debug)]
pub struct ManifestPublicationStore {
    inner: Arc<dyn ObjectStore>,
    manifest_dir: Path,
    publication: ManifestPublication,
}

impl ManifestPublicationStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        db_path: Path,
        publication: ManifestPublication,
    ) -> Self {
        Self {
            inner,
            manifest_dir: db_path.join("manifest"),
            publication,
        }
    }

    fn is_manifest(&self, location: &Path) -> bool {
        location.extension() == Some("manifest")
            && location
                .prefix_match(&self.manifest_dir)
                .is_some_and(|mut suffix| suffix.next().is_some() && suffix.next().is_none())
    }

    async fn manifest_access_for(
        &self,
        location: &Path,
    ) -> object_store::Result<Option<OwnedRwLockReadGuard<()>>> {
        if !self.is_manifest(location) {
            return Ok(None);
        }
        if self.publication.state.disabled.load(Ordering::Acquire) {
            return Err(segment_seal_incomplete());
        }

        let access = Arc::clone(&self.publication.state.access);
        let guard = match access.clone().try_read_owned() {
            Ok(guard) => guard,
            Err(_) => {
                #[cfg(test)]
                {
                    self.publication
                        .state
                        .waiting_manifest_writes
                        .fetch_add(1, Ordering::AcqRel);
                    self.publication.state.waiting_notify.notify_waiters();
                }
                #[cfg(feature = "failpoints")]
                fail_point!(fp::MANIFEST_PUBLICATION_WAITING);
                access.read_owned().await
            }
        };
        if self.publication.state.disabled.load(Ordering::Acquire) {
            return Err(segment_seal_incomplete());
        }
        Ok(Some(guard))
    }
}

impl Display for ManifestPublicationStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ManifestPublicationStore({})", self.inner)
    }
}

fn segment_seal_incomplete() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(SegmentSealIncomplete),
    }
}

#[async_trait]
impl ObjectStore for ManifestPublicationStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _manifest_access = self.manifest_access_for(location).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Keeping manifest writes blocked for a multipart upload would require
        // wrapping the upload through complete or abort. SlateDB currently writes
        // each manifest with one atomic PUT, so reject multipart manifests.
        if self.is_manifest(location) {
            return Err(object_store::Error::NotSupported {
                source: Box::new(MultipartManifest),
            });
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
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

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let _manifest_access = self.manifest_access_for(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let _manifest_access = self.manifest_access_for(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    #[test]
    fn recognizes_only_direct_manifest_children() {
        let store = ManifestPublicationStore::new(
            Arc::new(InMemory::new()),
            Path::from("db"),
            ManifestPublication::new(),
        );

        assert!(store.is_manifest(&Path::from("db/manifest/00000000000000000001.manifest")));
        assert!(!store.is_manifest(&Path::from("db/compacted/file.manifest")));
        assert!(!store.is_manifest(&Path::from("db/manifest/nested/file.manifest")));
        assert!(!store.is_manifest(&Path::from("db/manifest/manifest-boundary")));
    }

    #[tokio::test]
    async fn hold_blocks_manifest_put_but_not_sst_put() {
        let inner = Arc::new(InMemory::new());
        let publication = ManifestPublication::new();
        let store: Arc<dyn ObjectStore> = Arc::new(ManifestPublicationStore::new(
            inner.clone(),
            Path::from("db"),
            publication.clone(),
        ));
        let hold = publication.hold().await.unwrap();
        let before = publication.waiting_manifest_writes();

        store
            .put(
                &Path::from("db/compacted/sst"),
                Bytes::from_static(b"sst").into(),
            )
            .await
            .unwrap();

        let manifest_store = Arc::clone(&store);
        let manifest_put = tokio::spawn(async move {
            manifest_store
                .put(
                    &Path::from("db/manifest/00000000000000000001.manifest"),
                    Bytes::from_static(b"manifest").into(),
                )
                .await
        });
        publication.wait_for_manifest_write_after(before).await;
        assert!(
            inner
                .head(&Path::from("db/manifest/00000000000000000001.manifest"))
                .await
                .is_err()
        );

        hold.release();
        manifest_put.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn unreleased_hold_fails_manifest_puts_with_terminal_error() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let publication = ManifestPublication::new();
        let store = Arc::new(ManifestPublicationStore::new(
            inner,
            Path::from("db"),
            publication.clone(),
        ));
        let hold = publication.hold().await.unwrap();
        let before = publication.waiting_manifest_writes();

        let manifest_put = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                store
                    .put(
                        &Path::from("db/manifest/00000000000000000001.manifest"),
                        Bytes::from_static(b"manifest").into(),
                    )
                    .await
            }
        });
        publication.wait_for_manifest_write_after(before).await;
        drop(hold);

        assert!(matches!(
            manifest_put.await.unwrap(),
            Err(object_store::Error::NotSupported { .. })
        ));
        assert!(publication.hold().await.is_err());
    }
}
