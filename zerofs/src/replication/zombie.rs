//! An object store for in-process zombie / fencing tests: pass-through, but its
//! writes can be "isolated" to model a network partition (writes fail, reads keep
//! working). Lets a test drive a deposed-leader scenario and confirm SlateDB's
//! writer-epoch fencing rejects it, without standing up MinIO or real processes.

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct ZombieObjectStore {
    inner: Arc<dyn ObjectStore>,
    isolated: Arc<AtomicBool>,
}

impl ZombieObjectStore {
    /// Returns the store and the shared isolation flag (true = partition writes).
    pub fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<AtomicBool>) {
        let isolated = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                inner,
                isolated: isolated.clone(),
            }),
            isolated,
        )
    }

    fn check_writable(&self) -> object_store::Result<()> {
        if self.isolated.load(Ordering::Acquire) {
            Err(object_store::Error::Generic {
                store: "ZombieObjectStore",
                source: "node is partitioned (zombie)".into(),
            })
        } else {
            Ok(())
        }
    }
}

impl Display for ZombieObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ZombieObjectStore")
    }
}

#[async_trait]
impl ObjectStore for ZombieObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check_writable()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check_writable()?;
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
        // When partitioned, every delete fails fast (yielding once per input,
        // as `ObjectStoreExt::delete` expects).
        if self.isolated.load(Ordering::Acquire) {
            return locations
                .map(|loc| {
                    loc.and_then(|_| {
                        Err(object_store::Error::Generic {
                            store: "ZombieObjectStore",
                            source: "node is partitioned (zombie)".into(),
                        })
                    })
                })
                .boxed();
        }
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
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
        self.check_writable()?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use slatedb::DbBuilder;
    use slatedb::config::{PutOptions as SlatePutOptions, ReadOptions, WriteOptions};

    /// True if `err` is SlateDB's fenced-by-a-newer-writer signal.
    fn is_fenced(err: &slatedb::Error) -> bool {
        use slatedb::{CloseReason, ErrorKind};
        matches!(err.kind(), ErrorKind::Closed(CloseReason::Fenced))
    }

    fn no_wait() -> WriteOptions {
        WriteOptions {
            await_durable: false,
            ..Default::default()
        }
    }

    // A deposed leader cannot corrupt shared data: a newer writer has bumped the
    // epoch, so the zombie's flush is fenced (terminal). Committed data is the
    // new leader's.
    #[tokio::test]
    async fn deposed_leader_is_fenced_and_cannot_corrupt() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let (zombie_store, isolated) = ZombieObjectStore::new(inner.clone());
        let path = object_store::path::Path::from("data");
        let key = Bytes::from_static(b"k");

        // Leader A opens the data db (epoch 1) through the zombie store and commits.
        let a = DbBuilder::new(path.clone(), zombie_store.clone() as Arc<dyn ObjectStore>)
            .build()
            .await
            .unwrap();
        a.put_with_options(
            &key,
            &Bytes::from_static(b"from-A"),
            &SlatePutOptions::default(),
            &no_wait(),
        )
        .await
        .unwrap();
        a.flush().await.unwrap();

        // A is partitioned. A SlateDB flush would hang retrying (the real
        // partition behavior the lease covers), so confirm the partition with a
        // direct write that fails fast, and do NOT flush A while isolated.
        isolated.store(true, Ordering::Release);
        assert!(
            zombie_store
                .put(
                    &object_store::path::Path::from("probe"),
                    PutPayload::from(Bytes::from_static(b"x")),
                )
                .await
                .is_err(),
            "an isolated (partitioned) store must reject writes"
        );

        // Leader B opens the same data db directly (epoch 2), fencing A.
        let b = DbBuilder::new(path.clone(), inner.clone())
            .build()
            .await
            .unwrap();
        b.put_with_options(
            &key,
            &Bytes::from_static(b"from-B"),
            &SlatePutOptions::default(),
            &no_wait(),
        )
        .await
        .unwrap();
        b.flush().await.unwrap();

        // A's partition heals and it tries to commit. The fence (B's higher
        // epoch) is terminal and may surface at the put or the flush depending on
        // timing; either way A cannot commit.
        isolated.store(false, Ordering::Release);
        let outcome = match a
            .put_with_options(
                &key,
                &Bytes::from_static(b"zombie"),
                &SlatePutOptions::default(),
                &no_wait(),
            )
            .await
        {
            Ok(_handle) => a.flush().await,
            Err(e) => Err(e),
        };
        assert!(
            outcome.is_err(),
            "a reconnected zombie must not be able to commit"
        );
        assert!(
            is_fenced(outcome.as_ref().unwrap_err()),
            "a reconnected zombie must be fenced, got {outcome:?}"
        );

        // The committed data is B's; the zombie never corrupted it.
        let reader = DbBuilder::new(path, inner).build().await.unwrap();
        let v = reader
            .get_with_options(&key, &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            v.as_deref(),
            Some(&b"from-B"[..]),
            "a zombie's writes must never become visible"
        );

        // These dbs are fenced/contended: drop, never clean-close.
        drop(a);
        drop(b);
        drop(reader);
    }
}
