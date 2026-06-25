use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

/// Wraps an [`ObjectStore`] and stamps a storage class onto every write.
///
/// The class is carried per-request as [`Attribute::StorageClass`], which each
/// backend translates into its own tiering header on both single PUTs and
/// multipart uploads.
///
/// The value is passed through verbatim, so it must be valid for the target
/// backend, and it must be a hot, instant-access class: ZeroFS reads
/// continuously, so archive tiers break reads and infrequent-access tiers add a
/// per-read fee.
#[derive(Debug)]
pub struct StorageClassObjectStore {
    inner: Arc<dyn ObjectStore>,
    storage_class: AttributeValue,
}

impl StorageClassObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, storage_class: impl Into<AttributeValue>) -> Self {
        Self {
            inner,
            storage_class: storage_class.into(),
        }
    }

    fn stamp(&self, attributes: &mut object_store::Attributes) {
        if attributes.get(&Attribute::StorageClass).is_none() {
            attributes.insert(Attribute::StorageClass, self.storage_class.clone());
        }
    }
}

/// Wraps `store` so every write is stamped with `storage_class`, or returns it
/// unchanged when no class is configured.
pub fn with_storage_class(
    store: Arc<dyn ObjectStore>,
    storage_class: Option<&str>,
) -> Arc<dyn ObjectStore> {
    match storage_class {
        Some(class) if !class.is_empty() => {
            Arc::new(StorageClassObjectStore::new(store, class.to_string()))
        }
        _ => store,
    }
}

impl Display for StorageClassObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StorageClassObjectStore({}, class={})",
            self.inner,
            self.storage_class.as_ref()
        )
    }
}

#[async_trait]
impl ObjectStore for StorageClassObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.stamp(&mut opts.attributes);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        mut opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.stamp(&mut opts.attributes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::{Attributes, ObjectStoreExt, PutMode};
    use std::sync::Mutex;

    /// Records the attributes of the most recent write, delegating storage to
    /// an in-memory backend.
    #[derive(Debug, Default)]
    struct RecordingStore {
        inner: InMemory,
        last_put: Mutex<Option<Attributes>>,
        last_multipart: Mutex<Option<Attributes>>,
    }

    impl Display for RecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            *self.last_put.lock().unwrap() = Some(opts.attributes.clone());
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            *self.last_multipart.lock().unwrap() = Some(opts.attributes.clone());
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
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn class_of(attrs: &Attributes) -> Option<&str> {
        attrs.get(&Attribute::StorageClass).map(|v| v.as_ref())
    }

    #[tokio::test]
    async fn stamps_storage_class_on_put_and_multipart() {
        let recorder = Arc::new(RecordingStore::default());
        let store = StorageClassObjectStore::new(recorder.clone(), "ONEZONE_IA".to_string());
        let path = Path::from("obj");

        store.put(&path, vec![1u8; 8].into()).await.unwrap();
        assert_eq!(
            class_of(recorder.last_put.lock().unwrap().as_ref().unwrap()),
            Some("ONEZONE_IA")
        );

        store.put_multipart(&path).await.unwrap();
        assert_eq!(
            class_of(recorder.last_multipart.lock().unwrap().as_ref().unwrap()),
            Some("ONEZONE_IA")
        );
    }

    #[tokio::test]
    async fn does_not_clobber_explicit_class() {
        let recorder = Arc::new(RecordingStore::default());
        let store = StorageClassObjectStore::new(recorder.clone(), "ONEZONE_IA".to_string());
        let path = Path::from("obj");

        let mut attributes = Attributes::new();
        attributes.insert(Attribute::StorageClass, "GLACIER".into());
        let opts = PutOptions {
            attributes,
            ..Default::default()
        };
        store
            .put_opts(&path, vec![1u8; 8].into(), opts)
            .await
            .unwrap();
        assert_eq!(
            class_of(recorder.last_put.lock().unwrap().as_ref().unwrap()),
            Some("GLACIER")
        );
    }

    #[tokio::test]
    async fn with_storage_class_none_does_not_wrap_or_stamp() {
        let recorder = Arc::new(RecordingStore::default());
        let store = with_storage_class(recorder.clone(), None);
        let path = Path::from("obj");

        store
            .put_opts(
                &path,
                vec![1u8; 8].into(),
                PutOptions::from(PutMode::Overwrite),
            )
            .await
            .unwrap();
        assert_eq!(
            class_of(recorder.last_put.lock().unwrap().as_ref().unwrap()),
            None
        );
    }

    #[tokio::test]
    async fn with_storage_class_empty_does_not_stamp() {
        let recorder = Arc::new(RecordingStore::default());
        let store = with_storage_class(recorder.clone(), Some(""));
        let path = Path::from("obj");

        store.put(&path, vec![1u8; 8].into()).await.unwrap();
        assert_eq!(
            class_of(recorder.last_put.lock().unwrap().as_ref().unwrap()),
            None
        );
    }
}
