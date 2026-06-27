//! Object-store request tracing: the backend analogue of `fatrace`.
//!
//! [`TracingObjectStore`] wraps the raw S3/GCS/Azure/local store at the bottom
//! of the wrapper stack, so it sees the requests that actually leave the
//! process: cache misses, prefetch reads, compaction, WAL writes, deletes.
//! Reads served from the prefetch cache make no backend request and so produce
//! no event, which is exactly what a request trace should show.
//!
//! Like [`AccessTracer`](crate::fs::tracing::AccessTracer), the tracer is
//! zero-cost when no one is watching: every method short-circuits to a plain
//! delegation until a subscriber connects.

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// A backend object-store request with its operation-specific parameters.
#[derive(Clone, Debug)]
pub enum ObjectOperation {
    /// A GET. `offset`/`length` describe the requested range (both `None` for a
    /// full-object read; `offset` `None` with a `length` is a suffix range).
    Get {
        offset: Option<u64>,
        length: Option<u64>,
    },
    Head,
    Put {
        size: u64,
    },
    PutMultipart,
    Delete,
    List,
    Copy {
        to: String,
    },
    Rename {
        to: String,
    },
}

impl Display for ObjectOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            ObjectOperation::Get { .. } => "get   ",
            ObjectOperation::Head => "head  ",
            ObjectOperation::Put { .. } => "put   ",
            ObjectOperation::PutMultipart => "mpart ",
            ObjectOperation::Delete => "delete",
            ObjectOperation::List => "list  ",
            ObjectOperation::Copy { .. } => "copy  ",
            ObjectOperation::Rename { .. } => "rename",
        };
        write!(f, "{}", s)
    }
}

/// An object-store access event emitted when a backend request completes.
#[derive(Clone, Debug)]
pub struct ObjectAccessEvent {
    pub timestamp: u64,
    /// Which backend the request hit: `"data"` or `"wal"`.
    pub store: &'static str,
    pub operation: ObjectOperation,
    pub path: String,
    /// How long the request took. `None` for stream-shaped ops (list, delete)
    /// where there is no single request to time.
    pub duration_us: Option<u64>,
    pub error: bool,
}

/// Traces object-store requests and broadcasts them to subscribers.
///
/// Cloning is cheap (a shared broadcast sender); the same tracer is handed to
/// every store wrapper and to the RPC server.
#[derive(Clone)]
pub struct ObjectTracer {
    sender: broadcast::Sender<ObjectAccessEvent>,
}

impl ObjectTracer {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { sender }
    }

    /// True when at least one subscriber is connected. Every wrapper method
    /// checks this first so tracing adds nothing when no one is watching.
    pub fn has_subscribers(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ObjectAccessEvent> {
        self.sender.subscribe()
    }

    fn emit(
        &self,
        store: &'static str,
        operation: ObjectOperation,
        path: String,
        duration: Option<Duration>,
        error: bool,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = self.sender.send(ObjectAccessEvent {
            timestamp,
            store,
            operation,
            path,
            duration_us: duration.map(|d| d.as_micros() as u64),
            error,
        });
    }
}

impl Default for ObjectTracer {
    fn default() -> Self {
        Self::new()
    }
}

/// An [`ObjectStore`] decorator that reports each backend request to an
/// [`ObjectTracer`]. Every other behaviour is a transparent delegation.
pub struct TracingObjectStore {
    inner: Arc<dyn ObjectStore>,
    tracer: ObjectTracer,
    store: &'static str,
}

impl TracingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, tracer: ObjectTracer, store: &'static str) -> Self {
        Self {
            inner,
            tracer,
            store,
        }
    }
}

impl Debug for TracingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracingObjectStore")
            .field("inner", &self.inner)
            .field("store", &self.store)
            .finish()
    }
}

impl Display for TracingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Transparent: keep the backend's own name in logs.
        write!(f, "{}", self.inner)
    }
}

fn classify_get(options: &GetOptions) -> ObjectOperation {
    if options.head {
        return ObjectOperation::Head;
    }
    match &options.range {
        Some(GetRange::Bounded(r)) => ObjectOperation::Get {
            offset: Some(r.start),
            length: Some(r.end - r.start),
        },
        Some(GetRange::Offset(o)) => ObjectOperation::Get {
            offset: Some(*o),
            length: None,
        },
        Some(GetRange::Suffix(n)) => ObjectOperation::Get {
            offset: None,
            length: Some(*n),
        },
        None => ObjectOperation::Get {
            offset: None,
            length: None,
        },
    }
}

#[async_trait]
impl ObjectStore for TracingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !self.tracer.has_subscribers() {
            return self.inner.get_opts(location, options).await;
        }
        let op = classify_get(&options);
        let start = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        self.tracer.emit(
            self.store,
            op,
            location.to_string(),
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !self.tracer.has_subscribers() {
            return self.inner.put_opts(location, payload, opts).await;
        }
        let size = payload.content_length() as u64;
        let start = Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await;
        self.tracer.emit(
            self.store,
            ObjectOperation::Put { size },
            location.to_string(),
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if !self.tracer.has_subscribers() {
            return self.inner.put_multipart_opts(location, opts).await;
        }
        let start = Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        // Records the open; the per-part uploads happen on the returned handle.
        self.tracer.emit(
            self.store,
            ObjectOperation::PutMultipart,
            location.to_string(),
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if !self.tracer.has_subscribers() {
            return self.inner.delete_stream(locations);
        }
        let tracer = self.tracer.clone();
        let store = self.store;
        self.inner
            .delete_stream(locations)
            .map(move |res| {
                match &res {
                    Ok(path) => tracer.emit(
                        store,
                        ObjectOperation::Delete,
                        path.to_string(),
                        None,
                        false,
                    ),
                    Err(_) => {
                        tracer.emit(store, ObjectOperation::Delete, String::new(), None, true)
                    }
                }
                res
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.tracer.has_subscribers() {
            self.tracer.emit(
                self.store,
                ObjectOperation::List,
                prefix.map(|p| p.to_string()).unwrap_or_default(),
                None,
                false,
            );
        }
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.tracer.has_subscribers() {
            self.tracer.emit(
                self.store,
                ObjectOperation::List,
                prefix.map(|p| p.to_string()).unwrap_or_default(),
                None,
                false,
            );
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        if !self.tracer.has_subscribers() {
            return self.inner.list_with_delimiter(prefix).await;
        }
        let path = prefix.map(|p| p.to_string()).unwrap_or_default();
        let start = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        self.tracer.emit(
            self.store,
            ObjectOperation::List,
            path,
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        if !self.tracer.has_subscribers() {
            return self.inner.copy_opts(from, to, options).await;
        }
        let start = Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        self.tracer.emit(
            self.store,
            ObjectOperation::Copy { to: to.to_string() },
            from.to_string(),
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        if !self.tracer.has_subscribers() {
            return self.inner.rename_opts(from, to, options).await;
        }
        let start = Instant::now();
        let result = self.inner.rename_opts(from, to, options).await;
        self.tracer.emit(
            self.store,
            ObjectOperation::Rename { to: to.to_string() },
            from.to_string(),
            Some(start.elapsed()),
            result.is_err(),
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    fn traced() -> (TracingObjectStore, ObjectTracer) {
        let tracer = ObjectTracer::new();
        let inner = Arc::new(InMemory::new());
        (
            TracingObjectStore::new(inner, tracer.clone(), "data"),
            tracer,
        )
    }

    #[tokio::test]
    async fn put_get_head_delete_emit_events_in_order() {
        let (store, tracer) = traced();
        let mut rx = tracer.subscribe();
        let path = Path::from("a/b");

        store
            .put(&path, PutPayload::from_static(b"hello"))
            .await
            .unwrap();
        store.get(&path).await.unwrap();
        store.head(&path).await.unwrap();
        store.delete(&path).await.unwrap();

        let put = rx.recv().await.unwrap();
        assert_eq!(put.store, "data");
        assert_eq!(put.path, "a/b");
        assert!(!put.error);
        assert!(matches!(put.operation, ObjectOperation::Put { size: 5 }));
        // Request-shaped ops carry a measured duration.
        assert!(put.duration_us.is_some());

        let get = rx.recv().await.unwrap();
        assert!(matches!(
            get.operation,
            ObjectOperation::Get {
                offset: None,
                length: None
            }
        ));

        let head = rx.recv().await.unwrap();
        assert!(matches!(head.operation, ObjectOperation::Head));

        let del = rx.recv().await.unwrap();
        assert!(matches!(del.operation, ObjectOperation::Delete));
        assert_eq!(del.path, "a/b");
        // Stream-shaped op: no single request to time.
        assert!(del.duration_us.is_none());
    }

    #[tokio::test]
    async fn ranged_get_records_offset_and_length() {
        let (store, tracer) = traced();
        let path = Path::from("obj");
        store
            .put(&path, PutPayload::from_static(b"0123456789"))
            .await
            .unwrap();
        let mut rx = tracer.subscribe();
        store.get_range(&path, 2..6).await.unwrap();
        let e = rx.recv().await.unwrap();
        assert!(matches!(
            e.operation,
            ObjectOperation::Get {
                offset: Some(2),
                length: Some(4)
            }
        ));
    }

    #[tokio::test]
    async fn a_failed_request_sets_the_error_flag() {
        let (store, tracer) = traced();
        let mut rx = tracer.subscribe();
        assert!(store.get(&Path::from("missing")).await.is_err());
        let e = rx.recv().await.unwrap();
        assert!(e.error);
        assert!(matches!(e.operation, ObjectOperation::Get { .. }));
    }

    #[tokio::test]
    async fn no_subscribers_stays_silent_and_transparent() {
        let (store, tracer) = traced();
        assert!(!tracer.has_subscribers());
        let path = Path::from("x");
        store
            .put(&path, PutPayload::from_static(b"y"))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], b"y");
        assert!(!tracer.has_subscribers());
    }
}
