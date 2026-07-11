use crate::db::Db;
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::fs::errors::FsError;
use crate::task::spawn_named;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Pre-flush hook: seals the data-plane open segment (PUT) before the metadata
/// memtable is flushed, so a durable manifest never references an un-PUT segment.
type SealHook =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), FsError>> + Send>> + Send + Sync>;
type Reply = oneshot::Sender<Result<(), FsError>>;

enum Request {
    Flush(Reply),
    Close(Reply),
}

#[derive(Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<Request>,
    seal_hook: Arc<OnceLock<SealHook>>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<Db>) -> Self {
        let seal_hook: Arc<OnceLock<SealHook>> = Arc::new(OnceLock::new());
        let hook = Arc::clone(&seal_hook);
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();

        spawn_named("flush-coordinator", async move {
            while let Some(request) = receiver.recv().await {
                let mut pending_senders = Vec::new();
                let mut closer = None;
                match request {
                    Request::Flush(sender) => pending_senders.push(sender),
                    Request::Close(sender) => closer = Some(sender),
                }
                while closer.is_none() {
                    match receiver.try_recv() {
                        Ok(Request::Flush(sender)) => pending_senders.push(sender),
                        Ok(Request::Close(sender)) => closer = Some(sender),
                        Err(_) => break,
                    }
                }

                // A close keeps the barrier through db.close(), leaving no gap
                // in which a FrameLoc can commit after the final seal.
                let barrier = db.flush_barrier().write_owned().await;
                let result = match hook.get() {
                    Some(seal) => match seal().await {
                        Ok(()) => {
                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::FLUSH_AFTER_SEAL_BEFORE_MANIFEST);
                            db.flush().await.map_err(|_| FsError::IoError)
                        }
                        Err(e) => Err(e),
                    },
                    None => db.flush().await.map_err(|_| FsError::IoError),
                };

                let close_result = if closer.is_some() && result.is_ok() {
                    db.mark_closing();
                    db.close().await.map_err(|_| FsError::IoError)
                } else {
                    result
                };
                drop(barrier);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::FLUSH_AFTER_COMPLETE);

                for sender in pending_senders.drain(..) {
                    let _ = sender.send(result);
                }
                if let Some(closer) = closer {
                    let _ = closer.send(close_result);
                    while let Ok(request) = receiver.try_recv() {
                        match request {
                            Request::Flush(sender) | Request::Close(sender) => {
                                let _ = sender.send(Err(FsError::ShuttingDown));
                            }
                        }
                    }
                    return;
                }
            }
        });

        Self { sender, seal_hook }
    }

    /// Install the pre-flush seal hook (first call wins). Set once at bring-up,
    /// after the data plane is constructed.
    pub fn set_sealer(&self, hook: SealHook) {
        let _ = self.seal_hook.set(hook);
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(Request::Flush(tx))
            .map_err(|_| FsError::ShuttingDown)?;

        rx.await.map_err(|_| FsError::ShuttingDown)?
    }

    /// Seal, flush, and close under one barrier write lock. On error, the
    /// caller must exit without closing the database separately.
    pub async fn close(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(Request::Close(tx))
            .map_err(|_| FsError::ShuttingDown)?;

        rx.await.map_err(|_| FsError::ShuttingDown)?
    }
}
