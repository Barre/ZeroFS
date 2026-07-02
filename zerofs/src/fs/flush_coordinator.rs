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

#[derive(Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<oneshot::Sender<Result<(), FsError>>>,
    seal_hook: Arc<OnceLock<SealHook>>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<Db>) -> Self {
        let seal_hook: Arc<OnceLock<SealHook>> = Arc::new(OnceLock::new());
        let hook = Arc::clone(&seal_hook);
        let (sender, mut receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<Result<(), FsError>>>();

        spawn_named("flush-coordinator", async move {
            let mut pending_senders = Vec::new();

            while let Some(sender) = receiver.recv().await {
                pending_senders.push(sender);

                while let Ok(sender) = receiver.try_recv() {
                    pending_senders.push(sender);
                }

                // Seal first (the durability barrier); if sealing fails, don't
                // flush — that would durably commit dangling extents. Hold the
                // flush barrier across seal+flush so a concurrent commit can't
                // slip a pointer to the newly-rotated open buffer into the
                // flushed set.
                let result = {
                    let _barrier = db.flush_barrier().write_owned().await;
                    match hook.get() {
                        Some(seal) => match seal().await {
                            Ok(()) => {
                                #[cfg(feature = "failpoints")]
                                fail_point!(fp::FLUSH_AFTER_SEAL_BEFORE_MANIFEST);
                                db.flush().await.map_err(|_| FsError::IoError)
                            }
                            Err(e) => Err(e),
                        },
                        None => db.flush().await.map_err(|_| FsError::IoError),
                    }
                };

                #[cfg(feature = "failpoints")]
                fail_point!(fp::FLUSH_AFTER_COMPLETE);

                for sender in pending_senders.drain(..) {
                    let _ = sender.send(result);
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

        self.sender.send(tx).map_err(|_| FsError::IoError)?;

        rx.await.map_err(|_| FsError::IoError)?
    }
}
