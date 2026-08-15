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

/// Move-only evidence that the shared flush coordinator completed a durability
/// barrier. The private field keeps callers from manufacturing a receipt and
/// clearing an HA reconnect barrier without performing the flush.
#[must_use = "the receipt must discharge the HA base-flush requirement"]
pub(crate) struct FlushReceipt {
    _private: (),
}

#[cfg(test)]
impl FlushReceipt {
    pub(crate) const fn for_tests() -> Self {
        Self { _private: () }
    }
}

enum Request {
    Flush(Reply),
    Close(Reply),
}

#[derive(Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<Request>,
    seal_hook: Arc<OnceLock<SealHook>>,
    /// Test-only count of submitted flush requests. Unlike completed cycles,
    /// this advances before the worker can block acquiring the flush barrier.
    #[cfg(test)]
    requested_flushes: Arc<std::sync::atomic::AtomicU64>,
    /// Test-only count of successful coordinator flush cycles.
    #[cfg(test)]
    completed_flushes: Arc<std::sync::atomic::AtomicU64>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<Db>) -> Self {
        let seal_hook: Arc<OnceLock<SealHook>> = Arc::new(OnceLock::new());
        let hook = Arc::clone(&seal_hook);
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();
        #[cfg(test)]
        let requested_flushes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let completed_flushes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let flush_counter = Arc::clone(&completed_flushes);

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

                // Drain requests covered by this flush before releasing the barrier.
                // A queued close keeps the barrier through db.close().
                while closer.is_none() {
                    match receiver.try_recv() {
                        Ok(Request::Flush(sender)) => pending_senders.push(sender),
                        Ok(Request::Close(sender)) => closer = Some(sender),
                        Err(_) => break,
                    }
                }

                let close_result = if closer.is_some() && result.is_ok() {
                    db.mark_closing();
                    db.close().await.map_err(|_| FsError::IoError)
                } else {
                    result
                };
                #[cfg(test)]
                if result.is_ok() {
                    flush_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
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

        Self {
            sender,
            seal_hook,
            #[cfg(test)]
            requested_flushes,
            #[cfg(test)]
            completed_flushes,
        }
    }

    /// Install the pre-flush seal hook (first call wins). Set once at bring-up,
    /// after the data plane is constructed.
    pub fn set_sealer(&self, hook: SealHook) {
        let _ = self.seal_hook.set(hook);
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        #[cfg(test)]
        self.requested_flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.sender
            .send(Request::Flush(tx))
            .map_err(|_| FsError::ShuttingDown)?;

        rx.await.map_err(|_| FsError::ShuttingDown)?
    }

    /// Flush and return proof suitable for discharging an HA Solo-base barrier.
    pub(crate) async fn flush_with_receipt(&self) -> Result<FlushReceipt, FsError> {
        self.flush().await?;
        Ok(FlushReceipt { _private: () })
    }

    #[cfg(test)]
    pub(crate) fn requested_flush_count(&self) -> u64 {
        self.requested_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn completed_flush_count(&self) -> u64 {
        self.completed_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::{Notify, oneshot};

    async fn coordinator_with_blocked_seal(
        entered: Arc<Notify>,
        release: Arc<Notify>,
        seal_calls: Arc<AtomicU64>,
    ) -> FlushCoordinator {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let raw = Arc::new(
            slatedb::DbBuilder::new(Path::from("flush-coordinator-test"), store)
                .build()
                .await
                .unwrap(),
        );
        let coordinator = FlushCoordinator::new(Arc::new(Db::new(raw, None)));
        coordinator.set_sealer(Arc::new(move || {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let seal_calls = Arc::clone(&seal_calls);
            Box::pin(async move {
                seal_calls.fetch_add(1, Ordering::Relaxed);
                entered.notify_one();
                release.notified().await;
                Ok(())
            })
        }));
        coordinator
    }

    #[tokio::test]
    async fn fsync_arriving_during_flush_joins_inflight_cycle() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let seal_calls = Arc::new(AtomicU64::new(0));
        let coordinator = coordinator_with_blocked_seal(
            Arc::clone(&entered),
            Arc::clone(&release),
            Arc::clone(&seal_calls),
        )
        .await;

        let first = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.flush().await }
        });
        entered.notified().await;

        let (second_tx, second_rx) = oneshot::channel();
        coordinator.sender.send(Request::Flush(second_tx)).unwrap();
        release.notify_one();

        first.await.unwrap().unwrap();
        second_rx.await.unwrap().unwrap();
        assert_eq!(seal_calls.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.completed_flush_count(), 1);
    }
}
