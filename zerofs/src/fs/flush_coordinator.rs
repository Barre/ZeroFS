use crate::db::Db;
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::task::spawn_named;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
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

struct Shared {
    seal_hook: OnceLock<SealHook>,
    /// Last committed SlateDB sequence touching each currently-dirty inode.
    /// Entries are pruned after a successful global flush.
    dirty_inodes: DashMap<InodeId, u64>,
    /// Highest inode generation covered by a successful seal + metadata flush.
    durable_seq: AtomicU64,
    /// Test-only count of submitted flush requests. Unlike completed cycles,
    /// this advances before the worker can block acquiring the flush barrier.
    #[cfg(test)]
    requested_flushes: AtomicU64,
    /// Test-only count of successful coordinator flush cycles.
    #[cfg(test)]
    completed_flushes: AtomicU64,
}

#[derive(Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<Request>,
    shared: Arc<Shared>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<Db>) -> Self {
        let shared = Arc::new(Shared {
            seal_hook: OnceLock::new(),
            dirty_inodes: DashMap::new(),
            durable_seq: AtomicU64::new(db.durable_seq()),
            #[cfg(test)]
            requested_flushes: AtomicU64::new(0),
            #[cfg(test)]
            completed_flushes: AtomicU64::new(0),
        });
        let worker_shared = Arc::clone(&shared);
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
                let result = match worker_shared.seal_hook.get() {
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

                let prune_through = if result.is_ok() {
                    // The write barrier proves every generation committed before
                    // this cycle was included. SlateDB's durable sequence captures
                    // all writes swept up by the global flush.
                    let through = db.durable_seq();
                    worker_shared
                        .durable_seq
                        .fetch_max(through, Ordering::Release);
                    Some(through)
                } else {
                    None
                };

                let close_result = if closer.is_some() && result.is_ok() {
                    db.mark_closing();
                    db.close().await.map_err(|_| FsError::IoError)
                } else {
                    result
                };
                #[cfg(test)]
                if result.is_ok() {
                    worker_shared
                        .completed_flushes
                        .fetch_add(1, Ordering::Relaxed);
                }
                drop(barrier);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::FLUSH_AFTER_COMPLETE);

                for sender in pending_senders.drain(..) {
                    let _ = sender.send(result);
                }
                // Keep the inode map proportional to mutations since the latest
                // flush without extending the exclusive write barrier. Racing
                // newer generations are greater than `through` and survive.
                if let Some(through) = prune_through {
                    worker_shared
                        .dirty_inodes
                        .retain(|_, generation| *generation > through);
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

        Self { sender, shared }
    }

    /// Install the pre-flush seal hook (first call wins). Set once at bring-up,
    /// after the data plane is constructed.
    pub fn set_sealer(&self, hook: SealHook) {
        let _ = self.shared.seal_hook.set(hook);
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        #[cfg(test)]
        self.shared
            .requested_flushes
            .fetch_add(1, Ordering::Relaxed);

        self.sender
            .send(Request::Flush(tx))
            .map_err(|_| FsError::ShuttingDown)?;

        rx.await.map_err(|_| FsError::ShuttingDown)?
    }

    /// Record the SlateDB generation of a successfully committed batch for all
    /// inodes it changed. The write coordinator calls this before acknowledging
    /// the batch, so every later per-inode fsync observes its target.
    pub(crate) fn mark_dirty_inodes(
        &self,
        inode_ids: impl IntoIterator<Item = InodeId>,
        generation: u64,
    ) {
        for inode_id in inode_ids {
            self.shared
                .dirty_inodes
                .entry(inode_id)
                .and_modify(|current| *current = (*current).max(generation))
                .or_insert(generation);
        }
    }

    /// Flush only when this inode has a committed generation that is not
    /// already covered by an earlier global flush.
    pub async fn flush_inode(&self, inode_id: InodeId) -> Result<(), FsError> {
        let Some(target) = self
            .shared
            .dirty_inodes
            .get(&inode_id)
            .map(|generation| *generation)
        else {
            return Ok(());
        };
        let durable = self.shared.durable_seq.load(Ordering::Acquire);
        if target <= durable {
            // A commit can publish its inode generation just after the flush
            // worker's bulk pruning pass. Remove that stale entry atomically,
            // but never erase a newer concurrent mutation.
            if let Entry::Occupied(entry) = self.shared.dirty_inodes.entry(inode_id)
                && *entry.get() <= durable
            {
                entry.remove();
            }
            return Ok(());
        }
        self.flush().await
    }

    /// Flush and return proof suitable for discharging an HA Solo-base barrier.
    pub(crate) async fn flush_with_receipt(&self) -> Result<FlushReceipt, FsError> {
        self.flush().await?;
        Ok(FlushReceipt { _private: () })
    }

    #[cfg(test)]
    pub(crate) fn requested_flush_count(&self) -> u64 {
        self.shared.requested_flushes.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn completed_flush_count(&self) -> u64 {
        self.shared.completed_flushes.load(Ordering::Relaxed)
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
    use bytes::Bytes;
    use slatedb::WriteBatch;
    use slatedb::config::WriteOptions;
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

    async fn coordinator_with_counting_seal(
        seal_calls: Arc<AtomicU64>,
    ) -> (FlushCoordinator, Arc<Db>) {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let raw = Arc::new(
            slatedb::DbBuilder::new(Path::from("flush-generation-test"), store)
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(raw, None));
        let coordinator = FlushCoordinator::new(Arc::clone(&db));
        coordinator.set_sealer(Arc::new(move || {
            let seal_calls = Arc::clone(&seal_calls);
            Box::pin(async move {
                seal_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }));
        (coordinator, db)
    }

    async fn mark_committed(
        coordinator: &FlushCoordinator,
        db: &Db,
        inode_id: InodeId,
        key: &'static [u8],
    ) {
        let mut batch = WriteBatch::new();
        batch.put_bytes(Bytes::from_static(key), Bytes::from_static(b"value"));
        let generation = db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        coordinator.mark_dirty_inodes([inode_id], generation);
    }

    #[tokio::test]
    async fn inode_flush_skips_clean_and_already_durable_generations() {
        let seal_calls = Arc::new(AtomicU64::new(0));
        let (coordinator, db) = coordinator_with_counting_seal(Arc::clone(&seal_calls)).await;

        coordinator.flush_inode(10).await.unwrap();
        assert_eq!(coordinator.requested_flush_count(), 0);

        mark_committed(&coordinator, &db, 10, b"first").await;
        coordinator.flush_inode(10).await.unwrap();
        assert_eq!(seal_calls.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.requested_flush_count(), 1);

        coordinator.flush_inode(10).await.unwrap();
        assert_eq!(seal_calls.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.requested_flush_count(), 1);

        mark_committed(&coordinator, &db, 20, b"second").await;
        coordinator.flush_inode(10).await.unwrap();
        assert_eq!(coordinator.requested_flush_count(), 1);
        coordinator.flush_inode(20).await.unwrap();
        assert_eq!(seal_calls.load(Ordering::Relaxed), 2);

        mark_committed(&coordinator, &db, 30, b"third").await;
        coordinator.flush().await.unwrap();
        assert_eq!(seal_calls.load(Ordering::Relaxed), 3);
        let requests_after_global_flush = coordinator.requested_flush_count();
        coordinator.flush_inode(30).await.unwrap();
        assert_eq!(
            coordinator.requested_flush_count(),
            requests_after_global_flush
        );
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
