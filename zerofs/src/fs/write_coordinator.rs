//! Commit coalescer: batched DB writes through a single worker task.
//!
//! Each `commit(txn)` call sends the transaction through an mpsc channel to one
//! worker task. The worker drains all currently-queued messages, merges their
//! ops into a single `WriteBatch`, attaches a `system_counter_key` write if the
//! inode counter advanced since the last emission, submits one
//! `db.write_with_options`, and replies to every caller in the batch.

use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::key_codec::KeyCodec;
use crate::fs::store::InodeStore;
use crate::task::spawn_named;
use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

type Reply = oneshot::Sender<Result<(), FsError>>;

#[derive(Clone)]
pub struct WriteCoordinator {
    sender: mpsc::UnboundedSender<(Transaction, Reply)>,
}

impl WriteCoordinator {
    pub fn new(db: Arc<Db>, inode_store: InodeStore) -> Self {
        // Capture synchronously: the spawned task starts later, by which point
        // callers may already have bumped `next_id`. If we captured inside the
        // task we'd over-shoot and skip the first emit.
        let initial_counter = inode_store.next_id();
        let (sender, receiver) = mpsc::unbounded_channel();
        spawn_named(
            "commit-worker",
            worker_loop(db, inode_store, receiver, initial_counter),
        );
        Self { sender }
    }

    pub async fn commit(&self, txn: Transaction) -> Result<(), FsError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send((txn, reply_tx))
            .map_err(|_| FsError::IoError)?;
        reply_rx.await.map_err(|_| FsError::IoError)?
    }
}

async fn worker_loop(
    db: Arc<Db>,
    inode_store: InodeStore,
    mut rx: mpsc::UnboundedReceiver<(Transaction, Reply)>,
    initial_counter: u64,
) {
    let mut last_emitted_counter = initial_counter;

    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(msg) = rx.try_recv() {
            batch.push(msg);
        }

        let mut merged = WriteBatch::new();
        let mut replies = Vec::with_capacity(batch.len());
        for (txn, reply) in batch {
            txn.apply_to(&mut merged);
            replies.push(reply);
        }

        // Emit the inode counter only if `next_id` actually advanced since the
        // last emission. See the module doc for why this value is always a safe
        // upper bound on every inode id in this batch.
        let current = inode_store.next_id();
        if current > last_emitted_counter {
            merged.put_bytes(
                KeyCodec::system_counter_key(),
                KeyCodec::encode_counter(current),
            );
            last_emitted_counter = current;
        }

        let result = db
            .write_with_options(
                merged,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError);
        for reply in replies {
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use bytes::Bytes;

    async fn make_fs() -> ZeroFS {
        ZeroFS::new_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn commits_single_transaction() {
        let fs = make_fs().await;
        let mut txn = Transaction::new();
        let key = Bytes::from("single/key");
        txn.put_bytes(&key, Bytes::from_static(b"value"));
        fs.write_coordinator.commit(txn).await.unwrap();
        let v = fs.db.get_bytes(&key).await.unwrap();
        assert_eq!(v.as_deref(), Some(&b"value"[..]));
    }

    #[tokio::test]
    async fn coalesces_concurrent_commits() {
        let fs = make_fs().await;
        let coord = fs.write_coordinator.clone();
        let mut handles = Vec::new();
        for i in 0u64..32 {
            let c = coord.clone();
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(
                    &Bytes::from(format!("coalesce/{i}")),
                    Bytes::from(vec![1u8; 8]),
                );
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for i in 0u64..32 {
            let v = fs
                .db
                .get_bytes(&Bytes::from(format!("coalesce/{i}")))
                .await
                .unwrap();
            assert!(v.is_some());
        }
    }

    #[tokio::test]
    async fn counter_emitted_only_when_advanced() {
        let fs = make_fs().await;
        let counter_key = KeyCodec::system_counter_key();
        let before = fs.db.get_bytes(&counter_key).await.unwrap();

        // A commit that doesn't allocate any inode.
        let mut txn = Transaction::new();
        txn.put_bytes(&Bytes::from("nocounter/key"), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_eq!(
            before, after,
            "counter key should not change without allocate"
        );

        // Now allocate and commit; counter must advance on disk.
        let _id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&Bytes::from("withcounter/key"), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after_allocate = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_ne!(
            after, after_allocate,
            "counter key should advance after allocate"
        );
    }
}
