//! Background cleanup of deferred file-deletion tombstones.

use crate::db::Transaction;
use crate::fs::EXTENT_SIZE;
use crate::fs::errors::FsError;
use crate::fs::metrics::FileSystemStats;
use crate::fs::store::{ExtentStore, TombstoneStore};
use crate::task::{spawn_named, spawn_named_on};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

const MAX_EXTENTS_PER_ROUND: usize = 10_000;
const MAX_TOMBSTONES_PER_ROUND: usize = 10_000;

pub struct TombstoneCleaner {
    tombstone_store: TombstoneStore,
    extent_store: ExtentStore,
    stats: Arc<FileSystemStats>,
}

impl TombstoneCleaner {
    pub fn new(
        tombstone_store: TombstoneStore,
        extent_store: ExtentStore,
        stats: Arc<FileSystemStats>,
    ) -> Self {
        Self {
            tombstone_store,
            extent_store,
            stats,
        }
    }

    /// Spawn the continuous tombstone-cleanup loop.
    pub(crate) fn start(
        self,
        shutdown: CancellationToken,
        runtime: Option<tokio::runtime::Handle>,
    ) -> JoinHandle<()> {
        let fut = async move {
            info!("Starting tombstone cleanup task");
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = self.run() => {
                        if let Err(e) = result {
                            tracing::error!("Tombstone cleanup failed: {:?}", e);
                        }
                    }
                }

                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                }
            }
            info!("Tombstone cleanup task shutting down");
        };

        if let Some(rt) = runtime {
            spawn_named_on("tombstone-cleanup", fut, &rt)
        } else {
            spawn_named("tombstone-cleanup", fut)
        }
    }

    pub async fn run(&self) -> Result<(), FsError> {
        self.stats
            .tombstone_cleanup_runs
            .fetch_add(1, Ordering::Relaxed);

        loop {
            // Empty tombstones have no extent work to share a commit with.
            let mut empty_tombstones = Transaction::new();
            let mut empty_tombstones_removed = 0;
            let mut extents_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut tombstones_scanned = 0;
            let mut more_work = false;

            let iter = self.tombstone_store.list().await?;
            futures::pin_mut!(iter);

            while let Some(result) = futures::StreamExt::next(&mut iter).await {
                let entry = result?;
                if tombstones_scanned == MAX_TOMBSTONES_PER_ROUND {
                    more_work = true;
                    break;
                }
                tombstones_scanned += 1;

                if tombstones_scanned % 100 == 0 {
                    tokio::task::yield_now().await;
                }

                if entry.remaining_size == 0 {
                    self.tombstone_store
                        .remove(&mut empty_tombstones, &entry.key);
                    empty_tombstones_removed += 1;
                    continue;
                }

                let extents_remaining_in_round = MAX_EXTENTS_PER_ROUND - extents_deleted_this_round;
                if extents_remaining_in_round == 0 {
                    more_work = true;
                    break;
                }

                let total_extents = entry.remaining_size.div_ceil(EXTENT_SIZE as u64) as usize;
                let extents_to_delete = total_extents.min(extents_remaining_in_round);
                let start_extent = total_extents - extents_to_delete;

                let is_final_batch = extents_to_delete == total_extents;
                if !is_final_batch {
                    more_work = true;
                }
                let mut txn = Transaction::new();
                if is_final_batch {
                    self.tombstone_store.remove(&mut txn, &entry.key);
                } else {
                    self.tombstone_store.update(
                        &mut txn,
                        &entry.key,
                        start_extent as u64 * EXTENT_SIZE as u64,
                    );
                }

                // Progress, extent deletes, and counter debits commit together
                // under the inode lock, serializing with segment repacks.
                self.extent_store
                    .delete_extents_and_commit(
                        txn,
                        entry.inode_id,
                        start_extent as u64,
                        total_extents as u64,
                    )
                    .await?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::TOMBSTONE_CLEANUP_AFTER_COMMIT);

                extents_deleted_this_round += extents_to_delete;

                if is_final_batch {
                    tombstones_completed_this_round += 1;
                }

                if extents_deleted_this_round % 1000 == 0 {
                    tokio::task::yield_now().await;
                }
            }

            if empty_tombstones_removed > 0 {
                self.extent_store
                    .commit_transaction(empty_tombstones)
                    .await?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::TOMBSTONE_CLEANUP_AFTER_COMMIT);

                tombstones_completed_this_round += empty_tombstones_removed;
            }

            if extents_deleted_this_round > 0 || tombstones_completed_this_round > 0 {
                self.stats
                    .tombstones_processed
                    .fetch_add(tombstones_completed_this_round, Ordering::Relaxed);
                self.stats
                    .tombstone_cleanup_extents_deleted
                    .fetch_add(extents_deleted_this_round as u64, Ordering::Relaxed);

                tracing::debug!(
                    "tombstone cleanup: completed {} tombstones, deleted {} extents",
                    tombstones_completed_this_round,
                    extents_deleted_this_round,
                );
            }

            if !more_work {
                break;
            }

            tokio::task::yield_now().await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::store::tombstone::TombstoneEntry;
    use bytes::Bytes;

    fn cleaner_for(fs: &ZeroFS) -> TombstoneCleaner {
        TombstoneCleaner::new(
            fs.tombstone_store.clone(),
            fs.extent_store.clone(),
            Arc::clone(&fs.stats),
        )
    }

    async fn tombstones(store: &TombstoneStore) -> Vec<TombstoneEntry> {
        let iter = store.list().await.unwrap();
        futures::pin_mut!(iter);
        let mut out = Vec::new();
        while let Some(r) = futures::StreamExt::next(&mut iter).await {
            out.push(r.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn tombstone_cleanup_deletes_extents_and_removes_the_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let cleaner = cleaner_for(&fs);

        let inode_id = 4242u64;
        let size = (EXTENT_SIZE * 3) as u64;

        let mut txn = fs.db.new_transaction().unwrap();
        fs.extent_store
            .write(
                &mut txn,
                inode_id,
                0,
                &Bytes::from(vec![1u8; size as usize]),
                0,
            )
            .await
            .unwrap();
        fs.tombstone_store.add(&mut txn, inode_id, size);
        fs.write_coordinator.commit(txn).await.unwrap();

        for idx in 0..3 {
            assert!(
                fs.extent_store.get(inode_id, idx).await.unwrap().is_some(),
                "extent {idx} should exist before tombstone cleanup"
            );
        }

        cleaner.run().await.unwrap();

        for idx in 0..3 {
            assert!(
                fs.extent_store.get(inode_id, idx).await.unwrap().is_none(),
                "extent {idx} must be reclaimed by tombstone cleanup"
            );
        }
        assert!(
            tombstones(&fs.tombstone_store).await.is_empty(),
            "tombstone must be removed"
        );
        assert_eq!(
            fs.stats
                .tombstone_cleanup_extents_deleted
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(fs.stats.tombstones_processed.load(Ordering::Relaxed), 1);
        assert!(fs.stats.tombstone_cleanup_runs.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn tombstone_cleanup_removes_a_zero_size_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let cleaner = cleaner_for(&fs);

        let mut txn = Transaction::new();
        fs.tombstone_store.add(&mut txn, 9u64, 0);
        fs.write_coordinator.commit(txn).await.unwrap();

        cleaner.run().await.unwrap();

        assert!(tombstones(&fs.tombstone_store).await.is_empty());
        assert_eq!(
            fs.stats
                .tombstone_cleanup_extents_deleted
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(fs.stats.tombstones_processed.load(Ordering::Relaxed), 1);
    }
}
