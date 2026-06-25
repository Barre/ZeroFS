use crate::db::Db;
use crate::fs::CHUNK_SIZE;
use crate::fs::errors::FsError;
use crate::fs::metrics::FileSystemStats;
use crate::fs::store::{ChunkStore, TombstoneStore};
use crate::task::{spawn_named, spawn_named_on};
use bytes::Bytes;
use slatedb::config::WriteOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

const MAX_CHUNKS_PER_ROUND: usize = 10_000;
const MAX_TOMBSTONES_PER_ROUND: usize = 10_000;

pub struct GarbageCollector {
    db: Arc<Db>,
    tombstone_store: TombstoneStore,
    chunk_store: ChunkStore,
    stats: Arc<FileSystemStats>,
}

impl GarbageCollector {
    pub fn new(
        db: Arc<Db>,
        tombstone_store: TombstoneStore,
        chunk_store: ChunkStore,
        stats: Arc<FileSystemStats>,
    ) -> Self {
        Self {
            db,
            tombstone_store,
            chunk_store,
            stats,
        }
    }

    pub fn start(
        self: Arc<Self>,
        shutdown: CancellationToken,
        runtime: Option<tokio::runtime::Handle>,
    ) -> JoinHandle<()> {
        let fut = async move {
            info!("Starting garbage collection task (runs continuously)");
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("GC task shutting down");
                        break;
                    }
                    result = self.run() => {
                        if let Err(e) = result {
                            tracing::error!("Garbage collection failed: {:?}", e);
                        }
                    }
                }

                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("GC task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {}
                }
            }
        };

        if let Some(rt) = runtime {
            spawn_named_on("gc", fut, &rt)
        } else {
            spawn_named("gc", fut)
        }
    }

    pub async fn run(&self) -> Result<(), FsError> {
        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            let mut tombstones_to_update: Vec<(Bytes, u64, usize, bool)> = Vec::new();
            let mut chunks_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut tombstones_processed_this_round = 0;
            let mut found_incomplete_tombstones = false;

            let iter = self.tombstone_store.list().await?;
            futures::pin_mut!(iter);

            let mut chunks_remaining_in_round = MAX_CHUNKS_PER_ROUND;

            while let Some(result) = futures::StreamExt::next(&mut iter).await {
                let entry = result?;
                tombstones_processed_this_round += 1;

                if tombstones_processed_this_round % 100 == 0 {
                    tokio::task::yield_now().await;
                }

                if tombstones_processed_this_round >= MAX_TOMBSTONES_PER_ROUND {
                    found_incomplete_tombstones = true;
                    break;
                }

                if entry.remaining_size == 0 {
                    tombstones_to_update.push((entry.key, 0, 0, true));
                    continue;
                }

                if chunks_remaining_in_round == 0 {
                    found_incomplete_tombstones = true;
                    break;
                }

                let total_chunks = entry.remaining_size.div_ceil(CHUNK_SIZE as u64) as usize;
                let chunks_to_delete = total_chunks.min(chunks_remaining_in_round);
                let start_chunk = total_chunks.saturating_sub(chunks_to_delete);

                let is_final_batch = chunks_to_delete == total_chunks;
                if !is_final_batch {
                    found_incomplete_tombstones = true;
                }
                tombstones_to_update.push((
                    entry.key,
                    entry.remaining_size,
                    start_chunk,
                    is_final_batch,
                ));

                let mut txn = self.db.new_transaction()?;
                self.chunk_store.delete_range(
                    &mut txn,
                    entry.inode_id,
                    start_chunk as u64,
                    total_chunks as u64,
                );

                if chunks_to_delete > 0 {
                    self.db
                        .write_with_options(
                            txn.into_inner(),
                            &WriteOptions {
                                await_durable: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|_| FsError::IoError)?;

                    #[cfg(feature = "failpoints")]
                    fail_point!(fp::GC_AFTER_CHUNK_DELETE);

                    chunks_deleted_this_round += chunks_to_delete;
                    chunks_remaining_in_round -= chunks_to_delete;

                    if is_final_batch {
                        tombstones_completed_this_round += 1;
                    }

                    if chunks_deleted_this_round % 1000 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }

            if !tombstones_to_update.is_empty() {
                let mut txn = self.db.new_transaction()?;

                for (key, old_size, start_chunk, delete_tombstone) in tombstones_to_update {
                    if delete_tombstone {
                        self.tombstone_store.remove(&mut txn, &key);
                    } else {
                        let remaining_chunks = start_chunk;
                        let remaining_size = (remaining_chunks as u64) * (CHUNK_SIZE as u64);
                        let actual_remaining = remaining_size.min(old_size);
                        self.tombstone_store
                            .update(&mut txn, &key, actual_remaining);
                    }
                }

                self.db
                    .write_with_options(
                        txn.into_inner(),
                        &WriteOptions {
                            await_durable: false,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| FsError::IoError)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::GC_AFTER_TOMBSTONE_UPDATE);

                self.stats
                    .tombstones_processed
                    .fetch_add(tombstones_completed_this_round, Ordering::Relaxed);
            }

            if chunks_deleted_this_round > 0 || tombstones_completed_this_round > 0 {
                self.stats
                    .gc_chunks_deleted
                    .fetch_add(chunks_deleted_this_round as u64, Ordering::Relaxed);

                tracing::debug!(
                    "GC: processed {} tombstones, deleted {} chunks",
                    tombstones_completed_this_round,
                    chunks_deleted_this_round,
                );
            }

            if !found_incomplete_tombstones {
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

    fn no_wait() -> WriteOptions {
        WriteOptions {
            await_durable: false,
            ..Default::default()
        }
    }

    async fn gc_for(fs: &ZeroFS) -> GarbageCollector {
        GarbageCollector::new(
            Arc::clone(&fs.db),
            fs.tombstone_store.clone(),
            fs.chunk_store.clone(),
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

    // The core sweep: a tombstone for a deleted file's chunks is reclaimed — every
    // chunk deleted, the tombstone removed, and the stats counters advanced.
    #[tokio::test]
    async fn gc_deletes_chunks_and_removes_the_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        let inode_id = 4242u64;
        let size = (CHUNK_SIZE * 3) as u64;

        let mut txn = fs.db.new_transaction().unwrap();
        fs.chunk_store
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
        fs.db
            .write_with_options(txn.into_inner(), &no_wait())
            .await
            .unwrap();

        for idx in 0..3 {
            assert!(
                fs.chunk_store.get(inode_id, idx).await.unwrap().is_some(),
                "chunk {idx} should exist before GC"
            );
        }

        gc.run().await.unwrap();

        for idx in 0..3 {
            assert!(
                fs.chunk_store.get(inode_id, idx).await.unwrap().is_none(),
                "chunk {idx} must be reclaimed by GC"
            );
        }
        assert!(
            tombstones(&fs.tombstone_store).await.is_empty(),
            "tombstone must be removed"
        );
        assert_eq!(fs.stats.gc_chunks_deleted.load(Ordering::Relaxed), 3);
        assert_eq!(fs.stats.tombstones_processed.load(Ordering::Relaxed), 1);
        assert!(fs.stats.gc_runs.load(Ordering::Relaxed) >= 1);
    }

    // A zero-remaining-size tombstone (its chunks already gone) is just removed.
    #[tokio::test]
    async fn gc_removes_a_zero_size_tombstone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        let mut txn = fs.db.new_transaction().unwrap();
        fs.tombstone_store.add(&mut txn, 9u64, 0);
        fs.db
            .write_with_options(txn.into_inner(), &no_wait())
            .await
            .unwrap();

        gc.run().await.unwrap();

        assert!(tombstones(&fs.tombstone_store).await.is_empty());
        assert_eq!(fs.stats.gc_chunks_deleted.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn gc_on_an_empty_store_is_a_noop() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let gc = gc_for(&fs).await;

        gc.run().await.unwrap();

        assert_eq!(fs.stats.gc_chunks_deleted.load(Ordering::Relaxed), 0);
        assert!(fs.stats.gc_runs.load(Ordering::Relaxed) >= 1);
    }
}
