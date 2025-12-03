use crate::encryption::EncryptedDb;
use crate::fs::CHUNK_SIZE;
use crate::fs::errors::FsError;
use crate::fs::metrics::FileSystemStats;
use crate::fs::store::{ChunkStore, TombstoneStore};
use bytes::Bytes;
use slatedb::config::WriteOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

const MAX_CHUNKS_PER_ROUND: usize = 10_000;

pub struct GarbageCollector {
    db: Arc<EncryptedDb>,
    tombstone_store: TombstoneStore,
    chunk_store: ChunkStore,
    stats: Arc<FileSystemStats>,
}

impl GarbageCollector {
    pub fn new(
        db: Arc<EncryptedDb>,
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

    pub fn start(self: Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
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
        })
    }

    pub async fn run(&self) -> Result<(), FsError> {
        self.stats.gc_runs.fetch_add(1, Ordering::Relaxed);

        loop {
            let mut tombstones_to_update: Vec<(Bytes, u64, usize, bool)> = Vec::new();
            let mut chunks_deleted_this_round = 0;
            let mut tombstones_completed_this_round = 0;
            let mut found_incomplete_tombstones = false;

            let iter = self.tombstone_store.list().await?;
            futures::pin_mut!(iter);

            let mut chunks_remaining_in_round = MAX_CHUNKS_PER_ROUND;

            while let Some(result) = futures::StreamExt::next(&mut iter).await {
                if chunks_remaining_in_round == 0 {
                    found_incomplete_tombstones = true;
                    break;
                }

                let entry = result?;

                if entry.remaining_size == 0 {
                    tombstones_to_update.push((entry.key, 0, 0, true));
                    continue;
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
                            txn,
                            &WriteOptions {
                                await_durable: false,
                            },
                        )
                        .await
                        .map_err(|_| FsError::IoError)?;

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
                        txn,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| FsError::IoError)?;

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
