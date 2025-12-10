use crate::encryption::{EncryptedDb, EncryptedTransaction};
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::{CHUNK_SIZE, FsError};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

const PARALLEL_CHUNK_OPS: usize = 20;
const ZERO_CHUNK: &[u8] = &[0u8; CHUNK_SIZE];

#[derive(Clone)]
pub struct ChunkStore {
    db: Arc<EncryptedDb>,
}

impl ChunkStore {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        Self { db }
    }

    pub async fn get(&self, id: InodeId, chunk_idx: u64) -> Result<Option<Bytes>, FsError> {
        let key = KeyCodec::chunk_key(id, chunk_idx);
        match self.db.get_bytes(&key).await {
            Ok(result) => Ok(result),
            Err(e) => {
                error!(
                    "Failed to read chunk (inode={}, chunk={}): {}",
                    id, chunk_idx, e
                );
                Err(FsError::IoError)
            }
        }
    }

    fn save(&self, txn: &mut EncryptedTransaction, id: InodeId, chunk_idx: u64, data: Bytes) {
        let key = KeyCodec::chunk_key(id, chunk_idx);
        txn.put_bytes(&key, data);
    }

    pub fn delete(&self, txn: &mut EncryptedTransaction, id: InodeId, chunk_idx: u64) {
        let key = KeyCodec::chunk_key(id, chunk_idx);
        txn.delete_bytes(&key);
    }

    pub fn delete_range(&self, txn: &mut EncryptedTransaction, id: InodeId, start: u64, end: u64) {
        for chunk_idx in start..end {
            self.delete(txn, id, chunk_idx);
        }
    }

    pub async fn read(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let end = offset + length;
        let start_chunk = offset / CHUNK_SIZE as u64;
        let end_chunk = (end - 1) / CHUNK_SIZE as u64;
        let start_offset = (offset % CHUNK_SIZE as u64) as usize;

        let start_key = KeyCodec::chunk_key(id, start_chunk);
        let end_key = KeyCodec::chunk_key(id, end_chunk + 1);

        let mut chunk_map: HashMap<u64, Bytes> = HashMap::new();
        let mut stream = self.db.scan(start_key..end_key).await.map_err(|e| {
            error!("Failed to scan chunks (inode={}): {}", id, e);
            FsError::IoError
        })?;

        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|e| {
                error!("Failed to read chunk during scan (inode={}): {}", id, e);
                FsError::IoError
            })?;
            if let Some(chunk_idx) = KeyCodec::parse_chunk_key(&key) {
                chunk_map.insert(chunk_idx, value);
            }
        }

        let mut result = BytesMut::with_capacity(length as usize);

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_data = chunk_map
                .get(&chunk_idx)
                .map(|b| b.as_ref())
                .unwrap_or(ZERO_CHUNK);

            let chunk_start = if chunk_idx == start_chunk {
                start_offset
            } else {
                0
            };
            let chunk_end = if chunk_idx == end_chunk {
                ((end - 1) % CHUNK_SIZE as u64 + 1) as usize
            } else {
                CHUNK_SIZE
            };
            result.extend_from_slice(&chunk_data[chunk_start..chunk_end]);
        }

        Ok(result.freeze())
    }

    pub async fn write(
        &self,
        txn: &mut EncryptedTransaction,
        id: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), FsError> {
        if data.is_empty() {
            return Ok(());
        }

        let end_offset = offset + data.len() as u64;
        let start_chunk = offset / CHUNK_SIZE as u64;
        let end_chunk = (end_offset - 1) / CHUNK_SIZE as u64;

        let existing_chunks: Result<HashMap<u64, Bytes>, FsError> =
            stream::iter(start_chunk..=end_chunk)
                .map(|chunk_idx| {
                    let chunk_start = chunk_idx * CHUNK_SIZE as u64;
                    let chunk_end = chunk_start + CHUNK_SIZE as u64;
                    let will_overwrite_fully = offset <= chunk_start && end_offset >= chunk_end;

                    let store = self.clone();
                    async move {
                        let data = if will_overwrite_fully {
                            Bytes::from_static(ZERO_CHUNK)
                        } else {
                            store
                                .get(id, chunk_idx)
                                .await?
                                .unwrap_or_else(|| Bytes::from_static(ZERO_CHUNK))
                        };
                        Ok::<(u64, Bytes), FsError>((chunk_idx, data))
                    }
                })
                .buffer_unordered(PARALLEL_CHUNK_OPS)
                .try_collect()
                .await;

        let existing_chunks = existing_chunks?;

        let mut data_offset = 0usize;
        for chunk_idx in start_chunk..=end_chunk {
            let chunk_start = chunk_idx * CHUNK_SIZE as u64;
            let chunk_end = chunk_start + CHUNK_SIZE as u64;

            let write_start = if offset > chunk_start {
                (offset - chunk_start) as usize
            } else {
                0
            };
            let write_end = if end_offset < chunk_end {
                (end_offset - chunk_start) as usize
            } else {
                CHUNK_SIZE
            };

            let write_len = write_end - write_start;
            let mut chunk = BytesMut::from(existing_chunks[&chunk_idx].as_ref());
            chunk[write_start..write_end]
                .copy_from_slice(&data[data_offset..data_offset + write_len]);
            data_offset += write_len;

            if chunk.as_ref() == ZERO_CHUNK {
                self.delete(txn, id, chunk_idx);
            } else {
                self.save(txn, id, chunk_idx, chunk.freeze());
            }
        }

        Ok(())
    }

    pub async fn truncate(
        &self,
        txn: &mut EncryptedTransaction,
        id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Result<(), FsError> {
        if new_size >= old_size {
            return Ok(());
        }

        let old_chunks = old_size.div_ceil(CHUNK_SIZE as u64);
        let new_chunks = new_size.div_ceil(CHUNK_SIZE as u64);

        self.delete_range(txn, id, new_chunks, old_chunks);

        if new_size > 0 {
            let last_chunk_idx = new_chunks - 1;
            let clear_from = (new_size % CHUNK_SIZE as u64) as usize;

            if clear_from > 0 {
                let existing = self.get(id, last_chunk_idx).await?;
                let mut chunk =
                    BytesMut::from(existing.as_ref().map(|b| b.as_ref()).unwrap_or(ZERO_CHUNK));
                chunk[clear_from..].fill(0);

                if chunk.as_ref() == ZERO_CHUNK {
                    self.delete(txn, id, last_chunk_idx);
                } else {
                    self.save(txn, id, last_chunk_idx, chunk.freeze());
                }
            }
        }

        Ok(())
    }

    pub async fn zero_range(
        &self,
        txn: &mut EncryptedTransaction,
        id: InodeId,
        offset: u64,
        length: u64,
        file_size: u64,
    ) {
        if length == 0 {
            return;
        }

        let end_offset = offset + length;
        let start_chunk = offset / CHUNK_SIZE as u64;
        let end_chunk = (end_offset - 1) / CHUNK_SIZE as u64;

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_start = chunk_idx * CHUNK_SIZE as u64;
            let chunk_end = chunk_start + CHUNK_SIZE as u64;

            if chunk_start >= file_size {
                continue;
            }

            if offset <= chunk_start && end_offset >= chunk_end {
                self.delete(txn, id, chunk_idx);
            } else if let Ok(Some(existing_data)) = self.get(id, chunk_idx).await {
                let zero_start = if offset > chunk_start {
                    (offset - chunk_start) as usize
                } else {
                    0
                };
                let zero_end = if end_offset < chunk_end {
                    (end_offset - chunk_start) as usize
                } else {
                    CHUNK_SIZE
                };

                let mut chunk_data = BytesMut::from(existing_data.as_ref());
                chunk_data[zero_start..zero_end].fill(0);

                if chunk_data.as_ref() == ZERO_CHUNK {
                    self.delete(txn, id, chunk_idx);
                } else {
                    self.save(txn, id, chunk_idx, chunk_data.freeze());
                }
            }
        }
    }
}
