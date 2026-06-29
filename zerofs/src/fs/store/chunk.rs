use crate::db::{Db, Transaction};
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use crate::fs::{CHUNK_SIZE, FsError};
use bytes::{Bytes, BytesMut};
use foyer::{Cache, CacheBuilder};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

const PARALLEL_CHUNK_OPS: usize = 20;
const ZERO_CHUNK: &[u8] = &[0u8; CHUNK_SIZE];

/// Weighted byte budget for the tail cache. Each entry is one CHUNK_SIZE buffer,
/// so this holds the just-written tail of ~`TAIL_CACHE_BYTES / CHUNK_SIZE` inodes
/// (32 MiB = 1024 tails). A global ceiling, independent of open-file count.
const TAIL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// What a `write` leaves for the tail cache. Applied by the caller only after the
/// transaction commits, so the cache never runs ahead of durable state.
pub enum TailUpdate {
    Set { chunk_idx: u64, data: Bytes },
    Clear,
    Keep,
}

#[derive(Clone)]
pub struct ChunkStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    /// Per-inode copy of the most-recently-written (chunk_idx, full chunk), so a
    /// sequential append splices into it rather than re-reading the chunk it just
    /// wrote. Sharded + LRU via foyer; eviction only ever costs a re-read.
    tail_cache: Cache<InodeId, (u64, Bytes)>,
    /// When true, a partial write to a chunk that already has a base is recorded
    /// as a sub-chunk merge delta instead of rewriting the whole chunk, cutting
    /// the volume of superseded versions compaction must later read. Gated off
    /// when a replication peer can't apply merge ops (see `all_peers_support_merge`).
    deltas_enabled: bool,
}

impl ChunkStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>, deltas_enabled: bool) -> Self {
        let tail_cache = CacheBuilder::new(TAIL_CACHE_BYTES)
            .with_weighter(|_id: &InodeId, (_idx, data): &(u64, Bytes)| data.len())
            .build();
        Self {
            db,
            key_codec,
            tail_cache,
            deltas_enabled,
        }
    }

    /// The cached tail chunk for `id`, if any.
    fn tail_get(&self, id: InodeId) -> Option<(u64, Bytes)> {
        self.tail_cache.get(&id).map(|e| (*e).clone())
    }

    fn tail_set(&self, id: InodeId, chunk_idx: u64, data: Bytes) {
        self.tail_cache.insert(id, (chunk_idx, data));
    }

    fn tail_invalidate(&self, id: InodeId) {
        self.tail_cache.remove(&id);
    }

    /// Apply a `write`'s tail-cache effect. Call only after its commit succeeds.
    pub fn apply_tail_update(&self, id: InodeId, update: TailUpdate) {
        match update {
            TailUpdate::Set { chunk_idx, data } => self.tail_set(id, chunk_idx, data),
            TailUpdate::Clear => self.tail_invalidate(id),
            TailUpdate::Keep => {}
        }
    }

    pub async fn get(&self, id: InodeId, chunk_idx: u64) -> Result<Option<Bytes>, FsError> {
        let key = self.key_codec.chunk_key(id, chunk_idx);
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

    fn save(&self, txn: &mut Transaction, id: InodeId, chunk_idx: u64, data: Bytes) {
        let key = self.key_codec.chunk_key(id, chunk_idx);
        txn.put_bytes(&key, data);
    }

    /// Record a partial write as a sub-chunk merge delta (the merge operator
    /// applies it onto the chunk's base on read/compaction). Only valid when the
    /// chunk already has a base; a new chunk must be established with `save`.
    fn save_delta(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        chunk_idx: u64,
        offset: usize,
        data: Bytes,
    ) {
        let key = self.key_codec.chunk_key(id, chunk_idx);
        txn.merge_bytes(&key, super::chunk_merge::encode_delta(offset, data));
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId, chunk_idx: u64) {
        let key = self.key_codec.chunk_key(id, chunk_idx);
        txn.delete_bytes(&key);
    }

    pub fn delete_range(&self, txn: &mut Transaction, id: InodeId, start: u64, end: u64) {
        self.tail_invalidate(id);
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

        // Fast path: read fits in a single chunk, skip the scan.
        if start_chunk == end_chunk {
            let chunk_end = start_offset + length as usize;
            return Ok(match self.get(id, start_chunk).await? {
                Some(data) => data.slice(start_offset..chunk_end),
                None => Bytes::copy_from_slice(&ZERO_CHUNK[start_offset..chunk_end]),
            });
        }

        let start_key = self.key_codec.chunk_key(id, start_chunk);
        let end_key = self.key_codec.chunk_key(id, end_chunk + 1);

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
            if let Some(chunk_idx) = self.key_codec.parse_chunk_key(&key) {
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
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        old_size: u64,
    ) -> Result<TailUpdate, FsError> {
        if data.is_empty() {
            return Ok(TailUpdate::Keep);
        }

        let end_offset = offset + data.len() as u64;
        let start_chunk = offset / CHUNK_SIZE as u64;
        let end_chunk = (end_offset - 1) / CHUNK_SIZE as u64;

        // The tail chunk of the previous write, splice-able without a re-read.
        let cached = self.tail_get(id);

        // For each touched chunk: its current bytes for splicing, plus whether a
        // real base exists in the store. `had_base` is what makes a partial write
        // eligible to be recorded as a delta: a new/hole chunk has no base to
        // merge onto, so it must be written whole.
        let existing_chunks: Result<HashMap<u64, (Bytes, bool)>, FsError> =
            stream::iter(start_chunk..=end_chunk)
                .map(|chunk_idx| {
                    let chunk_start = chunk_idx * CHUNK_SIZE as u64;
                    let chunk_end = chunk_start + CHUNK_SIZE as u64;
                    let will_overwrite_fully = offset <= chunk_start && end_offset >= chunk_end;
                    // Chunks that begin at or beyond the old file size cannot have any
                    // on-disk data (truncate deletes chunks past EOF), so the partial
                    // read-modify-write read is a guaranteed miss. Skip it.
                    let beyond_eof = chunk_start >= old_size;

                    let store = self.clone();
                    let cached = cached.clone();
                    async move {
                        let (data, had_base) = if will_overwrite_fully || beyond_eof {
                            // A full overwrite replaces the chunk and a beyond-EOF
                            // chunk is new: neither has a base to delta against.
                            (Bytes::from_static(ZERO_CHUNK), false)
                        } else if let Some((_, bytes)) = cached.filter(|(ci, _)| *ci == chunk_idx) {
                            // We wrote this chunk last; splice into our copy of it
                            // rather than re-reading it back from the store.
                            (bytes, true)
                        } else {
                            match store.get(id, chunk_idx).await? {
                                Some(bytes) => (bytes, true),
                                // A hole within the file: no base on disk.
                                None => (Bytes::from_static(ZERO_CHUNK), false),
                            }
                        };
                        Ok::<(u64, (Bytes, bool)), FsError>((chunk_idx, (data, had_base)))
                    }
                })
                .buffer_unordered(PARALLEL_CHUNK_OPS)
                .try_collect()
                .await;

        let existing_chunks = existing_chunks?;

        // Cache the new tail only when this write reaches EOF (so end_chunk really
        // is the file's tail) and that tail is partial; a future append re-reads
        // exactly that chunk. Any other outcome drops a now-stale entry.
        let cache_tail = end_offset >= old_size && !end_offset.is_multiple_of(CHUNK_SIZE as u64);

        let mut data_offset = 0usize;
        let mut tail: Option<Bytes> = None;
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
            let (base, had_base) = &existing_chunks[&chunk_idx];
            let is_full = write_start == 0 && write_end == CHUNK_SIZE;
            let new_bytes = data.slice(data_offset..data_offset + write_len);
            data_offset += write_len;

            if !is_full && self.deltas_enabled && *had_base {
                // Partial write to an existing chunk: record only the changed
                // range as a merge delta. The base stays put and the merge
                // operator reassembles the chunk on read/compaction, so we avoid
                // rewriting (and later re-reading) a whole superseded chunk.
                self.save_delta(txn, id, chunk_idx, write_start, new_bytes.clone());
                // Keep the tail cache materialized so a following append still
                // splices without a re-read.
                if chunk_idx == end_chunk && cache_tail {
                    let mut spliced = BytesMut::from(base.as_ref());
                    spliced[write_start..write_end].copy_from_slice(&new_bytes);
                    tail = Some(spliced.freeze());
                }
                continue;
            }

            // Full overwrite, a new/hole chunk, or deltas disabled: write the
            // whole chunk, establishing or replacing its base.
            let chunk: Bytes = if is_full {
                new_bytes
            } else {
                let mut chunk = BytesMut::from(base.as_ref());
                chunk[write_start..write_end].copy_from_slice(&new_bytes);
                chunk.freeze()
            };

            if chunk.as_ref() == ZERO_CHUNK {
                self.delete(txn, id, chunk_idx);
            } else {
                if chunk_idx == end_chunk && cache_tail {
                    tail = Some(chunk.clone());
                }
                self.save(txn, id, chunk_idx, chunk);
            }
        }

        Ok(match tail {
            Some(data) => TailUpdate::Set {
                chunk_idx: end_chunk,
                data,
            },
            None => TailUpdate::Clear,
        })
    }

    pub async fn truncate(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Result<(), FsError> {
        if new_size >= old_size {
            return Ok(());
        }
        self.tail_invalidate(id);

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
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        length: u64,
        file_size: u64,
    ) {
        if length == 0 {
            return;
        }
        self.tail_invalidate(id);

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
