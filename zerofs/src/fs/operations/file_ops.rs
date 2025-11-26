use super::common::validate_filename;
use crate::fs::cache::{CacheKey, CacheValue};
use crate::fs::errors::FsError;
use crate::fs::inode::{FileInode, Inode};
use crate::fs::key_codec::KeyCodec;
use crate::fs::permissions::{AccessMode, Credentials, check_access, validate_mode};
use crate::fs::types::{
    AuthContext, FileAttributes, InodeId, InodeWithId, SetAttributes, SetGid, SetMode, SetUid,
};
use crate::fs::{CHUNK_SIZE, ZeroFS, get_current_time};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, error};

const READ_CHUNK_BUFFER_SIZE: usize = 1024;

impl ZeroFS {
    pub async fn process_write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        let mut seq_guard = self.allocate_sequence();
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let creds = Credentials::from_auth_context(auth);

        // Optimistically load inode and check parent permissions before lock
        let _ = self.load_inode(id).await?;
        self.check_parent_execute_permissions(id, &creds).await?;

        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.load_inode(id).await?;

        // NFS RFC 1813 section 4.4: Allow owners to write to their files regardless of permission bits
        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            _ => {}
        }

        match &mut inode {
            Inode::File(file) => {
                let old_size = file.size;
                let end_offset = offset + data.len() as u64;
                let new_size = std::cmp::max(file.size, end_offset);

                if new_size > old_size {
                    let size_increase = new_size - old_size;
                    let (used_bytes, _) = self.global_stats.get_totals();

                    if used_bytes.saturating_add(size_increase) > self.max_bytes {
                        debug!(
                            "Write would exceed quota: used={}, increase={}, max={}",
                            used_bytes, size_increase, self.max_bytes
                        );
                        return Err(FsError::NoSpace);
                    }
                }

                let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                let end_chunk = (end_offset.saturating_sub(1) / CHUNK_SIZE as u64) as usize;

                let mut batch = self.new_write_batch()?;

                let chunk_processing_start = std::time::Instant::now();

                // Optimization: skip loading chunks that will be completely overwritten
                let existing_chunks: HashMap<usize, Bytes> = stream::iter(start_chunk..=end_chunk)
                    .map(|chunk_idx| {
                        let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
                        let chunk_end = chunk_start + CHUNK_SIZE as u64;

                        let will_overwrite_fully = offset <= chunk_start && end_offset >= chunk_end;

                        let chunk_key = KeyCodec::chunk_key(id, chunk_idx as u64);
                        let db = self.db.clone();
                        async move {
                            let data = if will_overwrite_fully {
                                Bytes::from(vec![0u8; CHUNK_SIZE])
                            } else {
                                db.get_bytes(&chunk_key)
                                    .await
                                    .ok()
                                    .flatten()
                                    .unwrap_or_else(|| Bytes::from(vec![0u8; CHUNK_SIZE]))
                            };
                            (chunk_idx, data)
                        }
                    })
                    .buffer_unordered(READ_CHUNK_BUFFER_SIZE)
                    .collect()
                    .await;

                let mut data_offset = 0;
                for chunk_idx in start_chunk..=end_chunk {
                    let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
                    let chunk_end = chunk_start + CHUNK_SIZE as u64;

                    let chunk_key = KeyCodec::chunk_key(id, chunk_idx as u64);

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

                    let data_len = write_end - write_start;

                    let mut chunk = BytesMut::from(existing_chunks[&chunk_idx].as_ref());
                    chunk[write_start..write_end]
                        .copy_from_slice(&data[data_offset..data_offset + data_len]);
                    data_offset += data_len;

                    batch.put_bytes(&chunk_key, chunk.freeze());
                }

                debug!(
                    "Chunk processing took: {:?}",
                    chunk_processing_start.elapsed()
                );

                file.size = new_size;
                let (now_sec, now_nsec) = get_current_time();
                file.mtime = now_sec;
                file.mtime_nsec = now_nsec;

                // POSIX: Clear SUID/SGID bits on write by non-owner
                if creds.uid != file.uid && creds.uid != 0 {
                    file.mode &= !0o6000;
                }

                let inode_key = KeyCodec::inode_key(id);
                let inode_data = bincode::serialize(&inode)?;
                batch.put_bytes(&inode_key, Bytes::from(inode_data));

                let stats_update = if let Some(update) = self
                    .global_stats
                    .prepare_size_change(id, old_size, new_size)
                    .await
                {
                    self.global_stats.add_to_batch(&update, &mut batch)?;
                    Some(update)
                } else {
                    None
                };

                let db_write_start = std::time::Instant::now();
                self.commit_batch_ordered(batch, &mut seq_guard).await?;
                debug!("DB write took: {:?}", db_write_start.elapsed());

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                self.cache.insert(
                    CacheKey::Metadata(id),
                    CacheValue::Metadata(Arc::new(inode.clone())),
                );

                let elapsed = start_time.elapsed();
                debug!(
                    "Write processed successfully for inode {}, new size: {}, took: {:?}",
                    id, new_size, elapsed
                );

                self.stats
                    .bytes_written
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok(InodeWithId { inode: &inode, id }.into())
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    pub async fn process_create(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        let mut seq_guard = self.allocate_sequence();
        validate_filename(name)?;

        debug!(
            "process_create: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        // Optimistic existence check without holding lock
        let entry_key = KeyCodec::dir_entry_key(dirid, name);
        if self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .is_some()
        {
            return Err(FsError::Exists);
        }

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                // Re-check existence inside lock (should hit cache and be fast)
                if self
                    .db
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| FsError::IoError)?
                    .is_some()
                {
                    return Err(FsError::Exists);
                }

                let file_id = self.allocate_inode().await?;
                debug!(
                    "Allocated inode {} for file {}",
                    file_id,
                    String::from_utf8_lossy(name)
                );

                let (now_sec, now_nsec) = get_current_time();

                let final_mode = match &attr.mode {
                    SetMode::Set(m) => validate_mode(*m),
                    SetMode::NoChange => 0o666,
                };

                let file_inode = FileInode {
                    size: 0,
                    mtime: now_sec,
                    mtime_nsec: now_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: now_sec,
                    atime_nsec: now_nsec,
                    mode: final_mode,
                    uid: match &attr.uid {
                        SetUid::Set(u) => *u,
                        SetUid::NoChange => creds.uid,
                    },
                    gid: match &attr.gid {
                        SetGid::Set(g) => *g,
                        SetGid::NoChange => creds.gid,
                    },
                    parent: Some(dirid),
                    nlink: 1,
                };

                let mut batch = self.new_write_batch()?;

                let file_inode_key = KeyCodec::inode_key(file_id);
                let file_inode_data = bincode::serialize(&Inode::File(file_inode.clone()))?;
                batch.put_bytes(&file_inode_key, Bytes::from(file_inode_data));

                batch.put_bytes(&entry_key, KeyCodec::encode_dir_entry(file_id));

                let scan_key = KeyCodec::dir_scan_key(dirid, file_id, name);
                batch.put_bytes(&scan_key, KeyCodec::encode_dir_entry(file_id));

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_key = KeyCodec::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode)?;
                batch.put_bytes(&dir_key, Bytes::from(dir_data));

                let stats_update = self.global_stats.prepare_inode_create(file_id).await;
                self.global_stats.add_to_batch(&stats_update, &mut batch)?;

                self.commit_batch_ordered(batch, &mut seq_guard)
                    .await
                    .inspect_err(|e| {
                        error!("Failed to write batch: {:?}", e);
                    })?;

                self.global_stats.commit_update(&stats_update);

                self.cache.remove(CacheKey::Metadata(dirid));

                self.stats.files_created.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                let inode = Inode::File(file_inode);
                let file_attrs = InodeWithId {
                    inode: &inode,
                    id: file_id,
                }
                .into();
                Ok((file_id, file_attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn process_create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        filename: &[u8],
    ) -> Result<InodeId, FsError> {
        let (id, _) = self
            .process_create(
                &Credentials::from_auth_context(auth),
                dirid,
                filename,
                &SetAttributes::default(),
            )
            .await?;
        Ok(id)
    }

    pub async fn process_read_file(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        debug!(
            "process_read_file: id={}, offset={}, count={}",
            id, offset, count
        );

        let inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    return Ok((Bytes::new(), true));
                }

                let end = std::cmp::min(offset + count as u64, file.size);
                let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                let end_chunk = (end.saturating_sub(1) / CHUNK_SIZE as u64) as usize;
                let start_offset = (offset % CHUNK_SIZE as u64) as usize;

                let chunks: Vec<Bytes> = stream::iter(start_chunk..=end_chunk)
                    .map(|chunk_idx| {
                        let db = self.db.clone();
                        let key = KeyCodec::chunk_key(id, chunk_idx as u64);
                        async move {
                            db.get_bytes(&key)
                                .await
                                .ok()
                                .flatten()
                                .unwrap_or_else(|| Bytes::from(vec![0u8; CHUNK_SIZE]))
                        }
                    })
                    .buffered(READ_CHUNK_BUFFER_SIZE)
                    .collect()
                    .await;

                let mut result = BytesMut::with_capacity((end - offset) as usize);

                for (chunk_idx, chunk_data) in (start_chunk..=end_chunk).zip(chunks) {
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

                let result_bytes = result.freeze();
                let eof = end >= file.size;

                self.stats
                    .bytes_read
                    .fetch_add(result_bytes.len() as u64, Ordering::Relaxed);
                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok((result_bytes, eof))
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    pub async fn trim(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Result<(), FsError> {
        let mut seq_guard = self.allocate_sequence();
        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire_write(id).await;
        let inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(FsError::IsDirectory),
        }

        let file = match &inode {
            Inode::File(f) => f,
            _ => return Err(FsError::IsDirectory),
        };

        let end_offset = offset + length;
        let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
        let end_chunk = ((end_offset.saturating_sub(1)) / CHUNK_SIZE as u64) as usize;

        let mut batch = self.new_write_batch()?;

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
            let chunk_end = chunk_start + CHUNK_SIZE as u64;

            if chunk_start >= file.size {
                continue;
            }

            let chunk_key = KeyCodec::chunk_key(id, chunk_idx as u64);

            if offset <= chunk_start && end_offset >= chunk_end {
                batch.delete_bytes(&chunk_key);
            } else {
                // Partial trim - load chunk, clear trimmed region, save or delete
                let trim_start = if offset > chunk_start {
                    (offset - chunk_start) as usize
                } else {
                    0
                };

                let trim_end = if end_offset < chunk_end {
                    (end_offset - chunk_start) as usize
                } else {
                    CHUNK_SIZE
                };

                if let Some(existing_data) = self
                    .db
                    .get_bytes(&chunk_key)
                    .await
                    .map_err(|_| FsError::IoError)?
                {
                    let mut chunk_data = BytesMut::from(existing_data.as_ref());
                    chunk_data[trim_start..trim_end].fill(0);

                    if chunk_data.iter().all(|&b| b == 0) {
                        batch.delete_bytes(&chunk_key);
                    } else {
                        batch.put_bytes(&chunk_key, chunk_data.freeze());
                    }
                }
                // If chunk doesn't exist, nothing to trim
            }
        }

        self.commit_batch_ordered(batch, &mut seq_guard)
            .await
            .inspect_err(|e| {
                error!("Failed to commit trim batch: {}", e);
            })?;

        debug!("Trim completed successfully for inode {}", id);
        Ok(())
    }
}
