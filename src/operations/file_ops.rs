use futures::future::join_all;
use futures::stream::{self, StreamExt};
use slatedb::config::WriteOptions;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tracing::{debug, error};
use zerofs_nfsserve::nfs::{fattr3, fileid3, nfsstat3, sattr3, set_gid3, set_mode3, set_uid3};
use zerofs_nfsserve::vfs::AuthContext;

use super::common::validate_filename;
use crate::cache::{self, CacheKey, CacheValue};
use crate::filesystem::{CHUNK_SIZE, SlateDbFs, get_current_time};
use crate::inode::{FileInode, Inode, InodeId};
use crate::permissions::{AccessMode, Credentials, check_access, validate_mode};

impl SlateDbFs {
    pub async fn process_write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        // NFS RFC 1813 section 4.4 suggests that servers should allow the owner of a file
        // to access it regardless of permission settings, to better emulate POSIX semantics
        // where a file descriptor retains its access rights even if the file mode changes.
        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            _ => {} // Owner can always write to their own files
        }

        match &mut inode {
            Inode::File(file) => {
                let old_size = file.size;
                let end_offset = offset + data.len() as u64;
                let new_size = std::cmp::max(file.size, end_offset);

                let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                let end_chunk = ((end_offset - 1) / CHUNK_SIZE as u64) as usize;

                let mut batch = self.db.new_write_batch();

                let chunk_processing_start = std::time::Instant::now();

                let partial_chunks: Vec<_> = (start_chunk..=end_chunk)
                    .filter(|&chunk_idx| {
                        let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
                        let write_start = if offset > chunk_start {
                            (offset - chunk_start) as usize
                        } else {
                            0
                        };
                        let write_end = if end_offset < chunk_start + CHUNK_SIZE as u64 {
                            (end_offset - chunk_start) as usize
                        } else {
                            CHUNK_SIZE
                        };

                        write_start > 0 || write_end < CHUNK_SIZE
                    })
                    .collect();

                let prefetch_futures: Vec<_> = partial_chunks
                    .iter()
                    .map(|&chunk_idx| {
                        let chunk_key = Self::chunk_key_by_index(id, chunk_idx);
                        let db = self.db.clone();
                        async move {
                            let data = db.get_bytes(&chunk_key).await.ok().flatten();
                            (chunk_idx, data)
                        }
                    })
                    .collect();

                let prefetched_data = join_all(prefetch_futures).await;
                let existing_chunks: HashMap<usize, Vec<u8>> = prefetched_data
                    .into_iter()
                    .filter_map(|(idx, data)| data.map(|d| (idx, d.to_vec())))
                    .collect();

                for chunk_idx in start_chunk..=end_chunk {
                    let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
                    let chunk_end = chunk_start + CHUNK_SIZE as u64;

                    let mut chunk_data = vec![0u8; CHUNK_SIZE];
                    let chunk_key = Self::chunk_key_by_index(id, chunk_idx);

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

                    if write_start > 0 || write_end < CHUNK_SIZE {
                        if let Some(existing_data) = existing_chunks.get(&chunk_idx) {
                            let copy_len = existing_data.len().min(CHUNK_SIZE);
                            chunk_data[..copy_len].copy_from_slice(&existing_data[..copy_len]);
                        }
                    }

                    let data_offset = (chunk_idx - start_chunk) * CHUNK_SIZE + write_start
                        - (offset % CHUNK_SIZE as u64) as usize;
                    let data_len = write_end - write_start;
                    chunk_data[write_start..write_end]
                        .copy_from_slice(&data[data_offset..data_offset + data_len]);

                    batch
                        .put_bytes(&chunk_key, &chunk_data)
                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                }

                debug!(
                    "Chunk processing took: {:?}",
                    chunk_processing_start.elapsed()
                );

                file.size = new_size;
                let (now_sec, now_nsec) = get_current_time();
                file.mtime = now_sec;
                file.mtime_nsec = now_nsec;

                // Clear SUID/SGID bits on write by non-owner
                if creds.uid != file.uid && creds.uid != 0 {
                    file.mode &= !0o6000; // Clear both SUID (4000) and SGID (2000)
                }

                let inode_key = Self::inode_key(id);
                let inode_data = bincode::serialize(&inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&inode_key, &inode_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

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
                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                debug!("DB write took: {:?}", db_write_start.elapsed());

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                self.metadata_cache.remove(CacheKey::Metadata(id));
                self.small_file_cache.remove(CacheKey::SmallFile(id));

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

                Ok(inode.to_fattr3(id))
            }
            _ => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    pub async fn process_create(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        validate_filename(filename)?;

        let filename_str = String::from_utf8_lossy(filename);
        debug!("process_create: dirid={}, filename={}", dirid, filename_str);

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = filename_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                    .is_some()
                {
                    debug!("File {} already exists", name);
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }

                let file_id = self.allocate_inode().await?;
                debug!("Allocated inode {} for file {}", file_id, name);

                let (now_sec, now_nsec) = get_current_time();

                // If parent has setgid bit set, file inherits parent's group
                // (gid was already set correctly from parent in the match above)

                let final_mode = match attr.mode {
                    set_mode3::mode(m) => validate_mode(m),
                    _ => 0o666,
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
                    uid: match attr.uid {
                        set_uid3::uid(u) => u,
                        _ => auth.uid,
                    },
                    gid: match attr.gid {
                        set_gid3::gid(g) => g,
                        _ => auth.gid,
                    },
                    parent: dirid,
                    nlink: 1,
                };

                let mut batch = self.db.new_write_batch();

                let file_inode_key = Self::inode_key(file_id);
                let file_inode_data = bincode::serialize(&Inode::File(file_inode.clone()))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&file_inode_key, &file_inode_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                batch
                    .put_bytes(&entry_key, &file_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let scan_key = Self::dir_scan_key(dirid, file_id, &name);
                batch
                    .put_bytes(&scan_key, &file_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                dir.entry_count += 1;
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                // Persist the counter
                let counter_key = Self::counter_key();
                let next_id = self.next_inode_id.load(Ordering::SeqCst);
                batch
                    .put_bytes(&counter_key, &next_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let dir_key = Self::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&dir_key, &dir_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                // Update statistics
                let stats_update = self.global_stats.prepare_inode_create(file_id).await;
                self.global_stats.add_to_batch(&stats_update, &mut batch)?;

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|e| {
                        error!("Failed to write batch: {:?}", e);
                        nfsstat3::NFS3ERR_IO
                    })?;

                // Update in-memory statistics after successful commit
                self.global_stats.commit_update(&stats_update);

                self.metadata_cache.remove(CacheKey::Metadata(dirid));

                self.stats.files_created.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok((file_id, Inode::File(file_inode).to_fattr3(file_id)))
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    pub async fn process_create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
    ) -> Result<fileid3, nfsstat3> {
        let (id, _) = self
            .process_create(auth, dirid, filename, sattr3::default())
            .await?;
        Ok(id)
    }

    pub async fn process_read_file(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        debug!(
            "process_read_file: id={}, offset={}, count={}",
            id, offset, count
        );

        let _guard = self.lock_manager.acquire_read(id).await;

        let inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    return Ok((vec![], true));
                }

                if file.size <= cache::SMALL_FILE_THRESHOLD_BYTES
                    && offset == 0
                    && count as u64 >= file.size
                {
                    let cache_key = CacheKey::SmallFile(id);
                    if let Some(CacheValue::SmallFile(cached_data)) =
                        self.small_file_cache.get(cache_key).await
                    {
                        debug!("Serving file {} from small file cache", id);
                        let eof = file.size <= count as u64;
                        self.stats
                            .bytes_read
                            .fetch_add(cached_data.len() as u64, Ordering::Relaxed);
                        self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);
                        return Ok(((*cached_data).clone(), eof));
                    }
                }

                let end = std::cmp::min(offset + count as u64, file.size);
                let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                let end_chunk = ((end - 1) / CHUNK_SIZE as u64) as usize;
                let start_offset = (offset % CHUNK_SIZE as u64) as usize;

                // Create a stream of futures for chunk reads
                let chunk_futures = stream::iter(start_chunk..=end_chunk).map(|chunk_idx| {
                    let db = self.db.clone();
                    let key = Self::chunk_key_by_index(id, chunk_idx);
                    async move {
                        let chunk_data_opt =
                            db.get_bytes(&key).await.map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        let chunk_vec_opt = chunk_data_opt.map(|bytes| bytes.to_vec());
                        Ok::<(usize, Option<Vec<u8>>), nfsstat3>((chunk_idx, chunk_vec_opt))
                    }
                });

                const BUFFER_SIZE: usize = 8;
                let mut chunks: Vec<(usize, Option<Vec<u8>>)> = chunk_futures
                    .buffered(BUFFER_SIZE)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                chunks.sort_by_key(|(idx, _)| *idx);

                // Assemble the result
                let mut result = Vec::with_capacity((end - offset) as usize);

                for (chunk_idx, chunk_data_opt) in chunks {
                    if let Some(chunk_data) = chunk_data_opt {
                        if chunk_idx == start_chunk && chunk_idx == end_chunk {
                            let end_offset = start_offset + (end - offset) as usize;
                            let safe_end = std::cmp::min(end_offset, chunk_data.len());
                            let safe_start = std::cmp::min(start_offset, chunk_data.len());
                            if safe_start < safe_end {
                                result.extend_from_slice(&chunk_data[safe_start..safe_end]);
                            }
                            // If we need more data than the chunk contains, pad with zeros
                            if end_offset > chunk_data.len() && safe_start < chunk_data.len() {
                                let zeros_needed = end_offset - chunk_data.len();
                                result.extend(vec![0u8; zeros_needed]);
                            }
                        } else if chunk_idx == start_chunk {
                            let safe_start = std::cmp::min(start_offset, chunk_data.len());
                            if safe_start < chunk_data.len() {
                                result.extend_from_slice(&chunk_data[safe_start..]);
                            }
                        } else if chunk_idx == end_chunk {
                            let bytes_in_last = ((end - 1) % CHUNK_SIZE as u64 + 1) as usize;
                            let safe_bytes = std::cmp::min(bytes_in_last, chunk_data.len());
                            result.extend_from_slice(&chunk_data[..safe_bytes]);
                            // If we need more data than the chunk contains, pad with zeros
                            if bytes_in_last > chunk_data.len() {
                                let zeros_needed = bytes_in_last - chunk_data.len();
                                result.extend(vec![0u8; zeros_needed]);
                            }
                        } else {
                            result.extend_from_slice(&chunk_data);
                        }
                    } else {
                        let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
                        let chunk_end = std::cmp::min(chunk_start + CHUNK_SIZE as u64, file.size);
                        let chunk_size = (chunk_end - chunk_start) as usize;

                        if chunk_idx == start_chunk && chunk_idx == end_chunk {
                            let end_offset = start_offset + (end - offset) as usize;
                            result.extend(vec![0u8; end_offset - start_offset]);
                        } else if chunk_idx == start_chunk {
                            result.extend(vec![0u8; chunk_size - start_offset]);
                        } else if chunk_idx == end_chunk {
                            let bytes_in_last = ((end - 1) % CHUNK_SIZE as u64 + 1) as usize;
                            result.extend(vec![0u8; bytes_in_last]);
                        } else {
                            result.extend(vec![0u8; chunk_size]);
                        }
                    }
                }

                let eof = end >= file.size;

                if file.size <= crate::cache::SMALL_FILE_THRESHOLD_BYTES
                    && offset == 0
                    && end >= file.size
                {
                    debug!("Caching small file {} ({} bytes)", id, file.size);
                    let cache_key = crate::cache::CacheKey::SmallFile(id);
                    let cache_value =
                        crate::cache::CacheValue::SmallFile(std::sync::Arc::new(result.clone()));
                    self.small_file_cache.insert(cache_key, cache_value, false);
                }

                self.stats
                    .bytes_read
                    .fetch_add(result.len() as u64, Ordering::Relaxed);
                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok((result, eof))
            }
            _ => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    pub async fn trim(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Result<(), nfsstat3> {
        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire_write(id).await;
        let inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        // Owner can always trim their files
        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(nfsstat3::NFS3ERR_ISDIR),
        }

        let file = match &inode {
            Inode::File(f) => f,
            _ => return Err(nfsstat3::NFS3ERR_ISDIR),
        };

        let end_offset = offset + length;
        let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
        let end_chunk = ((end_offset.saturating_sub(1)) / CHUNK_SIZE as u64) as usize;

        let mut batch = self.db.new_write_batch();

        for chunk_idx in start_chunk..=end_chunk {
            let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
            let chunk_end = chunk_start + CHUNK_SIZE as u64;

            if chunk_start >= file.size {
                continue;
            }

            let chunk_key = Self::chunk_key_by_index(id, chunk_idx);

            // Check if entire chunk is being trimmed
            if offset <= chunk_start && end_offset >= chunk_end {
                // Delete the entire chunk
                batch.delete_bytes(&chunk_key);

                let cache_key = CacheKey::Block {
                    inode_id: id,
                    block_index: chunk_idx as u64,
                };
                self.metadata_cache.remove(cache_key);
            } else {
                // Partial trim - need to zero out the trimmed portion
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

                // Load existing chunk data
                let mut chunk_data = vec![0u8; CHUNK_SIZE];
                let mut has_data = false;

                if let Some(existing_data) = self
                    .db
                    .get_bytes(&chunk_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                {
                    let copy_len = existing_data.len().min(CHUNK_SIZE);
                    chunk_data[..copy_len].copy_from_slice(&existing_data[..copy_len]);
                    has_data = true;
                }

                if has_data {
                    chunk_data[trim_start..trim_end].fill(0);

                    if chunk_data.iter().all(|&b| b == 0) {
                        batch.delete_bytes(&chunk_key);

                        let cache_key = CacheKey::Block {
                            inode_id: id,
                            block_index: chunk_idx as u64,
                        };
                        self.metadata_cache.remove(cache_key);
                    } else {
                        let chunk_size_in_file =
                            std::cmp::min(CHUNK_SIZE, (file.size - chunk_start) as usize);
                        batch
                            .put_bytes(&chunk_key, &chunk_data[..chunk_size_in_file])
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                        let cache_key = CacheKey::Block {
                            inode_id: id,
                            block_index: chunk_idx as u64,
                        };
                        self.metadata_cache.remove(cache_key);
                    }
                }
            }
        }

        if file.size <= crate::cache::SMALL_FILE_THRESHOLD_BYTES {
            let cache_key = CacheKey::SmallFile(id);
            self.small_file_cache.remove(cache_key);
        }

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|e| {
                error!("Failed to commit trim batch: {}", e);
                nfsstat3::NFS3ERR_IO
            })?;

        debug!("Trim completed successfully for inode {}", id);
        Ok(())
    }
}
