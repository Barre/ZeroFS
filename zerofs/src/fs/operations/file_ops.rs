use super::common::validate_filename;
use crate::fs::cache::{CacheKey, CacheValue};
use crate::fs::errors::FsError;
use crate::fs::inode::{FileInode, Inode};
use crate::fs::permissions::{AccessMode, Credentials, check_access, validate_mode};
use crate::fs::types::{
    AuthContext, FileAttributes, InodeId, InodeWithId, SetAttributes, SetGid, SetMode, SetUid,
};
use crate::fs::{ZeroFS, get_current_time};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{debug, error};

impl ZeroFS {
    pub async fn process_write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let creds = Credentials::from_auth_context(auth);

        // Optimistically load inode and check parent permissions before lock
        let _ = self.inode_store.get(id).await?;
        self.check_parent_execute_permissions(id, &creds).await?;

        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.inode_store.get(id).await?;

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

                let mut txn = self.db.new_transaction()?;

                self.chunk_store.write(&mut txn, id, offset, data).await;

                file.size = new_size;
                let (now_sec, now_nsec) = get_current_time();
                file.mtime = now_sec;
                file.mtime_nsec = now_nsec;

                // POSIX: Clear SUID/SGID bits on write by non-owner
                if creds.uid != file.uid && creds.uid != 0 {
                    file.mode &= !0o6000;
                }

                self.inode_store.save(&mut txn, id, &inode)?;

                let stats_update = if let Some(update) = self
                    .global_stats
                    .prepare_size_change(id, old_size, new_size)
                    .await
                {
                    self.global_stats.add_to_transaction(&update, &mut txn)?;
                    Some(update)
                } else {
                    None
                };

                let db_write_start = std::time::Instant::now();
                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;
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
        validate_filename(name)?;

        debug!(
            "process_create: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        // Optimistic existence check without holding lock
        if self.directory_store.exists(dirid, name).await? {
            return Err(FsError::Exists);
        }

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                // Re-check existence inside lock (should hit cache and be fast)
                if self.directory_store.exists(dirid, name).await? {
                    return Err(FsError::Exists);
                }

                let file_id = self.inode_store.allocate()?;
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

                let mut txn = self.db.new_transaction()?;

                self.inode_store
                    .save(&mut txn, file_id, &Inode::File(file_inode.clone()))?;
                self.directory_store.add(&mut txn, dirid, name, file_id);

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                let stats_update = self.global_stats.prepare_inode_create(file_id).await;
                self.global_stats
                    .add_to_transaction(&stats_update, &mut txn)?;

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard)
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

        let inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    return Ok((Bytes::new(), true));
                }

                let read_len = std::cmp::min(count as u64, file.size - offset);
                let result_bytes = self.chunk_store.read(id, offset, read_len).await;
                let eof = offset + read_len >= file.size;

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
        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire_write(id).await;
        let inode = self.inode_store.get(id).await?;

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

        let mut txn = self.db.new_transaction()?;

        self.chunk_store
            .zero_range(&mut txn, id, offset, length, file.size)
            .await;

        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard)
            .await
            .inspect_err(|e| {
                error!("Failed to commit trim batch: {}", e);
            })?;

        debug!("Trim completed successfully for inode {}", id);
        Ok(())
    }
}
