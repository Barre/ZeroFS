use futures::stream::{self, StreamExt};
use nfsserve::nfs::{
    fattr3, fileid3, ftype3, nfsstat3, nfstime3, sattr3, set_atime, set_gid3, set_mode3, set_mtime,
    set_size3, set_uid3,
};
use nfsserve::vfs::{AuthContext, DirEntry, ReadDirResult};
use slatedb::{WriteBatch, config::WriteOptions};
use std::sync::atomic::Ordering;
use tracing::{debug, error};

use crate::filesystem::{CHUNK_SIZE, LOCK_SHARD_COUNT, SlateDbFs, get_current_time, get_umask};
use crate::inode::{DirectoryInode, FileInode, Inode, InodeId, SpecialInode, SymlinkInode};
use crate::permissions::{
    AccessMode, Credentials, can_set_times, check_access, check_ownership, check_sticky_bit_delete,
    validate_mode,
};

impl SlateDbFs {
    async fn is_ancestor_of(
        &self,
        ancestor_id: InodeId,
        descendant_id: InodeId,
    ) -> Result<bool, nfsstat3> {
        if ancestor_id == descendant_id {
            return Ok(true);
        }

        let mut current_id = descendant_id;

        while current_id != 0 {
            let inode = self.load_inode(current_id).await?;
            let parent_id = match inode {
                Inode::File(f) => f.parent,
                Inode::Directory(d) => d.parent,
                Inode::Symlink(s) => s.parent,
                Inode::Fifo(s) => s.parent,
                Inode::Socket(s) => s.parent,
                Inode::CharDevice(s) => s.parent,
                Inode::BlockDevice(s) => s.parent,
            };

            if parent_id == ancestor_id {
                return Ok(true);
            }

            current_id = parent_id;
        }

        Ok(false)
    }

    /// Check execute permission on all parent directories leading to a file
    async fn check_parent_execute_permissions(
        &self,
        id: InodeId,
        creds: &Credentials,
    ) -> Result<(), nfsstat3> {
        // Root directory doesn't need parent permission check
        if id == 0 {
            return Ok(());
        }

        let inode = self.load_inode(id).await?;
        let parent_id = match &inode {
            Inode::File(f) => f.parent,
            Inode::Directory(d) => d.parent,
            Inode::Symlink(s) => s.parent,
            Inode::Fifo(s) => s.parent,
            Inode::Socket(s) => s.parent,
            Inode::CharDevice(s) => s.parent,
            Inode::BlockDevice(s) => s.parent,
        };

        // Walk up the directory tree checking execute permissions
        let mut current_id = parent_id;
        while current_id != 0 {
            let parent_inode = self.load_inode(current_id).await?;

            check_access(&parent_inode, creds, AccessMode::Execute)?;

            current_id = match &parent_inode {
                Inode::Directory(d) => d.parent,
                _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
            };
        }

        Ok(())
    }

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

        let lock = self.get_inode_lock(id);
        let _guard = lock.lock().await;
        let mut inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        // Check execute permission on all parent directories
        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Write)?;

        match &mut inode {
            Inode::File(file) => {
                let end_offset = offset + data.len() as u64;
                let new_size = std::cmp::max(file.size, end_offset);

                let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                let end_chunk = ((end_offset - 1) / CHUNK_SIZE as u64) as usize;

                let mut batch = WriteBatch::new();

                let chunk_processing_start = std::time::Instant::now();
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
                        if let Some(existing_data) = self
                            .db
                            .get(&chunk_key)
                            .await
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?
                        {
                            let copy_len = existing_data.len().min(CHUNK_SIZE);
                            chunk_data[..copy_len].copy_from_slice(&existing_data[..copy_len]);
                        }
                    }

                    let data_offset = (chunk_idx - start_chunk) * CHUNK_SIZE + write_start
                        - (offset % CHUNK_SIZE as u64) as usize;
                    let data_len = write_end - write_start;
                    chunk_data[write_start..write_end]
                        .copy_from_slice(&data[data_offset..data_offset + data_len]);

                    let chunk_data_to_store =
                        &chunk_data[..std::cmp::min(CHUNK_SIZE, (new_size - chunk_start) as usize)];
                    batch.put(chunk_key, chunk_data_to_store);
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
                batch.put(inode_key, &inode_data);

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

                let elapsed = start_time.elapsed();
                debug!(
                    "Write processed successfully for inode {}, new size: {}, took: {:?}",
                    id, new_size, elapsed
                );
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
        let filename_str = String::from_utf8_lossy(filename);
        debug!("process_create: dirid={}, filename={}", dirid, filename_str);

        let lock = self.get_inode_lock(dirid);
        let _guard = lock.lock().await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid, _parent_mode) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid, d.mode),
            _ => {
                #[cfg(unix)]
                unsafe {
                    (libc::getuid(), libc::getgid(), 0o755)
                }
                #[cfg(not(unix))]
                (0, 0, 0o755)
            }
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = filename_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get(&entry_key)
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

                // Apply umask to file mode
                let umask = get_umask();
                let requested_mode = match attr.mode {
                    set_mode3::mode(m) => validate_mode(m),
                    _ => 0o666,
                };
                let final_mode = requested_mode & !umask;

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

                let mut batch = WriteBatch::new();

                let file_inode_key = Self::inode_key(file_id);
                let file_inode_data = bincode::serialize(&Inode::File(file_inode.clone()))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(file_inode_key, &file_inode_data);

                batch.put(entry_key, file_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, file_id, &name);
                batch.put(scan_key, file_id.to_le_bytes());

                dir.entry_count += 1;
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                // Persist the counter
                let counter_key = Self::counter_key();
                let next_id = self.next_inode_id.load(Ordering::SeqCst);
                batch.put(counter_key, next_id.to_le_bytes());

                let dir_key = Self::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(dir_key, &dir_data);

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

    pub async fn process_mkdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        dirname: &[u8],
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let dirname_str = String::from_utf8_lossy(dirname);
        debug!("process_mkdir: dirid={}, dirname={}", dirid, dirname_str);

        let lock = self.get_inode_lock(dirid);
        let _guard = lock.lock().await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid),
            _ => {
                #[cfg(unix)]
                unsafe {
                    (libc::getuid(), libc::getgid())
                }
                #[cfg(not(unix))]
                (0, 0)
            }
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = dirname_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                    .is_some()
                {
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }

                let new_dir_id = self.allocate_inode().await?;

                let (now_sec, now_nsec) = get_current_time();

                let umask = get_umask();
                let mut new_mode = match attr.mode {
                    set_mode3::mode(m) => m & !umask,
                    set_mode3::Void => 0o777 & !umask,
                };

                let parent_mode = dir.mode;
                if parent_mode & 0o2000 != 0 {
                    new_mode |= 0o2000;
                }

                // Apply uid/gid from attributes, with defaults
                let new_uid = match attr.uid {
                    set_uid3::uid(u) => u,
                    set_uid3::Void => auth.uid,
                };

                let new_gid = match attr.gid {
                    set_gid3::gid(g) => g,
                    set_gid3::Void => {
                        // If parent has setgid bit, inherit parent's gid
                        if parent_mode & 0o2000 != 0 {
                            dir.gid
                        } else {
                            auth.gid
                        }
                    }
                };

                // Apply time attributes
                let (atime_sec, atime_nsec) = match attr.atime {
                    set_atime::SET_TO_CLIENT_TIME(nfstime3 { seconds, nseconds }) => {
                        (seconds as u64, nseconds)
                    }
                    set_atime::SET_TO_SERVER_TIME | set_atime::DONT_CHANGE => (now_sec, now_nsec),
                };

                let (mtime_sec, mtime_nsec) = match attr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(nfstime3 { seconds, nseconds }) => {
                        (seconds as u64, nseconds)
                    }
                    set_mtime::SET_TO_SERVER_TIME | set_mtime::DONT_CHANGE => (now_sec, now_nsec),
                };

                let new_dir_inode = DirectoryInode {
                    mtime: mtime_sec,
                    mtime_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: atime_sec,
                    atime_nsec,
                    mode: new_mode,
                    uid: new_uid,
                    gid: new_gid,
                    entry_count: 0,
                    parent: dirid,
                    nlink: 2, // . and parent's reference
                };

                let mut batch = WriteBatch::new();

                let new_dir_key = Self::inode_key(new_dir_id);
                let new_dir_data = bincode::serialize(&Inode::Directory(new_dir_inode.clone()))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(new_dir_key, &new_dir_data);

                batch.put(entry_key, new_dir_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, new_dir_id, &name);
                batch.put(scan_key, new_dir_id.to_le_bytes());

                dir.entry_count += 1;
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                // Persist the counter
                let counter_key = Self::counter_key();
                let next_id = self.next_inode_id.load(Ordering::SeqCst);
                batch.put(counter_key, next_id.to_le_bytes());

                let parent_dir_key = Self::inode_key(dirid);
                let parent_dir_data =
                    bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(parent_dir_key, &parent_dir_data);

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                Ok((
                    new_dir_id,
                    Inode::Directory(new_dir_inode).to_fattr3(new_dir_id),
                ))
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    pub async fn process_remove(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
    ) -> Result<(), nfsstat3> {
        let lock = self.get_inode_lock(dirid);
        let _guard = lock.lock().await;
        let dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);

        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let is_dir = matches!(dir_inode, Inode::Directory(_));
        if !is_dir {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }

        let name = String::from_utf8_lossy(filename).to_string();

        let entry_key = Self::dir_entry_key(dirid, &name);
        let entry_data = self
            .db
            .get(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let file_id = u64::from_le_bytes(bytes);

        let mut file_inode = self.load_inode(file_id).await?;

        check_sticky_bit_delete(&dir_inode, &file_inode, &creds)?;

        let mut dir_inode = self.load_inode(dirid).await?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let mut batch = WriteBatch::new();

                match &mut file_inode {
                    Inode::File(file) => {
                        // Check if this is the last hard link
                        if file.nlink > 1 {
                            // Just decrement the link count, don't delete the file
                            file.nlink -= 1;
                            let (now_sec, now_nsec) = get_current_time();
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            // Save the updated inode
                            let inode_key = Self::inode_key(file_id);
                            let inode_data = bincode::serialize(&file_inode)
                                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                            batch.put(inode_key, &inode_data);
                        } else {
                            // Last link, delete all data chunks
                            let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;
                            for chunk_idx in 0..total_chunks {
                                let chunk_key = Self::chunk_key_by_index(file_id, chunk_idx);
                                batch.delete(chunk_key);
                            }
                            // Delete the inode
                            let inode_key = Self::inode_key(file_id);
                            batch.delete(inode_key);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                        }
                        // Delete the directory inode
                        let inode_key = Self::inode_key(file_id);
                        batch.delete(inode_key);
                    }
                    Inode::Symlink(_) => {
                        // Delete the symlink inode
                        let inode_key = Self::inode_key(file_id);
                        batch.delete(inode_key);
                    }
                    Inode::Fifo(special)
                    | Inode::Socket(special)
                    | Inode::CharDevice(special)
                    | Inode::BlockDevice(special) => {
                        // Check if this is the last hard link
                        if special.nlink > 1 {
                            // Just decrement the link count, don't delete the inode
                            special.nlink -= 1;
                            let (now_sec, now_nsec) = get_current_time();
                            special.ctime = now_sec;
                            special.ctime_nsec = now_nsec;

                            let inode_key = Self::inode_key(file_id);
                            let inode_data = bincode::serialize(&file_inode)
                                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                            batch.put(inode_key, &inode_data);
                        } else {
                            // Last link, delete the inode
                            let inode_key = Self::inode_key(file_id);
                            batch.delete(inode_key);
                        }
                    }
                }

                batch.delete(entry_key);

                let scan_key = Self::dir_scan_key(dirid, file_id, &name);
                batch.delete(scan_key);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_key = Self::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(dir_key, &dir_data);

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                Ok(())
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    pub async fn process_rename(
        &self,
        auth: &AuthContext,
        from_dirid: fileid3,
        from_filename: &[u8],
        to_dirid: fileid3,
        to_filename: &[u8],
    ) -> Result<(), nfsstat3> {
        // Validate filenames are not empty
        if from_filename.is_empty() || to_filename.is_empty() {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        let from_name = String::from_utf8_lossy(from_filename).to_string();
        let to_name = String::from_utf8_lossy(to_filename).to_string();

        if from_name == "." || from_name == ".." {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        if to_name == "." || to_name == ".." {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        if from_dirid == to_dirid && from_name == to_name {
            return Ok(());
        }

        debug!(
            "process_rename: from_dir={}, from_name={}, to_dir={}, to_name={}",
            from_dirid, from_name, to_dirid, to_name
        );

        let from_entry_key = Self::dir_entry_key(from_dirid, &from_name);
        let entry_data = self
            .db
            .get(&from_entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let source_inode_id = u64::from_le_bytes(bytes);

        // Check if we're trying to move something into itself
        // This catches cases like: rename /foo /foo/bar
        if to_dirid == source_inode_id {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        // For directories, check if we're trying to move into a descendant
        // This prevents cycles like: rename /a /a/b/c
        let source_inode = self.load_inode(source_inode_id).await?;
        if matches!(source_inode, Inode::Directory(_))
            && self.is_ancestor_of(source_inode_id, to_dirid).await?
        {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        let creds = Credentials::from_auth_context(auth);

        if from_dirid == to_dirid {
            let lock = self.get_inode_lock(from_dirid);
            let _guard = lock.lock().await;

            let dir_inode = self.load_inode(from_dirid).await?;
            check_access(&dir_inode, &creds, AccessMode::Write)?;
            check_access(&dir_inode, &creds, AccessMode::Execute)?;

            let source_inode = self.load_inode(source_inode_id).await?;
            check_sticky_bit_delete(&dir_inode, &source_inode, &creds)?;

            let inode_id = source_inode_id;

            let to_entry_key = Self::dir_entry_key(from_dirid, &to_name);
            let target_exists = if let Some(existing_entry) = self
                .db
                .get(&to_entry_key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let mut existing_bytes = [0u8; 8];
                existing_bytes.copy_from_slice(&existing_entry[..8]);
                let existing_id = u64::from_le_bytes(existing_bytes);

                let existing_inode = self.load_inode(existing_id).await?;

                if let Inode::Directory(dir) = &existing_inode {
                    if dir.entry_count > 0 {
                        return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                    }
                }
                true
            } else {
                false
            };

            let mut batch = WriteBatch::new();

            // If target exists, clean it up
            if let Some(existing_entry) = self
                .db
                .get(&to_entry_key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let mut existing_bytes = [0u8; 8];
                existing_bytes.copy_from_slice(&existing_entry[..8]);
                let existing_id = u64::from_le_bytes(existing_bytes);
                let existing_inode = self.load_inode(existing_id).await?;

                match existing_inode {
                    Inode::File(file) => {
                        let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;
                        for chunk_idx in 0..total_chunks {
                            let chunk_key = Self::chunk_key_by_index(existing_id, chunk_idx);
                            batch.delete(chunk_key);
                        }
                    }
                    Inode::Directory(_) => {
                        // Directory case already handled above (must be empty)
                    }
                    Inode::Symlink(_) => {}
                    Inode::Fifo(_)
                    | Inode::Socket(_)
                    | Inode::CharDevice(_)
                    | Inode::BlockDevice(_) => {
                        // Special files don't have data chunks
                    }
                }

                // Delete the existing inode
                let inode_key = Self::inode_key(existing_id);
                batch.delete(inode_key);

                // Delete the existing scan entry
                let existing_scan_key = Self::dir_scan_key(from_dirid, existing_id, &to_name);
                batch.delete(existing_scan_key);
            }

            // Delete old entry
            batch.delete(from_entry_key);

            // Delete old scan entry
            let from_scan_key = Self::dir_scan_key(from_dirid, inode_id, &from_name);
            batch.delete(from_scan_key);

            // Add new entry
            batch.put(to_entry_key, inode_id.to_le_bytes());

            // Add new scan entry
            let to_scan_key = Self::dir_scan_key(to_dirid, inode_id, &to_name);
            batch.put(to_scan_key, inode_id.to_le_bytes());

            // Update directory metadata
            let mut dir_inode = self.load_inode(from_dirid).await?;
            if let Inode::Directory(dir) = &mut dir_inode {
                // When renaming within the same directory:
                // - If replacing an existing entry: we remove 2 (source + target) and add 1 = net -1
                // - If not replacing: we remove 1 (source) and add 1 = net 0 (handled by just updating mtime)
                if target_exists {
                    dir.entry_count = dir.entry_count.saturating_sub(1);
                }
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;
            }

            let dir_key = Self::inode_key(from_dirid);
            let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
            batch.put(dir_key, &dir_data);

            self.db
                .write_with_options(
                    batch,
                    &WriteOptions {
                        await_durable: false,
                    },
                )
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;

            Ok(())
        } else {
            // Calculate shard indices for both directories
            let from_shard = (from_dirid as usize) % LOCK_SHARD_COUNT;
            let to_shard = (to_dirid as usize) % LOCK_SHARD_COUNT;

            // Get both locks first
            let from_lock = self.get_inode_lock(from_dirid);
            let to_lock = self.get_inode_lock(to_dirid);

            // Acquire locks in consistent order based on shard index
            let _guards = if from_shard < to_shard {
                (from_lock.lock().await, to_lock.lock().await)
            } else if to_shard < from_shard {
                (to_lock.lock().await, from_lock.lock().await)
            } else {
                // Same shard - need to order by inode ID to handle different inodes in same shard
                if from_dirid < to_dirid {
                    (from_lock.lock().await, to_lock.lock().await)
                } else {
                    (to_lock.lock().await, from_lock.lock().await)
                }
            };

            // Check permissions on both directories
            let from_dir = self.load_inode(from_dirid).await?;
            let to_dir = self.load_inode(to_dirid).await?;

            // Need write+execute on both directories
            check_access(&from_dir, &creds, AccessMode::Write)?;
            check_access(&from_dir, &creds, AccessMode::Execute)?;
            check_access(&to_dir, &creds, AccessMode::Write)?;
            check_access(&to_dir, &creds, AccessMode::Execute)?;

            // Check sticky bit on source directory
            let source_inode = self.load_inode(source_inode_id).await?;
            check_sticky_bit_delete(&from_dir, &source_inode, &creds)?;

            let inode_id = source_inode_id;

            // Check if target already exists and handle it
            let to_entry_key = Self::dir_entry_key(to_dirid, &to_name);
            let target_exists = if let Some(existing_entry) = self
                .db
                .get(&to_entry_key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let mut existing_bytes = [0u8; 8];
                existing_bytes.copy_from_slice(&existing_entry[..8]);
                let existing_id = u64::from_le_bytes(existing_bytes);

                let existing_inode = self.load_inode(existing_id).await?;

                if let Inode::Directory(dir) = &existing_inode {
                    if dir.entry_count > 0 {
                        return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                    }
                }
                true
            } else {
                false
            };

            let mut batch = WriteBatch::new();

            // If target exists, clean it up
            if let Some(existing_entry) = self
                .db
                .get(&to_entry_key)
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?
            {
                let mut existing_bytes = [0u8; 8];
                existing_bytes.copy_from_slice(&existing_entry[..8]);
                let existing_id = u64::from_le_bytes(existing_bytes);
                let existing_inode = self.load_inode(existing_id).await?;

                match existing_inode {
                    Inode::File(file) => {
                        let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;
                        for chunk_idx in 0..total_chunks {
                            let chunk_key = Self::chunk_key_by_index(existing_id, chunk_idx);
                            batch.delete(chunk_key);
                        }
                    }
                    Inode::Directory(_) => {
                        // Directory case already handled above (must be empty)
                    }
                    Inode::Symlink(_) => {}
                    Inode::Fifo(_)
                    | Inode::Socket(_)
                    | Inode::CharDevice(_)
                    | Inode::BlockDevice(_) => {
                        // Special files don't have data chunks
                    }
                }

                // Delete the existing inode
                let inode_key = Self::inode_key(existing_id);
                batch.delete(inode_key);

                // Delete the existing scan entry
                let existing_scan_key = Self::dir_scan_key(to_dirid, existing_id, &to_name);
                batch.delete(existing_scan_key);

                // Update target directory entry count (will be decremented for removal, then incremented for addition)
            }

            // Delete from source directory
            batch.delete(from_entry_key);

            // Delete old scan entry
            let from_scan_key = Self::dir_scan_key(from_dirid, inode_id, &from_name);
            batch.delete(from_scan_key);

            // Add to target directory
            batch.put(to_entry_key, inode_id.to_le_bytes());

            // Add new scan entry
            let to_scan_key = Self::dir_scan_key(to_dirid, inode_id, &to_name);
            batch.put(to_scan_key, inode_id.to_le_bytes());

            // Update the moved inode's parent field
            let mut moved_inode = self.load_inode(inode_id).await?;
            match &mut moved_inode {
                Inode::File(f) => f.parent = to_dirid,
                Inode::Directory(d) => d.parent = to_dirid,
                Inode::Symlink(s) => s.parent = to_dirid,
                Inode::Fifo(s) => s.parent = to_dirid,
                Inode::Socket(s) => s.parent = to_dirid,
                Inode::CharDevice(s) => s.parent = to_dirid,
                Inode::BlockDevice(s) => s.parent = to_dirid,
            }
            batch.put(
                Self::inode_key(inode_id),
                &bincode::serialize(&moved_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?,
            );

            let (now_sec, now_nsec) = get_current_time();

            // Update source directory
            let mut from_dir = self.load_inode(from_dirid).await?;
            if let Inode::Directory(d) = &mut from_dir {
                d.entry_count = d.entry_count.saturating_sub(1);
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            batch.put(
                Self::inode_key(from_dirid),
                &bincode::serialize(&from_dir).map_err(|_| nfsstat3::NFS3ERR_IO)?,
            );

            // Update target directory
            let mut to_dir = self.load_inode(to_dirid).await?;
            if let Inode::Directory(d) = &mut to_dir {
                // Only increment if we're not replacing an existing entry
                if !target_exists {
                    d.entry_count += 1;
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            batch.put(
                Self::inode_key(to_dirid),
                &bincode::serialize(&to_dir).map_err(|_| nfsstat3::NFS3ERR_IO)?,
            );

            self.db
                .write_with_options(
                    batch,
                    &WriteOptions {
                        await_durable: false,
                    },
                )
                .await
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;

            Ok(())
        }
    }

    pub async fn process_setattr(
        &self,
        auth: &AuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        debug!("process_setattr: id={}, setattr={:?}", id, setattr);
        let lock = self.get_inode_lock(id);
        let _guard = lock.lock().await;
        let mut inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        // Check execute permission on all parent directories
        self.check_parent_execute_permissions(id, &creds).await?;

        // Check permissions for various operations
        // For chmod (mode change), must be owner
        if matches!(setattr.mode, set_mode3::mode(_)) {
            check_ownership(&inode, &creds)?;
        }

        // For chown/chgrp, must be root (or owner with restrictions)
        // Note: If both uid and gid are not being changed (Void), allow the operation
        let changing_uid = matches!(setattr.uid, set_uid3::uid(_));
        let changing_gid = matches!(setattr.gid, set_gid3::gid(_));

        if (changing_uid || changing_gid) && creds.uid != 0 {
            // Non-root users cannot change uid
            if changing_uid {
                return Err(nfsstat3::NFS3ERR_PERM);
            }

            // Non-root users can only change group if they own the file and are member of both old and new groups
            if let set_gid3::gid(new_gid) = setattr.gid {
                check_ownership(&inode, &creds)?;

                // Get current gid of the inode
                let current_gid = match &inode {
                    Inode::File(f) => f.gid,
                    Inode::Directory(d) => d.gid,
                    Inode::Symlink(s) => s.gid,
                    Inode::Fifo(s)
                    | Inode::Socket(s)
                    | Inode::CharDevice(s)
                    | Inode::BlockDevice(s) => s.gid,
                };

                // Check if user is member of both current and new group
                if !creds.is_member_of_group(current_gid) || !creds.is_member_of_group(new_gid) {
                    return Err(nfsstat3::NFS3ERR_PERM);
                }
            }
        }

        // For setting times, check can_set_times
        match setattr.mtime {
            set_mtime::SET_TO_CLIENT_TIME(_) => {
                can_set_times(&inode, &creds, false)?;
            }
            set_mtime::SET_TO_SERVER_TIME => {
                can_set_times(&inode, &creds, true)?;
            }
            set_mtime::DONT_CHANGE => {}
        }

        if matches!(setattr.size, set_size3::size(_)) {
            check_access(&inode, &creds, AccessMode::Write)?;
        }

        match &mut inode {
            Inode::File(file) => {
                if let set_size3::size(new_size) = setattr.size {
                    let old_size = file.size;
                    if new_size != old_size {
                        file.size = new_size;
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        let mut batch = WriteBatch::new();

                        if new_size < old_size {
                            let old_chunks = old_size.div_ceil(CHUNK_SIZE as u64) as usize;
                            let new_chunks = new_size.div_ceil(CHUNK_SIZE as u64) as usize;

                            for chunk_idx in new_chunks..old_chunks {
                                let key = Self::chunk_key_by_index(id, chunk_idx);
                                batch.delete(key);
                            }

                            if new_size > 0 && new_size % CHUNK_SIZE as u64 != 0 {
                                let last_chunk_idx = new_chunks - 1;
                                let last_chunk_size = (new_size % CHUNK_SIZE as u64) as usize;

                                let key = Self::chunk_key_by_index(id, last_chunk_idx);
                                if let Some(old_chunk_data) =
                                    self.db.get(&key).await.map_err(|_| nfsstat3::NFS3ERR_IO)?
                                {
                                    let mut new_chunk_data = vec![0u8; last_chunk_size];
                                    new_chunk_data.copy_from_slice(
                                        &old_chunk_data
                                            [..last_chunk_size.min(old_chunk_data.len())],
                                    );
                                    batch.put(key, &new_chunk_data);
                                }
                            }
                        } else if new_size > old_size && old_size > 0 {
                            let last_old_chunk_idx = ((old_size - 1) / CHUNK_SIZE as u64) as usize;
                            let last_old_chunk_end = (old_size % CHUNK_SIZE as u64) as usize;

                            if last_old_chunk_end > 0 {
                                let key = Self::chunk_key_by_index(id, last_old_chunk_idx);
                                if let Some(old_chunk_data) =
                                    self.db.get(&key).await.map_err(|_| nfsstat3::NFS3ERR_IO)?
                                {
                                    let new_chunk_size = if (last_old_chunk_idx + 1) * CHUNK_SIZE
                                        <= new_size as usize
                                    {
                                        CHUNK_SIZE
                                    } else {
                                        (new_size % CHUNK_SIZE as u64) as usize
                                    };

                                    if new_chunk_size > old_chunk_data.len() {
                                        let mut extended_chunk = vec![0u8; new_chunk_size];
                                        extended_chunk[..old_chunk_data.len()]
                                            .copy_from_slice(&old_chunk_data);
                                        batch.put(key, &extended_chunk);
                                    }
                                }
                            }
                        }

                        let inode_key = Self::inode_key(id);
                        let inode_data =
                            bincode::serialize(&inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        batch.put(inode_key, &inode_data);

                        self.db
                            .write_with_options(
                                batch,
                                &WriteOptions {
                                    await_durable: false,
                                },
                            )
                            .await
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                        return Ok(inode.to_fattr3(id));
                    }
                }

                if let set_mode3::mode(mode) = setattr.mode {
                    debug!("Setting file mode from {} to {:#o}", file.mode, mode);
                    file.mode = validate_mode(mode);
                    // POSIX: If non-root user sets mode with setgid bit and doesn't belong to file's group, clear setgid
                    if creds.uid != 0
                        && (file.mode & 0o2000) != 0
                        && !creds.is_member_of_group(file.gid)
                    {
                        file.mode &= !0o2000;
                    }
                }
                if let set_uid3::uid(uid) = setattr.uid {
                    file.uid = uid;
                    if creds.uid != 0 {
                        file.mode &= !0o4000;
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    file.gid = gid;
                    if creds.uid != 0 {
                        // Clear both SUID and SGID bits when non-root changes ownership
                        file.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    set_atime::SET_TO_CLIENT_TIME(t) => {
                        file.atime = t.seconds as u64;
                        file.atime_nsec = t.nseconds;
                    }
                    set_atime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.atime = now_sec;
                        file.atime_nsec = now_nsec;
                    }
                    set_atime::DONT_CHANGE => {}
                }
                match setattr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(t) => {
                        file.mtime = t.seconds as u64;
                        file.mtime_nsec = t.nseconds;
                    }
                    set_mtime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                    }
                    set_mtime::DONT_CHANGE => {}
                }

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || matches!(setattr.uid, set_uid3::uid(_))
                    || matches!(setattr.gid, set_gid3::gid(_))
                    || matches!(setattr.size, set_size3::size(_))
                    || matches!(
                        setattr.atime,
                        set_atime::SET_TO_CLIENT_TIME(_) | set_atime::SET_TO_SERVER_TIME
                    )
                    || matches!(
                        setattr.mtime,
                        set_mtime::SET_TO_CLIENT_TIME(_) | set_mtime::SET_TO_SERVER_TIME
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    file.ctime = now_sec;
                    file.ctime_nsec = now_nsec;
                }
                let (now_sec, now_nsec) = get_current_time();
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Directory(dir) => {
                if let set_mode3::mode(mode) = setattr.mode {
                    debug!("Setting directory mode from {} to {:#o}", dir.mode, mode);
                    dir.mode = validate_mode(mode);
                    // POSIX: If non-root user sets mode with setgid bit and doesn't belong to directory's group, clear setgid
                    if creds.uid != 0
                        && (dir.mode & 0o2000) != 0
                        && !creds.is_member_of_group(dir.gid)
                    {
                        dir.mode &= !0o2000;
                    }
                }
                if let set_uid3::uid(uid) = setattr.uid {
                    dir.uid = uid;
                    if creds.uid != 0 {
                        dir.mode &= !0o4000;
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    dir.gid = gid;
                    if creds.uid != 0 {
                        // Clear both SUID and SGID bits when non-root changes ownership
                        dir.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    set_atime::SET_TO_CLIENT_TIME(t) => {
                        dir.atime = t.seconds as u64;
                        dir.atime_nsec = t.nseconds;
                    }
                    set_atime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.atime = now_sec;
                        dir.atime_nsec = now_nsec;
                    }
                    set_atime::DONT_CHANGE => {}
                }
                match setattr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(t) => {
                        dir.mtime = t.seconds as u64;
                        dir.mtime_nsec = t.nseconds;
                    }
                    set_mtime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.mtime = now_sec;
                        dir.mtime_nsec = now_nsec;
                    }
                    set_mtime::DONT_CHANGE => {}
                }

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || matches!(setattr.uid, set_uid3::uid(_))
                    || matches!(setattr.gid, set_gid3::gid(_))
                    || matches!(
                        setattr.atime,
                        set_atime::SET_TO_CLIENT_TIME(_) | set_atime::SET_TO_SERVER_TIME
                    )
                    || matches!(
                        setattr.mtime,
                        set_mtime::SET_TO_CLIENT_TIME(_) | set_mtime::SET_TO_SERVER_TIME
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    dir.ctime = now_sec;
                    dir.ctime_nsec = now_nsec;
                }
            }
            Inode::Symlink(symlink) => {
                if let set_mode3::mode(mode) = setattr.mode {
                    symlink.mode = validate_mode(mode);
                }
                if let set_uid3::uid(uid) = setattr.uid {
                    symlink.uid = uid;
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    symlink.gid = gid;
                }
                match setattr.atime {
                    set_atime::SET_TO_CLIENT_TIME(t) => {
                        symlink.atime = t.seconds as u64;
                        symlink.atime_nsec = t.nseconds;
                    }
                    set_atime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.atime = now_sec;
                        symlink.atime_nsec = now_nsec;
                    }
                    set_atime::DONT_CHANGE => {}
                }
                match setattr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(t) => {
                        symlink.mtime = t.seconds as u64;
                        symlink.mtime_nsec = t.nseconds;
                    }
                    set_mtime::SET_TO_SERVER_TIME => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.mtime = now_sec;
                        symlink.mtime_nsec = now_nsec;
                    }
                    set_mtime::DONT_CHANGE => {}
                }

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || matches!(setattr.uid, set_uid3::uid(_))
                    || matches!(setattr.gid, set_gid3::gid(_))
                    || matches!(
                        setattr.atime,
                        set_atime::SET_TO_CLIENT_TIME(_) | set_atime::SET_TO_SERVER_TIME
                    )
                    || matches!(
                        setattr.mtime,
                        set_mtime::SET_TO_CLIENT_TIME(_) | set_mtime::SET_TO_SERVER_TIME
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    symlink.ctime = now_sec;
                    symlink.ctime_nsec = now_nsec;
                }
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if let set_mode3::mode(mode) = setattr.mode {
                    special.mode = validate_mode(mode);
                }
                if let set_uid3::uid(uid) = setattr.uid {
                    special.uid = uid;
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    special.gid = gid;
                }
                match setattr.atime {
                    set_atime::SET_TO_CLIENT_TIME(t) => {
                        special.atime = t.seconds as u64;
                        special.atime_nsec = t.nseconds;
                    }
                    set_atime::SET_TO_SERVER_TIME => {
                        let (sec, nsec) = get_current_time();
                        special.atime = sec;
                        special.atime_nsec = nsec;
                    }
                    _ => {}
                }
                match setattr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(t) => {
                        special.mtime = t.seconds as u64;
                        special.mtime_nsec = t.nseconds;
                    }
                    set_mtime::SET_TO_SERVER_TIME => {
                        let (sec, nsec) = get_current_time();
                        special.mtime = sec;
                        special.mtime_nsec = nsec;
                    }
                    _ => {}
                }

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || matches!(setattr.uid, set_uid3::uid(_))
                    || matches!(setattr.gid, set_gid3::gid(_))
                    || matches!(
                        setattr.atime,
                        set_atime::SET_TO_CLIENT_TIME(_) | set_atime::SET_TO_SERVER_TIME
                    )
                    || matches!(
                        setattr.mtime,
                        set_mtime::SET_TO_CLIENT_TIME(_) | set_mtime::SET_TO_SERVER_TIME
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    special.ctime = now_sec;
                    special.ctime_nsec = now_nsec;
                }
            }
        }

        self.save_inode(id, &inode).await?;
        Ok(inode.to_fattr3(id))
    }

    pub async fn process_symlink(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        linkname: &[u8],
        target: &[u8],
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "process_symlink: dirid={}, linkname={:?}, target={:?}",
            dirid, linkname, target
        );

        let linkname_str = String::from_utf8_lossy(linkname);

        let lock = self.get_inode_lock(dirid);
        let _guard = lock.lock().await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        // Get parent directory's uid/gid as defaults before the mutable borrow
        let (_default_uid, _default_gid) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid),
            _ => (65534, 65534),
        };

        let dir = match &mut dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };

        // Check if entry already exists
        let entry_key = Self::dir_entry_key(dirid, &linkname_str);
        if self
            .db
            .get(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .is_some()
        {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        let new_id = self.allocate_inode().await?;

        let umask = get_umask();
        let mode = if let set_mode3::mode(m) = attr.mode {
            (m & !umask) | 0o120000
        } else {
            0o120777 & !umask
        };

        let uid = if let set_uid3::uid(u) = attr.uid {
            u
        } else {
            auth.uid
        };

        let gid = if let set_gid3::gid(g) = attr.gid {
            g
        } else {
            auth.gid
        };

        let (now_sec, now_nsec) = get_current_time();
        let symlink_inode = Inode::Symlink(SymlinkInode {
            target: target.to_vec(),
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode,
            uid,
            gid,
            parent: dirid,
            nlink: 1,
        });

        let mut batch = WriteBatch::new();

        let inode_key = Self::inode_key(new_id);
        let inode_data = bincode::serialize(&symlink_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch.put(inode_key, &inode_data);

        // Add directory entry (for lookup by name)
        batch.put(entry_key, new_id.to_le_bytes());

        // Add directory scan entry (for efficient readdir)
        let scan_key = Self::dir_scan_key(dirid, new_id, &linkname_str);
        batch.put(scan_key, new_id.to_le_bytes());

        // Update directory metadata
        dir.entry_count += 1;
        let (now_sec, now_nsec) = get_current_time();
        dir.mtime = now_sec;
        dir.mtime_nsec = now_nsec;
        dir.ctime = now_sec;
        dir.ctime_nsec = now_nsec;

        // Persist the counter
        let counter_key = Self::counter_key();
        let next_id = self.next_inode_id.load(Ordering::SeqCst);
        batch.put(counter_key, next_id.to_le_bytes());

        let dir_key = Self::inode_key(dirid);
        let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch.put(dir_key, &dir_data);

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        Ok((new_id, symlink_inode.to_fattr3(new_id)))
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
        let inode = self.load_inode(id).await?;

        // Check read permission
        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    return Ok((vec![], true));
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
                            db.get(&key).await.map_err(|_| nfsstat3::NFS3ERR_IO)?;
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

                Ok((result, end >= file.size))
            }
            _ => Err(nfsstat3::NFS3ERR_ISDIR),
        }
    }

    pub async fn process_mknod(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
        ftype: ftype3,
        attr: &sattr3,
        rdev: Option<(u32, u32)>, // For device files
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let filename_str = String::from_utf8_lossy(filename);
        debug!(
            "process_mknod: dirid={}, filename={}, ftype={:?}",
            dirid, filename_str, ftype
        );

        let lock = self.get_inode_lock(dirid);
        let _guard = lock.lock().await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid, _parent_mode) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid, d.mode),
            _ => {
                debug!("Parent is not a directory");
                return Err(nfsstat3::NFS3ERR_NOTDIR);
            }
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = filename_str.to_string();
                let entry_key = Self::dir_entry_key(dirid, &name);

                if self
                    .db
                    .get(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                    .is_some()
                {
                    debug!("File already exists");
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }

                let special_id = self.allocate_inode().await?;
                let (now_sec, now_nsec) = get_current_time();

                let umask = get_umask();
                let base_mode = match ftype {
                    ftype3::NF3FIFO => 0o666,
                    ftype3::NF3CHR | ftype3::NF3BLK => 0o666,
                    ftype3::NF3SOCK => 0o666,
                    _ => return Err(nfsstat3::NFS3ERR_INVAL),
                };

                let mut final_mode = base_mode & !umask;
                if let set_mode3::mode(m) = attr.mode {
                    final_mode = validate_mode(m) & !umask;
                }

                let special_inode = SpecialInode {
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
                    rdev,
                };

                let inode = match ftype {
                    ftype3::NF3FIFO => Inode::Fifo(special_inode),
                    ftype3::NF3CHR => Inode::CharDevice(special_inode),
                    ftype3::NF3BLK => Inode::BlockDevice(special_inode),
                    ftype3::NF3SOCK => Inode::Socket(special_inode),
                    _ => return Err(nfsstat3::NFS3ERR_INVAL),
                };

                let mut batch = WriteBatch::new();

                let special_inode_key = Self::inode_key(special_id);
                let special_inode_data =
                    bincode::serialize(&inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(special_inode_key, &special_inode_data);

                batch.put(entry_key, special_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, special_id, &name);
                batch.put(scan_key, special_id.to_le_bytes());

                dir.entry_count += 1;
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;

                let dir_inode_key = Self::inode_key(dirid);
                let dir_inode_data =
                    bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(dir_inode_key, &dir_inode_data);

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                Ok((special_id, inode.to_fattr3(special_id)))
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    pub async fn process_link(
        &self,
        auth: &AuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &[u8],
    ) -> Result<(), nfsstat3> {
        let linkname_str = String::from_utf8_lossy(linkname);
        debug!(
            "process_link: fileid={}, linkdirid={}, linkname={}",
            fileid, linkdirid, linkname_str
        );

        // Get locks in a consistent order to avoid deadlocks
        let (lock1, lock2) = if fileid < linkdirid {
            (self.get_inode_lock(fileid), self.get_inode_lock(linkdirid))
        } else {
            (self.get_inode_lock(linkdirid), self.get_inode_lock(fileid))
        };

        let _guard1 = lock1.lock().await;
        let _guard2 = lock2.lock().await;

        // Check that the link directory exists and is a directory
        let link_dir_inode = self.load_inode(linkdirid).await?;
        let creds = Credentials::from_auth_context(auth);

        check_access(&link_dir_inode, &creds, AccessMode::Write)?;
        check_access(&link_dir_inode, &creds, AccessMode::Execute)?;

        let mut link_dir = match link_dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };

        // Check that the target file exists
        let mut file_inode = self.load_inode(fileid).await?;

        // Don't allow hard links to directories
        if matches!(file_inode, Inode::Directory(_)) {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        // Don't allow hard links to symlinks (they're typically not allowed)
        if matches!(file_inode, Inode::Symlink(_)) {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        let name = linkname_str.to_string();
        let entry_key = Self::dir_entry_key(linkdirid, &name);

        // Check if the name already exists
        if self
            .db
            .get(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .is_some()
        {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        let mut batch = WriteBatch::new();
        batch.put(entry_key, fileid.to_le_bytes());

        let scan_key = Self::dir_scan_key(linkdirid, fileid, &name);
        batch.put(scan_key, fileid.to_le_bytes());

        let (now_sec, now_nsec) = get_current_time();
        match &mut file_inode {
            Inode::File(file) => {
                file.nlink += 1;
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                special.nlink += 1;
                special.ctime = now_sec;
                special.ctime_nsec = now_nsec;
            }
            _ => unreachable!(), // We already filtered out directories and symlinks
        }

        // Save updated inode
        let file_inode_key = Self::inode_key(fileid);
        let file_inode_data = bincode::serialize(&file_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch.put(file_inode_key, &file_inode_data);

        // Update directory
        link_dir.entry_count += 1;
        link_dir.mtime = now_sec;
        link_dir.mtime_nsec = now_nsec;
        link_dir.ctime = now_sec;
        link_dir.ctime_nsec = now_nsec;

        let dir_inode_key = Self::inode_key(linkdirid);
        let dir_inode_data =
            bincode::serialize(&Inode::Directory(link_dir)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch.put(dir_inode_key, &dir_inode_data);

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        Ok(())
    }

    pub async fn process_readdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        debug!(
            "process_readdir: dirid={}, start_after={}, max_entries={}",
            dirid, start_after, max_entries
        );
        let dir_inode = self.load_inode(dirid).await?;

        // Check read permission on directory
        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Read)?;

        match &dir_inode {
            Inode::Directory(dir) => {
                let mut entries = Vec::new();

                if start_after == 0 {
                    debug!("readdir: adding . entry for current directory");
                    entries.push(DirEntry {
                        fileid: dirid,
                        name: b".".to_vec().into(),
                        attr: dir_inode.to_fattr3(dirid),
                    });

                    debug!("readdir: adding .. entry for parent directory");
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        // Root directory, use self
                        dir_inode.to_fattr3(dirid)
                    } else {
                        // Load parent directory attributes
                        match self.load_inode(parent_id).await {
                            Ok(parent_inode) => parent_inode.to_fattr3(parent_id),
                            Err(_) => dir_inode.to_fattr3(dirid), // Fallback to self on error
                        }
                    };
                    entries.push(DirEntry {
                        fileid: parent_id,
                        name: b"..".to_vec().into(),
                        attr: parent_attr,
                    });
                }

                // Use dirscan index for efficient pagination
                let scan_prefix = Self::dir_scan_prefix(dirid);
                let start_key = if start_after == 0 {
                    scan_prefix.clone()
                } else {
                    // Start right after the given inode (formatted with leading zeros)
                    format!("dirscan:{}/{:020}", dirid, start_after + 1)
                };
                let end_key = format!("dirscan:{}0", dirid);

                let mut iter = self
                    .db
                    .scan(start_key..end_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                // First, collect directory entries up to max_entries
                let mut dir_entries = Vec::new();
                while let Some(kv) = iter.next().await.map_err(|_| nfsstat3::NFS3ERR_IO)? {
                    if dir_entries.len() >= max_entries - entries.len() {
                        debug!("readdir: reached max_entries limit");
                        break;
                    }

                    let key = kv.key;
                    let _value = kv.value;
                    let key_str = String::from_utf8_lossy(&key);

                    // Extract the inode ID and filename from the key
                    // Key format: dirscan:{dir_id}/{inode_id:020}/{filename}
                    if let Some(suffix) = key_str.strip_prefix(&scan_prefix) {
                        // Split by '/' to get inode_id and filename
                        if let Some(slash_pos) = suffix.find('/') {
                            let inode_str = &suffix[..slash_pos];
                            let filename = &suffix[slash_pos + 1..];

                            if let Ok(inode_id) = inode_str.parse::<u64>() {
                                debug!("readdir: found entry {} (inode {})", filename, inode_id);
                                dir_entries.push((inode_id, filename.as_bytes().to_vec()));
                            }
                        }
                    }
                }

                const BUFFER_SIZE: usize = 16;
                let inode_futures =
                    stream::iter(dir_entries.into_iter()).map(|(inode_id, name)| async move {
                        debug!("readdir: loading inode {} for entry", inode_id);
                        let inode = self.load_inode(inode_id).await?;
                        debug!("readdir: loaded inode {} successfully", inode_id);
                        Ok::<(u64, Vec<u8>, Inode), nfsstat3>((inode_id, name, inode))
                    });

                let loaded_entries: Vec<_> = inode_futures
                    .buffered(BUFFER_SIZE)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                // Add the loaded entries to results
                for (inode_id, name, inode) in loaded_entries {
                    entries.push(DirEntry {
                        fileid: inode_id,
                        name: name.into(),
                        attr: inode.to_fattr3(inode_id),
                    });
                    debug!("readdir: added entry to results");
                }

                // Check if we've reached the end by seeing if we got fewer entries than requested
                let end = entries.len() < max_entries;

                let result = ReadDirResult { end, entries };
                debug!(
                    "readdir: returning {} entries, end={}",
                    result.entries.len(),
                    result.end
                );
                Ok(result)
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::SlateDbFs;
    use crate::test_helpers::test_helpers::test_auth;
    use nfsserve::nfs::{ftype3, set_atime, set_mtime};

    #[tokio::test]
    async fn test_process_create_file() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let attr = sattr3 {
            mode: set_mode3::mode(0o644),
            uid: set_uid3::uid(1000),
            gid: set_gid3::gid(1000),
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let (file_id, fattr) = fs
            .process_create(&test_auth(), 0, b"test.txt", attr)
            .await
            .unwrap();

        assert!(file_id > 0);
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.size, 0);

        // Check that the file was added to the directory
        let entry_key = SlateDbFs::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get(&entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_create_file_already_exists() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let attr = sattr3::default();

        let _ = fs
            .process_create(&test_auth(), 0, b"test.txt", attr)
            .await
            .unwrap();

        let result = fs.process_create(&test_auth(), 0, b"test.txt", attr).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_EXIST)));
    }

    #[tokio::test]
    async fn test_process_mkdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        assert!(dir_id > 0);
        assert_eq!(fattr.mode, 0o755);
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));

        let new_dir_inode = fs.load_inode(dir_id).await.unwrap();
        match new_dir_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_mkdir_with_custom_attrs() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Test with custom mode
        let custom_attrs = sattr3 {
            mode: set_mode3::mode(0o700),
            uid: set_uid3::uid(1001),
            gid: set_gid3::gid(1001),
            size: set_size3::Void,
            atime: set_atime::SET_TO_CLIENT_TIME(nfstime3 {
                seconds: 1234567890,
                nseconds: 0,
            }),
            mtime: set_mtime::SET_TO_CLIENT_TIME(nfstime3 {
                seconds: 1234567890,
                nseconds: 0,
            }),
        };

        let (_dir_id, fattr) = fs
            .process_mkdir(&test_auth(), 0, b"customdir", &custom_attrs)
            .await
            .unwrap();

        // Check that attributes were applied correctly
        assert_eq!(fattr.mode & 0o777, 0o700, "Custom mode should be applied");
        assert_eq!(fattr.uid, 1001, "Custom uid should be applied");
        assert_eq!(fattr.gid, 1001, "Custom gid should be applied");
        assert_eq!(
            fattr.atime.seconds, 1234567890,
            "Custom atime should be applied"
        );
        assert_eq!(
            fattr.mtime.seconds, 1234567890,
            "Custom mtime should be applied"
        );
    }

    #[tokio::test]
    async fn test_process_write_and_read() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .process_write(&test_auth(), file_id, 0, data)
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_write_partial_chunks() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.process_write(&test_auth(), file_id, 0, &data1)
            .await
            .unwrap();

        let data2 = vec![b'B'; 50];
        fs.process_write(&test_auth(), file_id, 50, &data2)
            .await
            .unwrap();

        let (read_data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_chunks() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"bigfile.txt", sattr3::default())
            .await
            .unwrap();

        let chunk_size = CHUNK_SIZE;
        let data = vec![b'X'; chunk_size * 2 + 1024];

        let fattr = fs
            .process_write(&test_auth(), file_id, 0, &data)
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_remove_file() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, b"some data")
            .await
            .unwrap();

        fs.process_remove(&test_auth(), 0, b"test.txt")
            .await
            .unwrap();

        // Check that the file was removed from the directory
        let entry_key = SlateDbFs::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get(&entry_key).await.unwrap();
        assert!(entry_data.is_none());

        let result = fs.load_inode(file_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_remove_empty_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        fs.process_remove(&test_auth(), 0, b"testdir")
            .await
            .unwrap();

        let result = fs.load_inode(dir_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_remove_non_empty_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        fs.process_create(&test_auth(), dir_id, b"file.txt", sattr3::default())
            .await
            .unwrap();

        let result = fs.process_remove(&test_auth(), 0, b"testdir").await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOTEMPTY)));
    }

    #[tokio::test]
    async fn test_process_rename_same_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"old.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_rename(&test_auth(), 0, b"old.txt", 0, b"new.txt")
            .await
            .unwrap();

        // Check old entry is gone and new entry exists
        let old_entry_key = SlateDbFs::dir_entry_key(0, "old.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        let new_entry_key = SlateDbFs::dir_entry_key(0, "new.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_rename_replace_existing() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create two files
        let (file1_id, _) = fs
            .process_create(&test_auth(), 0, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_write(&test_auth(), file1_id, 0, b"content1")
            .await
            .unwrap();

        let (file2_id, _) = fs
            .process_create(&test_auth(), 0, b"file2.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_write(&test_auth(), file2_id, 0, b"content2")
            .await
            .unwrap();

        fs.process_rename(&test_auth(), 0, b"file1.txt", 0, b"file2.txt")
            .await
            .unwrap();

        // Check that file1.txt no longer exists
        let old_entry_key = SlateDbFs::dir_entry_key(0, "file1.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        // Check that file2.txt exists and has file1's content
        let new_entry_key = SlateDbFs::dir_entry_key(0, "file2.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file1_id);

        // Verify content
        let (read_data, _) = fs
            .process_read_file(&test_auth(), file1_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data, b"content1");

        // Check that the original file2 inode is gone
        let result = fs.load_inode(file2_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_rename_across_directories() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir1_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"dir1", &sattr3::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"dir2", &sattr3::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), dir1_id, b"file.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_rename(&test_auth(), dir1_id, b"file.txt", dir2_id, b"moved.txt")
            .await
            .unwrap();

        // Check file removed from dir1
        let old_entry_key = SlateDbFs::dir_entry_key(dir1_id, "file.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        // Check file added to dir2
        let new_entry_key = SlateDbFs::dir_entry_key(dir2_id, "moved.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);

        // Check entry counts
        let dir1_inode = fs.load_inode(dir1_id).await.unwrap();
        match dir1_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }

        let dir2_inode = fs.load_inode(dir2_id).await.unwrap();
        match dir2_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 1);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_rename_directory_entry_count() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a directory with two files
        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), dir_id, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), dir_id, b"file2.txt", sattr3::default())
            .await
            .unwrap();

        // Check initial entry count
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 2),
            _ => panic!("Should be a directory"),
        }

        fs.process_rename(&test_auth(), dir_id, b"file1.txt", dir_id, b"file2.txt")
            .await
            .unwrap();

        // Check that entry count decreased by 1
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 1),
            _ => panic!("Should be a directory"),
        }

        fs.process_remove(&test_auth(), dir_id, b"file2.txt")
            .await
            .unwrap();

        // Directory should now be empty and removable
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 0),
            _ => panic!("Should be a directory"),
        }

        // Should be able to remove the empty directory
        fs.process_remove(&test_auth(), 0, b"testdir")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_setattr_file_size() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, &vec![b'A'; 1000])
            .await
            .unwrap();

        let setattr = sattr3 {
            size: set_size3::size(500),
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .process_setattr(&test_auth(), file_id, setattr)
            .await
            .unwrap();
        assert_eq!(fattr.size, 500);

        let (read_data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(read_data.len(), 500);
    }

    #[tokio::test]
    async fn test_process_symlink() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let target = b"/path/to/target";
        let attr = sattr3::default();

        let (link_id, fattr) = fs
            .process_symlink(&test_auth(), 0, b"link", target, attr)
            .await
            .unwrap();

        assert!(link_id > 0);
        assert!(matches!(fattr.ftype, ftype3::NF3LNK));
        assert_eq!(fattr.size, target.len() as u64);

        let link_inode = fs.load_inode(link_id).await.unwrap();
        match link_inode {
            Inode::Symlink(symlink) => {
                assert_eq!(symlink.target, target);
            }
            _ => panic!("Should be a symlink"),
        }
    }

    #[tokio::test]
    async fn test_process_readdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        fs.process_create(&test_auth(), 0, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), 0, b"file2.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_mkdir(&test_auth(), 0, b"dir1", &sattr3::default())
            .await
            .unwrap();

        let result = fs.process_readdir(&test_auth(), 0, 0, 10).await.unwrap();

        assert!(result.end);
        assert_eq!(result.entries.len(), 5);

        assert_eq!(result.entries[0].name.0, b".");
        assert_eq!(result.entries[1].name.0, b"..");

        let names: Vec<&[u8]> = result.entries[2..]
            .iter()
            .map(|e| e.name.0.as_ref())
            .collect();
        assert!(names.contains(&b"file1.txt".as_ref()));
        assert!(names.contains(&b"file2.txt".as_ref()));
        assert!(names.contains(&b"dir1".as_ref()));
    }

    #[tokio::test]
    async fn test_process_readdir_pagination() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        for i in 0..10 {
            fs.process_create(
                &test_auth(),
                0,
                format!("file{}.txt", i).as_bytes(),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        let result1 = fs.process_readdir(&test_auth(), 0, 0, 5).await.unwrap();
        assert!(!result1.end);
        assert_eq!(result1.entries.len(), 5);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .process_readdir(&test_auth(), 0, last_id, 10)
            .await
            .unwrap();
        assert!(result2.end);
    }

    #[tokio::test]
    async fn test_process_rename_prevent_directory_cycles() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c
        let (a_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"a", &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_auth(), a_id, b"b", &sattr3::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_auth(), b_id, b"c", &sattr3::default())
            .await
            .unwrap();

        // Test 1: Try to rename /a into /a/b (direct descendant)
        let result = fs
            .process_rename(&test_auth(), 0, b"a", b_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 2: Try to rename /a into /a/b/c (deeper descendant)
        let result = fs
            .process_rename(&test_auth(), 0, b"a", c_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 3: Try to rename /a/b into /a/b/c (moving into immediate child)
        let result = fs
            .process_rename(&test_auth(), a_id, b"b", c_id, b"b_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 4: Valid rename - moving /a/b/c to root
        let result = fs
            .process_rename(&test_auth(), b_id, b"c", 0, b"c_moved")
            .await;
        assert!(result.is_ok());

        // Test 5: Valid rename - moving a file (not a directory) should work
        let (_file_id, _) = fs
            .process_create(&test_auth(), a_id, b"file.txt", sattr3::default())
            .await
            .unwrap();
        let result = fs
            .process_rename(&test_auth(), a_id, b"file.txt", b_id, b"file_moved.txt")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_ancestor_of() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c/d
        let (a_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"a", &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_auth(), a_id, b"b", &sattr3::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_auth(), b_id, b"c", &sattr3::default())
            .await
            .unwrap();
        let (d_id, _) = fs
            .process_mkdir(&test_auth(), c_id, b"d", &sattr3::default())
            .await
            .unwrap();

        // Test ancestry relationships
        assert!(fs.is_ancestor_of(a_id, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(c_id, d_id).await.unwrap());

        // Test non-ancestry relationships
        assert!(!fs.is_ancestor_of(b_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, c_id).await.unwrap());

        // Test root relationships
        assert!(fs.is_ancestor_of(0, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, d_id).await.unwrap());
        assert!(!fs.is_ancestor_of(a_id, 0).await.unwrap());

        // Test self-relationships (should return true)
        assert!(fs.is_ancestor_of(a_id, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, b_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_parent_directory_execute_permissions() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"test_dir", &sattr3::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), dir_id, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, b"initial data")
            .await
            .unwrap();

        let no_exec_attrs = sattr3 {
            mode: set_mode3::mode(0o644),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.process_setattr(&test_auth(), dir_id, no_exec_attrs)
            .await
            .unwrap();

        let chmod_attrs = sattr3 {
            mode: set_mode3::mode(0o600),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let result = fs.process_setattr(&test_auth(), file_id, chmod_attrs).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let result = fs.process_read_file(&test_auth(), file_id, 0, 100).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let result = fs
            .process_write(&test_auth(), file_id, 0, b"new data")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let exec_attrs = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.process_setattr(&test_auth(), dir_id, exec_attrs)
            .await
            .unwrap();

        fs.process_setattr(&test_auth(), file_id, chmod_attrs)
            .await
            .unwrap();

        let (data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(data, b"initial data");

        fs.process_write(&test_auth(), file_id, 0, b"updated data")
            .await
            .unwrap();
    }
}
