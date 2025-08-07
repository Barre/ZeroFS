use slatedb::config::WriteOptions;
use tracing::debug;
use zerofs_nfsserve::nfs::{
    fattr3, fileid3, ftype3, nfsstat3, sattr3, set_atime, set_gid3, set_mode3, set_mtime,
    set_size3, set_uid3,
};
use zerofs_nfsserve::vfs::AuthContext;

use super::common::validate_filename;
use crate::filesystem::cache::CacheKey;
use crate::filesystem::{CHUNK_SIZE, SlateDbFs, get_current_time};
use crate::filesystem::inode::{Inode, SpecialInode};
use crate::filesystem::permissions::{
    AccessMode, Credentials, can_set_times, check_access, check_ownership, validate_mode,
};

impl SlateDbFs {
    pub async fn process_setattr(
        &self,
        auth: &AuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        debug!("process_setattr: id={}, setattr={:?}", id, setattr);
        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.load_inode(id).await?;

        let creds = Credentials::from_auth_context(auth);

        self.check_parent_execute_permissions(id, &creds).await?;

        // Check permissions for various operations
        // For chmod (mode change), must be owner
        if matches!(setattr.mode, set_mode3::mode(_)) {
            check_ownership(&inode, &creds)?;
        }

        // For chown/chgrp, must be root (or owner with restrictions)
        // Note: If both uid and gid are not being changed (Void), allow the operation
        const DONT_CHANGE_ID: u32 = 0xFFFFFFFF;

        let changing_uid = match &setattr.uid {
            set_uid3::uid(uid) => *uid != DONT_CHANGE_ID,
            set_uid3::Void => false,
        };
        let changing_gid = match &setattr.gid {
            set_gid3::gid(gid) => *gid != DONT_CHANGE_ID,
            set_gid3::Void => false,
        };

        // If neither uid nor gid is being changed, skip permission checks for chown
        if (changing_uid || changing_gid) && creds.uid != 0 {
            // First check ownership - non-root users can only chown files they own
            check_ownership(&inode, &creds)?;

            // Non-root users cannot change uid to a different user
            if changing_uid {
                if let set_uid3::uid(new_uid) = setattr.uid {
                    if new_uid != DONT_CHANGE_ID && new_uid != creds.uid {
                        return Err(nfsstat3::NFS3ERR_PERM);
                    }
                }
            }

            // Non-root users can only change group if they own the file and are member of the new group
            if let set_gid3::gid(new_gid) = setattr.gid {
                if new_gid != DONT_CHANGE_ID {
                    // Check if user is member of the new group
                    // POSIX: Owner can change group to any group they belong to
                    if !creds.is_member_of_group(new_gid) {
                        return Err(nfsstat3::NFS3ERR_PERM);
                    }
                }
            }
        }

        // For setting times, check can_set_times
        match setattr.atime {
            set_atime::SET_TO_CLIENT_TIME(_) => {
                can_set_times(&inode, &creds, false)?;
            }
            set_atime::SET_TO_SERVER_TIME => {
                can_set_times(&inode, &creds, true)?;
            }
            set_atime::DONT_CHANGE => {}
        }
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

                        let mut batch = self.db.new_write_batch();

                        if new_size < old_size {
                            // NOTE: We intentionally do NOT use tombstones for deferred deletion here,
                            // unlike in process_remove. This is because truncation can be followed by
                            // file extension and writes to the same chunk positions, which would create
                            // race conditions with garbage collection. Consider this sequence:
                            // 1. Truncate 10MB -> 1MB (would create tombstone for chunks 64-640)
                            // 2. Extend to 10MB (file.size = 10MB)
                            // 3. Write at offset 9MB (creates new chunk 576)
                            // 4. GC deletes old chunk 576 from tombstone
                            // 5. Read at offset 9MB fails or returns wrong data!
                            //
                            // By deleting chunks immediately, we ensure correctness at the cost of
                            // potentially blocking the operation for large files. This is acceptable
                            // because:
                            // - Truncation is less common than removal
                            // - Correctness is more important than performance
                            // - The operation is still atomic within the write batch
                            let old_chunks = old_size.div_ceil(CHUNK_SIZE as u64) as usize;
                            let new_chunks = new_size.div_ceil(CHUNK_SIZE as u64) as usize;

                            for chunk_idx in new_chunks..old_chunks {
                                let key = Self::chunk_key_by_index(id, chunk_idx);
                                batch.delete_bytes(&key);
                            }

                            if new_size > 0 && new_size % CHUNK_SIZE as u64 != 0 {
                                let last_chunk_idx = new_chunks - 1;
                                let last_chunk_size = (new_size % CHUNK_SIZE as u64) as usize;

                                let key = Self::chunk_key_by_index(id, last_chunk_idx);
                                if let Some(old_chunk_data) = self
                                    .db
                                    .get_bytes(&key)
                                    .await
                                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                                {
                                    let mut new_chunk_data = vec![0u8; last_chunk_size];
                                    let copy_len = last_chunk_size.min(old_chunk_data.len());
                                    new_chunk_data[..copy_len]
                                        .copy_from_slice(&old_chunk_data[..copy_len]);
                                    batch
                                        .put_bytes(&key, &new_chunk_data)
                                        .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                                }
                            }
                        } else if new_size > old_size && old_size > 0 {
                            let last_old_chunk_idx = ((old_size - 1) / CHUNK_SIZE as u64) as usize;
                            let last_old_chunk_end = (old_size % CHUNK_SIZE as u64) as usize;

                            if last_old_chunk_end > 0 {
                                let key = Self::chunk_key_by_index(id, last_old_chunk_idx);
                                if let Some(old_chunk_data) = self
                                    .db
                                    .get_bytes(&key)
                                    .await
                                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
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
                                        batch
                                            .put_bytes(&key, &extended_chunk)
                                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                                    }
                                }
                            }
                        }

                        let inode_key = Self::inode_key(id);
                        let inode_data =
                            bincode::serialize(&inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
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

                        self.db
                            .write_with_options(
                                batch,
                                &WriteOptions {
                                    await_durable: false,
                                },
                            )
                            .await
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                        // Update in-memory statistics after successful commit
                        if let Some(update) = stats_update {
                            self.global_stats.commit_update(&update);
                        }

                        // Remove both cache entries in batch
                        self.cache
                            .remove_batch(vec![CacheKey::Metadata(id), CacheKey::SmallFile(id)])
                            .await;

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
                    if uid != DONT_CHANGE_ID {
                        file.uid = uid;
                        if creds.uid != 0 {
                            file.mode &= !0o4000;
                        }
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    if gid != DONT_CHANGE_ID {
                        file.gid = gid;
                        // Clear SUID/SGID bits when non-root user calls chown with a gid
                        // This happens even if the gid doesn't actually change (POSIX behavior)
                        if creds.uid != 0 {
                            file.mode &= !0o6000;
                        }
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

                let uid_changed = match &setattr.uid {
                    set_uid3::uid(uid) => *uid != DONT_CHANGE_ID,
                    _ => false,
                };
                let gid_changed = match &setattr.gid {
                    set_gid3::gid(gid) => *gid != DONT_CHANGE_ID,
                    _ => false,
                };

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || uid_changed
                    || gid_changed
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
                    if uid != DONT_CHANGE_ID {
                        dir.uid = uid;
                        if creds.uid != 0 {
                            dir.mode &= !0o4000;
                        }
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    if gid != DONT_CHANGE_ID {
                        dir.gid = gid;
                        // Clear SUID/SGID bits when non-root user calls chown with a gid
                        // This happens even if the gid doesn't actually change (POSIX behavior)
                        if creds.uid != 0 {
                            dir.mode &= !0o6000;
                        }
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

                let uid_changed = match &setattr.uid {
                    set_uid3::uid(uid) => *uid != DONT_CHANGE_ID,
                    _ => false,
                };
                let gid_changed = match &setattr.gid {
                    set_gid3::gid(gid) => *gid != DONT_CHANGE_ID,
                    _ => false,
                };

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || uid_changed
                    || gid_changed
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
                    if uid != DONT_CHANGE_ID {
                        symlink.uid = uid;
                        if creds.uid != 0 {
                            symlink.mode &= !0o4000;
                        }
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    if gid != DONT_CHANGE_ID {
                        symlink.gid = gid;
                        if creds.uid != 0 {
                            symlink.mode &= !0o6000;
                        }
                    }
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

                let uid_changed = match &setattr.uid {
                    set_uid3::uid(uid) => *uid != DONT_CHANGE_ID,
                    _ => false,
                };
                let gid_changed = match &setattr.gid {
                    set_gid3::gid(gid) => *gid != DONT_CHANGE_ID,
                    _ => false,
                };

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || uid_changed
                    || gid_changed
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
                    if uid != DONT_CHANGE_ID {
                        special.uid = uid;
                        if creds.uid != 0 {
                            special.mode &= !0o4000;
                        }
                    }
                }
                if let set_gid3::gid(gid) = setattr.gid {
                    if gid != DONT_CHANGE_ID {
                        special.gid = gid;
                        if creds.uid != 0 {
                            special.mode &= !0o6000;
                        }
                    }
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

                let uid_changed = match &setattr.uid {
                    set_uid3::uid(uid) => *uid != DONT_CHANGE_ID,
                    _ => false,
                };
                let gid_changed = match &setattr.gid {
                    set_gid3::gid(gid) => *gid != DONT_CHANGE_ID,
                    _ => false,
                };

                let attribute_changed = matches!(setattr.mode, set_mode3::mode(_))
                    || uid_changed
                    || gid_changed
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

    pub async fn process_mknod(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
        ftype: ftype3,
        attr: &sattr3,
        rdev: Option<(u32, u32)>, // For device files
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        validate_filename(filename)?;

        let filename_str = String::from_utf8_lossy(filename);
        debug!(
            "process_mknod: dirid={}, filename={}, ftype={:?}",
            dirid, filename_str, ftype
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
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
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                    .is_some()
                {
                    debug!("File already exists");
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }

                let special_id = self.allocate_inode().await?;
                let (now_sec, now_nsec) = get_current_time();

                let base_mode = match ftype {
                    ftype3::NF3FIFO => 0o666,
                    ftype3::NF3CHR | ftype3::NF3BLK => 0o666,
                    ftype3::NF3SOCK => 0o666,
                    _ => return Err(nfsstat3::NFS3ERR_INVAL),
                };

                let final_mode = if let set_mode3::mode(m) = attr.mode {
                    validate_mode(m)
                } else {
                    base_mode
                };

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

                let mut batch = self.db.new_write_batch();

                let special_inode_key = Self::inode_key(special_id);
                let special_inode_data =
                    bincode::serialize(&inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&special_inode_key, &special_inode_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                batch
                    .put_bytes(&entry_key, &special_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let scan_key = Self::dir_scan_key(dirid, special_id, &name);
                batch
                    .put_bytes(&scan_key, &special_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_inode_key = Self::inode_key(dirid);
                let dir_inode_data =
                    bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&dir_inode_key, &dir_inode_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let stats_update = self.global_stats.prepare_inode_create(special_id).await;
                self.global_stats.add_to_batch(&stats_update, &mut batch)?;

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                self.global_stats.commit_update(&stats_update);

                self.cache.remove(CacheKey::Metadata(dirid)).await;

                Ok((special_id, inode.to_fattr3(special_id)))
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
}
