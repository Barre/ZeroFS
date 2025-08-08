use super::common::validate_filename;
use crate::fs::cache::CacheKey;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, SpecialInode};
use crate::fs::permissions::{
    AccessMode, Credentials, can_set_times, check_access, check_ownership, validate_mode,
};
use crate::fs::types::{
    FileAttributes, FileType, InodeWithId, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid,
};
use crate::fs::{CHUNK_SIZE, InodeId, ZeroFS, get_current_time};
use slatedb::config::WriteOptions;
use tracing::debug;

impl ZeroFS {
    pub async fn process_setattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        setattr: &SetAttributes,
    ) -> Result<FileAttributes, FsError> {
        debug!("process_setattr: id={}, setattr={:?}", id, setattr);
        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.load_inode(id).await?;

        self.check_parent_execute_permissions(id, creds).await?;

        // For chmod (mode change), must be owner
        if matches!(setattr.mode, SetMode::Set(_)) {
            check_ownership(&inode, creds)?;
        }

        // For chown/chgrp, must be root (or owner with restrictions)
        let changing_uid = matches!(&setattr.uid, SetUid::Set(_));
        let changing_gid = matches!(&setattr.gid, SetGid::Set(_));

        if (changing_uid || changing_gid) && creds.uid != 0 {
            check_ownership(&inode, creds)?;

            if let SetUid::Set(new_uid) = setattr.uid
                && new_uid != creds.uid
            {
                return Err(FsError::OperationNotPermitted);
            }

            // POSIX: Owner can change group to any group they belong to
            if let SetGid::Set(new_gid) = setattr.gid
                && !creds.is_member_of_group(new_gid)
            {
                return Err(FsError::OperationNotPermitted);
            }
        }

        match setattr.atime {
            SetTime::SetToClientTime(_) => {
                can_set_times(&inode, creds, false)?;
            }
            SetTime::SetToServerTime => {
                can_set_times(&inode, creds, true)?;
            }
            SetTime::NoChange => {}
        }
        match setattr.mtime {
            SetTime::SetToClientTime(_) => {
                can_set_times(&inode, creds, false)?;
            }
            SetTime::SetToServerTime => {
                can_set_times(&inode, creds, true)?;
            }
            SetTime::NoChange => {}
        }

        if matches!(setattr.size, SetSize::Set(_)) {
            check_access(&inode, creds, AccessMode::Write)?;
        }

        match &mut inode {
            Inode::File(file) => {
                if let SetSize::Set(new_size) = setattr.size {
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
                            // Truncation logic - delete chunks beyond new size
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
                                    .map_err(|_| FsError::IoError)?
                                {
                                    let mut new_chunk_data = vec![0u8; last_chunk_size];
                                    let copy_len = last_chunk_size.min(old_chunk_data.len());
                                    new_chunk_data[..copy_len]
                                        .copy_from_slice(&old_chunk_data[..copy_len]);
                                    batch.put_bytes(&key, &new_chunk_data);
                                }
                            }
                        } else if new_size > old_size && old_size > 0 {
                            // Extension logic - extend the last chunk if needed
                            let last_old_chunk_idx = ((old_size - 1) / CHUNK_SIZE as u64) as usize;
                            let last_old_chunk_end = (old_size % CHUNK_SIZE as u64) as usize;

                            if last_old_chunk_end > 0 {
                                let key = Self::chunk_key_by_index(id, last_old_chunk_idx);
                                if let Some(old_chunk_data) = self
                                    .db
                                    .get_bytes(&key)
                                    .await
                                    .map_err(|_| FsError::IoError)?
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
                                        batch.put_bytes(&key, &extended_chunk);
                                    }
                                }
                            }
                        }

                        let inode_key = Self::inode_key(id);
                        let inode_data = bincode::serialize(&inode)?;
                        batch.put_bytes(&inode_key, &inode_data);

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
                            .map_err(|_| FsError::IoError)?;

                        // Update in-memory statistics after successful commit
                        if let Some(update) = stats_update {
                            self.global_stats.commit_update(&update);
                        }

                        // Remove both cache entries in batch
                        self.cache
                            .remove_batch(vec![CacheKey::Metadata(id), CacheKey::SmallFile(id)])
                            .await;

                        return Ok(InodeWithId { inode: &inode, id }.into());
                    }
                }

                if let SetMode::Set(mode) = setattr.mode {
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
                if let SetUid::Set(uid) = setattr.uid {
                    file.uid = uid;
                    if creds.uid != 0 {
                        file.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    file.gid = gid;
                    // Clear SUID/SGID bits when non-root user calls chown with a gid
                    // This happens even if the gid doesn't actually change (POSIX behavior)
                    if creds.uid != 0 {
                        file.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        file.atime = t.seconds;
                        file.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.atime = now_sec;
                        file.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        file.mtime = t.seconds;
                        file.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(setattr.size, SetSize::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    file.ctime = now_sec;
                    file.ctime_nsec = now_nsec;
                }
            }
            Inode::Directory(dir) => {
                if let SetMode::Set(mode) = setattr.mode {
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
                if let SetUid::Set(uid) = setattr.uid {
                    dir.uid = uid;
                    if creds.uid != 0 {
                        dir.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    dir.gid = gid;
                    // Clear SUID/SGID bits when non-root user calls chown with a gid
                    // This happens even if the gid doesn't actually change (POSIX behavior)
                    if creds.uid != 0 {
                        dir.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        dir.atime = t.seconds;
                        dir.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.atime = now_sec;
                        dir.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        dir.mtime = t.seconds;
                        dir.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        dir.mtime = now_sec;
                        dir.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    dir.ctime = now_sec;
                    dir.ctime_nsec = now_nsec;
                }
            }
            Inode::Symlink(symlink) => {
                if let SetMode::Set(mode) = setattr.mode {
                    symlink.mode = validate_mode(mode);
                }
                if let SetUid::Set(uid) = setattr.uid {
                    symlink.uid = uid;
                    if creds.uid != 0 {
                        symlink.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    symlink.gid = gid;
                    if creds.uid != 0 {
                        symlink.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        symlink.atime = t.seconds;
                        symlink.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.atime = now_sec;
                        symlink.atime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        symlink.mtime = t.seconds;
                        symlink.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.mtime = now_sec;
                        symlink.mtime_nsec = now_nsec;
                    }
                    SetTime::NoChange => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
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
                if let SetMode::Set(mode) = setattr.mode {
                    special.mode = validate_mode(mode);
                }
                if let SetUid::Set(uid) = setattr.uid {
                    special.uid = uid;
                    if creds.uid != 0 {
                        special.mode &= !0o4000;
                    }
                }
                if let SetGid::Set(gid) = setattr.gid {
                    special.gid = gid;
                    if creds.uid != 0 {
                        special.mode &= !0o6000;
                    }
                }
                match setattr.atime {
                    SetTime::SetToClientTime(t) => {
                        special.atime = t.seconds;
                        special.atime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (sec, nsec) = get_current_time();
                        special.atime = sec;
                        special.atime_nsec = nsec;
                    }
                    _ => {}
                }
                match setattr.mtime {
                    SetTime::SetToClientTime(t) => {
                        special.mtime = t.seconds;
                        special.mtime_nsec = t.nanoseconds;
                    }
                    SetTime::SetToServerTime => {
                        let (sec, nsec) = get_current_time();
                        special.mtime = sec;
                        special.mtime_nsec = nsec;
                    }
                    _ => {}
                }

                let attribute_changed = matches!(setattr.mode, SetMode::Set(_))
                    || matches!(setattr.uid, SetUid::Set(_))
                    || matches!(setattr.gid, SetGid::Set(_))
                    || matches!(
                        setattr.atime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    )
                    || matches!(
                        setattr.mtime,
                        SetTime::SetToClientTime(_) | SetTime::SetToServerTime
                    );

                if attribute_changed {
                    let (now_sec, now_nsec) = get_current_time();
                    special.ctime = now_sec;
                    special.ctime_nsec = now_nsec;
                }
            }
        }

        self.save_inode(id, &inode).await?;
        Ok(InodeWithId { inode: &inode, id }.into())
    }

    pub async fn process_mknod(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        filename: &[u8],
        ftype: FileType,
        attr: &SetAttributes,
        rdev: Option<(u32, u32)>, // For device files
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(filename)?;

        let filename_str = String::from_utf8_lossy(filename);
        debug!(
            "process_mknod: dirid={}, filename={}, ftype={:?}",
            dirid, filename_str, ftype
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid, _parent_mode) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid, d.mode),
            _ => {
                debug!("Parent is not a directory");
                return Err(FsError::NotDirectory);
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
                    .map_err(|_| FsError::IoError)?
                    .is_some()
                {
                    debug!("File already exists");
                    return Err(FsError::Exists);
                }

                let special_id = self.allocate_inode().await?;
                let (now_sec, now_nsec) = get_current_time();

                let base_mode = match ftype {
                    FileType::Fifo => 0o666,
                    FileType::CharDevice | FileType::BlockDevice => 0o666,
                    FileType::Socket => 0o666,
                    _ => return Err(FsError::InvalidArgument),
                };

                let final_mode = if let SetMode::Set(m) = attr.mode {
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
                        SetUid::Set(u) => u,
                        _ => creds.uid,
                    },
                    gid: match attr.gid {
                        SetGid::Set(g) => g,
                        _ => creds.gid,
                    },
                    parent: dirid,
                    nlink: 1,
                    rdev,
                };

                let inode = match ftype {
                    FileType::Fifo => Inode::Fifo(special_inode),
                    FileType::CharDevice => Inode::CharDevice(special_inode),
                    FileType::BlockDevice => Inode::BlockDevice(special_inode),
                    FileType::Socket => Inode::Socket(special_inode),
                    _ => return Err(FsError::InvalidArgument),
                };

                let mut batch = self.db.new_write_batch();

                let special_inode_key = Self::inode_key(special_id);
                let special_inode_data = bincode::serialize(&inode)?;
                batch.put_bytes(&special_inode_key, &special_inode_data);

                batch.put_bytes(&entry_key, &special_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, special_id, &name);
                batch.put_bytes(&scan_key, &special_id.to_le_bytes());

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_inode_key = Self::inode_key(dirid);
                let dir_inode_data = bincode::serialize(&dir_inode)?;
                batch.put_bytes(&dir_inode_key, &dir_inode_data);

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
                    .map_err(|_| FsError::IoError)?;

                self.global_stats.commit_update(&stats_update);

                self.cache.remove(CacheKey::Metadata(dirid)).await;

                Ok((
                    special_id,
                    InodeWithId {
                        inode: &inode,
                        id: special_id,
                    }
                    .into(),
                ))
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}
