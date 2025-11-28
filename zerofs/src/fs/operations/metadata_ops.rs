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
use crate::fs::{InodeId, ZeroFS, get_current_time};
use tracing::debug;

impl ZeroFS {
    pub async fn setattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        setattr: &SetAttributes,
    ) -> Result<FileAttributes, FsError> {
        debug!("setattr: id={}, setattr={:?}", id, setattr);
        let _guard = self.lock_manager.acquire_write(id).await;
        let mut inode = self.inode_store.get(id).await?;

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
                        if new_size > old_size {
                            let size_increase = new_size - old_size;
                            let (used_bytes, _) = self.global_stats.get_totals();
                            if used_bytes.saturating_add(size_increase) > self.max_bytes {
                                debug!(
                                    "Setattr size change would exceed quota: used={}, increase={}, max={}",
                                    used_bytes, size_increase, self.max_bytes
                                );
                                return Err(FsError::NoSpace);
                            }
                        }

                        file.size = new_size;
                        let (now_sec, now_nsec) = get_current_time();
                        file.mtime = now_sec;
                        file.mtime_nsec = now_nsec;
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        let mut txn = self.db.new_transaction()?;

                        self.chunk_store
                            .truncate(&mut txn, id, old_size, new_size)
                            .await;

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

                        let mut seq_guard = self.write_coordinator.allocate_sequence();
                        self.commit_transaction(txn, &mut seq_guard).await?;

                        if let Some(update) = stats_update {
                            self.global_stats.commit_update(&update);
                        }

                        self.cache.remove(CacheKey::Metadata(id));

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

        let mut txn = self.db.new_transaction()?;
        self.inode_store.save(&mut txn, id, &inode)?;
        let mut seq_guard = self.write_coordinator.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        Ok(InodeWithId { inode: &inode, id }.into())
    }

    pub async fn mknod(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        ftype: FileType,
        attr: &SetAttributes,
        rdev: Option<(u32, u32)>,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(name)?;

        debug!(
            "mknod: dirid={}, filename={}, ftype={:?}",
            dirid,
            String::from_utf8_lossy(name),
            ftype
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.inode_store.get(dirid).await?;

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
                if self.directory_store.exists(dirid, name).await? {
                    debug!("File already exists");
                    return Err(FsError::Exists);
                }

                let special_id = self.inode_store.allocate()?;
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
                    parent: Some(dirid),
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

                let mut txn = self.db.new_transaction()?;

                self.inode_store.save(&mut txn, special_id, &inode)?;
                self.directory_store.add(&mut txn, dirid, name, special_id);

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                let stats_update = self.global_stats.prepare_inode_create(special_id).await;
                self.global_stats
                    .add_to_transaction(&stats_update, &mut txn)?;

                let mut seq_guard = self.write_coordinator.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;

                self.global_stats.commit_update(&stats_update);

                self.cache.remove(CacheKey::Metadata(dirid));

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
