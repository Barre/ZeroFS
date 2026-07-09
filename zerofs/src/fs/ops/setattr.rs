//! setattr: mode/owner/size/times in one guarded update.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::permissions::{
    AccessMode, Credentials, can_set_times, check_access, check_ownership, validate_mode,
};
use crate::fs::stats;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{
    FileAttributes, InodeWithId, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid,
};
use crate::fs::{ZeroFS, get_current_time};
use ::tracing::debug;
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Attribute update behind POSIX gates: chmod needs ownership; chown needs
    /// root (an owner may chgrp within their groups); explicit timestamps need
    /// `can_set_times`. A size change goes through the extent store: shrink
    /// drops extents, growth is a sparse size bump.
    pub async fn setattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        setattr: &SetAttributes,
    ) -> Result<FileAttributes, FsError> {
        debug!(
            "setattr: id={}, setattr={:?}, creds=(uid={}, gid={}, groups={:?})",
            id,
            setattr,
            creds.uid,
            creds.gid,
            &creds.groups[..creds.groups_count]
        );
        let _guard = self.lock_manager.acquire(id).await;
        let mut inode = self.inode_store.get(id).await?;

        // For chmod (mode change), must be owner
        if matches!(setattr.mode, SetMode::Set(_)) {
            check_ownership(&inode, creds)?;
        }

        // For chown/chgrp, must be root (or owner with restrictions)
        let changing_uid = matches!(&setattr.uid, SetUid::Set(_));
        let changing_gid = matches!(&setattr.gid, SetGid::Set(_));

        if (changing_uid || changing_gid) && creds.uid != 0 {
            debug!(
                "setattr: non-root chown attempt: creds.uid={}, inode.uid={}, changing_uid={}, changing_gid={}, setattr.uid={:?}, setattr.gid={:?}",
                creds.uid,
                inode.uid(),
                changing_uid,
                changing_gid,
                setattr.uid,
                setattr.gid
            );
            check_ownership(&inode, creds)?;

            if let SetUid::Set(new_uid) = setattr.uid
                && new_uid != creds.uid
            {
                debug!(
                    "setattr: denied uid change from {} to {} by non-root user {}",
                    inode.uid(),
                    new_uid,
                    creds.uid
                );
                return Err(FsError::OperationNotPermitted);
            }

            // POSIX: Owner can change group to any group they belong to
            if let SetGid::Set(new_gid) = setattr.gid
                && !creds.is_member_of_group(new_gid)
            {
                debug!(
                    "setattr: denied gid change to {} - user {} is not a member of that group",
                    new_gid, creds.uid
                );
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

                        self.extent_store
                            .truncate(&mut txn, id, old_size, new_size)
                            .await?;

                        #[cfg(feature = "failpoints")]
                        fail_point!(fp::TRUNCATE_AFTER_EXTENTS);

                        let parent_name_for_update = file.parent.zip(file.name.clone());

                        self.inode_store.save(&mut txn, id, &inode)?;

                        #[cfg(feature = "failpoints")]
                        fail_point!(fp::TRUNCATE_AFTER_INODE);

                        if let Some((parent_id, name)) = parent_name_for_update {
                            self.directory_store
                                .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                                .await?;
                        }

                        txn.add_stats_delta(id, stats::size_delta(old_size, new_size), 0);

                        self.write_coordinator.commit(txn).await?;

                        #[cfg(feature = "failpoints")]
                        fail_point!(fp::TRUNCATE_AFTER_COMMIT);

                        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                        self.tracer.emit(
                            &self.inode_store,
                            id,
                            FileOperation::Setattr {
                                mode: match setattr.mode {
                                    SetMode::Set(m) => Some(m),
                                    SetMode::NoChange => None,
                                },
                            },
                        );

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

        if let Some(parent_id) = inode.parent()
            && let Some(name) = inode.name()
        {
            self.directory_store
                .update_inode_in_entry(&mut txn, parent_id, name, id, &inode)
                .await?;
        }

        self.write_coordinator.commit(txn).await?;

        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Setattr {
                mode: match setattr.mode {
                    SetMode::Set(m) => Some(m),
                    SetMode::NoChange => None,
                },
            },
        );

        Ok(InodeWithId { inode: &inode, id }.into())
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::types::{SetAttributes, SetSize};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_process_setattr_file_size() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(vec![b'A'; 1000]),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(500),
            ..Default::default()
        };

        let fattr = fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 500);

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(read_data.len(), 500);
    }
}
