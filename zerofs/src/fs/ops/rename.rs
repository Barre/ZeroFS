//! rename, with its idempotent variant and the directory-cycle guard.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};
use crate::fs::stats;
use crate::fs::tracing::FileOperation;
use crate::fs::types::AuthContext;
use crate::fs::{
    EXTENT_SIZE, SMALL_FILE_TOMBSTONE_THRESHOLD, ZeroFS, get_current_time, validate_filename,
};
use ::tracing::debug;
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// True when `ancestor_id` is `descendant_id` or on its parent chain: the
    /// rename cycle guard. A parentless (hardlinked) inode reports false.
    pub async fn is_ancestor_of(
        &self,
        ancestor_id: InodeId,
        descendant_id: InodeId,
    ) -> Result<bool, FsError> {
        if ancestor_id == descendant_id {
            return Ok(true);
        }

        let mut current_id = descendant_id;

        while current_id != 0 {
            let inode = self.inode_store.get(current_id).await?;
            let parent_id = inode.parent();

            // If parent is None (file is hardlinked), can't determine ancestry
            let Some(pid) = parent_id else {
                return Ok(false);
            };

            if pid == ancestor_id {
                return Ok(true);
            }

            current_id = pid;
        }

        Ok(false)
    }

    /// POSIX rename: moves the entry, replacing an existing target (a
    /// directory only over an empty one; a replaced open target defers reclaim
    /// as in `remove`). [`Self::is_ancestor_of`] blocks moving a directory
    /// under its own subtree.
    pub async fn rename(
        &self,
        auth: &AuthContext,
        from_dirid: u64,
        from_name: &[u8],
        to_dirid: u64,
        to_name: &[u8],
    ) -> Result<(), FsError> {
        self.rename_idempotent(auth, from_dirid, from_name, to_dirid, to_name, [0u8; 16])
            .await
    }

    /// `rename` tagged with an idempotency op-id: an applied retry is a no-op
    /// success instead of failing on the now-missing source. See
    /// [`Self::create_idempotent`].
    pub async fn rename_idempotent(
        &self,
        auth: &AuthContext,
        from_dirid: u64,
        from_name: &[u8],
        to_dirid: u64,
        to_name: &[u8],
        op_id: crate::dedup::OpId,
    ) -> Result<(), FsError> {
        if from_name.is_empty() || to_name.is_empty() {
            return Err(FsError::InvalidArgument);
        }

        validate_filename(from_name)?;
        validate_filename(to_name)?;

        // Replay a completed rename before resolving source and target names.
        if self
            .replay_dedup_result(&op_id, DedupResult::into_rename)?
            .is_some()
        {
            return Ok(());
        }

        if from_name == b"." || from_name == b".." {
            return Err(FsError::InvalidArgument);
        }
        if to_name == b"." || to_name == b".." {
            return Err(FsError::Exists);
        }

        if from_dirid == to_dirid && from_name == to_name {
            // No-op renames still publish a durable replay result.
            if crate::dedup::has_op_id(&op_id) {
                let mut txn = self.db.new_transaction()?;
                txn.set_dedup_result(op_id, DedupResult::Rename);
                self.write_coordinator.commit(txn).await?;
            }
            return Ok(());
        }

        debug!(
            "rename: from_dir={}, from_name={}, to_dir={}, to_name={}",
            from_dirid,
            String::from_utf8_lossy(from_name),
            to_dirid,
            String::from_utf8_lossy(to_name)
        );

        let creds = Credentials::from_auth_context(auth);

        // Look up all inode IDs without holding any locks
        let (source_inode_id, source_cookie) = match self
            .directory_store
            .get_entry_with_cookie(from_dirid, from_name)
            .await
        {
            Ok(entry) => entry,
            Err(error) => {
                if self
                    .replay_dedup_result(&op_id, DedupResult::into_rename)?
                    .is_some()
                {
                    return Ok(());
                }
                return Err(error);
            }
        };

        if to_dirid == source_inode_id {
            return Err(FsError::InvalidArgument);
        }

        let target_entry = match self
            .directory_store
            .get_entry_with_cookie(to_dirid, to_name)
            .await
        {
            Ok((id, cookie)) => Some((id, cookie)),
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };
        let target_inode_id = target_entry.map(|(id, _)| id);

        let mut all_inodes_to_lock = vec![from_dirid, source_inode_id];
        if from_dirid != to_dirid {
            all_inodes_to_lock.push(to_dirid);
        }
        if let Some(target_id) = target_inode_id {
            all_inodes_to_lock.push(target_id);
        }

        let _guards = self.lock_manager.acquire_multi(all_inodes_to_lock).await;

        // Recheck replay state after waiting for inode locks.
        if self
            .replay_dedup_result(&op_id, DedupResult::into_rename)?
            .is_some()
        {
            return Ok(());
        }

        // Re-verify inside lock that entries still point to same inodes
        let (verified_source_id, verified_source_cookie) = self
            .directory_store
            .get_entry_with_cookie(from_dirid, from_name)
            .await?;
        if verified_source_id != source_inode_id || verified_source_cookie != source_cookie {
            return Err(FsError::StaleHandle);
        }

        let verified_target_entry = match self
            .directory_store
            .get_entry_with_cookie(to_dirid, to_name)
            .await
        {
            Ok((id, cookie)) => Some((id, cookie)),
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };
        if verified_target_entry.map(|(id, _)| id) != target_inode_id {
            return Err(FsError::StaleHandle);
        }
        let target_cookie = verified_target_entry.map(|(_, cookie)| cookie);

        let mut source_inode = self.inode_store.get(source_inode_id).await?;
        if matches!(source_inode, Inode::Directory(_))
            && self.is_ancestor_of(source_inode_id, to_dirid).await?
        {
            return Err(FsError::InvalidArgument);
        }

        let mut from_dir = self.inode_store.get(from_dirid).await?;
        let mut to_dir = if from_dirid != to_dirid {
            Some(self.inode_store.get(to_dirid).await?)
        } else {
            None
        };

        if matches!(&from_dir, Inode::Directory(dir) if dir.nlink == 0)
            || matches!(&to_dir, Some(Inode::Directory(dir)) if dir.nlink == 0)
        {
            return Err(FsError::StaleHandle);
        }

        check_access(&from_dir, &creds, AccessMode::Write)?;
        check_access(&from_dir, &creds, AccessMode::Execute)?;
        if let Some(ref to_dir) = to_dir {
            check_access(to_dir, &creds, AccessMode::Write)?;
            check_access(to_dir, &creds, AccessMode::Execute)?;
        }

        check_sticky_bit_delete(&from_dir, &source_inode, &creds)?;

        // Capture old path before rename for tracing (name will change after)
        let trace_old_path = if self.tracer.has_subscribers() {
            Some(self.inode_store.resolve_path_lossy(source_inode_id).await)
        } else {
            None
        };

        // POSIX: Moving directories in sticky directories requires ownership of the moved directory
        if from_dirid != to_dirid
            && matches!(source_inode, Inode::Directory(_))
            && let Inode::Directory(from_dir_data) = &from_dir
            && from_dir_data.mode & 0o1000 != 0
        {
            let source_uid = match &source_inode {
                Inode::Directory(d) => d.uid,
                _ => unreachable!(),
            };
            if creds.uid != 0 && creds.uid != source_uid {
                return Err(FsError::PermissionDenied);
            }
        }

        let same_inode = target_inode_id == Some(source_inode_id);

        let target = if let Some(target_id) = target_inode_id {
            if same_inode {
                None
            } else {
                let inode = self.inode_store.get(target_id).await?;
                if let Inode::Directory(dir) = &inode
                    && dir.entry_count > 0
                {
                    return Err(FsError::NotEmpty);
                }

                let target_dir = if let Some(ref to_dir) = to_dir {
                    to_dir
                } else {
                    &from_dir
                };
                check_sticky_bit_delete(target_dir, &inode, &creds)?;
                Some((target_id, inode))
            }
        } else {
            None
        };

        let target_should_defer = target
            .as_ref()
            .is_some_and(|(target_id, _)| self.should_defer_unlinked_inode(*target_id));

        let mut txn = self.db.new_transaction()?;
        txn.set_dedup_result(op_id, DedupResult::Rename);

        let mut target_was_directory = false;
        let mut deferred_target_id = None;
        if let Some((target_id, existing_inode)) = target {
            target_was_directory = matches!(existing_inode, Inode::Directory(_));

            let (original_nlink, original_file_size, should_always_remove_stats) =
                match &existing_inode {
                    Inode::File(f) => (f.nlink, Some(f.size), false),
                    Inode::Directory(_) | Inode::Symlink(_) => (1, None, true),
                    Inode::Fifo(s)
                    | Inode::Socket(s)
                    | Inode::CharDevice(s)
                    | Inode::BlockDevice(s) => (s.nlink, None, false),
                };

            // Set when the clobbered target's last link is dropped while a 9P
            // fid still holds it open: defer reclaim exactly like remove().
            // Declared before the macro so the macro body (which sets it) can
            // resolve it under macro_rules hygiene.
            let mut target_deferred = false;

            macro_rules! handle_special_file {
                ($special:expr, $inode_variant:ident) => {
                    if $special.nlink > 1 {
                        $special.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        $special.ctime = now_sec;
                        $special.ctime_nsec = now_nsec;

                        self.inode_store.save(
                            &mut txn,
                            target_id,
                            &Inode::$inode_variant($special),
                        )?;
                    } else if target_should_defer {
                        // POSIX open-unlink on a rename-clobbered special file.
                        $special.nlink = 0;
                        let (now_sec, now_nsec) = get_current_time();
                        $special.ctime = now_sec;
                        $special.ctime_nsec = now_nsec;
                        $special.parent = None;
                        $special.name = None;
                        self.inode_store.save(
                            &mut txn,
                            target_id,
                            &Inode::$inode_variant($special),
                        )?;
                        self.orphan_store.add(&mut txn, target_id);
                        target_deferred = true;
                    } else {
                        self.inode_store.delete(&mut txn, target_id);
                    }
                };
            }

            match existing_inode {
                Inode::File(mut file) => {
                    if file.nlink > 1 {
                        file.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        self.inode_store
                            .save(&mut txn, target_id, &Inode::File(file))?;
                    } else if target_should_defer {
                        // POSIX open-unlink on a rename-clobbered target.
                        file.nlink = 0;
                        let (now_sec, now_nsec) = get_current_time();
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;
                        // Detach from the namespace: the target's dir entry is
                        // about to be reused by the source inode, so the target
                        // must not retain (to_dirid, to_name) — otherwise a
                        // write through its open fid would clobber the renamed
                        // entry. Mirror the hardlink (nlink>1) representation.
                        file.parent = None;
                        file.name = None;
                        self.inode_store
                            .save(&mut txn, target_id, &Inode::File(file))?;
                        self.orphan_store.add(&mut txn, target_id);
                        target_deferred = true;
                    } else {
                        let total_extents = file.size.div_ceil(EXTENT_SIZE as u64);

                        if total_extents as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                            self.extent_store
                                .delete_range(&mut txn, target_id, 0, total_extents)
                                .await?;
                        } else {
                            self.tombstone_store.add(&mut txn, target_id, file.size);
                            self.stats
                                .tombstones_created
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        self.inode_store.delete(&mut txn, target_id);
                    }
                }
                Inode::Directory(mut dir) => {
                    if target_should_defer {
                        dir.nlink = 0;
                        let (now_sec, now_nsec) = get_current_time();
                        dir.ctime = now_sec;
                        dir.ctime_nsec = now_nsec;
                        dir.name = None;
                        self.inode_store
                            .save(&mut txn, target_id, &Inode::Directory(dir))?;
                        self.orphan_store.add(&mut txn, target_id);
                        target_deferred = true;
                    } else {
                        self.inode_store.delete(&mut txn, target_id);
                        self.directory_store.delete_directory(&mut txn, target_id);
                    }
                }
                Inode::Symlink(mut symlink) => {
                    if target_should_defer {
                        // POSIX open-unlink on a rename-clobbered symlink.
                        symlink.nlink = 0;
                        let (now_sec, now_nsec) = get_current_time();
                        symlink.ctime = now_sec;
                        symlink.ctime_nsec = now_nsec;
                        symlink.parent = None;
                        symlink.name = None;
                        self.inode_store
                            .save(&mut txn, target_id, &Inode::Symlink(symlink))?;
                        self.orphan_store.add(&mut txn, target_id);
                        target_deferred = true;
                    } else {
                        self.inode_store.delete(&mut txn, target_id);
                    }
                }
                Inode::Fifo(mut special) => {
                    handle_special_file!(special, Fifo);
                }
                Inode::Socket(mut special) => {
                    handle_special_file!(special, Socket);
                }
                Inode::CharDevice(mut special) => {
                    handle_special_file!(special, CharDevice);
                }
                Inode::BlockDevice(mut special) => {
                    handle_special_file!(special, BlockDevice);
                }
            }

            #[cfg(feature = "failpoints")]
            fail_point!(fp::RENAME_AFTER_TARGET_DELETE);

            // For directories and symlinks: always remove from stats
            // For files and special files: only remove if this is the last link.
            // When deferred, the target's storage is still live; its stats are
            // subtracted at reclaim (last clunk / startup drain).
            if !target_deferred && (should_always_remove_stats || original_nlink <= 1) {
                txn.add_stats_delta(
                    target_id,
                    stats::size_delta(original_file_size.unwrap_or(0), 0),
                    -1,
                );
            }

            if target_deferred {
                deferred_target_id = Some(target_id);
            }

            self.directory_store
                .unlink_entry(&mut txn, to_dirid, to_name, target_cookie.unwrap());
        } else if same_inode {
            self.directory_store
                .unlink_entry(&mut txn, to_dirid, to_name, target_cookie.unwrap());
        }

        self.directory_store
            .unlink_entry(&mut txn, from_dirid, from_name, source_cookie);

        #[cfg(feature = "failpoints")]
        fail_point!(fp::RENAME_AFTER_SOURCE_UNLINK);

        let dir_changed = from_dirid != to_dirid;
        let (now_sec, now_nsec) = get_current_time();
        match &mut source_inode {
            Inode::Directory(d) => {
                if dir_changed {
                    d.parent = to_dirid;
                }
                d.name = Some(to_name.to_vec());
            }
            Inode::File(f) => {
                if same_inode {
                    f.nlink = f.nlink.saturating_sub(1);
                    f.ctime = now_sec;
                    f.ctime_nsec = now_nsec;
                }
                if f.nlink == 1 {
                    f.parent = Some(to_dirid);
                    f.name = Some(to_name.to_vec());
                }
            }
            Inode::Symlink(s) => {
                if s.nlink == 1 {
                    if dir_changed {
                        s.parent = Some(to_dirid);
                    }
                    s.name = Some(to_name.to_vec());
                }
            }
            Inode::Fifo(s) | Inode::Socket(s) | Inode::CharDevice(s) | Inode::BlockDevice(s) => {
                if same_inode {
                    s.nlink = s.nlink.saturating_sub(1);
                    s.ctime = now_sec;
                    s.ctime_nsec = now_nsec;
                }
                if s.nlink == 1 {
                    s.parent = Some(to_dirid);
                    s.name = Some(to_name.to_vec());
                }
            }
        }

        let new_cookie = if from_dirid == to_dirid {
            source_cookie
        } else {
            self.directory_store
                .allocate_cookie(to_dirid, &mut txn)
                .await?
        };

        let embed_inode = if source_inode.nlink() == 1 {
            Some(&source_inode)
        } else {
            None
        };
        self.directory_store.add(
            &mut txn,
            to_dirid,
            to_name,
            source_inode_id,
            new_cookie,
            embed_inode,
        );

        #[cfg(feature = "failpoints")]
        fail_point!(fp::RENAME_AFTER_NEW_ENTRY);

        self.inode_store
            .save(&mut txn, source_inode_id, &source_inode)?;

        let is_moved_dir = matches!(source_inode, Inode::Directory(_));

        if let Inode::Directory(d) = &mut from_dir {
            if from_dirid != to_dirid {
                // Moving to different directory: source leaves from_dir
                d.entry_count = d.entry_count.saturating_sub(1);
                if is_moved_dir {
                    d.nlink = d.nlink.saturating_sub(1);
                }
            } else if target_inode_id.is_some() {
                // Same directory with target: only target was removed (source renamed in place)
                d.entry_count = d.entry_count.saturating_sub(1);
            }
            // If same dir without target: entry_count unchanged (rename only)
            d.mtime = now_sec;
            d.mtime_nsec = now_nsec;
            d.ctime = now_sec;
            d.ctime_nsec = now_nsec;
        }
        self.inode_store.save(&mut txn, from_dirid, &from_dir)?;

        if let Inode::Directory(from_dir_data) = &from_dir
            && let Some(dir_name) = &from_dir_data.name
        {
            self.directory_store
                .update_inode_in_entry(
                    &mut txn,
                    from_dir_data.parent,
                    dir_name,
                    from_dirid,
                    &from_dir,
                )
                .await
                .ok();
        }

        if let Some(ref mut to_dir) = to_dir {
            if let Inode::Directory(d) = to_dir {
                if target_inode_id.is_none() {
                    d.entry_count += 1;
                }
                if is_moved_dir && (target_inode_id.is_none() || !target_was_directory) {
                    if d.nlink == u32::MAX {
                        return Err(FsError::NoSpace);
                    }
                    d.nlink += 1;
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            let to_dir_update_info = if let Inode::Directory(d) = &to_dir {
                d.name.clone().map(|n| (d.parent, n))
            } else {
                None
            };

            self.inode_store.save(&mut txn, to_dirid, to_dir)?;

            if let Some((parent_id, dir_name)) = to_dir_update_info {
                self.directory_store
                    .update_inode_in_entry(&mut txn, parent_id, &dir_name, to_dirid, to_dir)
                    .await
                    .ok();
            }
        }

        self.write_coordinator.commit(txn).await?;

        if let Some(target_id) = deferred_target_id {
            self.schedule_deferred_orphan_reclaim(target_id);
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::RENAME_AFTER_COMMIT);

        match source_inode {
            Inode::File(_) => {
                self.stats.files_renamed.fetch_add(1, Ordering::Relaxed);
            }
            Inode::Directory(_) => {
                self.stats
                    .directories_renamed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Inode::Symlink(_) => {
                self.stats.links_renamed.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        // Emit rename event with old path and new path
        if let Some(old_path) = trace_old_path {
            let inode_store = self.inode_store.clone();
            let tracer = self.tracer.clone();
            let to_name = String::from_utf8_lossy(to_name).to_string();
            tokio::spawn(async move {
                let to_dir_path = inode_store.resolve_path_lossy(to_dirid).await;
                let new_path = format!("{}/{}", to_dir_path.trim_end_matches('/'), to_name);
                tracer.emit_with_path(old_path, FileOperation::Rename { new_path });
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::dedup::DedupResult;
    use crate::fs::inode::Inode;
    use crate::fs::key_codec::KeyCodec;
    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::types::SetAttributes;
    use bytes::Bytes;

    #[tokio::test]
    async fn rename_retry_replays_success_after_source_is_gone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let op_id = [0x41; 16];
        fs.create(&test_creds(), 0, b"before", &SetAttributes::default())
            .await
            .unwrap();

        fs.rename_idempotent(&(&test_auth()).into(), 0, b"before", 0, b"after", op_id)
            .await
            .unwrap();
        fs.rename_idempotent(&(&test_auth()).into(), 0, b"before", 0, b"after", op_id)
            .await
            .unwrap();

        assert!(matches!(fs.dedup.get(&op_id), Some(DedupResult::Rename)));
        assert!(fs.lookup(&test_creds(), 0, b"before").await.is_err());
        assert!(fs.lookup(&test_creds(), 0, b"after").await.is_ok());
    }

    #[tokio::test]
    async fn rename_retry_requires_a_rename_result() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let op_id = [0x42; 16];
        fs.create(&test_creds(), 0, b"source", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&test_creds(), 0, b"removed", &SetAttributes::default())
            .await
            .unwrap();
        fs.remove_idempotent(&(&test_auth()).into(), 0, b"removed", op_id)
            .await
            .unwrap();

        let result = fs
            .rename_idempotent(&(&test_auth()).into(), 0, b"source", 0, b"target", op_id)
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));
        assert!(fs.lookup(&test_creds(), 0, b"source").await.is_ok());
        assert!(fs.lookup(&test_creds(), 0, b"target").await.is_err());
    }

    #[tokio::test]
    async fn test_process_rename_same_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"old.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.rename(&(&test_auth()).into(), 0, b"old.txt", 0, b"new.txt")
            .await
            .unwrap();

        // Check old entry is gone and new entry exists
        let old_entry_key = KeyCodec::new().dir_entry_key(0, b"old.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        let new_entry_key = KeyCodec::new().dir_entry_key(0, b"new.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_rename_replace_existing() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create two files
        let (file1_id, _) = fs
            .create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &(&test_auth()).into(),
            file1_id,
            0,
            &Bytes::from(b"content1".to_vec()),
        )
        .await
        .unwrap();

        let (file2_id, _) = fs
            .create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(
            &(&test_auth()).into(),
            file2_id,
            0,
            &Bytes::from(b"content2".to_vec()),
        )
        .await
        .unwrap();

        fs.rename(&(&test_auth()).into(), 0, b"file1.txt", 0, b"file2.txt")
            .await
            .unwrap();

        // Check that file1.txt no longer exists
        let old_entry_key = KeyCodec::new().dir_entry_key(0, b"file1.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check that file2.txt exists and has file1's content
        let new_entry_key = KeyCodec::new().dir_entry_key(0, b"file2.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file1_id);

        // Verify content
        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file1_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data.as_ref(), b"content1");

        // Check that the original file2 inode is gone
        let result = fs.inode_store.get(file2_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_rename_across_directories() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir1_id, _) = fs
            .mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&test_creds(), 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .create(
                &test_creds(),
                dir1_id,
                b"file.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        fs.rename(
            &(&test_auth()).into(),
            dir1_id,
            b"file.txt",
            dir2_id,
            b"moved.txt",
        )
        .await
        .unwrap();

        // Check file removed from dir1
        let old_entry_key = KeyCodec::new().dir_entry_key(dir1_id, b"file.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check file added to dir2
        let new_entry_key = KeyCodec::new().dir_entry_key(dir2_id, b"moved.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);

        // Check entry counts
        let dir1_inode = fs.inode_store.get(dir1_id).await.unwrap();
        match dir1_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }

        let dir2_inode = fs.inode_store.get(dir2_id).await.unwrap();
        match dir2_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 1);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_rename_directory_entry_count() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a directory with two files
        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(
            &test_creds(),
            dir_id,
            b"file1.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();
        fs.create(
            &test_creds(),
            dir_id,
            b"file2.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        // Check initial entry count
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 2),
            _ => panic!("Should be a directory"),
        }

        fs.rename(
            &(&test_auth()).into(),
            dir_id,
            b"file1.txt",
            dir_id,
            b"file2.txt",
        )
        .await
        .unwrap();

        // Check that entry count decreased by 1
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 1),
            _ => panic!("Should be a directory"),
        }

        fs.remove(&(&test_auth()).into(), dir_id, b"file2.txt")
            .await
            .unwrap();

        // Directory should now be empty and removable
        let dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 0),
            _ => panic!("Should be a directory"),
        }

        // Should be able to remove the empty directory
        fs.remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_rename_prevent_directory_cycles() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c
        let (a_id, _) = fs
            .mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();

        // Test 1: Try to rename /a into /a/b (direct descendant)
        let result = fs
            .rename(&(&test_auth()).into(), 0, b"a", b_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 2: Try to rename /a into /a/b/c (deeper descendant)
        let result = fs
            .rename(&(&test_auth()).into(), 0, b"a", c_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 3: Try to rename /a/b into /a/b/c (moving into immediate child)
        let result = fs
            .rename(&(&test_auth()).into(), a_id, b"b", c_id, b"b_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 4: Valid rename - moving /a/b/c to root
        let result = fs
            .rename(&(&test_auth()).into(), b_id, b"c", 0, b"c_moved")
            .await;
        assert!(result.is_ok());

        // Test 5: Valid rename - moving a file (not a directory) should work
        let (_file_id, _) = fs
            .create(&test_creds(), a_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();
        let result = fs
            .rename(
                &(&test_auth()).into(),
                a_id,
                b"file.txt",
                b_id,
                b"file_moved.txt",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_ancestor_of() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c/d
        let (a_id, _) = fs
            .mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();
        let (d_id, _) = fs
            .mkdir(&test_creds(), c_id, b"d", &SetAttributes::default())
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
    async fn test_rename_hardlink_over_same_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => assert_eq!(f.nlink, 1, "Initial nlink should be 1"),
            _ => panic!("Expected file inode"),
        }

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => assert_eq!(f.nlink, 2, "After link, nlink should be 2"),
            _ => panic!("Expected file inode"),
        }

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 2, "Directory should have 2 entries"),
            _ => panic!("Expected directory inode"),
        }

        fs.rename(
            &(&test_auth()).into(),
            0,
            b"hardlink.txt",
            0,
            b"original.txt",
        )
        .await
        .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1, "After rename, nlink should be 1");
                // Since nlink is 1, parent and name should be set
                assert_eq!(f.parent, Some(0), "Parent should be set to root");
                assert_eq!(f.name, Some(b"original.txt".to_vec()), "Name should be set");
            }
            _ => panic!("Expected file inode"),
        }

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 1, "Directory should have 1 entry"),
            _ => panic!("Expected directory inode"),
        }

        let result = fs.directory_store.get(0, b"hardlink.txt").await;
        assert!(matches!(result, Err(FsError::NotFound)));

        let stored_id = fs.directory_store.get(0, b"original.txt").await.unwrap();
        assert_eq!(stored_id, file_id, "original.txt should point to the file");

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data.as_ref(), b"test content");

        fs.remove(&(&test_auth()).into(), 0, b"original.txt")
            .await
            .unwrap();

        let root_inode = fs.inode_store.get(0).await.unwrap();
        match &root_inode {
            Inode::Directory(d) => assert_eq!(d.entry_count, 0, "Directory should be empty"),
            _ => panic!("Expected directory inode"),
        }

        let result = fs.inode_store.get(file_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }
}
