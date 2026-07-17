//! remove (unlink/rmdir), with its idempotent variant.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};
use crate::fs::stats;
use crate::fs::tracing::FileOperation;
use crate::fs::types::AuthContext;
use crate::fs::{
    EXTENT_SIZE, SMALL_FILE_TOMBSTONE_THRESHOLD, ZeroFS, get_current_time, validate_filename,
};
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Unlink `name` from `dirid`; an empty directory is removed, a non-empty
    /// one is ENOTEMPTY, sticky-bit rules apply. Dropping the last link of an
    /// inode a fid still holds open defers reclaim: nlink goes to 0 and the
    /// inode joins the durable orphan set until the last close.
    pub async fn remove(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        name: &[u8],
    ) -> Result<(), FsError> {
        self.remove_idempotent(auth, dirid, name, [0u8; 16]).await
    }

    /// `remove` tagged with an idempotency op-id: an applied retry is a no-op
    /// success instead of NotFound. See [`Self::create_idempotent`].
    pub async fn remove_idempotent(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        name: &[u8],
        op_id: crate::dedup::OpId,
    ) -> Result<(), FsError> {
        validate_filename(name)?;

        // Replay a completed remove before resolving the missing entry.
        if self
            .replay_dedup_result(&op_id, DedupResult::into_remove)?
            .is_some()
        {
            return Ok(());
        }

        let creds = Credentials::from_auth_context(auth);

        let (file_id, cookie) = match self
            .directory_store
            .get_entry_with_cookie(dirid, name)
            .await
        {
            Ok(entry) => entry,
            Err(error) => {
                if self
                    .replay_dedup_result(&op_id, DedupResult::into_remove)?
                    .is_some()
                {
                    return Ok(());
                }
                return Err(error);
            }
        };

        let _guards = self.lock_manager.acquire_multi(vec![dirid, file_id]).await;

        // Recheck replay state after waiting for inode locks.
        if self
            .replay_dedup_result(&op_id, DedupResult::into_remove)?
            .is_some()
        {
            return Ok(());
        }

        let mut dir_inode = self.inode_store.get(dirid).await?;
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let is_dir = matches!(dir_inode, Inode::Directory(_));
        if !is_dir {
            return Err(FsError::NotDirectory);
        }
        if matches!(&dir_inode, Inode::Directory(dir) if dir.nlink == 0) {
            return Err(FsError::StaleHandle);
        }

        // Re-check inside lock to verify entry still points to same inode
        let (verified_id, verified_cookie) = self
            .directory_store
            .get_entry_with_cookie(dirid, name)
            .await?;
        if verified_id != file_id || verified_cookie != cookie {
            return Err(FsError::NotFound);
        }

        let mut file_inode = self.inode_store.get(file_id).await?;
        let defer_unlinked = self.should_defer_unlinked_inode(file_id);

        let original_nlink = match &file_inode {
            Inode::File(f) => f.nlink,
            Inode::Fifo(s) | Inode::Socket(s) | Inode::CharDevice(s) | Inode::BlockDevice(s) => {
                s.nlink
            }
            _ => 1,
        };

        check_sticky_bit_delete(&dir_inode, &file_inode, &creds)?;

        // Capture path before deletion for tracing (inode will be gone after)
        let trace_path = if self.tracer.has_subscribers() {
            Some(self.inode_store.resolve_path_lossy(file_id).await)
        } else {
            None
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let mut txn = self.db.new_transaction()?;
                txn.set_dedup_result(op_id, DedupResult::Remove);
                let (now_sec, now_nsec) = get_current_time();

                // Set when the last link is dropped but a 9P fid still holds
                // the inode open: the inode + extents are kept and recorded in
                // the durable orphan set, and the stats subtraction is deferred
                // to reclaim (the storage is still live). The namespace effect
                // (entry gone, nlink=0) is committed as durably as ever.
                let mut deferred = false;
                match &mut file_inode {
                    Inode::File(file) => {
                        if file.nlink > 1 {
                            file.nlink -= 1;
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                        } else if defer_unlinked {
                            // POSIX open-unlink: defer reclaim until last clunk.
                            file.nlink = 0;
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;
                            // Detach from the namespace (the dir entry is being
                            // removed below) so write()/setattr() through the
                            // still-open fid do not try to update a now-gone
                            // directory entry, same representation hardlinked
                            // (nlink>1) files already use.
                            file.parent = None;
                            file.name = None;
                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                            self.orphan_store.add(&mut txn, file_id);
                            deferred = true;

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::REMOVE_AFTER_ORPHAN_ADD);
                        } else {
                            let total_extents = file.size.div_ceil(EXTENT_SIZE as u64);

                            if total_extents as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                                self.extent_store
                                    .delete_range(&mut txn, file_id, 0, total_extents)
                                    .await?;
                            } else {
                                self.tombstone_store.add(&mut txn, file_id, file.size);

                                #[cfg(feature = "failpoints")]
                                fail_point!(fp::REMOVE_AFTER_TOMBSTONE);

                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            self.inode_store.delete(&mut txn, file_id);

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::REMOVE_AFTER_INODE_DELETE);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(FsError::NotEmpty);
                        }
                        if defer_unlinked {
                            // An open directory retains its inode and cookie counter
                            // until last clunk.
                            subdir.nlink = 0;
                            subdir.ctime = now_sec;
                            subdir.ctime_nsec = now_nsec;
                            subdir.name = None;
                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                            self.orphan_store.add(&mut txn, file_id);
                            deferred = true;

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::REMOVE_AFTER_ORPHAN_ADD);
                        } else {
                            self.inode_store.delete(&mut txn, file_id);

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::RMDIR_AFTER_INODE_DELETE);

                            self.directory_store.delete_directory(&mut txn, file_id);

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::RMDIR_AFTER_DIR_CLEANUP);

                            self.stats
                                .directories_deleted
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        dir.nlink = dir.nlink.saturating_sub(1);
                    }
                    Inode::Symlink(symlink) => {
                        if defer_unlinked {
                            // POSIX open-unlink: defer reclaim until last clunk.
                            symlink.nlink = 0;
                            symlink.ctime = now_sec;
                            symlink.ctime_nsec = now_nsec;
                            symlink.parent = None;
                            symlink.name = None;
                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                            self.orphan_store.add(&mut txn, file_id);
                            deferred = true;

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::REMOVE_AFTER_ORPHAN_ADD);
                        } else {
                            self.inode_store.delete(&mut txn, file_id);
                            self.stats.links_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Fifo(special)
                    | Inode::Socket(special)
                    | Inode::CharDevice(special)
                    | Inode::BlockDevice(special) => {
                        if special.nlink > 1 {
                            special.nlink -= 1;
                            special.ctime = now_sec;
                            special.ctime_nsec = now_nsec;

                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                        } else if defer_unlinked {
                            // POSIX open-unlink: defer reclaim until last clunk.
                            special.nlink = 0;
                            special.ctime = now_sec;
                            special.ctime_nsec = now_nsec;
                            special.parent = None;
                            special.name = None;
                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                            self.orphan_store.add(&mut txn, file_id);
                            deferred = true;

                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::REMOVE_AFTER_ORPHAN_ADD);
                        } else {
                            self.inode_store.delete(&mut txn, file_id);
                        }
                    }
                }

                self.directory_store
                    .unlink_entry(&mut txn, dirid, name, cookie);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::REMOVE_AFTER_DIR_UNLINK);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let parent_update_info = dir.name.clone().map(|n| (dir.parent, n));

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                if let Some((parent_id, dir_name)) = parent_update_info {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &dir_name, dirid, &dir_inode)
                        .await
                        .ok();
                }

                // For directories and symlinks: always remove from stats
                // For files and special files: only remove if this is the last link
                let (file_size, should_always_remove_stats) = match &file_inode {
                    Inode::File(f) => (Some(f.size), false),
                    Inode::Directory(_) | Inode::Symlink(_) => (None, true),
                    _ => (None, false),
                };

                // When deferred, the inode's storage is still live; its stats
                // are subtracted at reclaim (last clunk / startup drain).
                if !deferred && (should_always_remove_stats || original_nlink <= 1) {
                    txn.add_stats_delta(file_id, stats::size_delta(file_size.unwrap_or(0), 0), -1);
                }

                self.write_coordinator.commit(txn).await?;

                if deferred {
                    self.schedule_deferred_orphan_reclaim(file_id);
                }

                #[cfg(feature = "failpoints")]
                fail_point!(fp::REMOVE_AFTER_COMMIT);

                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                if let Some(path) = trace_path {
                    self.tracer.emit_with_path(path, FileOperation::Remove);
                }

                Ok(())
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::dedup::DedupResult;
    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::key_codec::KeyCodec;

    use crate::fs::types::SetAttributes;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_process_remove_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"some data".to_vec()),
        )
        .await
        .unwrap();

        fs.remove(&(&test_auth()).into(), 0, b"test.txt")
            .await
            .unwrap();

        // Check that the file was removed from the directory
        let entry_key = KeyCodec::new().dir_entry_key(0, b"test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap();
        assert!(entry_data.is_none());

        let result = fs.inode_store.get(file_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();

        let result = fs.inode_store.get(dir_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_non_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.create(
            &test_creds(),
            dir_id,
            b"file.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        let result = fs.remove(&(&test_auth()).into(), 0, b"testdir").await;
        assert!(matches!(result, Err(FsError::NotEmpty)));
    }

    #[tokio::test]
    async fn remove_retry_requires_a_remove_result() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let op_id = [0x31; 16];
        fs.create_idempotent(&test_creds(), 0, b"file", &SetAttributes::default(), op_id)
            .await
            .unwrap();

        let result = fs
            .remove_idempotent(&(&test_auth()).into(), 0, b"file", op_id)
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));
        assert!(fs.lookup(&test_creds(), 0, b"file").await.is_ok());
    }

    #[tokio::test]
    async fn remove_retry_replays_success_after_entry_is_gone() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let op_id = [0x32; 16];
        fs.create(&test_creds(), 0, b"file", &SetAttributes::default())
            .await
            .unwrap();

        fs.remove_idempotent(&(&test_auth()).into(), 0, b"file", op_id)
            .await
            .unwrap();
        fs.remove_idempotent(&(&test_auth()).into(), 0, b"file", op_id)
            .await
            .unwrap();
        assert!(matches!(fs.dedup.get(&op_id), Some(DedupResult::Remove)));
    }
}
