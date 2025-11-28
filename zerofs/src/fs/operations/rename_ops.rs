use super::common::validate_filename;
use crate::fs::cache::CacheKey;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::operations::common::SMALL_FILE_TOMBSTONE_THRESHOLD;
use crate::fs::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};
use crate::fs::types::AuthContext;
use crate::fs::{CHUNK_SIZE, ZeroFS, get_current_time};
use std::sync::atomic::Ordering;
use tracing::debug;

impl ZeroFS {
    pub async fn process_rename(
        &self,
        auth: &AuthContext,
        from_dirid: u64,
        from_name: &[u8],
        to_dirid: u64,
        to_name: &[u8],
    ) -> Result<(), FsError> {
        if from_name.is_empty() || to_name.is_empty() {
            return Err(FsError::InvalidArgument);
        }

        validate_filename(from_name)?;
        validate_filename(to_name)?;

        if from_name == b"." || from_name == b".." {
            return Err(FsError::InvalidArgument);
        }
        if to_name == b"." || to_name == b".." {
            return Err(FsError::Exists);
        }

        if from_dirid == to_dirid && from_name == to_name {
            return Ok(());
        }

        debug!(
            "process_rename: from_dir={}, from_name={}, to_dir={}, to_name={}",
            from_dirid,
            String::from_utf8_lossy(from_name),
            to_dirid,
            String::from_utf8_lossy(to_name)
        );

        let creds = Credentials::from_auth_context(auth);

        // Look up all inode IDs without holding any locks
        let source_inode_id = self.directory_store.get(from_dirid, from_name).await?;

        if to_dirid == source_inode_id {
            return Err(FsError::InvalidArgument);
        }

        let target_inode_id = match self.directory_store.get(to_dirid, to_name).await {
            Ok(id) => Some(id),
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };

        let mut all_inodes_to_lock = vec![from_dirid, source_inode_id];
        if from_dirid != to_dirid {
            all_inodes_to_lock.push(to_dirid);
        }
        if let Some(target_id) = target_inode_id {
            all_inodes_to_lock.push(target_id);
        }

        let _guards = self
            .lock_manager
            .acquire_multiple_write(all_inodes_to_lock)
            .await;

        // Re-check inside lock to verify entry still points to same inode
        let verified_id = self.directory_store.get(from_dirid, from_name).await?;
        if verified_id != source_inode_id {
            return Err(FsError::NotFound);
        }

        let source_inode = self.load_inode(source_inode_id).await?;
        if matches!(source_inode, Inode::Directory(_))
            && self.is_ancestor_of(source_inode_id, to_dirid).await?
        {
            return Err(FsError::InvalidArgument);
        }

        let from_dir = self.load_inode(from_dirid).await?;
        let to_dir = if from_dirid != to_dirid {
            Some(self.load_inode(to_dirid).await?)
        } else {
            None
        };

        check_access(&from_dir, &creds, AccessMode::Write)?;
        check_access(&from_dir, &creds, AccessMode::Execute)?;
        if let Some(ref to_dir) = to_dir {
            check_access(to_dir, &creds, AccessMode::Write)?;
            check_access(to_dir, &creds, AccessMode::Execute)?;
        }

        check_sticky_bit_delete(&from_dir, &source_inode, &creds)?;

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

        if let Some(target_id) = target_inode_id {
            let target_inode = self.load_inode(target_id).await?;
            if let Inode::Directory(dir) = &target_inode
                && dir.entry_count > 0
            {
                return Err(FsError::NotEmpty);
            }

            let target_dir = if let Some(ref to_dir) = to_dir {
                to_dir
            } else {
                &from_dir
            };
            check_sticky_bit_delete(target_dir, &target_inode, &creds)?;
        }

        let mut txn = self.new_transaction()?;

        let mut target_was_directory = false;
        let mut target_stats_update = None;
        if let Some(target_id) = target_inode_id {
            let existing_inode = self.load_inode(target_id).await?;

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
                    } else {
                        let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;

                        if total_chunks <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                            for chunk_idx in 0..total_chunks {
                                txn.delete_chunk(target_id, chunk_idx as u64);
                            }
                        } else {
                            txn.add_tombstone(target_id, file.size);
                            self.stats
                                .tombstones_created
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        self.inode_store.delete(&mut txn, target_id);
                    }
                }
                Inode::Directory(_) => {
                    self.inode_store.delete(&mut txn, target_id);
                }
                Inode::Symlink(_) => {
                    self.inode_store.delete(&mut txn, target_id);
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

            // For directories and symlinks: always remove from stats
            // For files and special files: only remove if this is the last link
            if should_always_remove_stats || original_nlink <= 1 {
                target_stats_update = Some(
                    self.global_stats
                        .prepare_inode_remove(target_id, original_file_size)
                        .await,
                );
            }

            self.directory_store
                .remove(&mut txn, to_dirid, to_name, target_id);
        }

        self.directory_store
            .remove(&mut txn, from_dirid, from_name, source_inode_id);
        self.directory_store
            .add(&mut txn, to_dirid, to_name, source_inode_id);

        if from_dirid != to_dirid {
            let mut moved_inode = self.load_inode(source_inode_id).await?;
            match &mut moved_inode {
                Inode::Directory(d) => d.parent = to_dirid,
                Inode::File(f) => {
                    // Lazy restoration: if nlink == 1, restore parent
                    if f.nlink == 1 {
                        f.parent = Some(to_dirid);
                    }
                }
                Inode::Symlink(s) => {
                    // Lazy restoration: if nlink == 1, restore parent
                    if s.nlink == 1 {
                        s.parent = Some(to_dirid);
                    }
                }
                Inode::Fifo(s)
                | Inode::Socket(s)
                | Inode::CharDevice(s)
                | Inode::BlockDevice(s) => {
                    // Lazy restoration: if nlink == 1, restore parent
                    if s.nlink == 1 {
                        s.parent = Some(to_dirid);
                    }
                }
            }
            self.inode_store
                .save(&mut txn, source_inode_id, &moved_inode)?;
        }

        let (now_sec, now_nsec) = get_current_time();

        let is_moved_dir = matches!(source_inode, Inode::Directory(_));

        let mut from_dir_inode = self.load_inode(from_dirid).await?;
        if let Inode::Directory(d) = &mut from_dir_inode {
            d.entry_count = d.entry_count.saturating_sub(1);
            if is_moved_dir && from_dirid != to_dirid {
                d.nlink = d.nlink.saturating_sub(1);
            }
            d.mtime = now_sec;
            d.mtime_nsec = now_nsec;
            d.ctime = now_sec;
            d.ctime_nsec = now_nsec;
        }
        self.inode_store
            .save(&mut txn, from_dirid, &from_dir_inode)?;

        if from_dirid != to_dirid {
            let mut to_dir_inode = self.load_inode(to_dirid).await?;
            if let Inode::Directory(d) = &mut to_dir_inode {
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
            self.inode_store.save(&mut txn, to_dirid, &to_dir_inode)?;
        } else {
            let mut dir_inode = self.load_inode(from_dirid).await?;
            if let Inode::Directory(d) = &mut dir_inode {
                if target_inode_id.is_some() {
                    d.entry_count = d.entry_count.saturating_sub(1);
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            self.inode_store.save(&mut txn, from_dirid, &dir_inode)?;
        }

        if let Some(ref update) = target_stats_update {
            self.global_stats.add_to_transaction(update, &mut txn)?;
        }

        let mut seq_guard = self.allocate_sequence();
        self.commit_transaction(txn, &mut seq_guard).await?;

        if let Some(update) = target_stats_update {
            self.global_stats.commit_update(&update);
        }

        let mut futures = vec![CacheKey::Metadata(source_inode_id)];

        if let Some(target_id) = target_inode_id {
            futures.push(CacheKey::Metadata(target_id));
        }

        futures.push(CacheKey::Metadata(from_dirid));

        if from_dirid != to_dirid {
            futures.push(CacheKey::Metadata(to_dirid));
        }

        futures.push(CacheKey::DirEntry {
            dir_id: to_dirid,
            name: to_name.to_vec(),
        });
        futures.push(CacheKey::DirEntry {
            dir_id: from_dirid,
            name: from_name.to_vec(),
        });

        self.cache.remove_batch(futures);

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
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}
