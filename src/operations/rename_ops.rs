use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::debug;
use zerofs_nfsserve::nfs::nfsstat3;
use zerofs_nfsserve::vfs::AuthContext;

use super::common::validate_filename;
use crate::cache::CacheKey;
use crate::filesystem::{CHUNK_SIZE, SlateDbFs, get_current_time};
use crate::inode::Inode;
use crate::operations::common::SMALL_FILE_TOMBSTONE_THRESHOLD;
use crate::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};

impl SlateDbFs {
    pub async fn process_rename(
        &self,
        auth: &AuthContext,
        from_dirid: u64,
        from_filename: &[u8],
        to_dirid: u64,
        to_filename: &[u8],
    ) -> Result<(), nfsstat3> {
        // Validate filenames are not empty
        if from_filename.is_empty() || to_filename.is_empty() {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        validate_filename(from_filename)?;
        validate_filename(to_filename)?;

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

        let creds = Credentials::from_auth_context(auth);

        // Look up all inode IDs without holding any locks
        let from_entry_key = Self::dir_entry_key(from_dirid, &from_name);
        let entry_data = self
            .db
            .get_bytes(&from_entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let source_inode_id = u64::from_le_bytes(bytes);

        // Check if we're trying to move something into itself
        if to_dirid == source_inode_id {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        // Check if target exists
        let to_entry_key = Self::dir_entry_key(to_dirid, &to_name);
        let target_inode_id = if let Some(existing_entry) = self
            .db
            .get_bytes(&to_entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
        {
            let mut existing_bytes = [0u8; 8];
            existing_bytes.copy_from_slice(&existing_entry[..8]);
            Some(u64::from_le_bytes(existing_bytes))
        } else {
            None
        };

        // Now determine all inodes we need to lock
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

        // Now perform all checks and operations with locks held
        // First re-verify that source still exists where we expect it
        let entry_data = self
            .db
            .get_bytes(&from_entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut verify_bytes = [0u8; 8];
        verify_bytes.copy_from_slice(&entry_data[..8]);
        if u64::from_le_bytes(verify_bytes) != source_inode_id {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }

        // Re-load source inode to check for directory cycles
        let source_inode = self.load_inode(source_inode_id).await?;
        if matches!(source_inode, Inode::Directory(_))
            && self.is_ancestor_of(source_inode_id, to_dirid).await?
        {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        // Load directories to check permissions
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

        // Additional check for moving directories across parents in sticky directories
        // When a directory is moved to a different parent, its ".." entry must be updated,
        // which requires ownership of the directory being moved
        if from_dirid != to_dirid && matches!(source_inode, Inode::Directory(_)) {
            if let Inode::Directory(from_dir_data) = &from_dir {
                if from_dir_data.mode & 0o1000 != 0 {
                    // Sticky bit set
                    let source_uid = match &source_inode {
                        Inode::Directory(d) => d.uid,
                        _ => unreachable!(),
                    };
                    if creds.uid != 0 && creds.uid != source_uid {
                        return Err(nfsstat3::NFS3ERR_PERM);
                    }
                }
            }
        }

        // Check if target directory is non-empty (if it exists and is a directory)
        if let Some(target_id) = target_inode_id {
            let target_inode = self.load_inode(target_id).await?;
            if let Inode::Directory(dir) = &target_inode {
                if dir.entry_count > 0 {
                    return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                }
            }

            let target_dir = if let Some(ref to_dir) = to_dir {
                to_dir
            } else {
                &from_dir
            };
            check_sticky_bit_delete(target_dir, &target_inode, &creds)?;
        }

        // Now perform the rename operation with all locks held
        let mut batch = self.db.new_write_batch();

        // Handle target if it exists
        let mut target_was_directory = false;
        if let Some(target_id) = target_inode_id {
            let existing_inode = self.load_inode(target_id).await?;

            // Track if we're replacing a directory
            target_was_directory = matches!(existing_inode, Inode::Directory(_));

            macro_rules! handle_special_file {
                ($special:expr, $inode_variant:ident) => {
                    if $special.nlink > 1 {
                        $special.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        $special.ctime = now_sec;
                        $special.ctime_nsec = now_nsec;

                        let inode_key = Self::inode_key(target_id);
                        let inode_data = bincode::serialize(&Inode::$inode_variant($special))
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        batch
                            .put_bytes(&inode_key, &inode_data)
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                    } else {
                        let inode_key = Self::inode_key(target_id);
                        batch.delete_bytes(&inode_key);
                    }
                };
            }

            match existing_inode {
                Inode::File(mut file) => {
                    // For regular files, check if it has multiple hard links
                    if file.nlink > 1 {
                        // Just decrement the link count
                        file.nlink -= 1;
                        let (now_sec, now_nsec) = get_current_time();
                        file.ctime = now_sec;
                        file.ctime_nsec = now_nsec;

                        let inode_key = Self::inode_key(target_id);
                        let inode_data = bincode::serialize(&Inode::File(file))
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        batch
                            .put_bytes(&inode_key, &inode_data)
                            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                    } else {
                        // Last link, check if we should delete immediately or defer
                        let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;

                        if total_chunks <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                            // Small file, delete chunks immediately
                            for chunk_idx in 0..total_chunks {
                                let chunk_key = Self::chunk_key_by_index(target_id, chunk_idx);
                                batch.delete_bytes(&chunk_key);
                            }
                        } else {
                            // Large file, create tombstone for deferred chunk deletion
                            let (timestamp, _) = get_current_time();
                            let tombstone_key = Self::tombstone_key(timestamp, target_id);
                            batch
                                .put_bytes(&tombstone_key, &file.size.to_le_bytes())
                                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                            self.stats
                                .tombstones_created
                                .fetch_add(1, Ordering::Relaxed);
                        }

                        // Delete the inode
                        let inode_key = Self::inode_key(target_id);
                        batch.delete_bytes(&inode_key);
                    }
                }
                Inode::Directory(_) => {
                    let inode_key = Self::inode_key(target_id);
                    batch.delete_bytes(&inode_key);
                }
                Inode::Symlink(_) => {
                    let inode_key = Self::inode_key(target_id);
                    batch.delete_bytes(&inode_key);
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

            // Delete the existing scan entry
            let existing_scan_key = Self::dir_scan_key(to_dirid, target_id, &to_name);
            batch.delete_bytes(&existing_scan_key);
        }

        batch.delete_bytes(&from_entry_key);
        let from_scan_key = Self::dir_scan_key(from_dirid, source_inode_id, &from_name);
        batch.delete_bytes(&from_scan_key);

        batch
            .put_bytes(&to_entry_key, &source_inode_id.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        let to_scan_key = Self::dir_scan_key(to_dirid, source_inode_id, &to_name);
        batch
            .put_bytes(&to_scan_key, &source_inode_id.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Update the moved inode's parent field if moving to different directory
        if from_dirid != to_dirid {
            let mut moved_inode = self.load_inode(source_inode_id).await?;
            match &mut moved_inode {
                Inode::File(f) => f.parent = to_dirid,
                Inode::Directory(d) => d.parent = to_dirid,
                Inode::Symlink(s) => s.parent = to_dirid,
                Inode::Fifo(s) => s.parent = to_dirid,
                Inode::Socket(s) => s.parent = to_dirid,
                Inode::CharDevice(s) => s.parent = to_dirid,
                Inode::BlockDevice(s) => s.parent = to_dirid,
            }
            batch
                .put_bytes(
                    &Self::inode_key(source_inode_id),
                    &bincode::serialize(&moved_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?,
                )
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }

        let (now_sec, now_nsec) = get_current_time();

        // Check if we're moving a directory - affects parent nlink counts
        let is_moved_dir = matches!(source_inode, Inode::Directory(_));

        // Update source directory
        let mut from_dir_inode = self.load_inode(from_dirid).await?;
        if let Inode::Directory(d) = &mut from_dir_inode {
            d.entry_count = d.entry_count.saturating_sub(1);
            // If we moved a directory out, decrement nlink (lost the .. entry)
            if is_moved_dir && from_dirid != to_dirid {
                d.nlink = d.nlink.saturating_sub(1);
            }
            d.mtime = now_sec;
            d.mtime_nsec = now_nsec;
            d.ctime = now_sec;
            d.ctime_nsec = now_nsec;
        }
        batch
            .put_bytes(
                &Self::inode_key(from_dirid),
                &bincode::serialize(&from_dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?,
            )
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Update target directory if different from source
        if from_dirid != to_dirid {
            let mut to_dir_inode = self.load_inode(to_dirid).await?;
            if let Inode::Directory(d) = &mut to_dir_inode {
                // Only increment entry count if we're not replacing an existing entry
                if target_inode_id.is_none() {
                    d.entry_count += 1;
                }
                // If we moved a directory in, increment nlink (gained the .. entry)
                // But if we replaced an existing directory, the net change is 0
                if is_moved_dir && (target_inode_id.is_none() || !target_was_directory) {
                    if d.nlink == u32::MAX {
                        return Err(nfsstat3::NFS3ERR_NOSPC);
                    }
                    d.nlink += 1;
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            batch
                .put_bytes(
                    &Self::inode_key(to_dirid),
                    &bincode::serialize(&to_dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?,
                )
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        } else {
            // Same directory rename
            let mut dir_inode = self.load_inode(from_dirid).await?;
            if let Inode::Directory(d) = &mut dir_inode {
                // When renaming within the same directory:
                // - If replacing an existing entry: we remove 2 (source + target) and add 1 = net -1
                // - If not replacing: we remove 1 (source) and add 1 = net 0 (handled by just updating mtime)
                if target_inode_id.is_some() {
                    d.entry_count = d.entry_count.saturating_sub(1);
                }
                d.mtime = now_sec;
                d.mtime_nsec = now_nsec;
                d.ctime = now_sec;
                d.ctime_nsec = now_nsec;
            }
            batch
                .put_bytes(
                    &Self::inode_key(from_dirid),
                    &bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?,
                )
                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
        }

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.metadata_cache
            .remove(CacheKey::Metadata(source_inode_id));
        if let Some(target_id) = target_inode_id {
            self.metadata_cache.remove(CacheKey::Metadata(target_id));
            self.small_file_cache.remove(CacheKey::SmallFile(target_id));
        }
        self.metadata_cache.remove(CacheKey::Metadata(from_dirid));
        if from_dirid != to_dirid {
            self.metadata_cache.remove(CacheKey::Metadata(to_dirid));
        }
        self.dir_entry_cache.remove(CacheKey::DirEntry {
            dir_id: from_dirid,
            name: from_name.clone(),
        });
        self.dir_entry_cache.remove(CacheKey::DirEntry {
            dir_id: to_dirid,
            name: to_name.clone(),
        });

        Ok(())
    }
}
