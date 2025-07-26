use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use zerofs_nfsserve::nfs::{fileid3, nfsstat3};
use zerofs_nfsserve::vfs::AuthContext;

use super::common::validate_filename;
use crate::filesystem::{CHUNK_SIZE, SlateDbFs, get_current_time};
use crate::inode::Inode;
use crate::operations::common::SMALL_FILE_TOMBSTONE_THRESHOLD;
use crate::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};

impl SlateDbFs {
    pub async fn process_remove(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &[u8],
    ) -> Result<(), nfsstat3> {
        validate_filename(filename)?;

        let name = String::from_utf8_lossy(filename).to_string();
        let creds = Credentials::from_auth_context(auth);

        // Look up the file_id without holding any locks
        let entry_key = Self::dir_entry_key(dirid, &name);
        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let file_id = u64::from_le_bytes(bytes);

        // Now acquire both locks in sorted order
        let _guards = self
            .lock_manager
            .acquire_multiple_write(vec![dirid, file_id])
            .await;

        // Verify everything is still valid after acquiring locks
        let dir_inode = self.load_inode(dirid).await?;
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let is_dir = matches!(dir_inode, Inode::Directory(_));
        if !is_dir {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }

        // Re-verify the entry still exists and points to the same file
        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let mut verify_bytes = [0u8; 8];
        verify_bytes.copy_from_slice(&entry_data[..8]);

        if u64::from_le_bytes(verify_bytes) != file_id {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }

        let mut file_inode = self.load_inode(file_id).await?;

        // Capture the original nlink before any modifications
        let original_nlink = match &file_inode {
            Inode::File(f) => f.nlink,
            Inode::Fifo(s) | Inode::Socket(s) | Inode::CharDevice(s) | Inode::BlockDevice(s) => {
                s.nlink
            }
            _ => 1,
        };

        check_sticky_bit_delete(&dir_inode, &file_inode, &creds)?;

        let mut dir_inode = self.load_inode(dirid).await?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let mut batch = self.db.new_write_batch();

                match &mut file_inode {
                    Inode::File(file) => {
                        // Check if this is the last hard link
                        if file.nlink > 1 {
                            // Just decrement the link count, don't delete the file
                            file.nlink -= 1;
                            let (now_sec, now_nsec) = get_current_time();
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            let inode_key = Self::inode_key(file_id);
                            let inode_data = bincode::serialize(&file_inode)
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
                                    let chunk_key = Self::chunk_key_by_index(file_id, chunk_idx);
                                    batch.delete_bytes(&chunk_key);
                                }
                            } else {
                                // Large file, create tombstone for deferred chunk deletion
                                let (timestamp, _) = get_current_time();
                                let tombstone_key = Self::tombstone_key(timestamp, file_id);
                                batch
                                    .put_bytes(&tombstone_key, &file.size.to_le_bytes())
                                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            // Delete the inode
                            let inode_key = Self::inode_key(file_id);
                            batch.delete_bytes(&inode_key);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(nfsstat3::NFS3ERR_NOTEMPTY);
                        }
                        // Delete the directory inode
                        let inode_key = Self::inode_key(file_id);
                        batch.delete_bytes(&inode_key);
                        // Decrement parent's nlink since we're removing a subdirectory
                        dir.nlink = dir.nlink.saturating_sub(1);
                        self.stats
                            .directories_deleted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Symlink(_) => {
                        // Delete the symlink inode
                        let inode_key = Self::inode_key(file_id);
                        batch.delete_bytes(&inode_key);
                        self.stats.links_deleted.fetch_add(1, Ordering::Relaxed);
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
                            batch
                                .put_bytes(&inode_key, &inode_data)
                                .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                        } else {
                            // Last link, delete the inode
                            let inode_key = Self::inode_key(file_id);
                            batch.delete_bytes(&inode_key);
                        }
                    }
                }

                batch.delete_bytes(&entry_key);

                let scan_key = Self::dir_scan_key(dirid, file_id, &name);
                batch.delete_bytes(&scan_key);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_key = Self::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&dir_key, &dir_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let stats_update = match &file_inode {
                    Inode::File(file) => {
                        // Only update stats if this was the last link (before decrement)
                        if original_nlink <= 1 {
                            Some(
                                self.global_stats
                                    .prepare_inode_remove(file_id, Some(file.size))
                                    .await,
                            )
                        } else {
                            None
                        }
                    }
                    Inode::Directory(_)
                    | Inode::Symlink(_)
                    | Inode::Fifo(_)
                    | Inode::Socket(_)
                    | Inode::CharDevice(_)
                    | Inode::BlockDevice(_) => {
                        // For special files, check original nlink too
                        if original_nlink <= 1 {
                            match &file_inode {
                                Inode::Directory(_) | Inode::Symlink(_) => {
                                    // These always get removed
                                    Some(
                                        self.global_stats.prepare_inode_remove(file_id, None).await,
                                    )
                                }
                                _ => {
                                    // Special files only if last link
                                    Some(
                                        self.global_stats.prepare_inode_remove(file_id, None).await,
                                    )
                                }
                            }
                        } else {
                            None
                        }
                    }
                };

                if let Some(ref update) = stats_update {
                    self.global_stats.add_to_batch(update, &mut batch)?;
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

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                self.metadata_cache
                    .remove(crate::cache::CacheKey::Metadata(file_id));
                self.metadata_cache
                    .remove(crate::cache::CacheKey::Metadata(dirid));
                self.small_file_cache
                    .remove(crate::cache::CacheKey::SmallFile(file_id));
                self.dir_entry_cache
                    .remove(crate::cache::CacheKey::DirEntry {
                        dir_id: dirid,
                        name: name.clone(),
                    });

                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
}
