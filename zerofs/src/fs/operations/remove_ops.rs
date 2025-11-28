use super::common::validate_filename;
use crate::fs::cache::CacheKey;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::operations::common::SMALL_FILE_TOMBSTONE_THRESHOLD;
use crate::fs::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};
use crate::fs::types::{AuthContext, InodeId};
use crate::fs::{CHUNK_SIZE, ZeroFS, get_current_time};
use std::sync::atomic::Ordering;

impl ZeroFS {
    pub async fn process_remove(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        name: &[u8],
    ) -> Result<(), FsError> {
        validate_filename(name)?;

        let creds = Credentials::from_auth_context(auth);

        let file_id = self.directory_store.get(dirid, name).await?;

        let _guards = self
            .lock_manager
            .acquire_multiple_write(vec![dirid, file_id])
            .await;

        let dir_inode = self.load_inode(dirid).await?;
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let is_dir = matches!(dir_inode, Inode::Directory(_));
        if !is_dir {
            return Err(FsError::NotDirectory);
        }

        // Re-check inside lock to verify entry still points to same inode
        let verified_id = self.directory_store.get(dirid, name).await?;
        if verified_id != file_id {
            return Err(FsError::NotFound);
        }

        let mut file_inode = self.load_inode(file_id).await?;

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
                let mut txn = self.new_transaction()?;
                let (now_sec, now_nsec) = get_current_time();

                match &mut file_inode {
                    Inode::File(file) => {
                        if file.nlink > 1 {
                            file.nlink -= 1;
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            self.inode_store.save(&mut txn, file_id, &file_inode)?;
                        } else {
                            let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64);

                            if total_chunks as usize <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                                self.chunk_store
                                    .delete_range(&mut txn, file_id, 0, total_chunks);
                            } else {
                                txn.add_tombstone(file_id, file.size);
                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            self.inode_store.delete(&mut txn, file_id);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(FsError::NotEmpty);
                        }
                        self.inode_store.delete(&mut txn, file_id);
                        dir.nlink = dir.nlink.saturating_sub(1);
                        self.stats
                            .directories_deleted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Symlink(_) => {
                        self.inode_store.delete(&mut txn, file_id);
                        self.stats.links_deleted.fetch_add(1, Ordering::Relaxed);
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
                        } else {
                            self.inode_store.delete(&mut txn, file_id);
                        }
                    }
                }

                self.directory_store.remove(&mut txn, dirid, name, file_id);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                // For directories and symlinks: always remove from stats
                // For files and special files: only remove if this is the last link
                let (file_size, should_always_remove_stats) = match &file_inode {
                    Inode::File(f) => (Some(f.size), false),
                    Inode::Directory(_) | Inode::Symlink(_) => (None, true),
                    _ => (None, false),
                };

                let stats_update = if should_always_remove_stats || original_nlink <= 1 {
                    Some(
                        self.global_stats
                            .prepare_inode_remove(file_id, file_size)
                            .await,
                    )
                } else {
                    None
                };

                if let Some(ref update) = stats_update {
                    self.global_stats.add_to_transaction(update, &mut txn)?;
                }

                let mut seq_guard = self.allocate_sequence();
                self.commit_transaction(txn, &mut seq_guard).await?;

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                let futures = vec![
                    CacheKey::Metadata(file_id),
                    CacheKey::Metadata(dirid),
                    CacheKey::DirEntry {
                        dir_id: dirid,
                        name: name.to_vec(),
                    },
                ];

                self.cache.remove_batch(futures);

                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}
