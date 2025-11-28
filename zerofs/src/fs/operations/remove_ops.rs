use super::common::validate_filename;
use crate::fs::cache::CacheKey;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::key_codec::KeyCodec;
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

        let entry_key = KeyCodec::dir_entry_key(dirid, name);
        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .ok_or(FsError::NotFound)?;

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let file_id = u64::from_le_bytes(bytes);

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

        let entry_data = self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .ok_or(FsError::NotFound)?;

        let mut verify_bytes = [0u8; 8];
        verify_bytes.copy_from_slice(&entry_data[..8]);

        if u64::from_le_bytes(verify_bytes) != file_id {
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

                            txn.save_inode(file_id, &file_inode)?;
                        } else {
                            let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;

                            if total_chunks <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                                for chunk_idx in 0..total_chunks {
                                    txn.delete_chunk(file_id, chunk_idx as u64);
                                }
                            } else {
                                txn.add_tombstone(file_id, file.size);
                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            txn.delete_inode(file_id);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(FsError::NotEmpty);
                        }
                        txn.delete_inode(file_id);
                        dir.nlink = dir.nlink.saturating_sub(1);
                        self.stats
                            .directories_deleted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Symlink(_) => {
                        txn.delete_inode(file_id);
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

                            txn.save_inode(file_id, &file_inode)?;
                        } else {
                            txn.delete_inode(file_id);
                        }
                    }
                }

                txn.remove_dir_entry(dirid, name, file_id);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                txn.save_inode(dirid, &dir_inode)?;

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
