use super::common::validate_filename;
use crate::fs::errors::FsError;
use crate::fs::inode::Inode;
use crate::fs::key_codec::KeyCodec;
use crate::fs::operations::common::SMALL_FILE_TOMBSTONE_THRESHOLD;
use crate::fs::permissions::{AccessMode, Credentials, check_access, check_sticky_bit_delete};
use crate::fs::types::{AuthContext, InodeId};
use crate::fs::{CHUNK_SIZE, ZeroFS, get_current_time};
use slatedb::config::WriteOptions;
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
                let mut batch = self.db.new_write_batch();
                let (now_sec, now_nsec) = get_current_time();

                match &mut file_inode {
                    Inode::File(file) => {
                        if file.nlink > 1 {
                            file.nlink -= 1;
                            file.ctime = now_sec;
                            file.ctime_nsec = now_nsec;

                            let inode_key = KeyCodec::inode_key(file_id);
                            let inode_data = bincode::serialize(&file_inode)?;
                            batch.put_bytes(&inode_key, &inode_data);
                        } else {
                            let total_chunks = file.size.div_ceil(CHUNK_SIZE as u64) as usize;

                            if total_chunks <= SMALL_FILE_TOMBSTONE_THRESHOLD {
                                for chunk_idx in 0..total_chunks {
                                    let chunk_key = KeyCodec::chunk_key(file_id, chunk_idx as u64);
                                    batch.delete_bytes(&chunk_key);
                                }
                            } else {
                                let (timestamp, _) = get_current_time();
                                let tombstone_key = KeyCodec::tombstone_key(timestamp, file_id);
                                batch.put_bytes(
                                    &tombstone_key,
                                    &KeyCodec::encode_tombstone_size(file.size),
                                );
                                self.stats
                                    .tombstones_created
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            let inode_key = KeyCodec::inode_key(file_id);
                            batch.delete_bytes(&inode_key);
                            self.stats.files_deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Inode::Directory(subdir) => {
                        if subdir.entry_count > 0 {
                            return Err(FsError::NotEmpty);
                        }
                        let inode_key = KeyCodec::inode_key(file_id);
                        batch.delete_bytes(&inode_key);
                        dir.nlink = dir.nlink.saturating_sub(1);
                        self.stats
                            .directories_deleted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Inode::Symlink(_) => {
                        let inode_key = KeyCodec::inode_key(file_id);
                        batch.delete_bytes(&inode_key);
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

                            let inode_key = KeyCodec::inode_key(file_id);
                            let inode_data = bincode::serialize(&file_inode)?;
                            batch.put_bytes(&inode_key, &inode_data);
                        } else {
                            let inode_key = KeyCodec::inode_key(file_id);
                            batch.delete_bytes(&inode_key);
                        }
                    }
                }

                batch.delete_bytes(&entry_key);

                let scan_key = KeyCodec::dir_scan_key(dirid, file_id, name);
                batch.delete_bytes(&scan_key);

                dir.entry_count = dir.entry_count.saturating_sub(1);
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let dir_key = KeyCodec::inode_key(dirid);
                let dir_data = bincode::serialize(&dir_inode)?;
                batch.put_bytes(&dir_key, &dir_data);

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
                    .map_err(|_| FsError::IoError)?;

                if let Some(update) = stats_update {
                    self.global_stats.commit_update(&update);
                }

                self.cache
                    .remove(crate::fs::cache::CacheKey::Metadata(file_id));
                self.cache
                    .remove(crate::fs::cache::CacheKey::Metadata(dirid));
                self.cache.remove(crate::fs::cache::CacheKey::DirEntry {
                    dir_id: dirid,
                    name: name.to_vec(),
                });

                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok(())
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}
