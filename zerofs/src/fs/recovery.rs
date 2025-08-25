use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::{ZeroFS, get_current_time};
use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

impl ZeroFS {
    pub async fn run_recovery_if_needed(&mut self) -> Result<(), FsError> {
        let clean_shutdown_key = Self::clean_shutdown_key();

        let was_clean = self
            .db
            .get_bytes(&clean_shutdown_key)
            .await
            .map_err(|_| FsError::IoError)?
            .and_then(|bytes| bincode::deserialize::<bool>(&bytes).ok())
            .unwrap_or(false);

        let mut batch = self.db.new_write_batch();
        let false_bytes = bincode::serialize(&false)?;
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        if !was_clean {
            info!("Unclean shutdown detected, running recovery...");
            self.recover_inode_consistency().await?;
            info!("Recovery completed successfully");
        } else {
            info!("Clean shutdown detected, skipping recovery");
        }

        Ok(())
    }

    pub async fn mark_clean_shutdown(&self) -> Result<(), FsError> {
        info!("Marking clean shutdown");

        self.db.flush().await.map_err(|_| FsError::IoError)?;

        let clean_shutdown_key = Self::clean_shutdown_key();

        let mut batch = self.db.new_write_batch();
        let true_bytes = bincode::serialize(&true)?;
        batch.put_bytes(&clean_shutdown_key, &true_bytes);

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        self.db.flush().await.map_err(|_| FsError::IoError)?;

        self.db.close().await.map_err(|_| FsError::IoError)?;

        Ok(())
    }

    /// Recovery mechanism for inode allocation consistency after ungraceful shutdown.
    ///
    /// This should rarely if ever be needed in practice.
    /// It exists to handle a very specific edge case: There's a theoretical possibility
    /// that directory entries get persisted while the inode allocation counter doesn't,
    /// particularly during forceful termination (kill -9, power loss, kernel panic).
    ///
    /// This can happen because:
    /// 1. Multiple tasks allocate inodes concurrently (counter increments in memory)
    /// 2. A batch with higher inode IDs gets flushed to object storage
    /// 3. A crash occurs before batches with lower IDs get flushed
    ///
    /// Without this recovery, the counter could be reused, causing old directory entries
    /// to suddenly point to newly created files - a serious data corruption issue.
    ///
    /// This recovery process:
    /// 1. Scans for any inodes with IDs >= our stored counter (orphaned entries)
    /// 2. Advances the counter past the highest found inode
    /// 3. Moves orphaned entries to lost+found to prevent them from appearing in wrong place and getting lost
    async fn recover_inode_consistency(&mut self) -> Result<(), FsError> {
        let stored_counter = self.next_inode_id.load(Ordering::SeqCst);
        info!(
            "Starting inode consistency recovery, counter at: {}",
            stored_counter
        );

        let orphaned = self.scan_for_orphaned_inodes(stored_counter).await?;

        if orphaned.is_empty() {
            info!("No orphaned inodes found");
            return Ok(());
        }

        let orphaned_count = orphaned.len();
        info!("Found {} orphaned inodes above counter", orphaned_count);

        let max_inode = orphaned.iter().map(|&(id, _)| id).max().unwrap();

        info!(
            "Updating inode counter from {} to {}",
            stored_counter,
            max_inode + 1
        );

        self.next_inode_id.store(max_inode + 1, Ordering::SeqCst);

        let lost_found_id = self.get_or_create_lost_found().await?;

        let mut batch = self.db.new_write_batch();

        for (inode_id, mut inode) in orphaned {
            warn!("Moving orphaned inode {} to lost+found", inode_id);

            match &mut inode {
                Inode::File(f) => f.parent = lost_found_id,
                Inode::Directory(d) => d.parent = lost_found_id,
                Inode::Symlink(s) => s.parent = lost_found_id,
                Inode::Fifo(s)
                | Inode::Socket(s)
                | Inode::CharDevice(s)
                | Inode::BlockDevice(s) => s.parent = lost_found_id,
            }

            let inode_key = Self::inode_key(inode_id);
            let inode_data = bincode::serialize(&inode)?;
            batch.put_bytes(&inode_key, &inode_data);

            let name = format!("orphan_{}", inode_id);
            let entry_key = Self::dir_entry_key(lost_found_id, &name);
            batch.put_bytes(&entry_key, &inode_id.to_le_bytes());

            let scan_key = Self::dir_scan_key(lost_found_id, inode_id, &name);
            batch.put_bytes(&scan_key, &inode_id.to_le_bytes());
        }

        let counter_key = Self::counter_key();
        batch.put_bytes(&counter_key, &(max_inode + 1).to_le_bytes());

        let mut lost_found_inode = self.load_inode(lost_found_id).await?;
        if let Inode::Directory(ref mut dir) = lost_found_inode {
            dir.entry_count += orphaned_count as u64;
            let (now_sec, now_nsec) = get_current_time();
            dir.mtime = now_sec;
            dir.mtime_nsec = now_nsec;

            let lost_found_key = Self::inode_key(lost_found_id);
            let lost_found_data = bincode::serialize(&lost_found_inode)?;
            batch.put_bytes(&lost_found_key, &lost_found_data);
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

        info!("Moved {} orphaned inodes to lost+found", orphaned_count);
        Ok(())
    }

    async fn scan_for_orphaned_inodes(
        &self,
        counter: InodeId,
    ) -> Result<Vec<(InodeId, Inode)>, FsError> {
        use bytes::Bytes;

        let mut orphaned = Vec::new();
        let mut max_seen = counter;

        // Create a range from the counter to scan all inodes with IDs >= counter
        // We scan the inode keyspace starting from the counter value
        let start_key = Self::inode_key(counter);
        // End at the next prefix (inodes are PREFIX_INODE = 0x01, so end at 0x02)
        let end_key = Bytes::from(vec![crate::fs::PREFIX_INODE + 1]);
        let range = start_key..end_key;

        let iter = self.db.scan(range).await.map_err(|_| FsError::IoError)?;
        futures::pin_mut!(iter);

        while let Some(Ok((key, value))) = futures::StreamExt::next(&mut iter).await {
            // Parse the inode ID from the key
            if key.len() == 9 && key[0] == crate::fs::PREFIX_INODE {
                let mut id_bytes = [0u8; 8];
                id_bytes.copy_from_slice(&key[1..9]);
                let inode_id = u64::from_be_bytes(id_bytes);

                // Track the maximum inode ID we've seen
                if inode_id > max_seen {
                    max_seen = inode_id;
                }

                // Orphaned inodes
                if inode_id >= counter
                    && let Ok(inode) = bincode::deserialize::<Inode>(&value)
                {
                    orphaned.push((inode_id, inode));
                }
            }
        }

        if !orphaned.is_empty() {
            info!(
                "Found {} orphaned inodes with IDs >= {} (max: {})",
                orphaned.len(),
                counter,
                max_seen
            );
        }

        Ok(orphaned)
    }

    async fn get_or_create_lost_found(&mut self) -> Result<InodeId, FsError> {
        let lost_found_name = "lost+found";

        match self.lookup_by_name(0, lost_found_name).await {
            Ok(inode_id) => {
                info!(
                    "Using existing lost+found directory with inode {}",
                    inode_id
                );
                Ok(inode_id)
            }
            Err(_) => {
                let auth = crate::fs::types::AuthContext {
                    uid: 0,
                    gid: 0,
                    gids: vec![],
                };
                let creds = crate::fs::permissions::Credentials::from_auth_context(&auth);

                let attr = crate::fs::types::SetAttributes {
                    mode: crate::fs::types::SetMode::Set(0o755),
                    uid: crate::fs::types::SetUid::Set(0),
                    gid: crate::fs::types::SetGid::Set(0),
                    ..Default::default()
                };

                let (lost_found_id, _) = self
                    .process_mkdir(&creds, 0, lost_found_name.as_bytes(), &attr)
                    .await?;

                info!("Created lost+found directory with inode {}", lost_found_id);
                Ok(lost_found_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::FileInode;

    #[tokio::test]
    async fn test_fsck_lost_found_repair() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        // Manually set clean shutdown flag without closing the DB
        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let true_bytes = bincode::serialize(&true).unwrap();
        batch.put_bytes(&clean_shutdown_key, &true_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let current_counter = fs.next_inode_id.load(Ordering::SeqCst);
        println!("Current inode counter: {}", current_counter);

        let orphaned_id = current_counter + 100;
        let (now_sec, now_nsec) = get_current_time();
        let orphaned_inode = Inode::File(FileInode {
            size: 1024,
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        });

        let inode_key = ZeroFS::inode_key(orphaned_id);
        let inode_data = bincode::serialize(&orphaned_inode).unwrap();
        let mut batch = fs.db.new_write_batch();
        batch.put_bytes(&inode_key, &inode_data);

        let entry_key = ZeroFS::dir_entry_key(0, "orphaned_file.txt");
        batch.put_bytes(&entry_key, &orphaned_id.to_le_bytes());

        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        fs.run_recovery_if_needed().await.unwrap();

        let new_counter = fs.next_inode_id.load(Ordering::SeqCst);
        assert!(
            new_counter > orphaned_id,
            "Counter should be updated past orphaned inode"
        );
        println!("Counter updated to: {}", new_counter);

        let lost_found_entry = ZeroFS::dir_entry_key(0, "lost+found"); // Root is at inode 0
        let lost_found_data = fs.db.get_bytes(&lost_found_entry).await.unwrap();
        assert!(lost_found_data.is_some(), "lost+found should exist");

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&lost_found_data.unwrap()[..8]);
        let lost_found_id = u64::from_le_bytes(bytes);
        println!("lost+found created with inode: {}", lost_found_id);

        let orphan_entry_key =
            ZeroFS::dir_entry_key(lost_found_id, &format!("orphan_{}", orphaned_id));
        let orphan_entry = fs.db.get_bytes(&orphan_entry_key).await.unwrap();
        assert!(
            orphan_entry.is_some(),
            "Orphaned file should be in lost+found"
        );

        let updated_inode_data = fs.db.get_bytes(&inode_key).await.unwrap().unwrap();
        let updated_inode: Inode = bincode::deserialize(&updated_inode_data).unwrap();
        if let Inode::File(f) = updated_inode {
            assert_eq!(
                f.parent, lost_found_id,
                "Parent should be updated to lost+found"
            );
        } else {
            panic!("Expected file inode");
        }

        let clean_flag = fs.db.get_bytes(&clean_shutdown_key).await.unwrap().unwrap();
        let is_clean: bool = bincode::deserialize(&clean_flag).unwrap();
        assert!(
            !is_clean,
            "Clean shutdown flag should be false during operation"
        );
    }

    #[tokio::test]
    async fn test_clean_shutdown_skips_recovery() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        // Manually set clean shutdown flag without closing the DB
        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let true_bytes = bincode::serialize(&true).unwrap();
        batch.put_bytes(&clean_shutdown_key, &true_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let flag_data = fs.db.get_bytes(&clean_shutdown_key).await.unwrap().unwrap();
        let is_clean: bool = bincode::deserialize(&flag_data).unwrap();
        assert!(is_clean, "Clean shutdown flag should be true");

        fs.run_recovery_if_needed().await.unwrap();

        let flag_data = fs.db.get_bytes(&clean_shutdown_key).await.unwrap().unwrap();
        let is_clean: bool = bincode::deserialize(&flag_data).unwrap();
        assert!(!is_clean, "Clean shutdown flag should be reset to false");
    }

    #[tokio::test]
    async fn test_recovery_with_no_orphans() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let counter_before = fs.next_inode_id.load(Ordering::SeqCst);

        fs.run_recovery_if_needed().await.unwrap();

        let counter_after = fs.next_inode_id.load(Ordering::SeqCst);
        assert_eq!(
            counter_before, counter_after,
            "Counter should not change when no orphans"
        );

        let lost_found_entry = ZeroFS::dir_entry_key(0, "lost+found");
        let lost_found_data = fs.db.get_bytes(&lost_found_entry).await.unwrap();
        assert!(
            lost_found_data.is_none(),
            "lost+found should not be created when no orphans"
        );
    }

    #[tokio::test]
    async fn test_recovery_with_multiple_orphans() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        let current_counter = fs.next_inode_id.load(Ordering::SeqCst);

        // Create multiple orphaned inodes, including one far ahead to test
        // that we find all orphans regardless of gaps
        let orphan_ids = vec![
            current_counter + 10,
            current_counter + 20,
            current_counter + 30,
            current_counter + 50_000, // Far ahead orphan with large gap
        ];

        let mut batch = fs.db.new_write_batch();
        for &id in &orphan_ids {
            let (now_sec, now_nsec) = get_current_time();
            let inode = Inode::File(FileInode {
                size: 100,
                mtime: now_sec,
                mtime_nsec: now_nsec,
                ctime: now_sec,
                ctime_nsec: now_nsec,
                atime: now_sec,
                atime_nsec: now_nsec,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                parent: 0,
                nlink: 1,
            });
            let key = ZeroFS::inode_key(id);
            let data = bincode::serialize(&inode).unwrap();
            batch.put_bytes(&key, &data);
        }
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        fs.run_recovery_if_needed().await.unwrap();

        // Counter should be past the highest orphan (50,000)
        let new_counter = fs.next_inode_id.load(Ordering::SeqCst);
        assert!(
            new_counter > current_counter + 50_000,
            "Counter should be past highest orphan (was {}, expected > {})",
            new_counter,
            current_counter + 50_000
        );

        // Verify lost+found was created
        let lost_found_entry = ZeroFS::dir_entry_key(0, "lost+found");
        let lost_found_data = fs.db.get_bytes(&lost_found_entry).await.unwrap();
        assert!(lost_found_data.is_some(), "lost+found should exist");

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&lost_found_data.unwrap()[..8]);
        let lost_found_id = u64::from_le_bytes(bytes);

        // Verify all orphans are in lost+found, including the far one
        for &id in &orphan_ids {
            let orphan_entry = ZeroFS::dir_entry_key(lost_found_id, &format!("orphan_{}", id));
            let entry_data = fs.db.get_bytes(&orphan_entry).await.unwrap();
            assert!(
                entry_data.is_some(),
                "Orphan {} should be in lost+found (including far orphan)",
                id
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_idempotency() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        // Create an orphaned inode
        let current_counter = fs.next_inode_id.load(Ordering::SeqCst);
        let orphaned_id = current_counter + 50;

        let (now_sec, now_nsec) = get_current_time();
        let inode = Inode::File(FileInode {
            size: 200,
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        });

        let key = ZeroFS::inode_key(orphaned_id);
        let data = bincode::serialize(&inode).unwrap();
        let mut batch = fs.db.new_write_batch();
        batch.put_bytes(&key, &data);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Set unclean shutdown
        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Run recovery first time
        fs.run_recovery_if_needed().await.unwrap();

        let counter_after_first = fs.next_inode_id.load(Ordering::SeqCst);

        // Verify lost+found was created and has the orphan
        let lost_found_entry = ZeroFS::dir_entry_key(0, "lost+found");
        let lost_found_data = fs.db.get_bytes(&lost_found_entry).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&lost_found_data[..8]);
        let lost_found_id = u64::from_le_bytes(bytes);

        // Verify orphan is in lost+found after first recovery
        let orphan_entry = ZeroFS::dir_entry_key(lost_found_id, &format!("orphan_{}", orphaned_id));
        let entry_data = fs.db.get_bytes(&orphan_entry).await.unwrap();
        assert!(
            entry_data.is_some(),
            "Orphan should be in lost+found after first recovery"
        );

        // Simulate another unclean shutdown
        let mut batch = fs.db.new_write_batch();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Run recovery again - should be idempotent (no new orphans to find)
        fs.run_recovery_if_needed().await.unwrap();

        let counter_after_second = fs.next_inode_id.load(Ordering::SeqCst);
        assert_eq!(
            counter_after_first, counter_after_second,
            "Counter should remain the same on second recovery"
        );

        // Verify the orphan is still in lost+found (wasn't duplicated or lost)
        let entry_data = fs.db.get_bytes(&orphan_entry).await.unwrap();
        assert!(
            entry_data.is_some(),
            "Orphan should still be in lost+found after second recovery"
        );
    }

    #[tokio::test]
    async fn test_recovery_finds_orphans_with_large_gaps() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        let current_counter = fs.next_inode_id.load(Ordering::SeqCst);

        // Create orphans with a big gap, simulating non-contiguous allocation
        let orphan_ids = vec![
            current_counter + 10,     // Initial orphan
            current_counter + 80_000, // Far orphan with large gap
        ];

        let mut batch = fs.db.new_write_batch();
        for &id in &orphan_ids {
            let (now_sec, now_nsec) = get_current_time();
            let inode = Inode::File(FileInode {
                size: 256,
                mtime: now_sec,
                mtime_nsec: now_nsec,
                ctime: now_sec,
                ctime_nsec: now_nsec,
                atime: now_sec,
                atime_nsec: now_nsec,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                parent: 0,
                nlink: 1,
            });
            let key = ZeroFS::inode_key(id);
            let data = bincode::serialize(&inode).unwrap();
            batch.put_bytes(&key, &data);
        }
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Set unclean shutdown
        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Run recovery - should find all orphans regardless of gaps
        fs.run_recovery_if_needed().await.unwrap();

        // Counter should be past the far orphan
        let new_counter = fs.next_inode_id.load(Ordering::SeqCst);
        assert!(
            new_counter > current_counter + 80_000,
            "Counter should be past far orphan at 80k (was {}, expected > {})",
            new_counter,
            current_counter + 80_000
        );

        // Verify lost+found was created
        let lost_found_entry = ZeroFS::dir_entry_key(0, "lost+found");
        let lost_found_data = fs.db.get_bytes(&lost_found_entry).await.unwrap();
        assert!(lost_found_data.is_some(), "lost+found should exist");

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&lost_found_data.unwrap()[..8]);
        let lost_found_id = u64::from_le_bytes(bytes);

        // Verify both orphans are in lost+found despite the large gap
        for &id in &orphan_ids {
            let orphan_entry = ZeroFS::dir_entry_key(lost_found_id, &format!("orphan_{}", id));
            let entry_data = fs.db.get_bytes(&orphan_entry).await.unwrap();
            assert!(
                entry_data.is_some(),
                "Orphan {} should be in lost+found (scan covers entire inode keyspace)",
                id
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_with_existing_lost_found() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();

        // Create lost+found directory using proper filesystem methods
        let auth = crate::fs::types::AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let creds = crate::fs::permissions::Credentials::from_auth_context(&auth);

        let attr = crate::fs::types::SetAttributes {
            mode: crate::fs::types::SetMode::Set(0o755),
            uid: crate::fs::types::SetUid::Set(0),
            gid: crate::fs::types::SetGid::Set(0),
            ..Default::default()
        };

        let (lost_found_id, _) = fs
            .process_mkdir(&creds, 0, b"lost+found", &attr)
            .await
            .unwrap();

        // Now create an orphaned inode
        let current_counter = fs.next_inode_id.load(Ordering::SeqCst);
        let orphaned_id = current_counter + 100;

        let (now_sec, now_nsec) = get_current_time();
        let orphaned_inode = Inode::File(FileInode {
            size: 500,
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            parent: 0,
            nlink: 1,
        });

        let mut batch = fs.db.new_write_batch();
        let key = ZeroFS::inode_key(orphaned_id);
        let data = bincode::serialize(&orphaned_inode).unwrap();
        batch.put_bytes(&key, &data);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Set unclean shutdown
        let clean_shutdown_key = ZeroFS::clean_shutdown_key();
        let mut batch = fs.db.new_write_batch();
        let false_bytes = bincode::serialize(&false).unwrap();
        batch.put_bytes(&clean_shutdown_key, &false_bytes);
        fs.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();

        // Run recovery - should reuse existing lost+found
        fs.run_recovery_if_needed().await.unwrap();

        // Verify the existing lost+found was used
        let entry_key = ZeroFS::dir_entry_key(0, "lost+found");
        let lost_found_data = fs.db.get_bytes(&entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&lost_found_data[..8]);
        let found_id = u64::from_le_bytes(bytes);
        assert_eq!(found_id, lost_found_id, "Should reuse existing lost+found");

        // Verify orphan was added to it
        let orphan_entry = ZeroFS::dir_entry_key(lost_found_id, &format!("orphan_{}", orphaned_id));
        let entry_data = fs.db.get_bytes(&orphan_entry).await.unwrap();
        assert!(
            entry_data.is_some(),
            "Orphan should be in existing lost+found"
        );
    }
}
