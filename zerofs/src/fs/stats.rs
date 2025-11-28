use super::STATS_SHARDS;
use super::errors::FsError;
use super::inode::InodeId;
use super::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct StatsShardData {
    pub used_bytes: u64,
    pub used_inodes: u64,
}

pub struct StatsShard {
    pub used_bytes: AtomicU64,
    pub used_inodes: AtomicU64,
    pub lock: RwLock<()>,
}

pub struct FileSystemGlobalStats {
    pub shards: Vec<StatsShard>,
}

pub struct StatsUpdate<'a> {
    pub shard_id: usize,
    pub shard_key: Bytes,
    pub shard_data: StatsShardData,
    pub _guard: tokio::sync::RwLockWriteGuard<'a, ()>,
}

impl FileSystemGlobalStats {
    pub fn new() -> Self {
        let shards = (0..STATS_SHARDS)
            .map(|_| StatsShard {
                used_bytes: AtomicU64::new(0),
                used_inodes: AtomicU64::new(0),
                lock: RwLock::new(()),
            })
            .collect();
        Self { shards }
    }

    pub fn get_totals(&self) -> (u64, u64) {
        let mut total_bytes = 0u64;
        let mut total_inodes = 0u64;
        for shard in &self.shards {
            total_bytes += shard.used_bytes.load(Ordering::Relaxed);
            total_inodes += shard.used_inodes.load(Ordering::Relaxed);
        }
        (total_bytes, total_inodes)
    }

    /// Prepare a statistics update for a new inode creation
    pub async fn prepare_inode_create(&self, inode_id: InodeId) -> StatsUpdate<'_> {
        let shard_id = inode_id as usize % STATS_SHARDS;
        let shard = &self.shards[shard_id];

        let guard = shard.lock.write().await;

        let mut shard_data = StatsShardData {
            used_bytes: shard.used_bytes.load(Ordering::Relaxed),
            used_inodes: shard.used_inodes.load(Ordering::Relaxed),
        };
        shard_data.used_inodes = shard_data.used_inodes.saturating_add(1);

        StatsUpdate {
            shard_id,
            shard_key: KeyCodec::stats_shard_key(shard_id),
            shard_data,
            _guard: guard,
        }
    }

    /// Prepare a statistics update for inode removal
    pub async fn prepare_inode_remove(
        &self,
        inode_id: InodeId,
        file_size: Option<u64>,
    ) -> StatsUpdate<'_> {
        let shard_id = inode_id as usize % STATS_SHARDS;
        let shard = &self.shards[shard_id];

        let guard = shard.lock.write().await;

        let mut shard_data = StatsShardData {
            used_bytes: shard.used_bytes.load(Ordering::Relaxed),
            used_inodes: shard.used_inodes.load(Ordering::Relaxed),
        };

        shard_data.used_inodes = shard_data.used_inodes.saturating_sub(1);
        if let Some(size) = file_size {
            shard_data.used_bytes = shard_data.used_bytes.saturating_sub(size);
        }

        StatsUpdate {
            shard_id,
            shard_key: KeyCodec::stats_shard_key(shard_id),
            shard_data,
            _guard: guard,
        }
    }

    /// Prepare a statistics update for file size change
    pub async fn prepare_size_change(
        &self,
        inode_id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Option<StatsUpdate<'_>> {
        if old_size == new_size {
            return None;
        }

        let shard_id = inode_id as usize % STATS_SHARDS;
        let shard = &self.shards[shard_id];

        let guard = shard.lock.write().await;

        let mut shard_data = StatsShardData {
            used_bytes: shard.used_bytes.load(Ordering::Relaxed),
            used_inodes: shard.used_inodes.load(Ordering::Relaxed),
        };

        if new_size > old_size {
            shard_data.used_bytes = shard_data.used_bytes.saturating_add(new_size - old_size);
        } else {
            shard_data.used_bytes = shard_data.used_bytes.saturating_sub(old_size - new_size);
        }

        Some(StatsUpdate {
            shard_id,
            shard_key: KeyCodec::stats_shard_key(shard_id),
            shard_data,
            _guard: guard,
        })
    }

    pub fn add_to_transaction(
        &self,
        update: &StatsUpdate,
        txn: &mut crate::encryption::EncryptedTransaction,
    ) -> Result<(), FsError> {
        let shard_bytes = bincode::serialize(&update.shard_data)?;
        txn.put_bytes(&update.shard_key, Bytes::from(shard_bytes));

        Ok(())
    }

    /// Commit the statistics update to memory after successful database write
    pub fn commit_update(&self, update: &StatsUpdate) {
        let shard = &self.shards[update.shard_id];
        shard
            .used_bytes
            .store(update.shard_data.used_bytes, Ordering::Relaxed);
        shard
            .used_inodes
            .store(update.shard_data.used_inodes, Ordering::Relaxed);
    }

    /// Load statistics from persistent storage
    pub fn load_shard(&self, shard_id: usize, data: &StatsShardData) {
        if shard_id < self.shards.len() {
            self.shards[shard_id]
                .used_bytes
                .store(data.used_bytes, Ordering::Relaxed);
            self.shards[shard_id]
                .used_inodes
                .store(data.used_inodes, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{AuthContext, SetAttributes};

    fn test_creds() -> Credentials {
        Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        }
    }

    fn test_auth() -> AuthContext {
        AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        }
    }

    #[tokio::test]
    async fn test_stats_initialization() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (bytes, inodes) = fs.global_stats.get_totals();

        // Should start empty - root directory is created during filesystem setup
        // but stats are initialized empty and loaded from persistent storage
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0);
    }

    #[tokio::test]
    async fn test_stats_file_creation() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();

        // Create a file
        let (_file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // New file has 0 bytes
        assert_eq!(inodes, 1); // Just the file
    }

    #[tokio::test]
    async fn test_stats_file_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create a file
        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Write 1000 bytes
        let data = vec![0u8; 1000];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000);
        assert_eq!(inodes, 1);

        // Write more data (extending the file)
        let data = vec![1u8; 500];
        fs.write(&auth, file_id, 1000, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1500);
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_file_overwrite() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create a file and write initial data
        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![0u8; 1000];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        // Overwrite part of the file (no size change)
        let data = vec![1u8; 500];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Size unchanged
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_sparse_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create a file
        let (file_id, _) = fs
            .create(&creds, 0, b"sparse.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Write 1 byte at offset 1GB (creating a sparse file)
        let data = vec![42u8; 1];
        let offset = 1_000_000_000;
        fs.write(&auth, file_id, offset, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1_000_000_001); // Logical size
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_file_removal() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create and write to a file
        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![0u8; 5000];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 5000);
        assert_eq!(inodes, 1);

        // Remove the file
        fs.remove(&test_auth(), 0, b"test.txt").await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0); // No tracked inodes
    }

    #[tokio::test]
    async fn test_stats_directory_operations() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directories
        let (dir1_id, _) = fs
            .mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();

        let (_dir2_id, _) = fs
            .mkdir(&test_creds(), dir1_id, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // Directories don't consume bytes
        assert_eq!(inodes, 2); // dir1 + dir2
    }

    #[tokio::test]
    async fn test_stats_symlink() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a symlink
        let (_link_id, _) = fs
            .symlink(
                &test_creds(),
                0,
                b"link",
                b"/target/path",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // Symlinks don't count as bytes
        assert_eq!(inodes, 1); // Just the symlink
    }

    #[tokio::test]
    async fn test_stats_hard_links() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a file with content
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![0u8; 1000];
        fs.write(&test_auth(), file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        // Create a hard link
        fs.link(&test_auth(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Same data, not duplicated
        assert_eq!(inodes, 1); // Still just 2 inodes (root + file)

        // Remove original - stats should remain
        fs.remove(&test_auth(), 0, b"original.txt").await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Data still exists via hard link
        assert_eq!(inodes, 1);

        // Remove hard link - now stats should update
        fs.remove(&test_auth(), 0, b"hardlink.txt").await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0); // No tracked inodes
    }

    #[tokio::test]
    async fn test_stats_file_truncate() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a file with content
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![0u8; 10000];
        fs.write(&test_auth(), file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        // Truncate to smaller size
        use crate::fs::types::SetSize;

        let setattr = SetAttributes {
            size: SetSize::Set(5000),
            ..Default::default()
        };

        fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 5000);
        assert_eq!(inodes, 1);

        // Extend to larger size
        let setattr = SetAttributes {
            size: SetSize::Set(15000),
            ..Default::default()
        };

        fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 15000);
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_concurrent_operations() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create multiple files concurrently
        let mut handles = vec![];

        for i in 0..10 {
            let fs_clone = fs.clone();
            let handle = tokio::spawn(async move {
                let fname = format!("file{i}.txt");
                let creds = test_creds();
                let auth = test_auth();
                let (file_id, _) = fs_clone
                    .create(&creds, 0, fname.as_bytes(), &SetAttributes::default())
                    .await
                    .unwrap();

                // Write different amounts of data
                let data = vec![0u8; (i + 1) * 1000];
                fs_clone
                    .write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        let (bytes, inodes) = fs.global_stats.get_totals();

        // Sum of 1000 + 2000 + ... + 10000 = 55000
        assert_eq!(bytes, 55000);
        assert_eq!(inodes, 10); // 10 files
    }

    #[tokio::test]
    async fn test_stats_sharding_distribution() {
        let stats = FileSystemGlobalStats::new();

        // Create inodes and verify they're distributed across shards
        let mut shard_counts = vec![0u32; STATS_SHARDS];

        for i in 0..1000 {
            let shard_id = i % STATS_SHARDS;
            shard_counts[shard_id] += 1;

            let update = stats.prepare_inode_create(i as u64).await;
            assert_eq!(update.shard_id, shard_id);
        }

        // Verify reasonable distribution (all shards should have some inodes)
        for count in &shard_counts {
            assert!(*count > 0);
        }
    }

    #[tokio::test]
    async fn test_fsstat_reporting() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create some files
        for i in 0..5 {
            let fname = format!("file{i}.txt");
            let (file_id, _) = fs
                .create(&creds, 0, fname.as_bytes(), &SetAttributes::default())
                .await
                .unwrap();

            let data = vec![0u8; 1_000_000]; // 1MB each
            fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
                .await
                .unwrap();
        }

        const TOTAL_BYTES: u64 = 8 << 60; // 8 EiB
        const TOTAL_INODES: u64 = 1 << 48;

        let next_inode_id = fs.inode_store.next_id();
        let (used_bytes, used_inodes) = fs.global_stats.get_totals();

        let fbytes = TOTAL_BYTES - used_bytes;
        let ffiles = TOTAL_INODES - next_inode_id;
        let tfiles = used_inodes + ffiles;

        assert_eq!(fbytes, TOTAL_BYTES - 5_000_000);
        assert_eq!(tfiles, used_inodes + (TOTAL_INODES - next_inode_id));
        assert_eq!(ffiles, TOTAL_INODES - next_inode_id);
    }

    #[tokio::test]
    async fn test_stats_rename_without_replacement() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        let (file_id, _) = fs
            .create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![0u8; 1000];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 1000);
        assert_eq!(inodes_before, 1);

        // Rename without replacing anything
        fs.rename(&auth, 0, b"original.txt", 0, b"renamed.txt")
            .await
            .unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 1000); // No change
        assert_eq!(inodes_after, 1); // No change
    }

    #[tokio::test]
    async fn test_stats_rename_replacing_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create source file with 1000 bytes
        let (file1_id, _) = fs
            .create(&test_creds(), 0, b"source.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data1 = vec![0u8; 1000];
        fs.write(&test_auth(), file1_id, 0, &Bytes::copy_from_slice(&data1))
            .await
            .unwrap();

        // Create target file with 2000 bytes
        let (file2_id, _) = fs
            .create(&test_creds(), 0, b"target.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data2 = vec![0u8; 2000];
        fs.write(&test_auth(), file2_id, 0, &Bytes::copy_from_slice(&data2))
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 3000); // 1000 + 2000
        assert_eq!(inodes_before, 2);

        // Rename source over target (replacing it)
        fs.rename(&test_auth(), 0, b"source.txt", 0, b"target.txt")
            .await
            .unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 1000); // Only source file remains
        assert_eq!(inodes_after, 1); // Only one inode remains
    }

    #[tokio::test]
    async fn test_stats_rename_replacing_file_with_hard_links() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create source file
        let (source_id, _) = fs
            .create(&test_creds(), 0, b"source.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data1 = vec![0u8; 500];
        fs.write(&test_auth(), source_id, 0, &Bytes::copy_from_slice(&data1))
            .await
            .unwrap();

        // Create target file with 1500 bytes
        let (target_id, _) = fs
            .create(&test_creds(), 0, b"target.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data2 = vec![0u8; 1500];
        fs.write(&test_auth(), target_id, 0, &Bytes::copy_from_slice(&data2))
            .await
            .unwrap();

        // Create a hard link to target
        fs.link(&test_auth(), target_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 2000); // 500 + 1500
        assert_eq!(inodes_before, 2); // source + target (hardlink doesn't add inode)

        // Rename source over target (which has a hard link)
        fs.rename(&test_auth(), 0, b"source.txt", 0, b"target.txt")
            .await
            .unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 2000); // Both files still exist (source + hardlinked target)
        assert_eq!(inodes_after, 2); // Both inodes remain

        // Verify hardlink still works
        let inode = fs.inode_store.get(target_id).await.unwrap();
        let attrs: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &inode,
            id: target_id,
        }
        .into();
        assert_eq!(attrs.size, 1500); // Original target size via hardlink
    }

    #[tokio::test]
    async fn test_stats_rename_replacing_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create source directory
        let (_source_dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"sourcedir", &SetAttributes::default())
            .await
            .unwrap();

        // Create target directory (must be empty to be replaceable)
        let (_target_dir_id, _) = fs
            .mkdir(&test_creds(), 0, b"targetdir", &SetAttributes::default())
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 0); // Directories don't consume bytes
        assert_eq!(inodes_before, 2); // Two directories

        // Rename source directory over target directory
        fs.rename(&test_auth(), 0, b"sourcedir", 0, b"targetdir")
            .await
            .unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 0);
        assert_eq!(inodes_after, 1); // Only source directory remains
    }

    #[tokio::test]
    async fn test_stats_rename_replacing_symlink() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create a file to rename
        let (file_id, _) = fs
            .create(&creds, 0, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data = vec![0u8; 750];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        // Create a symlink
        let (_link_id, _) = fs
            .symlink(
                &creds,
                0,
                b"link",
                b"/some/target",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 750);
        assert_eq!(inodes_before, 2); // file + symlink

        // Rename file over symlink
        fs.rename(&auth, 0, b"file.txt", 0, b"link").await.unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 750);
        assert_eq!(inodes_after, 1); // Only file remains
    }

    #[tokio::test]
    async fn test_stats_rename_cross_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create two directories
        let (dir1_id, _) = fs
            .mkdir(&creds, 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&creds, 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        // Create file in dir1
        let (file_id, _) = fs
            .create(&creds, dir1_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();
        let data = vec![0u8; 1234];
        fs.write(&auth, file_id, 0, &Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        // Create another file in dir2 that will be replaced
        let (target_id, _) = fs
            .create(&creds, dir2_id, b"target.txt", &SetAttributes::default())
            .await
            .unwrap();
        let target_data = vec![0u8; 5678];
        fs.write(&auth, target_id, 0, &Bytes::copy_from_slice(&target_data))
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 6912); // 1234 + 5678
        assert_eq!(inodes_before, 4); // 2 dirs + 2 files

        // Rename file from dir1 to dir2, replacing target
        fs.rename(&auth, dir1_id, b"file.txt", dir2_id, b"target.txt")
            .await
            .unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 1234); // Only source file remains
        assert_eq!(inodes_after, 3); // 2 dirs + 1 file
    }

    #[tokio::test]
    async fn test_stats_rename_special_files() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // Create a FIFO
        let (_fifo_id, _) = fs
            .mknod(
                &creds,
                0,
                b"fifo1",
                crate::fs::types::FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
            .unwrap();

        // Create another FIFO to be replaced
        let (_fifo2_id, _) = fs
            .mknod(
                &creds,
                0,
                b"fifo2",
                crate::fs::types::FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
            .unwrap();

        let (bytes_before, inodes_before) = fs.global_stats.get_totals();
        assert_eq!(bytes_before, 0); // Special files don't have size
        assert_eq!(inodes_before, 2); // Two FIFOs

        // Rename fifo1 over fifo2
        fs.rename(&auth, 0, b"fifo1", 0, b"fifo2").await.unwrap();

        let (bytes_after, inodes_after) = fs.global_stats.get_totals();
        assert_eq!(bytes_after, 0);
        assert_eq!(inodes_after, 1); // Only one FIFO remains
    }
}
