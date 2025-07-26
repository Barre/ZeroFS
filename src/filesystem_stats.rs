use crate::filesystem::STATS_SHARDS;
use crate::inode::InodeId;
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use zerofs_nfsserve::nfs::nfsstat3;

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
    pub async fn prepare_inode_create(&self, inode_id: InodeId) -> StatsUpdate {
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
            shard_key: stats_shard_key(shard_id),
            shard_data,
            _guard: guard,
        }
    }

    /// Prepare a statistics update for inode removal
    pub async fn prepare_inode_remove(
        &self,
        inode_id: InodeId,
        file_size: Option<u64>,
    ) -> StatsUpdate {
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
            shard_key: stats_shard_key(shard_id),
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
    ) -> Option<StatsUpdate> {
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
            shard_key: stats_shard_key(shard_id),
            shard_data,
            _guard: guard,
        })
    }

    /// Add the statistics update to a write batch
    pub fn add_to_batch(
        &self,
        update: &StatsUpdate,
        batch: &mut crate::encryption::EncryptedWriteBatch,
    ) -> Result<(), nfsstat3> {
        let shard_bytes =
            bincode::serialize(&update.shard_data).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch
            .put_bytes(&update.shard_key, &shard_bytes)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;
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

fn stats_shard_key(shard_id: usize) -> Bytes {
    Bytes::from(format!("stats:shard:{shard_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::SlateDbFs;
    use crate::test_helpers::test_helpers_mod::{filename, test_auth};
    use zerofs_nfsserve::nfs::sattr3;
    use zerofs_nfsserve::vfs::NFSFileSystem;

    #[tokio::test]
    async fn test_stats_initialization() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();
        let (bytes, inodes) = fs.global_stats.get_totals();

        // Should start empty - root directory is created during filesystem setup
        // but stats are initialized empty and loaded from persistent storage
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0);
    }

    #[tokio::test]
    async fn test_stats_file_creation() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file
        let (_file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // New file has 0 bytes
        assert_eq!(inodes, 1); // Just the file
    }

    #[tokio::test]
    async fn test_stats_file_write() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file
        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        // Write 1000 bytes
        let data = vec![0u8; 1000];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000);
        assert_eq!(inodes, 1);

        // Write more data (extending the file)
        let data = vec![1u8; 500];
        fs.write(&test_auth(), file_id, 1000, &data).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1500);
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_file_overwrite() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file and write initial data
        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = vec![0u8; 1000];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        // Overwrite part of the file (no size change)
        let data = vec![1u8; 500];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Size unchanged
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_sparse_file() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file
        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"sparse.txt"), sattr3::default())
            .await
            .unwrap();

        // Write 1 byte at offset 1GB (creating a sparse file)
        let data = vec![42u8; 1];
        let offset = 1_000_000_000;
        fs.write(&test_auth(), file_id, offset, &data)
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1_000_000_001); // Logical size
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_file_removal() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create and write to a file
        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = vec![0u8; 5000];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 5000);
        assert_eq!(inodes, 1);

        // Remove the file
        fs.remove(&test_auth(), 0, &filename(b"test.txt"))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0); // No tracked inodes
    }

    #[tokio::test]
    async fn test_stats_directory_operations() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create directories
        let (dir1_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"dir1"), &sattr3::default())
            .await
            .unwrap();

        let (_dir2_id, _) = fs
            .mkdir(
                &test_auth(),
                dir1_id,
                &filename(b"dir2"),
                &sattr3::default(),
            )
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // Directories don't consume bytes
        assert_eq!(inodes, 2); // dir1 + dir2
    }

    #[tokio::test]
    async fn test_stats_symlink() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a symlink
        let (_link_id, _) = fs
            .symlink(
                &test_auth(),
                0,
                &filename(b"link"),
                &filename(b"/target/path"),
                &sattr3::default(),
            )
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0); // Symlinks don't count as bytes
        assert_eq!(inodes, 1); // Just the symlink
    }

    #[tokio::test]
    async fn test_stats_hard_links() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file with content
        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"original.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        let data = vec![0u8; 1000];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        // Create a hard link
        fs.link(&test_auth(), file_id, 0, &filename(b"hardlink.txt"))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Same data, not duplicated
        assert_eq!(inodes, 1); // Still just 2 inodes (root + file)

        // Remove original - stats should remain
        fs.remove(&test_auth(), 0, &filename(b"original.txt"))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 1000); // Data still exists via hard link
        assert_eq!(inodes, 1);

        // Remove hard link - now stats should update
        fs.remove(&test_auth(), 0, &filename(b"hardlink.txt"))
            .await
            .unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 0);
        assert_eq!(inodes, 0); // No tracked inodes
    }

    #[tokio::test]
    async fn test_stats_file_truncate() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a file with content
        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = vec![0u8; 10000];
        fs.write(&test_auth(), file_id, 0, &data).await.unwrap();

        // Truncate to smaller size
        use zerofs_nfsserve::nfs::{
            set_atime, set_gid3, set_mode3, set_mtime, set_size3, set_uid3,
        };

        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(5000),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.setattr(&test_auth(), file_id, setattr).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 5000);
        assert_eq!(inodes, 1);

        // Extend to larger size
        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(15000),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.setattr(&test_auth(), file_id, setattr).await.unwrap();

        let (bytes, inodes) = fs.global_stats.get_totals();
        assert_eq!(bytes, 15000);
        assert_eq!(inodes, 1);
    }

    #[tokio::test]
    async fn test_stats_concurrent_operations() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create multiple files concurrently
        let mut handles = vec![];

        for i in 0..10 {
            let fs_clone = fs.clone();
            let handle = tokio::spawn(async move {
                let fname = format!("file{}.txt", i);
                let (file_id, _) = fs_clone
                    .create(
                        &test_auth(),
                        0,
                        &filename(fname.as_bytes()),
                        sattr3::default(),
                    )
                    .await
                    .unwrap();

                // Write different amounts of data
                let data = vec![0u8; (i + 1) * 1000];
                fs_clone
                    .write(&test_auth(), file_id, 0, &data)
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
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create some files
        for i in 0..5 {
            let fname = format!("file{}.txt", i);
            let (file_id, _) = fs
                .create(
                    &test_auth(),
                    0,
                    &filename(fname.as_bytes()),
                    sattr3::default(),
                )
                .await
                .unwrap();

            let data = vec![0u8; 1_000_000]; // 1MB each
            fs.write(&test_auth(), file_id, 0, &data).await.unwrap();
        }

        // Check fsstat
        let fsstat = fs.fsstat(&test_auth(), 0).await.unwrap();

        const TOTAL_BYTES: u64 = 8 << 60; // 8 EiB
        const TOTAL_INODES: u64 = 1 << 48;

        assert_eq!(fsstat.tbytes, TOTAL_BYTES);
        assert_eq!(fsstat.fbytes, TOTAL_BYTES - 5_000_000);
        assert_eq!(fsstat.abytes, TOTAL_BYTES - 5_000_000);
        assert_eq!(fsstat.tfiles, TOTAL_INODES);
        assert_eq!(fsstat.ffiles, TOTAL_INODES - 5); // 5 files
        assert_eq!(fsstat.afiles, TOTAL_INODES - 5);
    }
}
