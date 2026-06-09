use super::STATS_SHARDS;
use super::inode::InodeId;
use super::key_codec::KeyCodec;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct StatsShardData {
    pub used_bytes: u64,
    pub used_inodes: u64,
}

pub struct StatsShard {
    pub used_bytes: AtomicU64,
    pub used_inodes: AtomicU64,
}

pub struct FileSystemGlobalStats {
    pub shards: Vec<StatsShard>,
    key_codec: Arc<KeyCodec>,
}

/// Absolute shard values staged by the commit worker for one batch. The
/// encoded value goes into the batch itself; once the batch is in the
/// memtable, `publish` makes the same values the visible in-memory counters.
pub struct StagedShard {
    shard_id: usize,
    pub key: Bytes,
    pub value: Bytes,
    data: StatsShardData,
}

/// Signed difference `new_size - old_size`, clamped to ±i64::MAX. The clamp
/// is symmetric so a clamped grow and the matching shrink cancel exactly.
pub fn size_delta(old_size: u64, new_size: u64) -> i64 {
    if new_size >= old_size {
        i64::try_from(new_size - old_size).unwrap_or(i64::MAX)
    } else {
        i64::try_from(old_size - new_size)
            .map(|d| -d)
            .unwrap_or(-i64::MAX)
    }
}

impl FileSystemGlobalStats {
    pub fn new(key_codec: Arc<KeyCodec>) -> Self {
        let shards = (0..STATS_SHARDS)
            .map(|_| StatsShard {
                used_bytes: AtomicU64::new(0),
                used_inodes: AtomicU64::new(0),
            })
            .collect();
        Self { shards, key_codec }
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

    pub fn shard_of(&self, inode_id: InodeId) -> usize {
        inode_id as usize % STATS_SHARDS
    }

    /// Compute the absolute shard value after applying an aggregated delta.
    /// Only the commit worker may call this and [`publish`]: being the single
    /// writer of the shard counters is what makes the lock-free
    /// read-modify-write sound.
    ///
    /// [`publish`]: Self::publish
    pub fn stage_delta(&self, shard_id: usize, bytes: i64, inodes: i64) -> StagedShard {
        let shard = &self.shards[shard_id];
        let data = StatsShardData {
            used_bytes: shard
                .used_bytes
                .load(Ordering::Relaxed)
                .saturating_add_signed(bytes),
            used_inodes: shard
                .used_inodes
                .load(Ordering::Relaxed)
                .saturating_add_signed(inodes),
        };
        StagedShard {
            shard_id,
            key: self.key_codec.stats_shard_key(shard_id),
            value: Bytes::from(
                bincode::serialize(&data).expect("StatsShardData serialization cannot fail"),
            ),
            data,
        }
    }

    /// Make staged values the visible in-memory counters, after the batch
    /// carrying them was written.
    pub fn publish(&self, staged: &StagedShard) {
        let shard = &self.shards[staged.shard_id];
        shard
            .used_bytes
            .store(staged.data.used_bytes, Ordering::Relaxed);
        shard
            .used_inodes
            .store(staged.data.used_inodes, Ordering::Relaxed);
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

    /// Read every stats shard directly from the db, decode it, and sum.
    /// `new_in_memory` builds v2-segmented volumes, hence `KeyCodec::new(true)`.
    async fn persisted_shard_totals(fs: &ZeroFS) -> (u64, u64) {
        let codec = KeyCodec::new(true);
        let mut bytes = 0u64;
        let mut inodes = 0u64;
        for i in 0..STATS_SHARDS {
            if let Some(raw) = fs.db.get_bytes(&codec.stats_shard_key(i)).await.unwrap() {
                let shard: StatsShardData = bincode::deserialize(&raw).unwrap();
                bytes += shard.used_bytes;
                inodes += shard.used_inodes;
            }
        }
        (bytes, inodes)
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

    #[test]
    fn test_stage_and_publish_delta() {
        let stats = FileSystemGlobalStats::new(Arc::new(KeyCodec::new(true)));
        let shard = stats.shard_of(7);

        // Staging alone must not change the visible counters.
        let staged = stats.stage_delta(shard, 1000, 1);
        assert_eq!(stats.get_totals(), (0, 0));
        stats.publish(&staged);
        assert_eq!(stats.get_totals(), (1000, 1));

        // Negative deltas saturate at zero rather than wrapping.
        let staged = stats.stage_delta(shard, -2000, -5);
        stats.publish(&staged);
        assert_eq!(stats.get_totals(), (0, 0));
    }

    #[test]
    fn test_size_delta_clamps() {
        assert_eq!(size_delta(100, 350), 250);
        assert_eq!(size_delta(350, 100), -250);
        assert_eq!(size_delta(0, u64::MAX), i64::MAX);
        assert_eq!(size_delta(u64::MAX, 0), -i64::MAX);
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

    /// Persisted shard values must equal the result of applying all committed
    /// operations, and the in-memory totals must match them, across a mix of
    /// create / write / truncate / remove / rename-with-replacement ops.
    #[tokio::test]
    async fn test_stats_persisted_shards_match_totals_after_mixed_ops() {
        use crate::fs::types::SetSize;

        let fs = ZeroFS::new_in_memory().await.unwrap();
        let creds = test_creds();
        let auth = test_auth();

        // create a (+1 inode), write 1000 bytes
        let (a_id, _) = fs
            .create(&creds, 0, b"a.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, a_id, 0, &Bytes::from(vec![0u8; 1000]))
            .await
            .unwrap();

        // create b (+1 inode), write 2500 bytes
        let (b_id, _) = fs
            .create(&creds, 0, b"b.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, b_id, 0, &Bytes::from(vec![0u8; 2500]))
            .await
            .unwrap();

        // truncate a to 300 (-700 bytes)
        let setattr = SetAttributes {
            size: SetSize::Set(300),
            ..Default::default()
        };
        fs.setattr(&creds, a_id, &setattr).await.unwrap();

        // create c (+1 inode), write 700 bytes
        let (c_id, _) = fs
            .create(&creds, 0, b"c.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, c_id, 0, &Bytes::from(vec![0u8; 700]))
            .await
            .unwrap();

        // remove b (-1 inode, -2500 bytes)
        fs.remove(&auth, 0, b"b.txt").await.unwrap();

        // create d (+1 inode), write 4242 bytes, then rename c over d
        // (replacement: -1 inode, -4242 bytes)
        let (d_id, _) = fs
            .create(&creds, 0, b"d.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, d_id, 0, &Bytes::from(vec![0u8; 4242]))
            .await
            .unwrap();
        fs.rename(&auth, 0, b"c.txt", 0, b"d.txt").await.unwrap();

        // mkdir (+1 inode), symlink (+1 inode); neither adds bytes
        fs.mkdir(&creds, 0, b"dir", &SetAttributes::default())
            .await
            .unwrap();
        fs.symlink(&creds, 0, b"link", b"/t", &SetAttributes::default())
            .await
            .unwrap();

        // a (300) + c-renamed-to-d (700); inodes: a, c, dir, symlink
        let expected = (1000u64, 4u64);
        assert_eq!(fs.global_stats.get_totals(), expected);
        assert_eq!(persisted_shard_totals(&fs).await, expected);
    }

    /// 64 concurrent tasks create + write deterministic sizes; half remove
    /// their file again. Both the in-memory totals and the persisted shard
    /// sum must land on the exact expected values.
    #[tokio::test]
    async fn test_stats_concurrent_create_write_remove_persisted() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let mut handles = Vec::new();
        for i in 0u64..64 {
            let fs = fs.clone();
            handles.push(tokio::spawn(async move {
                let name = format!("stress{i}.bin");
                let creds = test_creds();
                let auth = test_auth();
                let (id, _) = fs
                    .create(&creds, 0, name.as_bytes(), &SetAttributes::default())
                    .await
                    .unwrap();
                let size = (i as usize + 1) * 100;
                fs.write(&auth, id, 0, &Bytes::from(vec![0u8; size]))
                    .await
                    .unwrap();
                if i % 2 == 0 {
                    fs.remove(&auth, 0, name.as_bytes()).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Survivors are odd i with size (i+1)*100: 100 * (2+4+...+64) = 105600.
        let expected = (105_600u64, 32u64);
        assert_eq!(fs.global_stats.get_totals(), expected);
        assert_eq!(persisted_shard_totals(&fs).await, expected);
    }

    /// Totals loaded by a second ZeroFS over the same backing store must match
    /// what the first instance committed (exercises the startup load_shard
    /// path against worker-persisted shard values).
    #[tokio::test]
    async fn test_stats_totals_survive_reopen() {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use crate::db::SlateDbHandle;
        use slatedb::BlockTransformer;
        use slatedb::DbBuilder;
        use slatedb::object_store::path::Path;

        let test_key = [0u8; 32];
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());

        // Path must match `new_in_memory_read_only` so the reader finds the db.
        let slatedb = Arc::new(
            DbBuilder::new(Path::from("test_slatedb"), object_store.clone())
                .with_block_transformer(block_transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies(true))
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let fs = ZeroFS::new_with_slatedb(
            SlateDbHandle::ReadWrite(slatedb),
            u64::MAX,
            None,
            false,
            true,
        )
        .await
        .unwrap();

        let creds = test_creds();
        let auth = test_auth();
        let (a_id, _) = fs
            .create(&creds, 0, b"keep.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, a_id, 0, &Bytes::from(vec![0u8; 1234]))
            .await
            .unwrap();
        let (b_id, _) = fs
            .create(&creds, 0, b"gone.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, b_id, 0, &Bytes::from(vec![0u8; 4096]))
            .await
            .unwrap();
        fs.remove(&auth, 0, b"gone.txt").await.unwrap();

        let expected = (1234u64, 1u64);
        assert_eq!(fs.global_stats.get_totals(), expected);

        fs.flush_coordinator.flush().await.unwrap();
        drop(fs);

        let fs_reopened = ZeroFS::new_in_memory_read_only(object_store).await.unwrap();
        assert_eq!(fs_reopened.global_stats.get_totals(), expected);
    }
}
