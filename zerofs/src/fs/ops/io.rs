//! File data plane: write, read, trim.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::stats;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, FileAttributes, InodeWithId};
use crate::fs::{ZeroFS, get_current_time};
use ::tracing::{debug, error};
use bytes::Bytes;
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Write `data` at `offset`, growing the file as needed; growth is
    /// quota-checked against `max_bytes`. Returns the post-write attributes.
    pub async fn write(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let creds = Credentials::from_auth_context(auth);

        let _guard = self.lock_manager.acquire(id).await;
        let mut inode = self.inode_store.get(id).await?;

        // NFS RFC 1813 section 4.4: Allow owners to write to their files regardless of permission bits
        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            _ => {}
        }

        match &mut inode {
            Inode::File(file) => {
                let old_size = file.size;
                let end_offset = offset
                    .checked_add(data.len() as u64)
                    .ok_or(FsError::InvalidArgument)?;
                let new_size = std::cmp::max(file.size, end_offset);

                if new_size > old_size {
                    let size_increase = new_size - old_size;
                    let (used_bytes, _) = self.global_stats.get_totals();

                    if used_bytes.saturating_add(size_increase) > self.max_bytes {
                        debug!(
                            "Write would exceed quota: used={}, increase={}, max={}",
                            used_bytes, size_increase, self.max_bytes
                        );
                        return Err(FsError::NoSpace);
                    }
                }

                let mut txn = self.db.new_transaction()?;

                let tail_update = self
                    .extent_store
                    .write(&mut txn, id, offset, data, old_size)
                    .await?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::WRITE_AFTER_EXTENT);

                file.size = new_size;
                let (now_sec, now_nsec) = get_current_time();
                file.mtime = now_sec;
                file.mtime_nsec = now_nsec;
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;

                // POSIX: Clear SUID/SGID bits on write by non-owner
                if creds.uid != file.uid && creds.uid != 0 {
                    file.mode &= !0o6000;
                }

                let parent_name_for_update = file.parent.zip(file.name.clone());

                self.inode_store.save(&mut txn, id, &inode)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::WRITE_AFTER_INODE);

                if let Some((parent_id, name)) = parent_name_for_update {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                        .await?;
                }

                txn.add_stats_delta(id, stats::size_delta(old_size, new_size), 0);

                let db_write_start = std::time::Instant::now();
                self.write_coordinator.commit(txn).await?;
                debug!("DB write took: {:?}", db_write_start.elapsed());

                // Only after the commit is durable: a cache ahead of the store
                // would splice later writes onto bytes that never landed.
                self.extent_store.apply_tail_update(id, tail_update);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::WRITE_AFTER_COMMIT);

                let elapsed = start_time.elapsed();
                debug!(
                    "Write processed successfully for inode {}, new size: {}, took: {:?}",
                    id, new_size, elapsed
                );

                self.stats
                    .bytes_written
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    id,
                    FileOperation::Write {
                        offset,
                        length: data.len() as u64,
                    },
                );

                Ok(InodeWithId { inode: &inode, id }.into())
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    /// Read up to `count` bytes at `offset`. The bool is EOF: true when the
    /// read reached the end of the file; at or past EOF it is `(empty, true)`.
    pub async fn read_file(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        debug!("read_file: id={}, offset={}, count={}", id, offset, count);

        let inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        check_access(&inode, &creds, AccessMode::Read)?;

        match &inode {
            Inode::File(file) => {
                if offset >= file.size {
                    self.tracer.emit(
                        &self.inode_store,
                        id,
                        FileOperation::Read { offset, length: 0 },
                    );
                    return Ok((Bytes::new(), true));
                }

                let read_len = std::cmp::min(count as u64, file.size - offset);
                let result_bytes = self.extent_store.read(id, offset, read_len).await?;
                let eof = offset + read_len >= file.size;

                self.stats
                    .bytes_read
                    .fetch_add(result_bytes.len() as u64, Ordering::Relaxed);
                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    id,
                    FileOperation::Read {
                        offset,
                        length: read_len,
                    },
                );

                Ok((result_bytes, eof))
            }
            _ => Err(FsError::IsDirectory),
        }
    }

    /// Punch a hole: zero `[offset, offset + length)` in place without
    /// changing the file size.
    pub async fn trim(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Result<(), FsError> {
        debug!(
            "Processing trim on inode {} at offset {} length {}",
            id, offset, length
        );

        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(FsError::IsDirectory),
        }

        let file = match &inode {
            Inode::File(f) => f,
            _ => return Err(FsError::IsDirectory),
        };

        // A client-supplied offset+length can overflow u64; reject before the
        // wrapped range reaches the extent layer.
        offset.checked_add(length).ok_or(FsError::InvalidArgument)?;

        let mut txn = self.db.new_transaction()?;

        self.extent_store
            .zero_range(&mut txn, id, offset, length, file.size)
            .await?;

        self.write_coordinator.commit(txn).await.inspect_err(|e| {
            error!("Failed to commit trim batch: {}", e);
        })?;

        debug!("Trim completed successfully for inode {}", id);

        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Trim { offset, length },
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::inode::Inode;
    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::types::{SetAttributes, SetSize};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_process_write_and_read() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(data),
            )
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), data);
        assert!(eof);
    }

    #[tokio::test]
    async fn write_offset_length_overflow_is_rejected() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"f.txt", &SetAttributes::default())
            .await
            .unwrap();

        for off in [u64::MAX - 4, u64::MAX - 99, u64::MAX] {
            let r = fs
                .write(
                    &(&test_auth()).into(),
                    file_id,
                    off,
                    &Bytes::from(vec![1u8; 100]),
                )
                .await;
            assert!(
                matches!(r, Err(FsError::InvalidArgument)),
                "write at offset {off} must be EINVAL, got {r:?}"
            );
        }
        // The rejected writes left the file untouched.
        let size = match fs.inode_store.get(file_id).await.unwrap() {
            Inode::File(f) => f.size,
            _ => panic!("expected a file"),
        };
        assert_eq!(size, 0, "a rejected overflow write must not grow the file");

        // trim's offset+length overflow is likewise rejected.
        let r = fs
            .trim(&(&test_auth()).into(), file_id, u64::MAX - 4, 100)
            .await;
        assert!(
            matches!(r, Err(FsError::InvalidArgument)),
            "trim overflow must be EINVAL, got {r:?}"
        );
    }

    #[tokio::test]
    async fn test_process_write_partial_extents() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data1),
        )
        .await
        .unwrap();

        let data2 = vec![b'B'; 50];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            50,
            &Bytes::copy_from_slice(&data2),
        )
        .await
        .unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_extents() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"bigfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let extent_size = EXTENT_SIZE;
        let data = vec![b'X'; extent_size * 2 + 1024];

        let fattr = fs
            .write(
                &(&test_auth()).into(),
                file_id,
                0,
                &Bytes::copy_from_slice(&data),
            )
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data.as_ref(), &data[..]);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_read_beyond_truncated_extent() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![b'A'; 300 * 1024];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&data),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(100 * 1024),
            ..Default::default()
        };
        fs.setattr(&test_creds(), file_id, &setattr).await.unwrap();

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 200 * 1024, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 0);
    }

    #[tokio::test]
    async fn test_tail_cache_sequential_append_matches() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"seq.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Small sequential appends that cross extent boundaries, so the tail extent
        // fills and rolls over repeatedly: every append into a partially-filled
        // extent takes the cached-tail splice path instead of re-reading.
        let step = 5000usize;
        let total = EXTENT_SIZE * 3 + 1234;
        let mut expected = Vec::with_capacity(total);
        let mut offset = 0u64;
        while expected.len() < total {
            let n = step.min(total - expected.len());
            let extent: Vec<u8> = (0..n)
                .map(|i| ((offset as usize + i) % 251) as u8)
                .collect();
            fs.write(
                &(&test_auth()).into(),
                file_id,
                offset,
                &Bytes::copy_from_slice(&extent),
            )
            .await
            .unwrap();
            expected.extend_from_slice(&extent);
            offset += n as u64;
        }

        let (read_data, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, total as u32)
            .await
            .unwrap();
        assert_eq!(&read_data[..], &expected[..]);
    }

    #[tokio::test]
    async fn test_tail_cache_invalidated_by_truncate() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"trunc.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Build a partial tail extent; this populates the tail cache.
        let a = vec![b'A'; 1000];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&a),
        )
        .await
        .unwrap();

        // Shrink into that extent. truncate must drop the cache, else the next
        // append splices onto the stale pre-truncate bytes.
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                size: SetSize::Set(800),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Append past the hole the truncate left. Bytes 800..900 must read back as
        // zeros, not the 'A's a non-invalidated cache would carry forward.
        let b = vec![b'B'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            900,
            &Bytes::copy_from_slice(&b),
        )
        .await
        .unwrap();

        let (gap, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 800, 100)
            .await
            .unwrap();
        assert_eq!(
            gap,
            vec![0u8; 100],
            "truncate must invalidate the tail cache"
        );

        let (tail, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 900, 100)
            .await
            .unwrap();
        assert_eq!(tail, b);
    }

    #[tokio::test]
    async fn test_tail_cache_sparse_write_creates_hole_without_corruption() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"sparse.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Partial tail in extent 0 -> cache holds extent 0.
        let a = vec![b'A'; 1000];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::copy_from_slice(&a),
        )
        .await
        .unwrap();

        // Write far past EOF, leaving a hole. The target extent is beyond_eof, so it
        // must build on zeros, never on the cached extent-0 bytes.
        let far = 6 * EXTENT_SIZE as u64 + 500;
        let b = vec![b'B'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            far,
            &Bytes::copy_from_slice(&b),
        )
        .await
        .unwrap();

        // The bytes before the far write within its own extent are part of the hole:
        // must be zeros, not the cached 'A's.
        let (lead, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 6 * EXTENT_SIZE as u64, 500)
            .await
            .unwrap();
        assert_eq!(
            lead,
            vec![0u8; 500],
            "hole extent must not inherit cached bytes"
        );

        // The hole between the two writes reads as zeros.
        let (hole, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 3 * EXTENT_SIZE as u64, 256)
            .await
            .unwrap();
        assert_eq!(hole, vec![0u8; 256]);

        // The original tail and the far write are both intact.
        let (head, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(head, a);
        let (tail, _) = fs
            .read_file(&(&test_auth()).into(), file_id, far, 100)
            .await
            .unwrap();
        assert_eq!(tail, b);

        // Back-fill into the old extent 0; it must read extent 0 from the store (the
        // cache moved to the far extent), not splice onto a stale entry.
        let c = vec![b'C'; 100];
        fs.write(
            &(&test_auth()).into(),
            file_id,
            500,
            &Bytes::copy_from_slice(&c),
        )
        .await
        .unwrap();
        let (head2, _) = fs
            .read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        let mut want = vec![b'A'; 1000];
        want[500..600].fill(b'C');
        assert_eq!(head2, want);
    }
}
