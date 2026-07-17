//! File data plane: write, read, trim.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::stats;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, FallocateMode, FileAttributes, InodeWithId};
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
        self.write_idempotent(auth, id, offset, data, [0u8; 16])
            .await
    }

    /// Idempotent write retaining the original post-write attributes.
    pub async fn write_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_write)? {
            return Ok(result);
        }
        let start_time = std::time::Instant::now();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let creds = Credentials::from_auth_context(auth);

        let _guard = self.lock_manager.acquire(id).await;
        // Direct filesystem callers do not pass through the 9P single-flight,
        // so re-check after waiting for the inode lock.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_write)? {
            return Ok(result);
        }
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

                let post_attrs: FileAttributes = InodeWithId { inode: &inode, id }.into();
                txn.set_dedup_result(
                    op_id,
                    crate::dedup::DedupResult::Write {
                        attrs: post_attrs.clone(),
                    },
                );
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

                Ok(post_attrs)
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
        if length == 0 {
            return Ok(());
        }

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

        let file_size = match &inode {
            Inode::File(file) => file.size,
            _ => unreachable!(),
        };
        offset.checked_add(length).ok_or(FsError::InvalidArgument)?;

        let mut txn = self.db.new_transaction()?;
        self.extent_store
            .zero_range(&mut txn, id, offset, length, file_size)
            .await?;
        self.write_coordinator.commit(txn).await.inspect_err(|e| {
            error!("Failed to commit trim batch: {}", e);
        })?;

        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);
        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Trim { offset, length },
        );

        Ok(())
    }

    /// Atomically allocate, punch, or zero a file range.
    ///
    /// ZeroFS represents zero-filled ranges as sparse holes. Its allocation
    /// guarantee comes from charging logical growth against quota, so a later
    /// write inside the file does not consume additional quota.
    pub async fn fallocate(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
    ) -> Result<FileAttributes, FsError> {
        self.fallocate_idempotent(auth, id, offset, length, mode, [0u8; 16])
            .await
    }

    /// Idempotent fallocate. Delayed punch-hole and zero-range retries do not
    /// apply after newer writes.
    pub async fn fallocate_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_fallocate)? {
            return Ok(result);
        }
        debug!(
            "Processing fallocate on inode {} at offset {} length {} mode {:?}",
            id, offset, length, mode
        );

        if length == 0 {
            return Err(FsError::InvalidArgument);
        }
        let end = offset.checked_add(length).ok_or(FsError::InvalidArgument)?;

        let _guard = self.lock_manager.acquire(id).await;
        // Direct filesystem callers do not pass through the 9P single-flight.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_fallocate)? {
            return Ok(result);
        }
        let mut inode = self.inode_store.get(id).await?;

        let creds = Credentials::from_auth_context(auth);

        match &inode {
            Inode::File(file) if creds.uid != file.uid => {
                check_access(&inode, &creds, AccessMode::Write)?;
            }
            Inode::File(_) => {}
            _ => return Err(FsError::IsDirectory),
        }

        let (old_size, parent_name_for_update) = match &inode {
            Inode::File(file) => (file.size, file.parent.zip(file.name.clone())),
            _ => unreachable!(),
        };

        let new_size = match mode {
            FallocateMode::Allocate | FallocateMode::ZeroRange { keep_size: false } => {
                old_size.max(end)
            }
            FallocateMode::PunchHole | FallocateMode::ZeroRange { keep_size: true } => old_size,
        };
        let zeroes_range = !matches!(mode, FallocateMode::Allocate);

        if new_size > old_size {
            let increase = new_size - old_size;
            let (used_bytes, _) = self.global_stats.get_totals();
            if used_bytes.saturating_add(increase) > self.max_bytes {
                return Err(FsError::NoSpace);
            }
        }

        let mut txn = self.db.new_transaction()?;

        if zeroes_range && offset < old_size {
            let zero_length = end.min(old_size) - offset;
            self.extent_store
                .zero_range(&mut txn, id, offset, zero_length, old_size)
                .await?;
        }

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_EXTENTS);

        if let Inode::File(file) = &mut inode {
            file.size = new_size;
            let (now_sec, now_nsec) = get_current_time();
            file.mtime = now_sec;
            file.mtime_nsec = now_nsec;
            file.ctime = now_sec;
            file.ctime_nsec = now_nsec;

            // Match the write path: a non-owner changing file state must not
            // leave a privileged executable carrying stale SUID/SGID bits.
            if creds.uid != file.uid && creds.uid != 0 {
                file.mode &= !0o6000;
            }
        }

        self.inode_store.save(&mut txn, id, &inode)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_INODE);

        if let Some((parent_id, name)) = parent_name_for_update {
            self.directory_store
                .update_inode_in_entry(&mut txn, parent_id, &name, id, &inode)
                .await?;
        }
        let post_attrs: FileAttributes = InodeWithId { inode: &inode, id }.into();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Fallocate {
                attrs: post_attrs.clone(),
            },
        );
        txn.add_stats_delta(id, stats::size_delta(old_size, new_size), 0);

        self.write_coordinator.commit(txn).await.inspect_err(|e| {
            error!("Failed to commit fallocate batch: {}", e);
        })?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::FALLOCATE_AFTER_COMMIT);

        debug!("Fallocate completed successfully for inode {}", id);

        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer.emit(
            &self.inode_store,
            id,
            FileOperation::Fallocate {
                offset,
                length,
                mode: mode.linux_mode(),
            },
        );

        Ok(post_attrs)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "failpoints")]
    use crate::failpoints as fp;
    use crate::fs::inode::Inode;
    use crate::fs::test_util::test_creds;
    use crate::fs::tracing::FileOperation;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;
    #[cfg(feature = "failpoints")]
    use std::sync::Arc;

    use crate::fs::types::{
        AuthContext, FallocateMode, FileAttributes, InodeWithId, SetAttributes, SetSize, SetTime,
        Timestamp,
    };
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
    async fn write_retry_replays_result_without_overwriting_a_later_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"retry.txt", &SetAttributes::default())
            .await
            .unwrap();
        let op_id = [0x41; 16];
        let auth: AuthContext = (&test_auth()).into();

        let original = fs
            .write_idempotent(&auth, file_id, 0, &Bytes::from_static(b"first"), op_id)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later"))
            .await
            .unwrap();

        let replayed = fs
            .write_idempotent(&auth, file_id, 0, &Bytes::from_static(b"first"), op_id)
            .await
            .unwrap();
        assert_eq!(replayed.size, original.size);
        assert_eq!(replayed.mtime, original.mtime);
        let (data, _) = fs.read_file(&auth, file_id, 0, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"later");
    }

    #[tokio::test]
    async fn fallocate_retry_does_not_zero_a_later_write() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(
                &test_creds(),
                0,
                b"range-retry.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"first"))
            .await
            .unwrap();
        let op_id = [0x43; 16];
        let mode = FallocateMode::ZeroRange { keep_size: true };

        let original = fs
            .fallocate_idempotent(&auth, file_id, 0, 5, mode, op_id)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"later"))
            .await
            .unwrap();

        let replayed = fs
            .fallocate_idempotent(&auth, file_id, 0, 5, mode, op_id)
            .await
            .unwrap();
        assert_eq!(replayed.mtime, original.mtime);
        let (data, _) = fs.read_file(&auth, file_id, 0, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"later");
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
    async fn fallocate_allocate_grows_without_overwriting() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"allocate.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"data"))
            .await
            .unwrap();

        let attrs = fs
            .fallocate(&auth, file_id, 8, 8, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_eq!(attrs.size, 16);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert_eq!(&data[..4], b"data");
        assert!(data[4..].iter().all(|&b| b == 0));

        let attrs = fs
            .fallocate(&auth, file_id, 1, 2, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_eq!(attrs.size, 16, "allocation inside EOF must not shrink");
    }

    #[tokio::test]
    async fn fallocate_reserves_logical_quota_for_later_writes() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.max_bytes = 8;
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"quota.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();

        fs.fallocate(&auth, file_id, 0, 8, FallocateMode::Allocate)
            .await
            .unwrap();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"12345678"))
            .await
            .expect("an overwrite inside the reservation consumes no new quota");
        let result = fs
            .fallocate(&auth, file_id, 8, 1, FallocateMode::Allocate)
            .await;
        assert!(matches!(result, Err(FsError::NoSpace)));
    }

    #[tokio::test]
    async fn fallocate_punch_and_zero_range_semantics() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"ranges.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"abcdefghijkl"))
            .await
            .unwrap();

        let attrs = fs
            .fallocate(&auth, file_id, 2, 3, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_eq!(attrs.size, 12);
        let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
        assert_eq!(&data[..2], b"ab");
        assert_eq!(&data[2..5], &[0; 3]);
        assert_eq!(&data[5..], b"fghijkl");

        let attrs = fs
            .fallocate(
                &auth,
                file_id,
                8,
                8,
                FallocateMode::ZeroRange { keep_size: true },
            )
            .await
            .unwrap();
        assert_eq!(attrs.size, 12, "KEEP_SIZE must not cross EOF");

        let attrs = fs
            .fallocate(
                &auth,
                file_id,
                8,
                8,
                FallocateMode::ZeroRange { keep_size: false },
            )
            .await
            .unwrap();
        assert_eq!(attrs.size, 16);
        let (data, _) = fs.read_file(&auth, file_id, 0, 16).await.unwrap();
        assert!(data[8..].iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn fallocate_updates_metadata_when_data_and_size_are_unchanged() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"metadata.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"data"))
            .await
            .unwrap();

        let old_mtime = Timestamp {
            seconds: 1,
            nanoseconds: 0,
        };
        let reset_mtime = SetAttributes {
            mtime: SetTime::SetToClientTime(old_mtime),
            ..SetAttributes::default()
        };

        fs.setattr(&test_creds(), file_id, &reset_mtime)
            .await
            .unwrap();
        let attrs = fs
            .fallocate(&auth, file_id, 0, 1, FallocateMode::Allocate)
            .await
            .unwrap();
        assert_ne!(
            attrs.mtime, old_mtime,
            "allocation inside EOF updates mtime"
        );

        fs.setattr(&test_creds(), file_id, &reset_mtime)
            .await
            .unwrap();
        let attrs = fs
            .fallocate(&auth, file_id, 100, 1, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_ne!(
            attrs.mtime, old_mtime,
            "hole punching beyond EOF still updates mtime"
        );
    }

    #[tokio::test]
    async fn fallocate_by_non_owner_clears_suid_and_sgid() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let attrs = SetAttributes {
            mode: crate::fs::types::SetMode::Set(0o6777),
            ..SetAttributes::default()
        };
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"privileged.bin", &attrs)
            .await
            .unwrap();
        let owner_auth = (&test_auth()).into();
        fs.write(
            &owner_auth,
            file_id,
            0,
            &Bytes::from_static(b"privileged data"),
        )
        .await
        .unwrap();

        let non_owner = AuthContext {
            uid: 2000,
            gid: 2000,
            gids: Vec::new(),
        };
        let attrs = fs
            .fallocate(&non_owner, file_id, 0, 1, FallocateMode::PunchHole)
            .await
            .unwrap();
        assert_eq!(attrs.mode & 0o6000, 0);
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn fallocate_failpoints_cover_transaction_stages() {
        let _scenario = fail::FailScenario::setup();
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let (file_id, _) = fs
            .create(
                &test_creds(),
                0,
                b"fallocate-failpoints.bin",
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        let original = Bytes::from_static(b"abcdefghijkl");
        fs.write(&auth, file_id, 0, &original).await.unwrap();

        for (point, offset) in [
            (fp::FALLOCATE_AFTER_EXTENTS, 0),
            (fp::FALLOCATE_AFTER_INODE, 4),
        ] {
            fail::cfg(point, "panic").unwrap();
            let fs_clone = Arc::clone(&fs);
            let auth_clone = auth.clone();
            let result = tokio::spawn(async move {
                fs_clone
                    .fallocate(&auth_clone, file_id, offset, 2, FallocateMode::PunchHole)
                    .await
            })
            .await;
            fail::cfg(point, "off").unwrap();

            assert!(result.unwrap_err().is_panic(), "{point} must be reached");
            let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
            assert_eq!(
                data, original,
                "pre-commit crash must discard the transaction"
            );
        }

        fail::cfg(fp::FALLOCATE_AFTER_COMMIT, "panic").unwrap();
        let fs_clone = Arc::clone(&fs);
        let auth_clone = auth.clone();
        let result = tokio::spawn(async move {
            fs_clone
                .fallocate(&auth_clone, file_id, 8, 2, FallocateMode::PunchHole)
                .await
        })
        .await;
        fail::cfg(fp::FALLOCATE_AFTER_COMMIT, "off").unwrap();

        assert!(
            result.unwrap_err().is_panic(),
            "post-commit failpoint must be reached"
        );
        let (data, _) = fs.read_file(&auth, file_id, 0, 12).await.unwrap();
        assert_eq!(&data[..8], b"abcdefgh");
        assert_eq!(&data[8..10], &[0, 0]);
        assert_eq!(&data[10..], b"kl");
    }

    #[tokio::test]
    async fn trim_remains_extent_only_while_fallocate_has_its_own_trace() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"trim.bin", &SetAttributes::default())
            .await
            .unwrap();
        let auth = (&test_auth()).into();
        fs.write(&auth, file_id, 0, &Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();

        let old_mtime = Timestamp {
            seconds: 1,
            nanoseconds: 0,
        };
        fs.setattr(
            &test_creds(),
            file_id,
            &SetAttributes {
                mtime: SetTime::SetToClientTime(old_mtime),
                ..SetAttributes::default()
            },
        )
        .await
        .unwrap();

        fs.trim(&auth, file_id, 1, 2).await.unwrap();
        let inode = fs.inode_store.get(file_id).await.unwrap();
        let attrs: FileAttributes = InodeWithId {
            inode: &inode,
            id: file_id,
        }
        .into();
        assert_eq!(
            attrs.mtime, old_mtime,
            "NBD trim must not rewrite inode metadata"
        );
        let (data, _) = fs.read_file(&auth, file_id, 0, 8).await.unwrap();
        assert_eq!(&data[..], b"a\0\0defgh");

        let mut events = fs.tracer.subscribe();
        fs.fallocate(&auth, file_id, 0, 1, FallocateMode::Allocate)
            .await
            .unwrap();
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event.operation,
            FileOperation::Fallocate {
                offset: 0,
                length: 1,
                mode: 0
            }
        ));
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
