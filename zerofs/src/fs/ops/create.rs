//! Child-creating ops: create, mkdir, mknod, and their idempotent variants.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::dedup::DedupResult;
use crate::fs::errors::FsError;
use crate::fs::inode::{DirectoryInode, FileInode, Inode, InodeId, SpecialInode};
use crate::fs::permissions::{AccessMode, Credentials, check_access, validate_mode};
#[cfg(test)]
use crate::fs::store::directory::COOKIE_FIRST_ENTRY;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{
    AuthContext, FileAttributes, FileType, InodeWithId, SetAttributes, SetGid, SetMode, SetTime,
    SetUid,
};
use crate::fs::{ZeroFS, get_current_time, validate_filename};
use ::tracing::{debug, error};
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Create an empty regular file in `dirid`: EEXIST if `name` is taken
    /// (opening an existing file is the caller's path). Unset attrs default to
    /// mode 0666 and the creating credentials.
    pub async fn create(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        self.create_idempotent(creds, dirid, name, attr, [0u8; 16])
            .await
    }

    /// `create` tagged with an idempotency op-id. A retry carrying an op-id this
    /// node already applied returns the existing file instead of EEXIST (the
    /// handler then opens it on the fid). The op-id is recorded atomically with
    /// the commit and shipped to the standby, so dedup also holds across a
    /// failover. All-zero opts out (the pattern is identical for the other
    /// `*_idempotent` ops below).
    pub async fn create_idempotent(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
        op_id: crate::dedup::OpId,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_create)? {
            return Ok(result);
        }
        validate_filename(name)?;

        debug!(
            "create: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        let _guard = self.lock_manager.acquire(dirid).await;
        // Direct filesystem callers do not pass through the 9P single-flight.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_create)? {
            return Ok(result);
        }
        // A cookie allocation is one point read followed by a transaction write.
        // Overlap the read with the existing parent and child-name reads, but
        // defer inspecting its result and staging the increment until their
        // original positions below. That preserves error precedence and keeps
        // the counter update atomic with the rest of the create transaction.
        let cookie_read = self.directory_store.read_cookie(dirid);
        let parent_and_name = async {
            tokio::try_join!(
                self.inode_store.get(dirid),
                self.directory_store.exists(dirid, name)
            )
        };
        let (parent_and_name, cookie) = tokio::join!(parent_and_name, cookie_read);
        let (mut dir_inode, exists) = parent_and_name?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if dir.nlink == 0 {
                    return Err(FsError::StaleHandle);
                }
                if exists {
                    return Err(FsError::Exists);
                }

                let file_id = self.inode_store.allocate();
                debug!(
                    "Allocated inode {} for file {}",
                    file_id,
                    String::from_utf8_lossy(name)
                );

                let (now_sec, now_nsec) = get_current_time();

                let final_mode = match &attr.mode {
                    SetMode::Set(m) => validate_mode(*m),
                    SetMode::NoChange => 0o666,
                };

                let file_inode = Inode::File(FileInode {
                    size: 0,
                    mtime: now_sec,
                    mtime_nsec: now_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: now_sec,
                    atime_nsec: now_nsec,
                    mode: final_mode,
                    uid: match &attr.uid {
                        SetUid::Set(u) => *u,
                        SetUid::NoChange => creds.uid,
                    },
                    gid: match &attr.gid {
                        SetGid::Set(g) => *g,
                        SetGid::NoChange => creds.gid,
                    },
                    parent: Some(dirid),
                    name: Some(name.to_vec()),
                    nlink: 1,
                });

                let mut txn = self.db.new_transaction()?;
                let cookie = cookie?;
                self.directory_store
                    .stage_cookie_increment(dirid, cookie, &mut txn);

                let file_attrs: FileAttributes = InodeWithId {
                    inode: &file_inode,
                    id: file_id,
                }
                .into();
                txn.set_dedup_result(
                    op_id,
                    crate::dedup::DedupResult::Create {
                        inode_id: file_id,
                        attrs: file_attrs.clone(),
                    },
                );
                self.inode_store.save(&mut txn, file_id, &file_inode)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CREATE_AFTER_INODE);

                self.directory_store
                    .add(&mut txn, dirid, name, file_id, cookie, Some(&file_inode));

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CREATE_AFTER_DIR_ENTRY);

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let parent_update_info = dir.name.clone().map(|n| (dir.parent, n));

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                if let Some((parent_id, dir_name)) = parent_update_info {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &dir_name, dirid, &dir_inode)
                        .await
                        .ok();
                }

                txn.add_stats_delta(file_id, 0, 1);

                self.write_coordinator.commit(txn).await.inspect_err(|e| {
                    error!("Failed to write batch: {:?}", e);
                })?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::CREATE_AFTER_COMMIT);

                self.stats.files_created.fetch_add(1, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    file_id,
                    FileOperation::Create { mode: final_mode },
                );

                Ok((file_id, file_attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    /// `create` with default attributes, returning only the id; EEXIST doubles
    /// as the exclusivity guarantee.
    pub async fn create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        filename: &[u8],
    ) -> Result<InodeId, FsError> {
        let (id, _) = self
            .create(
                &Credentials::from_auth_context(auth),
                dirid,
                filename,
                &SetAttributes::default(),
            )
            .await?;
        Ok(id)
    }

    /// Create a directory in `dirid`. Unset mode defaults to 0777; a setgid
    /// parent propagates the bit and its gid (BSD group semantics).
    pub async fn mkdir(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        self.mkdir_idempotent(creds, dirid, name, attr, [0u8; 16])
            .await
    }

    /// `mkdir` tagged with an idempotency op-id: an applied retry returns the
    /// existing directory instead of EEXIST. See [`Self::create_idempotent`].
    pub async fn mkdir_idempotent(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        attr: &SetAttributes,
        op_id: crate::dedup::OpId,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_mkdir)? {
            return Ok(result);
        }
        validate_filename(name)?;

        debug!(
            "mkdir: dirid={}, dirname={}",
            dirid,
            String::from_utf8_lossy(name)
        );

        let _guard = self.lock_manager.acquire(dirid).await;
        // Direct filesystem callers do not pass through the 9P single-flight.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_mkdir)? {
            return Ok(result);
        }
        let (mut dir_inode, exists) = tokio::try_join!(
            self.inode_store.get(dirid),
            self.directory_store.exists(dirid, name)
        )?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if dir.nlink == 0 {
                    return Err(FsError::StaleHandle);
                }
                if exists {
                    return Err(FsError::Exists);
                }

                let new_dir_id = self.inode_store.allocate();

                let (now_sec, now_nsec) = get_current_time();

                let mut new_mode = match &attr.mode {
                    SetMode::Set(m) => *m,
                    SetMode::NoChange => 0o777,
                };

                let parent_mode = dir.mode;
                if parent_mode & 0o2000 != 0 {
                    new_mode |= 0o2000;
                }

                let new_uid = match &attr.uid {
                    SetUid::Set(u) => *u,
                    SetUid::NoChange => creds.uid,
                };

                let new_gid = match &attr.gid {
                    SetGid::Set(g) => *g,
                    SetGid::NoChange => {
                        if parent_mode & 0o2000 != 0 {
                            dir.gid
                        } else {
                            creds.gid
                        }
                    }
                };

                let (atime_sec, atime_nsec) = match &attr.atime {
                    SetTime::SetToClientTime(ts) => (ts.seconds, ts.nanoseconds),
                    SetTime::SetToServerTime | SetTime::NoChange => (now_sec, now_nsec),
                };

                let (mtime_sec, mtime_nsec) = match &attr.mtime {
                    SetTime::SetToClientTime(ts) => (ts.seconds, ts.nanoseconds),
                    SetTime::SetToServerTime | SetTime::NoChange => (now_sec, now_nsec),
                };

                let new_dir_inode = DirectoryInode {
                    mtime: mtime_sec,
                    mtime_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: atime_sec,
                    atime_nsec,
                    mode: new_mode,
                    uid: new_uid,
                    gid: new_gid,
                    entry_count: 0,
                    parent: dirid,
                    name: Some(name.to_vec()),
                    nlink: 2,
                };

                let mut txn = self.db.new_transaction()?;
                let cookie = self
                    .directory_store
                    .allocate_cookie(dirid, &mut txn)
                    .await?;

                let new_dir_inode_enum = Inode::Directory(new_dir_inode.clone());
                let attrs: FileAttributes = InodeWithId {
                    inode: &new_dir_inode_enum,
                    id: new_dir_id,
                }
                .into();
                txn.set_dedup_result(
                    op_id,
                    crate::dedup::DedupResult::Mkdir {
                        inode_id: new_dir_id,
                        attrs: attrs.clone(),
                    },
                );
                self.inode_store
                    .save(&mut txn, new_dir_id, &new_dir_inode_enum)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKDIR_AFTER_INODE);

                self.directory_store.add(
                    &mut txn,
                    dirid,
                    name,
                    new_dir_id,
                    cookie,
                    Some(&new_dir_inode_enum),
                );

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKDIR_AFTER_DIR_ENTRY);

                dir.entry_count += 1;
                if dir.nlink == u32::MAX {
                    return Err(FsError::NoSpace);
                }
                dir.nlink += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let parent_update_info = dir.name.clone().map(|n| (dir.parent, n));

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                if let Some((parent_id, dir_name)) = parent_update_info {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &dir_name, dirid, &dir_inode)
                        .await
                        .ok();
                }

                txn.add_stats_delta(new_dir_id, 0, 1);

                self.write_coordinator.commit(txn).await?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKDIR_AFTER_COMMIT);

                self.stats
                    .directories_created
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    new_dir_id,
                    FileOperation::Mkdir {
                        mode: new_dir_inode.mode,
                    },
                );

                Ok((new_dir_id, attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    /// Create a special node: fifo, socket, or device (`rdev` = (major, minor)
    /// for char/block devices). Other file types are EINVAL.
    pub async fn mknod(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        ftype: FileType,
        attr: &SetAttributes,
        rdev: Option<(u32, u32)>,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        self.mknod_idempotent(creds, dirid, name, ftype, attr, rdev, [0u8; 16])
            .await
    }

    /// `mknod` tagged with an idempotency op-id: an applied retry returns the
    /// existing node instead of EEXIST. See [`Self::create_idempotent`].
    #[allow(clippy::too_many_arguments)]
    pub async fn mknod_idempotent(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        name: &[u8],
        ftype: FileType,
        attr: &SetAttributes,
        rdev: Option<(u32, u32)>,
        op_id: crate::dedup::OpId,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_mknod)? {
            return Ok(result);
        }
        validate_filename(name)?;

        debug!(
            "mknod: dirid={}, filename={}, ftype={:?}",
            dirid,
            String::from_utf8_lossy(name),
            ftype
        );

        let _guard = self.lock_manager.acquire(dirid).await;
        // Direct filesystem callers do not pass through the 9P single-flight.
        if let Some(result) = self.replay_dedup_result(&op_id, DedupResult::into_mknod)? {
            return Ok(result);
        }
        let (mut dir_inode, exists) = tokio::try_join!(
            self.inode_store.get(dirid),
            self.directory_store.exists(dirid, name)
        )?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                if dir.nlink == 0 {
                    return Err(FsError::StaleHandle);
                }
                if exists {
                    debug!("File already exists");
                    return Err(FsError::Exists);
                }

                let special_id = self.inode_store.allocate();
                let (now_sec, now_nsec) = get_current_time();

                let base_mode = match ftype {
                    FileType::Fifo => 0o666,
                    FileType::CharDevice | FileType::BlockDevice => 0o666,
                    FileType::Socket => 0o666,
                    _ => return Err(FsError::InvalidArgument),
                };

                let final_mode = if let SetMode::Set(m) = attr.mode {
                    validate_mode(m)
                } else {
                    base_mode
                };

                let special_inode = SpecialInode {
                    mtime: now_sec,
                    mtime_nsec: now_nsec,
                    ctime: now_sec,
                    ctime_nsec: now_nsec,
                    atime: now_sec,
                    atime_nsec: now_nsec,
                    mode: final_mode,
                    uid: match attr.uid {
                        SetUid::Set(u) => u,
                        _ => creds.uid,
                    },
                    gid: match attr.gid {
                        SetGid::Set(g) => g,
                        _ => creds.gid,
                    },
                    parent: Some(dirid),
                    name: Some(name.to_vec()),
                    nlink: 1,
                    rdev,
                };

                let inode = match ftype {
                    FileType::Fifo => Inode::Fifo(special_inode),
                    FileType::CharDevice => Inode::CharDevice(special_inode),
                    FileType::BlockDevice => Inode::BlockDevice(special_inode),
                    FileType::Socket => Inode::Socket(special_inode),
                    _ => return Err(FsError::InvalidArgument),
                };

                let mut txn = self.db.new_transaction()?;
                let cookie = self
                    .directory_store
                    .allocate_cookie(dirid, &mut txn)
                    .await?;

                let attrs: FileAttributes = InodeWithId {
                    inode: &inode,
                    id: special_id,
                }
                .into();
                txn.set_dedup_result(
                    op_id,
                    crate::dedup::DedupResult::Mknod {
                        inode_id: special_id,
                        attrs: attrs.clone(),
                    },
                );

                self.inode_store.save(&mut txn, special_id, &inode)?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKNOD_AFTER_INODE);

                self.directory_store
                    .add(&mut txn, dirid, name, special_id, cookie, Some(&inode));

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKNOD_AFTER_DIR_ENTRY);

                dir.entry_count += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let parent_update_info = dir.name.clone().map(|n| (dir.parent, n));

                self.inode_store.save(&mut txn, dirid, &dir_inode)?;

                if let Some((parent_id, dir_name)) = parent_update_info {
                    self.directory_store
                        .update_inode_in_entry(&mut txn, parent_id, &dir_name, dirid, &dir_inode)
                        .await
                        .ok();
                }

                txn.add_stats_delta(special_id, 0, 1);

                self.write_coordinator.commit(txn).await?;

                #[cfg(feature = "failpoints")]
                fail_point!(fp::MKNOD_AFTER_COMMIT);

                self.stats.files_created.fetch_add(1, Ordering::Relaxed);
                self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    special_id,
                    FileOperation::Mknod { mode: final_mode },
                );

                Ok((special_id, attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::inode::Inode;
    use crate::fs::key_codec::KeyCodec;

    use crate::fs::types::{FileType, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid};

    #[tokio::test]
    async fn test_process_create_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = SetAttributes {
            mode: SetMode::Set(0o644),
            uid: SetUid::Set(1000),
            gid: SetGid::Set(1000),
            ..Default::default()
        };

        let (file_id, fattr) = fs
            .create(&test_creds(), 0, b"test.txt", &attr)
            .await
            .unwrap();

        assert!(file_id > 0);
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.size, 0);

        // Check that the file was added to the directory
        let entry_key = KeyCodec::new().dir_entry_key(0, b"test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap().unwrap();
        let (stored_id, cookie) = KeyCodec::decode_dir_entry(&entry_data).unwrap();
        assert_eq!(stored_id, file_id);
        assert_eq!(cookie, super::COOKIE_FIRST_ENTRY);

        let (second_id, _) = fs
            .create(&test_creds(), 0, b"second.txt", &SetAttributes::default())
            .await
            .unwrap();
        let second_entry_key = KeyCodec::new().dir_entry_key(0, b"second.txt");
        let second_entry_data = fs.db.get_bytes(&second_entry_key).await.unwrap().unwrap();
        let (stored_second_id, second_cookie) =
            KeyCodec::decode_dir_entry(&second_entry_data).unwrap();
        assert_eq!(stored_second_id, second_id);
        assert_eq!(second_cookie, super::COOKIE_FIRST_ENTRY + 1);

        let counter_key = KeyCodec::new().dir_cookie_counter_key(0);
        let counter_data = fs.db.get_bytes(&counter_key).await.unwrap().unwrap();
        assert_eq!(
            KeyCodec::decode_counter(&counter_data).unwrap(),
            super::COOKIE_FIRST_ENTRY + 2
        );
    }

    #[tokio::test]
    async fn test_process_create_file_already_exists() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = &SetAttributes::default();

        let _ = fs
            .create(&test_creds(), 0, b"test.txt", attr)
            .await
            .unwrap();

        let result = fs.create(&test_creds(), 0, b"test.txt", attr).await;
        assert!(matches!(result, Err(FsError::Exists)));
    }

    #[tokio::test]
    async fn create_retry_replays_original_result_after_name_is_replaced() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let op_id = [1u8; 16];
        let attrs = SetAttributes {
            mode: SetMode::Set(0o640),
            ..Default::default()
        };

        let (original_id, original_attrs) = fs
            .create_idempotent(&test_creds(), 0, b"file", &attrs, op_id)
            .await
            .unwrap();
        fs.remove(&(&test_auth()).into(), 0, b"file").await.unwrap();
        let (replacement_id, _) = fs
            .create(&test_creds(), 0, b"file", &SetAttributes::default())
            .await
            .unwrap();

        let (retry_id, retry_attrs) = fs
            .create_idempotent(&test_creds(), 0, b"file", &attrs, op_id)
            .await
            .unwrap();
        assert_eq!(retry_id, original_id);
        assert_eq!(retry_attrs.fileid, original_attrs.fileid);
        assert_eq!(retry_attrs.mode, original_attrs.mode);
        assert_eq!(retry_attrs.mtime, original_attrs.mtime);
        assert_eq!(
            fs.lookup(&test_creds(), 0, b"file").await.unwrap(),
            replacement_id
        );
        assert_ne!(replacement_id, original_id);
    }

    #[tokio::test]
    async fn test_process_mkdir() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        assert!(dir_id > 0);
        assert_eq!(fattr.mode, 0o777);
        assert_eq!(fattr.file_type, FileType::Directory);

        let new_dir_inode = fs.inode_store.get(dir_id).await.unwrap();
        match new_dir_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn mkdir_and_mknod_retries_do_not_retarget_replacements() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let mkdir_op = [2u8; 16];
        let (old_dir, old_dir_attrs) = fs
            .mkdir_idempotent(
                &test_creds(),
                0,
                b"dir",
                &SetAttributes::default(),
                mkdir_op,
            )
            .await
            .unwrap();
        fs.remove(&(&test_auth()).into(), 0, b"dir").await.unwrap();
        let (new_dir, _) = fs
            .mkdir(&test_creds(), 0, b"dir", &SetAttributes::default())
            .await
            .unwrap();
        let (retry_dir, retry_dir_attrs) = fs
            .mkdir_idempotent(
                &test_creds(),
                0,
                b"dir",
                &SetAttributes::default(),
                mkdir_op,
            )
            .await
            .unwrap();
        assert_eq!(retry_dir, old_dir);
        assert_eq!(retry_dir_attrs.fileid, old_dir_attrs.fileid);
        assert_eq!(fs.lookup(&test_creds(), 0, b"dir").await.unwrap(), new_dir);

        let mknod_op = [3u8; 16];
        let (old_node, old_node_attrs) = fs
            .mknod_idempotent(
                &test_creds(),
                0,
                b"node",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
                mknod_op,
            )
            .await
            .unwrap();
        fs.remove(&(&test_auth()).into(), 0, b"node").await.unwrap();
        let (new_node, _) = fs
            .mknod(
                &test_creds(),
                0,
                b"node",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
            .unwrap();
        let (retry_node, retry_node_attrs) = fs
            .mknod_idempotent(
                &test_creds(),
                0,
                b"node",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
                mknod_op,
            )
            .await
            .unwrap();
        assert_eq!(retry_node, old_node);
        assert_eq!(retry_node_attrs.fileid, old_node_attrs.fileid);
        assert_eq!(
            fs.lookup(&test_creds(), 0, b"node").await.unwrap(),
            new_node
        );
    }

    #[tokio::test]
    async fn test_process_mkdir_with_custom_attrs() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Test with custom mode
        let custom_attrs = SetAttributes {
            mode: SetMode::Set(0o700),
            uid: SetUid::Set(1001),
            gid: SetGid::Set(1001),
            size: SetSize::NoChange,
            atime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
            mtime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
        };

        let (_dir_id, fattr) = fs
            .mkdir(&test_creds(), 0, b"customdir", &custom_attrs)
            .await
            .unwrap();

        // Check that attributes were applied correctly
        assert_eq!(fattr.mode & 0o777, 0o700, "Custom mode should be applied");
        assert_eq!(fattr.uid, 1001, "Custom uid should be applied");
        assert_eq!(fattr.gid, 1001, "Custom gid should be applied");
        assert_eq!(
            fattr.atime.seconds, 1234567890,
            "Custom atime should be applied"
        );
        assert_eq!(
            fattr.mtime.seconds, 1234567890,
            "Custom mtime should be applied"
        );
    }
}
