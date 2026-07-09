//! symlink and link (hardlink), with their idempotent variants.

#[cfg(feature = "failpoints")]
use crate::failpoints as fp;
#[cfg(feature = "failpoints")]
use fp::fail_point;

use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId, SymlinkInode};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::store::inode::MAX_HARDLINKS_PER_INODE;
use crate::fs::tracing::FileOperation;
use crate::fs::types::{
    AuthContext, FileAttributes, InodeWithId, SetAttributes, SetGid, SetMode, SetUid,
};
use crate::fs::{ZeroFS, get_current_time, validate_filename};
use ::tracing::debug;
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Create a symlink to `target`, an opaque byte string the fs never
    /// resolves or validates.
    pub async fn symlink(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        linkname: &[u8],
        target: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        self.symlink_idempotent(creds, dirid, linkname, target, attr, [0u8; 16])
            .await
    }

    /// `symlink` tagged with an idempotency op-id: an applied retry returns the
    /// existing link instead of EEXIST. See [`Self::create_idempotent`].
    pub async fn symlink_idempotent(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        linkname: &[u8],
        target: &[u8],
        attr: &SetAttributes,
        op_id: crate::dedup::OpId,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(linkname)?;

        debug!(
            "symlink: dirid={}, linkname={:?}, target={:?}",
            dirid,
            String::from_utf8_lossy(linkname),
            target
        );

        let _guard = self.lock_manager.acquire(dirid).await;
        let (mut dir_inode, exists) = tokio::try_join!(
            self.inode_store.get(dirid),
            self.directory_store.exists(dirid, linkname)
        )?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        let dir = match &mut dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        if exists {
            // Applied retry of our own op: return the existing link.
            if crate::dedup::has_op_id(&op_id) && self.dedup.get(&op_id).is_some() {
                return self.existing_child_result(dirid, linkname).await;
            }
            return Err(FsError::Exists);
        }

        let new_id = self.inode_store.allocate();

        let mode = match &attr.mode {
            SetMode::Set(m) => *m | 0o120000,
            SetMode::NoChange => 0o120777,
        };

        let uid = match &attr.uid {
            SetUid::Set(u) => *u,
            SetUid::NoChange => creds.uid,
        };

        let gid = match &attr.gid {
            SetGid::Set(g) => *g,
            SetGid::NoChange => creds.gid,
        };

        let (now_sec, now_nsec) = get_current_time();
        let symlink_inode = Inode::Symlink(SymlinkInode {
            target: target.to_vec(),
            mtime: now_sec,
            mtime_nsec: now_nsec,
            ctime: now_sec,
            ctime_nsec: now_nsec,
            atime: now_sec,
            atime_nsec: now_nsec,
            mode,
            uid,
            gid,
            parent: Some(dirid),
            name: Some(linkname.to_vec()),
            nlink: 1,
        });

        let mut txn = self.db.new_transaction()?;
        txn.set_op_id(op_id);
        let cookie = self
            .directory_store
            .allocate_cookie(dirid, &mut txn)
            .await?;

        self.inode_store.save(&mut txn, new_id, &symlink_inode)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::SYMLINK_AFTER_INODE);

        self.directory_store.add(
            &mut txn,
            dirid,
            linkname,
            new_id,
            cookie,
            Some(&symlink_inode),
        );

        #[cfg(feature = "failpoints")]
        fail_point!(fp::SYMLINK_AFTER_DIR_ENTRY);

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

        txn.add_stats_delta(new_id, 0, 1);

        self.write_coordinator.commit(txn).await?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::SYMLINK_AFTER_COMMIT);

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        self.tracer.emit(
            &self.inode_store,
            new_id,
            FileOperation::Symlink {
                target: String::from_utf8_lossy(target).to_string(),
            },
        );

        Ok((
            new_id,
            InodeWithId {
                inode: &symlink_inode,
                id: new_id,
            }
            .into(),
        ))
    }

    /// Hard-link `fileid` as `linkdirid/linkname`. Directories and symlinks
    /// are EINVAL; nlink caps at [`MAX_HARDLINKS_PER_INODE`]. Linking an
    /// open-unlinked inode (nlink 0) resurrects it and clears its orphan
    /// record.
    pub async fn link(
        &self,
        auth: &AuthContext,
        fileid: InodeId,
        linkdirid: InodeId,
        linkname: &[u8],
    ) -> Result<(), FsError> {
        self.link_idempotent(auth, fileid, linkdirid, linkname, [0u8; 16])
            .await
    }

    /// `link` tagged with an idempotency op-id: an applied retry returns success
    /// instead of EEXIST. See [`Self::create_idempotent`].
    pub async fn link_idempotent(
        &self,
        auth: &AuthContext,
        fileid: InodeId,
        linkdirid: InodeId,
        linkname: &[u8],
        op_id: crate::dedup::OpId,
    ) -> Result<(), FsError> {
        validate_filename(linkname)?;

        let linkname_str = String::from_utf8_lossy(linkname);
        debug!(
            "link: fileid={}, linkdirid={}, linkname={}",
            fileid, linkdirid, linkname_str
        );

        let _guards = self
            .lock_manager
            .acquire_multi(vec![fileid, linkdirid])
            .await;

        let creds = Credentials::from_auth_context(auth);

        let (link_dir_inode, mut file_inode, exists) = tokio::try_join!(
            self.inode_store.get(linkdirid),
            self.inode_store.get(fileid),
            self.directory_store.exists(linkdirid, linkname)
        )?;

        check_access(&link_dir_inode, &creds, AccessMode::Write)?;
        check_access(&link_dir_inode, &creds, AccessMode::Execute)?;

        let mut link_dir = match link_dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        if matches!(file_inode, Inode::Directory(_)) {
            return Err(FsError::InvalidArgument);
        }

        if matches!(file_inode, Inode::Symlink(_)) {
            return Err(FsError::InvalidArgument);
        }

        if exists {
            // Applied retry of our own op: the link is present, report success.
            if crate::dedup::has_op_id(&op_id) && self.dedup.get(&op_id).is_some() {
                return Ok(());
            }
            return Err(FsError::Exists);
        }

        let original_parent_name = file_inode
            .parent()
            .zip(file_inode.name().map(|n| n.to_vec()));

        let mut txn = self.db.new_transaction()?;
        txn.set_op_id(op_id);
        let cookie = self
            .directory_store
            .allocate_cookie(linkdirid, &mut txn)
            .await?;

        self.directory_store
            .add(&mut txn, linkdirid, linkname, fileid, cookie, None);

        #[cfg(feature = "failpoints")]
        fail_point!(fp::LINK_AFTER_DIR_ENTRY);

        if let Some((orig_parent, orig_name)) = original_parent_name {
            self.directory_store
                .convert_to_reference(&mut txn, orig_parent, &orig_name, fileid)
                .await?;
        }

        let (now_sec, now_nsec) = get_current_time();
        // True when we are resurrecting an open-unlinked inode (nlink 0 -> 1):
        // linkat(AT_EMPTY_PATH) can give a name back to a file held open after
        // its last unlink. It's no longer an orphan, so its durable orphan-set
        // key must be cleared in this same txn.
        let resurrected;
        match &mut file_inode {
            Inode::File(file) => {
                if file.nlink == MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                resurrected = file.nlink == 0;
                file.nlink += 1;
                if file.nlink > 1 {
                    file.parent = None;
                    file.name = None;
                }
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if special.nlink == MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                resurrected = special.nlink == 0;
                special.nlink += 1;
                if special.nlink > 1 {
                    special.parent = None;
                    special.name = None;
                }
                special.ctime = now_sec;
                special.ctime_nsec = now_nsec;
            }
            _ => unreachable!(),
        }

        if resurrected {
            self.orphan_store.remove(&mut txn, fileid);
        }

        self.inode_store.save(&mut txn, fileid, &file_inode)?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::LINK_AFTER_INODE);

        link_dir.entry_count += 1;
        link_dir.mtime = now_sec;
        link_dir.mtime_nsec = now_nsec;
        link_dir.ctime = now_sec;
        link_dir.ctime_nsec = now_nsec;

        let link_dir_inode_updated = Inode::Directory(link_dir.clone());
        self.inode_store
            .save(&mut txn, linkdirid, &link_dir_inode_updated)?;

        if let Some(dir_name) = &link_dir.name {
            self.directory_store
                .update_inode_in_entry(
                    &mut txn,
                    link_dir.parent,
                    dir_name,
                    linkdirid,
                    &link_dir_inode_updated,
                )
                .await
                .ok();
        }

        self.write_coordinator.commit(txn).await?;

        #[cfg(feature = "failpoints")]
        fail_point!(fp::LINK_AFTER_COMMIT);

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.write_operations.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        // Emit event with the original file's path and new link path
        if self.tracer.has_subscribers() {
            let inode_store = self.inode_store.clone();
            let tracer = self.tracer.clone();
            let linkname = String::from_utf8_lossy(linkname).to_string();
            tokio::spawn(async move {
                let file_path = inode_store.resolve_path_lossy(fileid).await;
                let dir_path = inode_store.resolve_path_lossy(linkdirid).await;
                let new_path = format!("{}/{}", dir_path.trim_end_matches('/'), linkname);
                tracer.emit_with_path(file_path, FileOperation::Link { new_path });
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::inode::Inode;
    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::store::inode::MAX_HARDLINKS_PER_INODE;
    use crate::fs::types::{FileType, SetAttributes};

    #[tokio::test]
    async fn test_process_symlink() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let target = b"/path/to/target";
        let attr = &SetAttributes::default();

        let (link_id, fattr) = fs
            .symlink(&test_creds(), 0, b"link", target, attr)
            .await
            .unwrap();

        assert!(link_id > 0);
        assert_eq!(fattr.file_type, FileType::Symlink);
        assert_eq!(fattr.size, target.len() as u64);

        let link_inode = fs.inode_store.get(link_id).await.unwrap();
        match link_inode {
            Inode::Symlink(symlink) => {
                assert_eq!(symlink.target, target);
            }
            _ => panic!("Should be a symlink"),
        }
    }

    #[tokio::test]
    async fn test_max_hardlinks_limit() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a file
        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Manually set nlink to just below the limit to avoid creating 65k files
        let mut inode = fs.inode_store.get(file_id).await.unwrap();
        match &mut inode {
            Inode::File(file) => {
                file.nlink = MAX_HARDLINKS_PER_INODE - 1;
            }
            _ => panic!("Expected file inode"),
        }
        let mut txn = fs.db.new_transaction().unwrap();
        fs.inode_store.save(&mut txn, file_id, &inode).unwrap();
        fs.write_coordinator.commit(txn).await.unwrap();

        // Create one more hardlink - should succeed
        let result = fs
            .link(&(&test_auth()).into(), file_id, 0, b"last_link.txt")
            .await;
        assert!(result.is_ok());

        // Verify the file now has exactly MAX_HARDLINKS_PER_INODE links
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }

        // Try to create one more hardlink - should fail
        let result = fs
            .link(&(&test_auth()).into(), file_id, 0, b"one_too_many.txt")
            .await;
        assert!(matches!(result, Err(FsError::TooManyLinks)));

        // Verify the file still has MAX_HARDLINKS_PER_INODE links
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }
    }
}
