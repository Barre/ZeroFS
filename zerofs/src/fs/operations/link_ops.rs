use super::common::validate_filename;
use crate::fs::cache::CacheKey;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, SymlinkInode};
use crate::fs::key_codec::KeyCodec;
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::types::{
    AuthContext, FileAttributes, InodeId, InodeWithId, SetAttributes, SetGid, SetMode, SetUid,
};
use crate::fs::{MAX_HARDLINKS_PER_INODE, ZeroFS, get_current_time};
use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::debug;

impl ZeroFS {
    pub async fn process_symlink(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        linkname: &[u8],
        target: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(linkname)?;

        debug!(
            "process_symlink: dirid={}, linkname={:?}, target={:?}",
            dirid,
            String::from_utf8_lossy(linkname),
            target
        );

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid),
            _ => (65534, 65534),
        };

        let dir = match &mut dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        let entry_key = KeyCodec::dir_entry_key(dirid, linkname);
        if self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .is_some()
        {
            return Err(FsError::Exists);
        }

        let new_id = self.allocate_inode().await?;

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
            parent: dirid,
            nlink: 1,
        });

        let mut batch = self.db.new_write_batch();

        let inode_key = KeyCodec::inode_key(new_id);
        let inode_data = bincode::serialize(&symlink_inode)?;
        batch.put_bytes(&inode_key, &inode_data);

        batch.put_bytes(&entry_key, &KeyCodec::encode_dir_entry(new_id));

        let scan_key = KeyCodec::dir_scan_key(dirid, new_id, linkname);
        batch.put_bytes(&scan_key, &KeyCodec::encode_dir_entry(new_id));

        dir.entry_count += 1;
        dir.mtime = now_sec;
        dir.mtime_nsec = now_nsec;
        dir.ctime = now_sec;
        dir.ctime_nsec = now_nsec;

        let counter_key = KeyCodec::system_counter_key();
        let next_id = self.next_inode_id.load(Ordering::SeqCst);
        batch.put_bytes(&counter_key, &KeyCodec::encode_counter(next_id));

        let dir_key = KeyCodec::inode_key(dirid);
        let dir_data = bincode::serialize(&dir_inode)?;
        batch.put_bytes(&dir_key, &dir_data);

        let stats_update = self.global_stats.prepare_inode_create(new_id).await;
        self.global_stats.add_to_batch(&stats_update, &mut batch)?;

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        self.global_stats.commit_update(&stats_update);

        self.cache.remove(CacheKey::Metadata(dirid));

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        Ok((
            new_id,
            InodeWithId {
                inode: &symlink_inode,
                id: new_id,
            }
            .into(),
        ))
    }

    pub async fn process_link(
        &self,
        auth: &AuthContext,
        fileid: InodeId,
        linkdirid: InodeId,
        linkname: &[u8],
    ) -> Result<(), FsError> {
        validate_filename(linkname)?;

        let linkname_str = String::from_utf8_lossy(linkname);
        debug!(
            "process_link: fileid={}, linkdirid={}, linkname={}",
            fileid, linkdirid, linkname_str
        );

        let _guards = self
            .lock_manager
            .acquire_multiple_write(vec![fileid, linkdirid])
            .await;

        let link_dir_inode = self.load_inode(linkdirid).await?;
        let creds = Credentials::from_auth_context(auth);

        check_access(&link_dir_inode, &creds, AccessMode::Write)?;
        check_access(&link_dir_inode, &creds, AccessMode::Execute)?;

        self.check_parent_execute_permissions(fileid, &creds)
            .await?;

        let mut link_dir = match link_dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(FsError::NotDirectory),
        };

        let mut file_inode = self.load_inode(fileid).await?;

        if matches!(file_inode, Inode::Directory(_)) {
            return Err(FsError::InvalidArgument);
        }

        if matches!(file_inode, Inode::Symlink(_)) {
            return Err(FsError::InvalidArgument);
        }

        let entry_key = KeyCodec::dir_entry_key(linkdirid, linkname);

        if self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| FsError::IoError)?
            .is_some()
        {
            return Err(FsError::Exists);
        }

        let mut batch = self.db.new_write_batch();
        batch.put_bytes(&entry_key, &KeyCodec::encode_dir_entry(fileid));

        let scan_key = KeyCodec::dir_scan_key(linkdirid, fileid, linkname);
        batch.put_bytes(&scan_key, &KeyCodec::encode_dir_entry(fileid));

        let (now_sec, now_nsec) = get_current_time();
        match &mut file_inode {
            Inode::File(file) => {
                if file.nlink >= MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                file.nlink += 1;
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if special.nlink >= MAX_HARDLINKS_PER_INODE {
                    return Err(FsError::TooManyLinks);
                }
                special.nlink += 1;
                special.ctime = now_sec;
                special.ctime_nsec = now_nsec;
            }
            _ => unreachable!(),
        }

        let file_inode_key = KeyCodec::inode_key(fileid);
        let file_inode_data = bincode::serialize(&file_inode)?;
        batch.put_bytes(&file_inode_key, &file_inode_data);

        link_dir.entry_count += 1;
        link_dir.mtime = now_sec;
        link_dir.mtime_nsec = now_nsec;
        link_dir.ctime = now_sec;
        link_dir.ctime_nsec = now_nsec;

        let dir_inode_key = KeyCodec::inode_key(linkdirid);
        let dir_inode_data = bincode::serialize(&Inode::Directory(link_dir))?;
        batch.put_bytes(&dir_inode_key, &dir_inode_data);

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| FsError::IoError)?;

        self.cache.remove_batch(vec![
            CacheKey::Metadata(fileid),
            CacheKey::Metadata(linkdirid),
        ]);

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}
