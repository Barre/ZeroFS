use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::debug;
use zerofs_nfsserve::nfs::{fattr3, fileid3, nfsstat3, sattr3, set_gid3, set_mode3, set_uid3};
use zerofs_nfsserve::vfs::AuthContext;

use super::common::validate_filename;
use crate::cache::CacheKey;
use crate::filesystem::{SlateDbFs, get_current_time};
use crate::inode::{Inode, SymlinkInode};
use crate::permissions::{AccessMode, Credentials, check_access};

impl SlateDbFs {
    pub async fn process_symlink(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        linkname: &[u8],
        target: &[u8],
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        validate_filename(linkname)?;

        debug!(
            "process_symlink: dirid={}, linkname={:?}, target={:?}",
            dirid, linkname, target
        );

        let linkname_str = String::from_utf8_lossy(linkname);

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        // Get parent directory's uid/gid as defaults before the mutable borrow
        let (_default_uid, _default_gid) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid),
            _ => (65534, 65534),
        };

        let dir = match &mut dir_inode {
            Inode::Directory(d) => d,
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };

        let entry_key = Self::dir_entry_key(dirid, &linkname_str);
        if self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .is_some()
        {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        let new_id = self.allocate_inode().await?;

        let mode = if let set_mode3::mode(m) = attr.mode {
            m | 0o120000
        } else {
            0o120777
        };

        let uid = if let set_uid3::uid(u) = attr.uid {
            u
        } else {
            auth.uid
        };

        let gid = if let set_gid3::gid(g) = attr.gid {
            g
        } else {
            auth.gid
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

        let inode_key = Self::inode_key(new_id);
        let inode_data = bincode::serialize(&symlink_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch
            .put_bytes(&inode_key, &inode_data)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Add directory entry (for lookup by name)
        batch
            .put_bytes(&entry_key, &new_id.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        // Add directory scan entry (for efficient readdir)
        let scan_key = Self::dir_scan_key(dirid, new_id, &linkname_str);
        batch
            .put_bytes(&scan_key, &new_id.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        dir.entry_count += 1;
        let (now_sec, now_nsec) = get_current_time();
        dir.mtime = now_sec;
        dir.mtime_nsec = now_nsec;
        dir.ctime = now_sec;
        dir.ctime_nsec = now_nsec;

        // Persist the counter
        let counter_key = Self::counter_key();
        let next_id = self.next_inode_id.load(Ordering::SeqCst);
        batch
            .put_bytes(&counter_key, &next_id.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let dir_key = Self::inode_key(dirid);
        let dir_data = bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch
            .put_bytes(&dir_key, &dir_data)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.metadata_cache.remove(CacheKey::Metadata(dirid));

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        Ok((new_id, symlink_inode.to_fattr3(new_id)))
    }

    pub async fn process_link(
        &self,
        auth: &AuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &[u8],
    ) -> Result<(), nfsstat3> {
        validate_filename(linkname)?;

        let linkname_str = String::from_utf8_lossy(linkname);
        debug!(
            "process_link: fileid={}, linkdirid={}, linkname={}",
            fileid, linkdirid, linkname_str
        );

        // Use lock manager to acquire locks in proper order
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
            _ => return Err(nfsstat3::NFS3ERR_NOTDIR),
        };

        let mut file_inode = self.load_inode(fileid).await?;

        // Don't allow hard links to directories
        if matches!(file_inode, Inode::Directory(_)) {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        // Don't allow hard links to symlinks (they're typically not allowed)
        if matches!(file_inode, Inode::Symlink(_)) {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }

        let name = linkname_str.to_string();
        let entry_key = Self::dir_entry_key(linkdirid, &name);

        if self
            .db
            .get_bytes(&entry_key)
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .is_some()
        {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }

        let mut batch = self.db.new_write_batch();
        batch
            .put_bytes(&entry_key, &fileid.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let scan_key = Self::dir_scan_key(linkdirid, fileid, &name);
        batch
            .put_bytes(&scan_key, &fileid.to_le_bytes())
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        let (now_sec, now_nsec) = get_current_time();
        match &mut file_inode {
            Inode::File(file) => {
                if file.nlink == u32::MAX {
                    return Err(nfsstat3::NFS3ERR_MLINK);
                }
                file.nlink += 1;
                file.ctime = now_sec;
                file.ctime_nsec = now_nsec;
            }
            Inode::Fifo(special)
            | Inode::Socket(special)
            | Inode::CharDevice(special)
            | Inode::BlockDevice(special) => {
                if special.nlink == u32::MAX {
                    return Err(nfsstat3::NFS3ERR_MLINK);
                }
                special.nlink += 1;
                special.ctime = now_sec;
                special.ctime_nsec = now_nsec;
            }
            _ => unreachable!(), // We already filtered out directories and symlinks
        }

        let file_inode_key = Self::inode_key(fileid);
        let file_inode_data = bincode::serialize(&file_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch
            .put_bytes(&file_inode_key, &file_inode_data)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        link_dir.entry_count += 1;
        link_dir.mtime = now_sec;
        link_dir.mtime_nsec = now_nsec;
        link_dir.ctime = now_sec;
        link_dir.ctime_nsec = now_nsec;

        let dir_inode_key = Self::inode_key(linkdirid);
        let dir_inode_data =
            bincode::serialize(&Inode::Directory(link_dir)).map_err(|_| nfsstat3::NFS3ERR_IO)?;
        batch
            .put_bytes(&dir_inode_key, &dir_inode_data)
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?;

        self.metadata_cache.remove(CacheKey::Metadata(fileid)); // File's metadata changed (nlink, ctime)
        self.metadata_cache.remove(CacheKey::Metadata(linkdirid)); // Directory's metadata changed

        self.stats.links_created.fetch_add(1, Ordering::Relaxed);
        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}
