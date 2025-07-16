use futures::stream::{self, StreamExt};
use nfsserve::nfs::{
    fattr3, fileid3, nfsstat3, nfstime3, sattr3, set_atime, set_gid3, set_mode3, set_mtime,
    set_uid3,
};
use nfsserve::vfs::{AuthContext, DirEntry, ReadDirResult};
use slatedb::{WriteBatch, config::WriteOptions};
use std::sync::atomic::Ordering;
use tracing::debug;

use super::common::validate_filename;
use crate::filesystem::{SlateDbFs, get_current_time};
use crate::inode::{DirectoryInode, Inode};
use crate::permissions::{AccessMode, Credentials, check_access};

impl SlateDbFs {
    pub async fn process_mkdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        dirname: &[u8],
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        validate_filename(dirname)?;

        let dirname_str = String::from_utf8_lossy(dirname);
        debug!("process_mkdir: dirid={}, dirname={}", dirid, dirname_str);

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Write)?;
        check_access(&dir_inode, &creds, AccessMode::Execute)?;

        let (_default_uid, _default_gid) = match &dir_inode {
            Inode::Directory(d) => (d.uid, d.gid),
            _ => {
                #[cfg(unix)]
                unsafe {
                    (libc::getuid(), libc::getgid())
                }
                #[cfg(not(unix))]
                (0, 0)
            }
        };

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = dirname_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                    .is_some()
                {
                    return Err(nfsstat3::NFS3ERR_EXIST);
                }

                let new_dir_id = self.allocate_inode().await?;

                let (now_sec, now_nsec) = get_current_time();

                let mut new_mode = match attr.mode {
                    set_mode3::mode(m) => m,
                    set_mode3::Void => 0o777,
                };

                let parent_mode = dir.mode;
                if parent_mode & 0o2000 != 0 {
                    new_mode |= 0o2000;
                }

                // Apply uid/gid from attributes, with defaults
                let new_uid = match attr.uid {
                    set_uid3::uid(u) => u,
                    set_uid3::Void => auth.uid,
                };

                let new_gid = match attr.gid {
                    set_gid3::gid(g) => g,
                    set_gid3::Void => {
                        // If parent has setgid bit, inherit parent's gid
                        if parent_mode & 0o2000 != 0 {
                            dir.gid
                        } else {
                            auth.gid
                        }
                    }
                };

                // Apply time attributes
                let (atime_sec, atime_nsec) = match attr.atime {
                    set_atime::SET_TO_CLIENT_TIME(nfstime3 { seconds, nseconds }) => {
                        (seconds as u64, nseconds)
                    }
                    set_atime::SET_TO_SERVER_TIME | set_atime::DONT_CHANGE => (now_sec, now_nsec),
                };

                let (mtime_sec, mtime_nsec) = match attr.mtime {
                    set_mtime::SET_TO_CLIENT_TIME(nfstime3 { seconds, nseconds }) => {
                        (seconds as u64, nseconds)
                    }
                    set_mtime::SET_TO_SERVER_TIME | set_mtime::DONT_CHANGE => (now_sec, now_nsec),
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
                    nlink: 2, // . and parent's reference
                };

                let mut batch = WriteBatch::new();

                let new_dir_key = Self::inode_key(new_dir_id);
                let new_dir_data = bincode::serialize(&Inode::Directory(new_dir_inode.clone()))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(new_dir_key, &new_dir_data);

                batch.put(entry_key, new_dir_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, new_dir_id, &name);
                batch.put(scan_key, new_dir_id.to_le_bytes());

                dir.entry_count += 1;
                dir.nlink += 1; // New subdirectory's ".." points to this directory
                let (now_sec, now_nsec) = get_current_time();
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                // Persist the counter
                let counter_key = Self::counter_key();
                let next_id = self.next_inode_id.load(Ordering::SeqCst);
                batch.put(counter_key, next_id.to_le_bytes());

                let parent_dir_key = Self::inode_key(dirid);
                let parent_dir_data =
                    bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch.put(parent_dir_key, &parent_dir_data);

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                self.metadata_cache.remove(&dirid);

                Ok((
                    new_dir_id,
                    Inode::Directory(new_dir_inode).to_fattr3(new_dir_id),
                ))
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    pub async fn process_readdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        debug!(
            "process_readdir: dirid={}, start_after={}, max_entries={}",
            dirid, start_after, max_entries
        );

        let _guard = self.lock_manager.acquire_read(dirid).await;

        let dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Read)?;

        match &dir_inode {
            Inode::Directory(dir) => {
                let mut entries = Vec::new();

                if start_after == 0 {
                    debug!("readdir: adding . entry for current directory");
                    entries.push(DirEntry {
                        fileid: dirid,
                        name: b".".to_vec().into(),
                        attr: dir_inode.to_fattr3(dirid),
                    });

                    debug!("readdir: adding .. entry for parent directory");
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        // Root directory, use self
                        dir_inode.to_fattr3(dirid)
                    } else {
                        // Load parent directory attributes
                        match self.load_inode(parent_id).await {
                            Ok(parent_inode) => parent_inode.to_fattr3(parent_id),
                            Err(_) => dir_inode.to_fattr3(dirid), // Fallback to self on error
                        }
                    };
                    entries.push(DirEntry {
                        fileid: parent_id,
                        name: b"..".to_vec().into(),
                        attr: parent_attr,
                    });
                }

                // Use dirscan index for efficient pagination
                let scan_prefix = Self::dir_scan_prefix(dirid);
                let start_key = if start_after == 0 {
                    scan_prefix.clone()
                } else {
                    // Start right after the given inode (formatted with leading zeros)
                    format!("dirscan:{}/{:020}", dirid, start_after + 1)
                };
                let end_key = format!("dirscan:{}0", dirid);

                let mut iter = self
                    .db
                    .scan(start_key..end_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let mut dir_entries = Vec::new();
                while let Some(kv) = iter.next().await.map_err(|_| nfsstat3::NFS3ERR_IO)? {
                    if dir_entries.len() >= max_entries - entries.len() {
                        debug!("readdir: reached max_entries limit");
                        break;
                    }

                    let key = kv.key;
                    let _value = kv.value;
                    let key_str = String::from_utf8_lossy(&key);

                    // Extract the inode ID and filename from the key
                    // Key format: dirscan:{dir_id}/{inode_id:020}/{filename}
                    if let Some(suffix) = key_str.strip_prefix(&scan_prefix) {
                        // Split by '/' to get inode_id and filename
                        if let Some(slash_pos) = suffix.find('/') {
                            let inode_str = &suffix[..slash_pos];
                            let filename = &suffix[slash_pos + 1..];

                            if let Ok(inode_id) = inode_str.parse::<u64>() {
                                debug!("readdir: found entry {} (inode {})", filename, inode_id);
                                dir_entries.push((inode_id, filename.as_bytes().to_vec()));
                            }
                        }
                    }
                }

                const BUFFER_SIZE: usize = 16;
                let inode_futures =
                    stream::iter(dir_entries.into_iter()).map(|(inode_id, name)| async move {
                        debug!("readdir: loading inode {} for entry", inode_id);
                        let inode = self.load_inode(inode_id).await?;
                        debug!("readdir: loaded inode {} successfully", inode_id);
                        Ok::<(u64, Vec<u8>, Inode), nfsstat3>((inode_id, name, inode))
                    });

                let loaded_entries: Vec<_> = inode_futures
                    .buffered(BUFFER_SIZE)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                // Add the loaded entries to results
                for (inode_id, name, inode) in loaded_entries {
                    entries.push(DirEntry {
                        fileid: inode_id,
                        name: name.into(),
                        attr: inode.to_fattr3(inode_id),
                    });
                    debug!("readdir: added entry to results");
                }

                // Check if we've reached the end by seeing if we got fewer entries than requested
                let end = entries.len() < max_entries;

                let result = ReadDirResult { end, entries };
                debug!(
                    "readdir: returning {} entries, end={}",
                    result.entries.len(),
                    result.end
                );
                Ok(result)
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
}
