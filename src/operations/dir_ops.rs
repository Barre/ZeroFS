use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::debug;
use zerofs_nfsserve::nfs::{
    fattr3, fileid3, nfsstat3, nfstime3, sattr3, set_atime, set_gid3, set_mode3, set_mtime,
    set_uid3,
};
use zerofs_nfsserve::vfs::{AuthContext, DirEntry, ReadDirResult};

use super::common::validate_filename;
use crate::cache::CacheKey;
use crate::filesystem::{EncodedFileId, SlateDbFs, get_current_time};
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

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = dirname_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get_bytes(&entry_key)
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

                let mut batch = self.db.new_write_batch();

                let new_dir_key = Self::inode_key(new_dir_id);
                let new_dir_data = bincode::serialize(&Inode::Directory(new_dir_inode.clone()))
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&new_dir_key, &new_dir_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                batch
                    .put_bytes(&entry_key, &new_dir_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let scan_key = Self::dir_scan_key(dirid, new_dir_id, &name);
                batch
                    .put_bytes(&scan_key, &new_dir_id.to_le_bytes())
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                dir.entry_count += 1;
                if dir.nlink == u32::MAX {
                    return Err(nfsstat3::NFS3ERR_NOSPC);
                }
                dir.nlink += 1; // New subdirectory's ".." points to this directory
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

                let parent_dir_key = Self::inode_key(dirid);
                let parent_dir_data =
                    bincode::serialize(&dir_inode).map_err(|_| nfsstat3::NFS3ERR_IO)?;
                batch
                    .put_bytes(&parent_dir_key, &parent_dir_data)
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                let stats_update = self.global_stats.prepare_inode_create(new_dir_id).await;
                self.global_stats.add_to_batch(&stats_update, &mut batch)?;

                self.db
                    .write_with_options(
                        batch,
                        &WriteOptions {
                            await_durable: false,
                        },
                    )
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;

                self.global_stats.commit_update(&stats_update);

                self.metadata_cache.remove(CacheKey::Metadata(dirid));

                self.stats
                    .directories_created
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

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

        let dir_inode = self.load_inode(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Read)?;

        match &dir_inode {
            Inode::Directory(dir) => {
                let mut entries = Vec::new();
                let mut inode_positions = std::collections::HashMap::new();

                let (start_inode, start_position) = if start_after == 0 {
                    (0, 0)
                } else {
                    EncodedFileId::from(start_after).decode()
                };

                let skip_special = start_after != 0;

                if !skip_special {
                    debug!("readdir: adding . entry for current directory");
                    entries.push(DirEntry {
                        fileid: dirid,
                        name: b".".to_vec().into(),
                        attr: dir_inode.to_fattr3(dirid),
                    });

                    debug!("readdir: adding .. entry for parent directory");
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        dir_inode.to_fattr3(dirid)
                    } else {
                        match self.load_inode(parent_id).await {
                            Ok(parent_inode) => parent_inode.to_fattr3(parent_id),
                            Err(_) => dir_inode.to_fattr3(dirid),
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
                    // Resume from the exact inode we left off at
                    format!("dirscan:{dirid}/{start_inode:020}")
                };
                let end_key = format!("dirscan:{dirid}0");

                let iter = self
                    .db
                    .scan(Bytes::from(start_key)..Bytes::from(end_key))
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?;
                pin_mut!(iter);

                let mut dir_entries = Vec::new();
                let mut resuming = start_after != 0;
                let mut has_more = false;

                while let Some(result) = iter.next().await {
                    // Check if we already have enough entries
                    if dir_entries.len() >= max_entries - entries.len() {
                        debug!("readdir: reached max_entries limit, found one more entry");
                        has_more = true;
                        break;
                    }

                    let (key, _value) = result.map_err(|_| nfsstat3::NFS3ERR_IO)?;
                    let key_str = String::from_utf8_lossy(&key);

                    // Extract the inode ID and filename from the key
                    if let Some(suffix) = key_str.strip_prefix(&scan_prefix) {
                        if let Some(slash_pos) = suffix.find('/') {
                            let inode_str = &suffix[..slash_pos];
                            let filename = &suffix[slash_pos + 1..];

                            if let Ok(inode_id) = inode_str.parse::<u64>() {
                                // If we're resuming, check if we need to skip entries
                                if resuming {
                                    if inode_id == start_inode {
                                        // Same inode - check position
                                        let pos = inode_positions.entry(inode_id).or_insert(0);
                                        if *pos <= start_position {
                                            *pos += 1;
                                            continue;
                                        }
                                    } else if inode_id < start_inode {
                                        // Earlier inode - skip
                                        continue;
                                    }
                                    resuming = false;
                                }

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

                // Add the loaded entries to results with encoded fileids
                for (inode_id, name, inode) in loaded_entries {
                    let position = inode_positions.entry(inode_id).or_insert(0);
                    let encoded_id = EncodedFileId::new(inode_id, *position).as_raw();
                    *position += 1;

                    entries.push(DirEntry {
                        fileid: encoded_id,
                        name: name.into(),
                        attr: inode.to_fattr3(inode_id),
                    });
                    debug!("readdir: added entry with encoded id {}", encoded_id);
                }

                // We're at the end if there are no more entries
                let end = !has_more;

                let result = ReadDirResult { end, entries };
                debug!(
                    "readdir: returning {} entries, end={}",
                    result.entries.len(),
                    result.end
                );

                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                Ok(result)
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }
}
