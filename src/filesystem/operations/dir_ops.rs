use super::common::validate_filename;
use crate::filesystem::cache::CacheKey;
use crate::filesystem::errors::FsError;
use crate::filesystem::inode::{DirectoryInode, Inode};
use crate::filesystem::permissions::{AccessMode, Credentials, check_access};
use crate::filesystem::types::{
    AuthContext, DirEntry, FileAttributes, InodeId, InodeWithId, ReadDirResult, SetAttributes,
    SetGid, SetMode, SetTime, SetUid,
};
use crate::filesystem::{EncodedFileId, PREFIX_DIR_SCAN, ParsedKey, ZeroFS, get_current_time};
use bytes::Bytes;
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use slatedb::config::WriteOptions;
use std::sync::atomic::Ordering;
use tracing::debug;

impl ZeroFS {
    pub async fn process_mkdir(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        dirname: &[u8],
        attr: &SetAttributes,
    ) -> Result<(InodeId, FileAttributes), FsError> {
        validate_filename(dirname)?;

        let dirname_str = String::from_utf8_lossy(dirname);
        debug!("process_mkdir: dirid={}, dirname={}", dirid, dirname_str);

        let _guard = self.lock_manager.acquire_write(dirid).await;
        let mut dir_inode = self.load_inode(dirid).await?;

        check_access(&dir_inode, creds, AccessMode::Write)?;
        check_access(&dir_inode, creds, AccessMode::Execute)?;

        match &mut dir_inode {
            Inode::Directory(dir) => {
                let name = dirname_str.to_string();

                let entry_key = Self::dir_entry_key(dirid, &name);
                if self
                    .db
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| FsError::IoError)?
                    .is_some()
                {
                    return Err(FsError::Exists);
                }

                let new_dir_id = self.allocate_inode().await?;

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
                    nlink: 2,
                };

                let mut batch = self.db.new_write_batch();

                let new_dir_key = Self::inode_key(new_dir_id);
                let new_dir_data = bincode::serialize(&Inode::Directory(new_dir_inode.clone()))?;
                batch.put_bytes(&new_dir_key, &new_dir_data);

                batch.put_bytes(&entry_key, &new_dir_id.to_le_bytes());

                let scan_key = Self::dir_scan_key(dirid, new_dir_id, &name);
                batch.put_bytes(&scan_key, &new_dir_id.to_le_bytes());

                dir.entry_count += 1;
                if dir.nlink == u32::MAX {
                    return Err(FsError::NoSpace);
                }
                dir.nlink += 1;
                dir.mtime = now_sec;
                dir.mtime_nsec = now_nsec;
                dir.ctime = now_sec;
                dir.ctime_nsec = now_nsec;

                let counter_key = Self::counter_key();
                let next_id = self.next_inode_id.load(Ordering::SeqCst);
                batch.put_bytes(&counter_key, &next_id.to_le_bytes());

                let parent_dir_key = Self::inode_key(dirid);
                let parent_dir_data = bincode::serialize(&dir_inode)?;
                batch.put_bytes(&parent_dir_key, &parent_dir_data);

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
                    .map_err(|_| FsError::IoError)?;

                self.global_stats.commit_update(&stats_update);

                self.cache.remove(CacheKey::Metadata(dirid)).await;

                self.stats
                    .directories_created
                    .fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                let new_inode = Inode::Directory(new_dir_inode);
                let attrs = InodeWithId {
                    inode: &new_inode,
                    id: new_dir_id,
                }
                .into();
                Ok((new_dir_id, attrs))
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    pub async fn process_readdir(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        start_after: InodeId,
        max_entries: usize,
    ) -> Result<ReadDirResult, FsError> {
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
                        name: b".".to_vec(),
                        attr: InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into(),
                    });

                    debug!("readdir: adding .. entry for parent directory");
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into()
                    } else {
                        match self.load_inode(parent_id).await {
                            Ok(parent_inode) => InodeWithId {
                                inode: &parent_inode,
                                id: parent_id,
                            }
                            .into(),
                            Err(_) => InodeWithId {
                                inode: &dir_inode,
                                id: dirid,
                            }
                            .into(),
                        }
                    };
                    entries.push(DirEntry {
                        fileid: parent_id,
                        name: b"..".to_vec(),
                        attr: parent_attr,
                    });
                }

                let scan_prefix = Self::dir_scan_prefix(dirid);
                let start_key = if start_after == 0 {
                    Bytes::from(scan_prefix.clone())
                } else {
                    let mut key = scan_prefix.clone();
                    key.extend_from_slice(&start_inode.to_be_bytes());
                    Bytes::from(key)
                };

                let mut end_key = Vec::with_capacity(9);
                end_key.push(PREFIX_DIR_SCAN);
                end_key.extend_from_slice(&(dirid + 1).to_be_bytes());

                let iter = self
                    .db
                    .scan(start_key..Bytes::from(end_key))
                    .await
                    .map_err(|_| FsError::IoError)?;
                pin_mut!(iter);

                let mut dir_entries = Vec::new();
                let mut resuming = start_after != 0;
                let mut has_more = false;

                while let Some(result) = iter.next().await {
                    if dir_entries.len() >= max_entries - entries.len() {
                        debug!("readdir: reached max_entries limit, found one more entry");
                        has_more = true;
                        break;
                    }

                    let (key, _value) = result.map_err(|_| FsError::IoError)?;

                    let (inode_id, filename) = match ParsedKey::parse(&key) {
                        Some(ParsedKey::DirScan { entry_id, name }) => (entry_id, name),
                        _ => continue,
                    };

                    if resuming {
                        if inode_id == start_inode {
                            let pos = inode_positions.entry(inode_id).or_insert(0);
                            if *pos <= start_position {
                                *pos += 1;
                                continue;
                            }
                        } else if inode_id < start_inode {
                            continue;
                        }
                        resuming = false;
                    }

                    debug!("readdir: found entry {} (inode {})", filename, inode_id);
                    dir_entries.push((inode_id, filename.as_bytes().to_vec()));
                }

                const BUFFER_SIZE: usize = 16;
                let inode_futures =
                    stream::iter(dir_entries.into_iter()).map(|(inode_id, name)| async move {
                        debug!("readdir: loading inode {} for entry", inode_id);
                        let inode = self.load_inode(inode_id).await?;
                        debug!("readdir: loaded inode {} successfully", inode_id);
                        Ok::<(u64, Vec<u8>, Inode), FsError>((inode_id, name, inode))
                    });

                let loaded_entries: Vec<_> = inode_futures
                    .buffered(BUFFER_SIZE)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;

                for (inode_id, name, inode) in loaded_entries {
                    let position = inode_positions.entry(inode_id).or_insert(0);
                    let encoded_id = EncodedFileId::new(inode_id, *position).as_raw();
                    *position += 1;

                    entries.push(DirEntry {
                        fileid: encoded_id,
                        name: name,
                        attr: InodeWithId {
                            inode: &inode,
                            id: inode_id,
                        }
                        .into(),
                    });
                    debug!("readdir: added entry with encoded id {}", encoded_id);
                }

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
            _ => Err(FsError::NotDirectory),
        }
    }
}
