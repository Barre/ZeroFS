//! Namespace reads: lookup and readdir.

use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::tracing::FileOperation;
use crate::fs::types::{AuthContext, DirEntry, InodeWithId, ReadDirResult};
use ::tracing::{debug, error};
use futures::pin_mut;
use futures::stream::{self, StreamExt};
use std::sync::atomic::Ordering;

impl ZeroFS {
    /// Resolve `filename` in `dirid` to its inode id (requires execute on the
    /// directory).
    pub async fn lookup(
        &self,
        creds: &Credentials,
        dirid: InodeId,
        filename: &[u8],
    ) -> Result<InodeId, FsError> {
        debug!(
            "lookup: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let (dir_inode_result, entry_result) = tokio::join!(
            self.inode_store.get(dirid),
            self.directory_store.get(dirid, filename)
        );

        let dir_inode = dir_inode_result?;

        match dir_inode {
            Inode::Directory(_) => {
                check_access(&dir_inode, creds, AccessMode::Execute)?;

                match entry_result {
                    Ok(inode_id) => {
                        debug!(
                            "lookup found: {} -> inode {}",
                            String::from_utf8_lossy(filename),
                            inode_id
                        );

                        self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                        self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                        self.tracer.emit(
                            &self.inode_store,
                            inode_id,
                            FileOperation::Lookup {
                                filename: String::from_utf8_lossy(filename).to_string(),
                            },
                        );

                        Ok(inode_id)
                    }
                    Err(FsError::NotFound) => {
                        debug!(
                            "lookup not found: {} in directory",
                            String::from_utf8_lossy(filename)
                        );
                        Err(FsError::NotFound)
                    }
                    Err(e) => Err(e),
                }
            }
            _ => Err(FsError::NotDirectory),
        }
    }

    /// Page through `dirid` starting after cookie `start_after` (0 starts at
    /// `.` and `..`), up to `max_entries`. Attributes come from the entry's
    /// embedded inode when current, else a point lookup; entries whose inode
    /// vanished mid-scan are skipped.
    pub async fn readdir(
        &self,
        auth: &AuthContext,
        dirid: InodeId,
        start_after: InodeId,
        max_entries: usize,
    ) -> Result<ReadDirResult, FsError> {
        let dir_inode = self.inode_store.get(dirid).await?;

        let creds = Credentials::from_auth_context(auth);
        check_access(&dir_inode, &creds, AccessMode::Read)?;

        use crate::fs::store::directory::{COOKIE_DOT, COOKIE_DOTDOT};

        match &dir_inode {
            Inode::Directory(dir) => {
                let mut entries = Vec::new();

                // Handle . and .. based on start_after cookie
                if start_after < COOKIE_DOT {
                    entries.push(DirEntry {
                        fileid: dirid,
                        name: b".".to_vec(),
                        attr: InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into(),
                        cookie: COOKIE_DOT,
                    });
                }

                if start_after < COOKIE_DOTDOT {
                    let parent_id = if dirid == 0 { 0 } else { dir.parent };
                    let parent_attr = if parent_id == dirid {
                        InodeWithId {
                            inode: &dir_inode,
                            id: dirid,
                        }
                        .into()
                    } else {
                        let parent_inode = self.inode_store.get(parent_id).await?;
                        InodeWithId {
                            inode: &parent_inode,
                            id: parent_id,
                        }
                        .into()
                    };
                    entries.push(DirEntry {
                        fileid: parent_id,
                        name: b"..".to_vec(),
                        attr: parent_attr,
                        cookie: COOKIE_DOTDOT,
                    });
                }

                // Get regular entries, starting after the given cookie
                let iter = if start_after < COOKIE_DOTDOT {
                    self.directory_store.list(dirid).await?
                } else {
                    self.directory_store.list_from(dirid, start_after).await?
                };
                pin_mut!(iter);

                let mut dir_entries: Vec<(InodeId, Vec<u8>, u64, Option<Inode>)> = Vec::new();
                let mut has_more = false;

                while let Some(result) = iter.next().await {
                    if dir_entries.len() >= max_entries - entries.len() {
                        has_more = true;
                        break;
                    }
                    let entry = result?;
                    dir_entries.push((entry.inode_id, entry.name, entry.cookie, entry.inode));
                }

                let lookup_indices: Vec<usize> = dir_entries
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, _, _, inode))| inode.is_none())
                    .map(|(i, _)| i)
                    .collect();

                if !lookup_indices.is_empty() {
                    const BUFFER_SIZE: usize = 256;

                    let lookup_entries: Vec<_> = lookup_indices
                        .iter()
                        .map(|&i| (i, dir_entries[i].0))
                        .collect();

                    let inode_futures =
                        stream::iter(lookup_entries).map(|(idx, inode_id)| async move {
                            match self.inode_store.get(inode_id).await {
                                Ok(inode) => Ok::<_, FsError>((idx, Some(inode))),
                                Err(FsError::NotFound) => {
                                    debug!("readdir: skipping deleted entry (inode {})", inode_id);
                                    Ok((idx, None))
                                }
                                Err(e) => {
                                    error!("readdir: failed to load inode {}: {:?}", inode_id, e);
                                    Err(e)
                                }
                            }
                        });

                    let loaded_inodes: Vec<_> = inode_futures
                        .buffered(BUFFER_SIZE)
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;

                    for (idx, inode_opt) in loaded_inodes {
                        dir_entries[idx].3 = inode_opt;
                    }
                }

                for (inode_id, name, cookie, inode_opt) in dir_entries {
                    if let Some(inode) = inode_opt {
                        entries.push(DirEntry {
                            fileid: inode_id,
                            name,
                            attr: InodeWithId {
                                inode: &inode,
                                id: inode_id,
                            }
                            .into(),
                            cookie,
                        });
                    }
                }

                self.stats.read_operations.fetch_add(1, Ordering::Relaxed);
                self.stats.total_operations.fetch_add(1, Ordering::Relaxed);

                self.tracer.emit(
                    &self.inode_store,
                    dirid,
                    FileOperation::Readdir {
                        count: entries.len() as u32,
                    },
                );

                Ok(ReadDirResult {
                    entries,
                    end: !has_more,
                })
            }
            _ => Err(FsError::NotDirectory),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::fs::inode::Inode;
    use crate::fs::test_util::test_creds;
    use crate::fs::*;
    use crate::test_helpers::test_helpers_mod::test_auth;

    use crate::fs::types::{SetAttributes, SetMode};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_process_readdir() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        fs.create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        assert!(result.end);
        assert_eq!(result.entries.len(), 5);

        assert_eq!(result.entries[0].name, b".");
        assert_eq!(result.entries[1].name, b"..");

        let names: Vec<&[u8]> = result.entries[2..]
            .iter()
            .map(|e| e.name.as_ref())
            .collect();
        assert!(names.contains(&b"file1.txt".as_ref()));
        assert!(names.contains(&b"file2.txt".as_ref()));
        assert!(names.contains(&b"dir1".as_ref()));
    }

    #[tokio::test]
    async fn test_process_readdir_pagination() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        for i in 0..10 {
            fs.create(
                &test_creds(),
                0,
                format!("file{i}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let result1 = fs.readdir(&(&test_auth()).into(), 0, 0, 5).await.unwrap();
        assert!(!result1.end);
        assert_eq!(result1.entries.len(), 5);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .readdir(&(&test_auth()).into(), 0, last_id, 10)
            .await
            .unwrap();
        assert!(result2.end);
    }

    #[tokio::test]
    async fn test_readdir_with_hardlinks_stable_cookies() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create files with hardlinks
        let (file1_id, _) = fs
            .create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Create hardlinks
        fs.link(&(&test_auth()).into(), file1_id, 0, b"hardlink1.txt")
            .await
            .unwrap();
        fs.link(&(&test_auth()).into(), file1_id, 0, b"hardlink2.txt")
            .await
            .unwrap();

        // Create another file
        let (file2_id, _) = fs
            .create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();

        // First readdir - get all entries
        let result1 = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();
        assert_eq!(result1.entries.len(), 6); // . .. file1.txt hardlink1.txt hardlink2.txt file2.txt

        // With stable cookies, hardlinks have the SAME fileid (raw inode) but DIFFERENT cookies
        let file1_entry = &result1.entries[2];
        let hardlink1_entry = &result1.entries[3];
        let hardlink2_entry = &result1.entries[4];
        let file2_entry = &result1.entries[5];

        // All hardlinks point to the same inode
        assert_eq!(file1_entry.fileid, file1_id);
        assert_eq!(hardlink1_entry.fileid, file1_id);
        assert_eq!(hardlink2_entry.fileid, file1_id);
        assert_eq!(file2_entry.fileid, file2_id);

        // Cookies are unique and stable for pagination
        assert_ne!(file1_entry.cookie, hardlink1_entry.cookie);
        assert_ne!(hardlink1_entry.cookie, hardlink2_entry.cookie);
        assert_ne!(hardlink2_entry.cookie, file2_entry.cookie);

        // Test pagination using cookies - start after the first hardlink
        let result2 = fs
            .readdir(&(&test_auth()).into(), 0, hardlink1_entry.cookie, 10)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 2); // hardlink2.txt file2.txt
        assert_eq!(result2.entries[0].name, b"hardlink2.txt");
        assert_eq!(result2.entries[1].name, b"file2.txt");
    }

    #[tokio::test]
    async fn test_dir_scan_entry_embeds_inode_on_create() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        assert_eq!(entry.name, b"test.txt");
        assert_eq!(entry.inode_id, file_id);
        assert!(
            entry.inode.is_some(),
            "Newly created file should have embedded inode"
        );

        let embedded = entry.inode.unwrap();
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1);
                assert!(f.parent.is_some());
                assert!(f.name.is_some());
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_updates_on_write() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"hello world".to_vec()),
        )
        .await
        .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        let embedded = entry.inode.expect("Should have embedded inode");
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.size, 11, "Embedded inode should have updated size");
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_updates_on_setattr() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let attrs = SetAttributes {
            mode: SetMode::Set(0o755),
            ..Default::default()
        };
        fs.setattr(&test_creds(), file_id, &attrs).await.unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        let embedded = entry.inode.expect("Should have embedded inode");
        match embedded {
            Inode::File(f) => {
                assert_eq!(
                    f.mode & 0o777,
                    0o755,
                    "Embedded inode should have updated mode"
                );
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_dir_scan_entry_becomes_reference_on_hardlink() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();
        assert!(
            entry.inode.is_some(),
            "Before hardlink, should have embedded inode"
        );
        drop(entries);

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();

        let entry1 = entries.next().await.unwrap().unwrap();
        let entry2 = entries.next().await.unwrap().unwrap();

        let (original, hardlink) = if entry1.name == b"original.txt" {
            (entry1, entry2)
        } else {
            (entry2, entry1)
        };

        assert!(
            original.inode.is_none(),
            "Original entry should be Reference after hardlink"
        );
        assert!(
            hardlink.inode.is_none(),
            "Hardlink entry should be Reference"
        );
    }

    #[tokio::test]
    async fn test_dir_scan_entry_restored_on_rename_over_same_inode() {
        use futures::StreamExt;

        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        while let Some(entry) = entries.next().await {
            let entry = entry.unwrap();
            assert!(
                entry.inode.is_none(),
                "Both entries should be Reference with nlink=2"
            );
        }
        drop(entries);

        fs.rename(
            &(&test_auth()).into(),
            0,
            b"hardlink.txt",
            0,
            b"original.txt",
        )
        .await
        .unwrap();

        let mut entries = fs.directory_store.list(0).await.unwrap();
        let entry = entries.next().await.unwrap().unwrap();

        assert_eq!(entry.name, b"original.txt");
        assert!(
            entry.inode.is_some(),
            "After rename-over-same-inode (nlink=1), should have embedded inode"
        );

        // Verify nlink is 1
        let embedded = entry.inode.unwrap();
        match embedded {
            Inode::File(f) => {
                assert_eq!(f.nlink, 1);
                assert!(f.parent.is_some(), "parent should be restored");
                assert!(f.name.is_some(), "name should be restored");
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_readdir_uses_embedded_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        let file_entry = result
            .entries
            .iter()
            .find(|e| e.name == b"test.txt")
            .expect("Should find test.txt");

        assert_eq!(file_entry.fileid, file_id);
        assert_eq!(
            file_entry.attr.size, 12,
            "Should have correct size from embedded inode"
        );
    }

    #[tokio::test]
    async fn test_readdir_fetches_inode_for_hardlinks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &(&test_auth()).into(),
            file_id,
            0,
            &Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        fs.link(&(&test_auth()).into(), file_id, 0, b"hardlink.txt")
            .await
            .unwrap();

        let result = fs.readdir(&(&test_auth()).into(), 0, 0, 10).await.unwrap();

        let original = result
            .entries
            .iter()
            .find(|e| e.name == b"original.txt")
            .expect("Should find original.txt");

        let hardlink = result
            .entries
            .iter()
            .find(|e| e.name == b"hardlink.txt")
            .expect("Should find hardlink.txt");

        // Both should have the same inode id and attributes
        assert_eq!(original.fileid, file_id);
        assert_eq!(hardlink.fileid, file_id);
        assert_eq!(original.attr.size, 12);
        assert_eq!(hardlink.attr.size, 12);
        assert_eq!(original.attr.nlink, 2);
        assert_eq!(hardlink.attr.nlink, 2);
    }

    // ---- POSIX open-unlink (deferred reclaim) ----
}
