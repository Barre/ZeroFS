use crate::cache::{CacheKey, CacheValue};
use crate::filesystem::{EncodedFileId, SlateDbFs};
use crate::inode::Inode;
use async_trait::async_trait;
use std::sync::atomic::Ordering;
use tracing::{debug, info};
use zerofs_nfsserve::nfs::{ftype3, *};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem, ReadDirResult, VFSCapabilities};

#[async_trait]
impl NFSFileSystem for SlateDbFs {
    fn root_dir(&self) -> fileid3 {
        0
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let encoded_dirid = EncodedFileId::from(dirid);
        let real_dirid = encoded_dirid.inode_id();
        let filename_str = String::from_utf8_lossy(filename);
        debug!(
            "lookup called: dirid={}, filename={}",
            real_dirid, filename_str
        );

        let dir_inode = self.load_inode(real_dirid).await?;

        match dir_inode {
            Inode::Directory(ref _dir) => {
                use crate::permissions::{AccessMode, Credentials, check_access};
                let creds = Credentials::from_auth_context(auth);
                check_access(&dir_inode, &creds, AccessMode::Execute)?;
                let name = filename_str.to_string();

                let cache_key = CacheKey::DirEntry {
                    dir_id: real_dirid,
                    name: name.clone(),
                };
                if let Some(CacheValue::DirEntry(inode_id)) =
                    self.dir_entry_cache.get(cache_key).await
                {
                    debug!("lookup cache hit: {} -> inode {}", name, inode_id);
                    return Ok(EncodedFileId::from_inode(inode_id).into());
                }

                let entry_key = SlateDbFs::dir_entry_key(real_dirid, &name);

                match self
                    .db
                    .get_bytes(&entry_key)
                    .await
                    .map_err(|_| nfsstat3::NFS3ERR_IO)?
                {
                    Some(entry_data) => {
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(&entry_data[..8]);
                        let inode_id = u64::from_le_bytes(bytes);
                        debug!("lookup found: {} -> inode {}", name, inode_id);

                        let cache_key = crate::cache::CacheKey::DirEntry {
                            dir_id: real_dirid,
                            name: name.clone(),
                        };
                        let cache_value = crate::cache::CacheValue::DirEntry(inode_id);
                        self.dir_entry_cache.insert(cache_key, cache_value, false);

                        Ok(EncodedFileId::from_inode(inode_id).into())
                    }
                    None => {
                        debug!("lookup not found: {} in directory", name);
                        Err(nfsstat3::NFS3ERR_NOENT)
                    }
                }
            }
            _ => Err(nfsstat3::NFS3ERR_NOTDIR),
        }
    }

    async fn getattr(&self, _auth: &AuthContext, id: fileid3) -> Result<fattr3, nfsstat3> {
        debug!("getattr called: id={}", id);
        let encoded_id = EncodedFileId::from(id);
        let real_id = encoded_id.inode_id();
        let inode = self.load_inode(real_id).await?;
        Ok(inode.to_fattr3(real_id))
    }

    async fn read(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        debug!("read called: id={}, offset={}, count={}", id, offset, count);
        let real_id = EncodedFileId::from(id).inode_id();
        self.process_read_file(auth, real_id, offset, count).await
    }

    async fn write(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        let real_id = EncodedFileId::from(id).inode_id();
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            real_id,
            offset
        );

        self.process_write(auth, real_id, offset, data).await
    }

    async fn create(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        let filename_str = String::from_utf8_lossy(filename);
        debug!(
            "create called: dirid={}, filename={}",
            real_dirid, filename_str
        );

        let (id, fattr) = self
            .process_create(auth, real_dirid, filename, attr)
            .await?;
        Ok((EncodedFileId::from_inode(id).into(), fattr))
    }

    async fn create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        debug!(
            "create_exclusive called: dirid={}, filename={:?}",
            real_dirid, filename
        );

        let id = self
            .process_create_exclusive(auth, real_dirid, filename)
            .await?;
        Ok(EncodedFileId::from_inode(id).into())
    }

    async fn mkdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        dirname: &filename3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        let dirname_str = String::from_utf8_lossy(dirname);
        debug!(
            "mkdir called: dirid={}, dirname={}",
            real_dirid, dirname_str
        );

        let (id, fattr) = self.process_mkdir(auth, real_dirid, dirname, attr).await?;
        Ok((EncodedFileId::from_inode(id).into(), fattr))
    }

    async fn remove(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        debug!(
            "remove called: dirid={}, filename={:?}",
            real_dirid, filename
        );

        self.process_remove(auth, real_dirid, filename).await
    }

    async fn rename(
        &self,
        auth: &AuthContext,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let real_from_dirid = EncodedFileId::from(from_dirid).inode_id();
        let real_to_dirid = EncodedFileId::from(to_dirid).inode_id();
        debug!(
            "rename called: from_dirid={}, to_dirid={}",
            real_from_dirid, real_to_dirid
        );

        self.process_rename(
            auth,
            real_from_dirid,
            from_filename,
            real_to_dirid,
            to_filename,
        )
        .await
    }

    async fn readdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        debug!(
            "readdir called: dirid={}, start_after={}, max_entries={}",
            real_dirid, start_after, max_entries
        );
        self.process_readdir(auth, real_dirid, start_after, max_entries)
            .await
    }

    async fn setattr(
        &self,
        auth: &AuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        let real_id = EncodedFileId::from(id).inode_id();
        debug!("setattr called: id={}, setattr={:?}", real_id, setattr);

        self.process_setattr(auth, real_id, setattr).await
    }

    async fn symlink(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        debug!(
            "symlink called: dirid={}, linkname={:?}, target={:?}",
            real_dirid, linkname, symlink
        );

        let (id, fattr) = self
            .process_symlink(auth, real_dirid, &linkname.0, &symlink.0, *attr)
            .await?;
        Ok((EncodedFileId::from_inode(id).into(), fattr))
    }

    async fn readlink(&self, _auth: &AuthContext, id: fileid3) -> Result<nfspath3, nfsstat3> {
        debug!("readlink called: id={}", id);
        let real_id = EncodedFileId::from(id).inode_id();

        let inode = self.load_inode(real_id).await?;
        match inode {
            Inode::Symlink(symlink) => Ok(nfspath3 { 0: symlink.target }),
            _ => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn mknod(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        ftype: ftype3,
        attr: &sattr3,
        spec: Option<&specdata3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let real_dirid = EncodedFileId::from(dirid).inode_id();
        debug!(
            "mknod called: dirid={}, filename={:?}, ftype={:?}",
            real_dirid, filename, ftype
        );

        // Extract device numbers if this is a device file
        let rdev = match ftype {
            ftype3::NF3CHR | ftype3::NF3BLK => {
                // Extract device numbers from specdata3
                spec.map(|s| (s.specdata1, s.specdata2))
            }
            _ => None,
        };

        let (id, fattr) = self
            .process_mknod(auth, real_dirid, &filename.0, ftype, attr, rdev)
            .await?;
        Ok((EncodedFileId::from_inode(id).into(), fattr))
    }

    async fn link(
        &self,
        auth: &AuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &filename3,
    ) -> Result<(), nfsstat3> {
        let real_fileid = EncodedFileId::from(fileid).inode_id();
        let real_linkdirid = EncodedFileId::from(linkdirid).inode_id();
        debug!(
            "link called: fileid={}, linkdirid={}, linkname={:?}",
            real_fileid, real_linkdirid, linkname
        );
        self.process_link(auth, real_fileid, real_linkdirid, &linkname.0)
            .await
    }

    async fn commit(
        &self,
        _auth: &AuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        let real_fileid = EncodedFileId::from(fileid).inode_id();
        tracing::debug!(
            "commit called: fileid={}, offset={}, count={}",
            real_fileid,
            offset,
            count
        );

        match self.flush().await {
            Ok(_) => {
                debug!("commit successful for file {}", real_fileid);
                Ok(self.serverid())
            }
            Err(nfsstat) => {
                tracing::error!("commit failed for file {}: {:?}", real_fileid, nfsstat);
                Err(nfsstat)
            }
        }
    }

    async fn fsstat(&self, auth: &AuthContext, fileid: fileid3) -> Result<fsstat3, nfsstat3> {
        let real_fileid = EncodedFileId::from(fileid).inode_id();
        debug!("fsstat called: fileid={}", real_fileid);

        let obj_attr = match self.getattr(auth, fileid).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(_) => post_op_attr::Void,
        };

        let (used_bytes, _used_inodes) = self.global_stats.get_totals();

        const TOTAL_BYTES: u64 = 8 << 60; // 8 EiB
        const TOTAL_INODES: u64 = 1 << 48; // ~281 trillion inodes

        // Get the next inode ID to determine how many IDs have been allocated
        let next_inode_id = self.next_inode_id.load(Ordering::Relaxed);

        // Available inodes = total possible inodes - allocated inode IDs
        // Note: We use next_inode_id because once allocated, inode IDs are never reused
        let available_inodes = TOTAL_INODES.saturating_sub(next_inode_id);

        let res = fsstat3 {
            obj_attributes: obj_attr,
            tbytes: TOTAL_BYTES,
            fbytes: TOTAL_BYTES.saturating_sub(used_bytes),
            abytes: TOTAL_BYTES.saturating_sub(used_bytes),
            tfiles: TOTAL_INODES,
            ffiles: available_inodes,
            afiles: available_inodes,
            invarsec: 1,
        };

        Ok(res)
    }
}

pub async fn start_nfs_server_with_config(
    filesystem: SlateDbFs,
    host: &str,
    port: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = NFSTcpListener::bind(&format!("{host}:{port}"), filesystem).await?;
    info!("NFS server listening on {}:{}", host, port);
    listener
        .handle_forever()
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::test_helpers_mod::{filename, test_auth};
    use zerofs_nfsserve::nfs::{
        ftype3, nfspath3, nfsstat3, sattr3, set_atime, set_gid3, set_mode3, set_mtime, set_size3,
        set_uid3,
    };

    #[tokio::test]
    async fn test_nfs_filesystem_trait() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        assert_eq!(fs.root_dir(), 0);
        assert!(matches!(fs.capabilities(), VFSCapabilities::ReadWrite));
    }

    #[tokio::test]
    async fn test_lookup() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"test.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"nonexistent.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_getattr() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let fattr = fs.getattr(&test_auth(), 0).await.unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));
        assert_eq!(fattr.fileid, 0);
    }

    #[tokio::test]
    async fn test_read_write() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let data = b"Hello, NFS!";
        let fattr = fs.write(&test_auth(), file_id, 0, data).await.unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .read(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();
        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_create_exclusive() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let file_id = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await
            .unwrap();
        assert!(file_id > 0);

        let result = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_EXIST)));
    }

    #[tokio::test]
    async fn test_mkdir_and_readdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .mkdir(&test_auth(), 0, &filename(b"mydir"), &sattr3::default())
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));

        let (_file_id, _) = fs
            .create(
                &test_auth(),
                dir_id,
                &filename(b"file_in_dir.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), dir_id, 0, 10).await.unwrap();
        assert!(result.end);

        let names: Vec<&[u8]> = result.entries.iter().map(|e| e.name.0.as_ref()).collect();
        assert!(names.contains(&b".".as_ref()));
        assert!(names.contains(&b"..".as_ref()));
        assert!(names.contains(&b"file_in_dir.txt".as_ref()));
    }

    #[tokio::test]
    async fn test_rename() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"original.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"test data")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            0,
            &filename(b"original.txt"),
            0,
            &filename(b"renamed.txt"),
        )
        .await
        .unwrap();

        let result = fs.lookup(&test_auth(), 0, &filename(b"original.txt")).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"renamed.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);
    }

    #[tokio::test]
    async fn test_remove() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"to_remove.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.remove(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await
            .unwrap();

        let result = fs
            .lookup(&test_auth(), 0, &filename(b"to_remove.txt"))
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));

        let result = fs.getattr(&test_auth(), file_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_setattr() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, initial_fattr) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        // Test changing mode (which any owner can do)
        let setattr_mode = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_mode)
            .await
            .unwrap();
        assert_eq!(fattr.mode, 0o755);

        // Test that uid/gid remain unchanged when not specified
        assert_eq!(fattr.uid, initial_fattr.uid);
        assert_eq!(fattr.gid, initial_fattr.gid);

        // Test changing size (truncate)
        let setattr_size = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(1024),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, setattr_size)
            .await
            .unwrap();
        assert_eq!(fattr.size, 1024);
    }

    #[tokio::test]
    async fn test_symlink_and_readlink() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let target = nfspath3 {
            0: b"/path/to/target".to_vec(),
        };
        let attr = sattr3::default();

        let (link_id, fattr) = fs
            .symlink(&test_auth(), 0, &filename(b"mylink"), &target, &attr)
            .await
            .unwrap();
        assert!(matches!(fattr.ftype, ftype3::NF3LNK));

        let read_target = fs.readlink(&test_auth(), link_id).await.unwrap();
        assert_eq!(read_target.0, target.0);
    }

    #[tokio::test]
    async fn test_complex_filesystem_operations() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (docs_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"documents"), &sattr3::default())
            .await
            .unwrap();
        let (images_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"images"), &sattr3::default())
            .await
            .unwrap();

        let (file1_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"readme.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file2_id, _) = fs
            .create(
                &test_auth(),
                docs_dir,
                &filename(b"notes.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();
        let (file3_id, _) = fs
            .create(
                &test_auth(),
                images_dir,
                &filename(b"photo.jpg"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file1_id, 0, b"This is the readme")
            .await
            .unwrap();
        fs.write(&test_auth(), file2_id, 0, b"These are my notes")
            .await
            .unwrap();
        fs.write(&test_auth(), file3_id, 0, b"JPEG data...")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            docs_dir,
            &filename(b"readme.txt"),
            images_dir,
            &filename(b"readme.txt"),
        )
        .await
        .unwrap();

        let docs_entries = fs.readdir(&test_auth(), docs_dir, 0, 10).await.unwrap();
        assert_eq!(docs_entries.entries.len(), 3);

        let images_entries = fs.readdir(&test_auth(), images_dir, 0, 10).await.unwrap();
        assert_eq!(images_entries.entries.len(), 4);

        let (data, _) = fs.read(&test_auth(), file1_id, 0, 100).await.unwrap();
        assert_eq!(data, b"This is the readme");
    }

    #[tokio::test]
    async fn test_large_directory_pagination() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a large number of files
        let num_files = 100;
        for i in 0..num_files {
            fs.create(
                &test_auth(),
                0,
                &filename(format!("file_{i:04}.txt").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Test pagination with different page sizes
        let page_sizes = vec![10, 25, 50];

        for page_size in page_sizes {
            let mut all_entries = Vec::new();
            let mut last_fileid = 0;
            let mut iterations = 0;

            loop {
                let result = fs
                    .readdir(&test_auth(), 0, last_fileid, page_size)
                    .await
                    .unwrap();

                // Skip . and .. if we're at the beginning
                let start_idx = if last_fileid == 0 { 2 } else { 0 };

                for entry in &result.entries[start_idx..] {
                    all_entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    last_fileid = entry.fileid;
                }

                iterations += 1;

                if result.end {
                    break;
                }

                // Safety check to prevent infinite loops
                assert!(
                    iterations < 50,
                    "Too many iterations for page size {page_size}"
                );
            }

            // Should have all files
            assert_eq!(
                all_entries.len(),
                num_files,
                "Wrong number of entries for page size {page_size}"
            );

            // Verify all files are present and in order
            all_entries.sort();
            for (i, entry) in all_entries.iter().enumerate().take(num_files) {
                assert_eq!(entry, &format!("file_{i:04}.txt"));
            }
        }
    }

    #[tokio::test]
    async fn test_pagination_with_many_hardlinks() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create original files
        let num_files = 5;
        let hardlinks_per_file = 20;

        let mut file_ids = Vec::new();
        for i in 0..num_files {
            let (file_id, _) = fs
                .create(
                    &test_auth(),
                    0,
                    &filename(format!("original_{i}.txt").as_bytes()),
                    sattr3::default(),
                )
                .await
                .unwrap();
            file_ids.push(file_id);
        }

        // Create many hardlinks for each file
        for (i, &file_id) in file_ids.iter().enumerate() {
            for j in 0..hardlinks_per_file {
                fs.link(
                    &test_auth(),
                    file_id,
                    0,
                    &filename(format!("link_{i}_{j:02}.txt").as_bytes()),
                )
                .await
                .unwrap();
            }
        }

        // Test pagination - should handle all entries correctly
        let mut all_entries = Vec::new();
        let mut last_fileid = 0;
        let page_size = 20;

        loop {
            let result = fs
                .readdir(&test_auth(), 0, last_fileid, page_size)
                .await
                .unwrap();

            let start_idx = if last_fileid == 0 { 2 } else { 0 };

            for entry in &result.entries[start_idx..] {
                let name = String::from_utf8_lossy(&entry.name).to_string();
                all_entries.push(name);

                // Verify encoded fileid can be decoded properly
                let encoded_id = EncodedFileId::from(entry.fileid);
                let (real_inode, position) = encoded_id.decode();
                assert!(real_inode > 0);
                assert!(position < 65535); // Should be within u16 range

                last_fileid = entry.fileid;
            }

            if result.end {
                break;
            }
        }

        // Should have all files: originals + all hardlinks
        let expected_count = num_files + (num_files * hardlinks_per_file);
        assert_eq!(all_entries.len(), expected_count);

        // Verify no duplicates
        all_entries.sort();
        for i in 1..all_entries.len() {
            assert_ne!(all_entries[i - 1], all_entries[i], "Found duplicate entry");
        }
    }

    #[tokio::test]
    async fn test_pagination_edge_cases() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Test 1: Empty directory (only . and ..)
        let (empty_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"empty"), &sattr3::default())
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), empty_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 2); // Only . and ..
        assert!(result.end);
        assert_eq!(result.entries[0].name.0, b".");
        assert_eq!(result.entries[1].name.0, b"..");

        // Test 2: Single entry directory
        let (single_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"single"), &sattr3::default())
            .await
            .unwrap();
        fs.create(
            &test_auth(),
            single_dir,
            &filename(b"file.txt"),
            sattr3::default(),
        )
        .await
        .unwrap();

        let result = fs.readdir(&test_auth(), single_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 3); // ., .., file.txt
        assert!(result.end);

        // Test 3: Pagination with exactly page_size entries
        let (exact_dir, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"exact"), &sattr3::default())
            .await
            .unwrap();

        // Create 8 files (so with . and .. we have 10 total)
        for i in 0..8 {
            fs.create(
                &test_auth(),
                exact_dir,
                &filename(format!("f{i}").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Read with page size 10 - should get all in one go
        let result = fs.readdir(&test_auth(), exact_dir, 0, 10).await.unwrap();
        assert_eq!(result.entries.len(), 10);
        assert!(result.end);

        // Read with page size 5 - should need exactly 2 reads
        let result1 = fs.readdir(&test_auth(), exact_dir, 0, 5).await.unwrap();
        assert_eq!(result1.entries.len(), 5);
        assert!(!result1.end);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .readdir(&test_auth(), exact_dir, last_id, 5)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 5);
        assert!(result2.end);

        // Test 4: Resume from non-existent cookie (should fail)
        let fake_cookie = EncodedFileId::new(999999, 0).as_raw();
        let _result = fs.readdir(&test_auth(), 0, fake_cookie, 10).await;
        // This should work but return no entries (or few entries if the inode exists)
        // The implementation continues scanning from the encoded position
    }

    #[tokio::test]
    async fn test_concurrent_readdir_operations() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create some files
        for i in 0..20 {
            fs.create(
                &test_auth(),
                0,
                &filename(format!("file_{i:02}.txt").as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        // Simulate multiple concurrent readdir operations
        let fs1 = fs.clone();
        let fs2 = fs.clone();

        let handle1 = tokio::spawn(async move {
            let mut entries = Vec::new();
            let mut last_id = 0;

            loop {
                let result = fs1.readdir(&test_auth(), 0, last_id, 5).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_id = entry.fileid;
                }
                if result.end {
                    break;
                }
            }
            entries
        });

        let handle2 = tokio::spawn(async move {
            let mut entries = Vec::new();
            let mut last_id = 0;

            loop {
                let result = fs2.readdir(&test_auth(), 0, last_id, 7).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_id = entry.fileid;
                }
                if result.end {
                    break;
                }
            }
            entries
        });

        let (entries1, entries2) = tokio::join!(handle1, handle2);
        let mut entries1 = entries1.unwrap();
        let mut entries2 = entries2.unwrap();

        // Both should have all 20 files
        assert_eq!(entries1.len(), 20);
        assert_eq!(entries2.len(), 20);

        // Sort and verify they're identical
        entries1.sort();
        entries2.sort();
        assert_eq!(entries1, entries2);
    }
}
