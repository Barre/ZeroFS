use crate::fs::ZeroFS;
use crate::fs::inode::Inode;
use crate::fs::permissions::Credentials;
use crate::fs::types::{FileType, InodeWithId, SetAttributes};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use zerofs_nfsserve::nfs::{ftype3, *};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{AuthContext as NfsAuthContext, NFSFileSystem, VFSCapabilities};

/// Adapter struct that implements the NFS trait for ZeroFS.
/// This prevents accidental direct calls to NFS trait methods on ZeroFS.
#[derive(Clone)]
pub struct NFSAdapter {
    fs: Arc<ZeroFS>,
}

impl NFSAdapter {
    pub fn new(fs: Arc<ZeroFS>) -> Self {
        Self { fs }
    }
}

#[async_trait]
impl NFSFileSystem for NFSAdapter {
    fn root_dir(&self) -> fileid3 {
        0
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    async fn lookup(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        debug!(
            "lookup called: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);

        let inode_id = self.fs.lookup(&creds, dirid, filename).await?;
        Ok(inode_id)
    }

    async fn getattr(&self, _auth: &NfsAuthContext, id: fileid3) -> Result<fattr3, nfsstat3> {
        debug!("getattr called: id={}", id);
        let inode = self.fs.inode_store.get(id).await?;
        Ok(InodeWithId { inode: &inode, id }.into())
    }

    async fn read(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        debug!("read called: id={}, offset={}, count={}", id, offset, count);
        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        self.fs
            .read_file(&auth_ctx, id, offset, count)
            .await
            .map(|(data, eof)| (data.to_vec(), eof))
            .map_err(|e| e.into())
    }

    async fn write(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        debug!(
            "Processing write of {} bytes to inode {} at offset {}",
            data.len(),
            id,
            offset
        );

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let data_bytes = bytes::Bytes::copy_from_slice(data);
        let file_attrs: crate::fs::types::FileAttributes =
            self.fs.write(&auth_ctx, id, offset, &data_bytes).await?;
        Ok((&file_attrs).into())
    }

    async fn create(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "create called: dirid={}, filename={}",
            dirid,
            String::from_utf8_lossy(filename)
        );

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = SetAttributes::from(attr);

        let (id, file_attrs): (u64, crate::fs::types::FileAttributes) =
            self.fs.create(&creds, dirid, filename, &fs_attr).await?;

        Ok((id, (&file_attrs).into()))
    }

    async fn create_exclusive(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        debug!(
            "create_exclusive called: dirid={}, filename={:?}",
            dirid, filename
        );

        let id = self
            .fs
            .create_exclusive(&auth.into(), dirid, filename)
            .await?;

        Ok(id)
    }

    async fn mkdir(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        dirname: &filename3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "mkdir called: dirid={}, dirname={}",
            dirid,
            String::from_utf8_lossy(dirname)
        );

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = SetAttributes::from(*attr);
        let (id, file_attrs): (u64, crate::fs::types::FileAttributes) =
            self.fs.mkdir(&creds, dirid, dirname, &fs_attr).await?;
        Ok((id, (&file_attrs).into()))
    }

    async fn remove(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!("remove called: dirid={}, filename={:?}", dirid, filename);

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        Ok(self.fs.remove(&auth_ctx, dirid, filename).await?)
    }

    async fn rename(
        &self,
        auth: &NfsAuthContext,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "rename called: from_dirid={}, to_dirid={}",
            from_dirid, to_dirid
        );

        self.fs
            .rename(
                &auth.into(),
                from_dirid,
                from_filename,
                to_dirid,
                to_filename,
            )
            .await
            .map_err(|e| e.into())
    }

    async fn readdir(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<zerofs_nfsserve::vfs::ReadDirResult, nfsstat3> {
        debug!(
            "readdir called: dirid={}, start_after={}, max_entries={}",
            dirid, start_after, max_entries
        );

        let result = self
            .fs
            .readdir(&auth.into(), dirid, start_after, max_entries)
            .await?;

        Ok(zerofs_nfsserve::vfs::ReadDirResult {
            entries: result
                .entries
                .into_iter()
                .map(|e| zerofs_nfsserve::vfs::DirEntry {
                    fileid: e.fileid,
                    name: e.name.into(),
                    attr: (&e.attr).into(),
                    cookie: e.cookie,
                })
                .collect(),
            end: result.end,
        })
    }

    async fn setattr(
        &self,
        auth: &NfsAuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        debug!("setattr called: id={}, setattr={:?}", id, setattr);

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = SetAttributes::from(setattr);
        let file_attrs = self.fs.setattr(&creds, id, &fs_attr).await?;
        Ok((&file_attrs).into())
    }

    async fn symlink(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "symlink called: dirid={}, linkname={:?}, target={:?}",
            dirid, linkname, symlink
        );

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = SetAttributes::from(*attr);
        let (id, file_attrs) = self
            .fs
            .symlink(&creds, dirid, &linkname.0, &symlink.0, &fs_attr)
            .await
            .map_err(|e: crate::fs::errors::FsError| -> nfsstat3 { e.into() })?;

        Ok((id, (&file_attrs).into()))
    }

    async fn readlink(&self, _auth: &NfsAuthContext, id: fileid3) -> Result<nfspath3, nfsstat3> {
        debug!("readlink called: id={}", id);

        let inode = self.fs.inode_store.get(id).await?;

        match inode {
            Inode::Symlink(symlink) => Ok(nfspath3 { 0: symlink.target }),
            _ => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn mknod(
        &self,
        auth: &NfsAuthContext,
        dirid: fileid3,
        filename: &filename3,
        ftype: ftype3,
        attr: &sattr3,
        spec: Option<&specdata3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        debug!(
            "mknod called: dirid={}, filename={:?}, ftype={:?}",
            dirid, filename, ftype
        );

        let rdev = match ftype {
            ftype3::NF3CHR | ftype3::NF3BLK => spec.map(|s| (s.specdata1, s.specdata2)),
            _ => None,
        };

        let auth_ctx: crate::fs::types::AuthContext = auth.into();
        let creds = Credentials::from_auth_context(&auth_ctx);
        let fs_attr = SetAttributes::from(*attr);
        let fs_type = FileType::from(ftype);
        let (id, file_attrs) = self
            .fs
            .mknod(&creds, dirid, &filename.0, fs_type, &fs_attr, rdev)
            .await?;

        Ok((id, (&file_attrs).into()))
    }

    async fn link(
        &self,
        auth: &NfsAuthContext,
        fileid: fileid3,
        linkdirid: fileid3,
        linkname: &filename3,
    ) -> Result<(), nfsstat3> {
        debug!(
            "link called: fileid={}, linkdirid={}, linkname={:?}",
            fileid, linkdirid, linkname
        );

        Ok(self
            .fs
            .link(&auth.into(), fileid, linkdirid, &linkname.0)
            .await?)
    }

    async fn commit(
        &self,
        _auth: &NfsAuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        tracing::debug!(
            "commit called: fileid={}, offset={}, count={}",
            fileid,
            offset,
            count
        );

        match self.fs.flush_coordinator.flush().await {
            Ok(_) => {
                debug!("commit successful for file {}", fileid);
                Ok(self.serverid())
            }
            Err(fs_error) => {
                let nfsstat: nfsstat3 = fs_error.into();
                tracing::error!("commit failed for file {}: {:?}", fileid, nfsstat);
                Err(nfsstat)
            }
        }
    }

    async fn fsstat(&self, auth: &NfsAuthContext, fileid: fileid3) -> Result<fsstat3, nfsstat3> {
        debug!("fsstat called: fileid={}", fileid);

        let obj_attr = match self.getattr(auth, fileid).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(e) => {
                debug!("fsstat: getattr failed for fileid {}: {:?}", fileid, e);
                post_op_attr::Void
            }
        };

        let (used_bytes, used_inodes) = self.fs.global_stats.get_totals();

        let next_inode_id = self.fs.inode_store.next_id();
        let available_inodes = u64::MAX.saturating_sub(next_inode_id);
        let total_inodes = used_inodes + available_inodes;

        // Use configured max_bytes from filesystem config, capped at 8 EiB
        // to avoid breaking NFS clients that can't handle larger values
        const MAX_NFS_BYTES: u64 = 8 * (1 << 60); // 8 EiB
        let total_bytes = self.fs.max_bytes.min(MAX_NFS_BYTES);
        let free_bytes = total_bytes.saturating_sub(used_bytes);

        let res = fsstat3 {
            obj_attributes: obj_attr,
            tbytes: total_bytes,
            fbytes: free_bytes,
            abytes: free_bytes,
            tfiles: total_inodes,
            ffiles: available_inodes,
            afiles: available_inodes,
            invarsec: 1,
        };

        Ok(res)
    }
}

pub async fn start_nfs_server_with_config(
    filesystem: Arc<ZeroFS>,
    socket: SocketAddr,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let adapter = NFSAdapter::new(filesystem);
    let listener = NFSTcpListener::bind(socket, adapter).await?;

    info!("NFS server listening on {}", socket);

    listener.handle_with_shutdown(shutdown).await?;
    Ok(())
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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let adapter = NFSAdapter::new(fs);

        assert_eq!(adapter.root_dir(), 0);
        assert!(matches!(adapter.capabilities(), VFSCapabilities::ReadWrite));
    }

    #[tokio::test]
    async fn test_lookup() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

        let fattr = fs.getattr(&test_auth(), 0).await.unwrap();

        assert!(matches!(fattr.ftype, ftype3::NF3DIR));
        assert_eq!(fattr.fileid, 0);
    }

    #[tokio::test]
    async fn test_read_write() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
            let mut last_cookie = 0u64;
            let mut iterations = 0;

            loop {
                let result = fs
                    .readdir(&test_auth(), 0, last_cookie, page_size)
                    .await
                    .unwrap();

                // Skip . and .. if we're at the beginning
                let start_idx = if last_cookie == 0 { 2 } else { 0 };

                for entry in &result.entries[start_idx..] {
                    all_entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    last_cookie = entry.cookie;
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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
        let mut last_cookie = 0u64;
        let page_size = 20;

        loop {
            let result = fs
                .readdir(&test_auth(), 0, last_cookie, page_size)
                .await
                .unwrap();

            let start_idx = if last_cookie == 0 { 2 } else { 0 };

            for entry in &result.entries[start_idx..] {
                let name = String::from_utf8_lossy(&entry.name).to_string();
                all_entries.push(name);

                // With stable cookies, fileid is the raw inode
                assert!(entry.fileid > 0);
                // cookie is used for pagination
                assert!(entry.cookie > 0);

                last_cookie = entry.cookie;
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
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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

        let last_cookie = result1.entries.last().unwrap().cookie;
        let result2 = fs
            .readdir(&test_auth(), exact_dir, last_cookie, 5)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 5);
        assert!(result2.end);

        // Test 4: Resume from non-existent cookie (should return no entries)
        let fake_cookie = 999999u64;
        let result = fs.readdir(&test_auth(), 0, fake_cookie, 10).await.unwrap();
        assert!(result.entries.is_empty() || result.end);
    }

    #[tokio::test]
    async fn test_concurrent_readdir_operations() {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let fs = NFSAdapter::new(fs);

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
            let mut last_cookie = 0u64;

            loop {
                let result = fs1.readdir(&test_auth(), 0, last_cookie, 5).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_cookie = entry.cookie;
                }
                if result.end {
                    break;
                }
            }
            entries
        });

        let handle2 = tokio::spawn(async move {
            let mut entries = Vec::new();
            let mut last_cookie = 0u64;

            loop {
                let result = fs2.readdir(&test_auth(), 0, last_cookie, 7).await.unwrap();
                for entry in &result.entries {
                    if entry.name.0 != b"." && entry.name.0 != b".." {
                        entries.push(String::from_utf8_lossy(&entry.name).to_string());
                    }
                    last_cookie = entry.cookie;
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
