pub mod common;
pub mod file_ops;
pub mod dir_ops;
pub mod link_ops;
pub mod metadata_ops;
pub mod rename_ops;
pub mod remove_ops;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::SlateDbFs;
    use crate::test_helpers::test_helpers::test_auth;
    use nfsserve::nfs::{ftype3, set_atime, set_mtime, sattr3, set_mode3, set_uid3, set_gid3, set_size3, nfstime3, nfsstat3};
    use crate::inode::Inode;
    use crate::filesystem::CHUNK_SIZE;

    #[tokio::test]
    async fn test_process_create_file() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let attr = sattr3 {
            mode: set_mode3::mode(0o644),
            uid: set_uid3::uid(1000),
            gid: set_gid3::gid(1000),
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let (file_id, fattr) = fs
            .process_create(&test_auth(), 0, b"test.txt", attr)
            .await
            .unwrap();

        assert!(file_id > 0);
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.size, 0);

        // Check that the file was added to the directory
        let entry_key = SlateDbFs::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get(&entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_create_file_already_exists() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let attr = sattr3::default();

        let _ = fs
            .process_create(&test_auth(), 0, b"test.txt", attr)
            .await
            .unwrap();

        let result = fs.process_create(&test_auth(), 0, b"test.txt", attr).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_EXIST)));
    }

    #[tokio::test]
    async fn test_process_mkdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        assert!(dir_id > 0);
        assert_eq!(fattr.mode, 0o755);
        assert!(matches!(fattr.ftype, ftype3::NF3DIR));

        let new_dir_inode = fs.load_inode(dir_id).await.unwrap();
        match new_dir_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_mkdir_with_custom_attrs() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Test with custom mode
        let custom_attrs = sattr3 {
            mode: set_mode3::mode(0o700),
            uid: set_uid3::uid(1001),
            gid: set_gid3::gid(1001),
            size: set_size3::Void,
            atime: set_atime::SET_TO_CLIENT_TIME(nfstime3 {
                seconds: 1234567890,
                nseconds: 0,
            }),
            mtime: set_mtime::SET_TO_CLIENT_TIME(nfstime3 {
                seconds: 1234567890,
                nseconds: 0,
            }),
        };

        let (_dir_id, fattr) = fs
            .process_mkdir(&test_auth(), 0, b"customdir", &custom_attrs)
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

    #[tokio::test]
    async fn test_process_write_and_read() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .process_write(&test_auth(), file_id, 0, data)
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_write_partial_chunks() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.process_write(&test_auth(), file_id, 0, &data1)
            .await
            .unwrap();

        let data2 = vec![b'B'; 50];
        fs.process_write(&test_auth(), file_id, 50, &data2)
            .await
            .unwrap();

        let (read_data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_chunks() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"bigfile.txt", sattr3::default())
            .await
            .unwrap();

        let chunk_size = CHUNK_SIZE;
        let data = vec![b'X'; chunk_size * 2 + 1024];

        let fattr = fs
            .process_write(&test_auth(), file_id, 0, &data)
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&test_auth(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_remove_file() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, b"some data")
            .await
            .unwrap();

        fs.process_remove(&test_auth(), 0, b"test.txt")
            .await
            .unwrap();

        // Check that the file was removed from the directory
        let entry_key = SlateDbFs::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get(&entry_key).await.unwrap();
        assert!(entry_data.is_none());

        let result = fs.load_inode(file_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_remove_empty_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        fs.process_remove(&test_auth(), 0, b"testdir")
            .await
            .unwrap();

        let result = fs.load_inode(dir_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_remove_non_empty_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();

        fs.process_create(&test_auth(), dir_id, b"file.txt", sattr3::default())
            .await
            .unwrap();

        let result = fs.process_remove(&test_auth(), 0, b"testdir").await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOTEMPTY)));
    }

    #[tokio::test]
    async fn test_process_rename_same_directory() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"old.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_rename(&test_auth(), 0, b"old.txt", 0, b"new.txt")
            .await
            .unwrap();

        // Check old entry is gone and new entry exists
        let old_entry_key = SlateDbFs::dir_entry_key(0, "old.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        let new_entry_key = SlateDbFs::dir_entry_key(0, "new.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_rename_replace_existing() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create two files
        let (file1_id, _) = fs
            .process_create(&test_auth(), 0, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_write(&test_auth(), file1_id, 0, b"content1")
            .await
            .unwrap();

        let (file2_id, _) = fs
            .process_create(&test_auth(), 0, b"file2.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_write(&test_auth(), file2_id, 0, b"content2")
            .await
            .unwrap();

        fs.process_rename(&test_auth(), 0, b"file1.txt", 0, b"file2.txt")
            .await
            .unwrap();

        // Check that file1.txt no longer exists
        let old_entry_key = SlateDbFs::dir_entry_key(0, "file1.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        // Check that file2.txt exists and has file1's content
        let new_entry_key = SlateDbFs::dir_entry_key(0, "file2.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file1_id);

        // Verify content
        let (read_data, _) = fs
            .process_read_file(&test_auth(), file1_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data, b"content1");

        // Check that the original file2 inode is gone
        let result = fs.load_inode(file2_id).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_NOENT)));
    }

    #[tokio::test]
    async fn test_process_rename_across_directories() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir1_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"dir1", &sattr3::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"dir2", &sattr3::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), dir1_id, b"file.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_rename(&test_auth(), dir1_id, b"file.txt", dir2_id, b"moved.txt")
            .await
            .unwrap();

        // Check file removed from dir1
        let old_entry_key = SlateDbFs::dir_entry_key(dir1_id, "file.txt");
        assert!(fs.db.get(&old_entry_key).await.unwrap().is_none());

        // Check file added to dir2
        let new_entry_key = SlateDbFs::dir_entry_key(dir2_id, "moved.txt");
        let entry_data = fs.db.get(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);

        // Check entry counts
        let dir1_inode = fs.load_inode(dir1_id).await.unwrap();
        match dir1_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 0);
            }
            _ => panic!("Should be a directory"),
        }

        let dir2_inode = fs.load_inode(dir2_id).await.unwrap();
        match dir2_inode {
            Inode::Directory(dir) => {
                assert_eq!(dir.entry_count, 1);
            }
            _ => panic!("Should be a directory"),
        }
    }

    #[tokio::test]
    async fn test_process_rename_directory_entry_count() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create a directory with two files
        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"testdir", &sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), dir_id, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), dir_id, b"file2.txt", sattr3::default())
            .await
            .unwrap();

        // Check initial entry count
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 2),
            _ => panic!("Should be a directory"),
        }

        fs.process_rename(&test_auth(), dir_id, b"file1.txt", dir_id, b"file2.txt")
            .await
            .unwrap();

        // Check that entry count decreased by 1
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 1),
            _ => panic!("Should be a directory"),
        }

        fs.process_remove(&test_auth(), dir_id, b"file2.txt")
            .await
            .unwrap();

        // Directory should now be empty and removable
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 0),
            _ => panic!("Should be a directory"),
        }

        // Should be able to remove the empty directory
        fs.process_remove(&test_auth(), 0, b"testdir")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_setattr_file_size() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), 0, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, &vec![b'A'; 1000])
            .await
            .unwrap();

        let setattr = sattr3 {
            size: set_size3::size(500),
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs
            .process_setattr(&test_auth(), file_id, setattr)
            .await
            .unwrap();
        assert_eq!(fattr.size, 500);

        let (read_data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(read_data.len(), 500);
    }

    #[tokio::test]
    async fn test_process_symlink() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let target = b"/path/to/target";
        let attr = sattr3::default();

        let (link_id, fattr) = fs
            .process_symlink(&test_auth(), 0, b"link", target, attr)
            .await
            .unwrap();

        assert!(link_id > 0);
        assert!(matches!(fattr.ftype, ftype3::NF3LNK));
        assert_eq!(fattr.size, target.len() as u64);

        let link_inode = fs.load_inode(link_id).await.unwrap();
        match link_inode {
            Inode::Symlink(symlink) => {
                assert_eq!(symlink.target, target);
            }
            _ => panic!("Should be a symlink"),
        }
    }

    #[tokio::test]
    async fn test_process_readdir() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        fs.process_create(&test_auth(), 0, b"file1.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_create(&test_auth(), 0, b"file2.txt", sattr3::default())
            .await
            .unwrap();
        fs.process_mkdir(&test_auth(), 0, b"dir1", &sattr3::default())
            .await
            .unwrap();

        let result = fs.process_readdir(&test_auth(), 0, 0, 10).await.unwrap();

        assert!(result.end);
        assert_eq!(result.entries.len(), 5);

        assert_eq!(result.entries[0].name.0, b".");
        assert_eq!(result.entries[1].name.0, b"..");

        let names: Vec<&[u8]> = result.entries[2..]
            .iter()
            .map(|e| e.name.0.as_ref())
            .collect();
        assert!(names.contains(&b"file1.txt".as_ref()));
        assert!(names.contains(&b"file2.txt".as_ref()));
        assert!(names.contains(&b"dir1".as_ref()));
    }

    #[tokio::test]
    async fn test_process_readdir_pagination() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        for i in 0..10 {
            fs.process_create(
                &test_auth(),
                0,
                format!("file{}.txt", i).as_bytes(),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        let result1 = fs.process_readdir(&test_auth(), 0, 0, 5).await.unwrap();
        assert!(!result1.end);
        assert_eq!(result1.entries.len(), 5);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .process_readdir(&test_auth(), 0, last_id, 10)
            .await
            .unwrap();
        assert!(result2.end);
    }

    #[tokio::test]
    async fn test_process_rename_prevent_directory_cycles() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c
        let (a_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"a", &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_auth(), a_id, b"b", &sattr3::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_auth(), b_id, b"c", &sattr3::default())
            .await
            .unwrap();

        // Test 1: Try to rename /a into /a/b (direct descendant)
        let result = fs
            .process_rename(&test_auth(), 0, b"a", b_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 2: Try to rename /a into /a/b/c (deeper descendant)
        let result = fs
            .process_rename(&test_auth(), 0, b"a", c_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 3: Try to rename /a/b into /a/b/c (moving into immediate child)
        let result = fs
            .process_rename(&test_auth(), a_id, b"b", c_id, b"b_moved")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_INVAL)));

        // Test 4: Valid rename - moving /a/b/c to root
        let result = fs
            .process_rename(&test_auth(), b_id, b"c", 0, b"c_moved")
            .await;
        assert!(result.is_ok());

        // Test 5: Valid rename - moving a file (not a directory) should work
        let (_file_id, _) = fs
            .process_create(&test_auth(), a_id, b"file.txt", sattr3::default())
            .await
            .unwrap();
        let result = fs
            .process_rename(&test_auth(), a_id, b"file.txt", b_id, b"file_moved.txt")
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_ancestor_of() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c/d
        let (a_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"a", &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_auth(), a_id, b"b", &sattr3::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_auth(), b_id, b"c", &sattr3::default())
            .await
            .unwrap();
        let (d_id, _) = fs
            .process_mkdir(&test_auth(), c_id, b"d", &sattr3::default())
            .await
            .unwrap();

        // Test ancestry relationships
        assert!(fs.is_ancestor_of(a_id, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(a_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, d_id).await.unwrap());
        assert!(fs.is_ancestor_of(c_id, d_id).await.unwrap());

        // Test non-ancestry relationships
        assert!(!fs.is_ancestor_of(b_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, a_id).await.unwrap());
        assert!(!fs.is_ancestor_of(c_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, b_id).await.unwrap());
        assert!(!fs.is_ancestor_of(d_id, c_id).await.unwrap());

        // Test root relationships
        assert!(fs.is_ancestor_of(0, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, b_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, c_id).await.unwrap());
        assert!(fs.is_ancestor_of(0, d_id).await.unwrap());
        assert!(!fs.is_ancestor_of(a_id, 0).await.unwrap());

        // Test self-relationships (should return true)
        assert!(fs.is_ancestor_of(a_id, a_id).await.unwrap());
        assert!(fs.is_ancestor_of(b_id, b_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_parent_directory_execute_permissions() {
        let fs = SlateDbFs::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_auth(), 0, b"test_dir", &sattr3::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&test_auth(), dir_id, b"test.txt", sattr3::default())
            .await
            .unwrap();

        fs.process_write(&test_auth(), file_id, 0, b"initial data")
            .await
            .unwrap();

        let no_exec_attrs = sattr3 {
            mode: set_mode3::mode(0o644),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.process_setattr(&test_auth(), dir_id, no_exec_attrs)
            .await
            .unwrap();

        let chmod_attrs = sattr3 {
            mode: set_mode3::mode(0o600),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let result = fs.process_setattr(&test_auth(), file_id, chmod_attrs).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let result = fs.process_read_file(&test_auth(), file_id, 0, 100).await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let result = fs
            .process_write(&test_auth(), file_id, 0, b"new data")
            .await;
        assert!(matches!(result, Err(nfsstat3::NFS3ERR_ACCES)));

        let exec_attrs = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        fs.process_setattr(&test_auth(), dir_id, exec_attrs)
            .await
            .unwrap();

        fs.process_setattr(&test_auth(), file_id, chmod_attrs)
            .await
            .unwrap();

        let (data, _) = fs
            .process_read_file(&test_auth(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(data, b"initial data");

        fs.process_write(&test_auth(), file_id, 0, b"updated data")
            .await
            .unwrap();
    }
}