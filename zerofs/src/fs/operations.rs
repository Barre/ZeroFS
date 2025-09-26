pub mod common;
pub mod dir_ops;
pub mod file_ops;
pub mod link_ops;
pub mod metadata_ops;
pub mod remove_ops;
pub mod rename_ops;

#[cfg(test)]
mod tests {
    use crate::fs::ZeroFS;
    use crate::fs::errors::FsError;
    use crate::fs::inode::Inode;
    use crate::fs::key_codec::KeyCodec;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{FileType, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid};
    use crate::fs::{CHUNK_SIZE, EncodedFileId};
    use crate::test_helpers::test_helpers_mod::test_auth;

    fn test_creds() -> Credentials {
        Credentials::from_auth_context(&(&test_auth()).into())
    }

    #[tokio::test]
    async fn test_process_create_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = SetAttributes {
            mode: SetMode::Set(0o644),
            uid: SetUid::Set(1000),
            gid: SetGid::Set(1000),
            ..Default::default()
        };

        let (file_id, fattr) = fs
            .process_create(&test_creds(), 0, b"test.txt", &attr)
            .await
            .unwrap();

        assert!(file_id > 0);
        assert_eq!(fattr.mode, 0o644);
        assert_eq!(fattr.uid, 1000);
        assert_eq!(fattr.gid, 1000);
        assert_eq!(fattr.size, 0);

        // Check that the file was added to the directory
        let entry_key = KeyCodec::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_create_file_already_exists() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let attr = &SetAttributes::default();

        let _ = fs
            .process_create(&test_creds(), 0, b"test.txt", attr)
            .await
            .unwrap();

        let result = fs.process_create(&test_creds(), 0, b"test.txt", attr).await;
        assert!(matches!(result, Err(FsError::Exists)));
    }

    #[tokio::test]
    async fn test_process_mkdir() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, fattr) = fs
            .process_mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        assert!(dir_id > 0);
        assert_eq!(fattr.mode, 0o777);
        assert_eq!(fattr.file_type, FileType::Directory);

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
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Test with custom mode
        let custom_attrs = SetAttributes {
            mode: SetMode::Set(0o700),
            uid: SetUid::Set(1001),
            gid: SetGid::Set(1001),
            size: SetSize::NoChange,
            atime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
            mtime: SetTime::SetToClientTime(crate::fs::types::Timestamp {
                seconds: 1234567890,
                nanoseconds: 0,
            }),
        };

        let (_dir_id, fattr) = fs
            .process_mkdir(&test_creds(), 0, b"customdir", &custom_attrs)
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
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = b"Hello, World!";
        let fattr = fs
            .process_write(&(&test_auth()).into(), file_id, 0, data)
            .await
            .unwrap();

        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_write_partial_chunks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data1 = vec![b'A'; 100];
        fs.process_write(&(&test_auth()).into(), file_id, 0, &data1)
            .await
            .unwrap();

        let data2 = vec![b'B'; 50];
        fs.process_write(&(&test_auth()).into(), file_id, 50, &data2)
            .await
            .unwrap();

        let (read_data, _) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 100);
        assert_eq!(&read_data[0..50], &vec![b'A'; 50]);
        assert_eq!(&read_data[50..100], &vec![b'B'; 50]);
    }

    #[tokio::test]
    async fn test_process_write_across_chunks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"bigfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let chunk_size = CHUNK_SIZE;
        let data = vec![b'X'; chunk_size * 2 + 1024];

        let fattr = fs
            .process_write(&(&test_auth()).into(), file_id, 0, &data)
            .await
            .unwrap();
        assert_eq!(fattr.size, data.len() as u64);

        let (read_data, eof) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 0, data.len() as u32)
            .await
            .unwrap();

        assert_eq!(read_data, data);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_process_remove_file() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(&(&test_auth()).into(), file_id, 0, b"some data")
            .await
            .unwrap();

        fs.process_remove(&(&test_auth()).into(), 0, b"test.txt")
            .await
            .unwrap();

        // Check that the file was removed from the directory
        let entry_key = KeyCodec::dir_entry_key(0, "test.txt");
        let entry_data = fs.db.get_bytes(&entry_key).await.unwrap();
        assert!(entry_data.is_none());

        let result = fs.load_inode(file_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();

        let result = fs.load_inode(dir_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_remove_non_empty_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_create(
            &test_creds(),
            dir_id,
            b"file.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        let result = fs
            .process_remove(&(&test_auth()).into(), 0, b"testdir")
            .await;
        assert!(matches!(result, Err(FsError::NotEmpty)));
    }

    #[tokio::test]
    async fn test_process_rename_same_directory() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"old.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_rename(&(&test_auth()).into(), 0, b"old.txt", 0, b"new.txt")
            .await
            .unwrap();

        // Check old entry is gone and new entry exists
        let old_entry_key = KeyCodec::dir_entry_key(0, "old.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        let new_entry_key = KeyCodec::dir_entry_key(0, "new.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file_id);
    }

    #[tokio::test]
    async fn test_process_rename_replace_existing() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create two files
        let (file1_id, _) = fs
            .process_create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_write(&(&test_auth()).into(), file1_id, 0, b"content1")
            .await
            .unwrap();

        let (file2_id, _) = fs
            .process_create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_write(&(&test_auth()).into(), file2_id, 0, b"content2")
            .await
            .unwrap();

        fs.process_rename(&(&test_auth()).into(), 0, b"file1.txt", 0, b"file2.txt")
            .await
            .unwrap();

        // Check that file1.txt no longer exists
        let old_entry_key = KeyCodec::dir_entry_key(0, "file1.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check that file2.txt exists and has file1's content
        let new_entry_key = KeyCodec::dir_entry_key(0, "file2.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&entry_data[..8]);
        let stored_id = u64::from_le_bytes(bytes);
        assert_eq!(stored_id, file1_id);

        // Verify content
        let (read_data, _) = fs
            .process_read_file(&(&test_auth()).into(), file1_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(read_data, b"content1");

        // Check that the original file2 inode is gone
        let result = fs.load_inode(file2_id).await;
        assert!(matches!(result, Err(FsError::NotFound)));
    }

    #[tokio::test]
    async fn test_process_rename_across_directories() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir1_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(
                &test_creds(),
                dir1_id,
                b"file.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        fs.process_rename(
            &(&test_auth()).into(),
            dir1_id,
            b"file.txt",
            dir2_id,
            b"moved.txt",
        )
        .await
        .unwrap();

        // Check file removed from dir1
        let old_entry_key = KeyCodec::dir_entry_key(dir1_id, "file.txt");
        assert!(fs.db.get_bytes(&old_entry_key).await.unwrap().is_none());

        // Check file added to dir2
        let new_entry_key = KeyCodec::dir_entry_key(dir2_id, "moved.txt");
        let entry_data = fs.db.get_bytes(&new_entry_key).await.unwrap().unwrap();
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
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a directory with two files
        let (dir_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_create(
            &test_creds(),
            dir_id,
            b"file1.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();
        fs.process_create(
            &test_creds(),
            dir_id,
            b"file2.txt",
            &SetAttributes::default(),
        )
        .await
        .unwrap();

        // Check initial entry count
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 2),
            _ => panic!("Should be a directory"),
        }

        fs.process_rename(
            &(&test_auth()).into(),
            dir_id,
            b"file1.txt",
            dir_id,
            b"file2.txt",
        )
        .await
        .unwrap();

        // Check that entry count decreased by 1
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 1),
            _ => panic!("Should be a directory"),
        }

        fs.process_remove(&(&test_auth()).into(), dir_id, b"file2.txt")
            .await
            .unwrap();

        // Directory should now be empty and removable
        let dir_inode = fs.load_inode(dir_id).await.unwrap();
        match &dir_inode {
            Inode::Directory(dir) => assert_eq!(dir.entry_count, 0),
            _ => panic!("Should be a directory"),
        }

        // Should be able to remove the empty directory
        fs.process_remove(&(&test_auth()).into(), 0, b"testdir")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_setattr_file_size() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(&(&test_auth()).into(), file_id, 0, &vec![b'A'; 1000])
            .await
            .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(500),
            ..Default::default()
        };

        let fattr = fs
            .process_setattr(&test_creds(), file_id, &setattr)
            .await
            .unwrap();
        assert_eq!(fattr.size, 500);

        let (read_data, _) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 0, 1000)
            .await
            .unwrap();
        assert_eq!(read_data.len(), 500);
    }

    #[tokio::test]
    async fn test_read_beyond_truncated_chunk() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = vec![b'A'; 300 * 1024];
        fs.process_write(&(&test_auth()).into(), file_id, 0, &data)
            .await
            .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(100 * 1024),
            ..Default::default()
        };
        fs.process_setattr(&test_creds(), file_id, &setattr)
            .await
            .unwrap();

        let (read_data, _) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 200 * 1024, 100)
            .await
            .unwrap();

        assert_eq!(read_data.len(), 0);
    }

    #[tokio::test]
    async fn test_process_symlink() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let target = b"/path/to/target";
        let attr = &SetAttributes::default();

        let (link_id, fattr) = fs
            .process_symlink(&test_creds(), 0, b"link", target, attr)
            .await
            .unwrap();

        assert!(link_id > 0);
        assert_eq!(fattr.file_type, FileType::Symlink);
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
        let fs = ZeroFS::new_in_memory().await.unwrap();

        fs.process_create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_mkdir(&test_creds(), 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs
            .process_readdir(&(&test_auth()).into(), 0, 0, 10)
            .await
            .unwrap();

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
            fs.process_create(
                &test_creds(),
                0,
                format!("file{i}.txt").as_bytes(),
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        }

        let result1 = fs
            .process_readdir(&(&test_auth()).into(), 0, 0, 5)
            .await
            .unwrap();
        assert!(!result1.end);
        assert_eq!(result1.entries.len(), 5);

        let last_id = result1.entries.last().unwrap().fileid;
        let result2 = fs
            .process_readdir(&(&test_auth()).into(), 0, last_id, 10)
            .await
            .unwrap();
        assert!(result2.end);
    }

    #[tokio::test]
    async fn test_process_rename_prevent_directory_cycles() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c
        let (a_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();

        // Test 1: Try to rename /a into /a/b (direct descendant)
        let result = fs
            .process_rename(&(&test_auth()).into(), 0, b"a", b_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 2: Try to rename /a into /a/b/c (deeper descendant)
        let result = fs
            .process_rename(&(&test_auth()).into(), 0, b"a", c_id, b"a_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 3: Try to rename /a/b into /a/b/c (moving into immediate child)
        let result = fs
            .process_rename(&(&test_auth()).into(), a_id, b"b", c_id, b"b_moved")
            .await;
        assert!(matches!(result, Err(FsError::InvalidArgument)));

        // Test 4: Valid rename - moving /a/b/c to root
        let result = fs
            .process_rename(&(&test_auth()).into(), b_id, b"c", 0, b"c_moved")
            .await;
        assert!(result.is_ok());

        // Test 5: Valid rename - moving a file (not a directory) should work
        let (_file_id, _) = fs
            .process_create(&test_creds(), a_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();
        let result = fs
            .process_rename(
                &(&test_auth()).into(),
                a_id,
                b"file.txt",
                b_id,
                b"file_moved.txt",
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_ancestor_of() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create directory structure: /a/b/c/d
        let (a_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .process_mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();
        let (d_id, _) = fs
            .process_mkdir(&test_creds(), c_id, b"d", &SetAttributes::default())
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
    async fn test_readdir_encoding_with_hardlinks() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create files with hardlinks
        let (file1_id, _) = fs
            .process_create(&test_creds(), 0, b"file1.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Create hardlinks
        fs.process_link(&(&test_auth()).into(), file1_id, 0, b"hardlink1.txt")
            .await
            .unwrap();
        fs.process_link(&(&test_auth()).into(), file1_id, 0, b"hardlink2.txt")
            .await
            .unwrap();

        // Create another file
        let (_file2_id, _) = fs
            .process_create(&test_creds(), 0, b"file2.txt", &SetAttributes::default())
            .await
            .unwrap();

        // First readdir - get all entries
        let result1 = fs
            .process_readdir(&(&test_auth()).into(), 0, 0, 10)
            .await
            .unwrap();
        assert_eq!(result1.entries.len(), 6); // . .. file1.txt hardlink1.txt hardlink2.txt file2.txt

        // Check that hardlinks have different encoded fileids
        let file1_encoded = result1.entries[2].fileid;
        let hardlink1_encoded = result1.entries[3].fileid;
        let hardlink2_encoded = result1.entries[4].fileid;

        // Decode to verify they point to the same inode
        let (file1_inode, file1_pos) = EncodedFileId::from(file1_encoded).decode();
        let (hardlink1_inode, hardlink1_pos) = EncodedFileId::from(hardlink1_encoded).decode();
        let (hardlink2_inode, hardlink2_pos) = EncodedFileId::from(hardlink2_encoded).decode();

        assert_eq!(file1_inode, hardlink1_inode);
        assert_eq!(file1_inode, hardlink2_inode);
        assert_eq!(file1_pos, 0);
        assert_eq!(hardlink1_pos, 1);
        assert_eq!(hardlink2_pos, 2);

        // Test pagination - start after the first hardlink
        let result2 = fs
            .process_readdir(&(&test_auth()).into(), 0, hardlink1_encoded, 10)
            .await
            .unwrap();
        assert_eq!(result2.entries.len(), 2); // hardlink2.txt file2.txt
        assert_eq!(result2.entries[0].name, b"hardlink2.txt");
        assert_eq!(result2.entries[1].name, b"file2.txt");
    }

    #[tokio::test]
    async fn test_max_hardlinks_limit() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        // Create a file
        let (file_id, _) = fs
            .process_create(&test_creds(), 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        // Manually set nlink to just below the limit to avoid creating 65k files
        let mut inode = fs.load_inode(file_id).await.unwrap();
        match &mut inode {
            Inode::File(file) => {
                file.nlink = crate::fs::MAX_HARDLINKS_PER_INODE - 1;
            }
            _ => panic!("Expected file inode"),
        }
        fs.save_inode(file_id, &inode).await.unwrap();

        // Create one more hardlink - should succeed
        let result = fs
            .process_link(&(&test_auth()).into(), file_id, 0, b"last_link.txt")
            .await;
        assert!(result.is_ok());

        // Verify the file now has exactly MAX_HARDLINKS_PER_INODE links
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, crate::fs::MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }

        // Try to create one more hardlink - should fail
        let result = fs
            .process_link(&(&test_auth()).into(), file_id, 0, b"one_too_many.txt")
            .await;
        assert!(matches!(result, Err(FsError::TooManyLinks)));

        // Verify the file still has MAX_HARDLINKS_PER_INODE links
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            Inode::File(file) => {
                assert_eq!(file.nlink, crate::fs::MAX_HARDLINKS_PER_INODE);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_parent_directory_execute_permissions() {
        let fs = ZeroFS::new_in_memory().await.unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&test_creds(), 0, b"test_dir", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(
                &test_creds(),
                dir_id,
                b"test.txt",
                &SetAttributes::default(),
            )
            .await
            .unwrap();

        fs.process_write(&(&test_auth()).into(), file_id, 0, b"initial data")
            .await
            .unwrap();

        let no_exec_attrs = SetAttributes {
            mode: SetMode::Set(0o644),
            ..Default::default()
        };

        fs.process_setattr(&test_creds(), dir_id, &no_exec_attrs)
            .await
            .unwrap();

        let chmod_attrs = SetAttributes {
            mode: SetMode::Set(0o600),
            ..Default::default()
        };

        fs.process_setattr(&test_creds(), file_id, &chmod_attrs)
            .await
            .unwrap();

        let (data, _) = fs
            .process_read_file(&(&test_auth()).into(), file_id, 0, 100)
            .await
            .unwrap();
        assert_eq!(data, b"initial data");

        fs.process_write(&(&test_auth()).into(), file_id, 0, b"updated data")
            .await
            .unwrap();

        let result = fs
            .process_create(
                &test_creds(),
                dir_id,
                b"new_file.txt",
                &SetAttributes::default(),
            )
            .await;
        assert!(matches!(result, Err(FsError::PermissionDenied)));
    }
}
