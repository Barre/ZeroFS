#[cfg(test)]
mod tests {
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{AuthContext, SetAttributes, SetGid, SetMode, SetSize, SetTime, SetUid};
    use std::sync::Arc;

    async fn create_test_fs() -> Arc<ZeroFS> {
        Arc::new(ZeroFS::new_in_memory().await.unwrap())
    }

    fn test_creds() -> Credentials {
        Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        }
    }

    #[tokio::test]
    async fn test_chmod_basic() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o644),
            uid: SetUid::NoChange,
            gid: SetGid::NoChange,
            size: SetSize::NoChange,
            atime: SetTime::NoChange,
            mtime: SetTime::NoChange,
        };

        let fattr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o777, 0o644);

        let setattr = SetAttributes {
            mode: SetMode::Set(0o7755),
            uid: SetUid::NoChange,
            gid: SetGid::NoChange,
            size: SetSize::NoChange,
            atime: SetTime::NoChange,
            mtime: SetTime::NoChange,
        };

        let fattr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o7777, 0o7755);
    }

    #[tokio::test]
    async fn test_umask_directory() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (dir_id, fattr) = fs
            .process_mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();
        assert_eq!(
            fattr.mode & 0o777,
            0o777,
            "Directory permissions should not have umask applied by server"
        );

        let setattr = SetAttributes {
            mode: SetMode::Set(0o1777),
            ..Default::default()
        };
        fs.process_setattr(&creds, dir_id, &setattr).await.unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o755),
            ..Default::default()
        };
        fs.process_setattr(&creds, dir_id, &setattr).await.unwrap();

        let parent_inode = fs.load_inode(dir_id).await.unwrap();
        let parent_fattr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &parent_inode,
            id: dir_id,
        }
        .into();
        assert_eq!(
            parent_fattr.mode & 0o1000,
            0,
            "Sticky bit should be cleared"
        );

        let (_subdir_id, subdir_fattr) = fs
            .process_mkdir(&creds, dir_id, b"subdir", &SetAttributes::default())
            .await
            .unwrap();
        assert_eq!(
            subdir_fattr.mode & 0o777,
            0o777,
            "Subdirectory should not have umask applied by server"
        );
    }

    #[tokio::test]
    async fn test_rename_to_descendant() {
        let fs = create_test_fs().await;
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

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

        let result = fs.process_rename(&auth, 0, b"a", b_id, b"a").await;
        assert!(result.is_err());

        let result = fs.process_rename(&auth, 0, b"a", c_id, b"a").await;
        assert!(result.is_err());

        let result = fs.process_rename(&auth, a_id, b".", 0, b"dot").await;
        assert!(result.is_err(), "Renaming '.' should fail");
    }

    #[tokio::test]
    async fn test_rename_overwrite_regular_file() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file1_id, _) = fs
            .process_create(&creds, 0, b"file1", &SetAttributes::default())
            .await
            .unwrap();
        let (file2_id, _) = fs
            .process_create(&creds, 0, b"file2", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            file1_id,
            0,
            &bytes::Bytes::from(b"content1".to_vec()),
        )
        .await
        .unwrap();
        fs.process_write(
            &auth,
            file2_id,
            0,
            &bytes::Bytes::from(b"content2".to_vec()),
        )
        .await
        .unwrap();

        fs.process_rename(&auth, 0, b"file1", 0, b"file2")
            .await
            .unwrap();

        let result = fs.process_lookup(&creds, 0, b"file1").await;
        assert!(result.is_err());

        let found_id = fs.process_lookup(&creds, 0, b"file2").await.unwrap();
        assert_eq!(found_id, file1_id);

        let (data, _) = fs.process_read_file(&auth, found_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"content1");

        let result = fs.load_inode(file2_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sticky_bit_permissions() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (parent_id, _) = fs
            .process_mkdir(&creds, 0, b"parent", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o1777),
            ..Default::default()
        };
        fs.process_setattr(&creds, parent_id, &setattr)
            .await
            .unwrap();

        let (_subdir_id, _) = fs
            .process_mkdir(&creds, parent_id, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_remove(&auth, parent_id, b"subdir")
            .await
            .unwrap();

        let result = fs.process_lookup(&creds, parent_id, b"subdir").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remove_non_empty_directory() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (dir_id, _) = fs
            .process_mkdir(&creds, 0, b"dir", &SetAttributes::default())
            .await
            .unwrap();
        fs.process_create(&creds, dir_id, b"file", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.process_remove(&auth, 0, b"dir").await;
        assert!(result.is_err());

        fs.process_remove(&auth, dir_id, b"file").await.unwrap();

        fs.process_remove(&auth, 0, b"dir").await.unwrap();
    }

    #[tokio::test]
    async fn test_symlink_creation() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let target = b"/path/to/target";
        let (link_id, fattr) = fs
            .process_symlink(&creds, 0, b"mylink", target, &SetAttributes::default())
            .await
            .unwrap();

        use crate::fs::types::FileType;
        assert!(matches!(fattr.file_type, FileType::Symlink));
        assert_eq!(fattr.size, target.len() as u64);

        let inode = fs.load_inode(link_id).await.unwrap();
        if let crate::fs::inode::Inode::Symlink(symlink) = inode {
            assert_eq!(&symlink.target, target);
        } else {
            panic!("Expected symlink");
        }
    }

    #[tokio::test]
    async fn test_truncate_with_setattr() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"Hello, World!".to_vec()),
        )
        .await
        .unwrap();

        let inode = fs.load_inode(file_id).await.unwrap();
        let fattr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &inode,
            id: file_id,
        }
        .into();
        assert_eq!(fattr.size, 13);

        let setattr = SetAttributes {
            size: SetSize::Set(5),
            ..Default::default()
        };

        let fattr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 5);

        let (data, _) = fs.process_read_file(&auth, file_id, 0, 10).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello");

        let setattr = SetAttributes {
            size: SetSize::Set(10),
            ..Default::default()
        };

        let fattr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 10);

        let (data, _) = fs.process_read_file(&auth, file_id, 0, 10).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello\0\0\0\0\0");
    }

    #[tokio::test]
    async fn test_sticky_bit_deletion() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (tmp_id, _) = fs
            .process_mkdir(&creds, 0, b"tmp", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o1777),
            ..Default::default()
        };
        fs.process_setattr(&creds, tmp_id, &setattr).await.unwrap();

        fs.process_create(&creds, tmp_id, b"file", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_remove(&auth, tmp_id, b"file").await.unwrap();

        let result = fs.process_lookup(&creds, tmp_id, b"file").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_times_permissions() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let server_time_setattr = SetAttributes {
            atime: SetTime::SetToServerTime,
            mtime: SetTime::SetToServerTime,
            ..Default::default()
        };

        let fattr = fs
            .process_setattr(&creds, file_id, &server_time_setattr)
            .await
            .unwrap();
        assert!(fattr.atime.seconds > 0);
        assert!(fattr.mtime.seconds > 0);

        let client_time = crate::fs::types::Timestamp {
            seconds: 1234567890,
            nanoseconds: 123456789,
        };

        let client_time_setattr = SetAttributes {
            atime: SetTime::SetToClientTime(client_time),
            mtime: SetTime::SetToClientTime(client_time),
            ..Default::default()
        };

        let fattr = fs
            .process_setattr(&creds, file_id, &client_time_setattr)
            .await
            .unwrap();
        assert_eq!(fattr.atime.seconds, client_time.seconds);
        assert_eq!(fattr.atime.nanoseconds, client_time.nanoseconds);
        assert_eq!(fattr.mtime.seconds, client_time.seconds);
        assert_eq!(fattr.mtime.nanoseconds, client_time.nanoseconds);
    }

    #[tokio::test]
    async fn test_create_exclusive() {
        let fs = create_test_fs().await;
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let file_id = fs
            .process_create_exclusive(&auth, 0, b"exclusive.txt")
            .await
            .unwrap();

        assert!(file_id > 0);

        let result = fs
            .process_create_exclusive(&auth, 0, b"exclusive.txt")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_readdir_basic() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (dir_id, _) = fs
            .process_mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        for i in 0..10 {
            let name = format!("file{i}");
            fs.process_create(&creds, dir_id, name.as_bytes(), &SetAttributes::default())
                .await
                .unwrap();
        }

        let mut entries = Vec::new();
        let mut start_after = 0;
        loop {
            let result = fs
                .process_readdir(&auth, dir_id, start_after, 5)
                .await
                .unwrap();
            let _count = result.entries.len();
            entries.extend(result.entries);

            if result.end {
                break;
            }
            start_after = entries.last().unwrap().fileid;
        }

        assert!(entries.len() >= 12);

        let names: Vec<String> = entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&".".to_string()));
        assert!(names.contains(&"..".to_string()));
        for i in 0..10 {
            assert!(names.contains(&format!("file{i}")));
        }
    }

    #[tokio::test]
    async fn test_rename_across_directories() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (dir1_id, _) = fs
            .process_mkdir(&creds, 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .process_mkdir(&creds, 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&creds, dir1_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        fs.process_rename(&auth, dir1_id, b"file.txt", dir2_id, b"file.txt")
            .await
            .unwrap();

        let result = fs.process_lookup(&creds, dir1_id, b"file.txt").await;
        assert!(result.is_err());

        let found_id = fs
            .process_lookup(&creds, dir2_id, b"file.txt")
            .await
            .unwrap();
        assert_eq!(found_id, file_id);

        let (data, _) = fs.process_read_file(&auth, found_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"test content");

        let _dir1_inode = fs.load_inode(dir1_id).await.unwrap();
        let _dir2_inode = fs.load_inode(dir2_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_operations_edge_cases() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.bin", &SetAttributes::default())
            .await
            .unwrap();

        let chunk_size = 128 * 1024;
        let test_data: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

        fs.process_write(&auth, file_id, 0, &bytes::Bytes::from(test_data.clone()))
            .await
            .unwrap();

        let (data1, _) = fs
            .process_read_file(&auth, file_id, 0, chunk_size as u32)
            .await
            .unwrap();
        assert_eq!(data1, test_data);

        let offset = chunk_size as u64 - 100;
        let (data2, _) = fs
            .process_read_file(&auth, file_id, offset, 200)
            .await
            .unwrap();
        assert_eq!(data2.len(), 100);
        assert_eq!(&data2[..], &test_data[offset as usize..]);

        let (data3, eof) = fs
            .process_read_file(&auth, file_id, chunk_size as u64, 100)
            .await
            .unwrap();
        assert_eq!(data3.len(), 0);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_chmod_setuid_setgid_sticky() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let special_modes = [0o4755, 0o2755, 0o1755, 0o7755];
        for mode in special_modes.iter() {
            let setattr = SetAttributes {
                mode: SetMode::Set(*mode),
                ..Default::default()
            };

            let new_attr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
            assert_eq!(new_attr.mode & 0o7777, *mode);
        }
    }

    #[tokio::test]
    async fn test_time_updates() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file_id, _) = fs
            .process_create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"Hello, World!".to_vec()),
        )
        .await
        .unwrap();

        let initial_inode = fs.load_inode(file_id).await.unwrap();
        let initial_attr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &initial_inode,
            id: file_id,
        }
        .into();
        let initial_mtime = initial_attr.mtime;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let setattr = SetAttributes {
            mtime: SetTime::SetToServerTime,
            ..Default::default()
        };

        let new_attr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert!(
            new_attr.mtime >= initial_mtime,
            "mtime should be updated to server time"
        );
    }

    #[tokio::test]
    async fn test_create_with_specific_attributes() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let attr = SetAttributes {
            mode: SetMode::Set(0o640),
            uid: SetUid::Set(1001),
            gid: SetGid::Set(1001),
            atime: SetTime::SetToServerTime,
            mtime: SetTime::SetToServerTime,
            ..Default::default()
        };

        let (_, fattr) = fs
            .process_create(&creds, 0, b"test.txt", &attr)
            .await
            .unwrap();

        assert_eq!(fattr.mode & 0o777, 0o640);
        assert_eq!(fattr.uid, 1001);
        assert_eq!(fattr.gid, 1001);
    }

    #[tokio::test]
    async fn test_symlink_operations() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let target = b"/nonexistent/path";
        let (link_id, _) = fs
            .process_symlink(&creds, 0, b"broken_link", target, &SetAttributes::default())
            .await
            .unwrap();

        let inode = fs.load_inode(link_id).await.unwrap();
        if let crate::fs::inode::Inode::Symlink(symlink) = inode {
            assert_eq!(&symlink.target, b"/nonexistent/path");
        } else {
            panic!("Expected symlink");
        }
    }

    #[tokio::test]
    async fn test_sparse_file_operations() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file_id, _) = fs
            .process_create(&creds, 0, b"sparse.dat", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(&auth, file_id, 100, &bytes::Bytes::from(b"Hello".to_vec()))
            .await
            .unwrap();

        let inode = fs.load_inode(file_id).await.unwrap();
        let attr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &inode,
            id: file_id,
        }
        .into();
        assert_eq!(attr.size, 105);

        let (data, _) = fs.process_read_file(&auth, file_id, 0, 100).await.unwrap();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.process_read_file(&auth, file_id, 100, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello");
    }

    #[tokio::test]
    async fn test_rename_replace_file() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (src_id, _) = fs
            .process_create(&creds, 0, b"source.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            src_id,
            0,
            &bytes::Bytes::from(b"source content".to_vec()),
        )
        .await
        .unwrap();

        let (_target_id, _) = fs
            .process_create(&creds, 0, b"target.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            _target_id,
            0,
            &bytes::Bytes::from(b"target content".to_vec()),
        )
        .await
        .unwrap();

        fs.process_rename(&auth, 0, b"source.txt", 0, b"target.txt")
            .await
            .unwrap();

        let result = fs.process_lookup(&creds, 0, b"source.txt").await;
        assert!(result.is_err());

        let new_id = fs.process_lookup(&creds, 0, b"target.txt").await.unwrap();
        assert_eq!(new_id, src_id);

        let (data, _) = fs.process_read_file(&auth, new_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"source content");
    }

    #[tokio::test]
    async fn test_directory_attributes() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (dir_id, initial_attr) = fs
            .process_mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        assert_eq!(initial_attr.nlink, 2);

        let setattr = SetAttributes {
            mode: SetMode::Set(0o700),
            ..Default::default()
        };

        let new_attr = fs.process_setattr(&creds, dir_id, &setattr).await.unwrap();
        assert_eq!(new_attr.mode & 0o777, 0o700);
        assert_eq!(new_attr.uid, 1000);
        assert_eq!(new_attr.gid, 1000);
    }

    #[tokio::test]
    async fn test_file_growth_and_truncation() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (file_id, _) = fs
            .process_create(&creds, 0, b"growth.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"Hello, World!".to_vec()),
        )
        .await
        .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(100),
            ..Default::default()
        };

        let attr_after = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(attr_after.size, 100);

        let (data, _) = fs.process_read_file(&auth, file_id, 0, 13).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello, World!");

        let (data, _) = fs.process_read_file(&auth, file_id, 13, 87).await.unwrap();
        assert_eq!(data.len(), 87);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.process_read_file(&auth, file_id, 0, 13).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello, World!");
    }

    #[tokio::test]
    async fn test_directory_hierarchy() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        let (a_id, _) = fs
            .process_mkdir(&creds, 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .process_mkdir(&creds, a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.process_readdir(&auth, 0, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&"a".to_string()));

        let result = fs.process_readdir(&auth, a_id, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&"b".to_string()));

        let result = fs.process_readdir(&auth, b_id, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&".".to_string()));
        assert!(names.contains(&"..".to_string()));
    }

    #[tokio::test]
    async fn test_directory_timestamps() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let parent_inode_before = fs.load_inode(0).await.unwrap();
        let parent_attr_before: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &parent_inode_before,
            id: 0,
        }
        .into();
        let mtime_before = parent_attr_before.mtime;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        fs.process_create(&creds, 0, b"newfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let parent_inode_after = fs.load_inode(0).await.unwrap();
        let parent_attr_after: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &parent_inode_after,
            id: 0,
        }
        .into();
        assert!(
            parent_attr_after.mtime >= mtime_before,
            "Parent directory mtime should be updated when a file is created"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let (_dir_id, _) = fs
            .process_mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        let parent_inode_final = fs.load_inode(0).await.unwrap();
        let parent_attr_final: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &parent_inode_final,
            id: 0,
        }
        .into();
        assert!(
            parent_attr_final.mtime >= parent_attr_after.mtime,
            "Parent directory mtime should be updated when a directory is created"
        );
    }

    #[tokio::test]
    async fn test_hardlink_both_names_visible() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };

        // Create file 'a'
        let (file_a_id, _) = fs
            .process_create(&creds, 0, b"a", &SetAttributes::default())
            .await
            .unwrap();

        // Create hard link 'b' pointing to same inode as 'a'
        fs.process_link(&auth, file_a_id, 0, b"b").await.unwrap();

        // Both 'a' and 'b' should be visible in directory listing
        let entries = fs.process_readdir(&auth, 0, 0, 100).await.unwrap();

        let names: Vec<String> = entries
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();

        // Check that both 'a' and 'b' exist
        assert!(
            names.contains(&"a".to_string()),
            "File 'a' should exist after creating hard link"
        );
        assert!(
            names.contains(&"b".to_string()),
            "Hard link 'b' should exist"
        );

        // Verify they point to the same inode
        let a_id = fs.process_lookup(&creds, 0, b"a").await.unwrap();
        let b_id = fs.process_lookup(&creds, 0, b"b").await.unwrap();
        assert_eq!(a_id, b_id, "Both names should point to the same inode");

        // Check link count
        let inode = fs.load_inode(a_id).await.unwrap();
        let fattr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &inode,
            id: a_id,
        }
        .into();
        assert_eq!(fattr.nlink, 2, "Link count should be 2");
    }

    #[tokio::test]
    async fn test_chmod_special_bits() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"special.txt", &SetAttributes::default())
            .await
            .unwrap();

        let modes_to_test = vec![
            (0o4755, "setuid bit"),
            (0o2755, "setgid bit"),
            (0o1755, "sticky bit"),
            (0o7755, "all special bits"),
            (0o0755, "no special bits"),
        ];

        for (mode, description) in modes_to_test {
            let setattr = SetAttributes {
                mode: SetMode::Set(mode),
                ..Default::default()
            };

            let new_attr = fs.process_setattr(&creds, file_id, &setattr).await.unwrap();
            assert_eq!(
                new_attr.mode & 0o7777,
                mode,
                "Mode should be {mode} for {description}"
            );
        }
    }

    #[tokio::test]
    async fn test_rename_directory_with_contents() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (dir1_id, _) = fs
            .process_mkdir(&creds, 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .process_mkdir(&creds, dir1_id, b"dir2", &SetAttributes::default())
            .await
            .unwrap();
        let (dir3_id, _) = fs
            .process_mkdir(&creds, dir2_id, b"dir3", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_create(&creds, dir3_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        fs.process_rename(&auth, dir2_id, b"dir3", 0, b"moved_dir3")
            .await
            .unwrap();

        let found_id = fs.process_lookup(&creds, 0, b"moved_dir3").await.unwrap();
        assert_eq!(found_id, dir3_id);

        let found_file = fs
            .process_lookup(&creds, dir3_id, b"file.txt")
            .await
            .unwrap();
        assert!(found_file > 0);
    }

    #[tokio::test]
    async fn test_hardlink_parent_becomes_none() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, Some(0));
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }

        fs.process_link(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            file_id,
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        // Parent should become None when file is hardlinked
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.nlink, 2);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_hardlink_parent_stays_none_after_unlink() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_link(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            file_id,
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        fs.process_remove(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        // Parent should stay None even when nlink drops back to 1
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_lazy_parent_restoration_on_rename() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_link(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            file_id,
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        fs.process_remove(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&creds, 0, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_rename(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            0,
            b"original.txt",
            dir_id,
            b"moved.txt",
        )
        .await
        .unwrap();

        // Parent should be lazily restored on rename when nlink=1
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, Some(dir_id));
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_rename_hardlinked_file_parent_stays_none() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .process_create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_link(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            file_id,
            0,
            b"hardlink.txt",
        )
        .await
        .unwrap();

        let (dir_id, _) = fs
            .process_mkdir(&creds, 0, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_rename(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            0,
            b"original.txt",
            dir_id,
            b"moved.txt",
        )
        .await
        .unwrap();

        // Parent should stay None when renaming file with nlink > 1
        let inode = fs.load_inode(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.nlink, 2);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_hardlink_permission_checks_skipped() {
        let fs = create_test_fs().await;
        let owner_creds = Credentials {
            uid: 1000,
            gid: 1000,
            groups: [1000; 16],
            groups_count: 1,
        };

        let (dir_id, _) = fs
            .process_mkdir(
                &owner_creds,
                0,
                b"private_dir",
                &SetAttributes {
                    mode: SetMode::Set(0o700),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let (file_id, _) = fs
            .process_create(&owner_creds, dir_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.process_link(
            &AuthContext {
                uid: 1000,
                gid: 1000,
                gids: vec![1000],
            },
            file_id,
            0,
            b"public_link.txt",
        )
        .await
        .unwrap();

        let inode = fs.load_inode(file_id).await.unwrap();
        match &inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.nlink, 2);
            }
            _ => panic!("Expected file inode"),
        }

        // Parent permission checks should be skipped when parent=None
        let result = fs
            .process_read_file(
                &AuthContext {
                    uid: 2000,
                    gid: 2000,
                    gids: vec![2000],
                },
                file_id,
                0,
                100,
            )
            .await;

        assert!(result.is_ok());
    }
}
