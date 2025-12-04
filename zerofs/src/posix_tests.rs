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
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
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

        let fattr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o777, 0o644);

        let setattr = SetAttributes {
            mode: SetMode::Set(0o7755),
            uid: SetUid::NoChange,
            gid: SetGid::NoChange,
            size: SetSize::NoChange,
            atime: SetTime::NoChange,
            mtime: SetTime::NoChange,
        };

        let fattr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o7777, 0o7755);
    }

    #[tokio::test]
    async fn test_umask_directory() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (dir_id, fattr) = fs
            .mkdir(&creds, 0, b"testdir", &SetAttributes::default())
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
        fs.setattr(&creds, dir_id, &setattr).await.unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o755),
            ..Default::default()
        };
        fs.setattr(&creds, dir_id, &setattr).await.unwrap();

        let parent_inode = fs.inode_store.get(dir_id).await.unwrap();
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
            .mkdir(&creds, dir_id, b"subdir", &SetAttributes::default())
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
            .mkdir(&test_creds(), 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_creds(), a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_creds(), b_id, b"c", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.rename(&auth, 0, b"a", b_id, b"a").await;
        assert!(result.is_err());

        let result = fs.rename(&auth, 0, b"a", c_id, b"a").await;
        assert!(result.is_err());

        let result = fs.rename(&auth, a_id, b".", 0, b"dot").await;
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
            .create(&creds, 0, b"file1", &SetAttributes::default())
            .await
            .unwrap();
        let (file2_id, _) = fs
            .create(&creds, 0, b"file2", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            file1_id,
            0,
            &bytes::Bytes::from(b"content1".to_vec()),
        )
        .await
        .unwrap();
        fs.write(
            &auth,
            file2_id,
            0,
            &bytes::Bytes::from(b"content2".to_vec()),
        )
        .await
        .unwrap();

        fs.rename(&auth, 0, b"file1", 0, b"file2").await.unwrap();

        let result = fs.lookup(&creds, 0, b"file1").await;
        assert!(result.is_err());

        let found_id = fs.lookup(&creds, 0, b"file2").await.unwrap();
        assert_eq!(found_id, file1_id);

        let (data, _) = fs.read_file(&auth, found_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"content1");

        let result = fs.inode_store.get(file2_id).await;
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
            .mkdir(&creds, 0, b"parent", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o1777),
            ..Default::default()
        };
        fs.setattr(&creds, parent_id, &setattr).await.unwrap();

        let (_subdir_id, _) = fs
            .mkdir(&creds, parent_id, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.remove(&auth, parent_id, b"subdir").await.unwrap();

        let result = fs.lookup(&creds, parent_id, b"subdir").await;
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
            .mkdir(&creds, 0, b"dir", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&creds, dir_id, b"file", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.remove(&auth, 0, b"dir").await;
        assert!(result.is_err());

        fs.remove(&auth, dir_id, b"file").await.unwrap();

        fs.remove(&auth, 0, b"dir").await.unwrap();
    }

    #[tokio::test]
    async fn test_symlink_creation() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let target = b"/path/to/target";
        let (link_id, fattr) = fs
            .symlink(&creds, 0, b"mylink", target, &SetAttributes::default())
            .await
            .unwrap();

        use crate::fs::types::FileType;
        assert!(matches!(fattr.file_type, FileType::Symlink));
        assert_eq!(fattr.size, target.len() as u64);

        let inode = fs.inode_store.get(link_id).await.unwrap();
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
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"Hello, World!".to_vec()),
        )
        .await
        .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
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

        let fattr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 5);

        let (data, _) = fs.read_file(&auth, file_id, 0, 10).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello");

        let setattr = SetAttributes {
            size: SetSize::Set(10),
            ..Default::default()
        };

        let fattr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(fattr.size, 10);

        let (data, _) = fs.read_file(&auth, file_id, 0, 10).await.unwrap();
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
            .mkdir(&creds, 0, b"tmp", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            mode: SetMode::Set(0o1777),
            ..Default::default()
        };
        fs.setattr(&creds, tmp_id, &setattr).await.unwrap();

        fs.create(&creds, tmp_id, b"file", &SetAttributes::default())
            .await
            .unwrap();

        fs.remove(&auth, tmp_id, b"file").await.unwrap();

        let result = fs.lookup(&creds, tmp_id, b"file").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_times_permissions() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let server_time_setattr = SetAttributes {
            atime: SetTime::SetToServerTime,
            mtime: SetTime::SetToServerTime,
            ..Default::default()
        };

        let fattr = fs
            .setattr(&creds, file_id, &server_time_setattr)
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
            .setattr(&creds, file_id, &client_time_setattr)
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
            .create_exclusive(&auth, 0, b"exclusive.txt")
            .await
            .unwrap();

        assert!(file_id > 0);

        let result = fs.create_exclusive(&auth, 0, b"exclusive.txt").await;
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
            .mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        for i in 0..10 {
            let name = format!("file{i}");
            fs.create(&creds, dir_id, name.as_bytes(), &SetAttributes::default())
                .await
                .unwrap();
        }

        let mut entries = Vec::new();
        let mut start_after = 0;
        loop {
            let result = fs.readdir(&auth, dir_id, start_after, 5).await.unwrap();
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
            .mkdir(&creds, 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&creds, 0, b"dir2", &SetAttributes::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .create(&creds, dir1_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"test content".to_vec()),
        )
        .await
        .unwrap();

        fs.rename(&auth, dir1_id, b"file.txt", dir2_id, b"file.txt")
            .await
            .unwrap();

        let result = fs.lookup(&creds, dir1_id, b"file.txt").await;
        assert!(result.is_err());

        let found_id = fs.lookup(&creds, dir2_id, b"file.txt").await.unwrap();
        assert_eq!(found_id, file_id);

        let (data, _) = fs.read_file(&auth, found_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"test content");

        let _dir1_inode = fs.inode_store.get(dir1_id).await.unwrap();
        let _dir2_inode = fs.inode_store.get(dir2_id).await.unwrap();
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
            .create(&creds, 0, b"test.bin", &SetAttributes::default())
            .await
            .unwrap();

        let chunk_size = 128 * 1024;
        let test_data: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

        fs.write(&auth, file_id, 0, &bytes::Bytes::from(test_data.clone()))
            .await
            .unwrap();

        let (data1, _) = fs
            .read_file(&auth, file_id, 0, chunk_size as u32)
            .await
            .unwrap();
        assert_eq!(data1, test_data);

        let offset = chunk_size as u64 - 100;
        let (data2, _) = fs.read_file(&auth, file_id, offset, 200).await.unwrap();
        assert_eq!(data2.len(), 100);
        assert_eq!(&data2[..], &test_data[offset as usize..]);

        let (data3, eof) = fs
            .read_file(&auth, file_id, chunk_size as u64, 100)
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
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let special_modes = [0o4755, 0o2755, 0o1755, 0o7755];
        for mode in special_modes.iter() {
            let setattr = SetAttributes {
                mode: SetMode::Set(*mode),
                ..Default::default()
            };

            let new_attr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
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
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            file_id,
            0,
            &bytes::Bytes::from(b"Hello, World!".to_vec()),
        )
        .await
        .unwrap();

        let initial_inode = fs.inode_store.get(file_id).await.unwrap();
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

        let new_attr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
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

        let (_, fattr) = fs.create(&creds, 0, b"test.txt", &attr).await.unwrap();

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
            .symlink(&creds, 0, b"broken_link", target, &SetAttributes::default())
            .await
            .unwrap();

        let inode = fs.inode_store.get(link_id).await.unwrap();
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
            .create(&creds, 0, b"sparse.dat", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(&auth, file_id, 100, &bytes::Bytes::from(b"Hello".to_vec()))
            .await
            .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        let attr: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &inode,
            id: file_id,
        }
        .into();
        assert_eq!(attr.size, 105);

        let (data, _) = fs.read_file(&auth, file_id, 0, 100).await.unwrap();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.read_file(&auth, file_id, 100, 5).await.unwrap();
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
            .create(&creds, 0, b"source.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            src_id,
            0,
            &bytes::Bytes::from(b"source content".to_vec()),
        )
        .await
        .unwrap();

        let (_target_id, _) = fs
            .create(&creds, 0, b"target.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
            &auth,
            _target_id,
            0,
            &bytes::Bytes::from(b"target content".to_vec()),
        )
        .await
        .unwrap();

        fs.rename(&auth, 0, b"source.txt", 0, b"target.txt")
            .await
            .unwrap();

        let result = fs.lookup(&creds, 0, b"source.txt").await;
        assert!(result.is_err());

        let new_id = fs.lookup(&creds, 0, b"target.txt").await.unwrap();
        assert_eq!(new_id, src_id);

        let (data, _) = fs.read_file(&auth, new_id, 0, 100).await.unwrap();
        assert_eq!(data.as_ref(), b"source content");
    }

    #[tokio::test]
    async fn test_directory_attributes() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (dir_id, initial_attr) = fs
            .mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        assert_eq!(initial_attr.nlink, 2);

        let setattr = SetAttributes {
            mode: SetMode::Set(0o700),
            ..Default::default()
        };

        let new_attr = fs.setattr(&creds, dir_id, &setattr).await.unwrap();
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
            .create(&creds, 0, b"growth.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.write(
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

        let attr_after = fs.setattr(&creds, file_id, &setattr).await.unwrap();
        assert_eq!(attr_after.size, 100);

        let (data, _) = fs.read_file(&auth, file_id, 0, 13).await.unwrap();
        assert_eq!(data.as_ref(), b"Hello, World!");

        let (data, _) = fs.read_file(&auth, file_id, 13, 87).await.unwrap();
        assert_eq!(data.len(), 87);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.read_file(&auth, file_id, 0, 13).await.unwrap();
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
            .mkdir(&creds, 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&creds, a_id, b"b", &SetAttributes::default())
            .await
            .unwrap();

        let result = fs.readdir(&auth, 0, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&"a".to_string()));

        let result = fs.readdir(&auth, a_id, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name).to_string())
            .collect();
        assert!(names.contains(&"b".to_string()));

        let result = fs.readdir(&auth, b_id, 0, 10).await.unwrap();
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

        let parent_inode_before = fs.inode_store.get(0).await.unwrap();
        let parent_attr_before: crate::fs::types::FileAttributes = crate::fs::types::InodeWithId {
            inode: &parent_inode_before,
            id: 0,
        }
        .into();
        let mtime_before = parent_attr_before.mtime;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        fs.create(&creds, 0, b"newfile.txt", &SetAttributes::default())
            .await
            .unwrap();

        let parent_inode_after = fs.inode_store.get(0).await.unwrap();
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
            .mkdir(&creds, 0, b"testdir", &SetAttributes::default())
            .await
            .unwrap();

        let parent_inode_final = fs.inode_store.get(0).await.unwrap();
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
            .create(&creds, 0, b"a", &SetAttributes::default())
            .await
            .unwrap();

        // Create hard link 'b' pointing to same inode as 'a'
        fs.link(&auth, file_a_id, 0, b"b").await.unwrap();

        // Both 'a' and 'b' should be visible in directory listing
        let entries = fs.readdir(&auth, 0, 0, 100).await.unwrap();

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
        let a_id = fs.lookup(&creds, 0, b"a").await.unwrap();
        let b_id = fs.lookup(&creds, 0, b"b").await.unwrap();
        assert_eq!(a_id, b_id, "Both names should point to the same inode");

        // Check link count
        let inode = fs.inode_store.get(a_id).await.unwrap();
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
            .create(&creds, 0, b"special.txt", &SetAttributes::default())
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

            let new_attr = fs.setattr(&creds, file_id, &setattr).await.unwrap();
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
            .mkdir(&creds, 0, b"dir1", &SetAttributes::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&creds, dir1_id, b"dir2", &SetAttributes::default())
            .await
            .unwrap();
        let (dir3_id, _) = fs
            .mkdir(&creds, dir2_id, b"dir3", &SetAttributes::default())
            .await
            .unwrap();

        fs.create(&creds, dir3_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        let auth = AuthContext {
            uid: 1000,
            gid: 1000,
            gids: vec![1000],
        };
        fs.rename(&auth, dir2_id, b"dir3", 0, b"moved_dir3")
            .await
            .unwrap();

        let found_id = fs.lookup(&creds, 0, b"moved_dir3").await.unwrap();
        assert_eq!(found_id, dir3_id);

        let found_file = fs.lookup(&creds, dir3_id, b"file.txt").await.unwrap();
        assert!(found_file > 0);
    }

    #[tokio::test]
    async fn test_hardlink_clears_parent_and_name() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, Some(0));
                assert_eq!(f.name, Some(b"original.txt".to_vec()));
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }

        fs.link(
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

        // Parent and name should become None when file is hardlinked
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.name, None);
                assert_eq!(f.nlink, 2);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_hardlink_parent_and_name_stay_none_after_unlink() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(
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

        fs.remove(
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

        // Parent and name should stay None even when nlink drops back to 1
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.name, None);
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_lazy_parent_and_name_restoration_on_rename() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(
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

        fs.remove(
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
            .mkdir(&creds, 0, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.rename(
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

        // Parent and name should be lazily restored on rename when nlink=1
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, Some(dir_id));
                assert_eq!(f.name, Some(b"moved.txt".to_vec()));
                assert_eq!(f.nlink, 1);
            }
            _ => panic!("Expected file inode"),
        }
    }

    #[tokio::test]
    async fn test_rename_hardlinked_file_parent_and_name_stay_none() {
        let fs = create_test_fs().await;
        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"original.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(
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
            .mkdir(&creds, 0, b"subdir", &SetAttributes::default())
            .await
            .unwrap();

        fs.rename(
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

        // Parent and name should stay None when renaming file with nlink > 1
        let inode = fs.inode_store.get(file_id).await.unwrap();
        match inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.name, None);
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
            .mkdir(
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
            .create(&owner_creds, dir_id, b"file.txt", &SetAttributes::default())
            .await
            .unwrap();

        fs.link(
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

        let inode = fs.inode_store.get(file_id).await.unwrap();
        match &inode {
            crate::fs::inode::Inode::File(f) => {
                assert_eq!(f.parent, None);
                assert_eq!(f.nlink, 2);
            }
            _ => panic!("Expected file inode"),
        }

        // Parent permission checks should be skipped when parent=None
        let result = fs
            .read_file(
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

    #[tokio::test]
    async fn test_quota_write_enforcement() {
        let fs = Arc::new(
            ZeroFS::new_with_slatedb(
                crate::encryption::SlateDbHandle::ReadWrite(Arc::new(
                    slatedb::DbBuilder::new(
                        slatedb::object_store::path::Path::from("test_quota"),
                        Arc::new(slatedb::object_store::memory::InMemory::new()),
                    )
                    .build()
                    .await
                    .unwrap(),
                )),
                [0u8; 32],
                1_000_000,
            )
            .await
            .unwrap(),
        );

        let creds = test_creds();
        let auth = AuthContext {
            uid: creds.uid,
            gid: creds.gid,
            gids: vec![],
        };

        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = bytes::Bytes::from(vec![0u8; 500_000]);
        let result = fs.write(&auth, file_id, 0, &data).await;
        assert!(result.is_ok());

        let data = bytes::Bytes::from(vec![0u8; 600_000]);
        let result = fs.write(&auth, file_id, 500_000, &data).await;
        assert!(matches!(result, Err(crate::fs::errors::FsError::NoSpace)));

        fs.remove(&auth, 0, b"test.txt").await.unwrap();

        let (file_id2, _) = fs
            .create(&creds, 0, b"test2.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = bytes::Bytes::from(vec![0u8; 500_000]);
        let result = fs.write(&auth, file_id2, 0, &data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_quota_setattr_enforcement() {
        let fs = Arc::new(
            ZeroFS::new_with_slatedb(
                crate::encryption::SlateDbHandle::ReadWrite(Arc::new(
                    slatedb::DbBuilder::new(
                        slatedb::object_store::path::Path::from("test_quota_setattr"),
                        Arc::new(slatedb::object_store::memory::InMemory::new()),
                    )
                    .build()
                    .await
                    .unwrap(),
                )),
                [0u8; 32],
                1_000_000,
            )
            .await
            .unwrap(),
        );

        let creds = test_creds();

        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let setattr = SetAttributes {
            size: SetSize::Set(500_000),
            ..Default::default()
        };
        let result = fs.setattr(&creds, file_id, &setattr).await;
        assert!(result.is_ok());

        let setattr = SetAttributes {
            size: SetSize::Set(1_500_000),
            ..Default::default()
        };
        let result = fs.setattr(&creds, file_id, &setattr).await;
        assert!(matches!(result, Err(crate::fs::errors::FsError::NoSpace)));

        let setattr = SetAttributes {
            size: SetSize::Set(100_000),
            ..Default::default()
        };
        let result = fs.setattr(&creds, file_id, &setattr).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_quota_allows_deletes_when_over_limit() {
        let fs = Arc::new(
            ZeroFS::new_with_slatedb(
                crate::encryption::SlateDbHandle::ReadWrite(Arc::new(
                    slatedb::DbBuilder::new(
                        slatedb::object_store::path::Path::from("test_quota_over"),
                        Arc::new(slatedb::object_store::memory::InMemory::new()),
                    )
                    .build()
                    .await
                    .unwrap(),
                )),
                [0u8; 32],
                1_000_000,
            )
            .await
            .unwrap(),
        );

        let creds = test_creds();
        let auth = AuthContext {
            uid: creds.uid,
            gid: creds.gid,
            gids: vec![],
        };

        let (file_id, _) = fs
            .create(&creds, 0, b"big.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = bytes::Bytes::from(vec![0u8; 900_000]);
        fs.write(&auth, file_id, 0, &data).await.unwrap();

        let (file_id2, _) = fs
            .create(&creds, 0, b"new.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = bytes::Bytes::from(vec![0u8; 200_000]);
        let result = fs.write(&auth, file_id2, 0, &data).await;
        assert!(matches!(result, Err(crate::fs::errors::FsError::NoSpace)));

        let result = fs.remove(&auth, 0, b"big.txt").await;
        assert!(result.is_ok());

        let data = bytes::Bytes::from(vec![0u8; 500_000]);
        let result = fs.write(&auth, file_id2, 0, &data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_quota_default_unlimited() {
        let fs = create_test_fs().await;
        let creds = test_creds();
        let auth = AuthContext {
            uid: creds.uid,
            gid: creds.gid,
            gids: vec![],
        };

        let (file_id, _) = fs
            .create(&creds, 0, b"test.txt", &SetAttributes::default())
            .await
            .unwrap();

        let data = bytes::Bytes::from(vec![0u8; 10_000_000]);
        let result = fs.write(&auth, file_id, 0, &data).await;
        assert!(result.is_ok());
    }
}
