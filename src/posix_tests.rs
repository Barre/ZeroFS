#[cfg(test)]
mod tests {
    use crate::filesystem::SlateDbFs;
    use crate::test_helpers::test_helpers::{filename, test_auth};
    use nfsserve::nfs::*;
    use nfsserve::vfs::NFSFileSystem;

    async fn create_test_fs() -> SlateDbFs {
        SlateDbFs::new_in_memory().await.unwrap()
    }

    #[allow(dead_code)]
    async fn stat(fs: &SlateDbFs, id: fileid3) -> Result<fattr3, nfsstat3> {
        fs.getattr(&test_auth(), id).await
    }

    #[tokio::test]
    async fn test_chmod_basic() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let setattr = sattr3 {
            mode: set_mode3::mode(0o644),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o777, 0o644);

        let setattr = sattr3 {
            mode: set_mode3::mode(0o7755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert_eq!(fattr.mode & 0o7777, 0o7755);
    }

    #[tokio::test]
    async fn test_umask_directory() {
        let fs = create_test_fs().await;

        let (dir_id, fattr) = fs
            .mkdir(&test_auth(), 0, &filename(b"testdir"), &sattr3::default())
            .await
            .unwrap();
        assert_eq!(
            fattr.mode & 0o777,
            0o755,
            "Directory permissions should respect umask"
        );

        let setattr = sattr3 {
            mode: set_mode3::mode(0o1777),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };
        fs.setattr(&test_auth(), dir_id, setattr).await.unwrap();

        let setattr = sattr3 {
            mode: set_mode3::mode(0o755),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };
        fs.setattr(&test_auth(), dir_id, setattr).await.unwrap();

        let parent_fattr = fs.getattr(&test_auth(), dir_id).await.unwrap();
        assert_eq!(
            parent_fattr.mode & 0o1000,
            0,
            "Sticky bit should be cleared"
        );

        let (_subdir_id, subdir_fattr) = fs
            .mkdir(
                &test_auth(),
                dir_id,
                &filename(b"subdir"),
                &sattr3::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            subdir_fattr.mode & 0o777,
            0o755,
            "Subdirectory should have default permissions"
        );
    }

    #[tokio::test]
    async fn test_rename_to_descendant() {
        let fs = create_test_fs().await;

        let (a_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"a"), &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_auth(), a_id, &filename(b"b"), &sattr3::default())
            .await
            .unwrap();
        let (c_id, _) = fs
            .mkdir(&test_auth(), b_id, &filename(b"c"), &sattr3::default())
            .await
            .unwrap();

        let result = fs
            .rename(&test_auth(), 0, &filename(b"a"), b_id, &filename(b"a"))
            .await;
        assert!(result.is_err());

        let result = fs
            .rename(&test_auth(), 0, &filename(b"a"), c_id, &filename(b"a"))
            .await;
        assert!(result.is_err());

        let result = fs
            .rename(&test_auth(), a_id, &filename(b"."), 0, &filename(b"dot"))
            .await;
        assert!(
            result.is_err(),
            "Renaming '.' should fail with NFS3ERR_INVAL"
        );
    }

    #[tokio::test]
    async fn test_rename_overwrite_regular_file() {
        let fs = create_test_fs().await;

        let (file1_id, _) = fs
            .create(&test_auth(), 0, &filename(b"file1"), sattr3::default())
            .await
            .unwrap();
        let (file2_id, _) = fs
            .create(&test_auth(), 0, &filename(b"file2"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), file1_id, 0, b"content1")
            .await
            .unwrap();
        fs.write(&test_auth(), file2_id, 0, b"content2")
            .await
            .unwrap();

        fs.rename(&test_auth(), 0, &filename(b"file1"), 0, &filename(b"file2"))
            .await
            .unwrap();

        let result = fs.lookup(&test_auth(), 0, &filename(b"file1")).await;
        assert!(result.is_err());

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"file2"))
            .await
            .unwrap();
        assert_eq!(found_id, file1_id);

        let (data, _) = fs.read(&test_auth(), found_id, 0, 100).await.unwrap();
        assert_eq!(&data, b"content1");

        let result = fs.getattr(&test_auth(), file2_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sticky_bit_permissions() {
        let fs = create_test_fs().await;

        let (parent_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"parent"), &sattr3::default())
            .await
            .unwrap();

        let setattr = sattr3 {
            mode: set_mode3::mode(0o1777),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };
        fs.setattr(&test_auth(), parent_id, setattr).await.unwrap();

        let (_subdir_id, _) = fs
            .mkdir(
                &test_auth(),
                parent_id,
                &filename(b"subdir"),
                &sattr3::default(),
            )
            .await
            .unwrap();

        fs.remove(&test_auth(), parent_id, &filename(b"subdir"))
            .await
            .unwrap();

        let result = fs
            .lookup(&test_auth(), parent_id, &filename(b"subdir"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remove_non_empty_directory() {
        let fs = create_test_fs().await;

        let (dir_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"dir"), &sattr3::default())
            .await
            .unwrap();
        fs.create(&test_auth(), dir_id, &filename(b"file"), sattr3::default())
            .await
            .unwrap();

        let result = fs.remove(&test_auth(), 0, &filename(b"dir")).await;
        assert!(result.is_err());

        fs.remove(&test_auth(), dir_id, &filename(b"file"))
            .await
            .unwrap();

        fs.remove(&test_auth(), 0, &filename(b"dir")).await.unwrap();
    }

    #[tokio::test]
    async fn test_symlink_creation() {
        let fs = create_test_fs().await;

        let target = b"/path/to/target";
        let (link_id, fattr) = fs
            .symlink(
                &test_auth(),
                0,
                &filename(b"mylink"),
                &filename(target),
                &sattr3::default(),
            )
            .await
            .unwrap();

        assert!(matches!(fattr.ftype, ftype3::NF3LNK));
        assert_eq!(fattr.size, target.len() as u64);

        let read_target = fs.readlink(&test_auth(), link_id).await.unwrap();
        assert_eq!(&read_target.0, target);
    }

    #[tokio::test]
    async fn test_truncate_with_setattr() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"Hello, World!")
            .await
            .unwrap();

        let fattr = fs.getattr(&test_auth(), file_id).await.unwrap();
        assert_eq!(fattr.size, 13);

        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(5),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert_eq!(fattr.size, 5);

        let (data, _) = fs.read(&test_auth(), file_id, 0, 10).await.unwrap();
        assert_eq!(&data, b"Hello");

        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(10),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let fattr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert_eq!(fattr.size, 10);

        let (data, _) = fs.read(&test_auth(), file_id, 0, 10).await.unwrap();
        assert_eq!(&data, b"Hello\0\0\0\0\0");
    }

    #[tokio::test]
    async fn test_sticky_bit_deletion() {
        let fs = create_test_fs().await;

        let (tmp_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"tmp"), &sattr3::default())
            .await
            .unwrap();

        let setattr = sattr3 {
            mode: set_mode3::mode(0o1777),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };
        fs.setattr(&test_auth(), tmp_id, setattr).await.unwrap();

        fs.create(&test_auth(), tmp_id, &filename(b"file"), sattr3::default())
            .await
            .unwrap();

        fs.remove(&test_auth(), tmp_id, &filename(b"file"))
            .await
            .unwrap();

        let result = fs.lookup(&test_auth(), tmp_id, &filename(b"file")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_set_times_permissions() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let server_time_setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::SET_TO_SERVER_TIME,
            mtime: set_mtime::SET_TO_SERVER_TIME,
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, server_time_setattr)
            .await
            .unwrap();
        assert!(fattr.atime.seconds > 0);
        assert!(fattr.mtime.seconds > 0);

        let client_time = nfstime3 {
            seconds: 1234567890,
            nseconds: 123456789,
        };

        let client_time_setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::SET_TO_CLIENT_TIME(client_time.clone()),
            mtime: set_mtime::SET_TO_CLIENT_TIME(client_time.clone()),
        };

        let fattr = fs
            .setattr(&test_auth(), file_id, client_time_setattr)
            .await
            .unwrap();
        assert_eq!(fattr.atime.seconds, client_time.seconds);
        assert_eq!(fattr.atime.nseconds, client_time.nseconds);
        assert_eq!(fattr.mtime.seconds, client_time.seconds);
        assert_eq!(fattr.mtime.nseconds, client_time.nseconds);
    }

    #[tokio::test]
    async fn test_create_exclusive() {
        let fs = create_test_fs().await;

        let filename_bytes = &filename(b"exclusive.txt");

        let file_id = fs
            .create_exclusive(&test_auth(), 0, filename_bytes)
            .await
            .unwrap();

        assert!(file_id > 0);

        let result = fs
            .create_exclusive(&test_auth(), 0, &filename(b"exclusive.txt"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_readdir_basic() {
        let fs = create_test_fs().await;

        let (dir_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"testdir"), &sattr3::default())
            .await
            .unwrap();

        for i in 0..10 {
            let name = format!("file{}", i);
            fs.create(
                &test_auth(),
                dir_id,
                &filename(name.as_bytes()),
                sattr3::default(),
            )
            .await
            .unwrap();
        }

        let mut entries = Vec::new();
        let mut start_after = 0;
        loop {
            let result = fs
                .readdir(&test_auth(), dir_id, start_after, 5)
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
            .map(|e| String::from_utf8_lossy(&e.name.0).to_string())
            .collect();
        assert!(names.contains(&".".to_string()));
        assert!(names.contains(&"..".to_string()));
        for i in 0..10 {
            assert!(names.contains(&format!("file{}", i)));
        }
    }

    #[tokio::test]
    async fn test_rename_across_directories() {
        let fs = create_test_fs().await;

        let (dir1_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"dir1"), &sattr3::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"dir2"), &sattr3::default())
            .await
            .unwrap();

        let (file_id, _) = fs
            .create(
                &test_auth(),
                dir1_id,
                &filename(b"file.txt"),
                sattr3::default(),
            )
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"test content")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            dir1_id,
            &filename(b"file.txt"),
            dir2_id,
            &filename(b"file.txt"),
        )
        .await
        .unwrap();

        let result = fs
            .lookup(&test_auth(), dir1_id, &filename(b"file.txt"))
            .await;
        assert!(result.is_err());

        let found_id = fs
            .lookup(&test_auth(), dir2_id, &filename(b"file.txt"))
            .await
            .unwrap();
        assert_eq!(found_id, file_id);

        let (data, _) = fs.read(&test_auth(), found_id, 0, 100).await.unwrap();
        assert_eq!(&data, b"test content");

        let _dir1_attr = fs.getattr(&test_auth(), dir1_id).await.unwrap();
        let _dir2_attr = fs.getattr(&test_auth(), dir2_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_operations_edge_cases() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.bin"), sattr3::default())
            .await
            .unwrap();

        let chunk_size = 64 * 1024;
        let test_data: Vec<u8> = (0..chunk_size).map(|i| (i % 256) as u8).collect();

        fs.write(&test_auth(), file_id, 0, &test_data)
            .await
            .unwrap();

        let (data1, _) = fs
            .read(&test_auth(), file_id, 0, chunk_size as u32)
            .await
            .unwrap();
        assert_eq!(data1, test_data);

        let offset = chunk_size as u64 - 100;
        let (data2, _) = fs.read(&test_auth(), file_id, offset, 200).await.unwrap();
        assert_eq!(data2.len(), 100);
        assert_eq!(&data2[..], &test_data[offset as usize..]);

        let (data3, eof) = fs
            .read(&test_auth(), file_id, chunk_size as u64, 100)
            .await
            .unwrap();
        assert_eq!(data3.len(), 0);
        assert!(eof);
    }

    #[tokio::test]
    async fn test_chmod_setuid_setgid_sticky() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        let special_modes = [0o4755, 0o2755, 0o1755, 0o7755];
        for mode in special_modes.iter() {
            let setattr = sattr3 {
                mode: set_mode3::mode(*mode),
                uid: set_uid3::Void,
                gid: set_gid3::Void,
                size: set_size3::Void,
                atime: set_atime::DONT_CHANGE,
                mtime: set_mtime::DONT_CHANGE,
            };

            let new_attr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
            assert_eq!(new_attr.mode & 0o7777, *mode);
        }
    }

    #[tokio::test]
    async fn test_time_updates() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"Hello, World!")
            .await
            .unwrap();

        let initial_attr = fs.getattr(&test_auth(), file_id).await.unwrap();
        let initial_mtime = initial_attr.mtime.seconds;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::SET_TO_SERVER_TIME,
        };

        let new_attr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert!(
            new_attr.mtime.seconds >= initial_mtime,
            "mtime should be updated to server time"
        );
    }

    #[tokio::test]
    async fn test_create_with_specific_attributes() {
        let fs = create_test_fs().await;

        let attr = sattr3 {
            mode: set_mode3::mode(0o640),
            uid: set_uid3::uid(1001),
            gid: set_gid3::gid(1001),
            size: set_size3::Void,
            atime: set_atime::SET_TO_SERVER_TIME,
            mtime: set_mtime::SET_TO_SERVER_TIME,
        };

        let (_, fattr) = fs
            .create(&test_auth(), 0, &filename(b"test.txt"), attr)
            .await
            .unwrap();

        assert_eq!(fattr.mode & 0o777, 0o640);
        assert_eq!(fattr.uid, 1001);
        assert_eq!(fattr.gid, 1001);
    }

    #[tokio::test]
    async fn test_symlink_operations() {
        let fs = create_test_fs().await;

        let target = b"/nonexistent/path";
        let (link_id, _) = fs
            .symlink(
                &test_auth(),
                0,
                &filename(b"broken_link"),
                &filename(target),
                &sattr3::default(),
            )
            .await
            .unwrap();

        let target = fs.readlink(&test_auth(), link_id).await.unwrap();
        assert_eq!(&target.0, b"/nonexistent/path");
    }

    #[tokio::test]
    async fn test_sparse_file_operations() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"sparse.dat"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 100, b"Hello")
            .await
            .unwrap();

        let attr = fs.getattr(&test_auth(), file_id).await.unwrap();
        assert_eq!(attr.size, 105);

        let (data, _) = fs.read(&test_auth(), file_id, 0, 100).await.unwrap();
        assert_eq!(data.len(), 100);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.read(&test_auth(), file_id, 100, 5).await.unwrap();
        assert_eq!(&data, b"Hello");
    }

    #[tokio::test]
    async fn test_rename_replace_file() {
        let fs = create_test_fs().await;

        let (src_id, _) = fs
            .create(&test_auth(), 0, &filename(b"source.txt"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), src_id, 0, b"source content")
            .await
            .unwrap();

        let (_target_id, _) = fs
            .create(&test_auth(), 0, &filename(b"target.txt"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), _target_id, 0, b"target content")
            .await
            .unwrap();

        fs.rename(
            &test_auth(),
            0,
            &filename(b"source.txt"),
            0,
            &filename(b"target.txt"),
        )
        .await
        .unwrap();

        let result = fs.lookup(&test_auth(), 0, &filename(b"source.txt")).await;
        assert!(result.is_err());

        let new_id = fs
            .lookup(&test_auth(), 0, &filename(b"target.txt"))
            .await
            .unwrap();
        assert_eq!(new_id, src_id);

        let (data, _) = fs.read(&test_auth(), new_id, 0, 100).await.unwrap();
        assert_eq!(&data, b"source content");
    }

    #[tokio::test]
    async fn test_directory_attributes() {
        let fs = create_test_fs().await;

        let (dir_id, initial_attr) = fs
            .mkdir(&test_auth(), 0, &filename(b"testdir"), &sattr3::default())
            .await
            .unwrap();

        assert_eq!(initial_attr.nlink, 2);

        let setattr = sattr3 {
            mode: set_mode3::mode(0o700),
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::Void,
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let new_attr = fs.setattr(&test_auth(), dir_id, setattr).await.unwrap();
        assert_eq!(new_attr.mode & 0o777, 0o700);
        assert_eq!(new_attr.uid, 1000);
        assert_eq!(new_attr.gid, 1000);
    }

    #[tokio::test]
    async fn test_file_growth_and_truncation() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(&test_auth(), 0, &filename(b"growth.txt"), sattr3::default())
            .await
            .unwrap();

        fs.write(&test_auth(), file_id, 0, b"Hello, World!")
            .await
            .unwrap();

        let setattr = sattr3 {
            mode: set_mode3::Void,
            uid: set_uid3::Void,
            gid: set_gid3::Void,
            size: set_size3::size(100),
            atime: set_atime::DONT_CHANGE,
            mtime: set_mtime::DONT_CHANGE,
        };

        let attr_after = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
        assert_eq!(attr_after.size, 100);

        let (data, _) = fs.read(&test_auth(), file_id, 0, 13).await.unwrap();
        assert_eq!(&data, b"Hello, World!");

        let (data, _) = fs.read(&test_auth(), file_id, 13, 87).await.unwrap();
        assert_eq!(data.len(), 87);
        assert!(data.iter().all(|&b| b == 0));

        let (data, _) = fs.read(&test_auth(), file_id, 0, 13).await.unwrap();
        assert_eq!(&data, b"Hello, World!");
    }

    #[tokio::test]
    async fn test_directory_hierarchy() {
        let fs = create_test_fs().await;

        let (a_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"a"), &sattr3::default())
            .await
            .unwrap();
        let (b_id, _) = fs
            .mkdir(&test_auth(), a_id, &filename(b"b"), &sattr3::default())
            .await
            .unwrap();

        let result = fs.readdir(&test_auth(), 0, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name.0).to_string())
            .collect();
        assert!(names.contains(&"a".to_string()));

        let result = fs.readdir(&test_auth(), a_id, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name.0).to_string())
            .collect();
        assert!(names.contains(&"b".to_string()));

        let result = fs.readdir(&test_auth(), b_id, 0, 10).await.unwrap();
        let names: Vec<String> = result
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name.0).to_string())
            .collect();
        assert!(names.contains(&".".to_string()));
        assert!(names.contains(&"..".to_string()));
    }

    #[tokio::test]
    async fn test_directory_timestamps() {
        let fs = create_test_fs().await;

        let parent_attr_before = fs.getattr(&test_auth(), 0).await.unwrap();
        let mtime_before = parent_attr_before.mtime.seconds;

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        fs.create(
            &test_auth(),
            0,
            &filename(b"newfile.txt"),
            sattr3::default(),
        )
        .await
        .unwrap();

        let parent_attr_after = fs.getattr(&test_auth(), 0).await.unwrap();
        assert!(
            parent_attr_after.mtime.seconds >= mtime_before,
            "Parent directory mtime should be updated when a file is created"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let (_dir_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"testdir"), &sattr3::default())
            .await
            .unwrap();

        let parent_attr_final = fs.getattr(&test_auth(), 0).await.unwrap();
        assert!(
            parent_attr_final.mtime.seconds >= parent_attr_after.mtime.seconds,
            "Parent directory mtime should be updated when a directory is created"
        );
    }

    #[tokio::test]
    async fn test_hardlink_both_names_visible() {
        let fs = create_test_fs().await;

        // Create file 'a'
        let (file_a_id, _) = fs
            .create(&test_auth(), 0, &filename(b"a"), sattr3::default())
            .await
            .unwrap();

        // Create hard link 'b' pointing to same inode as 'a'
        fs.link(&test_auth(), file_a_id, 0, &filename(b"b"))
            .await
            .unwrap();

        // Both 'a' and 'b' should be visible in directory listing
        let entries = fs.readdir(&test_auth(), 0, 0, 100).await.unwrap();

        let names: Vec<String> = entries
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(&e.name.0).to_string())
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
        let a_id = fs.lookup(&test_auth(), 0, &filename(b"a")).await.unwrap();
        let b_id = fs.lookup(&test_auth(), 0, &filename(b"b")).await.unwrap();
        assert_eq!(a_id, b_id, "Both names should point to the same inode");

        // Check link count
        let fattr = fs.getattr(&test_auth(), a_id).await.unwrap();
        assert_eq!(fattr.nlink, 2, "Link count should be 2");
    }

    #[tokio::test]
    async fn test_chmod_special_bits() {
        let fs = create_test_fs().await;

        let (file_id, _) = fs
            .create(
                &test_auth(),
                0,
                &filename(b"special.txt"),
                sattr3::default(),
            )
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
            let setattr = sattr3 {
                mode: set_mode3::mode(mode),
                uid: set_uid3::Void,
                gid: set_gid3::Void,
                size: set_size3::Void,
                atime: set_atime::DONT_CHANGE,
                mtime: set_mtime::DONT_CHANGE,
            };

            let new_attr = fs.setattr(&test_auth(), file_id, setattr).await.unwrap();
            assert_eq!(
                new_attr.mode & 0o7777,
                mode,
                "Mode should be {} for {}",
                mode,
                description
            );
        }
    }

    #[tokio::test]
    async fn test_rename_directory_with_contents() {
        let fs = create_test_fs().await;

        let (dir1_id, _) = fs
            .mkdir(&test_auth(), 0, &filename(b"dir1"), &sattr3::default())
            .await
            .unwrap();
        let (dir2_id, _) = fs
            .mkdir(
                &test_auth(),
                dir1_id,
                &filename(b"dir2"),
                &sattr3::default(),
            )
            .await
            .unwrap();
        let (dir3_id, _) = fs
            .mkdir(
                &test_auth(),
                dir2_id,
                &filename(b"dir3"),
                &sattr3::default(),
            )
            .await
            .unwrap();

        fs.create(
            &test_auth(),
            dir3_id,
            &filename(b"file.txt"),
            sattr3::default(),
        )
        .await
        .unwrap();

        fs.rename(
            &test_auth(),
            dir2_id,
            &filename(b"dir3"),
            0,
            &filename(b"moved_dir3"),
        )
        .await
        .unwrap();

        let found_id = fs
            .lookup(&test_auth(), 0, &filename(b"moved_dir3"))
            .await
            .unwrap();
        assert_eq!(found_id, dir3_id);

        let found_file = fs
            .lookup(&test_auth(), dir3_id, &filename(b"file.txt"))
            .await
            .unwrap();
        assert!(found_file > 0);
    }
}
