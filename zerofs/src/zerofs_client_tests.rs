//! Integration tests for `zerofs-client` against the real ZeroFS 9P server
//! (in-process, over a unix socket, backed by an in-memory filesystem).

use crate::fs::ZeroFS;
use crate::ninep::NinePServer;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zerofs_client::{
    Client, ConnectOptions, FileType, NodeKind, OpenOptions, SetAttrs, SetTime, Timestamp,
    ZeroFsError,
};

fn start_server(fs: Arc<ZeroFS>, sock: std::path::PathBuf) -> CancellationToken {
    let server = NinePServer::new_unix(fs, sock);
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = server.start(server_shutdown).await;
    });
    shutdown
}

async fn connect_with_retry(sock: &std::path::Path) -> Arc<Client> {
    let target = format!("unix:{}", sock.display());
    for _ in 0..100 {
        if let Ok(c) = Client::connect(&target).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("client failed to connect");
}

async fn connect_opts_with_retry(sock: &std::path::Path, opts: ConnectOptions) -> Arc<Client> {
    let target = format!("unix:{}", sock.display());
    for _ in 0..100 {
        if let Ok(c) = Client::connect_with(&target, opts.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("client failed to connect with options");
}

async fn setup() -> (Arc<Client>, CancellationToken, tempfile::TempDir) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("test.9p.sock");
    let shutdown = start_server(fs, sock.clone());
    let client = connect_with_retry(&sock).await;
    (client, shutdown, dir)
}

/// Outstanding fid count once the janitor has quiesced: poll until two reads
/// ~10ms apart agree, so a leak-test baseline isn't inflated by fids the setup
/// just freed that the background janitor hasn't reclaimed yet (which would let
/// a small constant leak slip under a `<= baseline` assertion).
async fn quiesced_fids(fs: &Client) -> usize {
    let mut prev = fs.outstanding_fids();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let now = fs.outstanding_fids();
        if now == prev {
            return now;
        }
        prev = now;
    }
    prev
}

#[tokio::test]
async fn write_read_roundtrip() {
    let (fs, _shutdown, _dir) = setup().await;

    assert!(!fs.exists("/hello.txt").await.unwrap());
    fs.write("/hello.txt", b"hello from zerofs").await.unwrap();
    assert!(fs.exists("/hello.txt").await.unwrap());

    assert_eq!(
        &fs.read("/hello.txt").await.unwrap()[..],
        b"hello from zerofs"
    );
    assert_eq!(
        &fs.read_range("/hello.txt", 6, 4).await.unwrap()[..],
        b"from"
    );
    // Reading past EOF is a short (empty) read, not an error.
    assert_eq!(
        &fs.read_range("/hello.txt", 1000, 10).await.unwrap()[..],
        b""
    );

    let meta = fs.stat("/hello.txt").await.unwrap();
    assert_eq!(meta.size, 17);
    assert!(meta.is_file());
    assert_eq!(meta.permissions(), 0o644);

    // write() truncates an existing file.
    fs.write("/hello.txt", b"shorter").await.unwrap();
    assert_eq!(&fs.read("/hello.txt").await.unwrap()[..], b"shorter");
}

#[tokio::test]
async fn append_offsets_and_content() {
    let (fs, _shutdown, _dir) = setup().await;

    // append creates the file if missing.
    assert_eq!(fs.append("/log.txt", b"one\n").await.unwrap(), 0);
    assert_eq!(fs.append("/log.txt", b"two\n").await.unwrap(), 4);
    assert_eq!(&fs.read("/log.txt").await.unwrap()[..], b"one\ntwo\n");
}

#[tokio::test]
async fn large_file_chunks_across_msize() {
    let (fs, _shutdown, _dir) = setup().await;

    let caps = fs.capabilities();
    assert!(caps.extensions_v1);
    assert!(caps.extensions_v2);

    // Cover the read/write chunking paths: 2.5x the largest single-message
    // payload, with a recognizable pattern.
    let len = (caps.max_write_chunk as usize) * 5 / 2;
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    fs.write("/big.bin", &data).await.unwrap();

    let back = fs.read("/big.bin").await.unwrap();
    assert_eq!(back.len(), data.len());
    assert_eq!(back, data);
}

#[tokio::test]
async fn open_options_semantics() {
    let (fs, _shutdown, _dir) = setup().await;

    // create_new is exclusive.
    let f = fs
        .open("/excl.txt", OpenOptions::write_only().create_new(true))
        .await
        .unwrap();
    f.write_at(0, b"first").await.unwrap();
    f.close().await;
    let err = fs
        .open("/excl.txt", OpenOptions::write_only().create_new(true))
        .await
        .unwrap_err();
    assert!(matches!(err, ZeroFsError::AlreadyExists { .. }), "{err}");

    // Plain open of a missing file is NotFound; with create it succeeds.
    let err = fs
        .open("/missing.txt", OpenOptions::read_only())
        .await
        .unwrap_err();
    assert!(matches!(err, ZeroFsError::NotFound { .. }), "{err}");

    // truncate empties a pre-existing file on open.
    let f = fs
        .open("/excl.txt", OpenOptions::write_only().truncate(true))
        .await
        .unwrap();
    f.close().await;
    assert_eq!(fs.stat("/excl.txt").await.unwrap().size, 0);

    // Opening a directory for writing is refused client-side.
    fs.create_dir("/adir", 0o755).await.unwrap();
    let err = fs
        .open("/adir", OpenOptions::write_only())
        .await
        .unwrap_err();
    assert!(matches!(err, ZeroFsError::IsADirectory { .. }), "{err}");
}

#[tokio::test]
async fn file_handle_positioned_io() {
    let (fs, _shutdown, _dir) = setup().await;

    let f = fs.create("/data.bin").await.unwrap();
    f.write_at(0, b"0123456789").await.unwrap();
    f.write_at(4, b"XY").await.unwrap();
    assert_eq!(&f.read_at(0, 10).await.unwrap()[..], b"0123XY6789");
    assert_eq!(&f.read_at(8, 100).await.unwrap()[..], b"89");

    assert_eq!(f.metadata().await.unwrap().size, 10);
    f.set_len(4).await.unwrap();
    assert_eq!(f.metadata().await.unwrap().size, 4);

    f.sync_data().await.unwrap();
    f.sync_all().await.unwrap();
    f.close().await;

    // Closed handle: every method reports Closed; close is idempotent.
    let err = f.read_at(0, 1).await.unwrap_err();
    assert!(matches!(err, ZeroFsError::Closed), "{err}");
    f.close().await;
}

#[tokio::test]
async fn directories_and_listing() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir_all("/a/b/c", 0o755).await.unwrap();
    // Idempotent over existing prefixes.
    fs.create_dir_all("/a/b/c", 0o755).await.unwrap();
    let err = fs.create_dir("/a/b", 0o755).await.unwrap_err();
    assert!(matches!(err, ZeroFsError::AlreadyExists { .. }), "{err}");

    fs.write("/a/one.txt", b"1").await.unwrap();
    fs.write("/a/two.txt", b"22").await.unwrap();

    let mut entries = fs.read_dir("/a").await.unwrap();
    entries.sort_by(|x, y| x.name.cmp(&y.name));
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, ["b", "one.txt", "two.txt"]);
    // readdirplus carries inline metadata on this server.
    assert_eq!(entries[0].file_type, FileType::Dir);
    let two = entries.iter().find(|e| e.name == "two.txt").unwrap();
    assert_eq!(two.metadata.as_ref().unwrap().size, 2);

    // Incremental listing with a cap, then rewind and drain.
    let dir = fs.open_dir("/a").await.unwrap();
    let first = dir.next_batch(Some(2)).await.unwrap();
    assert_eq!(first.len(), 2);
    let rest = dir.next_batch(Some(100)).await.unwrap();
    assert_eq!(rest.len(), 1);
    assert!(dir.next_batch(None).await.unwrap().is_empty());
    dir.rewind().await.unwrap();
    assert_eq!(dir.next_batch(None).await.unwrap().len(), 3);
    dir.close().await;

    let err = fs.open_dir("/a/one.txt").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotADirectory { .. }), "{err}");
}

#[tokio::test]
async fn non_utf8_names_via_dir_at() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir("/bytes", 0o755).await.unwrap();
    let dir = fs.open_dir("/bytes").await.unwrap();

    let weird: &[u8] = b"\xff\xfe-not-utf8";
    let f = dir
        .open_at(weird, OpenOptions::write_only().create_new(true))
        .await
        .unwrap();
    f.write_at(0, b"payload").await.unwrap();
    f.close().await;

    // The listing surfaces both the lossy display name and the exact bytes.
    let entries = dir.next_batch(None).await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert!(!entry.name_is_utf8);
    assert_eq!(entry.name_bytes, weird);

    // The bytes round-trip into every *_at operation.
    let meta = dir.metadata_at(&entry.name_bytes).await.unwrap();
    assert_eq!(meta.size, 7);
    let f = dir
        .open_at(&entry.name_bytes, OpenOptions::read_only())
        .await
        .unwrap();
    assert_eq!(&f.read_at(0, 100).await.unwrap()[..], b"payload");
    f.close().await;

    // Paths are bytes end-to-end: the same entry is addressable through the
    // plain path API with an OsStr built from raw bytes; no separate "raw"
    // API needed.
    use std::os::unix::ffi::OsStrExt;
    let byte_path = std::path::Path::new(std::ffi::OsStr::from_bytes(b"/bytes/\xff\xfe-not-utf8"));
    assert_eq!(fs.stat(byte_path).await.unwrap().size, 7);
    assert_eq!(&fs.read(byte_path).await.unwrap()[..], b"payload");
    assert_eq!(fs.canonicalize(byte_path).await.unwrap(), byte_path);

    // Byte-exact symlink target, readable back verbatim.
    dir.symlink_at(b"link", b"\xfftarget").await.unwrap();
    assert_eq!(&dir.read_link_at(b"link").await.unwrap()[..], b"\xfftarget");

    dir.remove_file_at(b"link").await.unwrap();
    dir.remove_file_at(weird).await.unwrap();
    assert!(
        dir.next_batch(None).await.unwrap().is_empty() || {
            dir.rewind().await.unwrap();
            dir.next_batch(None).await.unwrap().is_empty()
        }
    );
    dir.close().await;
}

#[tokio::test]
async fn dir_at_suite_create_rename_remove() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir_all("/x/y", 0o755).await.unwrap();
    let x = fs.open_dir("/x").await.unwrap();
    let y = fs.open_dir("/x/y").await.unwrap();

    let meta = x.create_dir_at(b"sub", 0o700).await.unwrap();
    assert!(meta.is_dir());
    assert_eq!(meta.permissions(), 0o700);

    fs.write("/x/file.txt", b"move me").await.unwrap();
    // Same-dir rename, then cross-dir rename.
    x.rename_at(b"file.txt", &x, b"renamed.txt").await.unwrap();
    x.rename_at(b"renamed.txt", &y, b"arrived.txt")
        .await
        .unwrap();
    assert_eq!(&fs.read("/x/y/arrived.txt").await.unwrap()[..], b"move me");

    // Hard link across directories via link_at; nlink reflects it.
    let meta = x.link_at(&y, b"arrived.txt", b"hard.txt").await.unwrap();
    assert_eq!(meta.nlink, 2);
    assert_eq!(&fs.read("/x/hard.txt").await.unwrap()[..], b"move me");

    // set_attr_at on a child without opening it.
    let meta = x
        .set_attr_at(
            b"hard.txt",
            SetAttrs {
                mode: Some(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(meta.permissions(), 0o600);

    let sub = x.open_dir_at(b"sub").await.unwrap();
    assert!(sub.next_batch(None).await.unwrap().is_empty());
    sub.close().await;
    x.remove_dir_at(b"sub").await.unwrap();

    let err = x.open_dir_at(b"hard.txt").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotADirectory { .. }), "{err}");

    x.close().await;
    y.close().await;
}

#[tokio::test]
async fn symlink_resolution() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir_all("/releases/v7", 0o755).await.unwrap();
    fs.write("/releases/v7/app.conf", b"conf").await.unwrap();
    fs.symlink("/releases/v7", "/current").await.unwrap();

    // stat is literal everywhere: the link itself stats as a symlink, and a
    // path THROUGH it fails NotADirectory.
    assert!(fs.stat("/current").await.unwrap().is_symlink());
    let err = fs.stat("/current/app.conf").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotADirectory { .. }), "{err}");

    // metadata and canonicalize resolve, including intermediate components.
    assert!(fs.metadata("/current").await.unwrap().is_dir());
    let meta = fs.metadata("/current/app.conf").await.unwrap();
    assert_eq!(meta.size, 4);
    assert_eq!(
        fs.canonicalize("/current/app.conf").await.unwrap(),
        std::path::Path::new("/releases/v7/app.conf")
    );
    assert_eq!(
        &fs.read(&fs.canonicalize("/current/app.conf").await.unwrap())
            .await
            .unwrap()[..],
        b"conf"
    );

    // Relative targets with `..` resolve against the link's parent.
    fs.create_dir("/releases/v7/etc", 0o755).await.unwrap();
    fs.symlink("../../other", "/releases/v7/back")
        .await
        .unwrap();
    fs.create_dir("/other", 0o755).await.unwrap();
    fs.write("/other/marker", b"m").await.unwrap();
    assert_eq!(
        fs.canonicalize("/releases/v7/back/marker").await.unwrap(),
        std::path::Path::new("/other/marker")
    );

    assert_eq!(
        fs.read_link("/current").await.unwrap(),
        std::path::Path::new("/releases/v7")
    );

    // A cycle trips the hop cap.
    fs.symlink("/loop-b", "/loop-a").await.unwrap();
    fs.symlink("/loop-a", "/loop-b").await.unwrap();
    let err = fs.metadata("/loop-a").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::TooManySymlinks { .. }), "{err}");
}

#[tokio::test]
async fn rename_remove_and_recursive_delete() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir_all("/tree/sub/deep", 0o755).await.unwrap();
    fs.write("/tree/f1", b"1").await.unwrap();
    fs.write("/tree/sub/f2", b"2").await.unwrap();
    fs.write("/tree/sub/deep/f3", b"3").await.unwrap();

    fs.rename("/tree/f1", "/tree/sub/f1-moved").await.unwrap();
    assert!(!fs.exists("/tree/f1").await.unwrap());
    assert_eq!(&fs.read("/tree/sub/f1-moved").await.unwrap()[..], b"1");

    let err = fs.remove_dir("/tree/sub").await.unwrap_err();
    assert!(
        matches!(err, ZeroFsError::DirectoryNotEmpty { .. }),
        "{err}"
    );

    fs.remove_dir_all("/tree").await.unwrap();
    assert!(!fs.exists("/tree").await.unwrap());

    let err = fs.remove_file("/tree/f1").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotFound { .. }), "{err}");
}

#[tokio::test]
async fn metadata_operations() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.write("/meta.txt", b"0123456789").await.unwrap();

    let meta = fs.chmod("/meta.txt", 0o600).await.unwrap();
    assert_eq!(meta.permissions(), 0o600);

    // chown to our own identity is permitted and reflected.
    let own = fs.stat("/meta.txt").await.unwrap();
    let meta = fs
        .chown("/meta.txt", Some(own.uid), Some(own.gid))
        .await
        .unwrap();
    assert_eq!((meta.uid, meta.gid), (own.uid, own.gid));

    let meta = fs.truncate("/meta.txt", 4).await.unwrap();
    assert_eq!(meta.size, 4);

    let mtime = SetTime::At {
        time: Timestamp {
            secs: 1_700_000_000,
            nanos: 42,
        },
    };
    let meta = fs.set_times("/meta.txt", None, Some(mtime)).await.unwrap();
    assert_eq!(meta.mtime.secs, 1_700_000_000);

    let meta = fs
        .set_times("/meta.txt", Some(SetTime::Now), None)
        .await
        .unwrap();
    assert!(meta.atime.secs >= 1_700_000_000);
}

#[tokio::test]
async fn statfs_sync_and_mknod() {
    let (fs, _shutdown, _dir) = setup().await;

    let sf = fs.statfs().await.unwrap();
    assert!(sf.block_size > 0);
    assert!(sf.max_name_len >= 255);

    fs.write("/durable.txt", b"x").await.unwrap();
    fs.sync().await.unwrap();

    let meta = fs.mknod("/fifo", NodeKind::Fifo, 0o644).await.unwrap();
    assert_eq!(meta.file_type, FileType::Fifo);
    let entries = fs.read_dir("/").await.unwrap();
    let fifo = entries.iter().find(|e| e.name == "fifo").unwrap();
    assert_eq!(fifo.file_type, FileType::Fifo);
}

#[tokio::test]
async fn path_validation_and_errors() {
    let (fs, _shutdown, _dir) = setup().await;

    let err = fs.read("/missing").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotFound { .. }), "{err}");
    assert_eq!(err.to_errno(), libc::ENOENT);

    let err = fs.read("/a/../b").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::InvalidArgument { .. }), "{err}");

    let long = "x".repeat(256);
    let err = fs.read(&format!("/{long}")).await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NameTooLong { .. }), "{err}");

    // A path through a regular file is NotADirectory.
    fs.write("/plain", b"f").await.unwrap();
    let err = fs.stat("/plain/below").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotADirectory { .. }), "{err}");

    // Operating on the attach root where a name is required.
    let err = fs.remove_file("/").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::InvalidArgument { .. }), "{err}");
    let err = fs.remove_dir_all("/").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::InvalidArgument { .. }), "{err}");

    // Leading-slash-optional and redundant separators are tolerated.
    fs.write("relative.txt", b"r").await.unwrap();
    assert_eq!(&fs.read("//relative.txt").await.unwrap()[..], b"r");
    assert_eq!(&fs.read("/./relative.txt").await.unwrap()[..], b"r");
}

#[tokio::test]
async fn close_semantics_and_handle_independence() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.write("/keep.txt", b"alive").await.unwrap();
    let f = fs
        .open("/keep.txt", OpenOptions::read_only())
        .await
        .unwrap();

    fs.close().await;
    fs.close().await; // idempotent

    let err = fs.read("/keep.txt").await.unwrap_err();
    assert!(matches!(err, ZeroFsError::Closed), "{err}");

    // Outstanding handles keep working after Client::close.
    assert_eq!(&f.read_at(0, 100).await.unwrap()[..], b"alive");
    f.close().await;
}

#[tokio::test]
async fn cancelled_futures_leak_nothing() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.write("/c.txt", b"cancel me").await.unwrap();
    let baseline = quiesced_fids(&fs).await;

    // Drop public futures at their first await point; the janitor reclaims
    // any fids they held, and the session keeps working.
    for _ in 0..50 {
        let read = fs.read("/c.txt");
        let stat = fs.stat("/c.txt");
        let write = fs.write("/c2.txt", b"x");
        let list = fs.read_dir("/");
        // The create-family ops allocate a guarded fid before their first await
        // (mkdir/symlink/...), so cancelling them must also reclaim that fid.
        let mkdir = fs.create_dir("/cdir", 0o755);
        let symlink = fs.symlink("/c.txt", "/clink");
        drop(
            tokio::time::timeout(Duration::ZERO, async {
                let _ = tokio::join!(read, stat, write, list, mkdir, symlink);
            })
            .await,
        );
    }

    // The janitor reclaims asynchronously, so poll: outstanding fids must return
    // to the pre-churn baseline rather than grow with the 50 rounds.
    let mut drained = false;
    for _ in 0..200 {
        if fs.outstanding_fids() <= baseline {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        drained,
        "fids leaked: {} outstanding, baseline {baseline}",
        fs.outstanding_fids()
    );

    // Everything still functions after the churn.
    assert_eq!(&fs.read("/c.txt").await.unwrap()[..], b"cancel me");
    fs.write("/after.txt", b"ok").await.unwrap();
    assert_eq!(&fs.read("/after.txt").await.unwrap()[..], b"ok");
}

#[tokio::test]
async fn handle_fids_are_recycled() {
    let (fs, _shutdown, _dir) = setup().await;
    fs.write("/r.txt", b"x").await.unwrap();
    let baseline = quiesced_fids(&fs).await;

    // Open and release many handles (close + drop, and drop alone). Their fid
    // numbers must be recycled, not consumed permanently, or a long-running
    // client eventually exhausts the 32-bit fid space.
    for _ in 0..300 {
        let f = fs.open("/r.txt", OpenOptions::read_only()).await.unwrap();
        f.read_at(0, 1).await.unwrap();
        f.close().await;
        drop(f);

        let d = fs.open_dir("/").await.unwrap();
        d.next_batch(None).await.unwrap();
        drop(d); // dropped without an explicit close
    }

    // Recycling is asynchronous (the janitor clunks first), so poll: outstanding
    // fids must return to the pre-loop baseline, not grow with the 300 rounds.
    let mut recycled = false;
    for _ in 0..200 {
        if fs.outstanding_fids() <= baseline {
            recycled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        recycled,
        "handle fids not recycled: {} outstanding, baseline {baseline}",
        fs.outstanding_fids()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_close_does_not_corrupt_or_leak() {
    let (fs, _shutdown, _dir) = setup().await;
    fs.write("/s.txt", b"hello").await.unwrap();
    let baseline = quiesced_fids(&fs).await;

    // Race close() against reads on a shared handle. close() only flips the
    // closed flag; the fid is clunked and recycled at Drop, once both tasks and
    // the last Arc are gone (a read borrows &self, so the fid cannot recycle
    // while it is in flight). So a racing read returns the bytes or sees Closed,
    // never wrong bytes; afterward the fid recycles.
    for _ in 0..100 {
        let f = fs.open("/s.txt", OpenOptions::read_only()).await.unwrap();
        let reader = {
            let f = Arc::clone(&f);
            tokio::spawn(async move {
                match f.read_at(0, 5).await {
                    Ok(bytes) => assert_eq!(&bytes[..], b"hello"),
                    Err(e) => assert!(matches!(e, ZeroFsError::Closed), "unexpected: {e}"),
                }
            })
        };
        let closer = {
            let f = Arc::clone(&f);
            tokio::spawn(async move {
                f.close().await;
            })
        };
        reader.await.unwrap();
        closer.await.unwrap();
        drop(f);
    }

    let mut recycled = false;
    for _ in 0..200 {
        if fs.outstanding_fids() <= baseline {
            recycled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        recycled,
        "fids not recycled after concurrent close/io: {} vs baseline {baseline}",
        fs.outstanding_fids()
    );
    // The session is unharmed.
    assert_eq!(&fs.read("/s.txt").await.unwrap()[..], b"hello");
}

#[tokio::test]
async fn concurrent_shared_client() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.create_dir("/conc", 0o755).await.unwrap();
    let mut handles = Vec::new();
    for i in 0..16 {
        let fs = Arc::clone(&fs);
        handles.push(tokio::spawn(async move {
            let path = format!("/conc/file-{i}");
            fs.write(&path, format!("payload-{i}").as_bytes())
                .await
                .unwrap();
            assert_eq!(
                fs.read(&path).await.unwrap(),
                format!("payload-{i}").as_bytes()
            );
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(fs.read_dir("/conc").await.unwrap().len(), 16);
}

#[tokio::test]
async fn file_cursor_async_io_and_seek() {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    let (fs, _shutdown, _dir) = setup().await;

    // Write through the cursor (AsyncWrite), spanning more than one msize so
    // the read side exercises chunked, partial-fill poll_read.
    let big: Vec<u8> = (0..(3 << 20)).map(|i| (i % 251) as u8).collect();
    let file = fs.create("/cursor.bin").await.unwrap();
    {
        let mut w = file.cursor();
        w.write_all(&big).await.unwrap();
        w.flush().await.unwrap();
    }

    // Seek + read it all back via AsyncRead.
    let mut r = file.cursor();
    assert_eq!(r.seek(std::io::SeekFrom::Start(0)).await.unwrap(), 0);
    let mut got = Vec::new();
    r.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, big);

    // SeekFrom::End resolves the size with a metadata round trip.
    let mut r2 = file.cursor();
    let pos = r2.seek(std::io::SeekFrom::End(-5)).await.unwrap();
    assert_eq!(pos, big.len() as u64 - 5);
    let mut tail = Vec::new();
    r2.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, &big[big.len() - 5..]);

    // SeekFrom::Current is relative to the cursor's own position.
    let mut r3 = file.cursor();
    r3.seek(std::io::SeekFrom::Start(10)).await.unwrap();
    assert_eq!(r3.seek(std::io::SeekFrom::Current(5)).await.unwrap(), 15);
    let mut five = [0u8; 5];
    r3.read_exact(&mut five).await.unwrap();
    assert_eq!(five, big[15..20]);

    // tokio::io::copy drives read+write across two independent cursors.
    let dst = fs.create("/cursor-copy.bin").await.unwrap();
    let mut src_cur = file.cursor();
    let mut dst_cur = dst.cursor();
    let n = tokio::io::copy(&mut src_cur, &mut dst_cur).await.unwrap();
    dst_cur.flush().await.unwrap();
    assert_eq!(n, big.len() as u64);
    assert_eq!(fs.read("/cursor-copy.bin").await.unwrap(), big);

    file.close().await;
    dst.close().await;
}

#[tokio::test]
async fn connect_with_options_identity_aname_and_msize() {
    let fs_be = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("opts.9p.sock");
    let shutdown = start_server(Arc::clone(&fs_be), sock.clone());

    // A default-identity client carves out a subtree.
    let admin = connect_with_retry(&sock).await;
    admin.create_dir("/vol", 0o777).await.unwrap();

    // Connect rooted at /vol (aname) as root with an explicit gid and a small
    // msize. Root avoids group-membership checks so the gid assertion is clean.
    let opts = ConnectOptions {
        uid: Some(0),
        gid: Some(4242),
        aname: "/vol".to_string(),
        msize: 256 * 1024,
        ..Default::default()
    };
    let confined = connect_opts_with_retry(&sock, opts).await;

    // The requested msize was honored downward by negotiation.
    let msize = confined.capabilities().msize;
    assert!((4096..=256 * 1024).contains(&msize), "msize = {msize}");

    // Writes land in the subtree and carry the configured gid.
    confined.write("/inside.txt", b"hi").await.unwrap();
    assert_eq!(confined.stat("/inside.txt").await.unwrap().gid, 4242);

    // The client is rooted at /vol: the file is /vol/inside.txt to admin, and
    // the real root's tree is not addressable from inside (no nested /vol).
    assert_eq!(&admin.read("/vol/inside.txt").await.unwrap()[..], b"hi");
    assert!(!confined.exists("/vol").await.unwrap());

    confined.close().await;
    shutdown.cancel();
}

#[tokio::test]
async fn connect_timeout_expires() {
    // A listener that accepts TCP but never speaks 9P: version negotiation
    // blocks, so a short connect_timeout_ms must surface as ConnectFailed
    // rather than hanging forever.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((s, _)) = listener.accept().await {
            held.push(s); // hold the connection open, send nothing
        }
    });

    let opts = ConnectOptions {
        connect_timeout_ms: Some(150),
        ..Default::default()
    };
    let err = Client::connect_with(&format!("tcp://{addr}"), opts)
        .await
        .unwrap_err();
    assert!(matches!(err, ZeroFsError::ConnectFailed { .. }), "{err}");
    assert_eq!(err.to_errno(), libc::EIO);
}

#[tokio::test]
async fn error_converts_to_io_error_losslessly() {
    let (fs, _shutdown, _dir) = setup().await;

    // A server errno maps to an io::Error with the matching ErrorKind, and the
    // original ZeroFsError is recoverable from the source (downcast); so the
    // exact errno survives via the preserved error even for the Io catch-all.
    let err = fs.read("/nope").await.unwrap_err();
    let io_err: std::io::Error = err.into();
    assert_eq!(io_err.kind(), std::io::ErrorKind::NotFound);
    let inner = io_err
        .get_ref()
        .unwrap()
        .downcast_ref::<ZeroFsError>()
        .expect("source is the original ZeroFsError");
    assert!(matches!(inner, ZeroFsError::NotFound { .. }));
    assert_eq!(inner.to_errno(), libc::ENOENT);

    // The `Io` catch-all carries a raw errno in a field (not a fixed mapping);
    // it must survive the io::Error round-trip too.
    let catchall = ZeroFsError::Io {
        errno: libc::EXDEV,
        path: "/x".to_string(),
        message: "cross-device".to_string(),
    };
    let want = catchall.to_errno();
    let io_err: std::io::Error = catchall.into();
    let inner = io_err
        .get_ref()
        .unwrap()
        .downcast_ref::<ZeroFsError>()
        .expect("source is the original ZeroFsError");
    assert!(matches!(inner, ZeroFsError::Io { errno, .. } if *errno == libc::EXDEV));
    assert_eq!(inner.to_errno(), want);
    assert_eq!(inner.to_errno(), libc::EXDEV);

    // `?` interop: a function returning io::Result can propagate client errors.
    async fn read_len(fs: &Client, path: &str) -> std::io::Result<usize> {
        Ok(fs.read(path).await?.len())
    }
    fs.write("/io.txt", b"abcd").await.unwrap();
    assert_eq!(read_len(&fs, "/io.txt").await.unwrap(), 4);
    assert_eq!(
        read_len(&fs, "/missing").await.unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn extreme_timestamps_decode_without_panic() {
    use std::time::SystemTime;
    // Out-of-range or un-normalized server timestamps must saturate, not panic.
    let _ = SystemTime::from(Timestamp {
        secs: i64::MAX,
        nanos: 2_000_000_000,
    });
    let _ = SystemTime::from(Timestamp {
        secs: i64::MIN,
        nanos: 999_999_999,
    });
    let _ = SystemTime::from(Timestamp {
        secs: 0,
        nanos: u32::MAX,
    });
}

#[tokio::test]
async fn dir_entries_stream() {
    use futures::StreamExt;

    let (fs, _shutdown, _dir) = setup().await;
    fs.create_dir("/s", 0o755).await.unwrap();
    for i in 0..5 {
        fs.write(format!("/s/f{i}"), b"x").await.unwrap();
    }

    let dir = fs.open_dir("/s").await.unwrap();
    let mut names: Vec<String> = dir
        .entries()
        .map(|e| e.unwrap().name)
        .collect::<Vec<_>>()
        .await;
    names.sort();
    assert_eq!(names, ["f0", "f1", "f2", "f3", "f4"]);
    dir.close().await;
}

#[tokio::test]
async fn serde_roundtrip_and_systemtime_conversions() {
    let (fs, _shutdown, _dir) = setup().await;

    fs.write("/s.txt", b"data").await.unwrap();
    let meta = fs.stat("/s.txt").await.unwrap();

    // Metadata serializes and deserializes back to an equal value.
    let json = serde_json::to_string(&meta).unwrap();
    let back: zerofs_client::Metadata = serde_json::from_str(&json).unwrap();
    assert_eq!(back.ino, meta.ino);
    assert_eq!(back.size, meta.size);
    assert_eq!(back.mtime, meta.mtime);

    // SystemTime <-> Timestamp round-trips, including a pre-epoch instant, and
    // SystemTime flows into set_times via SetTime.
    let when = std::time::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 250);
    let ts: Timestamp = when.into();
    assert_eq!(
        ts,
        Timestamp {
            secs: 1_700_000_000,
            nanos: 250
        }
    );
    assert_eq!(std::time::SystemTime::from(ts), when);

    let pre = std::time::UNIX_EPOCH - std::time::Duration::new(0, 500);
    assert_eq!(std::time::SystemTime::from(Timestamp::from(pre)), pre);

    let updated = fs
        .set_times("/s.txt", None, Some(SetTime::from(when)))
        .await
        .unwrap();
    assert_eq!(updated.mtime, ts);
}

#[tokio::test]
async fn create_dir_all_accepts_symlink_to_dir() {
    let (fs, _shutdown, _dir) = setup().await;
    fs.create_dir_all("/data/real", 0o755).await.unwrap();

    // A path whose final component is a symlink to a directory is accepted, as
    // std::fs::create_dir_all is (it resolves the link).
    fs.symlink("/data/real", "/data/link").await.unwrap();
    fs.create_dir_all("/data/link", 0o755).await.unwrap();

    // A symlink to a non-directory is still rejected.
    fs.write("/data/file", b"x").await.unwrap();
    fs.symlink("/data/file", "/data/badlink").await.unwrap();
    let err = fs.create_dir_all("/data/badlink", 0o755).await.unwrap_err();
    assert!(matches!(err, ZeroFsError::NotADirectory { .. }), "{err}");
}

#[tokio::test]
async fn set_times_rejects_pre_epoch() {
    let (fs, _shutdown, _dir) = setup().await;
    fs.write("/f", b"x").await.unwrap();

    // Pre-epoch seconds are unrepresentable on the unsigned setattr wire; reject
    // them rather than wrapping to a huge value.
    let pre = SetTime::At {
        time: Timestamp { secs: -1, nanos: 0 },
    };
    let err = fs.set_times("/f", None, Some(pre)).await.unwrap_err();
    assert!(matches!(err, ZeroFsError::InvalidArgument { .. }), "{err}");

    // A valid (post-epoch) time still applies.
    let ok = SetTime::At {
        time: Timestamp {
            secs: 1_000_000,
            nanos: 0,
        },
    };
    fs.set_times("/f", None, Some(ok)).await.unwrap();
}

#[tokio::test]
async fn read_zero_length_returns_empty() {
    let (fs, _shutdown, _dir) = setup().await;
    fs.write("/f", b"hello").await.unwrap();
    assert!(fs.read_range("/f", 0, 0).await.unwrap().is_empty());
}
