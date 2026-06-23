//! Op-id dedup: a retried mutating op carrying the same op-id replays the
//! original reply rather than double-applying or returning a spurious
//! EEXIST/ENOENT. In-process 9P server over a unix socket + in-memory fs + the
//! real ninep-client, isolating the dedup path (failover is a jepsen concern).

use crate::fs::ZeroFS;
use crate::ninep::NinePServer;
use ninep_client::{ClientError, NOFID, NinePClient};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const AT_REMOVEDIR: u32 = 0x200;
const O_CREAT_RDWR: u32 = 0x42; // O_CREAT | O_RDWR

fn is_errno(r: &Result<(), ClientError>, want: u32) -> bool {
    matches!(r, Err(ClientError::Errno(e)) if *e == want)
}

/// In-process 9P server + fresh in-memory fs; returns a connected, attached
/// client. `connect_unix` negotiates `.zerofs3`, so requests carry the op-id frame.
async fn setup() -> (Arc<NinePClient>, CancellationToken, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("dedup.9p.sock");
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let server = NinePServer::new_unix(fs, sock.clone());
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = server.start(server_shutdown).await;
    });

    let mut client = None;
    for _ in 0..100 {
        if let Ok(c) = NinePClient::connect_unix(&sock, 256 * 1024).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let client = client.expect("client failed to connect to the in-process 9P server");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");
    (client, shutdown, dir)
}

#[tokio::test]
async fn op_id_dedups_a_retried_mkdir() {
    let (client, _shutdown, _dir) = setup().await;

    let op_id = [42u8; 16];
    let qid1 = client
        .mkdir_op_id(1, b"dedup_dir", 0o755, 0, op_id)
        .await
        .expect("first mkdir");
    // Same op-id = a retry: deduplicated, not EEXIST.
    let qid2 = client
        .mkdir_op_id(1, b"dedup_dir", 0o755, 0, op_id)
        .await
        .expect("a retry with the same op-id must be deduplicated, not EEXIST");
    assert_eq!(
        qid1.path, qid2.path,
        "the deduplicated retry must return the original directory's qid"
    );
    // A DIFFERENT op-id must see EEXIST: dedup is op-id-specific, not a blanket
    // ignore-EEXIST.
    let err = client
        .mkdir_op_id(1, b"dedup_dir", 0o755, 0, [99u8; 16])
        .await;
    assert!(
        matches!(err, Err(ClientError::Errno(e)) if e == 17 /* EEXIST */),
        "a different op-id must not be deduplicated; expected EEXIST, got {err:?}"
    );
}

#[tokio::test]
async fn op_id_dedups_symlink_unlink_and_rename() {
    let (client, _shutdown, _dir) = setup().await;

    // symlink: matching-op-id retry dedups; different op-id sees EEXIST.
    let sx = [10u8; 16];
    client
        .symlink_op_id(1, b"sym", b"target", 0, sx)
        .await
        .expect("symlink");
    client
        .symlink_op_id(1, b"sym", b"target", 0, sx)
        .await
        .expect("symlink retry with same op-id must dedup, not EEXIST");
    assert!(
        is_errno(
            &client
                .symlink_op_id(1, b"sym", b"target", 0, [11u8; 16])
                .await
                .map(|_| ()),
            17
        ),
        "symlink with a different op-id must see EEXIST"
    );

    // unlink (rmdir): matching-op-id retry dedups; different op-id sees ENOENT.
    client
        .mkdir_op_id(1, b"d", 0o755, 0, [20u8; 16])
        .await
        .expect("mkdir d");
    let ux = [21u8; 16];
    client
        .unlinkat_op_id(1, b"d", AT_REMOVEDIR, ux)
        .await
        .expect("rmdir");
    client
        .unlinkat_op_id(1, b"d", AT_REMOVEDIR, ux)
        .await
        .expect("unlink retry with same op-id must dedup, not ENOENT");
    assert!(
        is_errno(
            &client
                .unlinkat_op_id(1, b"d", AT_REMOVEDIR, [22u8; 16])
                .await,
            2
        ),
        "unlink with a different op-id must see ENOENT once gone"
    );

    // rename: matching-op-id retry dedups; different op-id sees ENOENT.
    client
        .mkdir_op_id(1, b"a", 0o755, 0, [30u8; 16])
        .await
        .expect("mkdir a");
    let rx = [31u8; 16];
    client
        .renameat_op_id(1, b"a", 1, b"b", rx)
        .await
        .expect("rename a->b");
    client
        .renameat_op_id(1, b"a", 1, b"b", rx)
        .await
        .expect("rename retry with same op-id must dedup, not ENOENT");
    assert!(
        is_errno(
            &client.renameat_op_id(1, b"a", 1, b"b", [32u8; 16]).await,
            2
        ),
        "rename with a different op-id must see ENOENT once the source is gone"
    );
}

#[tokio::test]
async fn op_id_dedups_lcreate() {
    let (client, _shutdown, _dir) = setup().await;

    let cx = [40u8; 16];
    client
        .lcreateattr_op_id(1, 2, b"f", O_CREAT_RDWR, 0o644, 0, cx)
        .await
        .expect("lcreate");
    // Same-op-id retry on a fresh newfid re-opens the existing file, not EEXIST.
    client
        .lcreateattr_op_id(1, 3, b"f", O_CREAT_RDWR, 0o644, 0, cx)
        .await
        .expect("lcreate retry with same op-id must dedup, not EEXIST");
    // Different op-id is a genuine create of an existing name -> EEXIST.
    let e = client
        .lcreateattr_op_id(1, 4, b"f", O_CREAT_RDWR, 0o644, 0, [41u8; 16])
        .await;
    assert!(
        matches!(e, Err(ClientError::Errno(c)) if c == 17 /* EEXIST */),
        "a different op-id must see EEXIST, got {e:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn op_id_dedups_concurrent_retries() {
    let (client, _shutdown, _dir) = setup().await;

    // Racing retries (same op-id): the single-flight gate serializes them so
    // every one returns the deduped reply.
    let op_id = [77u8; 16];
    let mut handles = Vec::new();
    for _ in 0..16 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            c.mkdir_op_id(1, b"cdir", 0o755, 0, op_id).await
        }));
    }
    for h in handles {
        h.await
            .unwrap()
            .expect("every concurrent retry of the same op-id must succeed (deduped)");
    }
}
