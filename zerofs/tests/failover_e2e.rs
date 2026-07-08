//! Real-process CLIENT failover: a leader + standby `zerofs run` over ONE
//! file:// store, exercising client re-routing across a failover (the RPC
//! FailoverClient and the multi-target ninep-client). This is the part the
//! jepsen suites (FUSE mount) don't reach; only what needs two live processes
//! plus a real client lives here.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use zerofs_client::FailoverClient;

/// Locate the workspace zerofs binary, building it on demand.
fn zerofs_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("ZEROFS_BIN") {
        return PathBuf::from(bin);
    }
    let exe = std::env::current_exe().expect("current_exe");
    let debug_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("test executable outside target dir");
    let candidate = debug_dir.join("zerofs");
    if candidate.exists() {
        return candidate;
    }
    let status = Command::new(env!("CARGO"))
        .args(["build", "-p", "zerofs", "--bin", "zerofs"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to run cargo build -p zerofs");
    assert!(status.success(), "cargo build -p zerofs failed");
    assert!(candidate.exists(), "zerofs binary missing after build");
    candidate
}

/// A `zerofs run` child; killed and reaped on drop.
struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reserve a currently-free localhost TCP port for a replication endpoint.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Caps how many of these process-heavy e2e tests run at once, independent of the
/// harness `--test-threads`. Each spawns a leader + standby, so on a many-core box
/// the default thread count would start dozens at once and starve the failover
/// timing. Each test holds a permit for its run. (The whole suite is `#[ignore]`d
/// by default: it is flaky on shared CI runners and meant to be run explicitly
/// with `--ignored`; the jepsen suites cover HA correctness in CI.)
static E2E_GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

async fn e2e_gate() -> tokio::sync::SemaphorePermit<'static> {
    E2E_GATE.acquire().await.expect("e2e gate")
}

#[allow(clippy::too_many_arguments)]
fn write_config(
    path: &Path,
    data_dir: &Path,
    cache_dir: &Path,
    node_id: &str,
    role: &str,
    ninep_sock: &Path,
    rpc_sock: &Path,
    peers: Option<&str>,
    replication_listen: Option<&str>,
) {
    let peers_line = match peers {
        Some(p) => format!("peers = [\"{p}\"]\n"),
        None => String::new(),
    };
    let listen_line = match replication_listen {
        Some(l) => format!("replication_listen = \"{l}\"\n"),
        None => String::new(),
    };
    let (storage_url, aws_section) = match std::env::var("ZEROFS_TEST_S3_URL") {
        Ok(base) => {
            let unique = data_dir
                .display()
                .to_string()
                .replace(['/', '.'], "-")
                .trim_matches('-')
                .to_string();
            (
                format!("{}/{unique}", base.trim_end_matches('/')),
                "\n[aws]\naccess_key_id = \"${AWS_ACCESS_KEY_ID}\"\n\
                 secret_access_key = \"${AWS_SECRET_ACCESS_KEY}\"\n\
                 endpoint = \"${AWS_ENDPOINT}\"\n\
                 region = \"us-east-1\"\n\
                 allow_http = \"true\"\n\
                 conditional_put = \"etag\"\n"
                    .to_string(),
            )
        }
        Err(_) => (format!("file://{}", data_dir.display()), String::new()),
    };
    // Short HA timing for a fast test; still valid (heartbeat < lease,
    // takeover > lease + 2s guard band).
    let cfg = format!(
        r#"
[cache]
dir = "{cache}"
disk_size_gb = 1.0

[storage]
url = "{storage_url}"
encryption_password = "failover-e2e-test"
{aws_section}
[servers]

[servers.ninep]
unix_socket = "{ninep}"

[servers.rpc]
unix_socket = "{rpc}"

[replication]
node_id = "{node_id}"
role = "{role}"
{peers_line}{listen_line}
[telemetry]
enabled = false
"#,
        cache = cache_dir.display(),
        ninep = ninep_sock.display(),
        rpc = rpc_sock.display(),
    );
    std::fs::write(path, cfg).unwrap();
}

fn spawn(config_path: &Path) -> Node {
    // A spawned server inherits this test process's stderr, which bypasses cargo's
    // output capture, so by default we silence it (RUST_LOG=off + discard stderr)
    // to keep a normal run clean -- including SlateDB's expected fencing errors on
    // failover. Set ZEROFS_TEST_LOG to a RUST_LOG filter (e.g. `warn` or `debug`)
    // to surface server logs for debugging.
    let log = std::env::var("ZEROFS_TEST_LOG").ok();
    let child = Command::new(zerofs_binary())
        .args(["run", "-c"])
        .arg(config_path)
        .env("RUST_LOG", log.as_deref().unwrap_or("off"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(if log.is_some() {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .spawn()
        .expect("failed to spawn zerofs run");
    Node { child }
}

async fn socket_up(sock: &Path) -> bool {
    tokio::net::UnixStream::connect(sock).await.is_ok()
}

async fn wait_for_socket(sock: &Path, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if socket_up(sock).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// A `FailoverClient` over both node addresses re-routes to the new leader
/// across a failover with no manual reconnection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn failover_client_transparently_reroutes() {
    let _e2e_permit = e2e_gate().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let l_rpc = dir.path().join("leader-rpc.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let s_rpc = dir.path().join("standby-rpc.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let standby_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-leader",
        "leader",
        &l_9p,
        &l_rpc,
        Some(&standby_repl),
        None,
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-standby",
        "standby",
        &s_9p,
        &s_rpc,
        None,
        Some(&standby_repl),
    );

    let standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects

    let targets = vec![
        format!("unix:{}", l_9p.display()),
        format!("unix:{}", s_9p.display()),
    ];
    let client = FailoverClient::connect(targets)
        .await
        .expect("failover client connects to the leader");

    client
        .write("/f", b"before")
        .await
        .expect("write before failover");
    assert_eq!(&client.read("/f").await.unwrap()[..], b"before");

    leader.child.kill().expect("kill leader");
    leader.child.wait().expect("reap leader");

    // Same client re-routes to the new leader (blocks through the failover gap).
    client
        .write("/f2", b"after")
        .await
        .expect("write must succeed after a transparent failover");
    assert_eq!(&client.read("/f2").await.unwrap()[..], b"after");

    // Pre-failover write survived (shipped via semi-sync, replayed on takeover).
    assert_eq!(
        &client.read("/f").await.unwrap()[..],
        b"before",
        "the pre-failover write must survive and be served by the new leader"
    );

    drop(standby);
}

/// FailoverClient re-routes a non-idempotent mutating op (mkdir) across a
/// failover, and the dir created on the old leader survives via semi-sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn failover_client_reroutes_mutating_ops() {
    let _e2e_permit = e2e_gate().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let l_rpc = dir.path().join("leader-rpc.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let s_rpc = dir.path().join("standby-rpc.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let standby_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-leader",
        "leader",
        &l_9p,
        &l_rpc,
        Some(&standby_repl),
        None,
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-standby",
        "standby",
        &s_9p,
        &s_rpc,
        None,
        Some(&standby_repl),
    );

    let standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects

    let targets = vec![
        format!("unix:{}", l_9p.display()),
        format!("unix:{}", s_9p.display()),
    ];
    let client = FailoverClient::connect(targets)
        .await
        .expect("failover client connects to the leader");

    let m = client
        .create_dir("/d1", 0o755)
        .await
        .expect("create_dir on leader");
    assert!(m.is_dir());

    leader.child.kill().expect("kill leader");
    leader.child.wait().expect("reap leader");

    let m2 = client
        .create_dir("/d2", 0o755)
        .await
        .expect("create_dir after failover must re-route to the new leader");
    assert!(m2.is_dir());

    // Pre-failover dir survived (semi-sync replay): re-creating it is EEXIST.
    let err = client.create_dir("/d1", 0o755).await;
    assert!(
        matches!(err, Err(zerofs_client::ZeroFsError::AlreadyExists { .. })),
        "/d1 must have survived the failover, got {err:?}"
    );

    drop(standby);
}

/// An OPEN `File` handle survives a leader failover: its I/O re-opens the path on
/// the re-probed leader and keeps working, instead of hanging on (or erroring
/// against) the dead leader's connection. This is the gap the audit flagged:
/// the handle used to be pinned to one leader with no per-op timeout and no
/// reroute, so an open-file workload could wedge across a failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn open_file_handle_survives_failover() {
    let _e2e_permit = e2e_gate().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let l_rpc = dir.path().join("leader-rpc.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let s_rpc = dir.path().join("standby-rpc.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let standby_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-leader",
        "leader",
        &l_9p,
        &l_rpc,
        Some(&standby_repl),
        None,
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-standby",
        "standby",
        &s_9p,
        &s_rpc,
        None,
        Some(&standby_repl),
    );

    let standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects

    let targets = vec![
        format!("unix:{}", l_9p.display()),
        format!("unix:{}", s_9p.display()),
    ];
    let client = FailoverClient::connect(targets)
        .await
        .expect("failover client connects to the leader");

    // Open a handle, write through it, and fsync so the file is durable on the
    // shared store (and replayed onto the standby) before the leader dies.
    let file = client.create("/f").await.expect("create /f on leader");
    file.write_at(0, b"before")
        .await
        .expect("write before failover");
    file.sync_all().await.expect("fsync before failover");

    leader.child.kill().expect("kill leader");
    leader.child.wait().expect("reap leader");

    // The SAME handle must keep working: it re-opens /f on the new leader and
    // retries, rather than hanging on the dead connection.
    file.write_at(0, b"after!")
        .await
        .expect("write through the open handle must re-route across the failover");
    let got = file
        .read_at(0, 6)
        .await
        .expect("read through the open handle must re-route across the failover");
    assert_eq!(
        &got[..],
        &b"after!"[..],
        "the handle must read its own post-failover write"
    );

    drop(standby);
}

/// FENCED-BUT-ALIVE leader (partition, not clean death): process + 9P listener
/// stay up, but once deposed the lease gate answers ops with P9_ENOTLEADER
/// rather than dropping the connection. The client must re-probe and re-route,
/// NOT surface EIO. The only test covering this sentinel path (others SIGKILL,
/// dropping the connection). Simulated SIGSTOP (standby takes over) then SIGCONT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn client_reroutes_around_a_fenced_but_alive_leader() {
    let _e2e_permit = e2e_gate().await;
    use ninep_client::{ClientError, NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let standby_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-leader",
        "leader",
        &l_9p,
        &dir.path().join("leader-rpc.sock"),
        Some(&standby_repl),
        None,
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-standby",
        "standby",
        &s_9p,
        &dir.path().join("standby-rpc.sock"),
        None,
        Some(&standby_repl),
    );

    let standby = spawn(&s_cfg);
    let leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects
    let leader_pid = leader.child.id();

    let client = NinePClient::connect_multi(
        vec![Target::Unix(l_9p.clone()), Target::Unix(s_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on the leader");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");
    client
        .mkdir(1, b"d1", 0o755, 0)
        .await
        .expect("mkdir d1 on leader");

    // Freeze (partition, not death): heartbeat stops, listener stays up.
    let _ = Command::new("kill")
        .args(["-STOP", &leader_pid.to_string()])
        .status();
    assert!(
        wait_for_socket(&s_9p, Duration::from_secs(90)).await,
        "standby never took over"
    );
    // Thaw: leader is now fenced-but-alive (answers ops with P9_ENOTLEADER).
    let _ = Command::new("kill")
        .args(["-CONT", &leader_pid.to_string()])
        .status();

    // Client still bound to the fenced leader must re-probe + re-route, not EIO.
    client
        .mkdir(1, b"d2", 0o755, 0)
        .await
        .expect("op must re-route around a fenced-but-alive leader, not EIO");
    // d1 survived to the new leader (shipped via semi-sync before the partition).
    assert!(
        matches!(
            client.mkdir(1, b"d1", 0o755, 0).await,
            Err(ClientError::Errno(17))
        ),
        "d1 must have survived onto the new leader"
    );

    drop(standby);
}

/// The higher-level `FailoverClient` must also re-route around a fenced-but-alive
/// leader: its `is_transport_failure` has to treat `NotLeader` (P9_ENOTLEADER) as
/// a re-route trigger, not surface it. (Pre-fix it only re-routed on EIO, so a
/// deposed-but-responsive leader's ENOTLEADER fell straight through to the caller.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn failover_client_reroutes_around_a_fenced_but_alive_leader() {
    let _e2e_permit = e2e_gate().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let standby_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-leader",
        "leader",
        &l_9p,
        &dir.path().join("leader-rpc.sock"),
        Some(&standby_repl),
        None,
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-standby",
        "standby",
        &s_9p,
        &dir.path().join("standby-rpc.sock"),
        None,
        Some(&standby_repl),
    );

    let standby = spawn(&s_cfg);
    let leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects
    let leader_pid = leader.child.id();

    let targets = vec![
        format!("unix:{}", l_9p.display()),
        format!("unix:{}", s_9p.display()),
    ];
    let client = FailoverClient::connect(targets)
        .await
        .expect("connect to leader");
    client
        .create_dir("/d1", 0o755)
        .await
        .expect("create_dir d1 on leader");

    // Freeze (partition, not death) so the standby takes over, then thaw: the old
    // leader is fenced-but-alive and answers ops with P9_ENOTLEADER.
    let _ = Command::new("kill")
        .args(["-STOP", &leader_pid.to_string()])
        .status();
    assert!(
        wait_for_socket(&s_9p, Duration::from_secs(90)).await,
        "standby never took over"
    );
    let _ = Command::new("kill")
        .args(["-CONT", &leader_pid.to_string()])
        .status();

    // The cached connection still points at the fenced leader; the next op gets
    // P9_ENOTLEADER and must re-probe + re-route, not surface it.
    client
        .create_dir("/d2", 0o755)
        .await
        .expect("must re-route around the fenced-but-alive leader, not surface ENOTLEADER");
    assert!(
        matches!(
            client.create_dir("/d1", 0o755).await,
            Err(zerofs_client::ZeroFsError::AlreadyExists { .. })
        ),
        "d1 must have survived onto the new leader"
    );

    drop(standby);
}

/// No-acked-loss across a leader RESTART (not just a failover). An un-flushed
/// acked op (a mkdir, shipped to the standby tail via semi-sync but not yet on
/// the object store) must survive the leader bouncing: the returning leader must
/// defer so the standby takes over and replays the tail, instead of re-taking and
/// dropping it. Symmetric config so the returning leader can defer and follow.
async fn unflushed_op_survives_a_leader_restart_case() {
    use ninep_client::{ClientError, NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");

    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-b",
        "standby",
        &s_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects

    let client = NinePClient::connect_multi(
        vec![Target::Unix(l_9p.clone()), Target::Unix(s_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on the leader");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");
    // Un-flushed acked op: committed + shipped to the standby tail, not flushed.
    client
        .mkdir(1, b"keep", 0o755, 0)
        .await
        .expect("mkdir keep on leader");

    // Bounce the leader fast (within takeover_ttl): the returning leader must NOT
    // re-take and drop the tail; the standby must take over and replay it.
    leader.child.kill().expect("kill leader");
    leader.child.wait().expect("reap leader");
    // Restart with a fresh cache dir: a SIGKILL can leave the local cache torn,
    // which is orthogonal to the leadership behavior under test here.
    let l_cfg2 = dir.path().join("leader2.toml");
    write_config(
        &l_cfg2,
        &data_dir,
        &dir.path().join("cache-l2"),
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    let _leader2 = spawn(&l_cfg2); // restarts as role=leader

    // The client re-routes to whoever serves; the pre-bounce un-flushed mkdir must
    // have survived (EEXIST), not been lost (which would let the mkdir succeed).
    assert!(
        matches!(
            client.mkdir(1, b"keep", 0o755, 0).await,
            Err(ClientError::Errno(17))
        ),
        "the un-flushed mkdir must survive a leader restart (standby replays the tail)"
    );

    drop(standby);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn unflushed_op_survives_a_leader_restart() {
    let _e2e_permit = e2e_gate().await;
    unflushed_op_survives_a_leader_restart_case().await;
}

// Segment data plane: an un-fsync'd file write's extent bytes live in the
// leader's un-PUT open segment. The leader ships those bytes alongside the
// FrameLoc, so on takeover the standby materializes the segment and the data
// reads back. Without the byte-shipping the replayed FrameLoc would resolve to a
// missing object (404 -> EIO). Same leader-restart shape as the mkdir case.
async fn unflushed_file_write_survives_failover_case() {
    use ninep_client::{NOFID, NinePClient, Target};
    const O_CREAT_RDWR: u32 = 0x42; // O_CREAT | O_RDWR
    const O_RDONLY: u32 = 0;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("leader-9p.sock");
    let s_9p = dir.path().join("standby-9p.sock");
    let l_cfg = dir.path().join("leader.toml");
    let s_cfg = dir.path().join("standby.toml");
    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &dir.path().join("cache-l"),
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-b",
        "standby",
        &s_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader replicator connects

    let client = NinePClient::connect_multi(
        vec![Target::Unix(l_9p.clone()), Target::Unix(s_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on the leader");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");

    // Un-fsync'd file write: committed + shipped, but the extent's segment is still
    // the leader's un-PUT open buffer (data < seal threshold, no fsync, no 30s
    // periodic flush within the test window).
    let data: Vec<u8> = (0..5000u32)
        .map(|i| i.wrapping_mul(2654435761) as u8)
        .collect();
    client.walk(1, 10, &[]).await.expect("clone root");
    client
        .lcreate(10, b"f", O_CREAT_RDWR, 0o644, 0)
        .await
        .expect("lcreate f");
    client.write(10, 0, &data).await.expect("write f");

    // Bounce the leader fast; the standby takes over, replays the tail, and
    // materializes the un-PUT segment from the shipped frame bytes.
    leader.child.kill().expect("kill leader");
    leader.child.wait().expect("reap leader");
    let l_cfg2 = dir.path().join("leader2.toml");
    write_config(
        &l_cfg2,
        &data_dir,
        &dir.path().join("cache-l2"),
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    let _leader2 = spawn(&l_cfg2);

    // Read back through the failover client: the data must be intact — not EIO, and
    // not zeros from a missing segment.
    client
        .walk(1, 20, &[b"f"])
        .await
        .expect("walk to f after failover");
    client.lopen(20, O_RDONLY).await.expect("open f");
    let got = client
        .read(20, 0, data.len() as u32)
        .await
        .expect("read f after failover");
    assert_eq!(
        got, data,
        "un-fsync'd file data must survive failover via the standby-materialized segment"
    );

    drop(standby);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn unflushed_file_write_survives_failover() {
    let _e2e_permit = e2e_gate().await;
    unflushed_file_write_survives_failover_case().await;
}

/// A FROZEN leader (SIGSTOP keeps its TCP connection open, so the clean-crash
/// reconnect never fires) must not hang a client: the per-request timeout tears
/// the dead-but-open connection down and re-routes to the standby that promoted
/// on the heartbeat gap. Without the timeout the op would hang indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn client_reroutes_around_a_frozen_leader() {
    let _e2e_permit = e2e_gate().await;
    use ninep_client::{NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let a_9p = dir.path().join("a-9p.sock");
    let b_9p = dir.path().join("b-9p.sock");
    let a_cfg = dir.path().join("a.toml");
    let b_cfg = dir.path().join("b.toml");
    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &a_cfg,
        &data_dir,
        &dir.path().join("cache-a"),
        "node-a",
        "leader",
        &a_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &b_cfg,
        &data_dir,
        &dir.path().join("cache-b"),
        "node-b",
        "standby",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let mut a = spawn(&a_cfg);
    assert!(
        wait_for_socket(&a_9p, Duration::from_secs(90)).await,
        "leader A 9P never came up"
    );
    let _b = spawn(&b_cfg);
    tokio::time::sleep(Duration::from_secs(6)).await; // B attaches; A's replicator connects

    let client = NinePClient::connect_multi(
        vec![Target::Unix(a_9p.clone()), Target::Unix(b_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on A");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");
    client.mkdir(1, b"x", 0o755, 0).await.expect("mkdir x on A");

    // Freeze A: SIGSTOP leaves its socket open (no FIN), so the client's request to
    // A neither completes nor sees a drop, so only the per-request timeout can unstick
    // it. A also stops heartbeating, so B promotes on the gap.
    let pid = a.child.id();
    assert!(
        std::process::Command::new("kill")
            .arg("-STOP")
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success(),
        "failed to SIGSTOP the leader"
    );

    let routed =
        tokio::time::timeout(Duration::from_secs(90), client.mkdir(1, b"y", 0o755, 0)).await;

    // Thaw + reap A regardless of the outcome (Drop also kills it).
    let _ = std::process::Command::new("kill")
        .arg("-CONT")
        .arg(pid.to_string())
        .status();
    a.child.kill().ok();

    routed
        .expect("client hung on the frozen leader -- no per-request timeout / reroute")
        .expect("mkdir y must succeed on the promoted standby");
}

/// REPRO for the jepsen `(File exists)`: does the mount's compound create
/// (lcreateattr) dedup when its retry lands on the PROMOTED STANDBY after a
/// failover? Same-server dedup is covered (op_id_dedup_tests::op_id_dedups_lcreate);
/// this is the cross-failover path that produced a spurious EEXIST and had no
/// deterministic test. If it dedups, the jepsen line was something else (e.g. Solo).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn op_id_dedups_create_across_failover() {
    let _e2e_permit = e2e_gate().await;
    use ninep_client::{ClientError, NOFID, NinePClient, Target};
    const O_CREAT_RDWR: u32 = 0x42; // O_CREAT | O_RDWR

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let a_9p = dir.path().join("a-9p.sock");
    let b_9p = dir.path().join("b-9p.sock");
    let a_cfg = dir.path().join("a.toml");
    let b_cfg = dir.path().join("b.toml");
    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &a_cfg,
        &data_dir,
        &dir.path().join("cache-a"),
        "node-a",
        "leader",
        &a_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &b_cfg,
        &data_dir,
        &dir.path().join("cache-b"),
        "node-b",
        "standby",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let mut a = spawn(&a_cfg);
    assert!(
        wait_for_socket(&a_9p, Duration::from_secs(90)).await,
        "leader A 9P never came up"
    );
    let _b = spawn(&b_cfg);
    tokio::time::sleep(Duration::from_secs(6)).await;

    let client = NinePClient::connect_multi(
        vec![Target::Unix(a_9p.clone()), Target::Unix(b_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on A");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");

    // Create "f" with op-id cx on the leader. Commit-then-apply ships it to B before
    // returning, so B holds both the inode and op-id cx (in its tail / dedup).
    let cx = [40u8; 16];
    client
        .lcreateattr_op_id(1, 2, b"f", O_CREAT_RDWR, 0o644, 0, cx)
        .await
        .expect("create f on A");
    client.clunk(2).await.ok();
    tokio::time::sleep(Duration::from_secs(3)).await; // let the ship settle on B

    a.child.kill().expect("kill A");
    assert!(
        wait_for_socket(&b_9p, Duration::from_secs(90)).await,
        "B never promoted"
    );

    // The retry (same op-id cx) now lands on B. It MUST dedup (re-open the existing
    // "f"), not EEXIST.
    let retry = client
        .lcreateattr_op_id(1, 3, b"f", O_CREAT_RDWR, 0o644, 0, cx)
        .await;
    assert!(
        retry.is_ok(),
        "cross-failover create retry (same op-id) must dedup, got {retry:?}"
    );

    // A DIFFERENT op-id is a genuine create of an existing name -> EEXIST, proving
    // "f" really is present on B (so the dedup above was real, not a fresh create).
    let genuine = client
        .lcreateattr_op_id(1, 4, b"f", O_CREAT_RDWR, 0o644, 0, [99u8; 16])
        .await;
    assert!(
        matches!(genuine, Err(ClientError::Errno(17))),
        "a different op-id must see EEXIST (f exists on B), got {genuine:?}"
    );
}

/// A restarting node that the latest-leader record names must NOT elect itself
/// from Hello silence: its silent peer may be live behind a partition, holding
/// acked writes in its RAM tail or already promoted. It blocks until a peer
/// answers; when the peer is permanently gone, the operator boots it by removing
/// the peer from `replication.peers` (the escape hatch this test also proves).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn a_latest_leader_survivor_blocks_on_silence_until_the_peer_is_removed() {
    let _e2e_permit = e2e_gate().await;
    use ninep_client::{ClientError, NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let a_9p = dir.path().join("a-9p.sock");
    let b_9p = dir.path().join("b-9p.sock");
    let a_cfg = dir.path().join("a.toml");
    let b_cfg = dir.path().join("b.toml");

    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &a_cfg,
        &data_dir,
        &dir.path().join("cache-a"),
        "node-a",
        "leader",
        &a_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &b_cfg,
        &data_dir,
        &dir.path().join("cache-b"),
        "node-b",
        "standby",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let mut a = spawn(&a_cfg);
    let mut b = spawn(&b_cfg);
    assert!(
        wait_for_socket(&a_9p, Duration::from_secs(90)).await,
        "leader A 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // A's replicator connects to B

    let client = NinePClient::connect_multi(
        vec![Target::Unix(a_9p.clone()), Target::Unix(b_9p.clone())],
        256 * 1024,
    )
    .await
    .expect("connect lands on A");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("attach");
    client.mkdir(1, b"x", 0o755, 0).await.expect("mkdir x on A");

    // Original leader A dies for good.
    a.child.kill().expect("kill A");
    a.child.wait().expect("reap A");
    assert!(
        wait_for_socket(&b_9p, Duration::from_secs(90)).await,
        "standby B never took over"
    );

    // The promoted B (now the recorded latest leader) restarts, fresh cache,
    // while A is still down. Its Hello goes unanswered, so it must block:
    // it cannot tell dead-A from partitioned-A, and electing against a live A
    // would fence it.
    b.child.kill().expect("kill B");
    b.child.wait().expect("reap B");
    let b2_cfg = dir.path().join("b2.toml");
    write_config(
        &b2_cfg,
        &data_dir,
        &dir.path().join("cache-b2"),
        "node-b",
        "standby",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );
    let mut b2 = spawn(&b2_cfg);
    assert!(
        !wait_for_socket(&b_9p, Duration::from_secs(15)).await,
        "a latest-leader survivor must not elect itself from silence"
    );
    assert!(
        b2.child.try_wait().expect("probe b2").is_none(),
        "the blocked survivor must be waiting, not crashed"
    );

    // The operator's escape hatch: declare the dead peer gone by removing it
    // from the config. The node then boots solo and serves the durable state.
    b2.child.kill().expect("kill b2");
    b2.child.wait().expect("reap b2");
    let b3_cfg = dir.path().join("b3.toml");
    write_config(
        &b3_cfg,
        &data_dir,
        &dir.path().join("cache-b3"),
        "node-b",
        "leader",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        None,
        None,
    );
    let _b3 = spawn(&b3_cfg);
    assert!(
        wait_for_socket(&b_9p, Duration::from_secs(120)).await,
        "with the dead peer removed from the config the survivor must serve"
    );
    // x (durable once B first took over) survives the restart.
    assert!(
        matches!(
            client.mkdir(1, b"x", 0o755, 0).await,
            Err(ClientError::Errno(17))
        ),
        "x must survive the survivor's restart"
    );
}

/// A configured standby never elects itself from silence, even on a fresh
/// database with no latest-leader record: the configured leader decides the
/// roles when it appears, and a silent peer may be a live leader a self-election
/// would fence. Promoting a standby whose peer is permanently gone is the same
/// config edit as for a blocked survivor (role = "leader", peer removed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn a_config_standby_never_elects_itself_from_silence() {
    let _e2e_permit = e2e_gate().await;

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let b_9p = dir.path().join("b-9p.sock");
    let b_cfg = dir.path().join("b.toml");
    let a_repl = format!("127.0.0.1:{}", free_port()); // nothing ever listens here
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &b_cfg,
        &data_dir,
        &dir.path().join("cache-b"),
        "node-b",
        "standby",
        &b_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );
    let mut b = spawn(&b_cfg);
    assert!(
        !wait_for_socket(&b_9p, Duration::from_secs(15)).await,
        "a configured standby must not elect itself while every peer is silent"
    );
    assert!(
        b.child.try_wait().expect("probe b").is_none(),
        "the waiting standby must be alive, not crashed"
    );
}

// --- Crash-recovery isolation (one solo writer, no failover) ---------------
//
// Isolates the jepsen HA finding that flushed rename/unlink can vanish on reopen
// after a writer SIGKILL, away from multi-node turbulence and the FUSE mount. A
// SOLO leader (role=leader, no peers => WAL off, exactly the jepsen durability
// model) makes three DURABLE (fsync'd) states, gets SIGKILL'd, and is reopened on
// the same store. A is a create+write control (the grow-only workload that was
// always 0-lost); B is rename-into-place; C is an unlink. We run it with a reused
// cache (matches jepsen, which keeps the cache dir across restarts) and a fresh
// cache (reads purely from the store, isolating store durability from a
// SIGKILL-torn local cache).

const O_CREAT_RDWR: u32 = 0x42; // O_CREAT | O_RDWR
const O_RDONLY: u32 = 0;

async fn make_durable_state(client: &ninep_client::NinePClient) {
    // A: create + write + fsync (the always-survived control).
    client.walk(1, 10, &[]).await.expect("clone root for A");
    client
        .lcreate(10, b"A", O_CREAT_RDWR, 0o644, 0)
        .await
        .expect("create A");
    client.write(10, 0, b"A").await.expect("write A");
    client.fsync(10, 0).await.expect("fsync A");
    client.clunk(10).await.ok();

    // B: create a temp, rename it into place, fsync (a ZeroFS fsync is global).
    client.walk(1, 11, &[]).await.expect("clone root for B.tmp");
    client
        .lcreate(11, b"B.tmp", O_CREAT_RDWR, 0o644, 0)
        .await
        .expect("create B.tmp");
    client.write(11, 0, b"B").await.expect("write B.tmp");
    client.clunk(11).await.ok();
    client
        .renameat(1, b"B.tmp", 1, b"B")
        .await
        .expect("rename B.tmp -> B");
    client.fsync(1, 0).await.expect("fsync after rename");

    // C: create + fsync (durable), then unlink + fsync (durable delete).
    client.walk(1, 12, &[]).await.expect("clone root for C");
    client
        .lcreate(12, b"C", O_CREAT_RDWR, 0o644, 0)
        .await
        .expect("create C");
    client.write(12, 0, b"C").await.expect("write C");
    client.fsync(12, 0).await.expect("fsync C");
    client.clunk(12).await.ok();
    client.unlinkat(1, b"C", 0).await.expect("unlink C");
    client.fsync(1, 0).await.expect("fsync after unlink");
}

/// Returns (A content, B present, B.tmp present, C present).
async fn read_state(client: &ninep_client::NinePClient) -> (Vec<u8>, bool, bool, bool) {
    client
        .walk(1, 20, &[b"A"])
        .await
        .expect("A (create+write control) must survive reopen");
    client.lopen(20, O_RDONLY).await.expect("open A");
    let a = client.read(20, 0, 16).await.expect("read A");
    let b = client.walk(1, 21, &[b"B"]).await.is_ok();
    let btmp = client.walk(1, 22, &[b"B.tmp"]).await.is_ok();
    let c = client.walk(1, 23, &[b"C"]).await.is_ok();
    (a, b, btmp, c)
}

async fn crash_recovery_case(reuse_cache: bool) {
    use ninep_client::{NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("l-9p.sock");
    let l_cfg = dir.path().join("l.toml");
    let cache1 = dir.path().join("cache1");
    write_config(
        &l_cfg,
        &data_dir,
        &cache1,
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("rpc.sock"),
        None,
        None,
    );

    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "solo leader 9P never came up"
    );
    {
        let client = NinePClient::connect_multi(vec![Target::Unix(l_9p.clone())], 256 * 1024)
            .await
            .expect("connect");
        client
            .attach(1, NOFID, "root", "/", 0)
            .await
            .expect("attach");
        make_durable_state(&client).await;
    }

    // Abrupt writer death, like jepsen kill-both (SIGKILL, no clean close).
    leader.child.kill().expect("kill writer");
    leader.child.wait().expect("reap writer");

    // Reopen the same store. Reused cache matches jepsen; a fresh cache reads
    // purely from the object store (isolates store durability from a torn cache).
    let l_cfg2 = dir.path().join("l2.toml");
    let cache2 = if reuse_cache {
        cache1
    } else {
        dir.path().join("cache2")
    };
    write_config(
        &l_cfg2,
        &data_dir,
        &cache2,
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("rpc.sock"),
        None,
        None,
    );
    let _leader2 = spawn(&l_cfg2);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "reopened 9P never came up"
    );

    let client = NinePClient::connect_multi(vec![Target::Unix(l_9p.clone())], 256 * 1024)
        .await
        .expect("reconnect");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("re-attach");
    let (a, b, btmp, c) = read_state(&client).await;

    assert_eq!(a, b"A", "control A (create+write) content survived");
    assert!(
        b,
        "B (renamed into place + fsync'd) must survive the writer SIGKILL"
    );
    assert!(!btmp, "B.tmp must be gone (the durable rename completed)");
    assert!(
        !c,
        "C (unlinked + fsync'd) must stay deleted across the writer SIGKILL"
    );
}

// Single-node reused-cache reopen: a SIGKILL'd writer reopened on its OWN cache dir
// must still recover. A torn cache block is foyer's to detect + re-fetch from the
// store; and a single node has one stable key, so there's no cross-key decrypt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn flushed_metadata_survives_writer_sigkill_reused_cache() {
    let _e2e_permit = e2e_gate().await;
    crash_recovery_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn flushed_metadata_survives_writer_sigkill_fresh_cache() {
    let _e2e_permit = e2e_gate().await;
    crash_recovery_case(false).await;
}

// Regression guard for the encryption-key INIT RACE. Leader + standby start
// concurrently on a fresh store; both used to race `load_or_init_encryption_key`
// and derive DIFFERENT keys (load -> None -> generate -> plain put, last-writer-
// wins, each keeping its own DEK), so a block one node wrote couldn't be decrypted
// by the other after a reopen, an aead failure surfacing as the jepsen-class
// "error transforming block". With the conditional-create key init they converge on
// one key and all three durable states survive kill-both.
async fn connected_killboth_case(reuse_cache: bool) {
    use ninep_client::{NOFID, NinePClient, Target};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let l_9p = dir.path().join("l-9p.sock");
    let s_9p = dir.path().join("s-9p.sock");
    let l_cfg = dir.path().join("l.toml");
    let s_cfg = dir.path().join("s.toml");
    let cache_l = dir.path().join("cache-l");
    let a_repl = format!("127.0.0.1:{}", free_port());
    let b_repl = format!("127.0.0.1:{}", free_port());
    write_config(
        &l_cfg,
        &data_dir,
        &cache_l,
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    write_config(
        &s_cfg,
        &data_dir,
        &dir.path().join("cache-s"),
        "node-b",
        "standby",
        &s_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );

    let mut standby = spawn(&s_cfg);
    let mut leader = spawn(&l_cfg);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "leader 9P never came up"
    );
    tokio::time::sleep(Duration::from_secs(5)).await; // leader connects -> Connected semi-sync
    {
        let client = NinePClient::connect_multi(
            vec![Target::Unix(l_9p.clone()), Target::Unix(s_9p.clone())],
            256 * 1024,
        )
        .await
        .expect("connect lands on leader");
        client
            .attach(1, NOFID, "root", "/", 0)
            .await
            .expect("attach");
        make_durable_state(&client).await;
    }

    // kill BOTH (jepsen kill-both): the standby's tail dies with it, so survival
    // depends entirely on the leader having flushed to the store on fsync.
    leader.child.kill().ok();
    leader.child.wait().ok();
    standby.child.kill().ok();
    standby.child.wait().ok();

    // Reopen the pair from the store: both tails died with the processes, so
    // survival depends entirely on what the leader flushed on fsync. Both nodes
    // restart because the leader is the recorded latest writer and must hear
    // its peer answer Hello before it opens.
    let l_cfg2 = dir.path().join("l2.toml");
    let cache_l2 = if reuse_cache {
        cache_l
    } else {
        dir.path().join("cache-l2")
    };
    write_config(
        &l_cfg2,
        &data_dir,
        &cache_l2,
        "node-a",
        "leader",
        &l_9p,
        &dir.path().join("a-rpc.sock"),
        Some(&b_repl),
        Some(&a_repl),
    );
    let s_cfg2 = dir.path().join("s2.toml");
    write_config(
        &s_cfg2,
        &data_dir,
        &dir.path().join("cache-s2"),
        "node-b",
        "standby",
        &s_9p,
        &dir.path().join("b-rpc.sock"),
        Some(&a_repl),
        Some(&b_repl),
    );
    let _standby2 = spawn(&s_cfg2);
    let _leader2 = spawn(&l_cfg2);
    assert!(
        wait_for_socket(&l_9p, Duration::from_secs(90)).await,
        "reopened leader never came up"
    );

    let client = NinePClient::connect_multi(vec![Target::Unix(l_9p.clone())], 256 * 1024)
        .await
        .expect("reconnect");
    client
        .attach(1, NOFID, "root", "/", 0)
        .await
        .expect("re-attach");
    let (a, b, btmp, c) = read_state(&client).await;

    assert_eq!(
        a, b"A",
        "control A (create+write) content survived kill-both"
    );
    assert!(
        b,
        "B (rename, fsync'd while Connected) must survive kill-both"
    );
    assert!(!btmp, "B.tmp must be gone (durable rename)");
    assert!(
        !c,
        "C (unlink, fsync'd while Connected) must stay deleted across kill-both"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn flushed_metadata_survives_connected_killboth_reused_cache() {
    let _e2e_permit = e2e_gate().await;
    connected_killboth_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real-process failover e2e: flaky on shared CI runners, run with --ignored. HA correctness is covered by the jepsen suites."]
async fn flushed_metadata_survives_connected_killboth_fresh_cache() {
    let _e2e_permit = e2e_gate().await;
    connected_killboth_case(false).await;
}
