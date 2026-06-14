//! Integration tests against a real zerofs server: a `zerofs run` child with
//! file:// storage and unix sockets in a temp dir, the real CSI services
//! served over a unix socket, and a real 9P client verifying what the
//! controller did to the gateway filesystem. No mocks.

use ninep_client::{NOFID, NinePClient};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tonic::Code;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use zerofs_csi::controller::ControllerService;
use zerofs_csi::identity::IdentityService;
use zerofs_csi::node::{MountAccess, NodeService};
use zerofs_csi::proto::csi::v1::controller_client::ControllerClient;
use zerofs_csi::proto::csi::v1::identity_client::IdentityClient;
use zerofs_csi::proto::csi::v1::node_client::NodeClient;
use zerofs_csi::proto::csi::v1::{
    CapacityRange, CreateVolumeRequest, DeleteVolumeRequest, GetPluginInfoRequest,
    NodeGetVolumeStatsRequest, NodePublishVolumeRequest, NodeUnpublishVolumeRequest, ProbeRequest,
    ValidateVolumeCapabilitiesRequest, VolumeCapability, controller_service_capability,
    volume_capability,
};
use zerofs_csi::server::serve;

/// Locate the zerofs binary built by this workspace, building it on demand
/// when only the csi tests were compiled.
fn zerofs_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("ZEROFS_BIN") {
        return PathBuf::from(bin);
    }
    // target/debug/deps/integration-... -> target/debug/zerofs
    let exe = std::env::current_exe().expect("current_exe");
    let debug_dir = exe
        .parent()
        .and_then(Path::parent)
        .expect("test executable outside target dir");
    let candidate = debug_dir.join("zerofs");
    if candidate.exists() {
        return candidate;
    }
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let status = Command::new(env!("CARGO"))
        .args(["build", "-p", "zerofs", "--bin", "zerofs"])
        .current_dir(workspace)
        .status()
        .expect("failed to run cargo build -p zerofs");
    assert!(status.success(), "cargo build -p zerofs failed");
    assert!(candidate.exists(), "zerofs binary missing after build");
    candidate
}

/// A real zerofs server child process backed by file:// storage in a temp
/// dir, exposing 9P and the admin RPC on unix sockets.
struct TestServer {
    child: Child,
    dir: tempfile::TempDir,
    ninep_sock: PathBuf,
    rpc_sock: PathBuf,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl TestServer {
    /// 9P gateway address in the form `zerofs mount` and the node service
    /// accept.
    fn gateway(&self) -> String {
        format!("unix:{}", self.ninep_sock.display())
    }

    /// Admin endpoint in the form the CSI controller accepts.
    fn admin_endpoint(&self) -> String {
        format!("unix://{}", self.rpc_sock.display())
    }
}

async fn start_zerofs_server() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let ninep_sock = dir.path().join("9p.sock");
    let rpc_sock = dir.path().join("rpc.sock");
    let config = format!(
        r#"
[cache]
dir = "{cache}"
disk_size_gb = 1.0

[storage]
url = "file://{data}"
encryption_password = "csi-integration-test"

[servers]

[servers.ninep]
unix_socket = "{ninep}"

[servers.rpc]
unix_socket = "{rpc}"

[telemetry]
enabled = false
"#,
        cache = dir.path().join("cache").display(),
        data = dir.path().join("data").display(),
        ninep = ninep_sock.display(),
        rpc = rpc_sock.display(),
    );
    let config_path = dir.path().join("zerofs.toml");
    std::fs::write(&config_path, config).unwrap();

    // Quiet on success but real failures still reach the test output.
    let child = Command::new(zerofs_binary())
        .args(["run", "-c"])
        .arg(&config_path)
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn zerofs run");

    let server = TestServer {
        child,
        dir,
        ninep_sock,
        rpc_sock,
    };

    // Wait until both sockets accept connections.
    for sock in [&server.ninep_sock, &server.rpc_sock] {
        let mut connected = false;
        for _ in 0..600 {
            if tokio::net::UnixStream::connect(sock).await.is_ok() {
                connected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(connected, "zerofs server never opened {:?}", sock);
    }
    server
}

/// Serve the real CSI services on a unix socket inside the server's temp dir
/// and return connected gRPC clients.
async fn start_csi(
    server: &TestServer,
) -> (
    IdentityClient<Channel>,
    ControllerClient<Channel>,
    NodeClient<Channel>,
) {
    let csi_sock = server.dir.path().join("csi.sock");
    let endpoint = format!("unix://{}", csi_sock.display());
    let identity = IdentityService::new(true);
    let controller = ControllerService::new();
    // The production default (--access all) needs root or user_allow_other
    // in /etc/fuse.conf; fall back to owner so the FUSE test still runs the
    // real mount path on unprivileged dev machines.
    let mount_access = if allow_other_permitted() {
        MountAccess::All
    } else {
        MountAccess::Owner
    };
    let node = NodeService::new(
        "test-node".to_string(),
        zerofs_binary().display().to_string(),
        mount_access,
    );
    tokio::spawn(async move {
        serve(
            &endpoint,
            identity,
            Some(controller),
            Some(node),
            std::future::pending(),
        )
        .await
        .unwrap();
    });

    let mut channel = None;
    for _ in 0..100 {
        let path = csi_sock.clone();
        let result = Endpoint::try_from("http://localhost")
            .unwrap()
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(&path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await;
        match result {
            Ok(c) => {
                channel = Some(c);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let channel = channel.expect("CSI server never came up");
    (
        IdentityClient::new(channel.clone()),
        ControllerClient::new(channel.clone()),
        NodeClient::new(channel),
    )
}

fn mount_capability(mode: volume_capability::access_mode::Mode) -> VolumeCapability {
    VolumeCapability {
        access_type: Some(volume_capability::AccessType::Mount(
            volume_capability::MountVolume::default(),
        )),
        access_mode: Some(volume_capability::AccessMode { mode: mode as i32 }),
    }
}

fn block_capability() -> VolumeCapability {
    VolumeCapability {
        access_type: Some(volume_capability::AccessType::Block(
            volume_capability::BlockVolume::default(),
        )),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

fn create_request(server: &TestServer, name: &str) -> CreateVolumeRequest {
    CreateVolumeRequest {
        name: name.to_string(),
        capacity_range: Some(CapacityRange {
            required_bytes: 1 << 30,
            limit_bytes: 0,
        }),
        volume_capabilities: vec![mount_capability(
            volume_capability::access_mode::Mode::SingleNodeWriter,
        )],
        parameters: HashMap::from([
            ("adminEndpoint".to_string(), server.admin_endpoint()),
            ("gateway".to_string(), server.gateway()),
        ]),
        ..Default::default()
    }
}

fn delete_request(server: &TestServer, volume_id: &str) -> DeleteVolumeRequest {
    DeleteVolumeRequest {
        volume_id: volume_id.to_string(),
        secrets: HashMap::from([("adminEndpoint".to_string(), server.admin_endpoint())]),
    }
}

/// Attach a fresh 9P session rooted at `aname`. Success implies the
/// directory exists on the gateway (a missing path fails the attach).
/// Returns the client and the attached root fid.
async fn attach_aname(
    server: &TestServer,
    aname: &str,
) -> std::io::Result<(Arc<NinePClient>, u32)> {
    let client = NinePClient::connect_unix(&server.ninep_sock, 256 * 1024).await?;
    let fid = client.alloc_fid();
    client
        .attach(fid, NOFID, "root", aname, 0)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok((client, fid))
}

#[tokio::test(flavor = "multi_thread")]
async fn create_and_delete_volume_against_real_gateway() {
    let server = start_zerofs_server().await;
    let (mut identity, mut controller, _node) = start_csi(&server).await;

    // Identity sanity.
    let info = identity
        .get_plugin_info(GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, "csi.zerofs.net");
    assert!(!info.vendor_version.is_empty());
    let probe = identity.probe(ProbeRequest {}).await.unwrap().into_inner();
    assert_eq!(probe.ready, Some(true));
    let caps = controller
        .controller_get_capabilities(
            zerofs_csi::proto::csi::v1::ControllerGetCapabilitiesRequest {},
        )
        .await
        .unwrap()
        .into_inner();
    let has_create_delete = caps.capabilities.iter().any(|c| {
        matches!(
            &c.r#type,
            Some(controller_service_capability::Type::Rpc(rpc))
                if rpc.r#type == controller_service_capability::rpc::Type::CreateDeleteVolume as i32
        )
    });
    assert!(has_create_delete);

    // CreateVolume provisions a directory on the gateway.
    let volume = controller
        .create_volume(create_request(&server, "pvc-itest"))
        .await
        .unwrap()
        .into_inner()
        .volume
        .unwrap();
    assert_eq!(volume.volume_id, "pvc-itest");
    assert_eq!(volume.capacity_bytes, 1 << 30);
    assert_eq!(
        volume.volume_context.get("gateway"),
        Some(&server.gateway())
    );
    assert_eq!(
        volume.volume_context.get("volumesRoot"),
        Some(&"/volumes".to_string())
    );

    // The directory is attachable as a 9P aname; a sibling that was never
    // created is not.
    attach_aname(&server, "/volumes/pvc-itest").await.unwrap();
    assert!(attach_aname(&server, "/volumes/pvc-missing").await.is_err());

    // Idempotent: same request again succeeds.
    controller
        .create_volume(create_request(&server, "pvc-itest"))
        .await
        .unwrap();

    // Required-field and capability validation.
    let status = controller
        .create_volume(CreateVolumeRequest {
            name: "pvc-bad".to_string(),
            volume_capabilities: vec![block_capability()],
            parameters: HashMap::from([
                ("adminEndpoint".to_string(), server.admin_endpoint()),
                ("gateway".to_string(), server.gateway()),
            ]),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    let status = controller
        .create_volume(CreateVolumeRequest {
            name: "pvc-noparams".to_string(),
            volume_capabilities: vec![mount_capability(
                volume_capability::access_mode::Mode::SingleNodeWriter,
            )],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    // ValidateVolumeCapabilities confirms mount volumes and rejects block.
    let confirmed = controller
        .validate_volume_capabilities(ValidateVolumeCapabilitiesRequest {
            volume_id: "pvc-itest".to_string(),
            volume_capabilities: vec![mount_capability(
                volume_capability::access_mode::Mode::MultiNodeMultiWriter,
            )],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(confirmed.confirmed.is_some());
    let rejected = controller
        .validate_volume_capabilities(ValidateVolumeCapabilitiesRequest {
            volume_id: "pvc-itest".to_string(),
            volume_capabilities: vec![block_capability()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(rejected.confirmed.is_none());
    assert!(!rejected.message.is_empty());

    // DeleteVolume removes the directory (the rename out of /volumes happens
    // before the RPC returns; background deletion follows).
    controller
        .delete_volume(delete_request(&server, "pvc-itest"))
        .await
        .unwrap();
    assert!(attach_aname(&server, "/volumes/pvc-itest").await.is_err());

    // Idempotent: deleting again (or a volume that never existed) succeeds.
    controller
        .delete_volume(delete_request(&server, "pvc-itest"))
        .await
        .unwrap();
    controller
        .delete_volume(delete_request(&server, "pvc-never-existed"))
        .await
        .unwrap();

    // Without the provisioner secret the controller cannot reach the
    // gateway and must say so.
    let status = controller
        .delete_volume(DeleteVolumeRequest {
            volume_id: "pvc-itest".to_string(),
            secrets: HashMap::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

/// Whether `zerofs mount --access all` (FUSE allow_other) can work for this
/// process: either we are root or /etc/fuse.conf enables user_allow_other.
fn allow_other_permitted() -> bool {
    if unsafe { libc::geteuid() } == 0 {
        return true;
    }
    std::fs::read_to_string("/etc/fuse.conf")
        .map(|conf| conf.lines().any(|l| l.trim() == "user_allow_other"))
        .unwrap_or(false)
}

/// FUSE is required: needs /dev/fuse and a fusermount helper. Skips
/// gracefully when unavailable (e.g. minimal CI containers).
fn fuse_available() -> bool {
    if !Path::new("/dev/fuse").exists() {
        return false;
    }
    ["fusermount3", "fusermount"].iter().any(|bin| {
        Command::new(bin)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn node_publish_and_unpublish_fuse_mount() {
    if !fuse_available() {
        eprintln!("skipping: /dev/fuse or fusermount not available");
        return;
    }

    let server = start_zerofs_server().await;
    let (_identity, mut controller, mut node) = start_csi(&server).await;

    controller
        .create_volume(create_request(&server, "pvc-fuse"))
        .await
        .unwrap();

    let target = server.dir.path().join("publish/target");
    let publish_request = NodePublishVolumeRequest {
        volume_id: "pvc-fuse".to_string(),
        target_path: target.display().to_string(),
        volume_capability: Some(mount_capability(
            volume_capability::access_mode::Mode::SingleNodeWriter,
        )),
        volume_context: HashMap::from([("gateway".to_string(), server.gateway())]),
        ..Default::default()
    };
    node.node_publish_volume(publish_request.clone())
        .await
        .unwrap();

    // The mount is live: write through FUSE...
    tokio::task::block_in_place(|| {
        use std::io::Write;
        let mut f = std::fs::File::create(target.join("hello.txt")).unwrap();
        f.write_all(b"from fuse").unwrap();
        f.sync_all().unwrap();
    });

    // ...and the file is visible on the gateway inside the volume subtree.
    let (client, root_fid) = attach_aname(&server, "/volumes/pvc-fuse").await.unwrap();
    client
        .walk(root_fid, client.alloc_fid(), &[b"hello.txt".as_ref()])
        .await
        .unwrap();

    // Publishing the same volume at the same target again is a no-op.
    node.node_publish_volume(publish_request.clone())
        .await
        .unwrap();

    // Stats come from statfs on the mountpoint.
    let stats = node
        .node_get_volume_stats(NodeGetVolumeStatsRequest {
            volume_id: "pvc-fuse".to_string(),
            volume_path: target.display().to_string(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!stats.usage.is_empty());

    // Kill the FUSE child (SIGKILL, no cleanup) to leave a broken mount:
    // statfs on the target starts failing with ENOTCONN. Publishing again
    // must lazily unmount the corpse and mount fresh.
    let pgrep = Command::new("pgrep")
        .args(["-f", &format!("zerofs mount .*{}", target.display())])
        .output()
        .unwrap();
    let pid: i32 = String::from_utf8_lossy(&pgrep.stdout)
        .trim()
        .parse()
        .expect("expected exactly one zerofs mount child");
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut broken = false;
    for _ in 0..100 {
        if std::fs::metadata(&target)
            .err()
            .and_then(|e| e.raw_os_error())
            == Some(libc::ENOTCONN)
        {
            broken = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(broken, "mount never went ENOTCONN after killing the child");
    node.node_publish_volume(publish_request.clone())
        .await
        .unwrap();
    let recovered =
        tokio::task::block_in_place(|| std::fs::read(target.join("hello.txt"))).unwrap();
    assert_eq!(recovered, b"from fuse");

    // Unpublish unmounts and removes the target directory.
    let unpublish_request = NodeUnpublishVolumeRequest {
        volume_id: "pvc-fuse".to_string(),
        target_path: target.display().to_string(),
    };
    node.node_unpublish_volume(unpublish_request.clone())
        .await
        .unwrap();
    assert!(!target.exists());

    // Idempotent: unpublishing again succeeds.
    node.node_unpublish_volume(unpublish_request).await.unwrap();

    controller
        .delete_volume(delete_request(&server, "pvc-fuse"))
        .await
        .unwrap();
}

/// When the gateway is gone but the mount child is still alive (wedged behind
/// its 9P reconnect loop), statfs on the mount blocks forever. The node plugin
/// must detect the dead gateway with its cancellable probe and report the
/// volume unavailable promptly, never parking on the syscall.
#[tokio::test(flavor = "multi_thread")]
async fn stats_and_republish_fail_fast_when_gateway_unreachable() {
    if !fuse_available() {
        eprintln!("skipping: /dev/fuse or fusermount not available");
        return;
    }

    let mut server = start_zerofs_server().await;
    let (_identity, mut controller, mut node) = start_csi(&server).await;

    controller
        .create_volume(create_request(&server, "pvc-wedge"))
        .await
        .unwrap();

    let target = server.dir.path().join("publish/wedge");
    let publish_request = NodePublishVolumeRequest {
        volume_id: "pvc-wedge".to_string(),
        target_path: target.display().to_string(),
        volume_capability: Some(mount_capability(
            volume_capability::access_mode::Mode::SingleNodeWriter,
        )),
        volume_context: HashMap::from([("gateway".to_string(), server.gateway())]),
        ..Default::default()
    };
    node.node_publish_volume(publish_request.clone())
        .await
        .unwrap();

    // Kill the gateway, not the mount child: the child stays alive and enters
    // its 9P reconnect loop, so any statfs on the mount blocks until a
    // reconnect that never comes. (TestServer::drop kills it again, harmlessly.)
    unsafe { libc::kill(server.child.id() as i32, libc::SIGKILL) };
    let _ = server.child.wait();
    // Wait until the 9P socket stops accepting, so the probe sees it as down.
    for _ in 0..100 {
        if tokio::net::UnixStream::connect(&server.ninep_sock)
            .await
            .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Stats must fail fast via the probe, not hang on statfs. A hang would take
    // the 5s statfs deadline (or longer); the probe answers in well under that.
    let start = std::time::Instant::now();
    let err = node
        .node_get_volume_stats(NodeGetVolumeStatsRequest {
            volume_id: "pvc-wedge".to_string(),
            volume_path: target.display().to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "stats hung for {:?} instead of failing fast",
        start.elapsed()
    );

    // Republishing the wedged mount likewise reports Unavailable promptly,
    // leaving the mount in place to reconnect rather than remounting.
    let start = std::time::Instant::now();
    let err = node
        .node_publish_volume(publish_request)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "publish hung for {:?} instead of failing fast",
        start.elapsed()
    );

    // Unpublish still cleans up the wedged mount (lazy unmount + SIGTERM).
    node.node_unpublish_volume(NodeUnpublishVolumeRequest {
        volume_id: "pvc-wedge".to_string(),
        target_path: target.display().to_string(),
    })
    .await
    .unwrap();
    assert!(!target.exists());
}
