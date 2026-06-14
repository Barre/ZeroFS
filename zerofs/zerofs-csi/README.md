# zerofs-csi

CSI driver (`csi.zerofs.net`) that exposes a shared ZeroFS gateway to
Kubernetes pods.

## Architecture

One ZeroFS instance (the "gateway") serves one StorageClass. It owns a single
bucket/prefix and runs as a single-replica StatefulSet, because ZeroFS is
single-writer per filesystem. Volumes are plain subdirectories of one
directory on the gateway filesystem (`volumesRoot`, default `/volumes`):
volume `pvc-1234` is the directory `/volumes/pvc-1234`.

- The **controller** service creates and deletes those directories through
  the gateway's admin RPC (`CreateDirectory` / `RemoveDirectory`, both
  idempotent; removal renames into a trash directory and deletes in the
  background).
- The **node** service publishes a volume by spawning
  `zerofs mount <gateway> <target> --aname <volumesRoot>/<volume_id> --access all`,
  a FUSE mount of just that subdirectory over 9P. 9P was chosen over NFS/NBD
  because the 9P client transparently reconnects and replays its protocol
  state (open file handles, locks, the aname-rooted attach), so a gateway
  restart or network blip does not invalidate existing mounts.

There is no staging step (`NodePublishVolume` mounts directly) and no attach
step (`attachRequired: false`).

## StorageClass parameters

| key | required | meaning |
|---|---|---|
| `adminEndpoint` | yes | Gateway admin RPC URL, e.g. `http://zerofs-gateway.zerofs.svc:7000` |
| `gateway` | yes | 9P address nodes mount from, e.g. `zerofs-gateway.zerofs.svc:5564` |
| `volumesRoot` | no | Directory holding volume directories (default `/volumes`) |

`DeleteVolume` receives no StorageClass parameters (CSI spec: only the volume
id and secrets), so the StorageClass must also reference a Secret carrying
`adminEndpoint` (and `volumesRoot` if non-default) via
`csi.storage.k8s.io/provisioner-secret-name` /
`csi.storage.k8s.io/provisioner-secret-namespace`. See
`deploy/storageclass-example.yaml`.

Supported capabilities: mount volumes only (no block), access modes
`SINGLE_NODE_WRITER`, `SINGLE_NODE_READER_ONLY`, `MULTI_NODE_READER_ONLY`,
`MULTI_NODE_MULTI_WRITER`.

## Binary

```
zerofs-csi --mode controller|node|all \
           --endpoint unix:///csi/csi.sock \
           --node-id <name>            # or env NODE_ID; node mode only
           --zerofs-bin zerofs         # mount binary, node mode only
           --mount-access all          # zerofs mount --access value
```

## Deploying

`deploy/` contains:

- `gateway-example.yaml` — example gateway StatefulSet + Service + config Secret
- `csidriver.yaml` — the CSIDriver object
- `rbac.yaml` — service accounts and provisioner RBAC
- `controller.yaml` — controller Deployment with the csi-provisioner sidecar
- `node.yaml` — node DaemonSet (privileged, needs `/dev/fuse`) with
  node-driver-registrar and livenessprobe sidecars
- `storageclass-example.yaml` — StorageClass plus the provisioner Secret
- `networkpolicy-example.yaml` — gateway lockdown (9P to node pods, admin RPC
  to the controller); see the security note under Limitations

## Limitations (v1)

- **Capacity is not enforced.** `CreateVolume` echoes the requested capacity
  but every volume shares the gateway filesystem; `NodeGetVolumeStats`
  reports whole-filesystem numbers. Use the gateway's
  `[filesystem] max_size_gb` to cap total usage.
- **No per-volume snapshots.** ZeroFS checkpoints cover the whole filesystem,
  not one volume directory.
- **StorageClass `mountOptions` are rejected.** The FUSE mount takes no
  pass-through options, so a capability carrying `mount_flags` fails with
  InvalidArgument instead of being silently ignored.
- **`CreateVolume` cannot detect a capacity mismatch.** The spec wants
  ALREADY_EXISTS when an existing volume is re-requested with a different
  capacity, but no per-volume capacity is stored (capacity is unenforced),
  so re-creation with any capacity succeeds. This will become a real check
  when per-volume quotas land.
- **FUSE mounts die with the node plugin.** The `zerofs mount` processes are
  children of the node plugin container; if it restarts, published mounts on
  that node break. Restarting the affected pods republishes and remounts
  them. Dedicated per-volume mount pods are the planned v2 fix.
- **The gateway has no authentication; network reachability is the trust
  boundary.** Both ports must be restricted to their legitimate clients:
  - 9P (5564) is the data plane. The aname is client-chosen and not a security
    boundary — anything that can reach it can attach any volume's subtree or
    the filesystem root — so it is open to every CSI node pod.
  - The admin RPC (7000) is a root-equivalent control plane that can trash any
    volume (`RemoveDirectory`). Its only client is the controller, so it is
    locked to the controller pod alone.

  Apply `deploy/networkpolicy-example.yaml` (adapted to your labels) alongside
  the gateway to enforce both. A NetworkPolicy is a no-op on a CNI that does
  not enforce them (e.g. vanilla flannel / default k3s); on such a cluster
  there is no in-cluster protection for either port, so do not expose the
  gateway to untrusted pods.

## Tests

`cargo test -p zerofs-csi` runs integration tests against a real zerofs
server (file:// storage, unix sockets, spawned from the workspace build).
The FUSE publish/unpublish test runs when `/dev/fuse` and a fusermount
helper are available and is skipped otherwise.

### CSI conformance (csi-sanity)

The driver passes [csi-sanity](https://github.com/kubernetes-csi/csi-test)
v5.4.0 (31 specs run; the rest cover undeclared capabilities). Against a
local gateway serving 9P and admin RPC on unix sockets:

```sh
# params.yaml:  adminEndpoint/gateway (unix:/path or host:port), volumesRoot
# secrets.yaml: CreateVolumeSecret/DeleteVolumeSecret with adminEndpoint,
#               ControllerValidateVolumeCapabilitiesSecret also with gateway
zerofs-csi --endpoint unix:///tmp/csi.sock --node-id sanity-node \
  --mode all --mount-access owner --zerofs-bin /path/to/zerofs &
csi-sanity -csi.endpoint unix:///tmp/csi.sock \
  -csi.testvolumeparameters params.yaml -csi.secrets secrets.yaml \
  -ginkgo.skip "already existing name and different capacity"
```

`--mount-access owner` avoids needing `user_allow_other` in /etc/fuse.conf
when not running as root. The skipped spec expects ALREADY_EXISTS for a
re-created volume with a different capacity, which requires the per-volume
capacity tracking described under Limitations.

CI runs this as `.github/workflows/csi-sanity.yml`.

### End-to-end (k3s)

`.github/workflows/csi-e2e-k3s.yml` deploys the shipped `deploy/` manifests
unmodified (only the image is overridden) on a single-node k3s cluster:
gateway from `e2e/gateway.yaml` (file:// storage on a hostPath), dynamic
provisioning of an RWX claim shared by two pods (`e2e/workload.yaml`),
write/read through both FUSE mounts, a gateway pod deletion proving
published mounts survive the restart via 9P reconnect, and reclaim of the
PV on PVC deletion.
