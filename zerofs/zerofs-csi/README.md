# zerofs-csi

CSI driver (`csi.zerofs.net`) that exposes a ZeroFS gateway (a leader/standby
pair) to Kubernetes pods.

## Architecture

A ZeroFS gateway serves one StorageClass. The gateway is a leader/standby pair
over one bucket/prefix: a leader serves the filesystem and a standby replicates
it and takes over within seconds if the leader fails, with at most one writer at
a time (the data db's writer epoch fences a deposed leader, independent of
timing; see the [HA design](../../documentation/src/app/high-availability)).
Volumes are plain subdirectories of one directory on the gateway filesystem
(`volumesRoot`, default `/volumes`): volume `pvc-1234` is the directory
`/volumes/pvc-1234`.

- The **controller** service creates and deletes those directories through
  the gateway's admin RPC (`CreateDirectory` / `RemoveDirectory`, both
  idempotent; removal renames into a trash directory and deletes in the
  background). It points at both nodes and provisions against whichever leads;
  a standby refuses the connection until it takes over, so it is skipped.
- The **node** service publishes a volume by spawning
  `zerofs mount <both-gateways> <target> --aname <volumesRoot>/<volume_id> --access all`,
  a FUSE mount of just that subdirectory over 9P. 9P was chosen over NFS/NBD
  because the client probes both nodes, follows the serving leader, and
  transparently reconnects and replays its protocol state (open file handles,
  locks, the aname-rooted attach), so a failover or network blip does not
  invalidate existing mounts. Open handles whose final filesystem link has
  already been removed are the exception: they are connection-local and make
  the logical session stale if it reconnects.

There is no staging step (`NodePublishVolume` mounts directly) and no attach
step (`attachRequired: false`). Both `gateway` and `adminEndpoint` are
comma-separated lists of the pair's node addresses; the data-plane mount and the
controller each probe the list and follow the serving leader, rerouting on
failover. Readiness is gated on the replication port so a standby stays
reachable for the leader's ships while its 9P/admin ports are still closed.

## StorageClass parameters

| key | required | meaning |
|---|---|---|
| `adminEndpoint` | yes | Both nodes' admin RPC URLs, comma-separated, e.g. `http://zerofs-gateway-a.zerofs.svc:7000,http://zerofs-gateway-b.zerofs.svc:7000`. The controller uses whichever leads. |
| `gateway` | yes | Both nodes' 9P addresses, comma-separated, e.g. `zerofs-gateway-a.zerofs.svc:5564,zerofs-gateway-b.zerofs.svc:5564`. The mount follows the serving leader. |
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

- `gateway-example.yaml` — the leader/standby gateway pair: two single-replica
  StatefulSets, per-node Services, and a config Secret each
- `csidriver.yaml` — the CSIDriver object
- `rbac.yaml` — service accounts and provisioner RBAC
- `controller.yaml` — controller Deployment with the csi-provisioner sidecar
- `node.yaml` — node DaemonSet (privileged, needs `/dev/fuse`) with
  node-driver-registrar and livenessprobe sidecars
- `storageclass-example.yaml` — StorageClass (both nodes, comma-separated) plus
  the provisioner Secret
- `networkpolicy-example.yaml` — gateway lockdown (9P to node pods, admin RPC to
  the controller, replication between the gateways); see the security note under
  Limitations

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
  boundary.** All three ports must be restricted to their legitimate clients:
  - 9P (5564) is the data plane. The aname is client-chosen and not a security
    boundary — anything that can reach it can attach any volume's subtree or
    the filesystem root — so it is open to every CSI node pod.
  - The admin RPC (7000) is a root-equivalent control plane that can trash any
    volume (`RemoveDirectory`). Its only client is the controller, so it is
    locked to the controller pod alone.
  - Replication (9000) is the leader → standby stream, heartbeats, and role
    discovery. Its only clients are the gateway pods themselves, so it is open
    just to them.

  Apply `deploy/networkpolicy-example.yaml` (adapted to your labels) alongside
  the gateway to enforce all three. A NetworkPolicy is a no-op on a CNI that
  does not enforce them (e.g. vanilla flannel / default k3s); on such a cluster
  there is no in-cluster protection for any port, so do not expose the gateway
  to untrusted pods.

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
the gateway pair from `e2e/gateway.yaml` (two nodes over one shared file://
store on a hostPath), dynamic provisioning of an RWX claim shared by two pods
(`e2e/workload.yaml`), write/read through both FUSE mounts, a **leader
failover** — it finds the serving leader (the node whose 9P port accepts) and
deletes it, then asserts the published mounts follow the promoted standby with
no remount — and reclaim of the PV on PVC deletion.
