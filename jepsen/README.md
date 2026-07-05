# ZeroFS jepsen (local-fs) glue

A filesystem-correctness suite for ZeroFS built on Jepsen's
[`local-fs`](https://github.com/jepsen-io/local-fs). It generates random
histories of POSIX operations (`append`, `write`, `read`, `truncate`, `mkdir`,
`mv`, `ln`, `rm`, `fsync`, …), applies them to a real ZeroFS mount, and checks
the result against a purely-functional in-memory filesystem model; on a
mismatch `test.check` shrinks the history to a minimal failing example.

**The upstream suite is not vendored here.** This directory holds only the
ZeroFS-specific glue; the workflow (`.github/workflows/jepsen.yml`) clones
upstream `local-fs` at a pinned commit and applies the glue on top:

| file              | what it is |
|-------------------|------------|
| `zerofs.clj`      | The single-node ZeroFS backend (`--db zerofs`), copied to `src/jepsen/local_fs/db/zerofs.clj`. Boots a ZeroFS server against an S3 object store (a per-run MinIO prefix) and mounts it over 9P with `zerofs mount`. |
| `zerofs-ha.clj`   | The HA backend (`--db zerofs-ha`), copied to `src/jepsen/local_fs/db/zerofs_ha.clj`. Boots a leader + standby over one shared S3 store (a per-run MinIO prefix), mounted multi-target so the FUSE client re-routes on failover. Its fault is `:failover` (kill leader → standby promotes → full-restart to canonical roles), exposed via the `Failover` protocol. |
| `local-fs.patch`  | The delta to upstream `local_fs.clj` / `shell/{workload,checker,client}.clj` / `db/core.clj`: register `:zerofs` and `:zerofs-ha`, add the `--zerofs-*`, `--quickcheck-tests`, `--history-scale` and `--failover` CLI options, emit `:lose-unfsynced-writes`/`:failover` only on request, add the `Failover` protocol and model `:failover` as a global flush (no loss) rather than a crash, give the slow fault ops a longer op-timeout, and make `quickcheck` exit non-zero on a failing case (so CI can gate). |
| `run.sh`          | Wrapper that puts GNU coreutils first on `PATH` (the client parses coreutils error *strings*, which differ on uutils) and execs `lein`. |

Pinned upstream commit: **`0921306efc27d89a72b9041daaf9f854c57ac980`**
(EPL-2.0 OR GPL-2.0; see the upstream repo). Bump the pin and regenerate
`local-fs.patch` together.

## Why these choices

- **9P, not NFS.** Over 9P, `fsync` returns only after data is durable, which is
  the contract the model assumes. NFS `COMMIT` can return early, which would
  surface as spurious lost writes.
- **Write-through mount (`--writeback false`, the default).** The FUSE client
  then holds no un-fsynced page cache, so the only place un-fsynced writes live
  is the server's memtable, which is what makes the crash fault meaningful.
- **S3 store on MinIO, not `file://`.** ZeroFS fences via conditional writes
  (If-Match / If-None-Match), which the `file://` backend doesn't implement but
  MinIO does. The store still persists across a server restart, which the crash
  fault relies on; each run gets a fresh `s3://zerofs-jepsen/run-<uuid>` prefix
  so trials don't see each other's files.

## Running locally

Needs a JDK (17 works), [Leiningen](https://leiningen.org), FUSE
(`/dev/fuse`, `fusermount3`), and `start-stop-daemon`. Build the binary first
(`cargo build` in `zerofs/`), then assemble and run the suite the same way CI
does:

```bash
git clone https://github.com/jepsen-io/local-fs.git /tmp/local-fs
git -C /tmp/local-fs checkout 0921306efc27d89a72b9041daaf9f854c57ac980
cp jepsen/zerofs.clj    /tmp/local-fs/src/jepsen/local_fs/db/zerofs.clj
cp jepsen/zerofs-ha.clj /tmp/local-fs/src/jepsen/local_fs/db/zerofs_ha.clj
git -C /tmp/local-fs apply "$PWD/jepsen/local-fs.patch"
cp jepsen/run.sh /tmp/local-fs/

# The glue points every run at s3://zerofs-jepsen/run-<uuid> on a local MinIO
# (127.0.0.1:9000); start one and make the bucket first.
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/zerofs-jepsen

cd /tmp/local-fs
# POSIX conformance, no faults:
./run.sh run quickcheck --db zerofs \
  --zerofs-bin "$OLDPWD/zerofs/target/debug/zerofs" \
  --quickcheck-tests 200 --time-limit 60

# Crash consistency: the same run plus the fault that SIGKILLs the server and
# recovers from the object store.
./run.sh run quickcheck --db zerofs \
  --zerofs-bin "$OLDPWD/zerofs/target/debug/zerofs" \
  --lose-unfsynced-writes --quickcheck-tests 200 --time-limit 60

# HA failover consistency: a leader+standby cluster, where the fault fails the
# leader over to the standby. Failovers are slow (takeover_ttl + a full
# restart), so use shorter histories and fewer trials.
./run.sh run quickcheck --db zerofs-ha --failover \
  --zerofs-bin "$OLDPWD/zerofs/target/debug/zerofs" \
  --quickcheck-tests 20 --history-scale 30 --time-limit 120
```

Each quickcheck trial does a full ZeroFS setup/teardown, so lower
`--quickcheck-tests` for faster runs (CI uses a small count). Browse results
with `./run.sh serve` (http://localhost:8080).

## Crash consistency (`--lose-unfsynced-writes`)

The fault is a server **SIGKILL + restart**: it drops the memtable (the
un-fsynced tail), then recovers from the object store (a graceful stop would
flush on exit and defeat it).

This requires the checker model to match ZeroFS rather than lazyfs. The stock
model assumes lazyfs's contract: metadata written through on every op, fsync
per inode. ZeroFS is uniform write-back, and any fsync is a *global*
`db.flush()` barrier. `local-fs.patch` adapts the model to suit: every op, data
and metadata alike, stays in the cache until a flush, and an fsync flushes the
whole filesystem, so a crash reverts to the last fsync. The `[lsm]` config in
`zerofs.clj` makes that the only durability point (the periodic and
unflushed-size flushes are pushed out of the way), so the recovered state is
deterministic.

## HA failover (`--db zerofs-ha --failover`)

`zerofs-ha` runs a leader + standby over one shared S3 store (MinIO), mounted
multi-target. The `:failover` fault kills the leader; the standby promotes
(replaying and flushing the semi-synced tail to the shared store before it
serves), then `failover!` full-restarts to the canonical `a=leader / b=standby`
state. It gates on the standby reconnecting (semi-sync resumed) before returning,
so the next failover is safe.

Unlike the single-node crash, a failover **loses no acknowledged write**: ZeroFS
HA semi-syncs every acked write to the standby before acking (commit-then-apply).
So the model treats `:failover` as a global flush, not a cache-dropping crash:
everything written before the failover must still be present afterward, and the
same fs model-checker catches any divergence (a lost write, a resurrected delete,
a botched rename) across the failover. HA timeouts are shortened
(`heartbeat=1s, lease_ttl=3s, takeover_ttl=8s`) so failover is quick, but it
still takes ~20s (promotion + restart), so the fault ops get a longer op-timeout
and runs use shorter histories than conformance.

## Standalone HA suite (`jepsen/ha/`)

Separate from the local-fs glue above, `jepsen/ha/` is a self-contained
classic-Jepsen test (its own Leiningen project) that runs a real leader + standby
cluster over **MinIO** behind a multi-target FUSE mount. A nemesis kills the
leader, the standby, or both, or pauses MinIO, then heals; a fencing scenario
freezes the leader past its lease so the standby promotes, then thaws the stale
leader and confirms it self-fences. The workload is a concurrent set (add/remove)
plus statfs and per-file content checks, and the checker verifies that no
acknowledged write is lost, resurrected, or corrupted across a failover.

Unlike the local-fs glue it needs the `minio` and `mc` binaries (a real object
store, so MinIO is killable). Run it from `jepsen/ha`:

```bash
cd jepsen/ha
# run.sh uses a user-local JDK + lein + bin/{minio,mc} under ~/jepsen-ha
# (override the dir with ZEROFS_JEPSEN_TOOLS, or set ZEROFS_BIN / MINIO_BIN /
# MC_BIN / ZEROFS_JEPSEN_WORK, or pass the matching --*-bin / --work-dir flags).
./run.sh run test --time-limit 120 --concurrency 10
./run.sh run test --time-limit 120 --concurrency 10 --no-fsync  # tests semi-sync of un-fsync'd writes
./run.sh run serve            # browse results at http://localhost:8080
```

`.github/workflows/jepsen-ha.yml` runs it on CI (a 10-minute fsync soak plus a
shorter `--no-fsync` run), downloading MinIO itself.
