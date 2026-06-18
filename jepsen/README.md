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
| `zerofs.clj`      | The ZeroFS backend, copied to `src/jepsen/local_fs/db/zerofs.clj`. Boots a ZeroFS server against a local `file://` object store and mounts it over 9P with `zerofs mount`. |
| `local-fs.patch`  | The delta to upstream `local_fs.clj`/`shell/workload.clj`: register `:zerofs` in the db registry, add the `--zerofs-*` and `--quickcheck-tests` CLI options, emit `:lose-unfsynced-writes` only on request, and make `quickcheck` exit non-zero on a failing case (so CI can gate). |
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
- **`file://` object store.** Self-contained; no S3/MinIO. It persists on local
  disk across a server restart, which the crash fault relies on.

## Running locally

Needs a JDK (17 works), [Leiningen](https://leiningen.org), FUSE
(`/dev/fuse`, `fusermount3`), and `start-stop-daemon`. Build the binary first
(`cargo build` in `zerofs/`), then assemble and run the suite the same way CI
does:

```bash
git clone https://github.com/jepsen-io/local-fs.git /tmp/local-fs
git -C /tmp/local-fs checkout 0921306efc27d89a72b9041daaf9f854c57ac980
cp jepsen/zerofs.clj /tmp/local-fs/src/jepsen/local_fs/db/zerofs.clj
git -C /tmp/local-fs apply "$PWD/jepsen/local-fs.patch"
cp jepsen/run.sh /tmp/local-fs/

cd /tmp/local-fs
# POSIX conformance, no faults:
./run.sh run quickcheck --db zerofs \
  --zerofs-bin "$OLDPWD/zerofs/target/debug/zerofs" \
  --quickcheck-tests 200 --time-limit 60

# Crash consistency: the same run plus the fault that SIGKILLs the server and
# recovers from the on-disk store.
./run.sh run quickcheck --db zerofs \
  --zerofs-bin "$OLDPWD/zerofs/target/debug/zerofs" \
  --lose-unfsynced-writes --quickcheck-tests 200 --time-limit 60
```

Each quickcheck trial does a full ZeroFS setup/teardown, so lower
`--quickcheck-tests` for faster runs (CI uses a small count). Browse results
with `./run.sh serve` (http://localhost:8080).

## Crash consistency (`--lose-unfsynced-writes`)

The fault is a server **SIGKILL + restart**: it drops the memtable (the
un-fsynced tail), then recovers from the on-disk store (a graceful stop would
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
