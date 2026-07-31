# Native Rust kernel module

This directory contains a Rust Linux filesystem module with a
small target-compiled C bridge for inline-only netfslib helpers. It registers
`zerofs` with VFS and mounts a ZeroFS server by speaking ZeroFS's private
`9P2000.L.Z` dialect directly over TCP or an AF_UNIX stream socket. It does not
use FUSE or Linux v9fs.

The implemented surface covers most of the core VFS entry points: mounting,
lookup, `getattr`, attribute and size changes, regular-file and directory
creation, namespace mutations, hard and symbolic links, special-node metadata,
persistent opens, `atomic_open`, directory iteration, netfslib-backed buffered
reads, dirty-folio writeback and writable mapping, netfslib-backed direct I/O,
`fallocate`, `SEEK_DATA`/`SEEK_HOLE`, POSIX record locks and `flock`, verified
durability barriers, and remote `statfs`. One connection per mount carries
tagged requests whose replies may complete out of order; a dedicated receiver
routes each reply to its waiting VFS caller.

## Connection parameters

The mount source selects the transport:

| Source | Transport |
|---|---|
| `none` or `tcp` | IPv4/TCP using `server_ipv4` and `server_port` |
| `10.0.0.1` or `10.0.0.1:5564` | IPv4/TCP, port `5564` when omitted |
| `fd00::1` or `[fd00::1]:5564` | IPv6/TCP, port `5564` when omitted |
| `tcp://10.0.0.1:5564` | The same, written explicitly |
| `/absolute/socket/path` | Filesystem-path AF_UNIX stream socket |
| `unix:/absolute/socket/path` | Explicit filesystem-path AF_UNIX stream socket |
| `@name` or `unix:@name` | Linux abstract AF_UNIX stream socket |
| `a,b` | One leader/standby pair, comma separated |

Filesystem paths and abstract names are limited by Linux's 108-byte
`sun_path`; pathname addresses may contain at most 107 bytes so the terminating
NUL also fits. Relative paths are rejected.

The module parameters are load-time-only:

| Parameter | Default | Meaning |
|---|---:|---|
| `server_ipv4` | `0x7f000001` | IPv4 address used by `none`/`tcp` sources (`127.0.0.1`); `0` disables it |
| `server_ipv4_peer` | `0` | The HA peer for `none`/`tcp` sources; `0` disables it |
| `server_port` | `5564` | TCP port used by `none`/`tcp` sources and by targets that omit one |
| `request_timeout_ms` | `5000` | Send, admission, reply-wait, and handshake timeout |
| `reconnect_grace_ms` | `120000` | Longest a request waits for reconnect and session replay |

Module parameters carry integers only, so the `none`/`tcp` source expresses two
fixed peers sharing `server_port`. Mixed TCP and Unix targets belong on the
mount source.

The session and consistency settings are mount options:

| Option | Default | Meaning |
|---|---:|---|
| `consistency=relaxed\|strict` | `relaxed` | One-second VFS/page caching, or remotely revalidated unbuffered I/O |
| `msize=N` | `10485760` | Requested `9P2000.L.Z` message size in bytes (`4096`–`10485760`) |

## Build and test

Every target kernel needs all of the following:

- x86_64 or little-endian arm64 Linux 6.18 or newer
- `CONFIG_MODULES=y`
- `CONFIG_NETFS_SUPPORT=y` or `CONFIG_NETFS_SUPPORT=m`
- `CONFIG_UNIX=y` or `CONFIG_UNIX=m`
- `CONFIG_FILE_LOCKING=y`
- matching kernel headers and `Module.symvers` when module versions are enabled

The normal external-module build additionally requires:

- `CONFIG_RUST=y`
- `CONFIG_EXTENDED_MODVERSIONS=y` when `CONFIG_MODVERSIONS=y`
- prebuilt Rust kernel metadata
- the exact Rust compiler build used for that metadata
- `bindgen` and Clang/libclang

Here `arm64` means the 64-bit AArch64 ABI. The 32-bit ARM/ARMv7 ABI is not
supported.

Linux 6.18 is the source-compatibility floor. It is the first upstream 6.x
release with the Rust abstractions this module currently consumes. Kbuild does
not select behavior from the release number: target-generated bindings,
target-derived layout assertions, and the netfslib compatibility bridge decide
whether the exact kernel tree is buildable.

Build against the exact target kernel:

```bash
cd kernel
make KDIR=/lib/modules/$(uname -r)/build
make test
```

Module build products and target-generated bindings are written below
`../target/kernel/<architecture>/<kernel-release>/`. Run `make module-path`
with the same `KDIR` to print the resulting `.ko` path. Set `MO` to an
absolute directory to use a different Kbuild output tree.

For a cross-built arm64 kernel, pass the same architecture and C-toolchain
selection used for that kernel:

```bash
cd kernel
make ARCH=arm64 CROSS_COMPILE=aarch64-linux-gnu- \
  KDIR=/path/to/arm64/kernel-build
```

The Makefile reads `CONFIG_RUSTC_VERSION_TEXT` and
`CONFIG_BINDGEN_VERSION_TEXT`. It prefers matching versioned distribution
binaries (for example `/usr/bin/rustc-1.92` and `/usr/bin/bindgen-0.71`) over
unversioned binaries and a rustup compiler in `PATH`. Set `RUSTC` or `BINDGEN`
explicitly when a matching tool lives elsewhere. The target's prebuilt kernel
Rust metadata is sufficient for this external module; it does not need a
per-user rustup `rust-src` tree.

Headers alone are not enough. The target build tree must contain
`rust/libkernel.rmeta`. On Ubuntu this is normally supplied by the matching
`linux-lib-rust-$(uname -r)` package. The exact compiler named by
`CONFIG_RUSTC_VERSION_TEXT` must also be present. Do not copy Rust metadata
from another kernel build: Rust symbol names and module-version CRCs can differ
even when the relevant C headers are identical.

Do not insert `zerofs.ko` into a different kernel release from the one it was
built against.

### Self-contained prebuilt modules

Release CI can also build ZeroFS for a kernel with `CONFIG_RUST=n`. This mode
compiles the exact kernel tree's Rust support and ZeroFS together as LLVM
bitcode, internalizes the Rust implementation, eliminates unreachable support
code, and passes the resulting object through the target kernel's ordinary
module and modpost machinery. The finished module imports only exported C
kernel symbols; it does not require Rust support in the running kernel.

This is a prebuilt/manual path. It requires the exact full kernel source and
configured build tree, including a complete `Module.symvers`, the
kernel-compatible Rust compiler with its standard-library sources, Python 3,
`bindgen`, and matching Clang/LLVM tools. Distribution release jobs have those
inputs; installed headers normally do not.

The self-contained path supports x86_64 kernels built with GCC or Clang. It
uses the target kernel's original C compiler and binutils for its C objects,
module link, `objtool`, and `modpost`; matching Clang/LLVM tools are used only
for the combined Rust bitcode. GCC targets using GCC plugins, KASAN, KCSAN,
KMSAN, GCOV, LTO, or CFI are rejected rather than mixing incompatible
instrumentation. The path also rejects FineIBT with BHI arity checking and
requires Rust-aware `pahole` support when the target emits BTF.

The normal external-module path additionally supports little-endian arm64.

Run the build against a clean source tree and its separate out-of-tree build
directory:

```bash
make -C kernel self-contained \
  KERNEL_SRC=/path/to/linux-source \
  KDIR=/path/to/linux-build
make -s -C kernel KDIR=/path/to/linux-build module-path
```

The source checkout must remain immutable from the target-kernel build through
the module build. The script checks that `KDIR/source`, the release, and the
non-Rust configuration match, but a kernel build directory does not contain a
digest of every source file from which it was produced.

The build uses a private support tree below `target/`, does not enable Rust in
the target kernel, and fails if the final module retains an undefined Rust
runtime or helper symbol. As with every prebuilt external module, the result is
specific to the target distribution, kernel release/ABI, flavor, architecture,
and configuration.

The repository CI builds and boots an exact Clang-built Linux 6.18
`CONFIG_RUST=n` target, covering the supported version floor, and verifies that
the self-contained module can mount ZeroFS and perform basic I/O. A daily
canary tests the latest final kernel and latest active release candidate on
x86-64 and arm64.

## Smoke test

Start a ZeroFS server with its 9P listener on `127.0.0.1:5564` or
`/tmp/zerofs.9p.sock`, then boot the matching kernel and run:

```bash
cd kernel
module_path=$(make -s module-path)
sudo modprobe netfs
sudo insmod "$module_path" \
  server_ipv4=0x7f000001 server_port=5564 \
  request_timeout_ms=5000
grep zerofs /proc/filesystems

sudo mkdir -p /mnt/zerofs-kmod
# TCP:
sudo mount -t zerofs -o consistency=relaxed,msize=10485760 \
  none /mnt/zerofs-kmod
# Or the configured Unix socket:
# sudo mount -t zerofs -o consistency=relaxed,msize=10485760 \
#   /tmp/zerofs.9p.sock /mnt/zerofs-kmod
# Strict metadata and unbuffered file I/O:
# sudo mount -t zerofs -o consistency=strict \
#   /tmp/zerofs.9p.sock /mnt/zerofs-kmod
ls -la /mnt/zerofs-kmod
find /mnt/zerofs-kmod -maxdepth 2 -print
stat /mnt/zerofs-kmod/<existing-path>
cat /mnt/zerofs-kmod/<existing-regular-file>

sudo mkdir /mnt/zerofs-kmod/kmod-smoke
sudo sh -c 'printf "created through VFS\n" > /mnt/zerofs-kmod/kmod-smoke/file'
sudo sh -c 'printf "O_TRUNC works\n" > /mnt/zerofs-kmod/kmod-smoke/file'
sudo truncate -s 7 /mnt/zerofs-kmod/kmod-smoke/file
sudo chmod 0640 /mnt/zerofs-kmod/kmod-smoke/file
sudo chown "$(id -u):$(id -g)" /mnt/zerofs-kmod/kmod-smoke/file
sudo touch -a -m -t 202401020304 /mnt/zerofs-kmod/kmod-smoke/file

sudo ln /mnt/zerofs-kmod/kmod-smoke/file \
  /mnt/zerofs-kmod/kmod-smoke/hardlink
sudo ln -s file /mnt/zerofs-kmod/kmod-smoke/symlink
readlink /mnt/zerofs-kmod/kmod-smoke/symlink
sudo mv /mnt/zerofs-kmod/kmod-smoke/hardlink \
  /mnt/zerofs-kmod/kmod-smoke/renamed

sudo mkfifo /mnt/zerofs-kmod/kmod-smoke/fifo
sudo mknod /mnt/zerofs-kmod/kmod-smoke/null-metadata c 1 3
stat /mnt/zerofs-kmod/kmod-smoke/fifo \
  /mnt/zerofs-kmod/kmod-smoke/null-metadata

sudo fallocate -l 1M /mnt/zerofs-kmod/kmod-smoke/allocated
sudo fallocate --punch-hole --keep-size -o 4096 -l 4096 \
  /mnt/zerofs-kmod/kmod-smoke/allocated
sudo fallocate --zero-range -o 8192 -l 4096 \
  /mnt/zerofs-kmod/kmod-smoke/allocated
sudo fallocate --zero-range --keep-size -o 16384 -l 4096 \
  /mnt/zerofs-kmod/kmod-smoke/allocated

sudo sync -f /mnt/zerofs-kmod/kmod-smoke/file
find /mnt/zerofs-kmod/kmod-smoke -maxdepth 1 -ls

sudo rm /mnt/zerofs-kmod/kmod-smoke/symlink \
  /mnt/zerofs-kmod/kmod-smoke/renamed \
  /mnt/zerofs-kmod/kmod-smoke/file \
  /mnt/zerofs-kmod/kmod-smoke/fifo \
  /mnt/zerofs-kmod/kmod-smoke/null-metadata \
  /mnt/zerofs-kmod/kmod-smoke/allocated
sudo rmdir /mnt/zerofs-kmod/kmod-smoke

sudo umount /mnt/zerofs-kmod
sudo rmmod zerofs
```

Mount fails if negotiation or root lookup cannot complete within the configured
timeout.

## Current limitations

- Linux does not provide a stable out-of-tree Rust or VFS module ABI. Build for
  the exact kernel release, configuration, flavor, and architecture that will
  load the module; a `.ko` built for a nearby kernel is not portable.
- The 9P transport does not authenticate its peer or encrypt traffic, and the
  server trusts the numeric credentials supplied by its client. Restrict the
  listener or AF_UNIX socket to trusted clients.
- Extended attributes and ACLs are not exposed. Buffered append is serialized
  within one mount; direct append uses netfslib's local EOF snapshot. The
  protocol has no server-atomic append across mounts or clients.
- `consistency=strict` uses unbuffered I/O and does not support `mmap`.
  Unbuffered I/O also rejects `RWF_NOWAIT`.
- Persistent FS-Cache is not enabled. An open-unlinked fid becomes stale after
  reconnect; losing a fid that held a recorded byte-range lock ends the logical
  session because its lock guarantee cannot be preserved.
