# ZeroFS source-DKMS packages

`zerofs-kernel-client` ships the ZeroFS kernel-module source, not prebuilt
modules. The native package installs it under
`/usr/src/zerofs-<package-version>`, registers it with DKMS, and sets
`AUTOINSTALL=yes`. Installing the package builds for the running and newest
installed kernels when their headers are present; distro kernel-install hooks
build it when a kernel is added later.

The DEB and RPM variants carry portable dependencies on DKMS, kmod, and
compiler tooling. Matching kernel headers are package-managed prerequisites;
kernels without packaged Rust metadata also require their exact distribution
source package. The build hook uses only installed files and must never contact
a package repository or choose mutable build inputs itself.

The package does not install, select, or hold a distribution kernel. DKMS
excludes module builds below the Linux 6.18 floor and applies no version
exclusion above it. Every otherwise eligible kernel presented to DKMS is
attempted. A failed build leaves the new kernel without `zerofs.ko`. On
Debian-family systems, the DKMS kernel hook normally propagates that failure
and leaves kernel package configuration incomplete. Fedora- and openSUSE-family
kernel hooks may let the kernel transaction complete despite the failed module
build. Keep the preceding kernel installed as a boot fallback and check
`dkms status` before booting the new kernel. Removal unregisters the matching
DKMS source version and leaves other ZeroFS versions alone.

If no installed kernel has headers yet, `zerofs-kernel-client` configuration
succeeds after registering the source and prints a warning. The same package
configuration step warns and leaves the source registered when an existing
kernel lacks its kernel-specific Rust metadata or exact distribution source;
install the missing input and run `dkms autoinstall`. Below-floor kernels are
skipped. Other compilation and module-install failures fail this package
configuration step. When a kernel is installed later, its distribution DKMS
hook performs the build and determines whether a failure also fails the kernel
transaction, as described above.

## Build modes

The packaged wrapper examines the target build tree and chooses one of three
paths:

1. `CONFIG_RUST=y` with `rust/libkernel.rmeta` uses the normal external Rust
   module build.
2. `CONFIG_RUST=y` without packaged metadata regenerates the metadata from the
   matching distribution kernel source, then uses the normal build.
3. `CONFIG_RUST=n` on a compatible x86-64 kernel uses the self-contained
   builder. It compiles the kernel Rust support and ZeroFS together,
   internalizes the Rust support, and leaves the module importing normal C
   kernel symbols.

The third path is intentionally narrower. It requires the configured header
tree (`.config`, generated headers, and `Module.symvers`), the matching
distribution kernel source, Rust sources and compiler, and matching C and LLVM
tools. It does not make kernels older than Linux 6.18 or otherwise incompatible
kernel configurations work, and currently supports x86-64 only.

No mode fetches toolchains or source while DKMS is running. Exact toolchain and
source availability is an operating-system package prerequisite, not an
install-script fallback.

## Compatibility lock

`kernels.lock.json` records the exact distro kernels that CI has certified. It
is test input, not a runtime gate or a list of modules embedded in
`zerofs-kernel-client`:

```sh
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/kernels.lock.json matrix
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/kernels.lock.json discovery-matrix
```

The compact file groups discovery and reproducible build inputs by distro
stream and architecture. Target IDs, package family, and workflow fields are
derived by the loader. The retained locks give CI a current target and a
rollback target and are ordered oldest to newest. The lock has no separate
package revision because changing it does not publish the source package.

ZeroFS requires Linux 6.18 or newer because older kernels predate the required
netfs API. The lock's stream and architecture keys are the source of truth for
CI coverage; the [kernel-client guide](../../documentation/src/app/kernel-client/page.mdx#compatibility)
shows the user-facing matrix.

## Kernel update flow

The hourly `kernel-target-updates` workflow discovers all channels and
maintains one aggregate update PR. Successful discoveries survive unrelated
channel failures, while conflicting edits to the same channel fail closed.
Force-with-lease and default-branch comparisons prevent stale automation from
overwriting newer lock changes.

The PR builds the actual source-DKMS package in a clean target environment,
installs the exact kernel, headers, source, and toolchain, and lets DKMS build
`zerofs.ko`. CI checks the resulting module and boots that kernel in QEMU,
where it loads ZeroFS and runs mount and I/O smoke tests. A new kernel is not
certified merely because compilation succeeds.

If the kernel is incompatible, the lock PR stays red. The ZeroFS fix lands
through a normal source PR on the default branch; the next discovery run
reconciles the lock branch on top of it and reruns compatibility CI. Merging
the lock then records compatibility, and the fix reaches users in the next
normal ZeroFS release. An hourly no-op closes an obsolete update PR. Lock
reconciliation uses force-with-lease, while repository writers use their
shared `queue: max` concurrency group; these are separate race controls for
separate resources.

## Reproducible target builds

`build-target.sh` resolves one lock in its digest-pinned builder image. Its
kernel, source-package, toolchain, and boot-test inputs are verified before
use:

```sh
target_id=$(packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/kernels.lock.json matrix |
  jq -r '.include[0].id')
packaging/kernel/build-target.sh \
  --manifest packaging/kernel/kernels.lock.json \
  --target-id "$target_id" \
  --source-package staged \
  --output-dir target/kernel-artifact
```

Use the RPM package for an RPM-family target. The source package can be built
with `packaging/kernel/kernel-source-package.py --output staged`; that helper
requires nFPM. It derives the package version from `zerofs/Cargo.toml` and uses
`SOURCE_DATE_EPOCH` when provided or the checked-out commit time otherwise. It
applies that timestamp to every nFPM input so rebuilding the same source
produces the same unsigned package.

With Docker available, run the package-manager checks locally with:

```sh
kernel/ci/dkms-package-install-smoke.sh \
  staged/deb/zerofs-kernel-client_*_all.deb \
  staged/rpm/zerofs-kernel-client-*.noarch.rpm
```

The script uses builder images resolved from the lock: Ubuntu and Fedora run
the complete install/remove lifecycle, while Tumbleweed checks RPM dependency
resolution. `build-target.sh` performs the full package install and DKMS build
for an exact locked kernel.

Tumbleweed uses the Docker Hub base image because
`registry.opensuse.org` has pruned old digest-addressed rolling images in
practice. Package inputs still come from the locked openSUSE historical
snapshot. Fedora jobs obtain any exact archived Rust build named by the target
kernel, reconstruct signed RPMs from Koji's retained signature headers, and
verify each package digest and signing-key fingerprint. Ubuntu and Debian jobs
likewise select the matching versioned Rust sources and compiler tools. These
networked resolution steps happen only in controlled CI; they are not part of
the installed DKMS hook.

Each target artifact includes the module, exact kernel image, loadable boot
dependencies, and a static target-architecture BusyBox. These are smoke-test
payloads only and are not installed by or published inside the source package.
Release builds require the tag version to match `zerofs/Cargo.toml`.

## Secure Boot

Modules are built on the client, so DKMS uses the machine-local signing key
configured by the distribution. ZeroFS does not publish a universal module
private key or certificate. A Secure Boot machine may require its local DKMS
key to be enrolled through the distribution's MOK workflow before
`modprobe zerofs` succeeds. Package scripts never enroll a key automatically.

## Publication

Kernel-lock updates only certify compatibility; `zerofs-kernel-client` ships
through normal ZeroFS releases. Unified repository layout, signing,
serialization, and the immutable release gate are documented in
[native package publishing](../README.md#publishing).
