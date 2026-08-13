# ZeroFS kernel-client packages

`zerofs-kernel-client` installs a DKMS-managed client under
`/usr/src/zerofs-<package-version>`, registers it with DKMS, and sets
`AUTOINSTALL=yes`. Package installation asks DKMS to provide the module for the
running and newest installed kernels when their headers are present; distro
kernel-install hooks repeat that operation when a kernel is added later.

The DKMS build first requests an exact, signed module that ZeroFS CI has built
and boot-tested for the installed kernel package. The client derives its URL
from the local package database, verifies the dedicated ZeroFS X.509 signature,
the signed distro/header-package identity, architecture, and vermagic, then
hands it back to DKMS for normal installation and any configured machine-local
signing. The lookup does not use the current operating-system release version,
so a retained kernel continues to resolve after an OS upgrade. A missing or
temporarily unreachable object falls through to source compilation; a
downloaded object that fails verification is a hard error.

The DEB and RPM variants depend only on DKMS and the tools needed to fetch and
verify a published module. Matching kernel headers are package-managed
prerequisites; kernels without packaged Rust metadata also require their exact
distribution source package for the source fallback. That fallback uses only
installed files and never invokes a package manager. It is an escape hatch for
prepared development systems; published modules are the normal support path.

The package does not install, select, or hold a distribution kernel. DKMS
excludes module builds below the Linux 6.18 floor and applies no version
exclusion above it. Every otherwise eligible kernel presented to DKMS is
attempted. If neither a published object nor the optional source prerequisites
are available, the eligible DKMS build fails rather than completing without
`zerofs.ko`. Authentication, compilation, and installation errors are hard
failures too. Debian-family kernel hooks normally propagate these failures and
leave kernel package configuration incomplete; Fedora- and openSUSE-family
hooks may let the transaction complete. Keep the preceding kernel installed as
a boot fallback and check `dkms status` before booting the new kernel. Removal
unregisters the matching DKMS source version and leaves other ZeroFS versions
alone.

With no installed headers, package configuration registers the source, warns,
and succeeds. Once an eligible kernel with headers is presented, publish its
module or install the source prerequisites before retrying `dkms autoinstall`.

## Source fallback modes

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

No fallback mode fetches toolchains or source while DKMS is running. Exact
toolchain and source availability is an operating-system package prerequisite,
not an install-script fallback.

## Compatibility lock

`kernels.lock.json` records the exact distro kernels that CI has certified and
for which it publishes signed modules. It is not embedded in
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
rollback target and are ordered oldest to newest. A lock update publishes
modules for the latest stable ZeroFS package without changing that package.
The workflow downloads and audits the GPG-signed DEB and RPM already present
in the public repositories; it does not reconstruct a hypothetical package
revision from newer tooling.

ZeroFS requires Linux 6.18 or newer because older kernels predate the required
netfs API. The lock's stream and architecture keys are the source of truth for
CI coverage; the [kernel-client guide](../../documentation/src/app/kernel-client/page.mdx#compatibility)
shows the user-facing matrix.

## Kernel update flow

Every 15 minutes, the `kernel-target-updates` workflow discovers all channels and
maintains one aggregate update PR. Successful discoveries survive unrelated
channel failures, while conflicting edits to the same channel fail closed.
Force-with-lease and default-branch comparisons prevent stale automation from
overwriting newer lock changes.

Ordinary PR workflows are skipped when the lock is the only changed file. The
trusted updater instead dispatches `ci` for the exact update-branch commit and
keeps one PR comment linked to that CI run. Each reconciled head updates the
same comment; merge when the linked run's `ci / required` job passes.

The PR builds the actual kernel-client package in a clean target environment,
installs the exact kernel, headers, source, and toolchain, and forces the DKMS
source fallback to build `zerofs.ko`. CI checks the resulting module and boots
that kernel in QEMU, where it loads ZeroFS and runs mount and I/O smoke tests. A
new kernel is not certified merely because compilation succeeds.

If the kernel is incompatible, the lock PR stays red. The ZeroFS fix lands
through a normal source PR on the default branch; the next discovery run
reconciles the lock branch on top of it and reruns compatibility CI. Merging
the lock records compatibility and triggers publication of the boot-tested
module for the latest stable ZeroFS release. Source fixes still reach users in
the next normal ZeroFS release. A no-op run closes an obsolete update PR. Lock
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

## Signing and Secure Boot

Published modules carry a ZeroFS signature used to authenticate the downloaded
artifact. DKMS preserves it and may append the machine-local key configured by
the distribution. A Secure Boot machine may require that local DKMS key to be
enrolled through the distribution's MOK workflow before `modprobe zerofs`
succeeds. Package scripts never enroll a key automatically.

## Publication

`zerofs-kernel-client` ships through normal ZeroFS releases. Releases and later
kernel-lock updates add immutable objects below `kernel-modules/v1`; old
objects remain available for rollback kernels. Unified repository layout,
signing, serialization, and the release gate are documented in
[native package publishing](../README.md#publishing).
