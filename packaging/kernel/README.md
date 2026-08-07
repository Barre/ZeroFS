# ZeroFS kernel module packages

Each repository channel has a `zerofs-kernel-client` selector for one tested
kernel. Selectors depend on co-installable module packages whose names contain
the target ID and exact kernel release. Module packages install:

```text
/lib/modules/<kernel-release>/updates/zerofs/zerofs.ko
```

The selector installs `/usr/lib/modules-load.d/zerofs.conf` and attempts
`modprobe zerofs` only when it targets the running kernel. Module packages run
`depmod`.

Selectors conflict with rolling kernel packages newer than their tested
target. Publishing a selector for a newer target raises that limit.

## Distribution channels

`targets.json` tracks Ubuntu 26.04, Ubuntu 24.04 HWE, Debian 13 backports,
Fedora 43 and 44, and openSUSE Tumbleweed. Ubuntu, Debian, and Fedora cover
x86-64 and arm64; openSUSE currently covers x86-64.

## Build

The builder requires `nfpm`, `modinfo`, Python, `readelf`, and `sha256sum`.
Signed packages additionally require OpenSSL. The module must be signed before
packaging when the target enforces module signatures.

```sh
packaging/kernel/build.sh \
  --module target/zerofs.ko \
  --kernel-release 7.0.0-28-generic \
  --target-id ubuntu-26.04-generic-7.0.0-28 \
  --channel-id ubuntu-26.04-generic-x86-64 \
  --arch x86_64 \
  --version 2.1.2 \
  --revision 1 \
  --source-commit 0123456789012345678901234567890123456789 \
  --source-tree-state clean \
  --tooling-commit 0123456789012345678901234567890123456789 \
  --tooling-tree-state clean \
  --kernel-package-dependency 'linux-image-7.0.0-28-generic (= 7.0.0-28.28)' \
  --kernel-upgrade-conflict 'linux-image-generic (>> 7.0.0-28.28)' \
  --license 'REVIEWED-LICENSE-EXPRESSION' \
  --family deb \
  --output-dir dist/kernel
```

Use `--family rpm` for RPM packages. Architectures may be written as
`amd64`/`x86_64` or `arm64`/`aarch64`.

`SOURCE_DATE_EPOCH`, when set, is recorded in each package's provenance
manifest. The module package also records the exact target, vermagic, module
size, and SHA-256 digest. The builder rejects a module whose name is not
`zerofs` or whose vermagic does not start with the requested kernel release.

`--kernel-package-dependency` is copied into the native package dependency
metadata and must pin the exact distro kernel package. Its syntax is
family-specific. For example:

```text
deb: linux-image-7.0.0-28-generic (= 7.0.0-28.28)
rpm: kernel-core-uname-r = 7.0.0-28.el10.x86_64
```

Each module package provides a versioned `zerofs-kernel-module` capability.

`--license` is mandatory. The release pipeline must pass the license expression
approved for the combined kernel payload; the builder does not infer one from
the repository or `MODULE_LICENSE`.

## Target catalog

`targets.json` is the canonical kernel target catalog:

```sh
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/targets.json matrix --scope ci
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/targets.json matrix --scope publish
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/targets.json matrix --scope discover
python3 packaging/kernel/kernel-targets.py \
  --manifest packaging/kernel/targets.json \
  field ubuntu-26.04-generic-7.0.0-28 kernel_release
```

Unsupported kernels are kept in `unsupported_targets`; validation prevents
them from entering CI. Every target names a stable repository channel and has
a positive package revision. Revisions for targets in the same channel must
increase in manifest order. A target cannot be published unless it is enabled
and CI-tested, and each channel can have only one current publication target.
The `publish` scope selects only targets with `publish: true`, so an
unpublished CI candidate cannot block a server release.

`ci: true` enables package and boot testing. `publish: true` adds a tested
target to its repository channel.

The hourly `kernel-target-updates` workflow checks each configured channel and
maintains one `kernel-update` issue per channel when a newer kernel is
available. It does not modify branches or pull requests. Each issue contains
the `discover` and `apply` commands for a manifest update. Manifest PRs run the
target build and QEMU smoke tests.

`build-target.sh` builds one manifest target in its configured container.
Builder images are digest-pinned except for openSUSE Tumbleweed, whose official
Docker Hub image exposes only the rolling `latest` tag. It selects the
external-module, generated-metadata, or self-contained build from the target
inputs. With `--module`, the host must already contain the matching
`/lib/modules/<release>` tree and `/boot/vmlinuz-<release>`.
The host also needs a statically linked, target-architecture `busybox`.
`build-target.sh` verifies its architecture and required applets before adding
it to the QEMU boot-test artifact.

For Fedora kernels that need generated Rust metadata, the container reads the
exact Fedora Rust build from the kernel's `auto.conf`. If the Fedora
repository has advanced, it installs that exact archived Rust build before
compiling metadata. Every Koji RPM is SHA-256-pinned. Targets using Koji's
signed archive also verify the configured Fedora key fingerprint. Other build
dependencies come from the configured Fedora repositories.
Ubuntu and Debian builds likewise install `rustfmt` from the same distribution
Rust build selected for `rustc`; versioned packages are preferred when
available. They also select the matching versioned Rust source package, which
matters for Ubuntu 24.04 HWE because the kernel toolchain is newer than the
distribution's default Rust toolchain. Ubuntu's arm64 kernel packages do not
currently ship the separate `linux-lib-rust` metadata package, so those jobs
reconstruct `libkernel.rmeta` from the exact snapshot source and configured
header tree. The container also prefers the compiler named by
`CONFIG_CC_VERSION_TEXT`, passes it to metadata and module builds, and records
both the configured and selected compiler in `build-info`. A same-family
fallback remains visible as `target_cc_exact=false`; the self-contained path
rejects such a fallback because it requires the exact target compiler.
The official openSUSE builder image supplies its trusted distribution keys;
snapshot refreshes never import a key advertised by repository metadata.

The self-contained path handles a narrower `CONFIG_RUST=n` case; it is not a
general compatibility layer. It needs the exact full kernel source, configured
build tree, `Module.symvers`, Rust sources/toolchain, and matching LLVM tools.
Those inputs are suitable for a controlled release job, not a headers-only
local build. It currently supports x86-64 only and does not make kernels with
an older netfs API compatible. Repository CI boots the Linux 6.18 floor with
`CONFIG_RUST=n`, then mounts and exercises ZeroFS through the resulting module.

```sh
ZEROFS_KERNEL_PACKAGE_LICENSE='REVIEWED-LICENSE-EXPRESSION' \
  packaging/kernel/build-target.sh \
    --manifest packaging/kernel/targets.json \
    --target-id ubuntu-26.04-generic-7.0.0-28 \
    --output-dir target/kernel-package
```

The output contains `artifact.json`, the prepared module, the module and
selector packages, the exact distro kernel image, the static boot-test
BusyBox, and loadable dependencies as raw `.ko` files in load order. The
upstream package version is read from `zerofs/Cargo.toml`; the release workflow
also requires tag `vX.Y.Z` to match it. Package revisions come from
`targets.json`. All module packages use the same `X.Y.Z` as the ZeroFS server
release. They do not depend on a locally installed server.

The artifact and both package provenance records identify the source and
packaging-tooling commits and whether each tree was clean. Set
`ZEROFS_REQUIRE_CLEAN_SOURCE=1` for release builds.

The boot modules and BusyBox are smoke-test payloads. They are neither
installed by the DEB/RPM packages nor published to the package repository.
The artifact records the BusyBox package identity, version banner, and exact
file digest so the CI input is attributable.

For signing, write the dedicated module-signing key and certificate into
private temporary files, select an independently trusted
kmodsign-compatible executable with `ZEROFS_MODULE_SIGNER`, and set
`ZEROFS_MODULE_SIGN_KEY` and `ZEROFS_MODULE_SIGN_CERT` to the key and PEM or
DER certificate paths. `ZEROFS_MODULE_SIGN_HASH` defaults to `sha256`. The
builder converts the public certificate to DER, stores its fingerprint in
provenance, and includes it with selectors. The builder never executes a signer
produced by a target build. Do not use the package-repository OpenPGP key for
module signing.

The release workflow keeps two identities separate:

- `GPG_PRIVATE_KEY` and `GPG_KEY_ID` sign DEB/RPM repository material;
- `KERNEL_MODULE_SIGNING_KEY` and `KERNEL_MODULE_SIGNING_CERT` sign
  `zerofs.ko`.

The module key is a passphrase-less PEM private key. Its matching PEM X.509
certificate must include the Code Signing extended key usage and remain valid
for at least 30 days when a release starts. Both private keys are supplied
through GitHub Secrets and should be backed up and access-controlled
independently.

Prebuilt selectors install the certificate at
`/usr/share/zerofs/zerofs-module-signing-cert.der`. If Secure Boot rejects the
module, its postinstall script leaves the package configured and prints:

```sh
sudo mokutil --import \
  /usr/share/zerofs/zerofs-module-signing-cert.der
```

Enrollment requires the distribution's MOK confirmation and a reboot; the
package never enrolls a key automatically. A module signature alone is not a
Secure Boot trust grant.

## Repository layout

Prebuilt module packages are safe to collect in a common artifact store because
their names include the target and exact kernel release.

The `zerofs-kernel-client` selector must be published only in the repository
channel for its target distribution, release, flavor, and architecture. Two
selectors with identical package versions but different dependencies must not
be published in one repository channel. Updating that selector is how a
channel moves users to a newly supported kernel.

## Publication

`kernel-artifacts.py` verifies target and release identity, file hashes, the
PKCS#7 module signature, and the shared signing certificate before staging
packages. `.github/workflows/_publish-repo.yml` signs and publishes each
repository channel.

## Prepare a module

`prepare-module.sh` copies a module to a new path, validates its name, exact
kernel release, and ELF architecture, and can strip debug data with an explicit
target strip executable:

```sh
packaging/kernel/prepare-module.sh \
  --input target/kernel/x86_64/7.0.0-28-generic/zerofs.ko \
  --output dist/zerofs.ko \
  --kernel-release 7.0.0-28-generic \
  --arch x86_64 \
  --strip-tool /usr/bin/x86_64-linux-gnu-strip
```

For a signed release artifact, pass a trusted signer with kmodsign-compatible
`HASH KEY CERT MODULE` arguments and the dedicated module-signing key pair.
`kmodsign` expects the certificate in DER form:

```sh
packaging/kernel/prepare-module.sh \
  --input target/zerofs.ko \
  --output dist/zerofs.ko \
  --kernel-release 7.0.0-28-generic \
  --arch x86_64 \
  --strip-tool /usr/bin/x86_64-linux-gnu-strip \
  --signer /usr/bin/kmodsign \
  --sign-key /secure/zerofs-module-signing-key.pem \
  --sign-cert /secure/zerofs-module-signing-key.der \
  --sign-hash sha256
```

Stripping always happens before signing. Signing arguments are all-or-none;
omitting them adds no signature, which permits unsigned CI artifacts when the
input is unsigned. The script never changes its input, atomically refuses to
overwrite its output, and verifies the signature metadata before publishing a
signed result.
