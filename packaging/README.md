# Native packages (.deb / .rpm)

Signed apt and yum repositories for ZeroFS, hosted on Cloudflare R2. The
`Publish native packages` workflow (`.github/workflows/release-packages.yml`)
builds userspace packages from the PGO release binaries on a `v*` tag push.
The DKMS-managed kernel-client package is published into the same `deb` and
`rpm` repositories.

Userspace packages cover **amd64** and **arm64** only (the statically linked
musl release binaries). The other architectures stay on the install script /
tarball. Kernel compatibility CI uses the matrix derived from
`packaging/kernel/kernels.lock.json`; the userspace architecture list does not
imply kernel-module availability.

## What the userspace package installs

| Path | Notes |
| --- | --- |
| `/usr/bin/zerofs` | the binary |
| `/lib/systemd/system/zerofs.service` | systemd unit, runs as the `zerofs` user, **not enabled by default** |
| `/etc/zerofs/config.toml` | config (conffile, preserved on upgrade) |
| `/etc/zerofs/zerofs.env` | secrets, `0600` (conffile) |

Install creates a `zerofs` system user. Secrets are read from `zerofs.env` and
referenced as `${VAR}` in `config.toml` (ZeroFS expands env vars in the config).
The service is intentionally left disabled: it can't start until you set the
storage URL and credentials.

```
sudo systemctl daemon-reload   # done by the package
$EDITOR /etc/zerofs/zerofs.env     # ZEROFS_PASSWORD, AWS_ACCESS_KEY_ID, ...
$EDITOR /etc/zerofs/config.toml    # [storage] url, protocols/ports
sudo systemctl enable --now zerofs
```

The unit is hardened (`ProtectSystem=strict`, private tmp, restricted caps).
The cache lives in `CacheDirectory=/var/cache/zerofs` and the unix sockets in
`RuntimeDirectory=/run/zerofs`. If you point `[cache] dir` at another path,
grant write access with a drop-in:

```
sudo systemctl edit zerofs
# [Service]
# ReadWritePaths=/your/path
```

## End-user userspace install

**Debian / Ubuntu**

```bash
curl -fsSL https://pkgs.zerofs.net/zerofs.gpg | sudo gpg --dearmor -o /usr/share/keyrings/zerofs.gpg
echo "deb [signed-by=/usr/share/keyrings/zerofs.gpg] https://pkgs.zerofs.net/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/zerofs.list
sudo apt update && sudo apt install zerofs
```

**Fedora / RHEL / Rocky**

```bash
curl -fsSL https://pkgs.zerofs.net/zerofs.repo | sudo tee /etc/yum.repos.d/zerofs.repo
sudo dnf install zerofs
```

(`pkgs.zerofs.net` above is the `REPO_BASE_URL` configured below.)

## Native kernel client

Each supported package family publishes `zerofs-kernel-client`. It registers
with DKMS and fetches an authenticated exact module that CI has boot-tested
for the installed kernel package. DKMS enables automatic installation for
eligible installed and future kernels and excludes
releases below the Linux 6.18 floor and places no upper bound on eligible
kernels. The DEB and RPM variants depend on the portable fetch-and-verification
tools. Matching kernel headers are still required. If a signed module is
unavailable, the package attempts a best-effort source build from already
installed Rust metadata or exact distribution source without invoking a
package manager. Kernel discovery CI builds, boots, signs, and publishes exact
distro modules. If neither path can produce a module for an eligible kernel,
the DKMS build fails so distributions that propagate hook failures do not
silently finish the kernel transaction. See the [kernel-client
guide](../documentation/src/app/kernel-client/page.mdx) for user instructions
and [the kernel packaging README](kernel/README.md) for source fallback and
update details.

## One-time infrastructure setup

### 1. R2 bucket + public domain
- Create an R2 bucket, e.g. `zerofs-packages`.
- R2 -> bucket -> Settings -> Public access -> **Connect a custom domain**, e.g.
  `pkgs.zerofs.net`. Objects are then served at `https://pkgs.zerofs.net/<key>`.

### 2. R2 API token
- R2 -> Manage R2 API Tokens -> create a token with **Object Read & Write**
  scoped to that bucket. Note the Access Key ID, Secret, and your Account ID
  (the S3 endpoint is `https://<account-id>.r2.cloudflarestorage.com`).

### 3. Repository signing key

Use a dedicated, **passphrase-less** OpenPGP key only for apt/RPM packages and
repository metadata. GitHub Secrets is the protection boundary; a
passphrase-less key keeps CI signing non-interactive.

```bash
gpg --batch --gen-key <<EOF
%no-protection
Key-Type: RSA
Key-Length: 4096
Name-Real: ZeroFS Packages
Name-Email: packages@zerofs.net
Expire-Date: 0
EOF

gpg --list-secret-keys --keyid-format=long      # -> the KEYID
gpg --armor --export-secret-keys <KEYID>        # -> GPG_PRIVATE_KEY secret
```

Keep the private key backed up offline. RSA-4096 is used for the broadest
apt/dnf compatibility.

### 4. Kernel module signing key

The public trust anchor is committed at
`packaging/kernel/zerofs-module-signing-cert.pem`. Store its matching,
passphrase-less private key in the `KERNEL_MODULE_SIGNING_KEY` secret and keep
an offline backup. Never commit the private key.

For a new trust root, generate the pair with:

```bash
signing_dir=$(mktemp -d /tmp/zerofs-module-signing.XXXXXXXX)
chmod 0700 "$signing_dir"
openssl req -new -x509 -newkey rsa:4096 -nodes -sha256 -days 3650 \
  -subj '/CN=ZeroFS Kernel Module Signing' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'keyUsage=critical,digitalSignature' \
  -addext 'extendedKeyUsage=codeSigning,1.3.6.1.4.1.2312.16.1.2' \
  -keyout "$signing_dir/zerofs-module-signing-key.pem" \
  -out packaging/kernel/zerofs-module-signing-cert.pem
chmod 0600 "$signing_dir/zerofs-module-signing-key.pem"
printf 'Private key: %s\n' "$signing_dir/zerofs-module-signing-key.pem"
```

The private key is created outside the checkout in a mode-0700 temporary
directory. Copy it into the GitHub secret and an encrypted offline backup, then
remove the temporary copy. Commit only the certificate. Rotating it requires a
new kernel-client package; already-published module objects remain available to
packages pinned to the previous certificate. The certificate is a public,
stable trust anchor: the private key exposes its public key, but does not
contain the certificate's serial number, validity interval, extensions, or
original self-signature. CI verifies that the `KERNEL_MODULE_SIGNING_KEY`
public key matches the certificate before starting the kernel matrix.

### 5. GitHub Actions configuration

Settings -> Secrets and variables -> Actions.

**Variables**

| Name | Example |
| --- | --- |
| `REPO_BASE_URL` | `https://pkgs.zerofs.net` |
| `R2_BUCKET` | `zerofs-packages` |
| `GPG_KEY_ID` | full fingerprint from step 3 |

**Secrets**

| Name | Value |
| --- | --- |
| `R2_ACCOUNT_ID` | Cloudflare account id |
| `R2_ACCESS_KEY_ID` | R2 token access key id |
| `R2_SECRET_ACCESS_KEY` | R2 token secret |
| `GPG_PRIVATE_KEY` | armored private key from step 3 |
| `KERNEL_MODULE_SIGNING_KEY` | dedicated module-signing private key |

### Bucket layout produced

```
<bucket>/
  deb/                 unified apt repo (userspace + kernel packages)
  kernel-modules/v1/   immutable signed exact-kernel modules
  rpm/                 unified rpm repo (userspace + kernel packages)
  zerofs.gpg           armored public key
  zerofs.repo          yum repo descriptor
```

## Publishing

Repository writers are serialized with `queue: max`, and publication preserves
old versions. Kernel-client publication also rejects downgrades and changed
equal-version packages. Userspace and kernel-client packages are published from
normal `v*` releases or by manually dispatching an existing release tag.

The release workflow resolves the tag to one immutable source commit and uses
the workflow commit for packaging and publishing tooling. This permits an old
tag to be repackaged with an explicit override in
`PACKAGE_REVISION_OVERRIDES`; retry an existing release run when no bump is
intended. Before a kernel-client package is published, its DEB and RPM are
installed and removed in clean distribution containers, then the same package
is exercised for every retained kernel lock and boot-tested with the released
ZeroFS server binary.

When a packaging-only change introduces a new kernel-module layout, publish
the revised `zerofs-kernel-client` package before merging a kernel-lock update.
For this initial rollout, merge the packaging changes, then dispatch
`release-packages.yml` from the current default branch with tag `v2.2.3`; the
revision override publishes `2.2.3-2`. Merging code alone does not publish the
package. The lock publisher audits the already-published package's layout and
trust certificate and rejects legacy packages rather than constructing an
unpublished package revision from current tooling.

Kernel-lock changes publish exact modules for the latest stable package but do
not publish new DEB or RPM packages. They build from the already-published,
repository-verified userspace and kernel-client packages, so module publication
cannot drift to an unreleased package revision. See the
[kernel update flow](kernel/README.md#kernel-update-flow) for discovery, failed
compatibility checks, and lock-branch reconciliation.

## Building userspace packages locally

No keys are needed; package and repository signing happen only in CI.

```bash
# from the repo root, with a prebuilt binary at ./zerofs
ARCH=amd64 VERSION=1.4.1 BIN=./zerofs \
  envsubst '${ARCH} ${VERSION} ${BIN}' < packaging/nfpm.yaml > /tmp/nfpm.yaml
nfpm pkg -f /tmp/nfpm.yaml -p deb -t .    # or -p rpm
```

Inspect: `dpkg-deb -c zerofs_*.deb`, `rpm -qlvp zerofs-*.rpm`.
