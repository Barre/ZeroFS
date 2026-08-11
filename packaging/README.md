# Native packages (.deb / .rpm)

Signed apt and yum repositories for ZeroFS, hosted on Cloudflare R2. The
`Publish native packages` workflow (`.github/workflows/release-packages.yml`)
builds userspace packages from the PGO release binaries on a `v*` tag push.
The source-DKMS kernel-client package is published into the same `deb` and
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

Each supported package family publishes `zerofs-kernel-client`. It installs
the ZeroFS module source under `/usr/src`, registers it with DKMS, and enables
automatic builds for eligible installed and future kernels. DKMS excludes
releases below the Linux 6.18 floor and places no upper bound on eligible
kernels. The DEB and RPM variants declare their portable build dependencies.
Matching kernel headers are still required; kernels without packaged Rust
metadata also need their exact distribution source package. The DKMS hook uses
installed package inputs and never downloads dependencies itself.

For `CONFIG_RUST=y`, the wrapper uses the normal external-module build and can
regenerate missing Rust metadata from the packaged kernel source. Compatible
x86-64 kernels with `CONFIG_RUST=n` use the self-contained build. Kernel
discovery CI still builds and boots exact distro kernels, but the lock file is
a compatibility record rather than package payload input. See the [kernel-client
guide](../documentation/src/app/kernel-client/page.mdx) for user instructions
and [the kernel packaging README](kernel/README.md) for build and update
details.

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

### 4. GitHub Actions configuration

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

### Bucket layout produced

```
<bucket>/
  deb/                 unified apt repo (userspace + kernel packages)
  rpm/                 unified rpm repo (userspace + kernel packages)
  zerofs.gpg           armored public key
  zerofs.repo          yum repo descriptor
```

## Publishing

Repository writers are serialized with `queue: max`, and publication preserves
old versions. Source-DKMS publication also rejects downgrades and changed
equal-version packages. Userspace and source-DKMS packages are published from
normal `v*` releases (or a manual dispatch for a tag containing this packaging
tooling) into the unified `deb` and `rpm` repositories.

The release workflow resolves the tag to one commit before building anything
and uses that commit for every source and publishing-tool checkout. Before a
source-DKMS package is published, its DEB and RPM are installed and removed in
clean distribution containers, then the same package is built for every
retained kernel lock and boot-tested with the released ZeroFS server binary.

Kernel-lock changes certify compatibility but do not publish packages. See the
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
