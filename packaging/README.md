# Native packages (.deb / .rpm)

Signed apt and yum repositories for ZeroFS, hosted on Cloudflare R2. The
`Publish userspace packages` workflow (`.github/workflows/release-packages.yml`)
builds userspace packages from the PGO release binaries on a `v*` tag push,
pulling the same `zerofs-pgo-multiplatform.tar.gz` release asset the Docker
build uses.

Userspace packages cover **amd64** and **arm64** only (the statically linked
musl release binaries). The other architectures stay on the install script /
tarball. Native kernel-client packages use a separate kernel-target matrix; the
userspace architecture list does not imply kernel-module availability.

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
`RuntimeDirectory=/run/zerofs`. If you point `[cache] dir` or a `file://` WAL at
another path, grant write access with a drop-in:

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

`zerofs-kernel-client` selects the tested kernel and module package for its
repository channel. Every channel uses a module built for one exact kernel.

The selector installs the tested distro kernel and prebuilt module, attempts
`modprobe zerofs` when the target is the running kernel, and installs
`/usr/lib/modules-load.d/zerofs.conf` for subsequent boots. Installing a module
for a kernel that is not running prepares the next boot without trying to load
it into the wrong kernel. Package removal does not unload a module that is in
use.

Use the same `X.Y.Z` release on the server. The module does not depend on a
local server package. The selector prevents the channel's rolling kernel
package from advancing beyond the tested version.

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

### 4. Prebuilt kernel module signing identity

Kernel modules use a separate private key and X.509 certificate. Do not reuse
the OpenPGP repository key. The release workflow accepts a passphrase-less PEM
private key and matching PEM certificate, verifies that they match, and
requires the certificate to remain valid for at least 30 days. The certificate
must include the Code Signing extended key usage.

```bash
umask 077
openssl req -new -x509 -newkey rsa:4096 -sha256 -nodes \
  -keyout zerofs-module-signing-key.pem \
  -out zerofs-module-signing-cert.pem \
  -days 3650 \
  -subj '/CN=ZeroFS Kernel Module/' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'keyUsage=critical,digitalSignature' \
  -addext 'extendedKeyUsage=codeSigning,1.3.6.1.4.1.2312.16.1.2'
chmod 0600 zerofs-module-signing-key.pem
chmod 0644 zerofs-module-signing-cert.pem
```

Back up the private key offline. Replacing it requires users with Secure Boot
to enroll the replacement certificate.

This signature identifies a prebuilt module but does not automatically make
its key trusted by Secure Boot. Prebuilt selectors install the public
certificate at `/usr/share/zerofs/zerofs-module-signing-cert.der`. If
`modprobe` is rejected, enroll it through the distribution's MOK flow:

```bash
sudo mokutil --import \
  /usr/share/zerofs/zerofs-module-signing-cert.der
```

Complete the firmware/MOK prompt and reboot. The modules-load entry retries the
load on the next boot; package installation remains successful while the key
awaits enrollment.

### 5. GitHub Actions configuration

Settings -> Secrets and variables -> Actions.

**Variables**

| Name | Example |
| --- | --- |
| `REPO_BASE_URL` | `https://pkgs.zerofs.net` |
| `R2_BUCKET` | `zerofs-packages` |
| `GPG_KEY_ID` | full fingerprint from step 3 |
| `KERNEL_MODULE_PACKAGE_LICENSE` | reviewed license expression for the combined module payload |

**Secrets**

| Name | Value |
| --- | --- |
| `R2_ACCOUNT_ID` | Cloudflare account id |
| `R2_ACCESS_KEY_ID` | R2 token access key id |
| `R2_SECRET_ACCESS_KEY` | R2 token secret |
| `GPG_PRIVATE_KEY` | armored private key from step 3 |
| `KERNEL_MODULE_SIGNING_KEY` | passphrase-less PEM private key from step 4 |
| `KERNEL_MODULE_SIGNING_CERT` | matching PEM X.509 certificate from step 4 |

### Bucket layout produced

```
<bucket>/
  deb/                 apt repo (dists/, pool/) managed by deb-s3
  rpm/                 yum repo (*.rpm + repodata/, signed repomd.xml)
  kernel/apt/<distro>/<release>/<flavor>/<arch>/
                       exact-kernel apt channel + zerofs-kernel.list
  kernel/rpm/<distro>/<release>/<flavor>/<arch>/
                       exact-kernel yum channel + zerofs-kernel.repo
  zerofs.gpg           armored public key
  zerofs.repo          yum repo descriptor
```

## Publishing

Repository publication is serialized and preserves previously published
package versions.

Userspace publishing runs on a `v*` tag push. To (re)publish for an existing
tag, run the workflow manually with the tag, for example `v1.4.1`. Kernel
packages are published for stable releases and targets marked `publish: true`.

## Building userspace packages locally

No keys are needed; package and repository signing happen only in CI.

```bash
# from the repo root, with a prebuilt binary at ./zerofs
ARCH=amd64 VERSION=1.4.1 BIN=./zerofs \
  envsubst '${ARCH} ${VERSION} ${BIN}' < packaging/nfpm.yaml > /tmp/nfpm.yaml
nfpm pkg -f /tmp/nfpm.yaml -p deb -t .    # or -p rpm
```

Inspect: `dpkg-deb -c zerofs_*.deb`, `rpm -qlvp zerofs-*.rpm`.
