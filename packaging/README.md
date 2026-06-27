# Native packages (.deb / .rpm)

Signed apt and yum repositories for ZeroFS, hosted on Cloudflare R2. The
`Publish deb/rpm packages` workflow (`.github/workflows/release-packages.yml`)
builds packages from the PGO release binaries and publishes the repos on a `v*`
tag push, pulling the same `zerofs-pgo-multiplatform.tar.gz` release asset the
Docker build uses.

Packages cover **amd64** and **arm64** only (the statically linked musl release
binaries). The other architectures stay on the install script / tarball.

## What a package installs

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

## End-user install

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

## One-time infrastructure setup

### 1. R2 bucket + public domain
- Create an R2 bucket, e.g. `zerofs-packages`.
- R2 -> bucket -> Settings -> Public access -> **Connect a custom domain**, e.g.
  `pkgs.zerofs.net`. Objects are then served at `https://pkgs.zerofs.net/<key>`.

### 2. R2 API token
- R2 -> Manage R2 API Tokens -> create a token with **Object Read & Write**
  scoped to that bucket. Note the Access Key ID, Secret, and your Account ID
  (the S3 endpoint is `https://<account-id>.r2.cloudflarestorage.com`).

### 3. Signing key
A dedicated, **passphrase-less** key used only for this repo. GitHub Secrets is
the protection boundary; a passphrase-less key keeps CI signing non-interactive.

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
| `GPG_KEY_ID` | long key id / fingerprint from step 3 |

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
  deb/                 apt repo (dists/, pool/) managed by deb-s3
  rpm/                 yum repo (*.rpm + repodata/, signed repomd.xml)
  zerofs.gpg           armored public key
  zerofs.repo          yum repo descriptor
```

## Publishing
Automatic on a `v*` tag push. To (re)publish for an existing tag, run the
workflow manually (`workflow_dispatch`) with the tag, e.g. `v1.4.1`. The job is
serialized by a concurrency group because it read-modify-writes repo metadata in
the bucket. Packages are also attached to the GitHub Release if one exists.

## Building locally
No keys needed; signing happens only in CI.

```bash
# from the repo root, with a prebuilt binary at ./zerofs
ARCH=amd64 VERSION=1.4.1 BIN=./zerofs \
  envsubst '${ARCH} ${VERSION} ${BIN}' < packaging/nfpm.yaml > /tmp/nfpm.yaml
nfpm pkg -f /tmp/nfpm.yaml -p deb -t .    # or -p rpm
```

Inspect: `dpkg-deb -c zerofs_*.deb`, `rpm -qlvp zerofs-*.rpm`.
