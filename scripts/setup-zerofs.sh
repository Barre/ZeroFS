#!/usr/bin/env bash
set -euo pipefail

echo "::group::Setting up ZeroFS"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS-$ARCH" in
    linux-x86_64|linux-amd64)
        BINARY_NAME="zerofs-linux-amd64-pgo"
        ;;
    linux-aarch64|linux-arm64)
        BINARY_NAME="zerofs-linux-arm64-pgo"
        ;;
    darwin-x86_64|darwin-amd64)
        BINARY_NAME="zerofs-darwin-x86_64-pgo"
        ;;
    darwin-aarch64|darwin-arm64)
        BINARY_NAME="zerofs-darwin-aarch64-pgo"
        ;;
    *)
        echo "::error::Unsupported OS/architecture combination: $OS-$ARCH"
        exit 1
        ;;
esac

if [ "$ZEROFS_VERSION" = "latest" ]; then
    DOWNLOAD_URL="https://github.com/Barre/zerofs/releases/latest/download/zerofs-pgo-multiplatform.tar.gz"
else
    DOWNLOAD_URL="https://github.com/Barre/zerofs/releases/download/${ZEROFS_VERSION}/zerofs-pgo-multiplatform.tar.gz"
fi

echo "Downloading ZeroFS archive from: $DOWNLOAD_URL"
echo "Will extract binary: $BINARY_NAME"

# Download and extract the specific binary
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

if ! curl -fsSL "$DOWNLOAD_URL" | tar xz -C "$TEMP_DIR"; then
    echo "::error::Failed to download or extract ZeroFS archive"
    exit 1
fi

if [ ! -f "$TEMP_DIR/$BINARY_NAME" ]; then
    echo "::error::Binary $BINARY_NAME not found in archive"
    ls -la "$TEMP_DIR"
    exit 1
fi

sudo mv "$TEMP_DIR/$BINARY_NAME" /usr/local/bin/zerofs
rm -rf "$TEMP_DIR"

sudo chmod +x /usr/local/bin/zerofs

echo "ZeroFS installed at: /usr/local/bin/zerofs"

if [ "$OS" = "linux" ]; then
    if ! command -v mount.nfs &> /dev/null; then
        echo "Installing NFS utilities..."
        sudo apt-get update && sudo apt-get install -y nfs-common || sudo yum install -y nfs-utils || true
    fi
fi

mkdir -p "$SLATEDB_CACHE_DIR"

if [ -z "${MOUNT_PATH:-}" ]; then
    case "$OS" in
        linux)
            MOUNT_PATH="/mnt/zerofs"
            ;;
        darwin)
            MOUNT_PATH="/tmp/zerofs"
            ;;
        *)
            MOUNT_PATH="/tmp/zerofs"
            ;;
    esac
fi

if [ "$OS" = "darwin" ] && [[ "$MOUNT_PATH" == /mnt/* ]]; then
    echo "::warning::macOS doesn't have /mnt by default, using /tmp instead"
    MOUNT_PATH="/tmp/$(basename "$MOUNT_PATH")"
fi

sudo mkdir -p "$MOUNT_PATH"

# Outside the workspace and mode 0700, since the config holds secrets.
STATE_DIR=$(mktemp -d "$RUNNER_TEMP/zerofs.XXXXXX")
CONFIG="$STATE_DIR/zerofs.toml"
LOG="$STATE_DIR/zerofs.log"

cat > "$CONFIG" << EOF
[cache]
dir = "$SLATEDB_CACHE_DIR"
disk_size_gb = $SLATEDB_CACHE_SIZE_GB
memory_size_gb = $ZEROFS_MEMORY_CACHE_SIZE_GB

[storage]
url = "$OBJECT_STORE_URL"
encryption_password = "$ZEROFS_ENCRYPTION_PASSWORD"

[servers.nfs]
addresses = ["$ZEROFS_NFS_HOST:$ZEROFS_NFS_HOST_PORT"]
EOF

# Add AWS config if credentials are provided
if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    cat >> "$CONFIG" << EOF

[aws]
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
default_region = "$AWS_DEFAULT_REGION"
EOF
    
    # Add optional AWS settings
    if [ -n "${AWS_ENDPOINT:-}" ]; then
        echo "endpoint = \"$AWS_ENDPOINT\"" >> "$CONFIG"
    fi
    if [ "$AWS_ALLOW_HTTP" = "true" ]; then
        echo "allow_http = \"true\"" >> "$CONFIG"
    fi
fi

# Add Azure config if credentials are provided
if [ -n "${AZURE_STORAGE_ACCOUNT_NAME:-}" ] && [ -n "${AZURE_STORAGE_ACCOUNT_KEY:-}" ]; then
    cat >> "$CONFIG" << EOF

[azure]
storage_account_name = "$AZURE_STORAGE_ACCOUNT_NAME"
storage_account_key = "$AZURE_STORAGE_ACCOUNT_KEY"
EOF
fi

# GitHub turns every unset optional input into an empty-string env var. ZeroFS
# seeds the object_store builder from the environment.
for var in $(compgen -e); do
    case "$var" in
        AWS_*|AZURE_*|GOOGLE_*)
            if [ -z "${!var:-}" ]; then
                unset "$var"
            fi
            ;;
    esac
done

# Records the exit status, which the post step cannot wait() for.
nohup sh -c '"$1" run -c "$2" & echo $! > "$3/pid"; wait $!
    echo $? > "$3/exit.tmp" && mv "$3/exit.tmp" "$3/exit"' \
    _ /usr/local/bin/zerofs "$CONFIG" "$STATE_DIR" > "$LOG" 2>&1 &
# Before anything can fail, so the post step still stops it.
echo "state_dir=$STATE_DIR" >> "$GITHUB_STATE"

echo "Waiting for ZeroFS to start..."
for i in {1..30}; do
    if nc -z "$ZEROFS_NFS_HOST" "$ZEROFS_NFS_HOST_PORT" 2>/dev/null; then
        echo "ZeroFS is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "::error::ZeroFS failed to start within 30 seconds"
        cat "$LOG"
        exit 1
    fi
    sleep 1
done

echo "Mounting ZeroFS..."
case "$OS" in
    linux)
        MOUNT_OPTS="vers=3,async,nolock,tcp,port=$ZEROFS_NFS_HOST_PORT,mountport=$ZEROFS_NFS_HOST_PORT,hard"
        ;;
    darwin)
        MOUNT_OPTS="async,nolocks,vers=3,tcp,port=$ZEROFS_NFS_HOST_PORT,mountport=$ZEROFS_NFS_HOST_PORT,hard"
        ;;
    *)
        echo "::error::Mounting is not supported on OS: $OS"
        exit 1
        ;;
esac

if ! sudo mount -t nfs -o "$MOUNT_OPTS" "$ZEROFS_NFS_HOST":/ "$MOUNT_PATH"; then
    echo "::error::Failed to mount ZeroFS"
    cat "$LOG"
    exit 1
fi
# Resolved because macOS lists /tmp mounts under /private/tmp.
MOUNT_PARENT=$(cd "$(dirname "$MOUNT_PATH")" && pwd -P)
echo "mount_path=${MOUNT_PARENT%/}/$(basename "$MOUNT_PATH")" >> "$GITHUB_STATE"

echo "ZeroFS successfully mounted at: $MOUNT_PATH"
df -h "$MOUNT_PATH"

echo "mount-path=$MOUNT_PATH" >> "$GITHUB_OUTPUT"
echo "nfs-endpoint=$ZEROFS_NFS_HOST:$ZEROFS_NFS_HOST_PORT" >> "$GITHUB_OUTPUT"

echo "::endgroup::"
echo "::notice::ZeroFS is ready! Volume mounted at: $MOUNT_PATH"
