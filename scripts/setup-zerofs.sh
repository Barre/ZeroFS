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
    linux-armv7*)
        BINARY_NAME="zerofs-linux-armv7-pgo"
        ;;
    linux-i686)
        BINARY_NAME="zerofs-linux-i686-pgo"
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

# Create ZeroFS configuration file
cat > zerofs-action.toml << EOF
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
    cat >> zerofs-action.toml << EOF

[aws]
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
default_region = "$AWS_DEFAULT_REGION"
EOF
    
    # Add optional AWS settings
    if [ -n "${AWS_ENDPOINT:-}" ]; then
        echo "endpoint = \"$AWS_ENDPOINT\"" >> zerofs-action.toml
    fi
    if [ "$AWS_ALLOW_HTTP" = "true" ]; then
        echo "allow_http = \"true\"" >> zerofs-action.toml
    fi
fi

# Add Azure config if credentials are provided
if [ -n "${AZURE_STORAGE_ACCOUNT_NAME:-}" ] && [ -n "${AZURE_STORAGE_ACCOUNT_KEY:-}" ]; then
    cat >> zerofs-action.toml << EOF

[azure]
storage_account_name = "$AZURE_STORAGE_ACCOUNT_NAME"
storage_account_key = "$AZURE_STORAGE_ACCOUNT_KEY"
EOF
fi

nohup /usr/local/bin/zerofs run -c zerofs-action.toml > zerofs.log 2>&1 &
ZEROFS_PID=$!
echo $ZEROFS_PID > zerofs.pid

echo "Waiting for ZeroFS to start..."
for i in {1..30}; do
    if nc -z "$ZEROFS_NFS_HOST" "$ZEROFS_NFS_HOST_PORT" 2>/dev/null; then
        echo "ZeroFS is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "::error::ZeroFS failed to start within 30 seconds"
        cat zerofs.log
        exit 1
    fi
    sleep 1
done

echo "Mounting ZeroFS..."
case "$OS" in
    linux)
        sudo mount -t nfs -o vers=3,async,nolock,tcp,port="$ZEROFS_NFS_HOST_PORT",mountport="$ZEROFS_NFS_HOST_PORT",hard "$ZEROFS_NFS_HOST":/ "$MOUNT_PATH"
        ;;
    darwin)
        sudo mount -t nfs -o async,nolocks,vers=3,tcp,port="$ZEROFS_NFS_HOST_PORT",mountport="$ZEROFS_NFS_HOST_PORT",hard "$ZEROFS_NFS_HOST":/ "$MOUNT_PATH"
        ;;
    *)
        echo "::error::Mounting is not supported on OS: $OS"
        exit 1
        ;;
esac

if mount | grep -q "$MOUNT_PATH"; then
    echo "ZeroFS successfully mounted at: $MOUNT_PATH"
    df -h "$MOUNT_PATH"
else
    echo "::error::Failed to mount ZeroFS"
    cat zerofs.log
    exit 1
fi

echo "mount-path=$MOUNT_PATH" >> $GITHUB_OUTPUT
echo "nfs-endpoint=$ZEROFS_NFS_HOST:$ZEROFS_NFS_HOST_PORT" >> $GITHUB_OUTPUT

cat > cleanup-zerofs.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "::group::Cleaning up ZeroFS"

if mount | grep -q "$MOUNT_PATH"; then
    echo "Unmounting $MOUNT_PATH..."
    sudo umount "$MOUNT_PATH" || sudo umount -f "$MOUNT_PATH" || true
fi

if [ -f zerofs.pid ]; then
    PID=$(cat zerofs.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "Stopping ZeroFS (PID: $PID)..."
        kill $PID || true
        sleep 2
        kill -9 $PID 2>/dev/null || true
    fi
    rm -f zerofs.pid
fi

if [ -f zerofs.log ] && grep -i error zerofs.log > /dev/null 2>&1; then
    echo "::warning::ZeroFS had errors during execution:"
    grep -i error zerofs.log | head -20
fi

echo "::endgroup::"
EOF
chmod +x cleanup-zerofs.sh

echo "$PWD/cleanup-zerofs.sh" >> $GITHUB_PATH

echo "::endgroup::"
echo "::notice::ZeroFS is ready! Volume mounted at: $MOUNT_PATH"
