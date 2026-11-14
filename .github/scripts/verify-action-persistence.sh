#!/usr/bin/env bash
# Remounts the volume after the action's post step stopped it and checks a file survived.
set -euo pipefail

file=$1
expected=$2

state=$(mktemp)
GITHUB_STATE=$state GITHUB_OUTPUT=/dev/null \
    ZEROFS_VERSION=latest \
    MOUNT_PATH=/mnt/zerofs-verify \
    SLATEDB_CACHE_DIR="$RUNNER_TEMP/zerofs-verify-cache" \
    SLATEDB_CACHE_SIZE_GB=1 \
    ZEROFS_MEMORY_CACHE_SIZE_GB=0.25 \
    ZEROFS_NFS_HOST=127.0.0.1 \
    ZEROFS_NFS_HOST_PORT=2049 \
    bash scripts/setup-zerofs.sh

# shellcheck source=/dev/null
. "$state"
actual=$(sudo cat "$mount_path/$file" || true)
MOUNT_PATH=$mount_path ZEROFS_STATE_DIR=$state_dir bash scripts/cleanup-zerofs.sh

if [ "$actual" != "$expected" ]; then
    echo "::error::$file after remount: expected '$expected', got '$actual'"
    exit 1
fi
echo "$file survived shutdown and remount"
