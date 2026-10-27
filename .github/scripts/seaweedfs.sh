#!/usr/bin/env bash
# Pinned local S3 fixture for Linux/macOS runners and native-client guests.
set -euo pipefail

# Keep the CSI image in zerofs/zerofs-csi/e2e/seaweedfs.yaml on this release too.
readonly version=4.47
readonly tools_dir="${RUNNER_TEMP:-/tmp}/seaweedfs-bin"
readonly work_dir="${RUNNER_TEMP:-/tmp}/seaweedfs"

install_weed() {
  local platform sha256 archive
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)
      platform=linux_amd64
      sha256=31fb804858885f9e7f18b6d3b1da09e824baac3e6a55b5a62c3c4c77e6ed6d7d ;;
    Linux-aarch64|Linux-arm64)
      platform=linux_arm64
      sha256=ba5c9def9ff98f78becdf309a50a9f906922f6383a7c4c772d01ede3cc012e16 ;;
    Darwin-x86_64)
      platform=darwin_amd64
      sha256=45475223d51b78efb413cb7a29c005c7e9e0cc3f0dfe13d10413e7f85c479b6a ;;
    Darwin-arm64)
      platform=darwin_arm64
      sha256=3ad466db33e83d0b103f90a06997feeb1cc42fd536e6cbdcdded975778f64fda ;;
    *) echo "Unsupported SeaweedFS platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
  esac
  mkdir -p "$tools_dir"
  archive="$tools_dir/$platform.tar.gz"
  curl -fsSL --retry 3 \
    "https://github.com/seaweedfs/seaweedfs/releases/download/$version/$platform.tar.gz" \
    -o "$archive"
  if command -v sha256sum >/dev/null; then
    printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check -
  else
    printf '%s  %s\n' "$sha256" "$archive" | shasum -a 256 --check -
  fi
  tar -xzf "$archive" -C "$tools_dir" weed
  "$tools_dir/weed" version
}

start_store() {
  local buckets=$1 store_pid bucket ready
  if [[ -f "$work_dir/seaweedfs.pid" ]] &&
    kill -0 "$(cat "$work_dir/seaweedfs.pid")" 2>/dev/null; then
    echo "SeaweedFS is already running; stop it before starting another fixture" >&2
    return 1
  fi
  install_weed
  mkdir -p "$work_dir/data"
  # mini runs master, volume, filer and S3 in one process, and creates buckets.
  # Keep the same credentials in workflow configs and the Jepsen/CSI fixtures.
  AWS_ACCESS_KEY_ID=zerofsadmin AWS_SECRET_ACCESS_KEY=zerofsadmin \
    nohup "$tools_dir/weed" mini -dir="$work_dir/data" \
      -ip=127.0.0.1 -ip.bind=127.0.0.1 -s3.port=9000 \
      -admin.ui=false -webdav=false -s3.port.iceberg=0 -s3.port.lance=0 \
      -master.volumeSizeLimitMB=128 -bucket="$buckets" \
      >"$work_dir/seaweedfs.log" 2>&1 </dev/null &
  store_pid=$!
  printf '%s\n' "$store_pid" >"$work_dir/seaweedfs.pid"

  # A listening socket does not guarantee that S3 auth and bucket creation work.
  # Bound each request as well as the overall wait, then fail with server logs.
  local bucket_names
  IFS=, read -r -a bucket_names <<<"$buckets"
  for _ in {1..60}; do
    kill -0 "$store_pid" 2>/dev/null || break
    ready=true
    for bucket in "${bucket_names[@]}"; do
      if ! curl -fsSI --connect-timeout 1 --max-time 2 \
        --aws-sigv4 aws:amz:us-east-1:s3 --user zerofsadmin:zerofsadmin \
        "http://127.0.0.1:9000/$bucket" >/dev/null 2>&1; then
        ready=false
        break
      fi
    done
    if [[ "$ready" == true ]]; then
      echo "SeaweedFS $version is ready; buckets: $buckets"
      return
    fi
    sleep 1
  done
  cat "$work_dir/seaweedfs.log" >&2
  kill "$store_pid" 2>/dev/null || true
  echo "SeaweedFS failed to make its S3 buckets ready" >&2
  return 1
}

case "${1:-}" in
  install) [[ $# == 1 ]] && install_weed ;;
  start) [[ $# == 2 && -n $2 ]] && start_store "$2" ;;
  stop)
    if [[ -f "$work_dir/seaweedfs.pid" ]]; then
      kill "$(cat "$work_dir/seaweedfs.pid")" 2>/dev/null || true
    fi ;;
  *) echo "Usage: $0 {install|start bucket[,bucket...]|stop}" >&2; exit 2 ;;
esac
