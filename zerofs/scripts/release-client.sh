#!/usr/bin/env bash
# Stamp the "client family" version into the lockstep crates.
#
#   ./scripts/release-client.sh X.Y.Z
#
# zerofs-client and zerofs-ffi always share ONE version (this is distinct from
# the server's [workspace.package].version). zerofs-ffi/Cargo.toml::version is
# the single source of truth that everything else derives from; this script is
# what writes it (and keeps zerofs-client in lockstep).
#
# It also rewrites zerofs-ffi's path dependency on zerofs-client to pin an exact
# version (`version = "=X.Y.Z"`) alongside the path, so the two never drift.
#
# Idempotent: running it twice with the same version is a no-op.
set -euo pipefail

ver="${1:?usage: release-client.sh X.Y.Z}"

# Validate a plain semver (optionally with a -prerelease / +build suffix).
if ! [[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
    echo "release-client.sh: '$ver' is not a valid semver (expected X.Y.Z)" >&2
    exit 1
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ws="$(cd "$here/.." && pwd)"          # the `zerofs` workspace dir
client_toml="$ws/zerofs-client/Cargo.toml"
ffi_toml="$ws/zerofs-ffi/Cargo.toml"

for f in "$client_toml" "$ffi_toml"; do
    [ -f "$f" ] || { echo "release-client.sh: missing $f" >&2; exit 1; }
done

# Set the top-of-file `version = "..."` in a [package] table. We anchor on the
# first `version = "..."` line, which in both files is the package version.
set_package_version() {
    local file="$1" v="$2"
    perl -0pi -e 'BEGIN{$v=shift} s/^version = "[^"]*"/version = "$v"/m' "$v" "$file"
}

set_package_version "$client_toml" "$ver"
set_package_version "$ffi_toml" "$ver"

# Pin zerofs-ffi's dependency on zerofs-client to this exact version, keeping
# the path so local/CI builds resolve against the in-tree crate. Matches a line
# of the form:  zerofs-client = { path = "../zerofs-client", ... }
perl -0pi -e 'BEGIN{$v=shift}
    s{^zerofs-client = \{ path = "\.\./zerofs-client".*\}$}
     {zerofs-client = { path = "../zerofs-client", version = "=$v", default-features = false }}m' \
    "$ver" "$ffi_toml"

# Verify the rewrite landed (guard against an upstream format change).
grep -qF "version = \"=$ver\"" "$ffi_toml" || {
    echo "release-client.sh: failed to pin zerofs-client dependency in $ffi_toml" >&2
    exit 1
}

echo "release-client.sh: set client-family version to $ver"
echo "  $client_toml"
echo "  $ffi_toml (zerofs-client pinned to =$ver)"
