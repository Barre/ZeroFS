#!/usr/bin/env bash
set -euo pipefail

readonly version=2.47.0
readonly sha256=0660ca602b2d2d2ae4781a06c692b3eeb9d437ffea05b831d76e41f4a3188783

(( $# == 0 )) || {
  echo "usage: $0" >&2
  exit 2
}
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || {
  echo "the pinned nFPM archive only supports Linux x86_64" >&2
  exit 1
}

: "${RUNNER_TEMP:?RUNNER_TEMP must be set}"
: "${GITHUB_PATH:?GITHUB_PATH must be set}"
: "${HOME:?HOME must be set}"

install_dir="$HOME/.local/bin"
archive="$RUNNER_TEMP/nfpm_${version}_Linux_x86_64.tar.gz"
url="https://github.com/goreleaser/nfpm/releases/download"
url+="/v${version}/nfpm_${version}_Linux_x86_64.tar.gz"

install -d -m 0755 "$install_dir"
curl -fsSL "$url" -o "$archive"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check -
tar xzf "$archive" -C "$install_dir" nfpm
printf '%s\n' "$install_dir" >>"$GITHUB_PATH"
