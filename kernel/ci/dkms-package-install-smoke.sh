#!/usr/bin/env bash

set -euo pipefail

readonly script_name=${0##*/}

die() {
    printf '%s: %s\n' "$script_name" "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 ||
        die "required command not found: $1"
}

require_healthy_dkms_registration() {
    local detail line status version=$1
    local prefix="zerofs/$version"
    local kernel_status_pattern=

    kernel_status_pattern='^[A-Za-z0-9._+~:-]+,[[:space:]]+'
    kernel_status_pattern+='[A-Za-z0-9._+~:-]+:[[:space:]]+(built|installed)$'

    status=$(dkms status -m zerofs -v "$version") ||
        die "cannot read DKMS status for $prefix"
    [[ -n "$status" ]] || die "DKMS did not register $prefix"
    printf '%s\n' "$status"
    while IFS= read -r line; do
        if [[ "$line" == "$prefix: added" ]]; then
            continue
        fi
        if [[ "$line" == "$prefix, "* ]]; then
            detail=${line#"$prefix, "}
            if [[ "$detail" =~ $kernel_status_pattern ]]; then
                continue
            fi
        fi
        die "unexpected DKMS status for $prefix: $line"
    done <<< "$status"
}

inside_deb() {
    local status

    : "${EXPECTED_VERSION:?EXPECTED_VERSION is required}"
    export DEBIAN_FRONTEND=noninteractive
    [[ $(dpkg-deb -f /tmp/zerofs-kernel-client.deb Package) == \
       zerofs-kernel-client ]]
    [[ $(dpkg-deb -f /tmp/zerofs-kernel-client.deb Version) == \
       "$EXPECTED_VERSION" ]]
    apt-get update
    apt-get install -y --no-install-recommends \
        /tmp/zerofs-kernel-client.deb
    [[ $(dpkg-query -W -f='${Version}' zerofs-kernel-client) == \
       "$EXPECTED_VERSION" ]]
    [[ -d "/usr/src/zerofs-$EXPECTED_VERSION" ]]
    require_healthy_dkms_registration "$EXPECTED_VERSION"
    apt-get remove -y zerofs-kernel-client
    [[ ! -e "/usr/src/zerofs-$EXPECTED_VERSION" ]]
    status=$(dkms status -m zerofs -v "$EXPECTED_VERSION" 2>/dev/null || true)
    [[ -z "$status" ]]
    echo "DEB install and removal lifecycle passed"
}

inside_fedora() {
    local status

    : "${EXPECTED_VERSION:?EXPECTED_VERSION is required}"
    [[ $(rpm -qp --qf '%{NAME}' /tmp/zerofs-kernel-client.rpm) == \
       zerofs-kernel-client ]]
    [[ $(rpm -qp --qf '%{VERSION}-%{RELEASE}' \
        /tmp/zerofs-kernel-client.rpm) == "$EXPECTED_VERSION" ]]
    dnf install -y --setopt=install_weak_deps=False \
        /tmp/zerofs-kernel-client.rpm
    [[ $(rpm -q --qf '%{VERSION}-%{RELEASE}' zerofs-kernel-client) == \
       "$EXPECTED_VERSION" ]]
    [[ -d "/usr/src/zerofs-$EXPECTED_VERSION" ]]
    require_healthy_dkms_registration "$EXPECTED_VERSION"
    dnf remove -y --setopt=clean_requirements_on_remove=False \
        zerofs-kernel-client
    [[ ! -e "/usr/src/zerofs-$EXPECTED_VERSION" ]]
    status=$(dkms status -m zerofs -v "$EXPECTED_VERSION" 2>/dev/null || true)
    [[ -z "$status" ]]
    echo "Fedora RPM install and removal lifecycle passed"
}

inside_opensuse() {
    : "${EXPECTED_VERSION:?EXPECTED_VERSION is required}"
    [[ $(rpm -qp --qf '%{NAME}' /tmp/zerofs-kernel-client.rpm) == \
       zerofs-kernel-client ]]
    [[ $(rpm -qp --qf '%{VERSION}-%{RELEASE}' \
        /tmp/zerofs-kernel-client.rpm) == "$EXPECTED_VERSION" ]]
    zypper --non-interactive refresh
    zypper --non-interactive --no-gpg-checks install --dry-run \
        --no-recommends /tmp/zerofs-kernel-client.rpm
    echo "openSUSE RPM dependency resolution passed"
}

case ${1:-} in
    --inside-deb)
        [[ $# -eq 1 ]] || die "--inside-deb takes no arguments"
        inside_deb
        exit
        ;;
    --inside-fedora)
        [[ $# -eq 1 ]] || die "--inside-fedora takes no arguments"
        inside_fedora
        exit
        ;;
    --inside-opensuse)
        [[ $# -eq 1 ]] || die "--inside-opensuse takes no arguments"
        inside_opensuse
        exit
        ;;
esac

[[ $# -eq 2 ]] || {
    echo "usage: $script_name PACKAGE.deb PACKAGE.rpm" >&2
    exit 2
}

require_command docker
require_command dpkg-deb
require_command realpath

script_path=$(realpath -e -- "${BASH_SOURCE[0]}") ||
    die "cannot resolve this script"
repo_root=$(cd -- "$(dirname -- "$script_path")/../.." && pwd -P)
catalog="$repo_root/packaging/kernel/kernels.lock.json"
catalog_helper="$repo_root/packaging/kernel/kernel-targets.py"
[[ -f "$catalog" && ! -L "$catalog" && -x "$catalog_helper" ]] ||
    die "kernel lock tooling is unavailable"
deb_image=$(
    "$catalog_helper" --manifest "$catalog" builder-image \
        ubuntu 26.04
)
fedora_image=$(
    "$catalog_helper" --manifest "$catalog" builder-image \
        fedora 44
)
opensuse_image=$(
    "$catalog_helper" --manifest "$catalog" builder-image \
        opensuse tumbleweed
)
deb=$(realpath -e -- "$1") || die "cannot resolve DEB package: $1"
rpm_package=$(realpath -e -- "$2") || die "cannot resolve RPM package: $2"
[[ -f "$deb" && ! -L "$deb" ]] || die "DEB is not a regular file: $deb"
[[ -f "$rpm_package" && ! -L "$rpm_package" ]] ||
    die "RPM is not a regular file: $rpm_package"
[[ $(dpkg-deb -f "$deb" Package) == zerofs-kernel-client ]] ||
    die "DEB has an unexpected package name"

deb_version=$(dpkg-deb -f "$deb" Version)
[[ "$deb_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[1-9][0-9]*$ ]] ||
    die "packages have an unsafe version: $deb_version"

docker run --rm --platform linux/amd64 \
    --env "EXPECTED_VERSION=$deb_version" \
    --mount "type=bind,src=$deb,dst=/tmp/zerofs-kernel-client.deb,readonly" \
    --mount "type=bind,src=$script_path,dst=/tmp/dkms-package-install-smoke,readonly" \
    "$deb_image" \
    bash /tmp/dkms-package-install-smoke --inside-deb

docker run --rm --platform linux/amd64 \
    --env "EXPECTED_VERSION=$deb_version" \
    --mount "type=bind,src=$rpm_package,dst=/tmp/zerofs-kernel-client.rpm,readonly" \
    --mount "type=bind,src=$script_path,dst=/tmp/dkms-package-install-smoke,readonly" \
    "$opensuse_image" \
    bash /tmp/dkms-package-install-smoke --inside-opensuse

docker run --rm --platform linux/amd64 \
    --env "EXPECTED_VERSION=$deb_version" \
    --mount "type=bind,src=$rpm_package,dst=/tmp/zerofs-kernel-client.rpm,readonly" \
    --mount "type=bind,src=$script_path,dst=/tmp/dkms-package-install-smoke,readonly" \
    "$fedora_image" \
    bash /tmp/dkms-package-install-smoke --inside-fedora
