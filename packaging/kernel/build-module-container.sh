#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
readonly output_root=/zerofs-out
readonly work_root=/zerofs-work

fedora_signing_fingerprint=
output_owner=

usage() {
    cat >&2 <<EOF
usage: $script_name DISTRO RELEASE KERNEL_RELEASE KERNEL_PACKAGE_VERSION \
SOURCE_IDENTITY SNAPSHOT APT_SUITE

This is the container-side implementation used by build-target.sh. The
kernel-client package must be mounted at /zerofs-source-package.deb or
/zerofs-source-package.rpm, and an empty output directory at $output_root.
EOF
}

die() {
    echo "$script_name: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 ||
        die "required command not found: $1"
}

restore_output_ownership() {
    local status=$?

    trap - EXIT
    chown -R -- "$output_owner" "$output_root" ||
        echo "$script_name: could not restore output ownership" >&2
    exit "$status"
}

select_command() {
    local candidate

    for candidate in "$@"; do
        [[ -n "$candidate" ]] || continue
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return
        fi
    done
    return 1
}

config_value() {
    local config_file=$1
    local key=$2
    local value

    value=$(sed -n "s/^${key}=//p" "$config_file")
    if [[ "$value" == \"*\" ]]; then
        value=${value#\"}
        value=${value%\"}
    fi
    printf '%s\n' "$value"
}

source_artifact_digest() {
    local filename=$1

    python3 - "$source_artifacts_json" "$filename" <<'PY'
import json
import re
import sys

try:
    artifacts = json.loads(sys.argv[1])
except json.JSONDecodeError as error:
    raise SystemExit(f"invalid source artifact map: {error}")
if not isinstance(artifacts, dict):
    raise SystemExit("source artifact map must be an object")

filename = sys.argv[2]
digest = artifacts.get(filename)
if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit(f"missing or invalid SHA-256 pin for {filename}")
print(digest)
PY
}

verify_source_artifact() {
    local path=$1
    local actual
    local expected
    local filename=${path##*/}

    if ! expected=$(source_artifact_digest "$filename"); then
        die "cannot authenticate source artifact: $filename"
    fi
    read -r actual _ < <(sha256sum -- "$path")
    [[ "$actual" == "$expected" ]] ||
        die "SHA-256 mismatch for source artifact: $filename"
}

verify_fedora_signature() {
    local path=$1
    local expected
    local verification

    expected="key fingerprint: $fedora_signing_fingerprint: OK"
    verification=$(rpmkeys --checksig --verbose "$path") ||
        die "Fedora signature verification failed: ${path##*/}"
    [[ "$verification" == *"$expected"* ]] ||
        die "Fedora RPM has the wrong signature: ${path##*/}"
}

# koji garbage-collects the materialized signed copies once a build is
# untagged, but keeps the original rpm and the detached signature header for
# the life of the build. Splicing the two reproduces the signed rpm byte for
# byte.
fetch_fedora_rpm() {
    local base=$1
    local arch_dir=$2
    local filename=$3
    local destination=$4
    local signing_key

    signing_key=${fedora_signing_fingerprint: -8}
    curl --fail --location --retry 5 --retry-all-errors \
        --output "$destination.unsigned" \
        "$base/$arch_dir/$filename"
    curl --fail --location --retry 5 --retry-all-errors \
        --output "$destination.sig" \
        "$base/data/sigcache/$signing_key/$arch_dir/$filename.sig"
    python3 - "$destination.sig" "$destination.unsigned" "$destination" <<'PY'
import sys

import koji

sighdr, unsigned, destination = sys.argv[1:]
with open(sighdr, "rb") as source:
    koji.splice_rpm_sighdr(source.read(), unsigned, destination)
PY
    rm -f -- "$destination.unsigned" "$destination.sig"
}

apt_package_available() {
    local package=$1

    [[ -n "$(apt-cache show "$package" 2>/dev/null)" ]]
}

install_apt_snapshot_ca() {
    # The minimal base images may not contain a current CA bundle. APT still
    # authenticates the signed repository metadata during this bootstrap.
    apt-get \
        -o Acquire::Check-Valid-Until=false \
        -o Acquire::https::Verify-Peer=false \
        update
    DEBIAN_FRONTEND=noninteractive apt-get \
        -o Acquire::https::Verify-Peer=false \
        install -y --no-install-recommends ca-certificates
    apt-get -o Acquire::Check-Valid-Until=false update
}

configure_ubuntu() {
    local auto_conf
    local candidate
    local identity
    local rust_metadata
    local rust_metadata_package
    local source_package
    local source_version
    local ubuntu_source_root="$work_root/ubuntu-source"
    local -a packages=()
    local -a source_candidates=()

    [[ "$source_identity" == ubuntu:*@* ]] ||
        die "invalid Ubuntu source identity: $source_identity"
    identity=${source_identity#ubuntu:}
    source_package=${identity%%@*}
    source_version=${identity#*@}
    [[ "$source_package" =~ ^[a-z0-9][a-z0-9+.-]*$ &&
       "$source_version" =~ ^[0-9A-Za-z.+:~_-]+$ ]] ||
        die "invalid Ubuntu source identity: $source_identity"
    [[ "$snapshot" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] ||
        die "invalid Ubuntu snapshot timestamp: $snapshot"

    rm -f -- /etc/apt/sources.list /etc/apt/sources.list.d/*
    cat >/etc/apt/sources.list.d/zerofs.sources <<EOF
Types: deb deb-src
URIs: https://snapshot.ubuntu.com/ubuntu/$snapshot/
Suites: $apt_suite ${apt_suite}-updates ${apt_suite}-security
Components: main universe
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF
    install_apt_snapshot_ca

    packages=(
        bc
        binutils
        bison
        build-essential
        dwarves
        flex
        kmod
        libelf-dev
        libssl-dev
        "linux-headers-$kernel_release=$kernel_package_version"
        "linux-image-$kernel_release=$kernel_package_version"
        "linux-modules-$kernel_release=$kernel_package_version"
        python3
        xz-utils
        zstd
    )
    rust_metadata_package=linux-lib-rust-$kernel_release
    if apt_package_available "$rust_metadata_package"; then
        packages+=("$rust_metadata_package=$kernel_package_version")
    fi
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends "${packages[@]}"

    kdir=$(realpath "/lib/modules/$kernel_release/build")
    auto_conf="$kdir/include/config/auto.conf"
    rust_metadata="/usr/src/linux-lib-rust-$kernel_release/rust"
    if grep -qx 'CONFIG_RUST=y' "$auto_conf" &&
       [[ ! -s "$kdir/rust/libkernel.rmeta" &&
          -s "$rust_metadata/libkernel.rmeta" ]]; then
        [[ -L "$kdir/rust" ]] ||
            die "kernel headers do not expose their packaged Rust metadata"
        ln -sfn "$rust_metadata" "$kdir/rust"
    fi
    if grep -qx 'CONFIG_RUST=y' "$auto_conf" &&
       [[ -s "$kdir/rust/libkernel.rmeta" ]]; then
        kernel_source=
        return
    fi

    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends dpkg-dev libdw-dev llvm
    mkdir -p "$ubuntu_source_root"
    (
        cd "$ubuntu_source_root"
        apt-get source "$source_package=$source_version"
    )
    for candidate in "$ubuntu_source_root"/*; do
        if [[ -d "$candidate" &&
              -f "$candidate/Makefile" &&
              -f "$candidate/rust/Makefile" ]]; then
            source_candidates+=("$candidate")
        fi
    done
    [[ ${#source_candidates[@]} -eq 1 ]] ||
        die "cannot select the exact Ubuntu kernel source tree"
    kernel_source=${source_candidates[0]}
    kernel_source_package_version=$source_version
    kernel_headers_package_version=$kernel_package_version
    make -s -C "$kernel_source" mrproper
}

configure_debian() {
    local base_suite=${apt_suite%-backports}
    local identity
    local source_package
    local source_series=${kernel_release%%+*}
    local source_version
    local suite_suffix=:$apt_suite

    [[ "$apt_suite" == *-backports && -n "$base_suite" ]] ||
        die "invalid Debian backports suite: $apt_suite"
    [[ "$source_identity" == debian:*@*"$suite_suffix" ]] ||
        die "invalid Debian source identity: $source_identity"
    identity=${source_identity#debian:}
    identity=${identity%"$suite_suffix"}
    source_package=${identity%%@*}
    source_version=${identity#*@}
    [[ "$source_package" == linux &&
       "$source_version" =~ ^[0-9A-Za-z.+:~_-]+$ ]] ||
        die "invalid Debian source identity: $source_identity"
    [[ "$snapshot" =~ ^[0-9]{8}T[0-9]{6}Z$ ]] ||
        die "invalid Debian snapshot timestamp: $snapshot"
    [[ "$source_series" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] ||
        die "cannot derive Debian source series from $kernel_release"
    source_series=${source_series%.*}

    rm -f -- /etc/apt/sources.list /etc/apt/sources.list.d/*
    cat >/etc/apt/sources.list <<EOF
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/$snapshot $base_suite main
deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/$snapshot $apt_suite main
EOF
    install_apt_snapshot_ca

    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
        bc \
        binutils \
        bison \
        build-essential \
        clang \
        dwarves \
        flex \
        kmod \
        libdw-dev \
        libelf-dev \
        libssl-dev \
        lld \
        llvm \
        "linux-headers-$kernel_release=$kernel_package_version" \
        "linux-image-$kernel_release=$kernel_package_version" \
        "linux-source-$source_series=$source_version" \
        python3 \
        bindgen \
        rustc \
        xz-utils \
        zstd

    kdir=$(realpath "/lib/modules/$kernel_release/build")
    source_archive="/usr/src/linux-source-$source_series.tar.xz"
    [[ -s "$source_archive" ]] ||
        die "Debian kernel source archive is missing: $source_archive"
    mkdir -p "$work_root/kernel-source"
    tar -xJf "$source_archive" -C "$work_root/kernel-source"
    kernel_source=$(find "$work_root/kernel-source" \
        -mindepth 1 -maxdepth 1 -type d -name 'linux-source-*' \
        -print -quit)
    [[ -n "$kernel_source" ]] ||
        die "cannot find extracted Debian kernel source"
    kernel_source_package_version=$source_version
    kernel_headers_package_version=$kernel_package_version
}

configure_fedora() {
    local kernel_arch
    local nvr
    local package_release
    local package_version
    local package_path
    local koji_base
    local rpm_dir="$work_root/fedora-rpms"
    local rpmbuild_root="$work_root/rpmbuild"
    local snapshot_suffix
    local source_version
    local -a source_candidates=()

    case $target_arch in
        x86_64) kernel_arch=x86 ;;
        aarch64) kernel_arch=arm64 ;;
        *) die "unsupported Fedora architecture: $target_arch" ;;
    esac
    [[ "$source_identity" == kernel-* ]] ||
        die "invalid Fedora source identity: $source_identity"
    snapshot_suffix=":$target_arch,noarch,src"
    case $snapshot in
        "koji-signed-build:"*"$snapshot_suffix")
            fedora_signing_fingerprint=${snapshot#koji-signed-build:}
            fedora_signing_fingerprint=${fedora_signing_fingerprint%"$snapshot_suffix"}
            [[ "$fedora_signing_fingerprint" =~ ^[0-9a-f]{40}$ ]] ||
                die "invalid Fedora signing fingerprint"
            ;;
        *)
            die "unexpected Fedora acquisition mode: $snapshot"
            ;;
    esac
    nvr=${source_identity#kernel-}
    [[ -n "$nvr" && "$nvr" == *-* ]] ||
        die "invalid Fedora source identity: $source_identity"
    package_version=${nvr%%-*}
    package_release=${nvr#*-}
    [[ "$package_release" == *".fc$release" ]] ||
        die "Fedora source build does not match release $release"
    [[ "$kernel_package_version" == "$nvr.$target_arch" ]] ||
        die "Fedora kernel package version does not match source NVR"

    dnf install -y \
        bc \
        binutils \
        bison \
        cargo \
        clang \
        cpio \
        dnf-plugins-core \
        dwarves \
        elfutils-libelf-devel \
        flex \
        gcc \
        kmod \
        lld \
        llvm \
        make \
        openssl-devel \
        python3 \
        python3-koji \
        rpm-build \
        rust \
        bindgen-cli \
        rust-src \
        rustfmt \
        xz \
        zstd

    require_command curl
    require_command rpmkeys
    require_command sha256sum
    mkdir -p "$rpm_dir"
    koji_base="https://kojipkgs.fedoraproject.org/packages/kernel/$package_version/$package_release"
    for package in kernel-core kernel-modules-core kernel-devel; do
        package_path="$rpm_dir/$package-$nvr.$target_arch.rpm"
        fetch_fedora_rpm "$koji_base" "$target_arch" \
            "$package-$nvr.$target_arch.rpm" "$package_path"
        verify_source_artifact "$package_path"
        verify_fedora_signature "$package_path"
    done
    package_path="$rpm_dir/kernel-$nvr.src.rpm"
    fetch_fedora_rpm "$koji_base" src "kernel-$nvr.src.rpm" "$package_path"
    verify_source_artifact "$package_path"
    verify_fedora_signature "$package_path"

    dnf install -y \
        "$rpm_dir/kernel-core-$nvr.$target_arch.rpm" \
        "$rpm_dir/kernel-modules-core-$nvr.$target_arch.rpm" \
        "$rpm_dir/kernel-devel-$nvr.$target_arch.rpm"
    mkdir -p "$rpmbuild_root"
    rpm -ivh \
        --define "_topdir $rpmbuild_root" \
        "$rpm_dir/kernel-$nvr.src.rpm"
    dnf builddep -y \
        -D "_topdir $rpmbuild_root" \
        --spec "$rpmbuild_root/SPECS/kernel.spec"
    rpmbuild -bp \
        --define "_topdir $rpmbuild_root" \
        --target "$target_arch" \
        "$rpmbuild_root/SPECS/kernel.spec"

    mapfile -t source_candidates < <(
        find "$rpmbuild_root/BUILD" -type d \
            -name "linux-$kernel_release" -print
    )
    if [[ ${#source_candidates[@]} -ne 1 ]]; then
        echo "expected one Fedora source directory named linux-$kernel_release" >&2
        find "$rpmbuild_root/BUILD" -type d -name 'linux-*' \
            -printf 'found Linux directory: %p\n' >&2
        find "$rpmbuild_root/BUILD" -type f -path '*/rust/Makefile' \
            -printf 'found Rust makefile: %p\n' >&2
        die "prepared Fedora kernel source selection failed"
    fi
    kernel_source=${source_candidates[0]}
    kernel_source_package_version="0:$nvr"
    kernel_headers_package_version="0:$nvr"
    [[ -f "$kernel_source/Makefile" &&
       -f "$kernel_source/rust/Makefile" ]] ||
        die "prepared Fedora source tree is incomplete: $kernel_source"
    source_version=$(make -s -C "$kernel_source" kernelversion)
    [[ -n "$source_version" ]] ||
        die "Fedora source tree did not report a kernel version"
    case $kernel_release in
        "$source_version" | "$source_version"-*) ;;
        *)
            die "Fedora source version $source_version does not match $kernel_release"
            ;;
    esac
    # Fedora's %prep runs its configuration checks in the source directory.
    # Remove only their generated Kbuild state before reusing the tree with
    # kernel-devel as a separate output directory.
    make -s -C "$kernel_source" mrproper
    [[ ! -f "$kernel_source/.config" &&
       ! -d "$kernel_source/include/config" &&
       ! -d "$kernel_source/arch/$kernel_arch/include/generated" ]] ||
        die "Fedora prepared source tree could not be cleaned for an O= build"
    kdir=$(realpath "/lib/modules/$kernel_release/build")
    install_fedora_rust_tools \
        "$kdir/include/config/auto.conf" "$target_arch"
}

configure_opensuse() {
    local entry
    local package
    local repository_url
    local version
    local -a package_entries=()
    local -A package_versions=()
    local -a expected_packages=(
        kernel-default
        kernel-default-devel
        kernel-devel
        kernel-source
        kernel-syms
    )

    [[ "$target_arch" == x86_64 ]] ||
        die "openSUSE ARM packaging needs a pinned ports snapshot"
    [[ "$release" == tumbleweed ]] ||
        die "unsupported openSUSE release: $release"
    [[ "$snapshot" =~ ^[0-9]{8}$ ]] ||
        die "invalid openSUSE snapshot: $snapshot"
    IFS=, read -ra package_entries <<<"$source_identity"
    for entry in "${package_entries[@]}"; do
        package=${entry%@*}
        version=${entry#*@}
        [[ "$entry" == "$package@$version" &&
           "$package" =~ ^[a-z][a-z0-9-]*$ &&
           "$version" =~ ^[0-9A-Za-z.+~_-]+$ ]] ||
            die "invalid openSUSE source package identity: $entry"
        [[ -z ${package_versions[$package]+present} ]] ||
            die "duplicate openSUSE source package identity: $package"
        package_versions[$package]=$version
    done
    [[ ${#package_versions[@]} -eq ${#expected_packages[@]} ]] ||
        die "openSUSE source identity has an unexpected package count"
    for package in "${expected_packages[@]}"; do
        [[ -n ${package_versions[$package]:-} ]] ||
            die "openSUSE source identity omits $package"
    done
    [[ "${package_versions[kernel-default]}" == \
       "$kernel_package_version" ]] ||
        die "openSUSE kernel-default identity does not match target package"
    repository_url="https://download.opensuse.org/history/$snapshot/tumbleweed/repo/oss/"

    zypper --non-interactive removerepo --all || true
    zypper --non-interactive addrepo --check "$repository_url" zerofs-snapshot
    # The official builder image provides the trusted distribution keys.
    # Never import a key supplied by repository metadata.
    zypper --non-interactive refresh
    zypper --non-interactive install --oldpackage \
        bc \
        binutils \
        bison \
        clang \
        diffutils \
        dwarves \
        findutils \
        flex \
        gcc \
        kernel-default="${package_versions[kernel-default]}" \
        kernel-default-devel="${package_versions[kernel-default-devel]}" \
        kernel-devel="${package_versions[kernel-devel]}" \
        kernel-source="${package_versions[kernel-source]}" \
        kernel-syms="${package_versions[kernel-syms]}" \
        libdw-devel \
        libelf-devel \
        libopenssl-devel \
        lld \
        llvm \
        make \
        python3 \
        rust \
        rust-bindgen \
        rust-src \
        xz \
        zstd

    kdir=$(realpath "/lib/modules/$kernel_release/build")
    kernel_source=$(realpath /usr/src/linux)
    kernel_source_package_version="0:${package_versions[kernel-source]}"
    kernel_headers_package_version="0:${package_versions[kernel-default-devel]}"
}

install_apt_rust_tools() {
    local auto_conf=$1
    local bindgen_package
    local bindgen_release
    local bindgen_series
    local bindgen_text
    local rustc_release
    local rustc_series
    local rustc_text
    local rustc_package
    local rustc_package_version
    local rust_src_package
    local rustfmt_binary
    local rustfmt_package
    local rustfmt_package_version
    local -a rustfmt_binaries=()
    local -a packages=()

    rustc_text=$(config_value "$auto_conf" CONFIG_RUSTC_VERSION_TEXT)
    bindgen_text=$(config_value "$auto_conf" CONFIG_BINDGEN_VERSION_TEXT)
    rustc_series=

    if [[ -n "$rustc_text" ]]; then
        rustc_release=${rustc_text#rustc }
        rustc_release=${rustc_release%% *}
        rustc_series=${rustc_release%.*}
        if apt_package_available "rustc-$rustc_series"; then
            rustc_package="rustc-$rustc_series"
        else
            rustc_package=rustc
        fi
    else
        rustc_package=rustc
    fi
    packages+=("$rustc_package")

    if [[ -n "$rustc_series" ]] &&
       apt_package_available "rust-$rustc_series-src"; then
        rust_src_package="rust-$rustc_series-src"
    elif apt_package_available rust-src; then
        rust_src_package=rust-src
    else
        die "cannot find Rust sources for the target compiler"
    fi
    packages+=("$rust_src_package")

    if [[ -n "$rustc_series" ]] &&
       apt_package_available "rustfmt-$rustc_series"; then
        rustfmt_package="rustfmt-$rustc_series"
    elif apt_package_available rustfmt; then
        rustfmt_package=rustfmt
    else
        die "cannot find a distribution rustfmt package"
    fi
    packages+=("$rustfmt_package")

    if [[ -n "$bindgen_text" ]]; then
        bindgen_release=${bindgen_text#bindgen }
        bindgen_release=${bindgen_release%% *}
        bindgen_series=${bindgen_release%.*}
        if apt_package_available "bindgen-$bindgen_series"; then
            bindgen_package="bindgen-$bindgen_series"
        elif apt_package_available bindgen; then
            bindgen_package=bindgen
        else
            bindgen_package=rust-bindgen
        fi
    elif apt_package_available bindgen; then
        bindgen_package=bindgen
    else
        bindgen_package=rust-bindgen
    fi
    packages+=("$bindgen_package")

    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends "${packages[@]}"

    mapfile -t rustfmt_binaries < <(
        while IFS= read -r rustfmt_binary; do
            if [[ ${rustfmt_binary##*/} == rustfmt &&
                  -f "$rustfmt_binary" &&
                  -x "$rustfmt_binary" ]]; then
                printf '%s\n' "$rustfmt_binary"
            fi
        done < <(dpkg-query -L "$rustfmt_package")
    )
    [[ ${#rustfmt_binaries[@]} -eq 1 ]] ||
        die "cannot select the binary installed by $rustfmt_package"
    RUSTFMT=${rustfmt_binaries[0]}
    export RUSTFMT

    rustc_package_version=$(
        dpkg-query -W -f='${Version}' "$rustc_package"
    )
    rustfmt_package_version=$(
        dpkg-query -W -f='${Version}' "$rustfmt_package"
    )
    if [[ -n "$rustc_text" ]]; then
        [[ "$rustfmt_package_version" == "$rustc_package_version" ]] ||
            die "$rustfmt_package does not match $rustc_package"
    fi
}

install_fedora_rust_tools() {
    local auto_conf=$1
    local target_rpm_arch=$2
    local installed_package_nvr
    local installed_version
    local koji_base
    local package
    local package_path
    local rpm_arch
    local rpm_dir="$work_root/fedora-rust-rpms"
    local rust_release
    local rust_version
    local rustc_text
    local rust_nvr
    local toolchain_matches=true
    local -a packages=()

    rustc_text=$(config_value "$auto_conf" CONFIG_RUSTC_VERSION_TEXT)
    [[ -n "$rustc_text" ]] ||
        die "Fedora target configuration does not record its Rust compiler"

    installed_version=$(rustc --version)
    if [[ ! "$rustc_text" =~ \(Fedora[[:space:]]+([^()]*)\)$ ]]; then
        die "cannot derive Fedora Rust build from: $rustc_text"
    fi
    rust_nvr=${BASH_REMATCH[1]}
    [[ "$rust_nvr" =~ ^[0-9A-Za-z.+~_]+-[0-9A-Za-z.+~_]+$ ]] ||
        die "unsafe Fedora Rust build identifier: $rust_nvr"
    rust_version=${rust_nvr%%-*}
    rust_release=${rust_nvr#*-}
    for package in cargo rust rust-src rust-std-static rustfmt; do
        installed_package_nvr=$(
            rpm -q --queryformat '%{VERSION}-%{RELEASE}' "$package" \
                2>/dev/null || true
        )
        if [[ "$installed_package_nvr" != "$rust_nvr" ]]; then
            toolchain_matches=false
        fi
    done
    if [[ "$installed_version" == "$rustc_text" &&
          "$toolchain_matches" == true ]]; then
        RUSTFMT=$(select_command /usr/bin/rustfmt rustfmt) ||
            die "cannot find Fedora rustfmt"
        export RUSTFMT
        return
    fi

    rpm_arch=$(uname -m)
    [[ "$rpm_arch" == "$target_rpm_arch" ]] ||
        die "Fedora builder $rpm_arch does not match $target_rpm_arch"
    koji_base="https://kojipkgs.fedoraproject.org/packages/rust/$rust_version/$rust_release"
    mkdir -p "$rpm_dir"
    for package in cargo rust rust-std-static rustfmt; do
        package_path="$rpm_dir/$package-$rust_nvr.$rpm_arch.rpm"
        packages+=("$package_path")
        fetch_fedora_rpm "$koji_base" "$rpm_arch" \
            "$package-$rust_nvr.$rpm_arch.rpm" "$package_path"
        verify_source_artifact "$package_path"
        verify_fedora_signature "$package_path"
    done
    package_path="$rpm_dir/rust-src-$rust_nvr.noarch.rpm"
    packages+=("$package_path")
    fetch_fedora_rpm "$koji_base" noarch \
        "rust-src-$rust_nvr.noarch.rpm" "$package_path"
    verify_source_artifact "$package_path"
    verify_fedora_signature "$package_path"

    # Fedora's kernel build dependencies may pull in clippy for the image's
    # newer Rust build. It is not a module build input and blocks the pin.
    if rpm -q clippy >/dev/null 2>&1; then
        rpm -e clippy
    fi
    dnf install -y --allow-downgrade "${packages[@]}"

    for package in cargo rust rust-src rust-std-static rustfmt; do
        installed_package_nvr=$(
            rpm -q --queryformat '%{VERSION}-%{RELEASE}' "$package"
        )
        [[ "$installed_package_nvr" == "$rust_nvr" ]] ||
            die "$package did not resolve to Fedora build $rust_nvr"
    done
    RUSTFMT=$(select_command /usr/bin/rustfmt rustfmt) ||
        die "cannot find Fedora rustfmt"
    export RUSTFMT
}

select_rust_tools() {
    local auto_conf=$1
    local bindgen_release
    local bindgen_series
    local bindgen_text
    local rustc_release
    local rustc_series
    local rustc_text

    rustc_text=$(config_value "$auto_conf" CONFIG_RUSTC_VERSION_TEXT)
    bindgen_text=$(config_value "$auto_conf" CONFIG_BINDGEN_VERSION_TEXT)

    rustc_series=
    if [[ -n "$rustc_text" ]]; then
        rustc_release=${rustc_text#rustc }
        rustc_release=${rustc_release%% *}
        rustc_series=${rustc_release%.*}
    fi
    rustc=$(select_command \
        "rustc-$rustc_series" \
        "/usr/bin/rustc-$rustc_series" \
        /usr/bin/rustc \
        rustc) || die "cannot find rustc"

    bindgen_series=
    if [[ -n "$bindgen_text" ]]; then
        bindgen_release=${bindgen_text#bindgen }
        bindgen_release=${bindgen_release%% *}
        bindgen_series=${bindgen_release%.*}
    fi
    bindgen=$(select_command \
        "bindgen-$bindgen_series" \
        "/usr/bin/bindgen-$bindgen_series" \
        /usr/bin/bindgen \
        bindgen) || die "cannot find bindgen"
    rustfmt=$(select_command \
        "${RUSTFMT:-}" \
        "rustfmt-$rustc_series" \
        "/usr/bin/rustfmt-$rustc_series" \
        "/usr/lib/rust-$rustc_series/bin/rustfmt" \
        /usr/bin/rustfmt \
        rustfmt) || die "cannot find rustfmt"
    RUSTFMT=$rustfmt
    export RUSTFMT
    if [[ -n "$rustc_text" && "$("$rustc" --version)" != "$rustc_text" ]]; then
        die "rustc does not match target configuration: $("$rustc" --version)"
    fi
    if [[ -n "$bindgen_text" && "$("$bindgen" --version)" != "$bindgen_text" ]]; then
        die "bindgen does not match target configuration: $("$bindgen" --version)"
    fi
}

select_target_cc() {
    local auto_conf=$1
    local configured_cc
    local target_cc_text
    local target_cc_version

    target_cc_text=$(config_value "$auto_conf" CONFIG_CC_VERSION_TEXT)
    configured_cc=${target_cc_text%% *}

    target_cc=
    if [[ -n "$configured_cc" ]]; then
        target_cc=$(select_command "$configured_cc" || true)
    fi
    if [[ -z "$target_cc" ]] &&
       grep -qx 'CONFIG_CC_IS_CLANG=y' "$auto_conf"; then
        target_cc=$(select_command clang cc || true)
    elif [[ -z "$target_cc" ]] &&
         grep -qx 'CONFIG_CC_IS_GCC=y' "$auto_conf"; then
        target_cc=$(select_command gcc cc || true)
    fi
    [[ -n "$target_cc" ]] ||
        die "cannot find the target kernel C compiler"

    target_cc_version=$(LC_ALL=C "$target_cc" --version | sed -n '1p')
    if [[ -z "$target_cc_text" ||
          "$target_cc_version" != "$target_cc_text" ]]; then
        echo "warning: selected C compiler does not exactly match target" >&2
        echo "  target: ${target_cc_text:-unknown}" >&2
        echo "  selected: $target_cc_version" >&2
    fi
}

select_llvm_tools() {
    local llvm_major

    llvm_major=$("$rustc" -vV |
        sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p')
    [[ -n "$llvm_major" ]] ||
        die "cannot determine rustc LLVM major version"

    if [[ "$distro" == ubuntu || "$distro" == debian ]]; then
        if apt_package_available "clang-$llvm_major"; then
            DEBIAN_FRONTEND=noninteractive apt-get install -y \
                --no-install-recommends \
                "clang-$llvm_major" \
                "lld-$llvm_major" \
                "llvm-$llvm_major"
        fi
    elif [[ "$distro" == opensuse ]]; then
        zypper --non-interactive install \
            "clang$llvm_major" \
            "lld$llvm_major" \
            "llvm$llvm_major"
    fi

    clang=$(select_command "clang-$llvm_major" "clang$llvm_major" clang) ||
        die "cannot find Clang $llvm_major"
    llvm_link=$(select_command \
        "llvm-link-$llvm_major" "llvm-link$llvm_major" llvm-link) ||
        die "cannot find llvm-link $llvm_major"
    llvm_opt=$(select_command "opt-$llvm_major" "opt$llvm_major" opt) ||
        die "cannot find opt $llvm_major"
    llvm_nm=$(select_command \
        "llvm-nm-$llvm_major" "llvm-nm$llvm_major" llvm-nm) ||
        die "cannot find llvm-nm $llvm_major"
}

copy_uncompressed_module() {
    local source=$1
    local destination=$2

    case $source in
        *.ko) install -m 0644 "$source" "$destination" ;;
        *.ko.gz) gzip -dc -- "$source" >"$destination" ;;
        *.ko.xz) xz -dc -- "$source" >"$destination" ;;
        *.ko.zst) zstd -dc -q -- "$source" >"$destination" ;;
        *) die "unsupported module compression: $source" ;;
    esac
    chmod 0644 "$destination"
}

build_source_package_module() {
    local auto_conf="$kdir/include/config/auto.conf"
    local dkms_make_log
    local installed_source
    local installed_module
    local installed_module_root
    local package_install_status=0
    local resolved_installed_module
    local package_version

    # Release modules receive one stable vendor signature before the final
    # exact-byte boot test. Prevent distro DKMS from appending an ephemeral
    # build-container MOK signature first.
    install -d -m 0755 /etc/dkms/framework.conf.d
    printf '%s\n' 'sign_file="/bin/true"' \
        >/etc/dkms/framework.conf.d/zerofs-ci-unsigned.conf

    if [[ "$distro" == ubuntu || "$distro" == debian ]]; then
        install_apt_rust_tools "$auto_conf"
    fi
    select_rust_tools "$auto_conf"
    select_target_cc "$auto_conf"
    if ! grep -qx 'CONFIG_RUST=y' "$auto_conf"; then
        select_llvm_tools
    fi

    export ZEROFS_DKMS_KERNEL=$kernel_release
    # Exact-target CI exercises the real local-build fallback.  Release
    # publication later signs this boot-tested module centrally; it must not
    # depend on, or accidentally consume, an already published object.
    export ZEROFS_DISABLE_PREBUILT=1
    module="$output_root/module/zerofs.ko"
    install -d -m 0755 "${module%/*}"
    export ZEROFS_RUSTC=$rustc
    export ZEROFS_RUSTFMT=$rustfmt
    export ZEROFS_BINDGEN=$bindgen
    export ZEROFS_TARGET_CC=$target_cc
    if [[ -n "$kernel_source" ]]; then
        [[ -n "$kernel_source_package_version" &&
           -n "$kernel_headers_package_version" ]] ||
            die "exact kernel source/header package provenance is missing"
        export ZEROFS_KERNEL_SOURCE=$kernel_source
        export ZEROFS_KERNEL_SOURCE_PACKAGE_VERSION=$kernel_source_package_version
        export ZEROFS_KERNEL_HEADERS_PACKAGE_VERSION=$kernel_headers_package_version
    fi
    if [[ -n "$clang" ]]; then
        export ZEROFS_CLANG=$clang
        export ZEROFS_LLVM_LINK=$llvm_link
        export ZEROFS_LLVM_OPT=$llvm_opt
        export ZEROFS_LLVM_NM=$llvm_nm
    fi

    case $distro in
        ubuntu | debian)
            package_version=$(dpkg-deb --field "$source_package" Version)
            ;;
        fedora | opensuse)
            package_version=$(rpm -qp --qf '%{VERSION}-%{RELEASE}' \
                "$source_package")
            ;;
        *) die "unsupported package target: $distro" ;;
    esac
    [[ "$package_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[1-9][0-9]*$ ]] ||
        die "kernel-client package has an unsafe version: $package_version"
    case $distro in
        ubuntu | debian)
            DEBIAN_FRONTEND=noninteractive apt-get install -y \
                --no-install-recommends "$source_package" ||
                package_install_status=$?
            ;;
        fedora)
            dnf install -y "$source_package" || package_install_status=$?
            ;;
        opensuse)
            zypper --non-interactive install --allow-unsigned-rpm \
                "$source_package" || package_install_status=$?
            ;;
    esac
    if ((package_install_status != 0)); then
        dkms_make_log="/var/lib/dkms/zerofs/$package_version/build/make.log"
        if [[ -f "$dkms_make_log" && ! -L "$dkms_make_log" ]]; then
            printf '\n%s\n' "DKMS build log: $dkms_make_log" >&2
            cat -- "$dkms_make_log" >&2 || true
        fi
        return "$package_install_status"
    fi
    installed_source="/usr/src/zerofs-$package_version/kernel"
    [[ -d "$installed_source" && ! -L "$installed_source" ]] ||
        die "installed kernel-client package has no regular kernel source tree"
    make -C "$installed_source" \
        "KDIR=$kdir" \
        "RUSTC=$rustc" \
        test

    installed_module=$(modinfo -k "$kernel_release" -n zerofs) ||
        die "DKMS did not install zerofs for $kernel_release"
    [[ -f "$installed_module" && ! -L "$installed_module" ]] ||
        die "DKMS module is not a regular file: $installed_module"
    installed_module_root=$(realpath -e -- "/lib/modules/$kernel_release") ||
        die "cannot resolve the module directory for $kernel_release"
    resolved_installed_module=$(realpath -e -- "$installed_module") ||
        die "cannot resolve the DKMS module path: $installed_module"
    case $resolved_installed_module in
        "$installed_module_root/"*) ;;
        *) die "DKMS returned an unsafe module path: $installed_module" ;;
    esac
    installed_module=$resolved_installed_module
    copy_uncompressed_module "$installed_module" "$module"
    [[ -s "$module" && ! -L "$module" ]] ||
        die "DKMS installed module could not be copied"
    [[ "$(modinfo -F name "$module")" == zerofs ]] ||
        die "DKMS built a module with the wrong name"
    case $(modinfo -F vermagic "$module") in
        "$kernel_release" | "$kernel_release "*) ;;
        *) die "DKMS module vermagic does not match $kernel_release" ;;
    esac
}

copy_module_plan() {
    local output_directory=$1
    local excluded_module=$2
    shift 2

    local dependency
    local dependency_index=0
    local dependency_name
    local dependency_output
    local directive
    local module_plan_text
    local path
    local requested_module
    local -a module_plan
    local -A seen_modules=()

    mkdir -p "$output_directory"
    for requested_module in "$@"; do
        module_plan_text=$(
            modprobe \
                --set-version "$kernel_release" \
                --show-depends \
                "$requested_module"
        ) || die "modprobe could not resolve $requested_module"
        [[ -n "$module_plan_text" ]] ||
            die "modprobe produced an empty plan for $requested_module"
        mapfile -t module_plan <<<"$module_plan_text"

        for dependency in "${module_plan[@]}"; do
            read -r directive path _ <<<"$dependency"
            [[ "$directive" == builtin ]] && continue
            [[ "$directive" == insmod ]] ||
                die "unsupported modprobe directive: $dependency"
            [[ -n "$path" && -f "$path" ]] ||
                die "modprobe dependency is not a file: $dependency"
            dependency_name=$(modinfo -F name "$path")
            if [[ -n "$excluded_module" &&
                  "$dependency_name" == "$excluded_module" ]]; then
                continue
            fi
            [[ -z ${seen_modules[$dependency_name]:-} ]] || continue
            seen_modules[$dependency_name]=1

            printf -v dependency_output \
                '%s/%04d.ko' \
                "$output_directory" "$dependency_index"
            copy_uncompressed_module "$path" "$dependency_output"
            case $(modinfo -F vermagic "$dependency_output") in
                "$kernel_release" | "$kernel_release "*) ;;
                *)
                    die "$dependency_name does not target $kernel_release"
                    ;;
            esac
            ((dependency_index += 1))
        done
    done
}

copy_boot_modules() {
    local transport

    case $target_arch in
        x86_64) transport=virtio_pci ;;
        aarch64) transport=virtio_mmio ;;
        *) die "unsupported boot architecture: $target_arch" ;;
    esac
    copy_module_plan "$output_root/boot-modules" "" \
        "$transport" virtio_net
}

arm64_image_is_raw() {
    local image=$1
    local magic

    [[ -s "$image" ]] || return 1
    magic=$(od -An -tx1 -j 56 -N 4 "$image" 2>/dev/null |
        tr -d '[:space:]')
    [[ "$magic" == 41524d64 ]]
}

arm64_image_is_zboot() {
    local image=$1
    local magic

    [[ -s "$image" ]] || return 1
    magic=$(od -An -tx1 -j 4 -N 4 "$image" 2>/dev/null |
        tr -d '[:space:]')
    [[ "$magic" == 7a696d67 ]]
}

normalize_arm64_kernel_image() {
    local source=$1
    local destination=$2
    local compression
    local image_size
    local llvm_major
    local llvm_objcopy
    local payload="$work_root/kernel-image.payload"
    local payload_offset
    local payload_size
    local zboot="$work_root/kernel-image.zboot"

    require_command dd
    require_command od
    require_command stat
    if arm64_image_is_raw "$source"; then
        install -m 0644 "$source" "$destination"
        return
    fi

    if arm64_image_is_zboot "$source"; then
        cp "$source" "$zboot"
    else
        llvm_major=$("$rustc" -vV |
            sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p')
        [[ -n "$llvm_major" ]] ||
            die "cannot determine rustc LLVM major version"
        if [[ "$distro" == ubuntu || "$distro" == debian ]] &&
           apt_package_available "llvm-$llvm_major"; then
            DEBIAN_FRONTEND=noninteractive apt-get install -y \
                --no-install-recommends "llvm-$llvm_major"
        fi
        llvm_objcopy=$(select_command \
            "llvm-objcopy-$llvm_major" \
            "llvm-objcopy$llvm_major" \
            llvm-objcopy) ||
            die "llvm-objcopy is required to unwrap the ARM64 kernel image"
        "$llvm_objcopy" --dump-section ".linux=$zboot" "$source" ||
            die "ARM64 kernel image has no extractable .linux section"
    fi

    arm64_image_is_zboot "$zboot" ||
        die "ARM64 kernel image does not contain an EFI zboot payload"
    payload_offset=$(od -An -tu4 -j 8 -N 4 "$zboot" |
        tr -d '[:space:]')
    payload_size=$(od -An -tu4 -j 12 -N 4 "$zboot" |
        tr -d '[:space:]')
    compression=$(dd if="$zboot" bs=1 skip=24 count=4 status=none)
    [[ "$payload_offset" =~ ^[1-9][0-9]*$ &&
       "$payload_size" =~ ^[1-9][0-9]*$ ]] ||
        die "ARM64 zboot payload has invalid bounds"
    image_size=$(stat -c '%s' "$zboot")
    ((payload_offset + payload_size <= image_size)) ||
        die "ARM64 zboot payload extends beyond its image"

    dd if="$zboot" of="$payload" \
        iflag=skip_bytes,count_bytes \
        skip="$payload_offset" count="$payload_size" status=none
    case $compression in
        gzip)
            require_command gzip
            gzip -dc -- "$payload" >"$destination"
            ;;
        zstd)
            require_command zstd
            zstd -d -q -f "$payload" -o "$destination"
            ;;
        *)
            die "unsupported ARM64 zboot compression: $compression"
            ;;
    esac
    arm64_image_is_raw "$destination" ||
        die "extracted ARM64 kernel is not a bootable Image"
    chmod 0644 "$destination"
}

publish_build_outputs() {
    local kernel_image
    local published_kernel_image="$output_root/vmlinuz"
    local candidate
    local -a kernel_image_candidates=(
        "/boot/vmlinuz-$kernel_release"
        "/usr/lib/modules/$kernel_release/vmlinuz"
        "/lib/modules/$kernel_release/vmlinuz"
    )

    kernel_image=
    for candidate in "${kernel_image_candidates[@]}"; do
        if [[ -s "$candidate" ]]; then
            kernel_image=$candidate
            break
        fi
    done
    [[ -n "$kernel_image" ]] ||
        die "target kernel image is missing for $kernel_release"

    if [[ "$target_arch" == aarch64 ]]; then
        normalize_arm64_kernel_image "$kernel_image" "$published_kernel_image"
    else
        install -m 0644 "$kernel_image" "$published_kernel_image"
    fi
}

[[ $# -eq 7 ]] || {
    usage
    exit 2
}

distro=$1
release=$2
kernel_release=$3
kernel_package_version=$4
source_identity=$5
snapshot=$6
apt_suite=$7
source_artifacts_json=${ZEROFS_SOURCE_ARTIFACTS:-'{}'}

case $distro in
    ubuntu | debian)
        [[ "$apt_suite" =~ ^[a-z0-9][a-z0-9._+-]*$ ]] ||
            die "invalid APT suite: ${apt_suite:-unset}"
        source_package=/zerofs-source-package.deb
        ;;
    fedora | opensuse)
        [[ -z "$apt_suite" ]] ||
            die "$distro target must not provide an APT suite"
        source_package=/zerofs-source-package.rpm
        ;;
    *) die "unsupported build distribution: $distro" ;;
esac

container_machine=$(uname -m)
case $container_machine in
    x86_64)
        target_arch=x86_64
        ;;
    aarch64 | arm64)
        target_arch=aarch64
        ;;
    *)
        die "unsupported builder architecture: $container_machine"
        ;;
esac
[[ -f "$source_package" && ! -L "$source_package" ]] ||
    die "kernel-client package is not a regular file: $source_package"

[[ -d "$output_root" ]] ||
    die "output directory is not mounted at $output_root"
[[ -z $(find -P "$output_root" -mindepth 1 -print -quit) ]] ||
    die "output directory is not empty"
output_owner=$(stat -c '%u:%g' "$output_root") ||
    die "cannot read output directory ownership"
[[ "$output_owner" =~ ^[0-9]+:[0-9]+$ ]] ||
    die "output directory has invalid ownership"
trap restore_output_ownership EXIT
mkdir -p "$work_root"

kdir=
kernel_source=
kernel_source_package_version=
kernel_headers_package_version=
rustc=
bindgen=
rustfmt=
clang=
llvm_link=
llvm_opt=
llvm_nm=
target_cc=
module=

case $distro in
    ubuntu) configure_ubuntu ;;
    debian) configure_debian ;;
    fedora) configure_fedora ;;
    opensuse) configure_opensuse ;;
    *) die "unsupported build distribution: $distro" ;;
esac

require_command make
require_command modinfo
require_command modprobe

build_source_package_module
copy_module_plan "$output_root/module-dependencies" zerofs zerofs
copy_boot_modules
publish_build_outputs
