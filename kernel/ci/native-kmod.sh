#!/usr/bin/env bash
#
# Build and load the native ZeroFS client on an Ubuntu CI runner.

set -euo pipefail

readonly script_name=${0##*/}
readonly request_timeout_ms=10000
readonly reconnect_grace_ms=180000

cleanup_directory=

cleanup() {
    if [[ -n "$cleanup_directory" && -d "$cleanup_directory" ]]; then
        case ${cleanup_directory##*/} in
            zerofs-module-ci.*)
                rm -rf -- "$cleanup_directory"
                ;;
        esac
    fi
}

trap cleanup EXIT

usage() {
    cat >&2 <<EOF
usage: $script_name load|unload

  load    Install the running kernel's build dependencies, run the kernel
          client tests, build zerofs.ko, and load it.
  unload  Unload zerofs.ko if it is loaded. All zerofs mounts must be gone.
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

run_as_root() {
    if ((EUID == 0)); then
        "$@"
    else
        sudo -- "$@"
    fi
}

module_is_loaded() {
    [[ -d /sys/module/zerofs ]]
}

unload_module() {
    if ! module_is_loaded; then
        echo "zerofs module is not loaded"
        return
    fi

    if findmnt -rn -t zerofs >/dev/null 2>&1; then
        echo "zerofs mounts must be unmounted before unloading the module:" >&2
        findmnt -rn -t zerofs -o TARGET,SOURCE,FSTYPE,OPTIONS >&2
        exit 1
    fi

    echo "unloading zerofs module"
    run_as_root rmmod zerofs
}

apt_package_available() {
    local package=$1

    apt-cache policy "$package" 2>/dev/null |
        awk -v package="$package" '
            $0 == package ":" {
                exact_package = 1
                next
            }
            exact_package && $1 == "Candidate:" {
                if ($2 != "(none)") {
                    available = 1
                }
                exact_package = 0
            }
            END {
                exit !available
            }
        '
}

select_tool_package() {
    local versioned_package=$1
    local unversioned_package=$2

    if apt_package_available "$versioned_package"; then
        printf '%s\n' "$versioned_package"
    elif apt_package_available "$unversioned_package"; then
        printf '%s\n' "$unversioned_package"
    else
        die "neither $versioned_package nor $unversioned_package is available"
    fi
}

select_tool_binary() {
    local versioned_binary=$1
    local unversioned_binary=$2

    if [[ -x "$versioned_binary" ]]; then
        printf '%s\n' "$versioned_binary"
    elif [[ -x "$unversioned_binary" ]]; then
        printf '%s\n' "$unversioned_binary"
    else
        die "neither $versioned_binary nor $unversioned_binary is executable"
    fi
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

tool_release() {
    local tool=$1
    local version_text=$2
    local prefix="${tool} "
    local remainder

    [[ "$version_text" == "$prefix"* ]] ||
        die "unexpected $tool version text in kernel configuration: $version_text"
    remainder=${version_text#"$prefix"}
    printf '%s\n' "${remainder%% *}"
}

verify_tool_version() {
    local tool_binary=$1
    local expected=$2
    local actual

    actual=$("$tool_binary" --version)
    [[ "$actual" == "$expected" ]] ||
        die "tool version mismatch: expected '$expected', found '$actual'"
}

require_rust_kernel() {
    local config_file=$1
    local kernel_release=$2

    grep -qx 'CONFIG_RUST=y' "$config_file" ||
        die "running kernel $kernel_release does not enable CONFIG_RUST=y; \
use a Rust-enabled runner"
}

check_ubuntu_host() {
    local distribution_id
    local version_id

    [[ -r /etc/os-release ]] || die "/etc/os-release is not readable"
    distribution_id=$(sed -n 's/^ID=//p' /etc/os-release)
    version_id=$(sed -n 's/^VERSION_ID=//p' /etc/os-release)
    distribution_id=${distribution_id%\"}
    distribution_id=${distribution_id#\"}
    version_id=${version_id%\"}
    version_id=${version_id#\"}

    [[ "$distribution_id" == ubuntu ]] ||
        die "load mode requires Ubuntu package naming; found $distribution_id $version_id"
    echo "using Ubuntu $version_id"
}

load_module() {
    local auto_conf
    local bindgen_binary
    local bindgen_package
    local bindgen_release
    local bindgen_series
    local bindgen_version_text
    local jobs
    local kdir
    local kernel_dir
    local kernel_release
    local metadata_package
    local module_path
    local rustc_binary
    local rustc_package
    local rustc_release
    local rustc_series
    local rustc_version_text
    local running_config
    local script_dir
    local staged_kernel_dir
    local staged_source

    check_ubuntu_host
    require_command apt-cache
    require_command apt-get
    require_command findmnt
    require_command sed
    require_command uname
    if ((EUID != 0)); then
        require_command sudo
        sudo -n true ||
            die "passwordless sudo is required"
    fi

    # Repeated CI setup is safe when no mount is using the previous build.
    unload_module

    kernel_release=$(uname -r)
    metadata_package="linux-lib-rust-$kernel_release"
    running_config="/boot/config-$kernel_release"
    if [[ -r "$running_config" ]]; then
        require_rust_kernel "$running_config" "$kernel_release"
    fi

    echo "installing headers for running kernel $kernel_release"
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get update
    apt_package_available "linux-headers-$kernel_release" ||
        die "linux-headers-$kernel_release is not available"
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
        build-essential \
        kmod \
        "linux-headers-$kernel_release"
    require_command make

    kdir="/lib/modules/$kernel_release/build"
    [[ -d "$kdir" ]] ||
        die "kernel headers did not create $kdir"
    auto_conf="$kdir/include/config/auto.conf"
    [[ -r "$auto_conf" ]] ||
        die "kernel configuration not found: $auto_conf"

    require_rust_kernel "$auto_conf" "$kernel_release"
    if [[ ! -s "$kdir/rust/libkernel.rmeta" ]]; then
        apt_package_available "$metadata_package" ||
            die "$metadata_package is not available; headers alone cannot build Rust modules"
        echo "installing Rust metadata for running kernel $kernel_release"
        run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y \
            --no-install-recommends \
            "$metadata_package"
    fi
    [[ -s "$kdir/rust/libkernel.rmeta" ]] ||
        die "Rust kernel metadata is missing: $kdir/rust/libkernel.rmeta"

    rustc_version_text=$(config_value "$auto_conf" CONFIG_RUSTC_VERSION_TEXT)
    bindgen_version_text=$(config_value "$auto_conf" CONFIG_BINDGEN_VERSION_TEXT)
    [[ -n "$rustc_version_text" ]] ||
        die "CONFIG_RUSTC_VERSION_TEXT is absent from $auto_conf"
    [[ -n "$bindgen_version_text" ]] ||
        die "CONFIG_BINDGEN_VERSION_TEXT is absent from $auto_conf"

    rustc_release=$(tool_release rustc "$rustc_version_text")
    bindgen_release=$(tool_release bindgen "$bindgen_version_text")
    rustc_series=${rustc_release%.*}
    bindgen_series=${bindgen_release%.*}

    rustc_package=$(select_tool_package "rustc-$rustc_series" rustc)
    bindgen_package=$(select_tool_package "bindgen-$bindgen_series" bindgen)
    echo "installing $rustc_package and $bindgen_package"
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
        "$rustc_package" \
        "$bindgen_package"

    rustc_binary=$(select_tool_binary \
        "/usr/bin/rustc-$rustc_series" /usr/bin/rustc)
    bindgen_binary=$(select_tool_binary \
        "/usr/bin/bindgen-$bindgen_series" /usr/bin/bindgen)
    verify_tool_version "$rustc_binary" "$rustc_version_text"
    verify_tool_version "$bindgen_binary" "$bindgen_version_text"

    script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
    kernel_dir=$(cd -- "$script_dir/.." && pwd)
    cleanup_directory=$(mktemp -d \
        "${TMPDIR:-/tmp}/zerofs-module-ci.XXXXXX")
    staged_source="$cleanup_directory/zerofs"
    "$kernel_dir/stage-module-source.sh" "$staged_source"
    staged_kernel_dir="$staged_source/kernel"
    module_path=$(make --no-print-directory -s -C "$staged_kernel_dir" \
        "KDIR=$kdir" module-path)
    jobs=$(nproc)

    echo "testing and building staged module source for $kernel_release"
    make -C "$staged_kernel_dir" \
        "KDIR=$kdir" "RUSTC=$rustc_binary" "BINDGEN=$bindgen_binary" clean
    make -C "$staged_kernel_dir" \
        "KDIR=$kdir" "RUSTC=$rustc_binary" "BINDGEN=$bindgen_binary" test
    make -j "$jobs" -C "$staged_kernel_dir" \
        "KDIR=$kdir" "RUSTC=$rustc_binary" "BINDGEN=$bindgen_binary"
    [[ -s "$module_path" ]] ||
        die "module build did not produce $module_path"

    echo "loading native client with ${reconnect_grace_ms}ms reconnect grace"
    run_as_root modprobe netfs
    run_as_root insmod "$module_path" \
        "request_timeout_ms=$request_timeout_ms" \
        "reconnect_grace_ms=$reconnect_grace_ms"

    module_is_loaded || die "zerofs module did not remain loaded"
    grep -qw zerofs /proc/filesystems ||
        die "zerofs is not registered in /proc/filesystems"
    echo "zerofs native client is loaded for $kernel_release"
}

main() {
    [[ $# -eq 1 ]] || {
        usage
        exit 2
    }

    case $1 in
        load)
            load_module
            ;;
        unload)
            require_command findmnt
            require_command rmmod
            if ((EUID != 0)); then
                require_command sudo
                sudo -n true ||
                    die "passwordless sudo is required"
            fi
            unload_module
            ;;
        -h | --help)
            usage
            ;;
        *)
            usage
            exit 2
            ;;
    esac
}

main "$@"
