#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
readonly pass_marker=ZEROFS_MODULE_SMOKE=PASS
readonly guest_server=10.0.2.2:5564
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd)
readonly repo_root
readonly catalog_helper="$repo_root/packaging/kernel/kernel-targets.py"

work_dir=
server_pid=
server_log=
server_log_printed=false

usage() {
    cat >&2 <<EOF
usage: $script_name prepare KERNEL_BUILD MODULE BUNDLE [BUSYBOX]
       $script_name bundle BUNDLE SERVER_BINARY
       $script_name package MANIFEST TARGET_ID ARTIFACT_DIR SERVER_BINARY
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

require_file() {
    [[ -s "$1" ]] || die "required file not found or empty: $1"
}

normalized_host_arch() {
    case $(uname -m) in
        x86_64) printf '%s\n' x86_64 ;;
        aarch64 | arm64) printf '%s\n' aarch64 ;;
        *) printf '\n' ;;
    esac
}

run_as_root() {
    if ((EUID == 0)); then
        "$@"
    else
        sudo -- "$@"
    fi
}

print_server_log() {
    [[ "$server_log_printed" == false && -f "$server_log" ]] || return
    echo "----- ZeroFS server log -----"
    cat "$server_log"
    echo "----- end ZeroFS server log -----"
    server_log_printed=true
}

cleanup() {
    local status=$?

    set +e
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null
        wait "$server_pid" 2>/dev/null
    fi
    if ((status != 0)); then
        print_server_log
    fi
    if [[ -n "$work_dir" && -d "$work_dir" ]]; then
        case ${work_dir##*/} in
            zerofs-module-smoke.*)
                run_as_root rm -rf -- "$work_dir"
                ;;
        esac
    fi
    return "$status"
}

trap cleanup EXIT

write_guest_init() {
    local output=$1
    local kernel_release=$2
    local boot_count=$3
    local dependency_count=$4
    local unload=$5
    local index

    {
        cat <<EOF
#!/bin/busybox sh
set -eu

bb=/bin/busybox
\$bb mount -t proc proc /proc
\$bb mount -t sysfs sysfs /sys
\$bb test "\$(\$bb uname -r)" = "$kernel_release"
EOF
        for ((index = 0; index < boot_count; index++)); do
            printf '$bb insmod /modules/boot-%04d.ko\n' "$index"
        done
        cat <<'EOF'
$bb ip link set lo up
$bb ip link set eth0 up
$bb ip address add 10.0.2.15/24 dev eth0
EOF
        for ((index = 0; index < dependency_count; index++)); do
            printf '$bb insmod /modules/dependency-%04d.ko\n' "$index"
        done
        cat <<EOF
\$bb insmod /modules/zerofs.ko
\$bb test -d /sys/module/zerofs
\$bb grep -qw zerofs /proc/filesystems
\$bb mount -t zerofs \
    -o consistency=strict,msize=1048576 \
    "$guest_server" /mnt/zerofs
\$bb grep -qw zerofs /proc/mounts
payload=zerofs-kernel-smoke
\$bb echo "\$payload" > /mnt/zerofs/original
\$bb sync
\$bb test "\$(\$bb cat /mnt/zerofs/original)" = "\$payload"
\$bb mv /mnt/zerofs/original /mnt/zerofs/renamed
\$bb test ! -e /mnt/zerofs/original
\$bb mkdir /mnt/zerofs/directory
\$bb mv /mnt/zerofs/renamed /mnt/zerofs/directory/renamed
\$bb test "\$(\$bb cat /mnt/zerofs/directory/renamed)" = "\$payload"
\$bb rm /mnt/zerofs/directory/renamed
\$bb rmdir /mnt/zerofs/directory
\$bb umount /mnt/zerofs
EOF
        if [[ "$unload" == true ]]; then
            cat <<'EOF'
$bb rmmod zerofs
$bb test ! -d /sys/module/zerofs
EOF
        fi
        cat <<EOF
\$bb echo "$pass_marker"
\$bb reboot -f
EOF
    } >"$output"
    chmod 0755 "$output"
}

prepare_bundle() {
    local kernel_build
    local module
    local bundle
    local busybox
    local applet
    local arch
    local host_arch
    local kernel_image
    local kernel_release
    local machine
    local module_vermagic

    [[ $# -eq 3 || $# -eq 4 ]] || {
        usage
        exit 2
    }

    kernel_build=$(realpath "$1")
    module=$(realpath "$2")
    bundle=$(realpath -m "$3")
    [[ "$bundle" != / ]] || die "bundle directory must not be /"

    require_file "$kernel_build/.config"
    if grep -qx 'CONFIG_X86_64=y' "$kernel_build/.config"; then
        arch=x86_64
        kernel_image="$kernel_build/arch/x86/boot/bzImage"
    elif grep -qx 'CONFIG_ARM64=y' "$kernel_build/.config"; then
        arch=aarch64
        kernel_image="$kernel_build/arch/arm64/boot/Image"
    else
        die "smoke kernel is not x86_64 or arm64"
    fi
    require_file "$kernel_image"
    require_file "$module"
    for option in \
        INET NET NETDEVICES NETFS_SUPPORT UNIX VIRTIO VIRTIO_NET; do
        grep -qx "CONFIG_${option}=y" "$kernel_build/.config" ||
            die "network smoke kernel does not enable CONFIG_$option=y"
    done
    case $arch in
        x86_64)
            for option in PCI VIRTIO_MENU VIRTIO_PCI; do
                grep -qx "CONFIG_${option}=y" "$kernel_build/.config" ||
                    die "x86 smoke kernel does not enable CONFIG_$option=y"
            done
            ;;
        aarch64)
            grep -qx 'CONFIG_VIRTIO_MMIO=y' "$kernel_build/.config" ||
                die "arm64 smoke kernel does not enable CONFIG_VIRTIO_MMIO=y"
            ;;
    esac

    require_command modinfo
    require_command readelf
    require_command uname
    if [[ $# -eq 4 ]]; then
        busybox=$(realpath "$4")
        require_file "$busybox"
    else
        require_command busybox
        busybox=$(command -v busybox)
    fi
    if readelf -l "$busybox" |
        grep -F 'Requesting program interpreter' >/dev/null; then
        die "$busybox is dynamically linked; install busybox-static"
    fi
    machine=$(readelf -h "$busybox" |
        sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
    case "$arch:$machine" in
        "x86_64:Advanced Micro Devices X86-64" | "aarch64:AArch64") ;;
        *) die "$busybox has the wrong architecture: $machine" ;;
    esac
    host_arch=$(normalized_host_arch)
    if [[ "$arch" == "$host_arch" ]]; then
        for applet in \
            cat echo grep insmod ip mkdir mount mv reboot rm rmdir rmmod \
            sh sync test umount uname; do
            "$busybox" --list | grep -Fx "$applet" >/dev/null ||
                die "$busybox does not provide the $applet applet"
        done
    fi

    [[ "$(modinfo -F name "$module")" == zerofs ]] ||
        die "module name is not zerofs"
    module_vermagic=$(modinfo -F vermagic "$module")
    kernel_release=${module_vermagic%% *}
    [[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~-]*$ ]] ||
        die "module vermagic contains an invalid kernel release"

    if [[ -e "$bundle" ]]; then
        [[ -d "$bundle" ]] || die "bundle path is not a directory: $bundle"
        [[ -z $(find "$bundle" -mindepth 1 -print -quit) ]] ||
            die "bundle directory is not empty: $bundle"
    else
        mkdir -p -- "$bundle"
    fi

    install -m 0644 \
        "$kernel_image" \
        "$bundle/kernel"
    install -m 0755 "$busybox" "$bundle/busybox"
    install -m 0644 "$module" "$bundle/zerofs.ko"
    printf '%s\n' "$arch" >"$bundle/arch"
    printf '%s\n' "$kernel_release" >"$bundle/kernel-release"
    echo "prepared module smoke bundle in $bundle"
}

resolve_artifact() {
    local manifest=$1
    local target_id=$2
    local artifact_dir=$3
    local entry
    local kind
    local path

    [[ -x "$catalog_helper" ]] ||
        die "target catalog helper is missing: $catalog_helper"
    manifest=$(realpath "$manifest")
    artifact_dir=$(realpath "$artifact_dir")
    [[ -f "$manifest" ]] || die "manifest is not a regular file: $manifest"
    [[ -d "$artifact_dir" ]] ||
        die "artifact directory does not exist: $artifact_dir"
    [[ -f "$artifact_dir/artifact.json" ]] ||
        die "artifact manifest is missing: $artifact_dir/artifact.json"

    arch=$("$catalog_helper" \
        --manifest "$manifest" field "$target_id" arch)
    kernel_release=$("$catalog_helper" \
        --manifest "$manifest" field "$target_id" kernel_release)
    [[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~-]*$ ]] ||
        die "kernel release contains unsupported characters: $kernel_release"
    case $arch in
        x86_64 | aarch64) ;;
        *)
            die "unsupported exact-kernel boot architecture: $arch"
            ;;
    esac

    export ZEROFS_ARTIFACT_DIR=$artifact_dir
    export ZEROFS_TARGET_ID=$target_id
    export ZEROFS_KERNEL_RELEASE=$kernel_release
    export ZEROFS_TARGET_ARCH=$arch
    mapfile -t artifact_entries < <(
        python3 - "$artifact_dir/artifact.json" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

manifest = Path(sys.argv[1])
try:
    value = json.loads(manifest.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"{manifest}: invalid artifact manifest: {error}")
if (
    not isinstance(value, dict)
    or type(value.get("schema_version")) is not int
    or value["schema_version"] != 2
):
    raise SystemExit(f"{manifest}: expected artifact schema version 2")

expected = {
    "target_id": os.environ["ZEROFS_TARGET_ID"],
    "kernel_release": os.environ["ZEROFS_KERNEL_RELEASE"],
    "arch": os.environ["ZEROFS_TARGET_ARCH"],
}
for key, expected_value in expected.items():
    if value.get(key) != expected_value:
        raise SystemExit(
            f"{manifest}: {key} is {value.get(key)!r}, expected {expected_value!r}"
        )

base = Path(os.environ["ZEROFS_ARTIFACT_DIR"]).resolve()
digests = value.get("sha256")
if not isinstance(digests, dict):
    raise SystemExit(f"{manifest}: sha256 must be an object")


def resolve(relative, key):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise SystemExit(f"{manifest}: {key} must be a relative path")
    if relative != relative.strip() or any(ord(char) < 32 for char in relative):
        raise SystemExit(f"{manifest}: {key} contains unsupported characters")
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        raise SystemExit(f"{manifest}: {key} escapes the artifact directory")
    if not candidate.is_file():
        raise SystemExit(f"{manifest}: {key} is not a regular file")
    expected_digest = digests.get(relative)
    if not isinstance(expected_digest, str) or len(expected_digest) != 64:
        raise SystemExit(f"{manifest}: missing SHA-256 for {key}")
    if hashlib.sha256(candidate.read_bytes()).hexdigest() != expected_digest:
        raise SystemExit(f"{manifest}: SHA-256 mismatch for {key}")
    return candidate


print(f"kernel\t{resolve(value.get('kernel_image'), 'kernel_image')}")
print(f"module\t{resolve(value.get('module'), 'module')}")
print(f"busybox\t{resolve(value.get('boot_busybox'), 'boot_busybox')}")
dependencies = value.get("module_dependencies")
boot_modules = value.get("boot_modules")
if not isinstance(dependencies, list) or not isinstance(boot_modules, list):
    raise SystemExit(f"{manifest}: module lists must be arrays")
for index, relative in enumerate(dependencies):
    print(f"dependency\t{resolve(relative, f'module_dependencies[{index}]')}")
for index, relative in enumerate(boot_modules):
    print(f"boot\t{resolve(relative, f'boot_modules[{index}]')}")
PY
    )

    kernel_image=
    module=
    busybox=
    dependencies=()
    boot_modules=()
    for entry in "${artifact_entries[@]}"; do
        kind=${entry%%$'\t'*}
        path=${entry#*$'\t'}
        [[ "$path" != "$entry" ]] ||
            die "artifact resolver returned an invalid entry"
        case $kind in
            kernel) kernel_image=$path ;;
            module) module=$path ;;
            busybox) busybox=$path ;;
            dependency) dependencies+=("$path") ;;
            boot) boot_modules+=("$path") ;;
            *) die "artifact resolver returned an unknown entry: $kind" ;;
        esac
    done
    [[ -n "$kernel_image" && -n "$module" && -n "$busybox" ]] ||
        die "artifact manifest did not resolve its boot inputs"
}

build_initramfs() {
    local rootfs="$work_dir/rootfs"
    local initramfs="$work_dir/initramfs.cpio"
    local index
    local name

    mkdir -p -- \
        "$rootfs/bin" \
        "$rootfs/dev" \
        "$rootfs/mnt/zerofs" \
        "$rootfs/modules" \
        "$rootfs/proc" \
        "$rootfs/sys"
    install -m 0755 "$busybox" "$rootfs/bin/busybox"
    install -m 0644 "$module" "$rootfs/modules/zerofs.ko"
    for index in "${!dependencies[@]}"; do
        printf -v name 'dependency-%04d.ko' "$index"
        install -m 0644 "${dependencies[$index]}" "$rootfs/modules/$name"
    done
    for index in "${!boot_modules[@]}"; do
        printf -v name 'boot-%04d.ko' "$index"
        install -m 0644 "${boot_modules[$index]}" "$rootfs/modules/$name"
    done
    write_guest_init \
        "$rootfs/init" \
        "$kernel_release" \
        "${#boot_modules[@]}" \
        "${#dependencies[@]}" \
        "$unload_module"
    run_as_root mknod "$rootfs/dev/console" c 5 1
    run_as_root chmod 0600 "$rootfs/dev/console"
    (
        cd "$rootfs"
        find . -print0 | sort -z |
            cpio --null --create --format=newc --quiet >"$initramfs"
    )
    require_file "$initramfs"
    initramfs_image=$initramfs
}

start_server() {
    local server_binary=$1
    local config="$work_dir/server.toml"
    local cache="$work_dir/cache"
    local ready=false

    server_binary=$(realpath "$server_binary")
    [[ -x "$server_binary" ]] ||
        die "server binary is not executable: $server_binary"
    mkdir -p -- "$cache"
    cat >"$config" <<EOF
[cache]
dir = "$cache"
disk_size_gb = 0.1
memory_size_gb = 0.25

[storage]
url = "memory:///"
encryption_password = "test-password-123"

[servers.ninep]
addresses = ["127.0.0.1:5564"]

[telemetry]
enabled = false
EOF

    server_log="$work_dir/server.log"
    "$server_binary" run -c "$config" >"$server_log" 2>&1 &
    server_pid=$!
    for _ in {1..60}; do
        if (
            exec 9<>/dev/tcp/127.0.0.1/5564
            exec 9>&-
        ) 2>/dev/null; then
            ready=true
            break
        fi
        kill -0 "$server_pid" 2>/dev/null || break
        sleep 1
    done
    [[ "$ready" == true ]] || die "ZeroFS did not become ready"
}

run_qemu() {
    local arch=$1
    local kernel_image=$2
    local initramfs=$3
    local label=$4
    local qemu
    local machine
    local console
    local network_device
    local accelerator=tcg
    local cpu=max
    local qemu_timeout
    local host_arch
    local serial_log="$work_dir/serial.log"
    local qemu_status
    local -a qemu_arguments

    case $arch in
        x86_64)
            qemu=qemu-system-x86_64
            machine=q35
            console=ttyS0
            network_device=virtio-net-pci
            qemu_timeout=120
            ;;
        aarch64)
            qemu=qemu-system-aarch64
            machine=virt
            console=ttyAMA0
            network_device=virtio-net-device
            qemu_timeout=300
            ;;
        *)
            die "unsupported smoke architecture: $arch"
            ;;
    esac
    require_command "$qemu"
    require_command timeout

    host_arch=$(normalized_host_arch)
    if [[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]] &&
       [[ "$arch" == "$host_arch" ]]; then
        accelerator=kvm
        cpu=host
    fi

    : >"$serial_log"
    qemu_arguments=(
        -machine "$machine,accel=$accelerator"
        -cpu "$cpu"
        -smp 1
        -m 768
        -nodefaults
        -display none
        -monitor none
        -no-reboot
        -serial "file:$serial_log"
        -kernel "$kernel_image"
        -initrd "$initramfs"
        -append "console=$console,115200 rdinit=/init panic=1 panic_on_warn=1 oops=panic"
        -netdev "user,id=net0,ipv6=off,net=10.0.2.0/24,host=10.0.2.2"
        -device "$network_device,netdev=net0"
    )

    set +e
    timeout --signal=TERM --kill-after=5s "${qemu_timeout}s" \
        "$qemu" "${qemu_arguments[@]}"
    qemu_status=$?
    set -e

    echo "----- $label serial log -----"
    sed 's/\r$//' "$serial_log"
    echo "----- end $label serial log -----"
    ((qemu_status == 0)) ||
        die "QEMU exited with status $qemu_status"
    grep -F "$pass_marker" "$serial_log" >/dev/null ||
        die "$label did not complete the ZeroFS smoke test"
}

run_resolved() {
    local server_binary=$1
    local label=$2

    require_command cpio
    require_command find
    require_command mknod
    require_command sort
    if ((EUID != 0)); then
        require_command sudo
        sudo -n true ||
            die "passwordless sudo is required to create /dev/console"
    fi
    work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zerofs-module-smoke.XXXXXX")
    build_initramfs
    start_server "$server_binary"
    run_qemu "$arch" "$kernel_image" "$initramfs_image" "$label"
    print_server_log
    echo "$label mount and I/O smoke passed"
}

run_bundle() {
    local bundle

    [[ $# -eq 2 ]] || {
        usage
        exit 2
    }
    bundle=$(realpath "$1")
    require_file "$bundle/kernel"
    require_file "$bundle/busybox"
    require_file "$bundle/zerofs.ko"
    require_file "$bundle/arch"
    require_file "$bundle/kernel-release"
    arch=$(<"$bundle/arch")
    case $arch in
        x86_64 | aarch64) ;;
        *) die "bundle contains an invalid architecture: $arch" ;;
    esac
    kernel_image="$bundle/kernel"
    busybox="$bundle/busybox"
    module="$bundle/zerofs.ko"
    kernel_release=$(<"$bundle/kernel-release")
    [[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~-]*$ ]] ||
        die "bundle contains an invalid kernel release"
    dependencies=()
    boot_modules=()
    unload_module=true
    run_resolved "$2" "upstream kernel"
}

run_package() {
    [[ $# -eq 4 ]] || {
        usage
        exit 2
    }
    require_command python3
    resolve_artifact "$1" "$2" "$3"
    unload_module=false
    run_resolved "$4" "exact distro kernel"
}

case ${1:-} in
    prepare)
        shift
        prepare_bundle "$@"
        ;;
    bundle)
        shift
        run_bundle "$@"
        ;;
    package)
        shift
        run_package "$@"
        ;;
    *)
        usage
        exit 2
        ;;
esac
