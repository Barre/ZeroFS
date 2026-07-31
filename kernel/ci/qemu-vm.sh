#!/usr/bin/env bash
#
# Run ZeroFS CI steps inside a pinned Ubuntu cloud image with KVM.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
readonly script_name=${0##*/}
readonly guest_user=runner
readonly image_release=20260720
readonly image_download_name=ubuntu-26.04-server-cloudimg-amd64.img
readonly image_cache_name=ubuntu-26.04-server-cloudimg-amd64-${image_release}.img
readonly image_base_url="https://cloud-images.ubuntu.com/releases/resolute"
readonly image_url="$image_base_url/release-${image_release}/$image_download_name"
readonly image_sha256=117816726abbdefc5ef3e38902e81a76f1c76c3610e709999d0885f9d5d9b477
readonly default_ssh_port=22222
readonly default_memory_mb=6144
readonly default_disk_gb=48

state_dir=
cache_dir=
base_image=
overlay_image=
seed_image=
user_data=
meta_data=
serial_log=
serial_artifact=
pid_file=
port_file=
ready_file=
ssh_private_key=
ssh_public_key=
known_hosts=
ssh_port=

usage() {
    cat >&2 <<EOF
usage: $script_name start
       $script_name exec <working-directory>
       $script_name pull <guest-path> <host-path>
       $script_name stop

exec reads a Bash script from stdin. Relative working directories are resolved
against the caller's current directory before entering the guest.
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

require_absolute_path() {
    local name=$1
    local path=$2

    [[ "$path" == /* ]] || die "$name must be an absolute path: $path"
    [[ "$path" != / ]] || die "$name must not be /"
    [[ "$path" != *$'\n'* ]] || die "$name must not contain a newline"
}

require_unsigned_integer() {
    local name=$1
    local value=$2

    [[ "$value" =~ ^[0-9]+$ ]] ||
        die "$name must be a positive integer: $value"
    ((10#$value > 0)) ||
        die "$name must be a positive integer: $value"
}

init_paths() {
    local cache_parent

    : "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE must be set}"
    : "${RUNNER_TEMP:?RUNNER_TEMP must be set}"
    require_absolute_path GITHUB_WORKSPACE "$GITHUB_WORKSPACE"
    require_absolute_path RUNNER_TEMP "$RUNNER_TEMP"

    if [[ -n ${RUNNER_TOOL_CACHE:-} ]]; then
        cache_parent=$RUNNER_TOOL_CACHE
    else
        : "${HOME:?HOME must be set when RUNNER_TOOL_CACHE is absent}"
        cache_parent="$HOME/.cache"
    fi
    require_absolute_path cache_parent "$cache_parent"

    state_dir="$RUNNER_TEMP/zerofs-qemu-vm"
    cache_dir="$cache_parent/zerofs-qemu"
    base_image="$cache_dir/$image_cache_name"
    overlay_image="$state_dir/root-overlay.qcow2"
    seed_image="$state_dir/cloud-init.iso"
    user_data="$state_dir/user-data"
    meta_data="$state_dir/meta-data"
    serial_log="$state_dir/serial.log"
    serial_artifact="$RUNNER_TEMP/zerofs-qemu-serial.log"
    pid_file="$state_dir/qemu.pid"
    port_file="$state_dir/ssh.port"
    ready_file="$state_dir/ready"
    ssh_private_key="$state_dir/id_ed25519"
    ssh_public_key="$state_dir/id_ed25519.pub"
    known_hosts="$state_dir/known_hosts"

    mkdir -p -- "$state_dir" "$cache_dir"
}

install_host_tools() {
    require_command apt-get
    if ((EUID != 0)); then
        require_command sudo
        sudo -n true || die "passwordless sudo is required"
    fi

    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get update
    run_as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends \
        ca-certificates \
        cloud-image-utils \
        cpio \
        curl \
        openssh-client \
        qemu-system-x86 \
        qemu-utils \
        rsync
}

require_kvm() {
    [[ "$(uname -m)" == x86_64 ]] ||
        die "KVM VM execution requires an x86_64 host"
    [[ -c /dev/kvm ]] ||
        die "/dev/kvm is unavailable; TCG emulation is intentionally unsupported"

    if [[ ! -r /dev/kvm || ! -w /dev/kvm ]]; then
        if ((EUID != 0)); then
            require_command sudo
            sudo -n true || die "passwordless sudo is required to access /dev/kvm"
        fi
        run_as_root chmod 0666 /dev/kvm
    fi
    [[ -r /dev/kvm && -w /dev/kvm ]] ||
        die "/dev/kvm is not readable and writable by the runner"
}

image_checksum_is_valid() {
    [[ -f "$base_image" ]] || return 1
    printf '%s  %s\n' "$image_sha256" "$base_image" |
        sha256sum --check --status
}

download_base_image() {
    local download_path="$base_image.download"

    if image_checksum_is_valid; then
        chmod 0444 "$base_image"
        echo "using cached Ubuntu image $base_image"
        return
    fi

    rm -f -- "$base_image" "$download_path"
    echo "downloading pinned Ubuntu image $image_url"
    curl --fail --location --retry 5 --retry-all-errors \
        --output "$download_path" "$image_url"
    printf '%s  %s\n' "$image_sha256" "$download_path" |
        sha256sum --check --status ||
        die "SHA256 verification failed for $image_url"
    mv -- "$download_path" "$base_image"
    chmod 0444 "$base_image"
}

read_vm_pid() {
    local pid

    [[ -r "$pid_file" ]] || return 1
    pid=$(<"$pid_file")
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$pid"
}

vm_process_matches() {
    local cmdline
    local pid=$1

    [[ -r "/proc/$pid/cmdline" ]] || return 1
    cmdline=$(tr '\0' '\n' <"/proc/$pid/cmdline")
    [[ "$cmdline" == *"$overlay_image"* ]]
}

vm_is_running() {
    local pid

    pid=$(read_vm_pid) || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    vm_process_matches "$pid"
}

load_ssh_port() {
    [[ -r "$port_file" ]] || die "VM SSH port is unavailable; run start first"
    ssh_port=$(<"$port_file")
    require_unsigned_integer ssh_port "$ssh_port"
    ((ssh_port <= 65535)) || die "invalid VM SSH port: $ssh_port"
}

ssh_guest() {
    ssh \
        -i "$ssh_private_key" \
        -p "$ssh_port" \
        -o BatchMode=yes \
        -o ConnectTimeout=5 \
        -o ServerAliveInterval=15 \
        -o ServerAliveCountMax=4 \
        -o StrictHostKeyChecking=no \
        -o "UserKnownHostsFile=$known_hosts" \
        "$guest_user@127.0.0.1" "$@"
}

rsync_shell() {
    printf 'ssh -i %q -p %q' "$ssh_private_key" "$ssh_port"
    printf ' -o %q' \
        BatchMode=yes \
        ConnectTimeout=5 \
        ServerAliveInterval=15 \
        ServerAliveCountMax=4 \
        StrictHostKeyChecking=no \
        "UserKnownHostsFile=$known_hosts"
}

rsync_to_guest() {
    local source=$1
    local destination=$2
    local transport

    transport=$(rsync_shell)
    rsync -a -s -e "$transport" \
        "$source" "$guest_user@127.0.0.1:$destination"
}

rsync_from_guest() {
    local source=$1
    local destination=$2
    local transport

    transport=$(rsync_shell)
    rsync -a -s -e "$transport" \
        "$guest_user@127.0.0.1:$source" "$destination"
}

collect_serial_log() {
    [[ -f "$serial_log" ]] || return 0
    cp -- "$serial_log" "$serial_artifact"
    echo "QEMU serial log: $serial_artifact"
    echo "----- QEMU serial log tail -----"
    tail -n 200 "$serial_log" || true
    echo "----- end QEMU serial log -----"
}

cleanup_runtime_files() {
    rm -f -- \
        "$overlay_image" \
        "$seed_image" \
        "$user_data" \
        "$meta_data" \
        "$serial_log" \
        "$pid_file" \
        "$port_file" \
        "$ready_file" \
        "$ssh_private_key" \
        "$ssh_public_key" \
        "$known_hosts"
}

create_cloud_init() {
    local instance_id
    local public_key

    ssh-keygen -q -t ed25519 -N '' -f "$ssh_private_key"
    public_key=$(<"$ssh_public_key")
    instance_id="zerofs-ci-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"

    cat >"$user_data" <<EOF
#cloud-config
users:
  - default
  - name: $guest_user
    gecos: ZeroFS CI runner
    groups: [adm, sudo]
    shell: /bin/bash
    sudo: ALL=(ALL) NOPASSWD:ALL
    lock_passwd: true
    ssh_authorized_keys:
      - $public_key
ssh_pwauth: false
package_update: true
packages:
  - ca-certificates
  - curl
  - docker.io
  - netcat-openbsd
  - rsync
  - wget
growpart:
  mode: auto
  devices: ['/']
resize_rootfs: true
runcmd:
  - [systemctl, enable, --now, docker]
EOF

    cat >"$meta_data" <<EOF
instance-id: $instance_id
local-hostname: zerofs-ci
EOF

    cloud-localds "$seed_image" "$user_data" "$meta_data"
}

create_overlay() {
    local disk_gb=${ZEROFS_QEMU_DISK_GB:-$default_disk_gb}

    require_unsigned_integer ZEROFS_QEMU_DISK_GB "$disk_gb"
    ((disk_gb >= 30)) ||
        die "ZEROFS_QEMU_DISK_GB must be at least 30"
    qemu-img create -q -f qcow2 -F qcow2 \
        -b "$base_image" "$overlay_image" "${disk_gb}G"
}

boot_vm() {
    local cpus=${ZEROFS_QEMU_CPUS:-}
    local memory_mb=${ZEROFS_QEMU_MEMORY_MB:-$default_memory_mb}

    if [[ -z "$cpus" ]]; then
        cpus=$(nproc)
        ((cpus > 4)) && cpus=4
    fi
    require_unsigned_integer ZEROFS_QEMU_CPUS "$cpus"
    require_unsigned_integer ZEROFS_QEMU_MEMORY_MB "$memory_mb"
    ((memory_mb >= 4096)) ||
        die "ZEROFS_QEMU_MEMORY_MB must be at least 4096"

    ssh_port=${ZEROFS_QEMU_SSH_PORT:-$default_ssh_port}
    require_unsigned_integer ZEROFS_QEMU_SSH_PORT "$ssh_port"
    ((ssh_port <= 65535)) ||
        die "ZEROFS_QEMU_SSH_PORT must be at most 65535"
    printf '%s\n' "$ssh_port" >"$port_file"
    : >"$serial_log"

    echo "booting Ubuntu VM with KVM: ${cpus} CPUs, ${memory_mb} MiB RAM"
    qemu-system-x86_64 \
        -name zerofs-ci \
        -machine q35,accel=kvm \
        -cpu host \
        -smp "$cpus" \
        -m "$memory_mb" \
        -nodefaults \
        -no-reboot \
        -display none \
        -serial "file:$serial_log" \
        -device virtio-rng-pci \
        -drive "if=virtio,format=qcow2,file=$overlay_image,discard=unmap" \
        -drive "if=virtio,format=raw,file=$seed_image,readonly=on" \
        -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$ssh_port-:22" \
        -device virtio-net-pci,netdev=net0 \
        -daemonize \
        -pidfile "$pid_file"
}

wait_for_ssh() {
    local attempt

    for ((attempt = 1; attempt <= 120; attempt++)); do
        vm_is_running || return 1
        if ssh_guest true >/dev/null 2>&1; then
            return
        fi
        sleep 2
    done
    return 1
}

prepare_guest_paths() {
    local quoted_temp
    local quoted_workspace

    printf -v quoted_workspace '%q' "$GITHUB_WORKSPACE"
    printf -v quoted_temp '%q' "$RUNNER_TEMP"
    ssh_guest \
        "sudo install -d -o $guest_user -g $guest_user -m 0755 -- \
$quoted_workspace $quoted_temp"
}

sync_checkout() {
    local transport

    transport=$(rsync_shell)
    echo "syncing checkout to guest $GITHUB_WORKSPACE"
    rsync -a -s --delete -e "$transport" \
        "$GITHUB_WORKSPACE/" \
        "$guest_user@127.0.0.1:$GITHUB_WORKSPACE/"

    echo "syncing runner temp artifacts to guest $RUNNER_TEMP"
    rsync -a -s --delete \
        --exclude="/${state_dir##*/}/" \
        --exclude="/${serial_artifact##*/}" \
        -e "$transport" \
        "$RUNNER_TEMP/" \
        "$guest_user@127.0.0.1:$RUNNER_TEMP/"
}

stop_vm() {
    local attempt
    local pid

    if vm_is_running; then
        pid=$(read_vm_pid)
        load_ssh_port
        if [[ -f "$ssh_private_key" ]]; then
            ssh_guest "sudo poweroff" >/dev/null 2>&1 || true
        fi

        for ((attempt = 1; attempt <= 30; attempt++)); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            vm_process_matches "$pid" ||
                die "PID $pid no longer belongs to the ZeroFS VM; refusing to signal it"
            kill "$pid"
            for ((attempt = 1; attempt <= 10; attempt++)); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
        fi
        if kill -0 "$pid" 2>/dev/null; then
            vm_process_matches "$pid" ||
                die "PID $pid no longer belongs to the ZeroFS VM; refusing to signal it"
            kill -KILL "$pid"
        fi
    else
        echo "ZeroFS CI VM is not running"
    fi

    collect_serial_log
    rm -f -- "$pid_file" "$port_file" "$ready_file"
}

start_vm() {
    local existing_pid

    require_kvm
    install_host_tools
    require_command cloud-localds
    require_command curl
    require_command qemu-img
    require_command qemu-system-x86_64
    require_command rsync
    require_command sha256sum
    require_command ssh
    require_command ssh-keygen

    if existing_pid=$(read_vm_pid) &&
        kill -0 "$existing_pid" 2>/dev/null &&
        ! vm_process_matches "$existing_pid"; then
        die "PID $existing_pid no longer belongs to the ZeroFS VM; refusing to replace it"
    fi

    if vm_is_running; then
        echo "reusing running ZeroFS CI VM"
        load_ssh_port
    else
        cleanup_runtime_files
        download_base_image
        create_overlay
        create_cloud_init
        boot_vm
    fi

    if ! wait_for_ssh; then
        collect_serial_log
        die "VM did not become reachable over SSH"
    fi
    echo "waiting for guest cloud-init"
    if ! ssh_guest "sudo timeout 600 cloud-init status --wait --long"; then
        collect_serial_log
        die "guest cloud-init failed"
    fi

    prepare_guest_paths
    sync_checkout
    touch "$ready_file"
    echo "ZeroFS CI VM is ready"
}

require_ready_vm() {
    vm_is_running || die "VM is not running; run start first"
    [[ -f "$ready_file" ]] || die "VM setup is incomplete; run start first"
    [[ -f "$ssh_private_key" ]] || die "VM SSH identity is missing"
    load_ssh_port
}

sync_file_to_guest_if_present() {
    local path=$1
    local parent
    local quoted_parent

    [[ -f "$path" ]] || return 0
    parent=$(dirname -- "$path")
    printf -v quoted_parent '%q' "$parent"
    ssh_guest "install -d -m 0755 -- $quoted_parent"
    rsync_to_guest "$path" "$path"
}

sync_command_files_to_guest() {
    local variable
    local path

    if [[ -n ${GITHUB_EVENT_PATH:-} ]]; then
        sync_file_to_guest_if_present "$GITHUB_EVENT_PATH"
    fi
    for variable in \
        GITHUB_ENV \
        GITHUB_OUTPUT \
        GITHUB_PATH \
        GITHUB_STEP_SUMMARY; do
        path=${!variable:-}
        [[ -n "$path" ]] || continue
        sync_file_to_guest_if_present "$path"
    done
}

pull_command_files_from_guest() {
    local variable
    local path
    local status=0

    for variable in \
        GITHUB_ENV \
        GITHUB_OUTPUT \
        GITHUB_PATH \
        GITHUB_STEP_SUMMARY; do
        path=${!variable:-}
        [[ -n "$path" ]] || continue
        [[ -f "$path" ]] || continue
        rsync_from_guest "$path" "$path" || status=$?
    done
    return "$status"
}

write_remote_environment() {
    local name

    printf 'export ZEROFS_CI_GUEST=1\n'
    while IFS= read -r name; do
        [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        case $name in
            _ | JAVA_HOME | JAVA_HOME_* | OLDPWD | PWD | RUNNER_TOOL_CACHE | SHLVL | SSH_AGENT_PID | SSH_AUTH_SOCK | ZEROFS_CI_GUEST)
                continue
                ;;
        esac
        printf 'export %s=%q\n' "$name" "${!name}"
    done < <(compgen -e)
}

exec_guest_script() {
    local cwd=$1
    local guest_cwd
    local quoted_cwd
    local quoted_shell_adapter
    local remote_command
    local script_file
    local command_status
    local sync_status=0

    require_ready_vm
    if [[ "$cwd" == /* ]]; then
        guest_cwd=$cwd
    else
        guest_cwd=$(realpath -m -- "$PWD/$cwd")
    fi
    require_absolute_path working-directory "$guest_cwd"
    printf -v quoted_cwd '%q' "$guest_cwd"
    printf -v quoted_shell_adapter '%q' "$script_dir/ci-shell.sh"

    script_file=$(mktemp "$state_dir/step.XXXXXX")
    write_remote_environment >"$script_file"
    cat >>"$script_file"

    sync_command_files_to_guest
    remote_command="cd -- $quoted_cwd && exec $quoted_shell_adapter --guest-step"

    set +e
    ssh_guest "$remote_command" <"$script_file"
    command_status=$?
    set -e
    rm -f -- "$script_file"
    pull_command_files_from_guest || sync_status=$?

    if ((command_status != 0)); then
        return "$command_status"
    fi
    return "$sync_status"
}

pull_guest_path() {
    local guest_path=$1
    local host_path=$2
    local host_parent
    local quoted_guest_path

    require_ready_vm
    require_absolute_path guest-path "$guest_path"
    printf -v quoted_guest_path '%q' "$guest_path"
    if ssh_guest "test -d $quoted_guest_path"; then
        mkdir -p -- "$host_path"
        rsync_from_guest "${guest_path%/}/" "${host_path%/}/"
        return
    fi

    host_parent=$(dirname -- "$host_path")
    mkdir -p -- "$host_parent"
    rsync_from_guest "$guest_path" "$host_path"
}

main() {
    local command=${1:-}

    case $command in
        start)
            [[ $# -eq 1 ]] || {
                usage
                exit 2
            }
            init_paths
            start_vm
            ;;
        exec)
            [[ $# -eq 2 ]] || {
                usage
                exit 2
            }
            init_paths
            exec_guest_script "$2"
            ;;
        pull)
            [[ $# -eq 3 ]] || {
                usage
                exit 2
            }
            init_paths
            pull_guest_path "$2" "$3"
            ;;
        stop)
            [[ $# -eq 1 ]] || {
                usage
                exit 2
            }
            init_paths
            stop_vm
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
