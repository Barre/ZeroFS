#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd -P)
readonly repo_root
readonly catalog_helper="$script_dir/kernel-targets.py"

manifest=
target_id=
output_dir=
source_package=
work_dir=

usage() {
    cat >&2 <<EOF
usage: $script_name \
  --manifest PATH \
  --target-id ID \
  --output-dir DIRECTORY \
  --source-package PACKAGE_OR_DIRECTORY

--source-package accepts a package file or a staging directory containing one
package below its deb/ or rpm/ subdirectory.
EOF
}

die() {
    echo "$script_name: $*" >&2
    exit 1
}

require_value() {
    [[ -n ${2:-} ]] || die "$1 requires a value"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 ||
        die "required command not found: $1"
}

require_container_path() {
    local path=$1
    local kind=$2
    local resolved

    case $kind in
        file)
            [[ -f "$path" && ! -L "$path" ]] ||
                die "target builder did not produce a regular file: $path"
            ;;
        directory)
            [[ -d "$path" && ! -L "$path" ]] ||
                die "target builder did not produce a directory: $path"
            ;;
        *)
            die "internal error: unsupported container path kind"
            ;;
    esac
    resolved=$(realpath -e -- "$path")
    case $resolved in
        "$container_output" | "$container_output"/*) ;;
        *) die "target builder output escapes its directory: $path" ;;
    esac
}

validate_module() {
    local path=$1
    local expected_name=${2:-}
    local module_machine
    local module_name

    module_name=$(modinfo -F name "$path") ||
        die "cannot read module metadata from $path"
    [[ -n "$module_name" ]] || die "module name is empty: $path"
    if [[ -n "$expected_name" && "$module_name" != "$expected_name" ]]; then
        die "expected module $expected_name, found $module_name"
    fi
    case $(modinfo -F vermagic "$path") in
        "$kernel_release" | "$kernel_release "*) ;;
        *) die "$module_name does not target $kernel_release" ;;
    esac
    module_machine=$(readelf -h "$path" |
        sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
    [[ "$module_machine" == "$elf_machine" ]] ||
        die "$module_name has the wrong architecture: $module_machine"
}

require_container_module_directory() {
    local directory=$1
    local path

    require_container_path "$directory" directory
    while IFS= read -r -d '' path; do
        require_container_path "$path" file
        [[ "$path" == *.ko ]] ||
            die "target builder produced an unexpected module file: $path"
    done < <(find -P "$directory" -mindepth 1 -print0)
}

publish_module_set() {
    local source_directory=$1
    local output_directory=$2
    local relative_directory=$3
    local forbidden_module=$4
    local expected_name
    local index=0
    local module_name
    local source
    local destination
    local -a sources
    local -A seen_modules=()

    mkdir -p -- "$output_directory"
    shopt -s nullglob
    sources=("$source_directory"/*.ko)
    shopt -u nullglob
    for source in "${sources[@]}"; do
        printf -v expected_name '%04d.ko' "$index"
        [[ "${source##*/}" == "$expected_name" ]] ||
            die "$relative_directory has an unordered module: ${source##*/}"
        [[ -f "$source" && ! -L "$source" ]] ||
            die "$relative_directory contains an unsafe module path: $source"
        module_name=$(modinfo -F name "$source")
        [[ -z ${seen_modules[$module_name]:-} ]] ||
            die "$relative_directory contains duplicate module $module_name"
        seen_modules[$module_name]=1
        if [[ -n "$forbidden_module" &&
              "$module_name" == "$forbidden_module" ]]; then
            die "$relative_directory includes forbidden module $module_name"
        fi
        validate_module "$source"

        destination=$output_directory/$expected_name
        [[ ! -e "$destination" ]] ||
            die "duplicate module output: ${destination##*/}"
        install -m 0644 "$source" "$destination"
        ((index += 1))
    done
}

cleanup() {
    if [[ -n "$work_dir" && -d "$work_dir" ]]; then
        case ${work_dir##*/} in
            zerofs-build-target.*)
                rm -rf -- "$work_dir"
                ;;
        esac
    fi
}

trap cleanup EXIT

while (($#)); do
    case $1 in
        --manifest)
            require_value "$1" "${2:-}"
            manifest=$2
            shift 2
            ;;
        --target-id)
            require_value "$1" "${2:-}"
            target_id=$2
            shift 2
            ;;
        --output-dir)
            require_value "$1" "${2:-}"
            output_dir=$2
            shift 2
            ;;
        --source-package)
            require_value "$1" "${2:-}"
            source_package=$2
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[[ -n "$manifest" ]] || die "--manifest is required"
[[ -n "$target_id" ]] || die "--target-id is required"
[[ -n "$output_dir" ]] || die "--output-dir is required"
[[ -n "$source_package" ]] || die "--source-package is required"
require_command busybox
require_command find
require_command modinfo
require_command python3
require_command readelf
require_command realpath

[[ -x "$catalog_helper" ]] ||
    die "kernel lock helper is missing: $catalog_helper"
manifest=$(realpath "$manifest")
[[ -f "$manifest" ]] || die "manifest is not a regular file: $manifest"
if [[ -e "$output_dir" || -L "$output_dir" ]]; then
    [[ -d "$output_dir" && ! -L "$output_dir" ]] ||
        die "output path is not a regular directory: $output_dir"
    [[ -z $(find -P "$output_dir" -mindepth 1 -print -quit) ]] ||
        die "output directory is not empty: $output_dir"
else
    mkdir -p -- "$output_dir"
fi
output_dir=$(realpath -e -- "$output_dir")

target_field() {
    "$catalog_helper" --manifest "$manifest" field "$target_id" "$1"
}

family=$(target_field family)
arch=$(target_field arch)
kernel_release=$(target_field kernel_release)
distro=$(target_field distro)
release=$(target_field release)
apt_suite=
if [[ "$distro" == ubuntu || "$distro" == debian ]]; then
    apt_suite=$(target_field suite)
fi
kernel_package_version=$(target_field kernel_package_version)
builder_image=$(target_field builder_image)
source_json=$(target_field source)

source_package_input=$source_package
source_package=$(realpath -e -- "$source_package" 2>/dev/null) ||
    die "source package does not exist: $source_package_input"
if [[ -d "$source_package" && ! -L "$source_package" ]]; then
    shopt -s nullglob
    packages=("$source_package/$family"/*."$family")
    shopt -u nullglob
    [[ ${#packages[@]} -eq 1 ]] ||
        die "expected one $family source package, found ${#packages[@]}"
    source_package=${packages[0]}
fi
[[ -f "$source_package" && ! -L "$source_package" ]] ||
    die "source package is not a regular file: $source_package"
case $family:$source_package in
    deb:*.deb | rpm:*.rpm) ;;
    *) die "source package does not match target family $family" ;;
esac
case $family in
    deb)
        require_command dpkg-deb
        package_name=$(dpkg-deb -f "$source_package" Package)
        package_architecture=$(dpkg-deb -f "$source_package" Architecture)
        package_version=$(dpkg-deb -f "$source_package" Version)
        expected_architecture=all
        ;;
    rpm)
        require_command rpm
        package_name=$(rpm -qp --qf '%{NAME}' "$source_package")
        package_architecture=$(rpm -qp --qf '%{ARCH}' "$source_package")
        package_version=$(rpm -qp --qf '%{VERSION}-%{RELEASE}' "$source_package")
        expected_architecture=noarch
        ;;
esac
[[ "$package_name" == zerofs-kernel-client ]] ||
    die "source package has an unexpected name: $package_name"
[[ "$package_architecture" == "$expected_architecture" ]] ||
    die "source package has an unexpected architecture: $package_architecture"
[[ "$package_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[1-9][0-9]*$ ]] ||
    die "source package has an unsafe version: $package_version"

case $arch in
    x86_64)
        docker_platform=linux/amd64
        elf_machine='Advanced Micro Devices X86-64'
        ;;
    aarch64)
        docker_platform=linux/arm64
        elf_machine=AArch64
        ;;
    *)
        die "unsupported target architecture after validation: $arch"
        ;;
esac

boot_busybox_source=$(command -v busybox)
boot_busybox_machine=$(readelf -h "$boot_busybox_source" |
    sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
[[ "$boot_busybox_machine" == "$elf_machine" ]] ||
    die "busybox is not a $arch ELF executable: $boot_busybox_machine"
if readelf -l "$boot_busybox_source" |
    grep -F 'Requesting program interpreter' >/dev/null; then
    die "busybox must be statically linked"
fi
for applet in \
    cat echo grep insmod ip mkdir mount mv reboot rm rmdir sh sync test \
    umount uname; do
    "$boot_busybox_source" --list | grep -Fx "$applet" >/dev/null ||
        die "busybox does not provide the $applet applet"
done

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zerofs-build-target.XXXXXX")
require_command docker
readarray -t source_fields < <(
    python3 - "$source_json" <<'PY'
import json
import sys

value = json.loads(sys.argv[1])
identity = value.get("identity")
snapshot = value.get("snapshot")
artifacts = value.get("artifacts", {})
if not isinstance(identity, str) or not identity:
    raise SystemExit("source identity is missing")
if not isinstance(snapshot, str) or not snapshot:
    raise SystemExit("source snapshot is missing")
if not isinstance(artifacts, dict):
    raise SystemExit("source artifacts must be an object")
print(identity)
print(snapshot)
print(json.dumps(artifacts, separators=(",", ":"), sort_keys=True))
PY
)
[[ ${#source_fields[@]} -eq 3 ]] ||
    die "target source identity is incomplete"
source_identity=${source_fields[0]}
snapshot=${source_fields[1]}
source_artifacts=${source_fields[2]}

container_output=$work_dir/container-output
mkdir -p -- "$container_output"
container_output=$(realpath -e -- "$container_output")
docker run --rm \
    --platform "$docker_platform" \
    --env "ZEROFS_SOURCE_ARTIFACTS=$source_artifacts" \
    --mount "type=bind,src=$repo_root,dst=/zerofs-tools,readonly" \
    --mount "type=bind,src=$source_package,dst=/zerofs-source-package.$family,readonly" \
    --mount "type=bind,src=$container_output,dst=/zerofs-out" \
    "$builder_image" \
    /zerofs-tools/packaging/kernel/build-module-container.sh \
    "$distro" \
    "$release" \
    "$kernel_release" \
    "$kernel_package_version" \
    "$source_identity" \
    "$snapshot" \
    "$apt_suite"

unsafe_output=$(find -P "$container_output" -mindepth 1 \
    \( -type l -o ! \( -type f -o -type d \) \) -print -quit)
[[ -z "$unsafe_output" ]] ||
    die "target builder produced an unsafe output path: $unsafe_output"

module=$container_output/module/zerofs.ko
kernel_image=$container_output/vmlinuz
dependency_source_dir=$container_output/module-dependencies
boot_module_source_dir=$container_output/boot-modules
require_container_path "$module" file
require_container_path "$kernel_image" file
require_container_module_directory "$dependency_source_dir"
require_container_module_directory "$boot_module_source_dir"
validate_module "$module" zerofs
[[ -z $(modinfo -F sig_id "$module") ]] ||
    die "target builder produced an already signed zerofs module"
install -m 0644 "$module" "$output_dir/zerofs.ko"

dependencies_dir=$output_dir/modules
publish_module_set \
    "$dependency_source_dir" \
    "$dependencies_dir" \
    modules \
    zerofs

boot_modules_dir=$output_dir/boot-modules
publish_module_set \
    "$boot_module_source_dir" \
    "$boot_modules_dir" \
    boot-modules \
    ""

install -m 0755 "$boot_busybox_source" "$output_dir/busybox"
install -m 0644 "$kernel_image" "$output_dir/kernel"
