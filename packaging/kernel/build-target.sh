#!/usr/bin/env bash

set -euo pipefail

readonly script_name=${0##*/}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd -P)
readonly repo_root
readonly catalog_helper="$script_dir/kernel-targets.py"

manifest=
target_id=
output_dir=
module=
source_root=$repo_root
source_root_explicit=false
work_dir=

usage() {
    cat >&2 <<EOF
usage: $script_name \
  --manifest PATH \
  --target-id ID \
  --output-dir DIRECTORY \
  [--source-root PATH] \
  [--module PREBUILT-ZEROFS.KO]

Without --module, the pinned builder image acquires and builds the exact target.
--source-root selects the ZeroFS checkout to build; tooling comes from the
current checkout. It defaults to the current checkout and cannot be combined
with --module.

Optional environment:
  ZEROFS_KERNEL_PACKAGE_LICENSE   reviewed combined-module license (required)
  ZEROFS_TARGET_STRIP             explicit target strip executable
  ZEROFS_MODULE_SIGNER            trusted kmodsign-compatible signer executable
  ZEROFS_MODULE_SIGN_KEY          dedicated private-key file
  ZEROFS_MODULE_SIGN_CERT         dedicated X.509 certificate file
  ZEROFS_MODULE_SIGN_HASH         signature hash (default: sha256)
  ZEROFS_REQUIRE_CLEAN_SOURCE     set to 1 to require clean source and tooling
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

is_full_git_commit() {
    [[ $1 =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]]
}

read_git_provenance() {
    local root=$1
    local label=$2
    local commit_variable=$3
    local state_variable=$4
    local commit=unknown
    local git_root
    local status
    local tree_state=unknown

    if command -v git >/dev/null 2>&1 &&
        git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git_root=$(git -C "$root" rev-parse --show-toplevel) ||
            die "cannot resolve the $label Git checkout"
        git_root=$(realpath -e -- "$git_root") ||
            die "cannot resolve the $label Git checkout root"
        [[ "$git_root" == "$root" ]] ||
            die "$label root is not the Git checkout root: $root"
        commit=$(git -C "$root" rev-parse --verify 'HEAD^{commit}') ||
            die "cannot resolve the $label Git commit"
        is_full_git_commit "$commit" ||
            die "$label commit is not a full Git object ID"
        status=$(git -C "$root" status --porcelain --untracked-files=normal) ||
            die "cannot inspect the $label Git tree"
        if [[ -z "$status" ]]; then
            tree_state=clean
        else
            tree_state=dirty
        fi
    fi

    printf -v "$commit_variable" '%s' "$commit"
    printf -v "$state_variable" '%s' "$tree_state"
}

recheck_git_provenance() {
    local root=$1
    local label=$2
    local expected_commit=$3
    local expected_tree_state=$4
    local current_commit
    local current_tree_state

    read_git_provenance \
        "$root" "$label" current_commit current_tree_state
    [[ "$current_commit" == "$expected_commit" &&
       "$current_tree_state" == "$expected_tree_state" ]] ||
        die "$label Git provenance changed during the target build"
}

require_safe_output_directory() {
    local root=$1
    local label=$2
    local relative

    command -v git >/dev/null 2>&1 ||
        return
    git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1 ||
        return
    case $output_dir in
        "$root")
            die "--output-dir cannot be the $label checkout root"
            ;;
        "$root"/*)
            relative=${output_dir#"$root"/}
            git -C "$root" check-ignore -q -- "$relative" ||
                die "--output-dir inside the $label checkout must be ignored by Git"
            ;;
    esac
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

copy_uncompressed_module() {
    local source=$1
    local destination=$2

    case $source in
        *.ko)
            cp -- "$source" "$destination"
            ;;
        *.ko.gz)
            require_command gzip
            gzip -d -c "$source" >"$destination"
            ;;
        *.ko.xz)
            require_command xz
            xz -d -c "$source" >"$destination"
            ;;
        *.ko.zst)
            require_command zstd
            zstd -q -d -c "$source" >"$destination"
            ;;
        *)
            die "unsupported module compression: $source"
            ;;
    esac
    chmod 0644 -- "$destination"
}

collect_module_plan() {
    local module_root=$1
    local output_directory=$2
    local excluded_module=$3
    shift 3

    local entry
    local module_name
    local module_output
    local module_plan_text
    local module_source
    local output_index=0
    local path
    local requested_module
    local directive
    local -a module_plan
    local -A seen_modules=()

    mkdir -p -- "$output_directory"
    for requested_module in "$@"; do
        module_plan_text=$(
            modprobe \
                -d "$module_root" \
                -S "$kernel_release" \
                --show-depends \
                "$requested_module"
        ) || die "modprobe could not resolve $requested_module"
        [[ -n "$module_plan_text" ]] ||
            die "modprobe returned an empty plan for $requested_module"
        mapfile -t module_plan <<<"$module_plan_text"

        for entry in "${module_plan[@]}"; do
            read -r directive path _ <<<"$entry"
            [[ "$directive" == builtin ]] && continue
            [[ "$directive" == insmod ]] ||
                die "unsupported modprobe directive: $entry"
            case $path in
                "$module_root"/*)
                    module_source=$path
                    ;;
                /lib/modules/*)
                    module_source=$module_root$path
                    ;;
                *)
                    die "modprobe returned an unsafe module path: $path"
                    ;;
            esac
            [[ -f "$module_source" ]] ||
                die "module does not exist: $module_source"

            module_name=$(modinfo -F name "$module_source")
            if [[ -n "$excluded_module" &&
                  "$module_name" == "$excluded_module" ]]; then
                continue
            fi
            [[ -z ${seen_modules[$module_name]:-} ]] || continue
            seen_modules[$module_name]=1

            printf -v module_output \
                '%s/%04d.ko' \
                "$output_directory" "$output_index"
            copy_uncompressed_module "$module_source" "$module_output"
            [[ "$(modinfo -F name "$module_output")" == "$module_name" ]] ||
                die "decompressed module metadata changed: $module_name"
            case $(modinfo -F vermagic "$module_output") in
                "$kernel_release" | "$kernel_release "*) ;;
                *) die "$module_name does not target $kernel_release" ;;
            esac
            ((output_index += 1))
        done
    done
}

publish_module_set() {
    local source_directory=$1
    local output_directory=$2
    local relative_directory=$3
    local manifest_file=$4
    local forbidden_module=$5
    local module_name
    local source
    local destination
    local -a sources
    local -A seen_modules=()

    mkdir -p -- "$output_directory"
    : >"$manifest_file"
    shopt -s nullglob
    sources=("$source_directory"/*.ko)
    shopt -u nullglob
    for source in "${sources[@]}"; do
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
        case $(modinfo -F vermagic "$source") in
            "$kernel_release" | "$kernel_release "*) ;;
            *) die "$module_name does not target $kernel_release" ;;
        esac

        destination=$output_directory/${source##*/}
        [[ ! -e "$destination" ]] ||
            die "duplicate module output: ${destination##*/}"
        install -m 0644 "$source" "$destination"
        printf '%s/%s\n' \
            "$relative_directory" "${destination##*/}" >>"$manifest_file"
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
        --module)
            require_value "$1" "${2:-}"
            module=$2
            shift 2
            ;;
        --source-root)
            require_value "$1" "${2:-}"
            source_root=$2
            source_root_explicit=true
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
if [[ "$source_root_explicit" == true && -n "$module" ]]; then
    die "--source-root cannot be used with --module"
fi
require_command busybox
require_command find
require_command modinfo
require_command python3
require_command readelf
require_command realpath
require_command sha256sum

[[ -x "$catalog_helper" ]] ||
    die "target catalog helper is missing: $catalog_helper"
manifest=$(realpath "$manifest")
[[ -f "$manifest" ]] || die "manifest is not a regular file: $manifest"
source_root_input=$source_root
source_root=$(realpath -e -- "$source_root" 2>/dev/null) ||
    die "source root does not exist: $source_root_input"
[[ -d "$source_root" ]] || die "source root is not a directory: $source_root"
[[ -f "$source_root/zerofs/Cargo.toml" &&
   ! -L "$source_root/zerofs/Cargo.toml" ]] ||
    die "source root has no regular zerofs/Cargo.toml: $source_root"

mkdir -p -- "$output_dir"
output_dir=$(realpath "$output_dir")
[[ ! -e "$output_dir/artifact.json" ]] ||
    die "refusing to overwrite $output_dir/artifact.json"
require_safe_output_directory "$source_root" source
require_safe_output_directory "$repo_root" tooling

target_field() {
    "$catalog_helper" --manifest "$manifest" field "$target_id" "$1"
}

enabled=$(target_field enabled)
family=$(target_field family)
arch=$(target_field arch)
kernel_release=$(target_field kernel_release)
kernel_dependency=$(target_field kernel_dependency)
kernel_upgrade_conflict=$(target_field kernel_upgrade_conflict)
distro=$(target_field distro)
release=$(target_field release)
kernel_package_version=$(target_field kernel_package_version)
kernel_selector_version=$(target_field kernel_selector_version)
channel_id=$(target_field channel_id)
package_revision=$(target_field package_revision)
builder_image=$(target_field builder_image)
source_json=$(target_field source)

[[ "$enabled" == true ]] || die "target is not enabled: $target_id"
[[ "$kernel_upgrade_conflict" != null ]] ||
    kernel_upgrade_conflict=
case $arch in
    x86_64)
        docker_platform=linux/amd64
        elf_machine='Advanced Micro Devices X86-64'
        boot_transport=virtio_pci
        ;;
    aarch64)
        docker_platform=linux/arm64
        elf_machine=AArch64
        boot_transport=virtio_mmio
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
boot_busybox_banner=$("$boot_busybox_source" 2>&1 | sed -n '1p')
[[ -n "$boot_busybox_banner" ]] ||
    die "busybox did not report a version banner"
boot_busybox_identity=unmanaged:$boot_busybox_source
if command -v dpkg-query >/dev/null 2>&1; then
    if busybox_owner=$(dpkg-query -S "$boot_busybox_source" 2>/dev/null); then
        busybox_owner=${busybox_owner%%$'\n'*}
        busybox_package=${busybox_owner%%: *}
        if busybox_version=$(
            dpkg-query -W -f='${Version}' "$busybox_package" 2>/dev/null
        ) && [[ -n "$busybox_version" ]]; then
            boot_busybox_identity=deb:$busybox_package=$busybox_version
        fi
    fi
elif command -v rpm >/dev/null 2>&1; then
    if busybox_package=$(
        rpm -qf \
            --qf 'rpm:%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}' \
            "$boot_busybox_source" 2>/dev/null
    ) && [[ -n "$busybox_package" ]]; then
        boot_busybox_identity=$busybox_package
    fi
fi
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

package_license=${ZEROFS_KERNEL_PACKAGE_LICENSE:-}
[[ -n "$package_license" ]] ||
    die "ZEROFS_KERNEL_PACKAGE_LICENSE must name the reviewed kernel payload license"

zerofs_version=$(python3 - "$source_root/zerofs/Cargo.toml" <<'PY'
import sys
import tomllib
from pathlib import Path

path = Path(sys.argv[1])
with path.open("rb") as stream:
    value = tomllib.load(stream)
version = value.get("workspace", {}).get("package", {}).get("version")
if not isinstance(version, str) or not version:
    raise SystemExit(f"{path}: workspace.package.version is missing")
print(version)
PY
)

read_git_provenance \
    "$source_root" "source" source_commit source_tree_state
read_git_provenance \
    "$repo_root" "tooling" tooling_commit tooling_tree_state
case ${ZEROFS_REQUIRE_CLEAN_SOURCE:-0} in
    0) ;;
    1)
        [[ "$source_tree_state" == clean ]] &&
            is_full_git_commit "$source_commit" ||
            die "release packaging requires a clean Git source tree"
        [[ "$tooling_tree_state" == clean ]] &&
            is_full_git_commit "$tooling_commit" ||
            die "release packaging requires a clean Git tooling tree"
        ;;
    *)
        die "ZEROFS_REQUIRE_CLEAN_SOURCE must be 0 or 1"
        ;;
esac

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zerofs-build-target.XXXXXX")
acquired=false
dependency_source_dir=
boot_module_source_dir=
kernel_config_source=
module_symvers_source=
build_info_source=
if [[ -z "$module" ]]; then
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
        --env "ZEROFS_KERNEL_TARGET_ID=$target_id" \
        --env "ZEROFS_TARGET_ARCH=$arch" \
        --env "ZEROFS_SOURCE_ARTIFACTS=$source_artifacts" \
        --env "ZEROFS_HOST_UID=$(id -u)" \
        --env "ZEROFS_HOST_GID=$(id -g)" \
        --mount "type=bind,src=$repo_root,dst=/zerofs-tools,readonly" \
        --mount "type=bind,src=$source_root,dst=/zerofs-source,readonly" \
        --mount "type=bind,src=$container_output,dst=/zerofs-out" \
        "$builder_image" \
        /zerofs-tools/packaging/kernel/build-module-container.sh \
        "$distro" \
        "$release" \
        "$kernel_release" \
        "$kernel_package_version" \
        "$source_identity" \
        "$snapshot" \
        "$zerofs_version"

    unsafe_output=$(find -P "$container_output" -mindepth 1 \
        \( -type l -o ! \( -type f -o -type d \) \) -print -quit)
    [[ -z "$unsafe_output" ]] ||
        die "target builder produced an unsafe output path: $unsafe_output"

    module=$container_output/module/zerofs.ko
    kernel_image=$container_output/vmlinuz
    dependency_source_dir=$container_output/module-dependencies
    boot_module_source_dir=$container_output/boot-modules
    kernel_config_source=$container_output/kernel.config
    module_symvers_source=$container_output/Module.symvers
    build_info_source=$container_output/build-info
    require_container_path "$module" file
    require_container_path "$kernel_image" file
    require_container_module_directory "$dependency_source_dir"
    require_container_module_directory "$boot_module_source_dir"
    require_container_path "$kernel_config_source" file
    require_container_path "$module_symvers_source" file
    require_container_path "$build_info_source" file
    acquired=true
else
    require_command depmod
    require_command modprobe
    module=$(realpath "$module")
    [[ -f "$module" ]] || die "module is not a regular file: $module"

    kernel_tree=/lib/modules/$kernel_release
    kernel_image=/boot/vmlinuz-$kernel_release
    [[ -d "$kernel_tree" ]] ||
        die "exact target module tree is unavailable: $kernel_tree"
    [[ -f "$kernel_image" ]] ||
        die "exact target kernel image is unavailable: $kernel_image"
    kernel_config_source=$kernel_tree/build/.config
    module_symvers_source=$kernel_tree/build/Module.symvers
    [[ -f "$kernel_config_source" ]] ||
        die "exact target kernel configuration is unavailable: $kernel_config_source"
    [[ -f "$module_symvers_source" ]] ||
        die "exact target symbol versions are unavailable: $module_symvers_source"
fi

recheck_git_provenance \
    "$source_root" "source" "$source_commit" "$source_tree_state"
recheck_git_provenance \
    "$repo_root" "tooling" "$tooling_commit" "$tooling_tree_state"

prepared_module=$output_dir/zerofs.ko

prepare_arguments=(
    --input "$module"
    --output "$prepared_module"
    --kernel-release "$kernel_release"
    --arch "$arch"
)
if [[ -n ${ZEROFS_TARGET_STRIP:-} ]]; then
    prepare_arguments+=(--strip-tool "$ZEROFS_TARGET_STRIP")
fi

signer=${ZEROFS_MODULE_SIGNER:-}
sign_key=${ZEROFS_MODULE_SIGN_KEY:-}
sign_cert=${ZEROFS_MODULE_SIGN_CERT:-}
sign_hash=${ZEROFS_MODULE_SIGN_HASH:-sha256}
signing_cert_der=
temporary_cert=
if [[ -n "$sign_key" || -n "$sign_cert" || -n "$signer" ]]; then
    [[ -n "$signer" && -n "$sign_key" && -n "$sign_cert" ]] ||
        die "ZEROFS_MODULE_SIGNER, ZEROFS_MODULE_SIGN_KEY, and ZEROFS_MODULE_SIGN_CERT must be supplied together"
    require_command openssl
    [[ -x "$signer" ]] ||
        die "ZEROFS_MODULE_SIGNER is not executable: $signer"
    temporary_cert=$work_dir/zerofs-module-signing-cert.der
    if ! openssl x509 \
        -in "$sign_cert" \
        -outform DER \
        -out "$temporary_cert" 2>/dev/null; then
        openssl x509 \
            -inform DER \
            -in "$sign_cert" \
            -outform DER \
            -out "$temporary_cert"
    fi
    openssl x509 -inform DER -in "$temporary_cert" -noout
    openssl x509 \
        -inform DER \
        -in "$temporary_cert" \
        -noout \
        -ext extendedKeyUsage |
        grep -Fq 'Code Signing' ||
        die "module-signing certificate lacks the codeSigning extended key usage"
    prepare_arguments+=(
        --signer "$signer"
        --sign-key "$sign_key"
        --sign-cert "$temporary_cert"
        --sign-hash "$sign_hash"
    )
elif [[ -n ${ZEROFS_MODULE_SIGN_HASH:-} ]]; then
    die "ZEROFS_MODULE_SIGN_HASH requires signer, key, and certificate"
fi

"$script_dir/prepare-module.sh" "${prepare_arguments[@]}" >/dev/null

build_arguments=(
    --module "$prepared_module"
    --kernel-package-dependency "$kernel_dependency"
    --family "$family"
    --kernel-release "$kernel_release"
    --target-id "$target_id"
    --channel-id "$channel_id"
    --arch "$arch"
    --version "$zerofs_version"
    --revision "$package_revision"
    --source-commit "$source_commit"
    --source-tree-state "$source_tree_state"
    --tooling-commit "$tooling_commit"
    --tooling-tree-state "$tooling_tree_state"
    --license "$package_license"
    --output-dir "$output_dir"
)

if [[ -n "$signer" ]]; then
    signing_cert_der=$output_dir/zerofs-module-signing-cert.der
    [[ ! -e "$signing_cert_der" ]] ||
        die "refusing to overwrite $signing_cert_der"
    install -m 0644 "$temporary_cert" "$signing_cert_der"
    build_arguments+=(--signing-cert-der "$signing_cert_der")
fi

if [[ -n "$kernel_upgrade_conflict" ]]; then
    build_arguments+=(--kernel-upgrade-conflict "$kernel_upgrade_conflict")
fi
package_log=$("$script_dir/build.sh" "${build_arguments[@]}")
mapfile -t package_lines <<<"$package_log"
[[ ${#package_lines[@]} -eq 2 ]] ||
    die "package builder did not return exactly a payload and selector package"
payload_package=${package_lines[-2]}
selector_package=${package_lines[-1]}
[[ -f "$payload_package" && -f "$selector_package" ]] ||
    die "package builder did not create both packages"

if [[ "$acquired" != true ]]; then
    module_root=$work_dir/root
    mkdir -p -- "$module_root/lib/modules"
    cp -a --reflink=auto "$kernel_tree" \
        "$module_root/lib/modules/$kernel_release"
    while IFS= read -r existing; do
        rm -f -- "$existing"
    done < <(
        find "$module_root/lib/modules/$kernel_release" -type f \
            \( -name 'zerofs.ko' -o -name 'zerofs.ko.gz' \
            -o -name 'zerofs.ko.xz' -o -name 'zerofs.ko.zst' \) -print
    )
    install -d -m 0755 \
        "$module_root/lib/modules/$kernel_release/updates/zerofs"
    install -m 0644 "$prepared_module" \
        "$module_root/lib/modules/$kernel_release/updates/zerofs/zerofs.ko"
    depmod -b "$module_root" "$kernel_release"

    dependency_source_dir=$work_dir/module-dependencies
    boot_module_source_dir=$work_dir/boot-modules
    collect_module_plan \
        "$module_root" "$dependency_source_dir" zerofs zerofs
    collect_module_plan \
        "$module_root" "$boot_module_source_dir" "" \
        "$boot_transport" virtio_net
fi

dependencies_dir=$output_dir/modules
dependencies_file=$work_dir/module-dependencies.list
publish_module_set \
    "$dependency_source_dir" \
    "$dependencies_dir" \
    modules \
    "$dependencies_file" \
    zerofs

boot_modules_dir=$output_dir/boot-modules
boot_modules_file=$work_dir/boot-modules.list
publish_module_set \
    "$boot_module_source_dir" \
    "$boot_modules_dir" \
    boot-modules \
    "$boot_modules_file" \
    ""

boot_busybox_output=$output_dir/boot-busybox
[[ ! -e "$boot_busybox_output" ]] ||
    die "refusing to overwrite $boot_busybox_output"
install -m 0755 "$boot_busybox_source" "$boot_busybox_output"

kernel_image_output=$output_dir/vmlinuz-$kernel_release
[[ ! -e "$kernel_image_output" ]] ||
    die "refusing to overwrite $kernel_image_output"
cp "$kernel_image" "$kernel_image_output"
chmod 0644 "$kernel_image_output"

kernel_config_output=$output_dir/kernel.config
module_symvers_output=$output_dir/Module.symvers
build_info_output=$output_dir/build-info
for metadata_output in \
    "$kernel_config_output" \
    "$module_symvers_output" \
    "$build_info_output"; do
    [[ ! -e "$metadata_output" ]] ||
        die "refusing to overwrite $metadata_output"
done
install -m 0644 "$kernel_config_source" "$kernel_config_output"
install -m 0644 "$module_symvers_source" "$module_symvers_output"
if [[ "$acquired" == true ]]; then
    install -m 0644 "$build_info_source" "$build_info_output"
else
    {
        printf 'target_id=%s\n' "$target_id"
        printf 'source_identity=local-matching-host\n'
        printf 'builder_os=local-matching-host\n'
        printf 'build_kind=prebuilt-module\n'
    } >"$build_info_output"
    chmod 0644 "$build_info_output"
fi

export ZEROFS_ARTIFACT_TARGET_ID=$target_id
export ZEROFS_ARTIFACT_KERNEL_RELEASE=$kernel_release
export ZEROFS_ARTIFACT_KERNEL_PACKAGE_VERSION=$kernel_package_version
export ZEROFS_ARTIFACT_KERNEL_SELECTOR_VERSION=$kernel_selector_version
export ZEROFS_ARTIFACT_CHANNEL_ID=$channel_id
export ZEROFS_ARTIFACT_PACKAGE_REVISION=$package_revision
export ZEROFS_ARTIFACT_ZEROFS_VERSION=$zerofs_version
export ZEROFS_ARTIFACT_SOURCE_COMMIT=$source_commit
export ZEROFS_ARTIFACT_SOURCE_TREE_STATE=$source_tree_state
export ZEROFS_ARTIFACT_TOOLING_COMMIT=$tooling_commit
export ZEROFS_ARTIFACT_TOOLING_TREE_STATE=$tooling_tree_state
export ZEROFS_ARTIFACT_FAMILY=$family
export ZEROFS_ARTIFACT_ARCH=$arch
export ZEROFS_ARTIFACT_BUILDER_IMAGE=$builder_image
export ZEROFS_ARTIFACT_SOURCE=$source_json
export ZEROFS_ARTIFACT_MODULE=${prepared_module##*/}
export ZEROFS_ARTIFACT_PAYLOAD=${payload_package##*/}
export ZEROFS_ARTIFACT_SELECTOR=${selector_package##*/}
export ZEROFS_ARTIFACT_KERNEL_IMAGE=${kernel_image_output##*/}
export ZEROFS_ARTIFACT_KERNEL_CONFIG=${kernel_config_output##*/}
export ZEROFS_ARTIFACT_MODULE_SYMVERS=${module_symvers_output##*/}
export ZEROFS_ARTIFACT_BUILD_INFO=${build_info_output##*/}
export ZEROFS_ARTIFACT_BOOT_BUSYBOX=${boot_busybox_output##*/}
export ZEROFS_ARTIFACT_BOOT_BUSYBOX_BANNER=$boot_busybox_banner
export ZEROFS_ARTIFACT_BOOT_BUSYBOX_IDENTITY=$boot_busybox_identity
export ZEROFS_ARTIFACT_SIGNING_CERTIFICATE=$signing_cert_der
export ZEROFS_ARTIFACT_DEPENDENCIES_FILE=$dependencies_file
export ZEROFS_ARTIFACT_BOOT_MODULES_FILE=$boot_modules_file
export ZEROFS_ARTIFACT_OUTPUT=$output_dir/artifact.json
python3 - <<'PY'
import hashlib
import json
import os
import subprocess
from pathlib import Path

dependencies = Path(
    os.environ["ZEROFS_ARTIFACT_DEPENDENCIES_FILE"]
).read_text(encoding="utf-8").splitlines()
boot_modules = Path(
    os.environ["ZEROFS_ARTIFACT_BOOT_MODULES_FILE"]
).read_text(encoding="utf-8").splitlines()
artifact = {
    "schema_version": 2,
    "target_id": os.environ["ZEROFS_ARTIFACT_TARGET_ID"],
    "kernel_release": os.environ["ZEROFS_ARTIFACT_KERNEL_RELEASE"],
    "kernel_package_version": os.environ[
        "ZEROFS_ARTIFACT_KERNEL_PACKAGE_VERSION"
    ],
    "kernel_selector_version": os.environ[
        "ZEROFS_ARTIFACT_KERNEL_SELECTOR_VERSION"
    ],
    "channel_id": os.environ["ZEROFS_ARTIFACT_CHANNEL_ID"],
    "package_revision": os.environ["ZEROFS_ARTIFACT_PACKAGE_REVISION"],
    "zerofs_version": os.environ["ZEROFS_ARTIFACT_ZEROFS_VERSION"],
    "source_commit": os.environ["ZEROFS_ARTIFACT_SOURCE_COMMIT"],
    "source_tree_state": os.environ["ZEROFS_ARTIFACT_SOURCE_TREE_STATE"],
    "tooling_commit": os.environ["ZEROFS_ARTIFACT_TOOLING_COMMIT"],
    "tooling_tree_state": os.environ["ZEROFS_ARTIFACT_TOOLING_TREE_STATE"],
    "family": os.environ["ZEROFS_ARTIFACT_FAMILY"],
    "arch": os.environ["ZEROFS_ARTIFACT_ARCH"],
    "builder_image": os.environ["ZEROFS_ARTIFACT_BUILDER_IMAGE"],
    "source": json.loads(os.environ["ZEROFS_ARTIFACT_SOURCE"]),
    "module": os.environ["ZEROFS_ARTIFACT_MODULE"],
    "payload_package": os.environ["ZEROFS_ARTIFACT_PAYLOAD"],
    "selector_package": os.environ["ZEROFS_ARTIFACT_SELECTOR"],
    "kernel_image": os.environ["ZEROFS_ARTIFACT_KERNEL_IMAGE"],
    "kernel_config": os.environ["ZEROFS_ARTIFACT_KERNEL_CONFIG"],
    "module_symvers": os.environ["ZEROFS_ARTIFACT_MODULE_SYMVERS"],
    "build_info": os.environ["ZEROFS_ARTIFACT_BUILD_INFO"],
    "boot_busybox": os.environ["ZEROFS_ARTIFACT_BOOT_BUSYBOX"],
    "boot_busybox_provenance": {
        "identity": os.environ["ZEROFS_ARTIFACT_BOOT_BUSYBOX_IDENTITY"],
        "banner": os.environ["ZEROFS_ARTIFACT_BOOT_BUSYBOX_BANNER"],
    },
    "module_dependencies": dependencies,
    "boot_modules": boot_modules,
}
certificate = os.environ["ZEROFS_ARTIFACT_SIGNING_CERTIFICATE"]
if certificate:
    certificate_path = Path(certificate)
    certificate_sha256 = hashlib.sha256(certificate_path.read_bytes()).hexdigest()
    module_path = Path(os.environ["ZEROFS_ARTIFACT_OUTPUT"]).parent / artifact["module"]
    def modinfo(field):
        return subprocess.run(
            ["modinfo", "-F", field, module_path],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.rstrip("\n")

    artifact["module_signing"] = {
        "certificate": certificate_path.name,
        "certificate_sha256": certificate_sha256,
        "signature_id": modinfo("sig_id"),
        "signer": modinfo("signer"),
        "key": modinfo("sig_key"),
        "hash_algorithm": modinfo("sig_hashalgo"),
    }
else:
    artifact["module_signing"] = None
output = Path(os.environ["ZEROFS_ARTIFACT_OUTPUT"])
base = output.parent
artifact_paths = [
    artifact["module"],
    artifact["payload_package"],
    artifact["selector_package"],
    artifact["kernel_image"],
    artifact["kernel_config"],
    artifact["module_symvers"],
    artifact["build_info"],
    artifact["boot_busybox"],
    *dependencies,
    *boot_modules,
]
if artifact["module_signing"] is not None:
    artifact_paths.append(artifact["module_signing"]["certificate"])
artifact["sha256"] = {
    relative: hashlib.sha256((base / relative).read_bytes()).hexdigest()
    for relative in artifact_paths
}
temporary = output.with_name(f".{output.name}.tmp")
temporary.write_text(
    json.dumps(artifact, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
temporary.chmod(0o644)
temporary.replace(output)
PY

recheck_git_provenance \
    "$source_root" "source" "$source_commit" "$source_tree_state"
recheck_git_provenance \
    "$repo_root" "tooling" "$tooling_commit" "$tooling_tree_state"

printf '%s\n' "$output_dir/artifact.json"
