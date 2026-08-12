#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir

die() {
    printf '%s: %s\n' "$script_name" "$*" >&2
    exit 1
}

fallback_unavailable() {
    printf '%s: source fallback unavailable: %s\n' \
        "$script_name" "$*" >&2
    # An eligible kernel must not finish its DKMS transaction without a
    # module. Keep 77 for DKMS's version-floor exclusion; missing publication
    # and fallback inputs are a hard failure that the distro hook can surface.
    exit 1
}

usage() {
    printf 'usage: %s KERNEL_RELEASE PACKAGE_VERSION\n' "$script_name" >&2
    exit 2
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
    local key=$1
    local value

    value=$(sed -n "s/^${key}=//p" "$auto_conf")
    if [[ "$value" == \"*\" ]]; then
        value=${value#\"}
        value=${value%\"}
    fi
    printf '%s\n' "$value"
}

[[ $# -eq 2 ]] || usage
kernel_release=$1
package_version=$2
[[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~:-]*$ ]] ||
    die "unsafe kernel release: $kernel_release"
[[ "$package_version" =~ ^[A-Za-z0-9][A-Za-z0-9._+~:-]*$ ]] ||
    die "unsafe package version: $package_version"

module_source="$script_dir/kernel"
kernel_build_link="/lib/modules/$kernel_release/build"
[[ -d "$kernel_build_link" ]] ||
    die "kernel headers are not installed for $kernel_release"
kernel_build=$(realpath -e -- "$kernel_build_link")
auto_conf="$kernel_build/include/config/auto.conf"
kernel_release_file="$kernel_build/include/config/kernel.release"
[[ -s "$kernel_release_file" ]] ||
    die "kernel release file is missing: $kernel_release_file"
[[ "$(<"$kernel_release_file")" == "$kernel_release" ]] ||
    die "kernel headers do not describe $kernel_release"

module_output="$script_dir/dkms-output"
support_output="$script_dir/dkms-support/$kernel_release"
source_output="$script_dir/dkms-kernel-source/$kernel_release"
metadata_output="$script_dir/dkms-metadata/$kernel_release"
build_complete=false

cleanup_incomplete_module() {
    local status=$?

    if [[ $status -ne 0 || $build_complete != true ]]; then
        rm -f -- "$module_output/zerofs.ko"
    fi
    exit "$status"
}

trap cleanup_incomplete_module EXIT

# Prefer the exact module already built and boot-tested by ZeroFS CI.  A
# missing or unreachable object is a cache miss and falls through to the
# source build below.  A downloaded object that fails authentication or ABI
# validation is fatal: silently compiling in that case would hide a broken or
# tampered publication.
install -d -m 0755 "$module_output"
rm -f -- "$module_output/zerofs.ko"
if [[ ${ZEROFS_DISABLE_PREBUILT:-0} != 1 ]]; then
    fetch_status=0
    "$script_dir/dkms-fetch-module" \
        "$kernel_release" "$package_version" \
        "$module_output/zerofs.ko" || fetch_status=$?
    case $fetch_status in
        0)
            build_complete=true
            exit 0
            ;;
        75) ;;
        *) exit "$fetch_status" ;;
    esac
fi

[[ -s "$auto_conf" ]] ||
    fallback_unavailable "kernel configuration is missing: $auto_conf"
for command_name in make find tar; do
    command -v "$command_name" >/dev/null 2>&1 ||
        fallback_unavailable "$command_name is not installed"
done

[[ -s "$kernel_build/Module.symvers" ]] ||
    fallback_unavailable \
        "kernel symbol versions are missing: $kernel_build/Module.symvers"
grep -qx 'CONFIG_MODULES=y' "$auto_conf" ||
    die "target kernel does not enable loadable modules"

rustc_text=$(config_value CONFIG_RUSTC_VERSION_TEXT)
rustc_release=${rustc_text#rustc }
rustc_release=${rustc_release%% *}
rustc_series=${rustc_release%.*}
[[ "$rustc_series" != "$rustc_release" ]] || rustc_series=
rustc=$(select_command \
    "${ZEROFS_RUSTC:-}" \
    "rustc-$rustc_series" \
    "/usr/bin/rustc-$rustc_series" \
    /usr/bin/rustc \
    rustc) || fallback_unavailable \
        "the target kernel's Rust compiler is not installed"

bindgen_text=$(config_value CONFIG_BINDGEN_VERSION_TEXT)
bindgen_release=${bindgen_text#bindgen }
bindgen_release=${bindgen_release%% *}
bindgen_series=${bindgen_release%.*}
[[ "$bindgen_series" != "$bindgen_release" ]] || bindgen_series=
bindgen=$(select_command \
    "${ZEROFS_BINDGEN:-}" \
    "bindgen-$bindgen_series" \
    "/usr/bin/bindgen-$bindgen_series" \
    /usr/bin/bindgen \
    bindgen) || fallback_unavailable "bindgen is not installed"

if [[ -n "$rustc_text" && "$("$rustc" --version)" != "$rustc_text" ]]; then
    fallback_unavailable \
        "installed rustc does not match target: $("$rustc" --version)"
fi
if [[ -n "$bindgen_text" && "$("$bindgen" --version)" != "$bindgen_text" ]]; then
    fallback_unavailable \
        "installed bindgen does not match target: $("$bindgen" --version)"
fi

rustfmt=$(select_command \
    "${ZEROFS_RUSTFMT:-}" \
    "rustfmt-$rustc_series" \
    "/usr/bin/rustfmt-$rustc_series" \
    "/usr/lib/rust-$rustc_series/bin/rustfmt" \
    /usr/bin/rustfmt \
    rustfmt) || fallback_unavailable "rustfmt is not installed"

target_cc_text=$(config_value CONFIG_CC_VERSION_TEXT)
target_cc_name=${target_cc_text%% *}
target_cc=$(select_command "${ZEROFS_TARGET_CC:-}" "$target_cc_name" || true)
if [[ -z "$target_cc" ]] && grep -qx 'CONFIG_CC_IS_CLANG=y' "$auto_conf"; then
    target_cc=$(select_command clang cc || true)
elif [[ -z "$target_cc" ]] && grep -qx 'CONFIG_CC_IS_GCC=y' "$auto_conf"; then
    target_cc=$(select_command gcc cc || true)
fi
[[ -n "$target_cc" ]] || fallback_unavailable \
    "the target kernel's C compiler is not installed"

jobs=${ZEROFS_BUILD_JOBS:-}
if [[ -z "$jobs" ]]; then
    jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1\n')
fi
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "ZEROFS_BUILD_JOBS must be positive"

kernel_source=
kernel_source_provenance=

find_kernel_source() {
    local record
    local status=0

    record=$("$script_dir/dkms-find-kernel-source" \
        "$kernel_release" "$kernel_build" "$source_output") || status=$?
    case $status in
        0) ;;
        75) fallback_unavailable "exact kernel source is unavailable" ;;
        *) die "kernel source resolver failed with status $status" ;;
    esac
    IFS=$'\t' read -r kernel_source kernel_source_provenance <<<"$record"
    [[ -n "$kernel_source" && -n "$kernel_source_provenance" ]] ||
        die "kernel source resolver returned an invalid record"
}

build_module() {
    make -j "$jobs" -C "$module_source" \
        KDIR="$kernel_build" \
        MO="$module_output" \
        CC="$target_cc" \
        RUSTC="$rustc" \
        RUSTFMT="$rustfmt" \
        BINDGEN="$bindgen" \
        modules
}

build_rust_metadata() {
    local dangling_link
    local syscall_reference=arch/x86/entry/syscalls/syscall_32.tbl
    local -a metadata_tar_excludes=(
        '--exclude=./rust'
        '--exclude=./source'
    )
    local -a metadata_make_arguments=()

    find_kernel_source
    if grep -qx 'CONFIG_ARM64=y' "$auto_conf" &&
       grep -qx 'CONFIG_COMPAT_VDSO=y' "$auto_conf"; then
        # rust/kernel.o needs the target configuration and generated headers,
        # not a rebuilt 32-bit userspace vDSO. Keep rustc_cfg from the copied
        # headers intact while suppressing that unrelated prepare side target.
        metadata_make_arguments+=(CONFIG_COMPAT_VDSO=)
    fi
    install -d -m 0755 "$metadata_output"
    find "$metadata_output" -mindepth 1 -delete
    # Dereference the prepared headers into a private output tree. Relative
    # distro-header symlinks would otherwise escape this tree and let Kbuild
    # write generated Rust files into package-owned /usr/src directories.
    # Header packages may also contain dangling links to unpackaged tooling;
    # omit only those links because tar cannot dereference them.
    while IFS= read -r -d '' dangling_link; do
        metadata_tar_excludes+=(
            "--exclude=./${dangling_link#"$kernel_build"/}"
        )
    done < <(find -P "$kernel_build" -xtype l -print0)
    tar --create --dereference --anchored --no-wildcards \
        "${metadata_tar_excludes[@]}" \
        --directory "$kernel_build" --file - . |
        tar --extract --directory "$metadata_output" --file -
    if [[ -f "$metadata_output/scripts/checksyscalls.sh" &&
          ! -f "$metadata_output/$syscall_reference" ]]; then
        [[ ! -e "$metadata_output/$syscall_reference" &&
           ! -L "$metadata_output/$syscall_reference" ]] ||
            die "kernel syscall reference is not a regular file"
        [[ -f "$kernel_source/$syscall_reference" ]] ||
            die "kernel source is missing $syscall_reference"
        install -D -m 0644 "$kernel_source/$syscall_reference" \
            "$metadata_output/$syscall_reference"
    fi

    make -j "$jobs" -C "$kernel_source" O="$metadata_output" \
        CC="$target_cc" \
        RUSTC="$rustc" \
        RUSTFMT="$rustfmt" \
        BINDGEN="$bindgen" \
        "${metadata_make_arguments[@]}" \
        KERNELRELEASE="$kernel_release" \
        rust/kernel.o
    [[ -s "$metadata_output/rust/libkernel.rmeta" ]] ||
        die "the kernel build did not produce rust/libkernel.rmeta"
    [[ "$(<"$metadata_output/include/config/kernel.release")" == \
       "$kernel_release" ]] ||
        die "Rust metadata preparation changed the target kernel release"
    kernel_build=$metadata_output
    auto_conf="$kernel_build/include/config/auto.conf"
}

if grep -qx 'CONFIG_RUST=y' "$auto_conf"; then
    if [[ ! -s "$kernel_build/rust/libkernel.rmeta" ]]; then
        build_rust_metadata
    fi
    build_module
else
    find_kernel_source
    rust_llvm=$("$rustc" -vV |
        sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p')
    [[ -n "$rust_llvm" ]] || fallback_unavailable \
        "cannot determine rustc's LLVM version"
    clang=$(select_command \
        "${ZEROFS_CLANG:-}" "clang-$rust_llvm" "clang$rust_llvm" clang) ||
        fallback_unavailable "Clang $rust_llvm is not installed"
    llvm_link=$(select_command \
        "${ZEROFS_LLVM_LINK:-}" \
        "llvm-link-$rust_llvm" "llvm-link$rust_llvm" llvm-link) ||
        fallback_unavailable "llvm-link $rust_llvm is not installed"
    llvm_opt=$(select_command \
        "${ZEROFS_LLVM_OPT:-}" "opt-$rust_llvm" "opt$rust_llvm" opt) ||
        fallback_unavailable "opt $rust_llvm is not installed"
    llvm_nm=$(select_command \
        "${ZEROFS_LLVM_NM:-}" \
        "llvm-nm-$rust_llvm" "llvm-nm$rust_llvm" llvm-nm) ||
        fallback_unavailable "llvm-nm $rust_llvm is not installed"

    ZEROFS_KERNEL_SOURCE_PROVENANCE="$kernel_source_provenance" \
        make -C "$module_source" \
            KDIR="$kernel_build" \
            KERNEL_SRC="$kernel_source" \
            MO="$module_output" \
            SELF_CONTAINED_OUTPUT="$support_output" \
            TARGET_CC="$target_cc" \
            RUSTC="$rustc" \
            RUSTFMT="$rustfmt" \
            BINDGEN="$bindgen" \
            CLANG="$clang" \
            LLVM_LINK="$llvm_link" \
            LLVM_OPT="$llvm_opt" \
            LLVM_NM="$llvm_nm" \
            self-contained
fi

[[ -s "$module_output/zerofs.ko" ]] ||
    die "the build completed without dkms-output/zerofs.ko"
[[ $(modinfo -F name "$module_output/zerofs.ko") == zerofs ]] ||
    die "the build produced a module with the wrong name"
case $(modinfo -F vermagic "$module_output/zerofs.ko") in
    "$kernel_release" | "$kernel_release "*) ;;
    *) die "the build produced a module with the wrong vermagic" ;;
esac
build_complete=true
