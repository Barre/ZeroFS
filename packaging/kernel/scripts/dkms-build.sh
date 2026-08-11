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

usage() {
    printf 'usage: %s KERNEL_RELEASE\n' "$script_name" >&2
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

[[ $# -eq 1 ]] || usage
kernel_release=$1
[[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~:-]*$ ]] ||
    die "unsafe kernel release: $kernel_release"

module_source="$script_dir/kernel"
kernel_build_link="/lib/modules/$kernel_release/build"
[[ -d "$kernel_build_link" ]] ||
    die "kernel headers are not installed for $kernel_release"
kernel_build=$(realpath -e -- "$kernel_build_link")
auto_conf="$kernel_build/include/config/auto.conf"
kernel_release_file="$kernel_build/include/config/kernel.release"
[[ -s "$auto_conf" ]] || die "kernel configuration is missing: $auto_conf"
[[ -s "$kernel_release_file" ]] ||
    die "kernel release file is missing: $kernel_release_file"
[[ "$(<"$kernel_release_file")" == "$kernel_release" ]] ||
    die "kernel headers do not describe $kernel_release"

module_output="$script_dir/dkms-output"
support_output="$script_dir/dkms-support/$kernel_release"
source_output="$script_dir/dkms-kernel-source/$kernel_release"
metadata_output="$script_dir/dkms-metadata/$kernel_release"

[[ -s "$kernel_build/Module.symvers" ]] ||
    die "kernel symbol versions are missing: $kernel_build/Module.symvers"
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
    rustc) || die "the target kernel's Rust compiler is not installed"

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
    bindgen) || die "bindgen is not installed"

if [[ -n "$rustc_text" && "$("$rustc" --version)" != "$rustc_text" ]]; then
    die "installed rustc does not match target: $("$rustc" --version)"
fi
if [[ -n "$bindgen_text" && "$("$bindgen" --version)" != "$bindgen_text" ]]; then
    die "installed bindgen does not match target: $("$bindgen" --version)"
fi

rustfmt=$(select_command \
    "${ZEROFS_RUSTFMT:-}" \
    "rustfmt-$rustc_series" \
    "/usr/bin/rustfmt-$rustc_series" \
    "/usr/lib/rust-$rustc_series/bin/rustfmt" \
    /usr/bin/rustfmt \
    rustfmt) || die "rustfmt is not installed"

target_cc_text=$(config_value CONFIG_CC_VERSION_TEXT)
target_cc_name=${target_cc_text%% *}
target_cc=$(select_command "${ZEROFS_TARGET_CC:-}" "$target_cc_name" || true)
if [[ -z "$target_cc" ]] && grep -qx 'CONFIG_CC_IS_CLANG=y' "$auto_conf"; then
    target_cc=$(select_command clang cc || true)
elif [[ -z "$target_cc" ]] && grep -qx 'CONFIG_CC_IS_GCC=y' "$auto_conf"; then
    target_cc=$(select_command gcc cc || true)
fi
[[ -n "$target_cc" ]] || die "the target kernel's C compiler is not installed"

jobs=${ZEROFS_BUILD_JOBS:-}
if [[ -z "$jobs" ]]; then
    jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1\n')
fi
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "ZEROFS_BUILD_JOBS must be positive"

kernel_source=
kernel_source_provenance=

find_kernel_source() {
    local record

    record=$("$script_dir/dkms-find-kernel-source" \
        "$kernel_release" "$kernel_build" "$source_output")
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
    local syscall_reference=arch/x86/entry/syscalls/syscall_32.tbl
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
    tar --create --dereference \
        --exclude='./rust' --exclude='./source' \
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

install -d -m 0755 "$module_output"
if grep -qx 'CONFIG_RUST=y' "$auto_conf"; then
    if [[ ! -s "$kernel_build/rust/libkernel.rmeta" ]]; then
        build_rust_metadata
    fi
    build_module
else
    find_kernel_source
    rust_llvm=$("$rustc" -vV |
        sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p')
    [[ -n "$rust_llvm" ]] || die "cannot determine rustc's LLVM version"
    clang=$(select_command \
        "${ZEROFS_CLANG:-}" "clang-$rust_llvm" "clang$rust_llvm" clang) ||
        die "Clang $rust_llvm is not installed"
    llvm_link=$(select_command \
        "${ZEROFS_LLVM_LINK:-}" \
        "llvm-link-$rust_llvm" "llvm-link$rust_llvm" llvm-link) ||
        die "llvm-link $rust_llvm is not installed"
    llvm_opt=$(select_command \
        "${ZEROFS_LLVM_OPT:-}" "opt-$rust_llvm" "opt$rust_llvm" opt) ||
        die "opt $rust_llvm is not installed"
    llvm_nm=$(select_command \
        "${ZEROFS_LLVM_NM:-}" \
        "llvm-nm-$rust_llvm" "llvm-nm$rust_llvm" llvm-nm) ||
        die "llvm-nm $rust_llvm is not installed"

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
