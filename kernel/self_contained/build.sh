#!/usr/bin/env bash

set -euo pipefail

usage() {
    echo "usage: $0 KERNEL_SRC KDIR MODULE_SRC MODULE_OUT SUPPORT_OUT RUSTC BINDGEN TARGET_CC TARGET_LD TARGET_AR TARGET_NM TARGET_OBJCOPY TARGET_OBJDUMP TARGET_READELF TARGET_STRIP CLANG LLVM_LINK OPT LLVM_NM" >&2
    exit 2
}

[[ $# -eq 19 ]] || usage

kernel_source=$(realpath "$1")
kernel_build=$(realpath "$2")
module_source=$(realpath "$3")
module_output=$(realpath -m "$4")
support_output=$(realpath -m "$5")
rustc=$6
bindgen=$7
target_cc=$8
target_ld=$9
target_ar=${10}
target_nm=${11}
target_objcopy=${12}
target_objdump=${13}
target_readelf=${14}
target_strip=${15}
clang=${16}
llvm_link=${17}
opt=${18}
llvm_nm=${19}
kernel_source_provenance=${ZEROFS_KERNEL_SOURCE_PROVENANCE:-}

runtime_compat="$module_source/self_contained/runtime_compat.py"
auto_conf="$kernel_build/include/config/auto.conf"
kernel_release_file="$kernel_build/include/config/kernel.release"

fail() {
    echo "self-contained module build: $*" >&2
    exit 1
}

require_file() {
    [[ -f "$1" ]] || fail "required file not found: $1"
}

require_tool() {
    command -v "$1" >/dev/null 2>&1 ||
        fail "required tool not found: $1"
}

require_config() {
    grep -Eq "^$1=(y|m)$" "$auto_conf" ||
        fail "target kernel does not enable $1"
}

require_file "$kernel_source/Makefile"
require_file "$kernel_source/rust/Makefile"
require_file "$kernel_build/.config"
require_file "$auto_conf"
require_file "$kernel_release_file"
require_file "$kernel_build/Module.symvers"
require_file "$runtime_compat"

[[ "$kernel_source" != "$kernel_build" ]] ||
    fail "KERNEL_SRC must be a clean source tree separate from KDIR"
if [[ -L "$kernel_build/source" ]]; then
    recorded_source=$(realpath "$kernel_build/source")
    [[ "$recorded_source" == "$kernel_source" ]] ||
        fail "KDIR was built from $recorded_source, not $kernel_source"
    source_is_recorded=1
elif [[ -e "$kernel_build/source" ]]; then
    fail "KDIR/source exists but is not a symbolic link"
else
    [[ -n "$kernel_source_provenance" ]] ||
        fail "KDIR does not record its source tree; set ZEROFS_KERNEL_SOURCE_PROVENANCE to the pinned distribution source-package identity"
    [[ "$kernel_source_provenance" != *$'\n'* &&
       "$kernel_source_provenance" != *$'\r'* ]] ||
        fail "kernel source provenance must fit on one line"
    source_is_recorded=0
fi

for tool in \
    "$rustc" "$bindgen" "$target_cc" "$clang" "$llvm_link" "$opt" \
    "$llvm_nm"; do
    require_tool "$tool"
done

grep -qx 'CONFIG_X86_64=y' "$auto_conf" ||
    fail "the self-contained build supports x86_64 only"
if grep -qx 'CONFIG_CC_IS_CLANG=y' "$auto_conf"; then
    target_compiler=Clang
elif grep -qx 'CONFIG_CC_IS_GCC=y' "$auto_conf"; then
    target_compiler=GCC
else
    fail "target kernel compiler is neither GCC nor Clang"
fi

read -r selected_compiler selected_cc_version < <(
    "$kernel_source/scripts/cc-version.sh" "$target_cc"
)
[[ "$selected_compiler" == "$target_compiler" ]] ||
    fail "target kernel uses $target_compiler, but TARGET_CC is $selected_compiler"
configured_cc_version=$(sed -n \
    "s/^CONFIG_${target_compiler^^}_VERSION=\\([0-9][0-9]*\\)$/\\1/p" \
    "$auto_conf")
[[ -n "$configured_cc_version" &&
   "$selected_cc_version" == "$configured_cc_version" ]] ||
    fail "TARGET_CC version does not match the target kernel"
configured_cc_text=$(sed -n 's/^CONFIG_CC_VERSION_TEXT=//p' "$auto_conf")
selected_cc_text=$(LC_ALL=C "$target_cc" --version | sed -n '1p')
[[ -n "$configured_cc_text" && "$selected_cc_text" == "$configured_cc_text" ]] ||
    fail "TARGET_CC is not the exact compiler used for the target kernel"

target_make_args=()
configured_as_version=$(sed -n \
    's/^CONFIG_AS_VERSION=\([0-9][0-9]*\)$/\1/p' "$auto_conf")
if grep -qx 'CONFIG_AS_IS_GNU=y' "$auto_conf"; then
    assembler_flags=()
    if [[ "$target_compiler" == Clang ]]; then
        target_make_args+=(LLVM_IAS=0)
        assembler_flags+=(-fno-integrated-as)
    fi
    read -r selected_assembler selected_as_version < <(
        "$kernel_source/scripts/as-version.sh" \
            "$target_cc" "${assembler_flags[@]}"
    )
    [[ "$selected_assembler" == GNU &&
       -n "$configured_as_version" &&
       "$selected_as_version" == "$configured_as_version" ]] ||
        fail "the selected GNU assembler does not match the target kernel"
elif grep -qx 'CONFIG_AS_IS_LLVM=y' "$auto_conf"; then
    [[ "$target_compiler" == Clang ]] ||
        fail "an LLVM assembler requires a Clang target compiler"
    [[ -n "$configured_as_version" &&
       "$configured_as_version" == "$selected_cc_version" ]] ||
        fail "the integrated assembler does not match the target Clang"
else
    fail "target kernel assembler is neither GNU nor LLVM"
fi

! grep -qx 'CONFIG_RUST=y' "$auto_conf" ||
    fail "use the normal module build for a target with CONFIG_RUST=y"
grep -qx 'CONFIG_MODULES=y' "$auto_conf" ||
    fail "target kernel does not enable CONFIG_MODULES"
require_config CONFIG_NETFS_SUPPORT
require_config CONFIG_UNIX
grep -qx 'CONFIG_FILE_LOCKING=y' "$auto_conf" ||
    fail "target kernel does not enable CONFIG_FILE_LOCKING"
! grep -qx 'CONFIG_CPU_BIG_ENDIAN=y' "$auto_conf" ||
    fail "big-endian targets are not supported"
! grep -Eq '^CONFIG_(GCC_PLUGIN_)?RANDSTRUCT=y$' "$auto_conf" ||
    fail "target kernel randomizes private VFS layouts"
if [[ "$target_compiler" == GCC ]]; then
    ! grep -qx 'CONFIG_GCC_PLUGINS=y' "$auto_conf" ||
        fail "GCC plugin kernels are not supported by the mixed compiler path"
    ! grep -Eq '^CONFIG_(KASAN|KCSAN|KMSAN|GCOV_KERNEL)=y$' "$auto_conf" ||
        fail "GCC KASAN, KCSAN, KMSAN, and GCOV targets are not supported"
    ! grep -qx 'CONFIG_LTO=y' "$auto_conf" ||
        fail "GCC LTO kernels are not supported"
    ! grep -Eq '^CONFIG_(CFI|CFI_CLANG)=y$' "$auto_conf" ||
        fail "CFI is not supported for GCC target kernels"
fi
if grep -Eq '^CONFIG_(CFI|CFI_CLANG)=y$' "$auto_conf" &&
   ! grep -qx 'CONFIG_CFI_ICALL_NORMALIZE_INTEGERS=y' "$auto_conf"; then
    fail "Clang CFI requires CONFIG_CFI_ICALL_NORMALIZE_INTEGERS"
fi
! grep -qx 'CONFIG_FINEIBT_BHI=y' "$auto_conf" ||
    fail "CONFIG_FINEIBT_BHI is incompatible with Rust KCFI callbacks"
if grep -qx 'CONFIG_DEBUG_INFO_BTF=y' "$auto_conf" &&
   { ! grep -qx 'CONFIG_PAHOLE_HAS_LANG_EXCLUDE=y' "$auto_conf" ||
     grep -qx 'CONFIG_LTO=y' "$auto_conf"; }; then
    fail "Rust module BTF requires PAHOLE_HAS_LANG_EXCLUDE and no LTO"
fi

rust_llvm=$("$rustc" -vV | sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p')
clang_llvm=$("$clang" --version | sed -n '1s/.*version \([0-9][0-9]*\).*/\1/p')
link_llvm=$("$llvm_link" --version |
    sed -n 's/.*LLVM version \([0-9][0-9]*\).*/\1/p')
opt_llvm=$("$opt" --version |
    sed -n 's/.*LLVM version \([0-9][0-9]*\).*/\1/p')
[[ -n "$rust_llvm" && "$rust_llvm" == "$clang_llvm" &&
   "$rust_llvm" == "$link_llvm" && "$rust_llvm" == "$opt_llvm" ]] ||
    fail "rustc, clang, llvm-link, and opt must use the same LLVM major version"

kernel_release=$(<"$kernel_release_file")
[[ -n "$kernel_release" ]] || fail "target kernel release is empty"
if ((source_is_recorded)); then
    source_release=$(make -s -C "$kernel_source" O="$kernel_build" \
        CC="$target_cc" "${target_make_args[@]}" kernelrelease)
    [[ "$source_release" == "$kernel_release" ]] ||
        fail "KERNEL_SRC produces $source_release, but KDIR was built as $kernel_release"
else
    source_version=$(make -s -C "$kernel_source" kernelversion)
    [[ -n "$source_version" ]] ||
        fail "pinned source did not report a kernel version"
    case $kernel_release in
        "$source_version" | "$source_version"[-+._~]*) ;;
        *)
            fail "pinned source version $source_version does not match target release $kernel_release"
            ;;
    esac
    printf 'self-contained module build: using packaged kernel source %s\n' \
        "$kernel_source_provenance"
fi

case "$support_output" in
    /|"$module_source"|"$module_output"|"$kernel_source"|"$kernel_build")
        fail "unsafe support output path: $support_output"
        ;;
esac
for protected in \
    "$module_source" "$module_output" "$kernel_source" "$kernel_build"; do
    case "$protected/" in
        "$support_output/"*)
            fail "support output contains protected path: $protected"
            ;;
    esac
    case "$support_output/" in
        "$protected/"*)
            fail "support output is inside protected path: $protected"
            ;;
    esac
done

support_source="$support_output/source"
support_build="$support_output/build"
support_marker="$support_output/.zerofs-self-contained-build"

if [[ -e "$support_marker" || -L "$support_marker" ]]; then
    [[ -f "$support_marker" && ! -L "$support_marker" ]] ||
        fail "ownership marker is not a regular file: $support_marker"
    [[ "$(<"$support_marker")" == "zerofs-self-contained-v1" ]] ||
        fail "invalid ownership marker below $support_output"
elif [[ -e "$support_source" || -e "$support_build" ]]; then
    fail "refusing to replace an unowned support tree below $support_output"
else
    mkdir -p "$support_output"
    printf '%s\n' "zerofs-self-contained-v1" >"$support_marker"
fi

rm -rf -- "$support_source" "$support_build"
mkdir -p "$support_source" "$support_build" "$module_output/self_contained"

while IFS= read -r -d '' entry; do
    name=${entry##*/}
    case "$name" in
        .git|rust)
            continue
            ;;
    esac
    ln -s "$entry" "$support_source/$name"
done < <(find "$kernel_source" -mindepth 1 -maxdepth 1 -print0)
rust_symlink=$(find "$kernel_source/rust" -type l -print -quit)
[[ -z "$rust_symlink" ]] ||
    fail "kernel Rust support contains a symlink: $rust_symlink"
cp -a "$kernel_source/rust" "$support_source/rust"
"$runtime_compat" "$support_source"

cp "$kernel_build/.config" "$support_build/.config"

clang_name=${clang##*/}
llvm_suffix=${clang_name#clang}
[[ "$llvm_suffix" != "$clang_name" ]] ||
    fail "cannot derive LLVM tool names from $clang"

tool_dir=${clang%/*}
if [[ "$tool_dir" == "$clang" ]]; then
    tool_dir=
else
    tool_dir+=/
fi

llvm_tool() {
    local name=$1
    local candidate="${tool_dir}${name}${llvm_suffix}"
    if command -v "$candidate" >/dev/null 2>&1; then
        printf '%s\n' "$candidate"
    elif command -v "$name" >/dev/null 2>&1; then
        command -v "$name"
    else
        fail "required LLVM tool not found: $candidate"
    fi
}

target_tool() {
    local requested=$1
    local fallback=$2
    if [[ -n "$requested" ]]; then
        require_tool "$requested"
        command -v "$requested"
    else
        require_tool "$fallback"
        command -v "$fallback"
    fi
}

if [[ "$target_compiler" == Clang ]]; then
    default_ar=$(llvm_tool llvm-ar)
    default_nm=$(llvm_tool llvm-nm)
    default_objcopy=$(llvm_tool llvm-objcopy)
    default_objdump=$(llvm_tool llvm-objdump)
    default_readelf=$(llvm_tool llvm-readelf)
    default_strip=$(llvm_tool llvm-strip)
else
    default_ar='ar'
    default_nm='nm'
    default_objcopy='objcopy'
    default_objdump='objdump'
    default_readelf='readelf'
    default_strip='strip'
fi

if grep -qx 'CONFIG_LD_IS_LLD=y' "$auto_conf"; then
    default_ld=$(llvm_tool ld.lld)
    expected_linker=LLD
    configured_ld_version=$(sed -n \
        's/^CONFIG_LLD_VERSION=\([0-9][0-9]*\)$/\1/p' "$auto_conf")
elif grep -qx 'CONFIG_LD_IS_BFD=y' "$auto_conf"; then
    default_ld=ld
    expected_linker=BFD
    configured_ld_version=$(sed -n \
        's/^CONFIG_LD_VERSION=\([0-9][0-9]*\)$/\1/p' "$auto_conf")
else
    fail "target kernel linker is neither LLD nor GNU ld"
fi
target_ld=$(target_tool "$target_ld" "$default_ld")
target_ar=$(target_tool "$target_ar" "$default_ar")
target_nm=$(target_tool "$target_nm" "$default_nm")
target_objcopy=$(target_tool "$target_objcopy" "$default_objcopy")
target_objdump=$(target_tool "$target_objdump" "$default_objdump")
target_readelf=$(target_tool "$target_readelf" "$default_readelf")
target_strip=$(target_tool "$target_strip" "$default_strip")
read -r selected_linker selected_ld_version < <(
    "$kernel_source/scripts/ld-version.sh" "$target_ld"
)
[[ "$selected_linker" == "$expected_linker" &&
   -n "$configured_ld_version" &&
   "$selected_ld_version" == "$configured_ld_version" ]] ||
    fail "TARGET_LD does not match the target kernel"
audit_strings=$(llvm_tool llvm-strings)

make_args=(
    -C "$support_source"
    O="$support_build"
    KERNELRELEASE="$kernel_release"
    "${target_make_args[@]}"
    CC="$target_cc"
    LD="$target_ld"
    AR="$target_ar"
    NM="$target_nm"
    OBJCOPY="$target_objcopy"
    OBJDUMP="$target_objdump"
    READELF="$target_readelf"
    STRIP="$target_strip"
    RUSTC="$rustc"
    BINDGEN="$bindgen"
    LLVM_LINK="$llvm_link"
)

make "${make_args[@]}" olddefconfig prepare modules_prepare

private_auto_conf="$support_build/include/config/auto.conf"
grep -qx 'CONFIG_RUST_IS_AVAILABLE=y' "$private_auto_conf" ||
    fail "the selected Rust toolchain is not compatible with this kernel"

rustc_version=$(sed -n \
    's/^CONFIG_RUSTC_VERSION=\([0-9][0-9]*\)$/\1/p' \
    "$private_auto_conf")
[[ "$rustc_version" =~ ^[0-9]+$ ]] ||
    fail "private preparation did not record the Rust compiler version"

require_rustc_version() {
    local minimum=$1
    local feature=$2
    if ((10#$rustc_version < 10#$minimum)); then
        fail "$feature requires a newer Rust compiler"
    fi
}

if grep -qx 'CONFIG_CALL_PADDING=y' "$auto_conf"; then
    require_rustc_version 108100 CONFIG_CALL_PADDING
fi
if grep -qx 'CONFIG_MITIGATION_RETHUNK=y' "$auto_conf" &&
   grep -qx 'CONFIG_KASAN=y' "$auto_conf"; then
    require_rustc_version 108300 CONFIG_MITIGATION_RETHUNK-with-KASAN
fi
if grep -qx 'CONFIG_CFI_AUTO_DEFAULT=y' "$auto_conf"; then
    require_rustc_version 108800 CONFIG_CFI_AUTO_DEFAULT
fi
if grep -Eq '^CONFIG_(CFI|CFI_CLANG)=y$' "$auto_conf"; then
    grep -qx 'CONFIG_HAVE_CFI_ICALL_NORMALIZE_INTEGERS_RUSTC=y' \
        "$private_auto_conf" ||
        fail "the selected Rust compiler lacks the target CFI ABI"
fi

compare_config() {
    local target=$1
    local private=$2
    local pattern=$3
    local difference
    if difference=$(diff -u \
        <(awk -v excluded="$pattern" \
            '$0 !~ excluded &&
             $0 !~ /^# CONFIG_[A-Z0-9_]+ is not set$/' "$target") \
        <(awk -v excluded="$pattern" \
            '$0 !~ excluded &&
             $0 !~ /^# CONFIG_[A-Z0-9_]+ is not set$/' "$private")); then
        return
    fi
    printf '%s\n' "$difference" >&2
    fail "private preparation changed the target kernel configuration"
}

compare_config "$kernel_build/.config" "$support_build/.config" \
    '^(# )?CONFIG_(RUST|HAVE_CFI_ICALL_NORMALIZE_INTEGERS_RUSTC)'
compare_config "$auto_conf" "$private_auto_conf" \
    '^CONFIG_(RUST|HAVE_CFI_ICALL_NORMALIZE_INTEGERS_RUSTC)'
compare_config \
    "$kernel_build/include/generated/autoconf.h" \
    "$support_build/include/generated/autoconf.h" \
    '^#define CONFIG_(RUST|HAVE_CFI_ICALL_NORMALIZE_INTEGERS_RUSTC)'

cp "$kernel_build/Module.symvers" "$support_build/Module.symvers"

make "${make_args[@]}" \
    CONFIG_MODVERSIONS= \
    CONFIG_RUST=y \
    CONFIG_RUST_OVERFLOW_CHECKS=y \
    RUSTFLAGS_KERNEL='--cfg CONFIG_RUST --cfg CONFIG_RUST_OVERFLOW_CHECKS --emit=llvm-bc --out-dir=rust' \
    rust/kernel.o

module_args=(
    "${make_args[@]}"
    CONFIG_RUST=y
    CONFIG_RUST_OVERFLOW_CHECKS=y
    M="$module_source"
    MO="$module_output"
    ZEROFS_SELF_CONTAINED=1
    ZEROFS_KERNEL_RUST_SOURCE="$support_source/rust"
    ZEROFS_BC_CLANG="$clang"
    RUSTFLAGS_MODULE='--cfg CONFIG_RUST --cfg CONFIG_RUST_OVERFLOW_CHECKS -Copt-level=3 -Cembed-bitcode=y'
)

make "${module_args[@]}" self_contained/bitcode.o

bitcode=(
    "$module_output/self_contained/zerofs_main.bc"
    "$support_build/rust/kernel.bc"
    "$support_build/rust/core.bc"
    "$support_build/rust/compiler_builtins.bc"
    "$support_build/rust/ffi.bc"
    "$support_build/rust/pin_init.bc"
    "$support_build/rust/bindings.bc"
    "$support_build/rust/uapi.bc"
)
if [[ -f "$support_build/rust/zerocopy.bc" ]]; then
    bitcode+=("$support_build/rust/zerocopy.bc")
fi
bitcode+=("$module_output/self_contained/kernel_helpers.bc")

for file in "${bitcode[@]}"; do
    require_file "$file"
done

linked="$module_output/self_contained/zerofs_main.linked.bc"
internal="$module_output/self_contained/zerofs_main.internal.bc"
"$llvm_link" --suppress-warnings "${bitcode[@]}" -o "$linked"
"$opt" --passes=internalize,globaldce \
    --internalize-public-api-list=init_module,cleanup_module \
    "$linked" -o "$internal"

make "${module_args[@]}" modules

module="$module_output/zerofs.ko"
require_file "$module"

if [[ "$target_compiler" == GCC ]]; then
    embedded_undefined=$("$llvm_nm" --undefined-only \
        "$module_output/self_contained/zerofs_main.o" |
        sed -n 's/^[[:space:]]*U[[:space:]]*//p')
    if grep -Eq '^(__fentry__|_mcount|mcount)$' \
        <<<"$embedded_undefined"; then
        fail "embedded Rust support unexpectedly retained ftrace calls"
    fi
fi

undefined=$("$llvm_nm" --undefined-only "$module" |
    sed -n 's/^[[:space:]]*U[[:space:]]*//p')
if grep -Eq '^(_R|__rust|rust_)' <<<"$undefined"; then
    grep -E '^(_R|__rust|rust_)' <<<"$undefined" >&2
    fail "module still imports Rust runtime symbols"
fi
if "$audit_strings" -a -n 3 "$module" | grep -F '%pA' >/dev/null; then
    fail "module still depends on the CONFIG_RUST %pA formatter"
fi
if "$audit_strings" -a "$module" | grep -F 'import_ns=' >/dev/null; then
    fail "embedded Rust support leaked a symbol namespace into the module"
fi

while IFS= read -r symbol; do
    [[ -z "$symbol" ]] && continue
    grep -Fq $'\t'"$symbol"$'\t' "$kernel_build/Module.symvers" ||
        fail "module imports a symbol absent from target Module.symvers: $symbol"
done <<<"$undefined"

printf '%s\n' "$module"
