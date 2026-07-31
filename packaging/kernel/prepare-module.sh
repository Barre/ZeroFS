#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Prepare a ZeroFS kernel module for packaging.

Usage:
  packaging/kernel/prepare-module.sh \
    --input INPUT.ko \
    --output OUTPUT.ko \
    --kernel-release RELEASE \
    --arch x86_64|amd64|aarch64|arm64 \
    [--strip-tool /path/to/target-strip] \
    [--signer /trusted/path/to/kmodsign \
     --sign-key /path/to/dedicated-private-key.pem \
     --sign-cert /path/to/dedicated-certificate.der \
     [--sign-hash sha256]]

Signing is optional, but --signer, --sign-key, and --sign-cert must always be
supplied together. The signer must use kmodsign-compatible arguments:
HASH KEY CERT MODULE. Stripping, when requested, happens before signing.
EOF
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_value() {
    option=$1
    value=${2-}
    [ -n "$value" ] || die "$option requires a value"
}

safe_identifier() {
    case $1 in
        '' | *[!A-Za-z0-9._+~-]*)
            return 1
            ;;
    esac
}

validate_module() {
    module=$1

    module_name=$(modinfo -F name "$module") ||
        die "cannot read module metadata from $module"
    [ "$module_name" = zerofs ] ||
        die "expected module name zerofs, found: $module_name"

    module_vermagic=$(modinfo -F vermagic "$module") ||
        die "cannot read module vermagic from $module"
    case $module_vermagic in
        "$kernel_release" | "$kernel_release "*)
            ;;
        *)
            die "module vermagic '$module_vermagic' does not target '$kernel_release'"
            ;;
    esac

    module_machine=$(readelf -h "$module" |
        sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
    case $canonical_arch:$module_machine in
        "x86_64:Advanced Micro Devices X86-64" | "aarch64:AArch64")
            ;;
        *)
            die "module machine '$module_machine' does not match '$canonical_arch'"
            ;;
    esac
}

input=
output=
kernel_release=
input_arch=
strip_tool=
signer=
sign_key=
sign_cert=
sign_hash=sha256
sign_hash_set=false

while [ "$#" -gt 0 ]; do
    case $1 in
        --input)
            require_value "$1" "${2-}"
            input=$2
            shift 2
            ;;
        --output)
            require_value "$1" "${2-}"
            output=$2
            shift 2
            ;;
        --kernel-release)
            require_value "$1" "${2-}"
            kernel_release=$2
            shift 2
            ;;
        --arch)
            require_value "$1" "${2-}"
            input_arch=$2
            shift 2
            ;;
        --strip-tool)
            require_value "$1" "${2-}"
            strip_tool=$2
            shift 2
            ;;
        --signer)
            require_value "$1" "${2-}"
            signer=$2
            shift 2
            ;;
        --sign-key)
            require_value "$1" "${2-}"
            sign_key=$2
            shift 2
            ;;
        --sign-cert)
            require_value "$1" "${2-}"
            sign_cert=$2
            shift 2
            ;;
        --sign-hash)
            require_value "$1" "${2-}"
            sign_hash=$2
            sign_hash_set=true
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

[ -n "$input" ] || die "--input is required"
[ -n "$output" ] || die "--output is required"
[ -n "$kernel_release" ] || die "--kernel-release is required"
[ -n "$input_arch" ] || die "--arch is required"

safe_identifier "$kernel_release" ||
    die "kernel release contains unsupported characters: $kernel_release"
case $sign_hash in
    '' | *[!A-Za-z0-9_-]*)
        die "signature hash contains unsupported characters"
        ;;
esac

case $input_arch in
    amd64 | x86_64)
        canonical_arch=x86_64
        ;;
    arm64 | aarch64)
        canonical_arch=aarch64
        ;;
    *)
        die "unsupported architecture: $input_arch"
        ;;
esac

signing=false
if [ -n "$signer" ] || [ -n "$sign_key" ] || [ -n "$sign_cert" ]; then
    [ -n "$signer" ] && [ -n "$sign_key" ] && [ -n "$sign_cert" ] ||
        die "--signer, --sign-key, and --sign-cert must be supplied together"
    signing=true
elif [ "$sign_hash_set" = true ]; then
    die "--sign-hash requires --signer, --sign-key, and --sign-cert"
fi

command -v modinfo >/dev/null 2>&1 || die "modinfo from kmod is required"
command -v readelf >/dev/null 2>&1 || die "readelf from binutils is required"

[ -f "$input" ] || die "input is not a regular file: $input"
[ ! -e "$output" ] && [ ! -L "$output" ] ||
    die "refusing to overwrite output: $output"

input_dir=$(CDPATH='' cd -P -- "$(dirname -- "$input")" && pwd)
input=$input_dir/$(basename -- "$input")
output_dir=$(CDPATH='' cd -P -- "$(dirname -- "$output")" && pwd) ||
    die "output directory does not exist: $(dirname -- "$output")"
output=$output_dir/$(basename -- "$output")

[ "$input" != "$output" ] || die "input and output must be different paths"

if [ -n "$strip_tool" ]; then
    [ -x "$strip_tool" ] || die "strip tool is not executable: $strip_tool"
fi
if [ "$signing" = true ]; then
    [ -x "$signer" ] || die "module signer is not executable: $signer"
    [ -f "$sign_key" ] && [ -r "$sign_key" ] ||
        die "signing key is not a readable regular file: $sign_key"
    [ -f "$sign_cert" ] && [ -r "$sign_cert" ] ||
        die "signing certificate is not a readable regular file: $sign_cert"
fi

validate_module "$input"

temporary=$(mktemp "$output_dir/.zerofs-module.XXXXXX.ko")
cleanup() {
    if [ -n "${temporary-}" ]; then
        rm -f -- "$temporary"
    fi
}
trap cleanup EXIT HUP INT TERM

cp "$input" "$temporary"

if [ -n "$strip_tool" ]; then
    "$strip_tool" --strip-debug "$temporary"
fi

validate_module "$temporary"

if [ "$signing" = true ]; then
    existing_signature=$(modinfo -F sig_id "$temporary") ||
        die "cannot inspect the prepared module signature"
    [ -z "$existing_signature" ] ||
        die "prepared module is already signed; refusing to append another signature"

    "$signer" "$sign_hash" "$sign_key" "$sign_cert" "$temporary"

    signature_id=$(modinfo -F sig_id "$temporary") ||
        die "cannot read signature type from prepared module"
    signature_signer=$(modinfo -F signer "$temporary") ||
        die "cannot read signature signer from prepared module"
    signature_key=$(modinfo -F sig_key "$temporary") ||
        die "cannot read signature key from prepared module"
    signature_hash=$(modinfo -F sig_hashalgo "$temporary") ||
        die "cannot read signature hash from prepared module"

    [ "$signature_id" = PKCS#7 ] ||
        die "unexpected module signature type: $signature_id"
    [ -n "$signature_signer" ] ||
        die "signed module has no signer metadata"
    [ -n "$signature_key" ] ||
        die "signed module has no signature key metadata"
    [ "$signature_hash" = "$sign_hash" ] ||
        die "signed module hash '$signature_hash' does not match '$sign_hash'"
fi

validate_module "$temporary"
chmod 0644 "$temporary"
if ! ln "$temporary" "$output"; then
    die "refusing to overwrite output: $output"
fi
rm -f -- "$temporary"
temporary=

printf '%s\n' "$output"
