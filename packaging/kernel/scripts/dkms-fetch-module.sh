#!/usr/bin/env bash

# Supply a prebuilt zerofs.ko from the DKMS build wrapper.
#
# URL schema (all components are obtained from the local package database):
#
#   ${ZEROFS_MODULE_BASE_URL}/v1/DISTRO/ARCH/
#       KERNEL_PACKAGE/KERNEL_PACKAGE_VERSION/KERNEL_RELEASE/
#       PACKAGE_VERSION/zerofs.ko.xz
#
# Configuration:
#   ZEROFS_MODULE_BASE_URL  HTTPS artifact root
#                           (default: https://pkgs.zerofs.net/kernel-modules)
#   ZEROFS_MODULE_CERT_FILE pinned PEM signer certificate shipped with ZeroFS
#
# Exit 75 means that no remote object could be obtained and permits the caller
# to select a fallback.  Exit 1 means that local identity or artifact
# verification failed; such failures must not silently fall back.
#
# The server is not trusted to select a module.  HTTPS protects transport, but
# acceptance depends on an appended Linux-module PKCS#7 signature verified by
# ZEROFS_MODULE_CERT_FILE and on exact signed target and vermagic checks.  In
# particular, this script never invokes apt, dnf, or zypper and therefore is
# safe to run while one of those package managers has invoked DKMS.

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
readonly script_dir

die() {
    printf '%s: %s\n' "$script_name" "$*" >&2
    exit 1
}

unavailable() {
    printf '%s: prebuilt module unavailable: %s\n' "$script_name" "$*" >&2
    exit 75
}

usage() {
    printf 'usage: %s KERNEL_RELEASE PACKAGE_VERSION DESTINATION\n' \
        "$script_name" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

cleanup() {
    [[ -z ${temporary_directory:-} ]] ||
        rm -rf -- "$temporary_directory"
    [[ -z ${destination_temporary:-} ]] ||
        rm -f -- "$destination_temporary"
}

download_module() {
    local url=$1
    local output=$2

    if ! curl --disable \
        --fail --silent --show-error --location \
        --proto '=https' --proto-redir '=https' --tlsv1.2 \
        --connect-timeout 10 --max-time 60 \
        --retry 2 --retry-delay 1 --retry-connrefused \
        --retry-max-time 75 \
        --max-filesize 134217728 \
        --user-agent 'zerofs-dkms-module-fetch/1' \
        --output "$output" -- "$url"; then
        unavailable "$url"
    fi
}

[[ $# -eq 3 ]] || usage
kernel_release=$1
package_version=$2
destination=$3

readonly safe_component_pattern='^[A-Za-z0-9][A-Za-z0-9._+~:-]*$'
[[ $kernel_release =~ $safe_component_pattern ]] ||
    die "unsafe kernel release: $kernel_release"
[[ $package_version =~ $safe_component_pattern ]] ||
    die "unsafe package version: $package_version"
[[ -n $destination && $destination != *$'\n'* &&
   $destination != *$'\r'* ]] || die 'unsafe destination path'

for command_name in curl dirname install mktemp modinfo mv openssl python3 realpath xz; do
    require_command "$command_name"
done

modules_root=${ZEROFS_MODULES_ROOT:-/lib/modules}
os_release_file=${ZEROFS_OS_RELEASE_FILE:-/etc/os-release}
kernel_build_link="$modules_root/$kernel_release/build"
[[ -d $kernel_build_link ]] ||
    die "kernel headers are not installed for $kernel_release"
kernel_build=$(realpath -e -- "$kernel_build_link")
kernel_release_file="$kernel_build/include/config/kernel.release"
[[ -s $kernel_release_file ]] ||
    die "kernel release metadata is missing: $kernel_release_file"
[[ $(<"$kernel_release_file") == "$kernel_release" ]] ||
    die "installed headers do not describe $kernel_release"

[[ -r $os_release_file ]] || die "$os_release_file is unavailable"
unset ID ID_LIKE
# os-release is a root-owned, shell-compatible data file.  The override exists
# for package tests and follows the same format.
# shellcheck disable=SC1090
. "$os_release_file"
os_id=${ID:-}
[[ $os_id =~ $safe_component_pattern ]] ||
    die "unsafe or missing ID in $os_release_file"
case $os_id in
    ubuntu | debian | fedora)
        distro=$os_id
        ;;
    opensuse | opensuse-leap | opensuse-tumbleweed)
        distro=opensuse
        ;;
    *)
        distro=
        read -r -a os_like <<<"${ID_LIKE:-}"
        for like in "${os_like[@]}"; do
            [[ $like =~ $safe_component_pattern ]] ||
                die "unsafe ID_LIKE entry in $os_release_file"
            case $like in
                ubuntu | debian | fedora)
                    distro=$like
                    break
                    ;;
                opensuse | suse)
                    distro=opensuse
                    break
                    ;;
            esac
        done
        [[ -n $distro ]] || unavailable "unsupported distribution: $os_id"
        ;;
esac

kernel_package=
kernel_package_version=
native_arch=
if [[ $distro == ubuntu || $distro == debian ]]; then
    require_command dpkg
    require_command dpkg-query
    package_owner=
    while IFS= read -r ownership; do
        # dpkg-query may emit diversion diagnostics ending in the same path.
        # Accept only an actual binary-package ownership record.
        if [[ $ownership =~ ^([a-z0-9][a-z0-9+.-]*(:[a-z0-9][a-z0-9-]*)?):[[:space:]]+(.+)$ &&
              ${BASH_REMATCH[3]} == "$kernel_release_file" ]]; then
            package_owner=${BASH_REMATCH[1]}
            break
        fi
    done < <(dpkg-query --search "$kernel_release_file" 2>/dev/null || true)
    [[ -n $package_owner ]] ||
        unavailable "cannot identify the package owning $kernel_release_file"
    package_record=$(dpkg-query --show \
        --showformat='${Package}\t${Version}\n' \
        "$package_owner" 2>/dev/null) ||
        unavailable "cannot query header package $package_owner"
    IFS=$'\t' read -r kernel_package kernel_package_version \
        <<<"$package_record"
    native_arch=$(dpkg --print-architecture 2>/dev/null) ||
        die 'cannot determine the native dpkg architecture'
else
    require_command rpm
    package_record=$(rpm --query --file \
        --queryformat '%{NAME}\t%{EPOCHNUM}\t%{VERSION}\t%{RELEASE}\n' \
        "$kernel_release_file" 2>/dev/null) ||
        unavailable "cannot identify the package owning $kernel_release_file"
    IFS=$'\t' read -r kernel_package package_epoch rpm_version \
        package_release <<<"$package_record"
    [[ $package_epoch =~ ^[0-9]+$ ]] ||
        die "invalid RPM epoch for $kernel_package"
    kernel_package_version="${package_epoch}:${rpm_version}-${package_release}"
    native_arch=$(rpm --eval '%{_arch}' 2>/dev/null) ||
        die 'cannot determine the native RPM architecture'
fi

for component in "$distro" "$native_arch" \
    "$kernel_package" "$kernel_package_version"; do
    [[ $component =~ $safe_component_pattern ]] ||
        die "unsafe package identity component: $component"
done
# Serialize colons exactly as the publisher does. Raw identity components
# cannot contain '@', so the transformation is collision-free.
distro_path=${distro//:/@}
native_arch_path=${native_arch//:/@}
kernel_package_path=${kernel_package//:/@}
kernel_package_version_path=${kernel_package_version//:/@}
kernel_release_path=${kernel_release//:/@}
module_base_url=${ZEROFS_MODULE_BASE_URL:-https://pkgs.zerofs.net/kernel-modules}
module_base_url=${module_base_url%/}
[[ $module_base_url =~ ^https://[A-Za-z0-9][A-Za-z0-9._:-]*(/[A-Za-z0-9._~+:-]+)*$ ]] ||
    die "ZEROFS_MODULE_BASE_URL must be a simple HTTPS URL: $module_base_url"
module_url="$module_base_url/v1/$distro_path/$native_arch_path/"\
"$kernel_package_path/$kernel_package_version_path/$kernel_release_path/"\
"$package_version/zerofs.ko.xz"
module_relative_path=${module_url#"$module_base_url/"}
module_identity="kernel-modules/${module_relative_path%/zerofs.ko.xz}"

certificate=${ZEROFS_MODULE_CERT_FILE:-$script_dir/zerofs-module-signing-cert.pem}
[[ -f $certificate && -s $certificate ]] ||
    die "ZeroFS module-signing certificate is unavailable: $certificate"

temporary_directory=$(mktemp -d -- "${TMPDIR:-/tmp}/zerofs-module.XXXXXXXX")
destination_temporary=
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
compressed_module="$temporary_directory/zerofs.ko.xz"
candidate_module="$temporary_directory/zerofs.ko"
unsigned_module="$temporary_directory/zerofs.unsigned.ko"
module_signature="$temporary_directory/zerofs.p7s"

download_module "$module_url" "$compressed_module"
if ! (ulimit -f 262144; xz --memlimit-decompress=256MiB \
    --decompress --stdout -- "$compressed_module" >"$candidate_module"); then
    die "cannot decompress downloaded module: $module_url"
fi
[[ -s $candidate_module ]] || die 'downloaded module decompresses to an empty file'

# Split the kernel module signing trailer without trusting modinfo to validate
# it.  Linux's trailer is: unsigned ELF, DER PKCS#7, 12-byte module_signature,
# and the fixed marker.  Kernel sign-file leaves every legacy field at zero and
# sets only the PKCS#7 identity type and signature length.
python3 -I - "$candidate_module" "$unsigned_module" "$module_signature" <<'PY'
import pathlib
import struct
import sys

source, unsigned_path, signature_path = sys.argv[1:]
data = pathlib.Path(source).read_bytes()
magic = b"~Module signature appended~\n"
trailer = struct.Struct(">BBBBBBBBI")
if not data.endswith(magic):
    raise SystemExit("downloaded module has no appended Linux module signature")
structure_end = len(data) - len(magic)
structure_start = structure_end - trailer.size
if structure_start < 0:
    raise SystemExit("downloaded module has a truncated signature trailer")
fields = trailer.unpack(data[structure_start:structure_end])
identity_type, signer_length, key_id_length = fields[2:5]
signature_length = fields[8]
if identity_type != 2:
    raise SystemExit("downloaded module signature is not PKCS#7")
if any(fields[:2]) or signer_length or key_id_length or any(fields[5:8]):
    raise SystemExit("downloaded module uses an unsupported signature trailer")
signature_start = structure_start - signature_length
if signature_length == 0 or signature_start < 64:
    raise SystemExit("downloaded module has an invalid signature length")
unsigned = data[:signature_start]
signature = data[signature_start:structure_start]
pathlib.Path(unsigned_path).write_bytes(unsigned)
pathlib.Path(signature_path).write_bytes(signature)
PY

# Kernel sign-file omits certificates from CMS.  Use only the pinned leaf to
# find and verify the signer; do not consult embedded or system certificates.
if ! openssl cms -verify -binary -inform DER \
    -in "$module_signature" -content "$unsigned_module" \
    -certfile "$certificate" -nointern -noverify \
    -out /dev/null >/dev/null 2>&1; then
    die 'downloaded module does not have a valid ZeroFS signature'
fi

module_name=$(modinfo -F name "$candidate_module" 2>/dev/null) ||
    die 'cannot read the downloaded module metadata'
[[ $module_name == zerofs ]] ||
    die "downloaded module has the wrong name: $module_name"
module_vermagic=$(modinfo -F vermagic "$candidate_module" 2>/dev/null) ||
    die 'cannot read the downloaded module vermagic'
[[ -n $module_vermagic ]] || die 'downloaded module has no vermagic'
downloaded_module_identity=$(
    modinfo -F zerofs_identity "$candidate_module" 2>/dev/null
) || die 'cannot read the downloaded module publication identity'
[[ $downloaded_module_identity == "$module_identity" ]] ||
    die 'downloaded module was signed for a different publication identity'
[[ $module_vermagic == "$kernel_release" ||
   $module_vermagic == "$kernel_release "* ]] ||
    die "downloaded module vermagic does not target $kernel_release"

destination_directory=$(dirname -- "$destination")
[[ -d $destination_directory ]] ||
    die "destination directory does not exist: $destination_directory"
destination_temporary=$(mktemp -- "$destination_directory/.zerofs.ko.XXXXXXXX")
install -m 0644 -- "$candidate_module" "$destination_temporary"
mv -fT -- "$destination_temporary" "$destination"
destination_temporary=
