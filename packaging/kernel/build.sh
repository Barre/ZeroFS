#!/bin/sh
set -eu

usage() {
    cat <<'EOF'
Build exact-kernel ZeroFS module packages with nFPM.

Usage:
  packaging/kernel/build.sh \
    --module PATH \
    --kernel-release RELEASE \
    --target-id ID \
    --channel-id ID \
    --arch ARCH \
    --version VERSION \
    --revision REVISION \
    --source-commit COMMIT \
    --source-tree-state clean|dirty|unknown \
    --tooling-commit COMMIT \
    --tooling-tree-state clean|dirty|unknown \
    --kernel-package-dependency DEPENDENCY \
    --kernel-upgrade-conflict CONFLICT \
    --license EXPRESSION \
    --family deb|rpm \
    [--signing-cert-der CERTIFICATE.der] \
    [--output-dir DIR]

ARCH accepts amd64, x86_64, arm64, or aarch64. The input module must already
be signed when module signing is required by the target kernel, and a signed
module must be accompanied by its public certificate in DER format.
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

safe_version() {
    case $1 in
        '' | *[!A-Za-z0-9._+~]*)
            return 1
            ;;
    esac
}

safe_metadata_text() {
    [ -n "$1" ] || return 1
    case $1 in
        *'
'*)
            return 1
            ;;
    esac
    LC_ALL=C printf '%s\n' "$1" |
        grep -Eq '^[A-Za-z0-9._+~:=(),/ <>-]+$'
}

package_slug() {
    LC_ALL=C printf '%s' "$1" |
        tr 'ABCDEFGHIJKLMNOPQRSTUVWXYZ_' 'abcdefghijklmnopqrstuvwxyz-' |
        sed 's/--*/-/g; s/^-//; s/-$//'
}

module_path=
kernel_release=
target_id=
channel_id=
input_arch=
zerofs_version=
package_revision=
source_commit=
source_tree_state=
tooling_commit=
tooling_tree_state=
kernel_package_dependency=
kernel_upgrade_conflict=
package_license=
family=
signing_cert_der=
output_dir=

while [ "$#" -gt 0 ]; do
    case $1 in
        --module)
            require_value "$1" "${2-}"
            module_path=$2
            shift 2
            ;;
        --kernel-release)
            require_value "$1" "${2-}"
            kernel_release=$2
            shift 2
            ;;
        --target-id)
            require_value "$1" "${2-}"
            target_id=$2
            shift 2
            ;;
        --channel-id)
            require_value "$1" "${2-}"
            channel_id=$2
            shift 2
            ;;
        --arch)
            require_value "$1" "${2-}"
            input_arch=$2
            shift 2
            ;;
        --version)
            require_value "$1" "${2-}"
            zerofs_version=$2
            shift 2
            ;;
        --revision)
            require_value "$1" "${2-}"
            package_revision=$2
            shift 2
            ;;
        --source-commit)
            require_value "$1" "${2-}"
            source_commit=$2
            shift 2
            ;;
        --source-tree-state)
            require_value "$1" "${2-}"
            source_tree_state=$2
            shift 2
            ;;
        --tooling-commit)
            require_value "$1" "${2-}"
            tooling_commit=$2
            shift 2
            ;;
        --tooling-tree-state)
            require_value "$1" "${2-}"
            tooling_tree_state=$2
            shift 2
            ;;
        --kernel-package-dependency)
            require_value "$1" "${2-}"
            kernel_package_dependency=$2
            shift 2
            ;;
        --kernel-upgrade-conflict)
            require_value "$1" "${2-}"
            kernel_upgrade_conflict=$2
            shift 2
            ;;
        --license)
            require_value "$1" "${2-}"
            package_license=$2
            shift 2
            ;;
        --family)
            require_value "$1" "${2-}"
            family=$2
            shift 2
            ;;
        --signing-cert-der)
            require_value "$1" "${2-}"
            signing_cert_der=$2
            shift 2
            ;;
        --output-dir)
            require_value "$1" "${2-}"
            output_dir=$2
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

[ -n "$module_path" ] || die "--module is required"
[ -n "$kernel_release" ] || die "--kernel-release is required"
[ -n "$target_id" ] || die "--target-id is required"
[ -n "$channel_id" ] || die "--channel-id is required"
[ -n "$input_arch" ] || die "--arch is required"
[ -n "$zerofs_version" ] || die "--version is required"
[ -n "$package_revision" ] || die "--revision is required"
[ -n "$source_commit" ] || die "--source-commit is required"
[ -n "$source_tree_state" ] || die "--source-tree-state is required"
[ -n "$tooling_commit" ] || die "--tooling-commit is required"
[ -n "$tooling_tree_state" ] || die "--tooling-tree-state is required"
[ -n "$kernel_package_dependency" ] ||
    die "--kernel-package-dependency is required"
[ -n "$package_license" ] || die "--license is required"
[ -n "$family" ] || die "--family is required"

safe_identifier "$kernel_release" ||
    die "kernel release contains unsupported characters: $kernel_release"
safe_identifier "$target_id" ||
    die "target ID contains unsupported characters: $target_id"
safe_identifier "$channel_id" ||
    die "channel ID contains unsupported characters: $channel_id"
safe_version "$zerofs_version" ||
    die "version must start with the upstream version and contain no '-'"
safe_version "$package_revision" ||
    die "revision contains unsupported characters"
safe_metadata_text "$kernel_package_dependency" ||
    die "kernel package dependency contains unsupported characters"
if [ -n "$kernel_upgrade_conflict" ]; then
    safe_metadata_text "$kernel_upgrade_conflict" ||
        die "kernel upgrade conflict contains unsupported characters"
fi
safe_metadata_text "$package_license" ||
    die "license expression contains unsupported characters"
if [ "$source_commit" != unknown ]; then
    LC_ALL=C printf '%s\n' "$source_commit" |
        grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$' ||
        die "source commit must be a full lowercase Git object ID or unknown"
fi
case $source_tree_state in
    clean | dirty | unknown) ;;
    *) die "source tree state must be clean, dirty, or unknown" ;;
esac
if [ "$tooling_commit" != unknown ]; then
    LC_ALL=C printf '%s\n' "$tooling_commit" |
        grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$' ||
        die "tooling commit must be a full lowercase Git object ID or unknown"
fi
case $tooling_tree_state in
    clean | dirty | unknown) ;;
    *) die "tooling tree state must be clean, dirty, or unknown" ;;
esac

case $zerofs_version in
    [0-9]*) ;;
    *) die "version must begin with a digit" ;;
esac
case $package_revision in
    [A-Za-z0-9]*) ;;
    *) die "revision must begin with a letter or digit" ;;
esac

case $input_arch in
    amd64 | x86_64)
        nfpm_arch=amd64
        package_arch_deb=amd64
        package_arch_rpm=x86_64
        canonical_arch=x86_64
        ;;
    arm64 | aarch64)
        nfpm_arch=arm64
        package_arch_deb=arm64
        package_arch_rpm=aarch64
        canonical_arch=aarch64
        ;;
    *)
        die "unsupported architecture: $input_arch"
        ;;
esac

case $family in
    deb | rpm) ;;
    *) die "--family must be deb or rpm" ;;
esac

case $family in
    deb)
        LC_ALL=C printf '%s\n' "$kernel_package_dependency" |
            grep -Eq '^[A-Za-z0-9.+:-]+ \(= [A-Za-z0-9._+~:-]+\)$' ||
            die "deb kernel dependency must use 'package (= exact-version)'"
        [ -n "$kernel_upgrade_conflict" ] ||
            die "deb package requires --kernel-upgrade-conflict"
        LC_ALL=C printf '%s\n' "$kernel_upgrade_conflict" |
            grep -Eq '^[A-Za-z0-9.+:-]+ \(>> [A-Za-z0-9._+~:-]+\)$' ||
            die "deb kernel conflict must use 'package (>> version)'"
        ;;
    rpm)
        LC_ALL=C printf '%s\n' "$kernel_package_dependency" |
            grep -Eq '^[A-Za-z0-9._+:-]+ = [A-Za-z0-9._+~:-]+$' ||
            die "rpm kernel dependency must use 'capability = exact-version'"
        [ -n "$kernel_upgrade_conflict" ] ||
            die "rpm package requires --kernel-upgrade-conflict"
        LC_ALL=C printf '%s\n' "$kernel_upgrade_conflict" |
            grep -Eq '^[A-Za-z0-9._+:-]+ > [A-Za-z0-9._+~:-]+$' ||
            die "rpm kernel conflict must use 'capability > version'"
        ;;
esac

command -v nfpm >/dev/null 2>&1 || die "nfpm is required"
command -v modinfo >/dev/null 2>&1 || die "modinfo from kmod is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"
command -v readelf >/dev/null 2>&1 || die "readelf from binutils is required"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"

[ -f "$module_path" ] || die "module is not a regular file: $module_path"
module_dir=$(CDPATH='' cd -P -- "$(dirname -- "$module_path")" && pwd)
module_path=$module_dir/$(basename -- "$module_path")

module_name=$(modinfo -F name "$module_path") ||
    die "cannot read module metadata from $module_path"
[ "$module_name" = zerofs ] ||
    die "expected module name zerofs, found: $module_name"

module_machine=$(readelf -h "$module_path" |
    sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
case $canonical_arch:$module_machine in
    "x86_64:Advanced Micro Devices X86-64" | "aarch64:AArch64")
        ;;
    *)
        die "module machine '$module_machine' does not match '$canonical_arch'"
        ;;
esac

module_vermagic=$(modinfo -F vermagic "$module_path") ||
    die "cannot read module vermagic from $module_path"
case $module_vermagic in
    "$kernel_release" | "$kernel_release "*)
        ;;
    *)
        die "module vermagic '$module_vermagic' does not target '$kernel_release'"
        ;;
esac
safe_metadata_text "$module_vermagic" ||
    die "module vermagic cannot be represented safely in provenance JSON"

signature_id=$(modinfo -F sig_id "$module_path") ||
    die "cannot read module signature metadata from $module_path"
certificate_sha256=
signature_signer=
signature_key=
signature_hash=
if [ -n "$signing_cert_der" ]; then
    command -v openssl >/dev/null 2>&1 ||
        die "openssl is required for a signed module"
    [ -f "$signing_cert_der" ] ||
        die "signing certificate is not a regular file: $signing_cert_der"
    signing_cert_dir=$(CDPATH='' cd -P -- \
        "$(dirname -- "$signing_cert_der")" && pwd)
    signing_cert_der=$signing_cert_dir/$(basename -- "$signing_cert_der")
    openssl x509 -inform DER -in "$signing_cert_der" -noout ||
        die "signing certificate is not valid DER X.509"
    openssl x509 \
        -inform DER \
        -in "$signing_cert_der" \
        -noout \
        -ext extendedKeyUsage |
        grep -Fq 'Code Signing' ||
        die "signing certificate lacks the codeSigning extended key usage"
    [ "$signature_id" = PKCS#7 ] ||
        die "module accompanied by a certificate is not PKCS#7 signed"
    signature_signer=$(modinfo -F signer "$module_path")
    signature_key=$(modinfo -F sig_key "$module_path")
    signature_hash=$(modinfo -F sig_hashalgo "$module_path")
    [ -n "$signature_signer" ] ||
        die "signed module has no signer metadata"
    [ -n "$signature_key" ] ||
        die "signed module has no signature key metadata"
    [ -n "$signature_hash" ] ||
        die "signed module has no signature hash metadata"
    certificate_sha256=$(sha256sum "$signing_cert_der")
    certificate_sha256=${certificate_sha256%% *}
elif [ -n "$signature_id" ]; then
    die "signed module requires --signing-cert-der"
fi

script_dir=$(CDPATH='' cd -P -- "$(dirname -- "$0")" && pwd)
target_slug=$(package_slug "$target_id")
kernel_slug=$(package_slug "$kernel_release")
[ -n "$target_slug" ] || die "target ID does not produce a valid package name"
[ -n "$kernel_slug" ] || die "kernel release does not produce a valid package name"

leaf_package=zerofs-kernel-client-"$target_slug"-"$kernel_slug"
meta_package=zerofs-kernel-client
full_version=$zerofs_version-$package_revision

if [ -z "$output_dir" ]; then
    output_dir=$PWD/dist/kernel/$family/$target_slug/$kernel_slug/$canonical_arch
fi
mkdir -p "$output_dir"
output_dir=$(CDPATH='' cd -P -- "$output_dir" && pwd)

case $family in
    deb)
        leaf_filename=${leaf_package}_${full_version}_${package_arch_deb}.deb
        meta_filename=${meta_package}_${full_version}_${target_slug}_${kernel_slug}_${package_arch_deb}.deb
        leaf_dependency="$leaf_package (= $full_version)"
        leaf_provides="zerofs-kernel-module (= $full_version)"
        ;;
    rpm)
        leaf_filename=${leaf_package}-${full_version}.${package_arch_rpm}.rpm
        meta_filename=${meta_package}-${full_version}.${target_slug}.${kernel_slug}.${package_arch_rpm}.rpm
        leaf_dependency="$leaf_package = $full_version"
        leaf_provides="zerofs-kernel-module = $full_version"
        ;;
esac

[ ! -e "$output_dir/$leaf_filename" ] ||
    die "refusing to overwrite $output_dir/$leaf_filename"
[ ! -e "$output_dir/$meta_filename" ] ||
    die "refusing to overwrite $output_dir/$meta_filename"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zerofs-kernel-package.XXXXXX")
cleanup() {
    case ${work_dir-} in
        "${TMPDIR:-/tmp}"/zerofs-kernel-package.*)
            rm -rf -- "$work_dir"
            ;;
    esac
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$work_dir/content" "$work_dir/scripts" "$work_dir/output"
cp "$module_path" "$work_dir/content/zerofs.ko"
cp "$script_dir/zerofs.conf" "$work_dir/content/zerofs.conf"
chmod 0644 "$work_dir/content/zerofs.ko"
chmod 0644 "$work_dir/content/zerofs.conf"
if [ -n "$signing_cert_der" ]; then
    cp "$signing_cert_der" "$work_dir/content/signing-cert.der"
    chmod 0644 "$work_dir/content/signing-cert.der"
fi

module_sha256=$(sha256sum "$work_dir/content/zerofs.ko")
module_sha256=${module_sha256%% *}
module_size=$(wc -c <"$work_dir/content/zerofs.ko" | tr -d ' ')

source_date_epoch_json=null
if [ -n "${SOURCE_DATE_EPOCH-}" ]; then
    case $SOURCE_DATE_EPOCH in
        *[!0-9]*) die "SOURCE_DATE_EPOCH must be an unsigned integer" ;;
        *) source_date_epoch_json=$SOURCE_DATE_EPOCH ;;
    esac
fi

export ZEROFS_PACKAGE_ARCH=$canonical_arch
export ZEROFS_PACKAGE_CERTIFICATE_SHA256="$certificate_sha256"
export ZEROFS_PACKAGE_CHANNEL_ID="$channel_id"
export ZEROFS_PACKAGE_FAMILY="$family"
export ZEROFS_PACKAGE_FULL_VERSION="$full_version"
export ZEROFS_PACKAGE_KERNEL_DEPENDENCY="$kernel_package_dependency"
export ZEROFS_PACKAGE_KERNEL_UPGRADE_CONFLICT="$kernel_upgrade_conflict"
export ZEROFS_PACKAGE_KERNEL_RELEASE="$kernel_release"
export ZEROFS_PACKAGE_LEAF="$leaf_package"
export ZEROFS_PACKAGE_LICENSE="$package_license"
export ZEROFS_PACKAGE_META=$meta_package
export ZEROFS_PACKAGE_MODULE_SHA256="$module_sha256"
export ZEROFS_PACKAGE_MODULE_SIZE="$module_size"
export ZEROFS_PACKAGE_MODULE_VERMAGIC="$module_vermagic"
export ZEROFS_PACKAGE_REVISION="$package_revision"
export ZEROFS_PACKAGE_SIGNATURE_HASH="$signature_hash"
export ZEROFS_PACKAGE_SIGNATURE_ID="$signature_id"
export ZEROFS_PACKAGE_SIGNATURE_KEY="$signature_key"
export ZEROFS_PACKAGE_SIGNATURE_SIGNER="$signature_signer"
export ZEROFS_PACKAGE_SOURCE_DATE_EPOCH="$source_date_epoch_json"
export ZEROFS_PACKAGE_SOURCE_COMMIT="$source_commit"
export ZEROFS_PACKAGE_SOURCE_TREE_STATE="$source_tree_state"
export ZEROFS_PACKAGE_TARGET_ID="$target_id"
export ZEROFS_PACKAGE_TOOLING_COMMIT="$tooling_commit"
export ZEROFS_PACKAGE_TOOLING_TREE_STATE="$tooling_tree_state"
export ZEROFS_PACKAGE_VERSION="$zerofs_version"
python3 - \
    "$work_dir/content/leaf-provenance.json" \
    "$work_dir/content/meta-provenance.json" <<'PY'
import json
import os
import sys
from pathlib import Path


def package(name):
    return {
        "name": name,
        "version": os.environ["ZEROFS_PACKAGE_VERSION"],
        "revision": os.environ["ZEROFS_PACKAGE_REVISION"],
        "family": os.environ["ZEROFS_PACKAGE_FAMILY"],
        "architecture": os.environ["ZEROFS_PACKAGE_ARCH"],
        "license": os.environ["ZEROFS_PACKAGE_LICENSE"],
    }


signature_id = os.environ["ZEROFS_PACKAGE_SIGNATURE_ID"]
if signature_id:
    module_signing = {
        "certificate_path": (
            "/usr/share/zerofs/zerofs-module-signing-cert.der"
        ),
        "certificate_sha256": os.environ[
            "ZEROFS_PACKAGE_CERTIFICATE_SHA256"
        ],
        "signature_id": signature_id,
        "signer": os.environ["ZEROFS_PACKAGE_SIGNATURE_SIGNER"],
        "key": os.environ["ZEROFS_PACKAGE_SIGNATURE_KEY"],
        "hash_algorithm": os.environ["ZEROFS_PACKAGE_SIGNATURE_HASH"],
    }
else:
    module_signing = None

source_date_epoch_text = os.environ["ZEROFS_PACKAGE_SOURCE_DATE_EPOCH"]
source_date_epoch = (
    None if source_date_epoch_text == "null" else int(source_date_epoch_text)
)
target = {
    "id": os.environ["ZEROFS_PACKAGE_TARGET_ID"],
    "channel_id": os.environ["ZEROFS_PACKAGE_CHANNEL_ID"],
    "kernel_release": os.environ["ZEROFS_PACKAGE_KERNEL_RELEASE"],
    "architecture": os.environ["ZEROFS_PACKAGE_ARCH"],
}
tooling = {
    "git_commit": os.environ["ZEROFS_PACKAGE_TOOLING_COMMIT"],
    "tree_state": os.environ["ZEROFS_PACKAGE_TOOLING_TREE_STATE"],
}
leaf = {
    "schema_version": 1,
    "artifact_kind": "zerofs-kernel-module",
    "package": package(os.environ["ZEROFS_PACKAGE_LEAF"]),
    "zerofs": {"version": os.environ["ZEROFS_PACKAGE_VERSION"]},
    "target": {
        **target,
        "kernel_package_dependency": os.environ[
            "ZEROFS_PACKAGE_KERNEL_DEPENDENCY"
        ],
    },
    "module": {
        "name": "zerofs",
        "vermagic": os.environ["ZEROFS_PACKAGE_MODULE_VERMAGIC"],
        "sha256": os.environ["ZEROFS_PACKAGE_MODULE_SHA256"],
        "size": int(os.environ["ZEROFS_PACKAGE_MODULE_SIZE"]),
    },
    "module_signing": module_signing,
    "source": {
        "git_commit": os.environ["ZEROFS_PACKAGE_SOURCE_COMMIT"],
        "tree_state": os.environ["ZEROFS_PACKAGE_SOURCE_TREE_STATE"],
    },
    "tooling": tooling,
    "source_date_epoch": source_date_epoch,
}
meta = {
    "schema_version": 1,
    "artifact_kind": "zerofs-kernel-client-selector",
    "package": package(os.environ["ZEROFS_PACKAGE_META"]),
    "target": target,
    "dependency": {
        "name": os.environ["ZEROFS_PACKAGE_LEAF"],
        "version": os.environ["ZEROFS_PACKAGE_FULL_VERSION"],
    },
    "kernel_upgrade_conflict": (
        os.environ["ZEROFS_PACKAGE_KERNEL_UPGRADE_CONFLICT"] or None
    ),
    "module_signing": module_signing,
    "source": {
        "git_commit": os.environ["ZEROFS_PACKAGE_SOURCE_COMMIT"],
        "tree_state": os.environ["ZEROFS_PACKAGE_SOURCE_TREE_STATE"],
    },
    "tooling": tooling,
    "source_date_epoch": source_date_epoch,
}
for path_text, value in zip(sys.argv[1:], (leaf, meta), strict=True):
    path = Path(path_text)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
PY

sed "s/@KERNEL_RELEASE@/$kernel_release/g" \
    "$script_dir/scripts/leaf-postinstall.sh.in" \
    >"$work_dir/scripts/leaf-postinstall.sh"
sed "s/@KERNEL_RELEASE@/$kernel_release/g" \
    "$script_dir/scripts/leaf-postremove.sh.in" \
    >"$work_dir/scripts/leaf-postremove.sh"
sed "s/@KERNEL_RELEASE@/$kernel_release/g" \
    "$script_dir/scripts/meta-postinstall.sh.in" \
    >"$work_dir/scripts/meta-postinstall.sh"
chmod 0755 "$work_dir/scripts/leaf-postinstall.sh" \
    "$work_dir/scripts/leaf-postremove.sh" \
    "$work_dir/scripts/meta-postinstall.sh"

sed \
    -e "s|@LEAF_PACKAGE@|$leaf_package|g" \
    -e "s|@NFPM_ARCH@|$nfpm_arch|g" \
    -e "s|@ZEROFS_VERSION@|$zerofs_version|g" \
    -e "s|@PACKAGE_REVISION@|$package_revision|g" \
    -e "s|@TARGET_ID@|$target_id|g" \
    -e "s|@KERNEL_RELEASE@|$kernel_release|g" \
    -e "s|@KERNEL_PACKAGE_DEPENDENCY@|$kernel_package_dependency|g" \
    -e "s|@LEAF_PROVIDES@|$leaf_provides|g" \
    -e "s|@PACKAGE_LICENSE@|$package_license|g" \
    "$script_dir/nfpm-leaf.yaml" >"$work_dir/nfpm-leaf.yaml"

sed \
    -e "s|@META_PACKAGE@|$meta_package|g" \
    -e "s|@NFPM_ARCH@|$nfpm_arch|g" \
    -e "s|@ZEROFS_VERSION@|$zerofs_version|g" \
    -e "s|@PACKAGE_REVISION@|$package_revision|g" \
    -e "s|@TARGET_ID@|$target_id|g" \
    -e "s|@KERNEL_RELEASE@|$kernel_release|g" \
    -e "s|@LEAF_DEPENDENCY@|$leaf_dependency|g" \
    -e "s|@PACKAGE_LICENSE@|$package_license|g" \
    "$script_dir/nfpm-meta.yaml" >"$work_dir/nfpm-meta.yaml"
export ZEROFS_PACKAGE_KERNEL_UPGRADE_CONFLICT="$kernel_upgrade_conflict"
export ZEROFS_PACKAGE_HAS_SIGNING_CERT=false
if [ -n "$signing_cert_der" ]; then
    ZEROFS_PACKAGE_HAS_SIGNING_CERT=true
    export ZEROFS_PACKAGE_HAS_SIGNING_CERT
fi
python3 - "$work_dir/nfpm-meta.yaml" <<'PY'
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
certificate_marker = "  # @SIGNING_CERT_CONTENT@"
conflict_marker = "# @KERNEL_UPGRADE_CONFLICT@"
text = path.read_text(encoding="utf-8")
for marker in (certificate_marker, conflict_marker):
    if text.count(marker) != 1:
        raise SystemExit(f"{path}: marker is missing or duplicated: {marker}")
if os.environ["ZEROFS_PACKAGE_HAS_SIGNING_CERT"] == "true":
    certificate = """  - src: ./content/signing-cert.der
    dst: /usr/share/zerofs/zerofs-module-signing-cert.der
    file_info:
      mode: 0644"""
else:
    certificate = ""
conflict = os.environ["ZEROFS_PACKAGE_KERNEL_UPGRADE_CONFLICT"]
conflict = f'conflicts:\n  - "{conflict}"' if conflict else ""
path.write_text(
    text.replace(certificate_marker, certificate)
    .replace(conflict_marker, conflict),
    encoding="utf-8",
)
PY

(
    cd "$work_dir"
    nfpm pkg \
        -f "$work_dir/nfpm-leaf.yaml" \
        -p "$family" \
        -t "$work_dir/output/$leaf_filename" >&2
    nfpm pkg \
        -f "$work_dir/nfpm-meta.yaml" \
        -p "$family" \
        -t "$work_dir/output/$meta_filename" >&2
)

mv "$work_dir/output/$leaf_filename" "$output_dir/$leaf_filename"
mv "$work_dir/output/$meta_filename" "$output_dir/$meta_filename"
chmod 0644 "$output_dir/$leaf_filename" "$output_dir/$meta_filename"

printf '%s\n' "$output_dir/$leaf_filename"
printf '%s\n' "$output_dir/$meta_filename"
