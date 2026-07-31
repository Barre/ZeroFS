#!/usr/bin/env bash

set -euo pipefail
export LC_ALL=C

readonly script_name=${0##*/}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
repo_root=$(cd -- "$script_dir/../.." && pwd)
readonly repo_root
readonly catalog_helper="$repo_root/packaging/kernel/kernel-targets.py"

manifest=
target_id=
artifact_dir=
expected_version=
work_dir=

usage() {
    cat >&2 <<EOF
usage: $script_name \
  --manifest PATH \
  --target-id ID \
  --artifact-dir DIRECTORY \
  [--expected-version VERSION]
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

has_relation() {
    local metadata=$1
    local relation=$2

    if [[ "$family" == deb ]]; then
        printf '%s\n' "$metadata" |
            tr ',' '\n' |
            sed 's/^[[:space:]]*//; s/[[:space:]]*$//' |
            grep -Fx -- "$relation" >/dev/null
    else
        printf '%s\n' "$metadata" |
            grep -Fx -- "$relation" >/dev/null
    fi
}

cleanup() {
    if [[ -n "$work_dir" && -d "$work_dir" ]]; then
        case ${work_dir##*/} in
            zerofs-package-smoke.*)
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
        --artifact-dir)
            require_value "$1" "${2:-}"
            artifact_dir=$2
            shift 2
            ;;
        --expected-version)
            require_value "$1" "${2:-}"
            expected_version=$2
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
[[ -n "$artifact_dir" ]] || die "--artifact-dir is required"

require_command cmp
require_command modinfo
require_command python3
require_command readelf
require_command stat
[[ -x "$catalog_helper" ]] ||
    die "target catalog helper is missing: $catalog_helper"

manifest=$(realpath "$manifest")
artifact_dir=$(realpath "$artifact_dir")
[[ -f "$manifest" ]] || die "manifest is not a regular file: $manifest"
[[ -d "$artifact_dir" ]] ||
    die "artifact directory does not exist: $artifact_dir"
[[ -f "$artifact_dir/artifact.json" ]] ||
    die "artifact manifest is missing: $artifact_dir/artifact.json"

target_field() {
    "$catalog_helper" --manifest "$manifest" field "$target_id" "$1"
}

family=$(target_field family)
arch=$(target_field arch)
kernel_release=$(target_field kernel_release)
kernel_dependency=$(target_field kernel_dependency)
kernel_package_version=$(target_field kernel_package_version)
kernel_selector_version=$(target_field kernel_selector_version)
kernel_upgrade_conflict=$(target_field kernel_upgrade_conflict)
channel_id=$(target_field channel_id)
package_revision=$(target_field package_revision)
builder_image=$(target_field builder_image)
source_json=$(target_field source)
[[ "$kernel_release" =~ ^[A-Za-z0-9][A-Za-z0-9._+~-]*$ ]] ||
    die "kernel release contains unsupported characters: $kernel_release"

case $arch in
    x86_64)
        canonical_arch=x86_64
        deb_arch=amd64
        rpm_arch=x86_64
        elf_machine='Advanced Micro Devices X86-64'
        ;;
    aarch64)
        canonical_arch=aarch64
        deb_arch=arm64
        rpm_arch=aarch64
        elf_machine=AArch64
        ;;
    *)
        die "unsupported target architecture after validation: $arch"
        ;;
esac

if [[ -z "$expected_version" ]]; then
    expected_version=$(python3 - "$repo_root/zerofs/Cargo.toml" <<'PY'
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
fi
[[ "$expected_version" =~ ^[0-9][A-Za-z0-9._+~]*$ ]] ||
    die "expected version contains unsupported characters: $expected_version"
expected_package_version=$expected_version-$package_revision

export ZEROFS_ARTIFACT_DIR=$artifact_dir
export ZEROFS_TARGET_ID=$target_id
export ZEROFS_KERNEL_RELEASE=$kernel_release
export ZEROFS_KERNEL_PACKAGE_VERSION=$kernel_package_version
export ZEROFS_KERNEL_SELECTOR_VERSION=$kernel_selector_version
export ZEROFS_CHANNEL_ID=$channel_id
export ZEROFS_PACKAGE_REVISION=$package_revision
export ZEROFS_ZEROFS_VERSION=$expected_version
export ZEROFS_PACKAGE_FAMILY=$family
export ZEROFS_TARGET_ARCH=$arch
export ZEROFS_BUILDER_IMAGE=$builder_image
export ZEROFS_TARGET_SOURCE=$source_json
mapfile -t artifact_paths < <(
    python3 - "$artifact_dir/artifact.json" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    value = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"{path}: invalid artifact manifest: {error}")
if not isinstance(value, dict):
    raise SystemExit(f"{path}: top level must be an object")
if (
    type(value.get("schema_version")) is not int
    or value["schema_version"] != 2
):
    raise SystemExit(f"{path}: schema_version must be 2")

expected = {
    "target_id": os.environ["ZEROFS_TARGET_ID"],
    "kernel_release": os.environ["ZEROFS_KERNEL_RELEASE"],
    "kernel_package_version": os.environ["ZEROFS_KERNEL_PACKAGE_VERSION"],
    "kernel_selector_version": os.environ[
        "ZEROFS_KERNEL_SELECTOR_VERSION"
    ],
    "channel_id": os.environ["ZEROFS_CHANNEL_ID"],
    "package_revision": os.environ["ZEROFS_PACKAGE_REVISION"],
    "zerofs_version": os.environ["ZEROFS_ZEROFS_VERSION"],
    "family": os.environ["ZEROFS_PACKAGE_FAMILY"],
    "arch": os.environ["ZEROFS_TARGET_ARCH"],
    "builder_image": os.environ["ZEROFS_BUILDER_IMAGE"],
    "source": json.loads(os.environ["ZEROFS_TARGET_SOURCE"]),
}
for key, expected_value in expected.items():
    if value.get(key) != expected_value:
        raise SystemExit(
            f"{path}: {key} is {value.get(key)!r}, expected {expected_value!r}"
        )

base = Path(os.environ["ZEROFS_ARTIFACT_DIR"]).resolve()


def resolve(relative, key):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise SystemExit(f"{path}: {key} must be a non-empty relative path")
    if relative != relative.strip() or any(ord(char) < 32 for char in relative):
        raise SystemExit(f"{path}: {key} contains unsupported characters")
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        raise SystemExit(f"{path}: {key} escapes the artifact directory")
    if not candidate.is_file():
        raise SystemExit(f"{path}: {key} is not a regular file: {candidate}")
    return candidate


primary = []
relative_paths = []
for key in (
    "module",
    "payload_package",
    "selector_package",
    "kernel_image",
    "kernel_config",
    "module_symvers",
    "build_info",
):
    relative = value.get(key)
    candidate = resolve(relative, key)
    relative_paths.append(relative)
    if key in ("module", "payload_package", "selector_package"):
        primary.append(str(candidate))

boot_busybox_relative = value.get("boot_busybox")
boot_busybox = resolve(boot_busybox_relative, "boot_busybox")
relative_paths.append(boot_busybox_relative)
primary.append(str(boot_busybox))
boot_busybox_provenance = value.get("boot_busybox_provenance")
if (
    not isinstance(boot_busybox_provenance, dict)
    or set(boot_busybox_provenance) != {"identity", "banner"}
):
    raise SystemExit(f"{path}: boot_busybox_provenance has an invalid shape")
for key, item in boot_busybox_provenance.items():
    if (
        not isinstance(item, str)
        or not item
        or item != item.strip()
        or any(ord(character) < 32 for character in item)
    ):
        raise SystemExit(
            f"{path}: boot_busybox_provenance.{key} must be printable text"
        )

module_signing = value.get("module_signing")
if module_signing is not None:
    expected_signing_keys = {
        "certificate",
        "certificate_sha256",
        "signature_id",
        "signer",
        "key",
        "hash_algorithm",
    }
    if (
        not isinstance(module_signing, dict)
        or set(module_signing) != expected_signing_keys
    ):
        raise SystemExit(f"{path}: module_signing has an invalid shape")
    certificate_relative = module_signing["certificate"]
    certificate = resolve(certificate_relative, "module_signing.certificate")
    relative_paths.append(certificate_relative)
    certificate_digest = hashlib.sha256(certificate.read_bytes()).hexdigest()
    if module_signing["certificate_sha256"] != certificate_digest:
        raise SystemExit(f"{path}: signing certificate fingerprint is incorrect")
    if module_signing["signature_id"] != "PKCS#7":
        raise SystemExit(f"{path}: signed module must use a PKCS#7 signature")
    for key in ("signer", "key", "hash_algorithm"):
        item = module_signing[key]
        if not isinstance(item, str) or not item:
            raise SystemExit(f"{path}: module_signing.{key} must not be empty")

dependencies = value.get("module_dependencies")
if not isinstance(dependencies, list):
    raise SystemExit(f"{path}: module_dependencies must be an ordered array")
for index, relative in enumerate(dependencies):
    resolve(relative, f"module_dependencies[{index}]")
    relative_paths.append(relative)

boot_modules = value.get("boot_modules")
if not isinstance(boot_modules, list):
    raise SystemExit(f"{path}: boot_modules must be an ordered array")
for index, relative in enumerate(boot_modules):
    resolve(relative, f"boot_modules[{index}]")
    relative_paths.append(relative)

if len(set(relative_paths)) != len(relative_paths):
    raise SystemExit(f"{path}: artifact paths must be distinct")
digests = value.get("sha256")
if not isinstance(digests, dict) or set(digests) != set(relative_paths):
    raise SystemExit(f"{path}: sha256 must cover every artifact path exactly")
for relative in relative_paths:
    expected_digest = digests.get(relative)
    if not isinstance(expected_digest, str) or len(expected_digest) != 64:
        raise SystemExit(f"{path}: invalid SHA-256 for {relative}")
    actual_digest = hashlib.sha256((base / relative).read_bytes()).hexdigest()
    if actual_digest != expected_digest:
        raise SystemExit(f"{path}: SHA-256 mismatch for {relative}")

build_info = resolve(value["build_info"], "build_info").read_text(
    encoding="utf-8"
)
fields = {}
for line in build_info.splitlines():
    key, separator, item = line.partition("=")
    if not separator or not key or key in fields:
        raise SystemExit(f"{path}: malformed build-info line: {line!r}")
    fields[key] = item
if fields.get("target_id") != os.environ["ZEROFS_TARGET_ID"]:
    raise SystemExit(f"{path}: build-info target_id does not match")
source_identity = value["source"].get("identity")
if fields.get("source_identity") not in (source_identity, "local-matching-host"):
    raise SystemExit(f"{path}: build-info source_identity does not match")
for provenance in ("source", "tooling"):
    commit_key = f"{provenance}_commit"
    state_key = f"{provenance}_tree_state"
    commit = value.get(commit_key)
    tree_state = value.get(state_key)
    if commit != "unknown" and not (
        isinstance(commit, str)
        and len(commit) in (40, 64)
        and all(character in "0123456789abcdef" for character in commit)
    ):
        raise SystemExit(f"{path}: {commit_key} is not a full Git object ID")
    if tree_state not in ("clean", "dirty", "unknown"):
        raise SystemExit(f"{path}: {state_key} is invalid")

print(*primary, sep="\n")
PY
)
[[ ${#artifact_paths[@]} -eq 4 ]] ||
    die "artifact manifest did not resolve the primary paths"
module=${artifact_paths[0]}
payload_package=${artifact_paths[1]}
selector_package=${artifact_paths[2]}
boot_busybox=${artifact_paths[3]}

case $family in
    deb)
        require_command dpkg-deb
        [[ "$payload_package" == *.deb && "$selector_package" == *.deb ]] ||
            die "deb target did not produce two .deb packages"
        ;;
    rpm)
        require_command cpio
        require_command rpm
        require_command rpm2cpio
        [[ "$payload_package" == *.rpm && "$selector_package" == *.rpm ]] ||
            die "rpm target did not produce two .rpm packages"
        ;;
    *)
        die "unsupported package family after validation: $family"
        ;;
esac

module_name=$(modinfo -F name "$module")
[[ "$module_name" == zerofs ]] ||
    die "artifact module name is '$module_name', expected 'zerofs'"
module_vermagic=$(modinfo -F vermagic "$module")
case $module_vermagic in
    "$kernel_release" | "$kernel_release "*) ;;
    *)
        die "module vermagic '$module_vermagic' does not target '$kernel_release'"
        ;;
esac
module_machine=$(readelf -h "$module" |
    sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
[[ "$module_machine" == "$elf_machine" ]] ||
    die "module machine '$module_machine' does not match '$canonical_arch'"
boot_busybox_machine=$(readelf -h "$boot_busybox" |
    sed -n 's/^[[:space:]]*Machine:[[:space:]]*//p')
[[ "$boot_busybox_machine" == "$elf_machine" ]] ||
    die "boot busybox machine does not match '$canonical_arch'"
if readelf -l "$boot_busybox" |
    grep -F 'Requesting program interpreter' >/dev/null; then
    die "boot busybox must be statically linked"
fi

mapfile -t signing_fields < <(
    python3 - "$artifact_dir/artifact.json" "$artifact_dir" <<'PY'
import json
import sys
from pathlib import Path

value = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
signing = value.get("module_signing")
if signing is None:
    print("unsigned")
else:
    print("signed")
    print(Path(sys.argv[2]).resolve() / signing["certificate"])
    print(signing["signature_id"])
    print(signing["signer"])
    print(signing["key"])
    print(signing["hash_algorithm"])
PY
)
signing_status=${signing_fields[0]}
module_signature_id=$(modinfo -F sig_id "$module")
if [[ "$signing_status" == signed ]]; then
    [[ ${#signing_fields[@]} -eq 6 ]] ||
        die "signed artifact has incomplete signing metadata"
    artifact_certificate=${signing_fields[1]}
    [[ "$module_signature_id" == "${signing_fields[2]}" ]] ||
        die "module signature type does not match artifact provenance"
    [[ "$(modinfo -F signer "$module")" == "${signing_fields[3]}" ]] ||
        die "module signer does not match artifact provenance"
    [[ "$(modinfo -F sig_key "$module")" == "${signing_fields[4]}" ]] ||
        die "module signature key does not match artifact provenance"
    [[ "$(modinfo -F sig_hashalgo "$module")" == "${signing_fields[5]}" ]] ||
        die "module signature hash does not match artifact provenance"
    require_command openssl
    openssl x509 -inform DER -in "$artifact_certificate" -noout
    openssl x509 \
        -inform DER \
        -in "$artifact_certificate" \
        -noout \
        -ext extendedKeyUsage |
        grep -Fq 'Code Signing' ||
        die "module-signing certificate lacks the codeSigning extended key usage"
elif [[ "$signing_status" == unsigned ]]; then
    [[ ${#signing_fields[@]} -eq 1 ]] ||
        die "unsigned artifact unexpectedly has signing metadata"
    [[ -z "$module_signature_id" ]] ||
        die "signed module is missing signing provenance and its public certificate"
    artifact_certificate=
else
    die "unknown artifact signing status: $signing_status"
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/zerofs-package-smoke.XXXXXX")
payload_root="$work_dir/payload"
selector_root="$work_dir/selector"
mkdir -p -- "$payload_root" "$selector_root"

if [[ "$family" == deb ]]; then
    payload_name=$(dpkg-deb --field "$payload_package" Package)
    selector_name=$(dpkg-deb --field "$selector_package" Package)
    payload_arch=$(dpkg-deb --field "$payload_package" Architecture)
    selector_arch=$(dpkg-deb --field "$selector_package" Architecture)
    payload_version=$(dpkg-deb --field "$payload_package" Version)
    selector_version=$(dpkg-deb --field "$selector_package" Version)
    payload_dependencies=$(dpkg-deb --field "$payload_package" Depends)
    selector_dependencies=$(dpkg-deb --field "$selector_package" Depends)
    payload_conflicts=$(dpkg-deb --field "$payload_package" Conflicts)
    selector_conflicts=$(dpkg-deb --field "$selector_package" Conflicts)
    payload_provides=$(dpkg-deb --field "$payload_package" Provides)

    [[ "$payload_arch" == "$deb_arch" && "$selector_arch" == "$deb_arch" ]] ||
        die "deb package architecture does not match '$deb_arch'"
    [[ "$payload_version" == "$selector_version" ]] ||
        die "deb payload and selector versions do not match"
    has_relation "$payload_dependencies" "$kernel_dependency" ||
        die "payload deb does not depend on exact kernel: $kernel_dependency"

    payload_control="$work_dir/payload-control"
    selector_control="$work_dir/selector-control"
    dpkg-deb --control "$payload_package" "$payload_control"
    dpkg-deb --control "$selector_package" "$selector_control"
    payload_install_script=$(<"$payload_control/postinst")
    payload_remove_path=$payload_control/postrm
    payload_remove_script=$(<"$payload_remove_path")
    selector_install_script=$(<"$selector_control/postinst")
    sh -n \
        "$payload_control/postinst" \
        "$payload_remove_path" \
        "$selector_control/postinst"
    dpkg-deb --extract "$payload_package" "$payload_root"
    dpkg-deb --extract "$selector_package" "$selector_root"
else
    payload_name=$(rpm -qp --queryformat '%{NAME}\n' "$payload_package")
    selector_name=$(rpm -qp --queryformat '%{NAME}\n' "$selector_package")
    payload_arch=$(rpm -qp --queryformat '%{ARCH}\n' "$payload_package")
    selector_arch=$(rpm -qp --queryformat '%{ARCH}\n' "$selector_package")
    payload_version=$(rpm -qp --queryformat '%{VERSION}-%{RELEASE}\n' "$payload_package")
    selector_version=$(rpm -qp --queryformat '%{VERSION}-%{RELEASE}\n' "$selector_package")
    payload_dependencies=$(rpm -qp --requires "$payload_package")
    selector_dependencies=$(rpm -qp --requires "$selector_package")
    payload_conflicts=$(rpm -qp --conflicts "$payload_package")
    selector_conflicts=$(rpm -qp --conflicts "$selector_package")
    payload_provides=$(rpm -qp --provides "$payload_package")
    payload_install_script=$(rpm -qp --scripts "$payload_package")
    payload_remove_script=$payload_install_script
    selector_install_script=$(rpm -qp --scripts "$selector_package")

    [[ "$payload_arch" == "$rpm_arch" && "$selector_arch" == "$rpm_arch" ]] ||
        die "rpm package architecture does not match '$rpm_arch'"
    [[ "$payload_version" == "$selector_version" ]] ||
        die "rpm payload and selector versions do not match"
    printf '%s\n' "$payload_dependencies" | grep -Fx -- "$kernel_dependency" >/dev/null ||
        die "payload rpm does not depend on exact kernel: $kernel_dependency"

    payload_archive="$work_dir/payload.cpio"
    selector_archive="$work_dir/selector.cpio"
    rpm2cpio "$payload_package" >"$payload_archive"
    rpm2cpio "$selector_package" >"$selector_archive"
    (
        cd "$payload_root"
        cpio -idm --quiet <"$payload_archive"
    )
    (
        cd "$selector_root"
        cpio -idm --quiet <"$selector_archive"
    )
fi
[[ "$selector_version" == "$expected_package_version" ]] ||
    die "selector version '$selector_version' does not match target revision"
[[ "$selector_name" == zerofs-kernel-client ]] ||
    die "unexpected selector package name: $selector_name"
if [[ "$family" == deb ]]; then
    selector_dependency="$payload_name (= $payload_version)"
    [[ "$kernel_upgrade_conflict" != null ]] ||
        die "deb target omits its kernel upgrade conflict"
else
    selector_dependency="$payload_name = $payload_version"
    [[ "$kernel_upgrade_conflict" != null ]] ||
        die "rpm target omits its kernel upgrade conflict"
fi
has_relation "$selector_conflicts" "$kernel_upgrade_conflict" ||
    die "selector does not block newer kernels: $kernel_upgrade_conflict"
if has_relation "$payload_conflicts" "$kernel_upgrade_conflict"; then
    die "kernel upgrade conflict must belong to the selector package"
fi
has_relation "$selector_dependencies" "$selector_dependency" ||
    die "selector package does not pin payload package: $selector_dependency"
printf '%s\n' "$selector_install_script" | grep -F 'modprobe zerofs' >/dev/null ||
    die "selector package does not load zerofs after installation"
printf '%s\n' "$selector_install_script" | grep -F 'uname -r' >/dev/null ||
    die "selector package does not limit install-time loading to the running kernel"
if printf '%s\n' "$payload_remove_script" |
    grep -E '(^|[[:space:]])(rmmod|modprobe[[:space:]]+-r)([[:space:]]|$)' \
        >/dev/null; then
    die "payload package must not unload a potentially active module"
fi

payload_provenance="$payload_root/usr/share/doc/$payload_name/provenance.json"
selector_provenance="$selector_root/usr/share/doc/$selector_name/provenance.json"
modules_load="$selector_root/usr/lib/modules-load.d/zerofs.conf"
packaged_certificate="$selector_root/usr/share/zerofs/zerofs-module-signing-cert.der"

[[ -f "$payload_provenance" ]] ||
    die "payload package omits provenance: $payload_provenance"
[[ -f "$selector_provenance" ]] ||
    die "selector package omits provenance: $selector_provenance"
[[ -f "$modules_load" ]] ||
    die "selector package omits modules-load configuration"
[[ $(stat -c '%a' "$modules_load") == 644 ]] ||
    die "modules-load configuration mode must be 0644"
printf 'zerofs\n' >"$work_dir/expected-zerofs.conf"
cmp -s "$work_dir/expected-zerofs.conf" "$modules_load" ||
    die "modules-load configuration must contain only 'zerofs'"

[[ "$payload_name" == zerofs-kernel-client-* ]] ||
    die "unexpected payload package name: $payload_name"
[[ "$payload_name" =~ ^zerofs-kernel-client-[A-Za-z0-9._+-]+$ ]] ||
    die "payload package name contains unsupported characters: $payload_name"
if [[ "$family" == deb ]]; then
    payload_module_provide="zerofs-kernel-module (= $payload_version)"
else
    payload_module_provide="zerofs-kernel-module = $payload_version"
fi
has_relation "$payload_provides" "$payload_module_provide" ||
    die "payload package must provide zerofs-kernel-module"
printf '%s\n' "$payload_install_script" | grep -F 'depmod' >/dev/null ||
    die "payload package does not refresh dependencies after installation"
if printf '%s\n' "$payload_install_script" |
    grep -F 'modprobe zerofs' >/dev/null; then
    die "co-installable payload package must leave loading to the selector"
fi
printf '%s\n' "$selector_install_script" |
    grep -F 'mokutil --import' >/dev/null ||
    die "selector package does not explain MOK enrollment after load failure"
printf '%s\n' "$payload_remove_script" | grep -F 'depmod' >/dev/null ||
    die "payload package does not refresh dependencies after removal"

packaged_module="$payload_root/lib/modules/$kernel_release/updates/zerofs/zerofs.ko"
[[ -f "$packaged_module" ]] ||
    die "payload package omits exact-release module path: $packaged_module"
[[ $(stat -c '%a' "$packaged_module") == 644 ]] ||
    die "packaged module mode must be 0644"
if [[ "$signing_status" == signed ]]; then
    [[ -f "$packaged_certificate" ]] ||
        die "signed selector package omits its public certificate"
    [[ $(stat -c '%a' "$packaged_certificate") == 644 ]] ||
        die "packaged signing certificate mode must be 0644"
    cmp -s "$artifact_certificate" "$packaged_certificate" ||
        die "packaged certificate differs from artifact certificate"
    openssl x509 -inform DER -in "$packaged_certificate" -noout
else
    [[ ! -e "$packaged_certificate" ]] ||
        die "unsigned selector package contains a signing certificate"
fi
if find "$payload_root" -type f -name 'zerofs-module-signing-cert.der' \
    -print -quit | grep . >/dev/null; then
    die "co-installable payload package owns the shared signing certificate"
fi
mapfile -t packaged_modules < <(
    find "$payload_root" -type f -name '*.ko' -print
)
[[ ${#packaged_modules[@]} -eq 1 ]] ||
    die "payload package must contain exactly one kernel module"
if find "$selector_root" -type f -name '*.ko' -print -quit |
    grep . >/dev/null; then
    die "selector package must not contain a kernel module"
fi
cmp -s "$module" "$packaged_module" ||
    die "packaged module differs from artifact module"

export ZEROFS_CANONICAL_ARCH=$canonical_arch
export ZEROFS_PAYLOAD_NAME=$payload_name
export ZEROFS_SELECTOR_NAME=$selector_name
export ZEROFS_PAYLOAD_VERSION=$payload_version
export ZEROFS_SELECTOR_VERSION=$selector_version
export ZEROFS_KERNEL_DEPENDENCY=$kernel_dependency
export ZEROFS_KERNEL_UPGRADE_CONFLICT=$kernel_upgrade_conflict
export ZEROFS_CHANNEL_ID=$channel_id
export ZEROFS_MODULE=$packaged_module
python3 - \
    "$payload_provenance" \
    "$selector_provenance" \
    "$artifact_dir/artifact.json" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path


def read(path_value):
    path = Path(path_value)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"{path}: invalid provenance: {error}")
    if not isinstance(value, dict):
        raise SystemExit(f"{path}: provenance must be an object")
    return path, value


payload_path, payload = read(sys.argv[1])
selector_path, selector = read(sys.argv[2])
artifact_path, artifact = read(sys.argv[3])
expected_target = {
    "id": os.environ["ZEROFS_TARGET_ID"],
    "channel_id": os.environ["ZEROFS_CHANNEL_ID"],
    "kernel_release": os.environ["ZEROFS_KERNEL_RELEASE"],
    "architecture": os.environ["ZEROFS_CANONICAL_ARCH"],
}
for path, value in ((payload_path, payload), (selector_path, selector)):
    target = value.get("target")
    if not isinstance(target, dict):
        raise SystemExit(f"{path}: target provenance is missing")
    for key, expected in expected_target.items():
        if target.get(key) != expected:
            raise SystemExit(
                f"{path}: target.{key} is {target.get(key)!r}, expected {expected!r}"
            )

if payload.get("artifact_kind") != "zerofs-kernel-module":
    raise SystemExit(f"{payload_path}: incorrect artifact_kind")
if selector.get("artifact_kind") != "zerofs-kernel-client-selector":
    raise SystemExit(f"{selector_path}: incorrect artifact_kind")
if (
    payload.get("zerofs", {}).get("version")
    != os.environ["ZEROFS_ZEROFS_VERSION"]
):
    raise SystemExit(f"{payload_path}: ZeroFS source version is incorrect")
if payload.get("package", {}).get("name") != os.environ["ZEROFS_PAYLOAD_NAME"]:
    raise SystemExit(
        f"{payload_path}: payload package name does not match native metadata"
    )
if selector.get("package", {}).get("name") != os.environ["ZEROFS_SELECTOR_NAME"]:
    raise SystemExit(
        f"{selector_path}: selector package name does not match native metadata"
    )
for path, value, full_version in (
    (payload_path, payload, os.environ["ZEROFS_PAYLOAD_VERSION"]),
    (selector_path, selector, os.environ["ZEROFS_SELECTOR_VERSION"]),
):
    package = value.get("package")
    if not isinstance(package, dict):
        raise SystemExit(f"{path}: package provenance is missing")
    recorded_version = f"{package.get('version')}-{package.get('revision')}"
    if recorded_version != full_version:
        raise SystemExit(f"{path}: package version does not match native metadata")
if payload["package"].get("license") != selector["package"].get("license"):
    raise SystemExit(
        f"{selector_path}: package license differs from the payload package"
    )
if payload.get("source_date_epoch") != selector.get("source_date_epoch"):
    raise SystemExit(f"{selector_path}: build timestamp differs from the payload package")

dependency = selector.get("dependency")
if not isinstance(dependency, dict):
    raise SystemExit(f"{selector_path}: selector dependency provenance is missing")
if dependency.get("name") != os.environ["ZEROFS_PAYLOAD_NAME"]:
    raise SystemExit(f"{selector_path}: selector dependency name is incorrect")
if dependency.get("version") != os.environ["ZEROFS_PAYLOAD_VERSION"]:
    raise SystemExit(f"{selector_path}: selector dependency version is incorrect")

expected_source = {
    "git_commit": artifact.get("source_commit"),
    "tree_state": artifact.get("source_tree_state"),
}
expected_tooling = {
    "git_commit": artifact.get("tooling_commit"),
    "tree_state": artifact.get("tooling_tree_state"),
}
for path, value in ((payload_path, payload), (selector_path, selector)):
    if value.get("source") != expected_source:
        raise SystemExit(f"{path}: source provenance does not match artifact")

for path, value in ((payload_path, payload), (selector_path, selector)):
    if value.get("tooling") != expected_tooling:
        raise SystemExit(
            f"{path}: tooling provenance does not match artifact"
        )

if (
    payload["target"].get("kernel_package_dependency")
    != os.environ["ZEROFS_KERNEL_DEPENDENCY"]
):
    raise SystemExit(
        f"{payload_path}: exact kernel dependency provenance is incorrect"
    )
expected_upgrade_conflict = os.environ["ZEROFS_KERNEL_UPGRADE_CONFLICT"]
if selector.get("kernel_upgrade_conflict") != (
    None
    if expected_upgrade_conflict == "null"
    else expected_upgrade_conflict
):
    raise SystemExit(
        f"{selector_path}: kernel upgrade conflict provenance is incorrect"
    )

artifact_signing = artifact.get("module_signing")
if artifact_signing is None:
    expected_signing = None
else:
    expected_signing = {
        "certificate_path": (
            "/usr/share/zerofs/zerofs-module-signing-cert.der"
        ),
        "certificate_sha256": artifact_signing.get(
            "certificate_sha256"
        ),
        "signature_id": artifact_signing.get("signature_id"),
        "signer": artifact_signing.get("signer"),
        "key": artifact_signing.get("key"),
        "hash_algorithm": artifact_signing.get("hash_algorithm"),
    }
for path, value in ((payload_path, payload), (selector_path, selector)):
    if value.get("module_signing") != expected_signing:
        raise SystemExit(
            f"{path}: module signing provenance does not match artifact"
        )

module_path = Path(os.environ["ZEROFS_MODULE"])
digest = hashlib.sha256(module_path.read_bytes()).hexdigest()
module = payload.get("module")
if not isinstance(module, dict):
    raise SystemExit(f"{payload_path}: module provenance is missing")
if module.get("name") != "zerofs":
    raise SystemExit(f"{payload_path}: module name is not zerofs")
if module.get("sha256") != digest:
    raise SystemExit(
        f"{payload_path}: module digest does not match package payload"
    )
vermagic = module.get("vermagic")
release = os.environ["ZEROFS_KERNEL_RELEASE"]
if not isinstance(vermagic, str) or not (
    vermagic == release or vermagic.startswith(release + " ")
):
    raise SystemExit(
        f"{payload_path}: module vermagic does not match target release"
    )
PY

echo "package smoke passed for $target_id ($kernel_release, $family, $canonical_arch)"
