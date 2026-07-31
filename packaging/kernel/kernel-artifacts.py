#!/usr/bin/env python3

"""Validate built kernel artifacts and stage packages by repository channel."""

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path, PurePosixPath
from typing import Any

from kernel_targets.catalog import Catalog, ManifestError, load_catalog


COMMIT_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
ARCHITECTURES = {
    "x86_64": {"deb": "amd64", "rpm": "x86_64"},
    "aarch64": {"deb": "arm64", "rpm": "aarch64"},
}
SELECTOR_PACKAGE = "zerofs-kernel-client"
CERTIFICATE_ASSET = "zerofs-module-signing-cert.der"
FINGERPRINT_ASSET = "zerofs-module-signing-cert.fingerprint"
ARTIFACT_FILE_FIELDS = (
    "module",
    "payload_package",
    "selector_package",
    "kernel_image",
    "kernel_config",
    "module_symvers",
    "build_info",
    "boot_busybox",
)
ARTIFACT_FILE_LIST_FIELDS = (
    "module_dependencies",
    "boot_modules",
)


class ArtifactError(ValueError):
    pass


def fail(message: str) -> None:
    raise ArtifactError(message)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_file(root: Path, relative: Any, label: str) -> Path:
    """Resolve a path from artifact.json without leaving the artifact tree."""
    if not isinstance(relative, str) or not relative:
        fail(f"{label}: must be a non-empty relative path")
    if "\\" in relative or relative != relative.strip():
        fail(f"{label}: contains unsupported path characters")
    pure = PurePosixPath(relative)
    if (
        pure.is_absolute()
        or str(pure) != relative
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        fail(f"{label}: must be a normalized relative path")
    path = root
    for part in pure.parts:
        path /= part
        if path.is_symlink():
            fail(f"{label}: symbolic links are not allowed")
    if not path.is_file():
        fail(f"{label}: is not a regular file")
    try:
        path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError):
        fail(f"{label}: escapes the artifact directory")
    return path


def package_slug(value: str) -> str:
    translated = value.translate(
        str.maketrans(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ_",
            "abcdefghijklmnopqrstuvwxyz-",
        )
    )
    return re.sub(r"-+", "-", translated).strip("-")


def expected_package_filenames(
    target: dict[str, Any],
    version: str,
) -> tuple[str, str]:
    target_slug = package_slug(target["id"])
    kernel_slug = package_slug(target["kernel_release"])
    full_version = f"{version}-{target['package_revision']}"
    architecture = ARCHITECTURES[target["arch"]][target["family"]]
    payload = f"{SELECTOR_PACKAGE}-{target_slug}-{kernel_slug}"
    if target["family"] == "deb":
        return (
            f"{payload}_{full_version}_{architecture}.deb",
            f"{SELECTOR_PACKAGE}_{full_version}_{target_slug}"
            f"_{kernel_slug}_{architecture}.deb",
        )
    return (
        f"{payload}-{full_version}.{architecture}.rpm",
        f"{SELECTOR_PACKAGE}-{full_version}.{target_slug}"
        f".{kernel_slug}.{architecture}.rpm",
    )


def validate_binding(
    artifact: Any,
    target: dict[str, Any],
    version: str,
    source_commit: str,
    tooling_commit: str,
) -> None:
    if not isinstance(artifact, dict):
        fail("artifact.json: top level must be an object")
    if (
        type(artifact.get("schema_version")) is not int
        or artifact["schema_version"] != 2
    ):
        fail("artifact.json: schema_version must be 2")
    expected = {
        "target_id": target["id"],
        "kernel_release": target["kernel_release"],
        "kernel_package_version": target["kernel_package_version"],
        "kernel_selector_version": target["kernel_selector_version"],
        "channel_id": target["channel_id"],
        "package_revision": target["package_revision"],
        "zerofs_version": version,
        "family": target["family"],
        "arch": target["arch"],
        "builder_image": target["builder_image"],
        "source": target["source"],
        "source_commit": source_commit,
        "source_tree_state": "clean",
        "tooling_commit": tooling_commit,
        "tooling_tree_state": "clean",
    }
    for field, value in expected.items():
        if artifact.get(field) != value:
            fail(
                f"artifact.{field}: expected {value!r}, "
                f"found {artifact.get(field)!r}"
            )


def artifact_paths(artifact: dict[str, Any]) -> list[str]:
    paths = []
    for field in ARTIFACT_FILE_FIELDS:
        relative = artifact.get(field)
        if not isinstance(relative, str) or not relative:
            fail(f"artifact.{field}: must be a non-empty relative path")
        paths.append(relative)
    for field in ARTIFACT_FILE_LIST_FIELDS:
        values = artifact.get(field)
        if not isinstance(values, list):
            fail(f"artifact.{field}: must be an array")
        for index, relative in enumerate(values):
            if not isinstance(relative, str) or not relative:
                fail(
                    f"artifact.{field}[{index}]: "
                    "must be a non-empty relative path"
                )
            paths.append(relative)

    signing = artifact.get("module_signing")
    if not isinstance(signing, dict):
        fail("artifact.module_signing: release artifact must be signed")
    certificate = signing.get("certificate")
    if not isinstance(certificate, str) or not certificate:
        fail(
            "artifact.module_signing.certificate: "
            "must be a non-empty relative path"
        )
    paths.append(certificate)
    if len(set(paths)) != len(paths):
        fail("artifact paths must be distinct")
    return paths


def verify_digests(
    root: Path,
    artifact: dict[str, Any],
    paths: list[str],
) -> None:
    digests = artifact.get("sha256")
    if not isinstance(digests, dict) or not digests:
        fail("artifact.sha256: must be a non-empty object")
    if any(not isinstance(relative, str) for relative in digests):
        fail("artifact.sha256: file names must be strings")
    if set(digests) != set(paths):
        fail("artifact.sha256: must cover every artifact path exactly")
    for relative in sorted(paths):
        expected = digests[relative]
        if not isinstance(expected, str) or not DIGEST_PATTERN.fullmatch(expected):
            fail(f"artifact.sha256.{relative}: must be a lowercase SHA-256 digest")
        path = artifact_file(root, relative, f"artifact.sha256.{relative}")
        if sha256_file(path) != expected:
            fail(f"artifact.sha256.{relative}: file digest does not match")


def signing_certificate(root: Path, artifact: dict[str, Any]) -> Path:
    signing = artifact.get("module_signing")
    if not isinstance(signing, dict):
        fail("artifact.module_signing: release artifact must be signed")
    if signing.get("signature_id") != "PKCS#7":
        fail("artifact.module_signing.signature_id: expected 'PKCS#7'")
    relative = signing.get("certificate")
    path = artifact_file(root, relative, "artifact.module_signing.certificate")
    if path.name != CERTIFICATE_ASSET:
        fail(f"artifact.module_signing.certificate: expected {CERTIFICATE_ASSET}")
    if signing.get("certificate_sha256") != artifact.get("sha256", {}).get(relative):
        fail("artifact.module_signing.certificate: digest records disagree")
    return path


def package_path(
    root: Path,
    artifact: dict[str, Any],
    field: str,
    expected_filename: str,
) -> Path:
    path = artifact_file(root, artifact.get(field), f"artifact.{field}")
    if path.name != expected_filename:
        fail(
            f"artifact.{field}: expected {expected_filename!r}, "
            f"found {path.name!r}"
        )
    return path


def channel_entry(
    catalog: Catalog,
    target: dict[str, Any],
    version: str,
) -> dict[str, Any]:
    channel = catalog.channels[target["channel_id"]]
    prefix = channel["prefix"]
    # A GitHub Actions matrix needs the same keys in every entry, so the fields
    # the other family ignores are present but empty.
    if target["family"] == "deb":
        specific = {
            "codename": channel["apt"]["codename"],
            "component": channel["apt"]["component"],
            "architectures": ARCHITECTURES[target["arch"]]["deb"],
            "probe_version": f"{version}-{target['package_revision']}",
            "descriptor_key": f"{prefix}/zerofs-kernel.list",
            "descriptor_id": "",
            "descriptor_name": "",
        }
    else:
        specific = {
            "codename": "stable",
            "component": "main",
            "architectures": ARCHITECTURES[target["arch"]]["rpm"],
            "probe_version": f"{version}-{target['package_revision']}",
            "descriptor_key": f"{prefix}/zerofs-kernel.repo",
            "descriptor_id": channel["rpm"]["repo_id"],
            "descriptor_name": f"ZeroFS kernel client ({channel['id']})",
        }
    return {
        "id": target["id"],
        "family": target["family"],
        "prefix": prefix,
        "probe_package": SELECTOR_PACKAGE,
        **specific,
    }


def stage_target(
    catalog: Catalog,
    target_id: str,
    directory: Path,
    version: str,
    source_commit: str,
    tooling_commit: str,
    output: Path,
) -> tuple[dict[str, Any], Path]:
    target = catalog.targets_by_id.get(target_id)
    if target is None:
        fail(f"unknown target id: {target_id}")
    if not target["publish"]:
        fail(f"target is not authorized for publication: {target_id}")
    if directory.is_symlink() or not directory.is_dir():
        fail(f"{directory}: artifact directory must be a regular directory")
    root = directory.resolve(strict=True)

    manifest = artifact_file(root, "artifact.json", "artifact.json")
    try:
        artifact = json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        fail(f"{manifest}: invalid JSON: {error}")
    validate_binding(
        artifact,
        target,
        version,
        source_commit,
        tooling_commit,
    )
    verify_digests(root, artifact, artifact_paths(artifact))
    certificate = signing_certificate(root, artifact)

    payload_filename, selector_filename = expected_package_filenames(target, version)
    packages = (
        package_path(root, artifact, "payload_package", payload_filename),
        package_path(root, artifact, "selector_package", selector_filename),
    )

    staged = output / target_id
    staged.mkdir(parents=True)
    for package in packages:
        shutil.copyfile(package, staged / package.name)
    return channel_entry(catalog, target, version), certificate


def artifact_directories(values: list[str]) -> dict[str, Path]:
    if not values:
        fail("at least one --artifact TARGET=DIRECTORY is required")
    result: dict[str, Path] = {}
    for value in values:
        target_id, separator, directory = value.partition("=")
        if not separator or not target_id or not directory:
            fail(f"invalid --artifact value: {value!r}")
        if target_id in result:
            fail(f"duplicate artifact target: {target_id}")
        result[target_id] = Path(directory)
    return result


def stage(arguments: argparse.Namespace) -> int:
    if not VERSION_PATTERN.fullmatch(arguments.version):
        fail("--version must be a stable X.Y.Z version")
    if not COMMIT_PATTERN.fullmatch(arguments.source_commit):
        fail("--source-commit must be a lowercase full Git object ID")
    if not COMMIT_PATTERN.fullmatch(arguments.tooling_commit):
        fail("--tooling-commit must be a lowercase full Git object ID")
    catalog = load_catalog(arguments.manifest)
    directories = artifact_directories(arguments.artifact)
    output = arguments.output
    output.mkdir(parents=True)

    entries = []
    certificate_digest = None
    for target_id in sorted(directories):
        entry, certificate = stage_target(
            catalog,
            target_id,
            directories[target_id],
            arguments.version,
            arguments.source_commit,
            arguments.tooling_commit,
            output,
        )
        entries.append(entry)
        digest = sha256_file(certificate)
        if certificate_digest is None:
            certificate_digest = digest
            shutil.copyfile(certificate, output / CERTIFICATE_ASSET)
        elif digest != certificate_digest:
            fail("publication targets use different module-signing certificates")

    # openssl x509 -fingerprint -sha256 digests the same DER bytes.
    fingerprint = ":".join(
        certificate_digest[index : index + 2].upper()
        for index in range(0, 64, 2)
    )
    (output / FINGERPRINT_ASSET).write_text(f"{fingerprint}\n", encoding="ascii")

    print(json.dumps({"include": entries}, separators=(",", ":"), sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate and stage ZeroFS kernel packages for publication.",
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--tooling-commit", required=True)
    parser.add_argument(
        "--artifact",
        action="append",
        default=[],
        metavar="TARGET=DIRECTORY",
    )
    parser.add_argument("--output", type=Path, required=True)
    try:
        return stage(parser.parse_args())
    except (ArtifactError, ManifestError, OSError) as error:
        print(f"kernel-artifacts.py: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
