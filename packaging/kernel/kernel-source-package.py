#!/usr/bin/env python3

"""Build architecture-independent ZeroFS kernel-client packages."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


PACKAGE_NAME = "zerofs-kernel-client"
PACKAGE_LICENSE = "AGPL-3.0"
# Repository packages are immutable. An old release may therefore be repacked
# with newer packaging tooling only under an explicit higher revision.
PACKAGE_REVISION_OVERRIDES = {"2.2.3": 2}
DKMS_MODULE = "zerofs"
FAMILY_ARCHITECTURES = {"deb": "all", "rpm": "noarch"}
VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+~:-]*$")

DEB_DEPENDENCIES = (
    "dkms (>= 3.0.11)",
    "kmod",
    "ca-certificates",
    "curl",
    "python3",
    "openssl",
    "xz-utils",
)
RPM_DEPENDENCIES = (
    "dkms >= 3.0.11",
    "kmod",
    "ca-certificates",
    "curl",
    "python3",
    "openssl",
    "xz",
)


class PackageError(ValueError):
    """The requested source package cannot be built safely."""


def fail(message: str) -> None:
    raise PackageError(message)


def yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def repository_version(source_root: Path) -> str:
    manifest = source_root / "zerofs/Cargo.toml"
    try:
        with manifest.open("rb") as source:
            document = tomllib.load(source)
        version = document["workspace"]["package"]["version"]
    except (KeyError, OSError, TypeError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot read the ZeroFS version from {manifest}: {error}")
    if not isinstance(version, str) or not VERSION_PATTERN.fullmatch(version):
        fail(f"{manifest}: workspace package version must be stable X.Y.Z")
    return version


def source_date_epoch(source_root: Path) -> int:
    value = os.environ.get("SOURCE_DATE_EPOCH")
    if value is None:
        result = subprocess.run(
            ["git", "-C", str(source_root), "show", "-s", "--format=%ct", "HEAD"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode:
            detail = result.stderr.strip()
            suffix = f": {detail}" if detail else ""
            fail(f"cannot derive SOURCE_DATE_EPOCH from HEAD{suffix}")
        value = result.stdout.strip()
    if not value.isdigit():
        fail("SOURCE_DATE_EPOCH must be an unsigned integer")
    return int(value)


def package_revision(version: str) -> int:
    return PACKAGE_REVISION_OVERRIDES.get(version, 1)


def package_version(version: str) -> str:
    return f"{version}-{package_revision(version)}"


def package_filename(family: str, version: str) -> str:
    architecture = FAMILY_ARCHITECTURES[family]
    if family == "deb":
        return f"{PACKAGE_NAME}_{package_version(version)}_{architecture}.deb"
    return f"{PACKAGE_NAME}-{package_version(version)}.{architecture}.rpm"


def dkms_configuration(version: str) -> str:
    if SAFE_TOKEN.fullmatch(version) is None:
        fail(f"unsupported DKMS version: {version!r}")
    return "\n".join(
        (
            f'PACKAGE_NAME="{DKMS_MODULE}"',
            f'PACKAGE_VERSION="{version}"',
            'BUILD_EXCLUSIVE_KERNEL_MIN="6.18"',
            '# DKMS 3.0.11 ignores PRE_BUILD failures; run the wrapper as MAKE.',
            f'MAKE[0]="./dkms-build $kernelver {version}"',
            '# Prebuilt installs do not need make, so suppress DKMS\'s default clean.',
            'CLEAN="/bin/true"',
            'BUILT_MODULE_NAME[0]="zerofs"',
            'BUILT_MODULE_LOCATION[0]="dkms-output"',
            'DEST_MODULE_LOCATION[0]="/kernel/fs/zerofs"',
            'NO_WEAK_MODULES="yes"',
            'STRIP[0]="no"',
            'AUTOINSTALL="yes"',
            "",
        )
    )


def render_script(template: Path, output: Path, version: str) -> None:
    text = template.read_text(encoding="utf-8")
    marker = "@DKMS_VERSION@"
    if text.count(marker) != 1:
        fail(f"{template}: expected exactly one {marker} marker")
    output.write_text(text.replace(marker, version), encoding="utf-8")
    output.chmod(0o755)


def validate_source_root(source_root: Path) -> Path:
    try:
        root = source_root.resolve(strict=True)
    except OSError as error:
        fail(f"cannot resolve source root {source_root}: {error}")
    required = (
        root / "LICENSE",
        root / "kernel/stage-module-source.sh",
        root / "kernel/Makefile",
        root / "kernel/Kbuild",
    )
    for path in required:
        if not path.is_file() or path.is_symlink():
            fail(f"source checkout is missing a regular {path.relative_to(root)}")
    return root


def stage_source_tree(
    source_root: Path,
    destination: Path,
    version: str,
) -> None:
    subprocess.run(
        [str(source_root / "kernel/stage-module-source.sh"), str(destination)],
        cwd=source_root,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    scripts = Path(__file__).with_name("scripts")
    for source_name, destination_name in (
        ("dkms-build.sh", "dkms-build"),
        ("dkms-fetch-module.sh", "dkms-fetch-module"),
        ("dkms-find-kernel-source.sh", "dkms-find-kernel-source"),
    ):
        output = destination / destination_name
        shutil.copyfile(scripts / source_name, output)
        output.chmod(0o755)
    signing_certificate = Path(__file__).with_name(
        "zerofs-module-signing-cert.pem"
    )
    certificate_destination = destination / signing_certificate.name
    shutil.copyfile(signing_certificate, certificate_destination)
    certificate_destination.chmod(0o644)
    (destination / "zerofs-module-layout").write_text("v1\n", encoding="ascii")
    configuration = destination / "dkms.conf"
    configuration.write_text(
        dkms_configuration(version),
        encoding="utf-8",
    )
    configuration.chmod(0o644)


def validate_source_tree(source: Path) -> None:
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            fail(f"source package contains a symbolic link: {path}")
        if not path.is_file() and not path.is_dir():
            fail(f"source package contains a non-regular file: {path}")


def normalize_staging_timestamps(root: Path, epoch: int) -> None:
    timestamp = epoch * 1_000_000_000
    paths = sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for path in paths:
        if path.is_symlink():
            fail(f"package staging tree contains a symbolic link: {path}")
        os.utime(path, ns=(timestamp, timestamp), follow_symlinks=False)
    os.utime(root, ns=(timestamp, timestamp), follow_symlinks=False)


def write_nfpm_configuration(
    path: Path,
    *,
    family: str,
    version: str,
    dkms_version: str,
) -> None:
    dependencies = DEB_DEPENDENCIES if family == "deb" else RPM_DEPENDENCIES
    source_destination = f"/usr/src/{DKMS_MODULE}-{dkms_version}"
    lines = [
        f"name: {yaml_string(PACKAGE_NAME)}",
        f"arch: {yaml_string(FAMILY_ARCHITECTURES[family])}",
        "platform: linux",
        f"version: {yaml_string(version)}",
        f"release: {package_revision(version)}",
        "section: kernel",
        "priority: optional",
        "maintainer: Pierre Barre <pierre@zerofs.net>",
        "description: |",
        "  ZeroFS native kernel client managed by DKMS.",
        "  Installs verified exact-kernel modules when published and retains",
        "  a best-effort source-build fallback.",
        "vendor: ZeroFS",
        "homepage: https://www.zerofs.net",
        f"license: {yaml_string(PACKAGE_LICENSE)}",
        "depends:",
    ]
    lines.extend(f"  - {yaml_string(value)}" for value in dependencies)
    lines.extend(
        (
            "contents:",
            "  - src: ./content/source",
            f"    dst: {yaml_string(source_destination)}",
            "    type: tree",
        )
    )
    lines.extend(("scripts:", "  preremove: ./scripts/preremove.sh"))
    if family == "deb":
        lines.append("  postinstall: ./scripts/postinstall.sh")
    lines.extend((f"{family}:", "  compression: zstd"))
    if family == "rpm":
        lines.extend(("  scripts:", "    posttrans: ./scripts/postinstall.sh"))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def build_family_package(
    *,
    family: str,
    source_root: Path,
    version: str,
    epoch: int,
    output_directory: Path,
) -> Path:
    if family not in FAMILY_ARCHITECTURES:
        fail(f"unsupported package family: {family!r}")
    dkms_version = package_version(version)
    filename = package_filename(family, version)
    output_directory.mkdir(parents=True, exist_ok=True)
    destination = output_directory / filename
    if destination.exists():
        fail(f"refusing to overwrite {destination}")

    script_directory = Path(__file__).with_name("scripts")
    with tempfile.TemporaryDirectory(prefix="zerofs-source-package.") as temporary:
        root = Path(temporary)
        content = root / "content"
        source = content / "source"
        scripts = root / "scripts"
        content.mkdir()
        scripts.mkdir()
        stage_source_tree(source_root, source, dkms_version)
        (source / ".zerofs-module-source").unlink()
        render_script(
            script_directory / "source-postinstall.sh.in",
            scripts / "postinstall.sh",
            dkms_version,
        )
        render_script(
            script_directory / "source-preremove.sh.in",
            scripts / "preremove.sh",
            dkms_version,
        )
        validate_source_tree(source)
        write_nfpm_configuration(
            root / "nfpm.yaml",
            family=family,
            version=version,
            dkms_version=dkms_version,
        )
        normalize_staging_timestamps(root, epoch)

        built = root / filename
        completed = subprocess.run(
            [
                "nfpm",
                "pkg",
                "-f",
                str(root / "nfpm.yaml"),
                "-p",
                family,
                "-t",
                str(built),
            ],
            cwd=root,
            env={**os.environ, "SOURCE_DATE_EPOCH": str(epoch)},
            check=False,
            stdout=subprocess.PIPE,
            text=True,
        )
        if completed.stdout:
            sys.stderr.write(completed.stdout)
        completed.check_returncode()
        built.chmod(0o644)
        shutil.move(built, destination)
    return destination


def build_source_packages(
    *,
    output_directory: Path,
    source_root: Path | None = None,
) -> None:
    if shutil.which("nfpm") is None:
        fail("nfpm is required to build kernel client packages")
    root = validate_source_root(source_root or repository_root())
    version = repository_version(root)
    epoch = source_date_epoch(root)
    if output_directory.is_symlink():
        fail(f"--output must not be a symbolic link: {output_directory}")
    output_directory.mkdir(parents=True, exist_ok=True)
    if any(output_directory.iterdir()):
        fail(f"--output must be empty: {output_directory}")

    for family in FAMILY_ARCHITECTURES:
        build_family_package(
            family=family,
            source_root=root,
            version=version,
            epoch=epoch,
            output_directory=output_directory / family,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build ZeroFS kernel-client repository packages.",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source-root",
        type=Path,
        help="release source checkout; defaults to the repository root",
    )
    arguments = parser.parse_args()
    try:
        build_source_packages(
            output_directory=arguments.output,
            source_root=arguments.source_root,
        )
    except (PackageError, OSError, subprocess.CalledProcessError) as error:
        print(f"kernel-source-package.py: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
