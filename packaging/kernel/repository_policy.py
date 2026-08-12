#!/usr/bin/env python3
"""Enforce monotonic, immutable package publication."""

import argparse
import re
import sys
from pathlib import Path


PACKAGE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.+-]*$")
ARCHITECTURE_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)-([1-9]\d*)$")


class PolicyError(ValueError):
    """A repository update violates the publication policy."""


def package_name(value: str) -> str:
    if PACKAGE_PATTERN.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(f"invalid package name: {value!r}")
    return value


def architecture(value: str) -> str:
    if ARCHITECTURE_PATTERN.fullmatch(value) is None:
        raise argparse.ArgumentTypeError(f"invalid architecture: {value!r}")
    return value


def sha256(value: str) -> str:
    normalized = value.lower()
    if SHA256_PATTERN.fullmatch(normalized) is None:
        raise argparse.ArgumentTypeError(f"invalid SHA-256 digest: {value!r}")
    return normalized


def parse_version(value: str) -> tuple[tuple[int, int, int], int]:
    match = VERSION_PATTERN.fullmatch(value)
    if match is None:
        raise PolicyError(f"unsupported package version: {value!r}")
    major, minor, patch, revision = (int(item) for item in match.groups())
    return (major, minor, patch), revision


def check_not_downgrade(incoming: str, published_versions: list[str]) -> None:
    incoming_version, incoming_revision = parse_version(incoming)
    for published in published_versions:
        try:
            published_version, published_revision = parse_version(published)
        except PolicyError:
            # This policy owns only ZeroFS's stable X.Y.Z-N version stream.
            # Unrelated historical spellings must not permanently wedge it.
            continue
        if (incoming_version, incoming_revision) < (
            published_version,
            published_revision,
        ):
            raise PolicyError(
                f"refusing package downgrade from {published} to {incoming}"
            )


def debian_paragraphs(path: Path) -> list[dict[str, str]]:
    paragraphs = []
    for paragraph in re.split(r"\n\s*\n", path.read_text(encoding="utf-8")):
        fields = {}
        for line in paragraph.splitlines():
            key, separator, value = line.partition(":")
            if separator:
                fields[key] = value.strip()
        if fields:
            paragraphs.append(fields)
    return paragraphs


def check_version_command(arguments: argparse.Namespace) -> None:
    check_not_downgrade(arguments.incoming, arguments.published)


def check_apt_command(arguments: argparse.Namespace) -> None:
    published = [
        fields
        for manifest in arguments.manifest
        for fields in debian_paragraphs(manifest)
        if fields.get("Package") == arguments.package
        and fields.get("Architecture") == arguments.architecture
    ]
    check_not_downgrade(
        arguments.incoming,
        [fields.get("Version", "") for fields in published],
    )

    for fields in published:
        if fields.get("Version") != arguments.incoming:
            continue
        published_digest = fields.get("SHA256", "").lower()
        if SHA256_PATTERN.fullmatch(published_digest) is None:
            raise PolicyError(
                "existing equal-version package has no valid SHA-256 digest"
            )
        if published_digest != arguments.incoming_sha256:
            raise PolicyError(
                "refusing to replace an equal-version package with different bytes"
            )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)

    check_version = commands.add_parser(
        "check-version", help="reject a package version or revision downgrade"
    )
    check_version.add_argument("incoming")
    check_version.add_argument("published", nargs="+")
    check_version.set_defaults(function=check_version_command)

    check_apt = commands.add_parser(
        "check-apt", help="check a package against versions in an APT manifest"
    )
    check_apt.add_argument(
        "--manifest", type=Path, action="append", required=True
    )
    check_apt.add_argument("--package", type=package_name, required=True)
    check_apt.add_argument(
        "--architecture", type=architecture, required=True
    )
    check_apt.add_argument("--incoming", required=True)
    check_apt.add_argument(
        "--incoming-sha256", type=sha256, required=True
    )
    check_apt.set_defaults(function=check_apt_command)
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        arguments.function(arguments)
    except (OSError, PolicyError, UnicodeError) as error:
        print(f"repository policy: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
