#!/usr/bin/env python3

"""Fetch signed build tools retained in a locked Tumbleweed snapshot."""

import argparse
import re
import sys
from functools import cmp_to_key
from html.parser import HTMLParser
from pathlib import Path

from kernel_targets.catalog import ManifestError
from kernel_targets.discovery.common import rpm_compare
from kernel_targets.discovery.runtime import SystemRunner


class PackageLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = set()

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.add(value.removeprefix("./"))


def select_group(links, names, version, arch, runner):
    editions = []
    pattern = re.compile(
        rf"{re.escape(names[0])}-({re.escape(version)}(?:\+[A-Za-z0-9._]+)?"
        rf"-[A-Za-z0-9._]+)\.{re.escape(arch)}\.rpm"
    )
    for link in links:
        match = pattern.fullmatch(link)
        if match and all(
            f"{name}-{match[1]}.{arch}.rpm" in links for name in names
        ):
            editions.append(match[1])
    if not editions:
        raise ManifestError(
            f"snapshot has no matching {'/'.join(names)} packages for {version}"
        )
    edition = max(
        editions, key=cmp_to_key(lambda left, right: rpm_compare(runner, left, right))
    )
    return [f"{name}-{edition}.{arch}.rpm" for name in names]


def select_packages(index, version, arch, runner, binutils_version=None):
    major = version.split(".")[0]
    parser = PackageLinks()
    parser.feed(index)
    packages = select_group(
        parser.links, (f"gcc{major}", f"cpp{major}"), version, arch, runner,
    )
    if binutils_version:
        binutils = select_group(
            parser.links, ("binutils",), binutils_version, arch, runner,
        )[0]
        packages.append(binutils)
        # Older binutils may need a library SONAME no longer in the index,
        # such as libsframe2 after the snapshot switches to libsframe3.
        edition = binutils.removeprefix("binutils-").removesuffix(f".{arch}.rpm")
        libraries = re.compile(
            rf"(?:libctf0|libctf-nobfd0|libsframe[0-9]+)-"
            rf"{re.escape(edition)}\.{re.escape(arch)}\.rpm"
        )
        packages.extend(sorted(
            link for link in parser.links if libraries.fullmatch(link)
        ))
    return packages


def fetch_packages(snapshot, version, arch, output, runner, binutils_version=None):
    base = (
        f"https://download.opensuse.org/history/{snapshot}/"
        f"tumbleweed/repo/oss/{arch}/"
    )
    output.mkdir(parents=True, exist_ok=True)
    index = output / "index.html"
    runner.download(base, index)
    filenames = select_packages(
        index.read_text(encoding="utf-8"), version, arch, runner, binutils_version,
    )
    packages = []
    for filename in filenames:
        package = output / filename
        runner.download(base + filename, package)
        # A digest-only RPM check also exits successfully for unsigned RPMs.
        # Require a verified signature from the builder's existing keyring.
        verification = runner.run([
            "rpmkeys", "--checksig", "--verbose", str(package),
        ])
        if not re.search(
            r"(?m)^\s+.*Signature, key ID [0-9a-fA-F]+: OK$", verification,
        ):
            raise ManifestError(f"RPM has no trusted signature: {filename}")
        packages.append(package)
    return packages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--binutils-version")
    parser.add_argument("--arch", choices=("x86_64",), required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    if not re.fullmatch(r"[0-9]{8}", arguments.snapshot):
        parser.error("snapshot must be YYYYMMDD")
    if not re.fullmatch(r"[1-9][0-9]*\.[0-9]+\.[0-9]+", arguments.version):
        parser.error("version must be a numeric GCC release")
    if arguments.binutils_version and not re.fullmatch(
        r"[1-9][0-9]*\.[0-9]+(?:\.[0-9]+)?", arguments.binutils_version,
    ):
        parser.error("binutils-version must be a numeric binutils release")
    try:
        packages = fetch_packages(
            arguments.snapshot, arguments.version, arguments.arch,
            arguments.output, SystemRunner(), arguments.binutils_version,
        )
    except (ManifestError, OSError) as error:
        print(f"opensuse-toolchain: {error}", file=sys.stderr)
        return 1
    for package in packages:
        print(package)
    return 0


if __name__ == "__main__":
    sys.exit(main())
