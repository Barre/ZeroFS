#!/usr/bin/env python3

"""Audit the published ZeroFS source-DKMS package layout."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


PACKAGE_NAME = "zerofs-kernel-client"
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+-[1-9][0-9]*$")
MODULE_LAYOUT = "v1"
DEB_DEPENDENCIES = {
    "ca-certificates",
    "curl",
    "dkms",
    "kmod",
    "openssl",
    "python3",
    "xz-utils",
}
RPM_DEPENDENCIES = {
    "ca-certificates",
    "curl",
    "dkms >= 3.0.11",
    "kmod",
    "openssl",
    "python3",
    "xz",
}
NETWORK_OR_PACKAGE_MANAGER = re.compile(
    r"(?<![A-Za-z0-9_.+-])"
    r"(?:apt(?:-get)?|curl|dnf|pip|wget|yum|zypper)"
    r"(?![A-Za-z0-9_.+-])"
)
DKMS_AUTOINSTALL_COMMAND = re.compile(
    r"(?m)^[ \t]*(?:(?:if|elif|then|do|command|exec|!)\s+)*"
    r"dkms\s+autoinstall(?=\s*(?:[;&|]|$))"
)


class AuditError(ValueError):
    pass


def command(*arguments: str) -> str:
    return subprocess.run(
        arguments,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.rstrip("\n")


def extract(package: Path, root: Path) -> tuple[str, str, str, str]:
    if package.suffix == ".deb":
        family = "deb"
        package_name = command("dpkg-deb", "--field", str(package), "Package")
        version = command("dpkg-deb", "--field", str(package), "Version")
        depends = command("dpkg-deb", "--field", str(package), "Depends")
        control = root.parent / "control"
        subprocess.run(
            ["dpkg-deb", "--control", str(package), str(control)], check=True
        )
        subprocess.run(
            ["dpkg-deb", "--extract", str(package), str(root)], check=True
        )
        scripts = []
        for script_name in ("postinst", "prerm"):
            path = control / script_name
            if not path.is_file():
                raise AuditError(f"{package}: missing {script_name}")
            subprocess.run(["sh", "-n", str(path)], check=True)
            scripts.append(path.read_text(encoding="utf-8"))
        script_text = "\n".join(scripts)
    elif package.suffix == ".rpm":
        family = "rpm"
        if shutil.which("rpm2cpio") is None or shutil.which("cpio") is None:
            raise AuditError("rpm2cpio and cpio are required for RPM auditing")
        package_name = command("rpm", "-qp", "--qf", "%{NAME}", str(package))
        version = command(
            "rpm", "-qp", "--qf", "%{VERSION}-%{RELEASE}", str(package)
        )
        depends = command("rpm", "-qp", "--requires", str(package))
        script_text = command("rpm", "-qp", "--scripts", str(package))
        archive = subprocess.run(
            ["rpm2cpio", str(package)], check=True, stdout=subprocess.PIPE
        ).stdout
        subprocess.run(
            ["cpio", "-idm", "--quiet", "--no-absolute-filenames"],
            cwd=root,
            input=archive,
            check=True,
        )
    else:
        raise AuditError(f"{package}: expected a .deb or .rpm package")

    if package_name != PACKAGE_NAME:
        raise AuditError(f"{package}: package name is {package_name!r}")
    return family, version, depends, script_text


def audit_dependencies(package: Path, family: str, dependencies: str) -> None:
    if family == "deb":
        actual = {
            dependency.strip().split(maxsplit=1)[0]
            for dependency in dependencies.replace("\n", " ").split(",")
            if dependency.strip()
        }
        expected = DEB_DEPENDENCIES
        dkms_minimum = re.search(
            r"(?:^|,)\s*dkms\s*\(>=\s*3\.0\.11\s*\)(?:,|$)",
            dependencies,
        )
        if dkms_minimum is None:
            raise AuditError(f"{package}: dkms dependency does not require 3.0.11")
        forbidden = tuple(
            dependency
            for dependency in actual
            if dependency.startswith(("linux-headers", "linux-source"))
        )
    else:
        actual = {
            " ".join(dependency.split())
            for dependency in dependencies.splitlines()
            if dependency.strip()
        }
        expected = RPM_DEPENDENCIES
        forbidden = tuple(
            dependency
            for dependency in actual
            if dependency.startswith(("kernel-devel", "kernel-source"))
        )
    missing = sorted(expected - actual)
    if missing:
        raise AuditError(
            f"{package}: missing runtime dependency {missing[0]!r}"
        )
    if forbidden:
        raise AuditError(
            f"{package}: hard-codes kernel prerequisite {sorted(forbidden)[0]!r}"
        )


def require_text(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        raise AuditError(f"missing regular file: {path}")
    return path.read_text(encoding="utf-8")


def audit(package: Path) -> tuple[str, bytes]:
    if not package.is_file() or package.is_symlink():
        raise AuditError(f"not a regular package: {package}")
    package = package.resolve()
    with tempfile.TemporaryDirectory(prefix="zerofs-dkms-audit.") as temporary:
        root = Path(temporary) / "root"
        root.mkdir()
        family, version, dependencies, scripts = extract(package, root)
        if VERSION.fullmatch(version) is None:
            raise AuditError(f"{package}: unsafe package version {version!r}")
        audit_dependencies(package, family, dependencies)

        source = root / f"usr/src/zerofs-{version}"
        configuration = require_text(source / "dkms.conf")
        builder = source / "dkms-build"
        builder_text = require_text(builder)
        fetcher = source / "dkms-fetch-module"
        require_text(fetcher)
        resolver = source / "dkms-find-kernel-source"
        resolver_text = require_text(resolver)
        for script in (builder, fetcher, resolver):
            if script.stat().st_mode & 0o111 == 0:
                raise AuditError(f"{package}: {script.name} is not executable")
            subprocess.run(["bash", "-n", str(script)], check=True)
        require_text(source / "LICENSE")
        certificate = source / "zerofs-module-signing-cert.pem"
        require_text(certificate)
        subprocess.run(
            ["openssl", "x509", "-in", str(certificate), "-noout"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        layout = require_text(source / "zerofs-module-layout")
        if layout != f"{MODULE_LAYOUT}\n":
            raise AuditError(
                f"{package}: unsupported module layout {layout!r}"
            )
        for directory in (source / "kernel", source / "zerofs"):
            if not directory.is_dir() or directory.is_symlink():
                raise AuditError(f"{package}: missing source directory {directory}")

        required_configuration = (
            'PACKAGE_NAME="zerofs"',
            f'PACKAGE_VERSION="{version}"',
            'BUILT_MODULE_NAME[0]="zerofs"',
            'BUILT_MODULE_LOCATION[0]="dkms-output"',
            'DEST_MODULE_LOCATION[0]="/kernel/fs/zerofs"',
            'STRIP[0]="no"',
            'AUTOINSTALL="yes"',
            f'MAKE[0]="./dkms-build $kernelver {version}"',
            'CLEAN="/bin/true"',
        )
        for entry in required_configuration:
            if entry not in configuration:
                raise AuditError(f"{package}: dkms.conf omits {entry!r}")
        if re.search(r"(?m)^\s*PRE_BUILD=", configuration):
            raise AuditError(f"{package}: dkms.conf uses unreliable PRE_BUILD")
        kernel_policy = tuple(
            line.strip()
            for line in configuration.splitlines()
            if not line.lstrip().startswith("#")
            and (
                "BUILD_EXCLUSIVE_KERNEL" in line
                or "OBSOLETE_BY" in line
            )
        )
        if kernel_policy != ('BUILD_EXCLUSIVE_KERNEL_MIN="6.18"',):
            raise AuditError(
                f"{package}: dkms.conf has unexpected kernel policy "
                f"{kernel_policy!r}"
            )
        if "dkms_binaries_only" in configuration:
            raise AuditError(
                f"{package}: dkms.conf contains 'dkms_binaries_only'"
            )

        for required in (
            "dkms add",
            'dkms build -m "$module" -v "$version" -k "$kernel"',
            'dkms install -m "$module" -v "$version" -k "$kernel"',
            "dkms remove",
        ):
            if required not in scripts:
                raise AuditError(f"{package}: maintainer scripts omit {required!r}")
        if DKMS_AUTOINSTALL_COMMAND.search(scripts):
            raise AuditError(f"{package}: maintainer scripts use unscoped autoinstall")
        package_scripts = f"{builder_text}\n{resolver_text}\n{scripts}"
        if NETWORK_OR_PACKAGE_MANAGER.search(package_scripts):
            raise AuditError(
                f"{package}: package scripts invoke the network or package manager"
            )
        forbidden_payloads = [
            path
            for path in root.rglob("*")
            if path.is_file()
            and (
                path.name.endswith((".ko", ".ko.gz", ".ko.xz", ".ko.zst"))
                or path.name.endswith(
                    (".tar", ".tar.bz2", ".tar.gz", ".tar.xz", ".tar.zst", ".tgz")
                )
            )
        ]
        if forbidden_payloads:
            raise AuditError(
                f"{package}: contains forbidden binary payload {forbidden_payloads[0]}"
            )
        return version, certificate.read_bytes()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packages", type=Path, nargs="+")
    parser.add_argument("--certificate-output", type=Path)
    arguments = parser.parse_args()
    try:
        audits = [audit(package) for package in arguments.packages]
        versions = {version for version, _ in audits}
        if len(versions) != 1:
            raise AuditError("native package versions differ")
        certificates = {certificate for _, certificate in audits}
        if len(certificates) != 1:
            raise AuditError("native packages contain different trust certificates")
        if arguments.certificate_output is not None:
            output = arguments.certificate_output
            if output.exists() or output.is_symlink():
                raise AuditError(f"certificate output already exists: {output}")
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(certificates.pop())
        print(versions.pop())
    except (AuditError, OSError, UnicodeError, subprocess.CalledProcessError) as error:
        print(f"dkms-package-smoke.py: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
