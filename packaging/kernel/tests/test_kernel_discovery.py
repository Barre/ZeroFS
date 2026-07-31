import re
import sys
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY / "packaging/kernel"))

from kernel_targets.catalog import ManifestError, validate_catalog
from kernel_targets.discovery import check_channel, discover_candidate, parse_as_of


DIGEST = "0" * 64
AS_OF = parse_as_of("2026-07-30T00:00:00Z")


class FakeRunner:
    def __init__(self, responder, *, status=None, arch="x86_64"):
        self.responder = responder
        self.status_responder = status or (lambda arguments: 1)
        self.arch = arch
        self.commands = []
        self.downloads = []
        self.sources = None
        self.existing_urls = set()
        self.auto_conf = ""

    def run(self, arguments):
        self.assert_arguments(arguments)
        self.commands.append(tuple(arguments))
        if arguments == ["uname", "-m"]:
            return f"{self.arch}\n"
        return self.responder(arguments)

    def status(self, arguments):
        self.assert_arguments(arguments)
        self.commands.append(tuple(arguments))
        return self.status_responder(arguments)

    def replace_apt_sources(self, filename, contents):
        self.sources = (filename, contents)

    def download(self, url, destination):
        self.downloads.append(url)
        destination.write_bytes(destination.name.encode())

    def url_exists(self, url):
        return url in self.existing_urls

    def kernel_auto_conf(self, _package, _destination):
        return self.auto_conf

    def assert_arguments(self, arguments):
        if not isinstance(arguments, list) or not all(
            isinstance(argument, str) for argument in arguments
        ):
            raise AssertionError(f"non-vector command: {arguments!r}")


class TimestampTests(unittest.TestCase):
    def test_rejects_future_timestamp(self):
        with self.assertRaisesRegex(
            ManifestError,
            "must not be in the future",
        ):
            parse_as_of("2999-01-01T00:00:00Z")


def catalog(channel, target):
    return validate_catalog(
        {
            "schema_version": 1,
            "channels": [channel],
            "targets": [target],
            "unsupported_targets": [],
        },
        "test catalog",
    )


def apt_channel(distro, channel_id, selector, suite, flavor):
    return {
        "id": channel_id,
        "distro": distro,
        "release": "stable",
        "family": "deb",
        "arch": "x86_64",
        "flavor": flavor,
        "apt": {
            "codename": "stable",
            "suite": "stable",
            "component": "main",
        },
        "discovery": {
            "kind": f"{distro}-snapshot",
            "builder_image": f"{distro}:test@sha256:{DIGEST}",
            "selector": selector,
            "suite": suite,
        },
    }


def target(
    channel,
    kernel_release,
    package_version,
    source,
    selector_version=None,
):
    return {
        "id": f"{channel['id']}-r1",
        "enabled": True,
        "ci": True,
        "publish": False,
        "channel_id": channel["id"],
        "package_revision": "1",
        "kernel_release": kernel_release,
        "kernel_package_name": (
            "kernel-core-uname-r"
            if channel["distro"] == "fedora"
            else (
                "kernel-default"
                if channel["distro"] == "opensuse"
                else f"linux-image-{kernel_release}"
            )
        ),
        "kernel_package_version": package_version,
        "kernel_selector_version": selector_version or package_version,
        "builder_image": channel["discovery"]["builder_image"],
        "source": source,
    }


def apt_record(package, version, extra=""):
    return f"Package: {package}\nVersion: {version}\n{extra}\n"


class KernelDiscoveryTest(unittest.TestCase):
    def test_ubuntu_uses_versioned_dependency_and_header_source(self):
        channel = apt_channel(
            "ubuntu",
            "ubuntu-stable-generic-x86-64",
            "linux-image-generic-hwe-24.04",
            "noble",
            "generic-hwe",
        )
        current = target(
            channel,
            "7.0.1-1-generic",
            "7.0.1-1.1",
            {
                "kind": "apt-snapshot",
                "identity": "ubuntu:linux-hwe-7.0@7.0.1-1.1",
                "snapshot": "20260720T000000Z",
            },
        )
        selector_version = "7.0.2.2"
        kernel_release = "7.0.2-2-generic"
        package_version = "7.0.2-2.2"
        selector_record = apt_record(
            "linux-image-generic-hwe-24.04",
            selector_version,
            f"Depends: linux-image-{kernel_release}",
        )
        responses = {
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "policy",
                "linux-image-generic-hwe-24.04",
            ): f"  Candidate: {selector_version}\n",
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-image-generic-hwe-24.04={selector_version}",
            ): selector_record + "\n" + selector_record,
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "policy",
                f"linux-image-{kernel_release}",
            ): f"  Candidate: {package_version}\n",
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-image-{kernel_release}={package_version}",
            ): apt_record(f"linux-image-{kernel_release}", package_version),
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-headers-{kernel_release}={package_version}",
            ): apt_record(
                f"linux-headers-{kernel_release}",
                package_version,
                f"Source: linux-hwe-7.0 ({package_version})",
            ),
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-modules-{kernel_release}={package_version}",
            ): apt_record(f"linux-modules-{kernel_release}", package_version),
        }

        def respond(arguments):
            if arguments[0] == "apt-get":
                return ""
            return responses[tuple(arguments)]

        runner = FakeRunner(respond)

        candidate = discover_candidate(
            catalog(channel, current),
            channel["id"],
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["kernel_release"], kernel_release)
        self.assertEqual(candidate["kernel_package_version"], package_version)
        self.assertEqual(
            candidate["kernel_selector_version"],
            selector_version,
        )
        self.assertEqual(
            candidate["source"],
            {
                "kind": "apt-snapshot",
                "identity": f"ubuntu:linux-hwe-7.0@{package_version}",
                "snapshot": "20260730T000000Z",
            },
        )
        self.assertEqual(runner.sources[0], "zerofs.sources")
        self.assertIn("noble-security", runner.sources[1])

    def test_debian_resolves_backports_source_package(self):
        channel = apt_channel(
            "debian",
            "debian-backports-amd64-x86-64",
            "linux-image-amd64",
            "trixie-backports",
            "amd64",
        )
        current = target(
            channel,
            "7.0.13+deb13-amd64",
            "7.0.13-1~bpo13+1",
            {
                "kind": "apt-snapshot",
                "identity": "debian:linux@7.0.13-1~bpo13+1:trixie-backports",
                "snapshot": "20260720T000000Z",
            },
        )
        selector_version = "7.1.3-1~bpo13+1"
        kernel_release = "7.1.3+deb13-amd64"

        def respond(arguments):
            if arguments[0] == "apt-get":
                return ""
            if arguments[-2:] == ["policy", "linux-image-amd64"]:
                return f"  Candidate: {selector_version}\n"
            specification = arguments[-1]
            package, version = specification.split("=", 1)
            extra = ""
            if package == "linux-image-amd64":
                extra = (
                    "Depends: "
                    f"linux-image-{kernel_release} (= {selector_version})"
                )
            elif package == f"linux-headers-{kernel_release}":
                extra = f"Source: linux ({selector_version})"
            return apt_record(package, version, extra)

        current_catalog = catalog(channel, current)
        check_runner = FakeRunner(respond)
        entry = check_channel(
            current_catalog,
            channel["id"],
            AS_OF,
            check_runner,
        )
        self.assertTrue(entry["update_available"])
        self.assertEqual(
            entry["current_source_snapshot"],
            "20260720T000000Z",
        )
        self.assertEqual(
            entry["candidate_source_snapshot"],
            "20260730T000000Z",
        )
        self.assertEqual(check_runner.downloads, [])

        runner = FakeRunner(respond)
        candidate = discover_candidate(
            current_catalog,
            channel["id"],
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["kernel_release"], kernel_release)
        self.assertEqual(
            candidate["source"]["identity"],
            f"debian:linux@{selector_version}:trixie-backports",
        )
        self.assertTrue(
            any(
                command[-1] == f"linux-source-7.1={selector_version}"
                for command in runner.commands
            )
        )

    def test_fedora_same_signed_build_skips_artifact_downloads(self):
        fingerprint = "a" * 40
        kernel_nvr = "7.1.5-200.fc44"
        rust_nvr = "1.97.1-1.fc44"
        artifacts = {
            f"kernel-{kernel_nvr}.src.rpm": DIGEST,
            f"rust-src-{rust_nvr}.noarch.rpm": DIGEST,
            **{
                f"{name}-{kernel_nvr}.x86_64.rpm": DIGEST
                for name in (
                    "kernel-core",
                    "kernel-devel",
                    "kernel-modules-core",
                )
            },
            **{
                f"{name}-{rust_nvr}.x86_64.rpm": DIGEST
                for name in (
                    "cargo",
                    "rust",
                    "rust-std-static",
                    "rustfmt",
                )
            },
        }
        channel = {
            "id": "fedora-44-kernel-core-x86-64",
            "distro": "fedora",
            "release": "44",
            "family": "rpm",
            "arch": "x86_64",
            "flavor": "kernel-core",
            "rpm": {"repo_id": "zerofs-fedora-44"},
            "discovery": {
                "kind": "fedora-koji",
                "builder_image": f"fedora:44@sha256:{DIGEST}",
                "selector": "kernel-core",
                "signing_fingerprint": fingerprint,
            },
        }
        source = {
            "kind": "koji",
            "identity": f"kernel-{kernel_nvr}",
            "snapshot": (
                f"koji-signed-build:{fingerprint}:x86_64,noarch,src"
            ),
            "artifacts": artifacts,
        }
        current = target(
            channel,
            f"{kernel_nvr}.x86_64",
            f"{kernel_nvr}.x86_64",
            source,
            selector_version=kernel_nvr,
        )

        def respond(arguments):
            if arguments[0] == "dnf":
                return (
                    "kernel-core\t0\t7.1.5\t200.fc44\tx86_64\t"
                    f"kernel-{kernel_nvr}.src.rpm\n"
                )
            if arguments[:2] == ["rpm", "--eval"]:
                return "0\n"
            raise AssertionError(arguments)

        current_catalog = catalog(channel, current)
        check_runner = FakeRunner(respond)
        entry = check_channel(
            current_catalog,
            channel["id"],
            AS_OF,
            check_runner,
        )
        self.assertFalse(entry["update_available"])
        self.assertEqual(entry["current_source_snapshot"], source["snapshot"])
        self.assertEqual(
            entry["candidate_source_snapshot"],
            source["snapshot"],
        )
        self.assertEqual(check_runner.downloads, [])

        runner = FakeRunner(respond)
        candidate = discover_candidate(
            current_catalog,
            channel["id"],
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["source"], source)
        self.assertEqual(runner.downloads, [])

    def test_fedora_aarch64_build_is_reacquired_from_signed_koji(self):
        fingerprint = "b" * 40
        kernel_nvr = "7.1.5-100.fc43"
        rust_nvr = "1.96.1-1.fc43"
        arch = "aarch64"
        channel = {
            "id": "fedora-43-kernel-core-aarch64",
            "distro": "fedora",
            "release": "43",
            "family": "rpm",
            "arch": arch,
            "flavor": "kernel-core",
            "rpm": {"repo_id": "zerofs-fedora-43-aarch64"},
            "discovery": {
                "kind": "fedora-koji",
                "builder_image": f"fedora:43@sha256:{DIGEST}",
                "selector": "kernel-core",
                "signing_fingerprint": fingerprint,
            },
        }
        current = target(
            channel,
            f"{kernel_nvr}.{arch}",
            f"{kernel_nvr}.{arch}",
            {
                "kind": "koji",
                "identity": f"kernel-{kernel_nvr}",
                "snapshot": f"koji-download-build:{arch},noarch,src",
                "artifacts": {"placeholder.rpm": DIGEST},
            },
            selector_version=kernel_nvr,
        )

        def rpm_metadata(filename):
            if filename == f"kernel-{kernel_nvr}.src.rpm":
                return f"kernel\t{kernel_nvr}\tx86_64\t(none)\n"
            for package in (
                "kernel-core",
                "kernel-devel",
                "kernel-modules-core",
            ):
                prefix = f"{package}-"
                if filename.startswith(prefix):
                    return (
                        f"{package}\t{kernel_nvr}\t{arch}\t"
                        f"kernel-{kernel_nvr}.src.rpm\n"
                    )
            if filename == f"rust-src-{rust_nvr}.noarch.rpm":
                return (
                    f"rust-src\t{rust_nvr}\tnoarch\t"
                    f"rust-{rust_nvr}.src.rpm\n"
                )
            for package in ("cargo", "rust-std-static", "rustfmt", "rust"):
                if filename.startswith(f"{package}-"):
                    return (
                        f"{package}\t{rust_nvr}\t{arch}\t"
                        f"rust-{rust_nvr}.src.rpm\n"
                    )
            raise AssertionError(filename)

        def respond(arguments):
            if arguments[0] == "dnf":
                return (
                    f"kernel-core\t0\t7.1.5\t100.fc43\t{arch}\t"
                    f"kernel-{kernel_nvr}.src.rpm\n"
                )
            if arguments[:2] == ["rpm", "--eval"]:
                return "0\n"
            if arguments[0] == "rpmkeys":
                return f"key fingerprint: {fingerprint}: OK\n"
            if arguments[:2] == ["rpm", "-qp"]:
                return rpm_metadata(Path(arguments[-1]).name)
            raise AssertionError(arguments)

        current_catalog = catalog(channel, current)
        check_runner = FakeRunner(respond, arch=arch)
        entry = check_channel(
            current_catalog,
            channel["id"],
            AS_OF,
            check_runner,
        )
        self.assertTrue(entry["update_available"])
        self.assertEqual(
            entry["candidate_kernel_release"],
            current["kernel_release"],
        )
        self.assertEqual(
            entry["candidate_source_snapshot"],
            f"koji-signed-build:{fingerprint}:{arch},noarch,src",
        )
        self.assertEqual(check_runner.downloads, [])
        self.assertFalse(
            any(
                command[0] == "rpmkeys"
                or command[:2] == ("rpm", "-qp")
                or "cpio" in command
                for command in check_runner.commands
            )
        )

        runner = FakeRunner(respond, arch=arch)
        runner.auto_conf = (
            'CONFIG_RUSTC_VERSION_TEXT="rustc 1.96.1 '
            f'(Fedora {rust_nvr})"\n'
        )
        candidate = discover_candidate(
            current_catalog,
            channel["id"],
            AS_OF,
            runner,
        )

        self.assertEqual(len(runner.downloads), 9)
        self.assertEqual(len(candidate["source"]["artifacts"]), 9)
        self.assertEqual(
            candidate["source"]["snapshot"],
            f"koji-signed-build:{fingerprint}:{arch},noarch,src",
        )
        self.assertTrue(
            all(f"/data/signed/{fingerprint[-8:]}/" in url
                for url in runner.downloads)
        )
        self.assertTrue(
            all(
                f"/{arch}/" in url or "/noarch/" in url or "/src/" in url
                for url in runner.downloads
            )
        )
        self.assertTrue(
            any(
                f"--arch={arch}" in command
                for command in runner.commands
            )
        )

    def test_opensuse_uses_independent_package_versions(self):
        packages = [
            "kernel-default",
            "kernel-default-devel",
            "kernel-devel",
            "kernel-source",
            "kernel-syms",
        ]
        old_versions = {
            package: "7.1.4-1.1"
            for package in packages
        }
        new_versions = {
            "kernel-default": "7.1.5-2.1",
            "kernel-default-devel": "7.1.5-2.2",
            "kernel-devel": "7.1.5-3.1",
            "kernel-source": "7.1.5-3.1",
            "kernel-syms": "7.1.5-2.3",
        }
        channel = {
            "id": "opensuse-tumbleweed-default-x86-64",
            "distro": "opensuse",
            "release": "tumbleweed",
            "family": "rpm",
            "arch": "x86_64",
            "flavor": "default",
            "rpm": {"repo_id": "zerofs-opensuse"},
            "discovery": {
                "kind": "opensuse-history",
                "builder_image": f"opensuse:test@sha256:{DIGEST}",
                "selector": "kernel-default",
                "packages": packages,
            },
        }
        current = target(
            channel,
            "7.1.4-1-default",
            old_versions["kernel-default"],
            {
                "kind": "opensuse-history",
                "identity": ",".join(
                    f"{package}@{old_versions[package]}"
                    for package in packages
                ),
                "snapshot": "20260720",
            },
        )

        def respond(arguments):
            if arguments[0] == "zypper" and "search" not in arguments:
                return ""
            if arguments[0] == "zypper":
                package = arguments[-1]
                return (
                    "<stream><solvable-list>"
                    f'<solvable name="{package}" '
                    f'edition="{new_versions[package]}" arch="x86_64" '
                    'repository="zerofs-snapshot"/>'
                    "</solvable-list></stream>"
                )
            if arguments[:2] == ["rpm", "--eval"]:
                expression = arguments[-1]
                left, right = re.findall(r"\[\[([^]]+)\]\]", expression)
                return "0\n" if left == right else "1\n"
            raise AssertionError(arguments)

        runner = FakeRunner(respond)
        runner.existing_urls.add(
            "https://download.opensuse.org/history/20260728/"
            "tumbleweed/repo/oss/repodata/repomd.xml"
        )
        candidate = discover_candidate(
            catalog(channel, current),
            channel["id"],
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["kernel_release"], "7.1.5-2-default")
        self.assertEqual(candidate["source"]["snapshot"], "20260728")
        self.assertEqual(
            candidate["source"]["identity"],
            ",".join(
                f"{package}@{new_versions[package]}"
                for package in packages
            ),
        )


if __name__ == "__main__":
    unittest.main()
