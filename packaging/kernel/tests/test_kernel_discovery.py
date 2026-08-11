import re
import sys
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY / "packaging/kernel"))

from kernel_targets.catalog import ManifestError, validate_catalog
from kernel_targets.discovery import discover_candidate, parse_as_of
from kernel_targets.updates import reconcile_candidates


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

    def splice_rpm_sighdr(self, _sighdr, _package, destination):
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


def single_channel_catalog(stream_id, stream):
    result = validate_catalog(
        {
            "schema_version": 3,
            "streams": {stream_id: stream},
        },
        "test catalog",
    )
    return result, next(iter(result.channels))


def apt_catalog(provider, stream_id, suite, lock, *, selector=None):
    stream = {
        "provider": provider,
        "release": "stable",
        "builder": f"{provider}:test@sha256:{DIGEST}",
        "suite": suite,
        "architectures": {"x86_64": [lock]},
    }
    if selector is not None:
        stream["selector"] = selector
    return single_channel_catalog(stream_id, stream)


def apt_record(package, version, extra=""):
    return f"Package: {package}\nVersion: {version}\n{extra}\n"


def fedora_artifacts(kernel_nvr, rust_nvr, arch):
    result = {
        f"kernel-{kernel_nvr}.src.rpm": DIGEST,
        f"rust-src-{rust_nvr}.noarch.rpm": DIGEST,
    }
    result.update(
        {
            f"{name}-{kernel_nvr}.{arch}.rpm": DIGEST
            for name in ("kernel-core", "kernel-devel", "kernel-modules-core")
        }
    )
    result.update(
        {
            f"{name}-{rust_nvr}.{arch}.rpm": DIGEST
            for name in ("cargo", "rust", "rust-std-static", "rustfmt")
        }
    )
    return result


class KernelDiscoveryTest(unittest.TestCase):
    def test_ubuntu_uses_versioned_dependency_and_header_source(self):
        current_lock = {
            "kernel": "7.0.1-1-generic",
            "version": "7.0.1-1.1",
            "source_version": "7.0.1-1.1",
            "snapshot": "20260720T000000Z",
            "source_name": "linux-hwe-7.0",
        }
        current_catalog, channel_id = apt_catalog(
            "ubuntu",
            "ubuntu-stable-generic",
            "noble",
            current_lock,
            selector="linux-image-generic-hwe-24.04",
        )
        meta_version = "7.0.2.2"
        kernel_release = "7.0.2-2-generic"
        package_version = "7.0.2-2.2"
        selector_record = apt_record(
            "linux-image-generic-hwe-24.04",
            meta_version,
            f"Depends: linux-image-{kernel_release}",
        )
        responses = {
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "policy",
                "linux-image-generic-hwe-24.04",
            ): f"  Candidate: {meta_version}\n",
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-image-generic-hwe-24.04={meta_version}",
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
            current_catalog,
            channel_id,
            AS_OF,
            runner,
        )

        self.assertEqual(set(candidate), {"base_target_id", "lock"})
        self.assertEqual(
            candidate["lock"],
            {
                "kernel": kernel_release,
                "version": package_version,
                "source_version": package_version,
                "snapshot": "20260730T000000Z",
                "source_name": "linux-hwe-7.0",
            },
        )
        self.assertEqual(runner.sources[0], "zerofs.sources")
        self.assertIn("noble-security", runner.sources[1])
        self.assertEqual(
            sum(
                command[-2:] == (
                    "show",
                    f"linux-image-{kernel_release}={package_version}",
                )
                for command in runner.commands
            ),
            1,
        )

    def test_ubuntu_selector_only_update_is_ignored(self):
        kernel_release = "7.0.1-1-generic"
        package_version = "7.0.1-1.1"
        current_lock = {
            "kernel": kernel_release,
            "version": package_version,
            "source_version": package_version,
            "snapshot": "20260720T000000Z",
            "source_name": "linux-hwe-7.0",
        }
        current_catalog, channel_id = apt_catalog(
            "ubuntu",
            "ubuntu-stable-generic",
            "noble",
            current_lock,
            selector="linux-image-generic-hwe-24.04",
        )
        meta_version = "7.0.1.3"
        responses = {
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "policy",
                "linux-image-generic-hwe-24.04",
            ): f"  Candidate: {meta_version}\n",
            (
                "apt-cache",
                "-o",
                "APT::Default-Release=noble",
                "show",
                f"linux-image-generic-hwe-24.04={meta_version}",
            ): apt_record(
                "linux-image-generic-hwe-24.04",
                meta_version,
                f"Depends: linux-image-{kernel_release} "
                f"(= {package_version})",
            ),
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

        candidate = discover_candidate(
            current_catalog,
            channel_id,
            AS_OF,
            FakeRunner(respond),
        )

        self.assertEqual(candidate["lock"], current_lock)
        self.assertEqual(
            reconcile_candidates(current_catalog, [candidate]),
            current_catalog.document,
        )

    def test_debian_resolves_backports_source_package(self):
        current_lock = {
            "kernel": "7.0.13+deb13-amd64",
            "version": "7.0.13-1~bpo13+1",
            "source_version": "7.0.13-1~bpo13+1",
            "snapshot": "20260720T000000Z",
        }
        current_catalog, channel_id = apt_catalog(
            "debian",
            "debian-backports-amd64",
            "forky-backports",
            current_lock,
        )
        meta_version = "7.1.3-1~bpo13+1"
        kernel_release = "7.1.3+deb13-amd64"

        def respond(arguments):
            if arguments[0] == "apt-get":
                return ""
            if arguments[-2:] == ["policy", "linux-image-amd64"]:
                return f"  Candidate: {meta_version}\n"
            specification = arguments[-1]
            package, version = specification.split("=", 1)
            extra = ""
            if package == "linux-image-amd64":
                extra = (
                    "Depends: "
                    f"linux-image-{kernel_release} (= {meta_version})"
                )
            elif package == f"linux-headers-{kernel_release}":
                extra = f"Source: linux ({meta_version})"
            return apt_record(package, version, extra)

        runner = FakeRunner(respond)
        candidate = discover_candidate(
            current_catalog,
            channel_id,
            AS_OF,
            runner,
        )

        self.assertEqual(
            candidate["lock"],
            {
                "kernel": kernel_release,
                "version": meta_version,
                "source_version": meta_version,
                "snapshot": "20260730T000000Z",
            },
        )
        self.assertTrue(
            any(
                command[-1] == f"linux-source-7.1={meta_version}"
                for command in runner.commands
            )
        )
        self.assertIn("forky main", runner.sources[1])

    def test_fedora_same_signed_build_skips_artifact_downloads(self):
        fingerprint = "a" * 40
        kernel_nvr = "7.1.5-200.fc44"
        rust_nvr = "1.97.1-1.fc44"
        artifacts = fedora_artifacts(kernel_nvr, rust_nvr, "x86_64")
        current_lock = {
            "nvr": f"kernel-{kernel_nvr}",
            "signing_fingerprint": fingerprint,
            "artifacts": artifacts,
        }
        current_catalog, channel_id = single_channel_catalog(
            "fedora-44-kernel-core",
            {
                "provider": "fedora",
                "release": "44",
                "builder": f"fedora:44@sha256:{DIGEST}",
                "signing_fingerprint": fingerprint,
                "architectures": {"x86_64": [current_lock]},
            },
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

        runner = FakeRunner(respond)
        candidate = discover_candidate(
            current_catalog,
            channel_id,
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["lock"], current_lock)
        self.assertEqual(runner.downloads, [])

    def test_fedora_aarch64_build_is_reacquired_from_signed_koji(self):
        fingerprint = "b" * 40
        kernel_nvr = "7.1.5-100.fc43"
        old_kernel_nvr = "7.1.4-99.fc43"
        rust_nvr = "1.96.1-1.fc43"
        arch = "aarch64"
        current_lock = {
            "nvr": f"kernel-{old_kernel_nvr}",
            "signing_fingerprint": fingerprint,
            "artifacts": fedora_artifacts(old_kernel_nvr, rust_nvr, arch),
        }
        current_catalog, channel_id = single_channel_catalog(
            "fedora-43-kernel-core",
            {
                "provider": "fedora",
                "release": "43",
                "builder": f"fedora:43@sha256:{DIGEST}",
                "signing_fingerprint": fingerprint,
                "architectures": {arch: [current_lock]},
            },
        )

        def rpm_metadata(filename):
            if filename == f"kernel-{kernel_nvr}.src.rpm":
                return f"kernel\t{kernel_nvr}\tppc64le\t(none)\n"
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

        runner = FakeRunner(respond, arch=arch)
        runner.auto_conf = (
            'CONFIG_RUSTC_VERSION_TEXT="rustc 1.96.1 '
            f'(Fedora {rust_nvr})"\n'
        )
        candidate = discover_candidate(
            current_catalog,
            channel_id,
            AS_OF,
            runner,
        )

        expected_artifacts = {
            f"kernel-{kernel_nvr}.src.rpm",
            f"kernel-core-{kernel_nvr}.{arch}.rpm",
            f"kernel-devel-{kernel_nvr}.{arch}.rpm",
            f"kernel-modules-core-{kernel_nvr}.{arch}.rpm",
            f"cargo-{rust_nvr}.{arch}.rpm",
            f"rust-{rust_nvr}.{arch}.rpm",
            f"rust-src-{rust_nvr}.noarch.rpm",
            f"rust-std-static-{rust_nvr}.{arch}.rpm",
            f"rustfmt-{rust_nvr}.{arch}.rpm",
        }
        self.assertEqual(set(candidate["lock"]["artifacts"]), expected_artifacts)
        self.assertEqual(
            candidate["lock"]["signing_fingerprint"],
            fingerprint,
        )
        self.assertEqual(
            sorted(
                (Path(url).name.removesuffix(".sig"), url.endswith(".sig"))
                for url in runner.downloads
            ),
            sorted(
                (filename, signature)
                for filename in expected_artifacts
                for signature in (False, True)
            ),
        )
        sigs = [url for url in runner.downloads if url.endswith(".rpm.sig")]
        self.assertFalse(
            any("/data/signed/" in url for url in runner.downloads)
        )
        self.assertTrue(
            all(f"/data/sigcache/{fingerprint[-8:]}/" in url for url in sigs)
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
        current_lock = {
            "snapshot": "20260720",
            "packages": old_versions,
        }
        current_catalog, channel_id = single_channel_catalog(
            "opensuse-tumbleweed-default",
            {
                "provider": "opensuse",
                "release": "tumbleweed",
                "builder": f"opensuse:test@sha256:{DIGEST}",
                "architectures": {"x86_64": [current_lock]},
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
            current_catalog,
            channel_id,
            AS_OF,
            runner,
        )

        self.assertEqual(candidate["lock"]["snapshot"], "20260728")
        self.assertEqual(
            candidate["lock"]["packages"],
            new_versions,
        )


if __name__ == "__main__":
    unittest.main()
