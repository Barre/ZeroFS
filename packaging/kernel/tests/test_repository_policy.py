import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
POLICY = REPOSITORY / "packaging/kernel/repository_policy.py"


class RepositoryPolicyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.manifest = Path(self.temporary.name) / "Packages"

    def tearDown(self):
        self.temporary.cleanup()

    def run_policy(self, *arguments, succeeds=True):
        result = subprocess.run(
            [sys.executable, str(POLICY), *arguments],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if succeeds and result.returncode:
            self.fail(result.stderr)
        if not succeeds and not result.returncode:
            self.fail("repository policy unexpectedly succeeded")

    def write_manifest(self, *records):
        paragraphs = []
        for package, architecture, version, digest in records:
            fields = [
                f"Package: {package}",
                f"Architecture: {architecture}",
                f"Version: {version}",
            ]
            if digest is not None:
                fields.append(f"SHA256: {digest}")
            paragraphs.append("\n".join(fields))
        self.manifest.write_text(
            "\n\n".join(paragraphs) + "\n",
            encoding="utf-8",
        )

    def check_apt(self, incoming, digest, *, succeeds=True):
        self.run_policy(
            "check-apt",
            "--manifest",
            str(self.manifest),
            "--package",
            "client",
            "--architecture",
            "amd64",
            "--incoming",
            incoming,
            "--incoming-sha256",
            digest,
            succeeds=succeeds,
        )

    def test_version_policy_compares_version_then_revision(self):
        self.run_policy("check-version", "2.2.0-4", "2.1.9-4")
        self.run_policy("check-version", "2.2.0-3", "2.1.9-4")
        self.run_policy(
            "check-version", "2.1.9-5", "2.2.0-4", succeeds=False
        )
        self.run_policy(
            "check-version", "2.2.0-3", "2.2.0-4", succeeds=False
        )

    def test_version_policy_ignores_unowned_historical_schemes(self):
        self.run_policy(
            "check-version", "2.2.0-1", "", "1:2.1.9-4", "2.1.9~rc1-1"
        )

    def test_apt_policy_is_scoped_to_package_and_architecture(self):
        self.write_manifest(
            ("client", "amd64", "2.1.9-4", "a" * 64),
            ("client", "arm64", "9.0.0-99", "b" * 64),
            ("unrelated", "amd64", "9.0.0-99", "c" * 64),
        )
        self.check_apt("2.2.0-4", "d" * 64)

    def test_apt_policy_compares_version_then_revision(self):
        self.write_manifest(("client", "amd64", "2.1.9-4", "a" * 64))
        self.check_apt("2.2.0-4", "b" * 64)
        self.check_apt("2.2.0-3", "b" * 64)
        self.check_apt("2.1.8-5", "b" * 64, succeeds=False)

        self.write_manifest(("client", "amd64", "2.2.0-4", "a" * 64))
        self.check_apt("2.2.0-3", "b" * 64, succeeds=False)

    def test_apt_policy_ignores_unowned_historical_schemes(self):
        self.write_manifest(
            ("client", "amd64", "1:2.1.9-4", "a" * 64),
            ("client", "amd64", "", "b" * 64),
        )
        self.check_apt("2.2.0-1", "c" * 64)

    def test_equal_version_requires_identical_bytes(self):
        digest = "a" * 64
        self.write_manifest(("client", "amd64", "2.1.9-4", digest.upper()))
        self.check_apt("2.1.9-4", digest)
        self.check_apt("2.1.9-4", "b" * 64, succeeds=False)

        self.write_manifest(("client", "amd64", "2.1.9-4", None))
        self.check_apt("2.1.9-4", digest, succeeds=False)

    def test_equal_version_must_match_in_every_manifest(self):
        digest = "a" * 64
        other_manifest = Path(self.temporary.name) / "Packages-amd64"
        self.write_manifest(("client", "all", "2.1.9-4", digest))
        other_manifest.write_text(
            self.manifest.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        arguments = (
            "check-apt",
            "--manifest",
            str(self.manifest),
            "--manifest",
            str(other_manifest),
            "--package",
            "client",
            "--architecture",
            "all",
            "--incoming",
            "2.1.9-4",
            "--incoming-sha256",
            digest,
        )
        self.run_policy(*arguments)

        other_manifest.write_text(
            other_manifest.read_text(encoding="utf-8").replace(
                digest, "b" * 64
            ),
            encoding="utf-8",
        )
        self.run_policy(*arguments, succeeds=False)


if __name__ == "__main__":
    unittest.main()
