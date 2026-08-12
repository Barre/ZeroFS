import copy
import sys
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY / "packaging/kernel"))

from kernel_targets.catalog import (
    ManifestError,
    debian_version_compare,
    load_catalog,
    rpm_version_compare,
    validate_catalog,
)


LOCK_PATH = REPOSITORY / "packaging/kernel/kernels.lock.json"


class CatalogTests(unittest.TestCase):
    def test_real_lock_loads(self):
        catalog = load_catalog(LOCK_PATH)

        self.assertTrue(catalog.channels)
        self.assertTrue(catalog.targets)

    def test_rejects_reversed_retained_locks_for_each_provider(self):
        original = load_catalog(LOCK_PATH).document
        streams = {
            stream["provider"]: stream_id
            for stream_id, stream in original["streams"].items()
            if any(len(locks) == 2 for locks in stream["architectures"].values())
        }

        for provider in ("ubuntu", "debian", "fedora", "opensuse"):
            with self.subTest(provider=provider):
                document = copy.deepcopy(original)
                stream = document["streams"][streams[provider]]
                locks = next(
                    locks
                    for locks in stream["architectures"].values()
                    if len(locks) == 2
                )
                locks.reverse()

                with self.assertRaisesRegex(
                    ManifestError,
                    "retained locks must be ordered oldest to newest",
                ):
                    validate_catalog(document, f"reversed {provider} locks")

    def test_package_version_ordering_handles_numeric_and_prerelease_parts(self):
        cases = (
            (debian_version_compare, "7.1.9-1", "7.1.10-1"),
            (debian_version_compare, "7.1.10~rc1-1", "7.1.10-1"),
            (debian_version_compare, "7.1.10-1", "1:7.1.9-1"),
            (rpm_version_compare, "7.1.9-101.fc44", "7.1.10-101.fc44"),
            (rpm_version_compare, "7.1.10~rc1-1.fc44", "7.1.10-1.fc44"),
            (rpm_version_compare, "7.1.10", "7.1.10^git1"),
        )
        for compare, older, newer in cases:
            with self.subTest(older=older, newer=newer):
                self.assertLess(compare(older, newer), 0)
                self.assertGreater(compare(newer, older), 0)

    def test_fedora_lock_requires_complete_pinned_toolchain(self):
        document = load_catalog(LOCK_PATH).document
        lock = document["streams"]["fedora-43-kernel-core"]["architectures"][
            "x86_64"
        ][0]
        cargo = next(
            filename
            for filename in lock["artifacts"]
            if filename.startswith("cargo-")
        )
        del lock["artifacts"][cargo]

        with self.assertRaisesRegex(ManifestError, "unexpected Fedora artifact set"):
            validate_catalog(document, "missing Fedora cargo")

    def test_ids_cover_effective_stream_config_and_provider_lock(self):
        original = load_catalog(LOCK_PATH)
        builder_changed = copy.deepcopy(original.document)
        builder_changed["streams"]["ubuntu-26.04-generic"]["builder"] = (
            "ubuntu:26.04@sha256:" + "0" * 64
        )
        lock_changed = copy.deepcopy(original.document)
        lock_changed["streams"]["ubuntu-26.04-generic"]["architectures"][
            "x86_64"
        ][-1]["snapshot"] = "20990101T000000Z"

        channel_id = "ubuntu-26.04-generic-x86-64"
        original_id = [
            target["id"]
            for target in original.targets
            if target["channel_id"] == channel_id
        ][-1]
        self.assertNotEqual(
            original_id,
            [
                target["id"]
                for target in validate_catalog(
                    builder_changed, "builder changed"
                ).targets
                if target["channel_id"] == channel_id
            ][-1],
        )
        self.assertNotEqual(
            original_id,
            [
                target["id"]
                for target in validate_catalog(lock_changed, "lock changed").targets
                if target["channel_id"] == channel_id
            ][-1],
        )

    def test_rejects_mutable_builder_and_excess_retention(self):
        document = load_catalog(LOCK_PATH).document
        document["streams"]["opensuse-tumbleweed-default"]["builder"] = (
            "opensuse/tumbleweed:latest"
        )
        with self.assertRaisesRegex(ManifestError, "digest-pinned"):
            validate_catalog(document, "mutable builder")

        document = load_catalog(LOCK_PATH).document
        locks = document["streams"]["ubuntu-26.04-generic"]["architectures"][
            "x86_64"
        ]
        locks.append(copy.deepcopy(locks[-1]))
        locks[-1]["kernel"] = "7.0.0-30-generic"
        with self.assertRaisesRegex(ManifestError, "exceeds retention=2"):
            validate_catalog(document, "excess retention")

        document = load_catalog(LOCK_PATH).document
        document["streams"]["opensuse-tumbleweed-default"]["release"] = "leap"
        with self.assertRaisesRegex(ManifestError, "only tumbleweed"):
            validate_catalog(document, "unsupported openSUSE release")

    def test_fedora_discovery_key_does_not_reidentify_retained_locks(self):
        original = load_catalog(LOCK_PATH)
        document = copy.deepcopy(original.document)
        document["streams"]["fedora-43-kernel-core"][
            "signing_fingerprint"
        ] = "0" * 40

        changed = validate_catalog(document, "rotated Fedora discovery key")
        original_ids = [
            target["id"]
            for target in original.targets
            if target["distro"] == "fedora" and target["release"] == "43"
        ]
        changed_targets = [
            target
            for target in changed.targets
            if target["distro"] == "fedora" and target["release"] == "43"
        ]
        self.assertEqual(original_ids, [target["id"] for target in changed_targets])
        original_snapshots = [
            target["source"]["snapshot"]
            for target in original.targets
            if target["distro"] == "fedora" and target["release"] == "43"
        ]
        self.assertEqual(
            original_snapshots,
            [target["source"]["snapshot"] for target in changed_targets],
        )


if __name__ == "__main__":
    unittest.main()
