import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
CONTROLLER = REPOSITORY / "packaging/kernel/kernel-targets.py"
DIGEST = "0" * 64
sys.path.insert(0, str(REPOSITORY / "packaging/kernel"))

from kernel_targets.catalog import validate_catalog


def apt_lock(
    *,
    kernel_release="7.0.1-generic",
    package_version="7.0.1-1",
    source_version=None,
    snapshot="20260729T000000Z",
):
    return {
        "kernel": kernel_release,
        "version": package_version,
        "source_version": source_version or package_version,
        "snapshot": snapshot,
    }


def manifest(
    *,
    x86_locks=None,
    arm_locks=None,
):
    architectures = {
        "x86_64": [apt_lock()] if x86_locks is None else x86_locks
    }
    if arm_locks is not None:
        architectures["aarch64"] = arm_locks
    return {
        "schema_version": 3,
        "streams": {
            "ubuntu-stable-generic": {
                "provider": "ubuntu",
                "release": "stable",
                "builder": f"ubuntu:test@sha256:{DIGEST}",
                "suite": "stable",
                "selector": "linux-image-generic",
                "architectures": architectures,
            }
        },
    }


def normalized_targets(document):
    return validate_catalog(document, "test lock").targets


def normalized_target(document, channel_id="ubuntu-stable-generic-x86-64"):
    return [
        item
        for item in normalized_targets(document)
        if item["channel_id"] == channel_id
    ][-1]


def fedora_artifacts(kernel_nvr, rust_nvr, arch="x86_64"):
    filenames = {
        f"kernel-{kernel_nvr}.src.rpm",
        f"rust-src-{rust_nvr}.noarch.rpm",
        *(
            f"{name}-{kernel_nvr}.{arch}.rpm"
            for name in ("kernel-core", "kernel-devel", "kernel-modules-core")
        ),
        *(
            f"{name}-{rust_nvr}.{arch}.rpm"
            for name in ("cargo", "rust", "rust-std-static", "rustfmt")
        ),
    }
    return {filename: DIGEST for filename in filenames}


def candidate_for(
    document,
    channel_id="ubuntu-stable-generic-x86-64",
    *,
    kernel_release=None,
    package_version=None,
    snapshot="20260730T000000Z",
):
    current = normalized_target(document, channel_id)
    kernel_release = kernel_release or current["kernel_release"]
    package_version = package_version or current["kernel_package_version"]
    return {
        "base_target_id": current["id"],
        "lock": apt_lock(
            kernel_release=kernel_release,
            package_version=package_version,
            snapshot=snapshot,
        ),
    }


class KernelTargetsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def write_json(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def run_controller(self, document, *arguments, succeeds=True):
        path = (
            document
            if isinstance(document, Path)
            else self.write_json("lock.json", document)
        )
        result = subprocess.run(
            [
                sys.executable,
                str(CONTROLLER),
                "--manifest",
                str(path),
                *map(str, arguments),
            ],
            cwd=REPOSITORY,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if succeeds and result.returncode:
            self.fail(result.stderr)
        if not succeeds and not result.returncode:
            self.fail("controller unexpectedly succeeded")
        return result

    def reconcile(
        self,
        document,
        *candidates,
        pending_base=None,
        pending_head=None,
        succeeds=True,
    ):
        path = self.write_json("reconciled.lock.json", document)
        arguments = ["reconcile"]
        if pending_base is not None:
            arguments.extend(
                [
                    "--pending-base",
                    self.write_json("pending-base.lock.json", pending_base),
                    "--pending-head",
                    self.write_json("pending-head.lock.json", pending_head),
                ]
            )
        for index, value in enumerate(candidates):
            arguments.append(self.write_json(f"candidate-{index}.json", value))
        result = self.run_controller(path, *arguments, succeeds=succeeds)
        if not succeeds:
            return result
        return json.loads(path.read_text(encoding="utf-8"))

    def two_arch_manifest(self):
        return manifest(arm_locks=[apt_lock()])

    def test_reconcile_noop_preserves_lock_exactly(self):
        document = manifest()
        updated = self.reconcile(document, candidate_for(document))
        self.assertEqual(updated, document)

    def test_field_exposes_validated_apt_suite(self):
        document = manifest()
        target = normalized_target(document)

        result = self.run_controller(document, "field", target["id"], "suite")

        self.assertEqual(result.stdout, "stable\n")

    def test_builder_image_is_resolved_by_provider_and_release(self):
        document = manifest()
        stream = document["streams"].pop("ubuntu-stable-generic")
        document["streams"]["renamed-stream"] = stream

        result = self.run_controller(
            document,
            "builder-image",
            "ubuntu",
            "stable",
        )

        self.assertEqual(
            result.stdout,
            f"ubuntu:test@sha256:{DIGEST}\n",
        )

    def test_builder_image_rejects_an_unknown_provider_release(self):
        result = self.run_controller(
            manifest(),
            "builder-image",
            "fedora",
            "stable",
            succeeds=False,
        )

        self.assertIn("no builder image matches", result.stderr)

    def test_builder_image_rejects_ambiguous_images(self):
        document = manifest()
        second = copy.deepcopy(document["streams"]["ubuntu-stable-generic"])
        second["builder"] = f"ubuntu:other@sha256:{'1' * 64}"
        document["streams"]["ubuntu-stable-hwe"] = second

        result = self.run_controller(
            document,
            "builder-image",
            "ubuntu",
            "stable",
            succeeds=False,
        )

        self.assertIn("multiple builder images match", result.stderr)

    def test_reconcile_same_uname_package_rebuild_replaces_lock(self):
        document = manifest()
        old_id = normalized_target(document)["id"]
        updated = self.reconcile(
            document,
            candidate_for(document, package_version="7.0.1-2"),
        )
        lock = updated["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ][0]
        self.assertEqual(lock["version"], "7.0.1-2")
        self.assertNotEqual(normalized_target(updated)["id"], old_id)

    def test_reconcile_accepts_distinct_apt_source_version(self):
        document = manifest()
        update = candidate_for(document, package_version="7.0.1-2+b1")
        update["lock"]["source_version"] = "7.0.1-2"

        updated = self.reconcile(document, update)

        lock = updated["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ][0]
        self.assertEqual(lock["version"], "7.0.1-2+b1")
        self.assertEqual(lock["source_version"], "7.0.1-2")

    def test_reconcile_accepts_ubuntu_source_name_from_candidate(self):
        document = manifest()
        update = candidate_for(document, package_version="7.0.1-2")
        update["lock"]["source_name"] = "linux-oem-7.0"

        updated = self.reconcile(document, update)

        lock = updated["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ][0]
        self.assertEqual(lock["source_name"], "linux-oem-7.0")

    def test_reconcile_retains_two_distinct_kernels_and_prunes_oldest(self):
        first = apt_lock(
            kernel_release="7.0.1-generic", package_version="7.0.1-1"
        )
        second = apt_lock(
            kernel_release="7.0.2-generic", package_version="7.0.2-1"
        )
        document = manifest(x86_locks=[first, second])
        updated = self.reconcile(
            document,
            candidate_for(
                document,
                kernel_release="7.0.3-generic",
                package_version="7.0.3-1",
            ),
        )
        self.assertEqual(
            [
                item["kernel"]
                for item in updated["streams"]["ubuntu-stable-generic"][
                    "architectures"
                ]["x86_64"]
            ],
            ["7.0.2-generic", "7.0.3-generic"],
        )

    def test_reconcile_rejects_rebuild_of_non_latest_retained_kernel(self):
        first = apt_lock(
            kernel_release="7.0.1-generic", package_version="7.0.1-1"
        )
        second = apt_lock(
            kernel_release="7.0.2-generic", package_version="7.0.2-1"
        )
        document = manifest(x86_locks=[first, second])
        result = self.reconcile(
            document,
            candidate_for(
                document,
                kernel_release="7.0.1-generic",
                package_version="7.0.1-2",
            ),
            succeeds=False,
        )
        self.assertIn("cannot replace non-latest retained kernel", result.stderr)

        pending = copy.deepcopy(document)
        locks = pending["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ]
        locks[:] = [
            apt_lock(
                kernel_release="7.0.1-generic",
                package_version="7.0.1-2",
            ),
            copy.deepcopy(second),
        ]
        result = self.reconcile(
            copy.deepcopy(document),
            pending_base=document,
            pending_head=pending,
            succeeds=False,
        )
        self.assertIn("cannot replace non-latest retained kernel", result.stderr)

    def test_reconcile_rejects_stale_and_duplicate_candidates(self):
        document = manifest()
        stale = candidate_for(document)
        stale["base_target_id"] = "stale-target"
        result = self.reconcile(document, stale, succeeds=False)
        self.assertIn("stale candidate", result.stderr)

        fresh = candidate_for(document)
        result = self.reconcile(document, fresh, fresh, succeeds=False)
        self.assertIn("multiple candidates", result.stderr)

    def test_reconcile_two_architectures_is_order_independent(self):
        document = self.two_arch_manifest()
        arm_id = "ubuntu-stable-generic-aarch64"
        x86 = candidate_for(document, package_version="7.0.1-2")
        arm = candidate_for(document, arm_id, package_version="7.0.1-2")
        forward = self.reconcile(copy.deepcopy(document), x86, arm)
        reverse = self.reconcile(copy.deepcopy(document), arm, x86)
        self.assertEqual(forward, reverse)

    def test_reconcile_carries_failed_channel_from_pending_pr(self):
        base = self.two_arch_manifest()
        arm_id = "ubuntu-stable-generic-aarch64"
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, arm_id, package_version="7.0.1-2"),
        )
        updated = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, package_version="7.0.1-3"),
            pending_base=base,
            pending_head=pending,
        )
        versions = {
            item["channel_id"]: item["kernel_package_version"]
            for item in normalized_targets(updated)
        }
        self.assertEqual(versions["ubuntu-stable-generic-x86-64"], "7.0.1-3")
        self.assertEqual(versions[arm_id], "7.0.1-2")

    def test_reconcile_keeps_pending_pr_when_all_discovery_fails(self):
        base = self.two_arch_manifest()
        arm_id = "ubuntu-stable-generic-aarch64"
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, arm_id, package_version="7.0.1-2"),
        )
        self.assertEqual(
            self.reconcile(
                copy.deepcopy(base),
                pending_base=base,
                pending_head=pending,
            ),
            pending,
        )

    def test_reconcile_keeps_pending_pr_after_noop_discovery(self):
        base = manifest()
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, package_version="7.0.1-2"),
        )

        self.assertEqual(
            self.reconcile(
                copy.deepcopy(base),
                candidate_for(base),
                pending_base=base,
                pending_head=pending,
            ),
            pending,
        )

    def test_reconcile_default_branch_wins_without_blocking_other_channels(self):
        base = self.two_arch_manifest()
        arm_id = "ubuntu-stable-generic-aarch64"
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, package_version="7.0.1-2"),
        )
        current = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, package_version="7.0.1-3"),
        )
        updated = self.reconcile(
            current,
            candidate_for(current, arm_id, package_version="7.0.1-4"),
            pending_base=base,
            pending_head=pending,
        )
        versions = {
            item["channel_id"]: item["kernel_package_version"]
            for item in normalized_targets(updated)
        }
        self.assertEqual(versions["ubuntu-stable-generic-x86-64"], "7.0.1-3")
        self.assertEqual(versions[arm_id], "7.0.1-4")

    def test_reconcile_discards_pending_removed_channel(self):
        base = self.two_arch_manifest()
        arm_id = "ubuntu-stable-generic-aarch64"
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, arm_id, package_version="7.0.1-2"),
        )
        current = copy.deepcopy(base)
        del current["streams"]["ubuntu-stable-generic"]["architectures"][
            "aarch64"
        ]

        updated = self.reconcile(
            current,
            candidate_for(current, package_version="7.0.1-3"),
            pending_base=base,
            pending_head=pending,
        )

        targets = normalized_targets(updated)
        self.assertEqual(
            [item["channel_id"] for item in targets],
            ["ubuntu-stable-generic-x86-64"],
        )
        self.assertEqual(targets[0]["kernel_package_version"], "7.0.1-3")

    def test_reconcile_rejects_pending_configuration_change(self):
        base = manifest()
        pending = self.reconcile(
            copy.deepcopy(base),
            candidate_for(base, package_version="7.0.1-2"),
        )
        pending["streams"]["ubuntu-stable-generic"]["builder"] = (
            f"ubuntu:new@sha256:{'1' * 64}"
        )
        result = self.reconcile(
            copy.deepcopy(base),
            pending_base=base,
            pending_head=pending,
            succeeds=False,
        )
        self.assertIn("changes non-lock configuration", result.stderr)

    def test_matrices_are_derived_from_stream_architectures(self):
        result = self.run_controller(self.two_arch_manifest(), "discovery-matrix")
        entries = json.loads(result.stdout)["include"]
        self.assertEqual(
            [entry["id"] for entry in entries],
            [
                "ubuntu-stable-generic-x86-64",
                "ubuntu-stable-generic-aarch64",
            ],
        )
        build = self.run_controller(self.two_arch_manifest(), "matrix")
        build_entries = json.loads(build.stdout)["include"]
        self.assertEqual(set(build_entries[0]), {"id", "arch", "runner"})

    def test_reconcile_rejects_mutated_pending_lock(self):
        base = manifest()
        pending = copy.deepcopy(base)
        lock = pending["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ][0]
        lock["snapshot"] = "20260730T000000Z"
        result = self.reconcile(
            copy.deepcopy(base),
            pending_base=base,
            pending_head=pending,
            succeeds=False,
        )
        self.assertIn("cannot change an existing locked package", result.stderr)

    def test_reconcile_rejects_pending_retention_shrink(self):
        base = manifest(
            x86_locks=[
                apt_lock(
                    kernel_release="7.0.1-generic",
                    package_version="7.0.1-1",
                ),
                apt_lock(
                    kernel_release="7.0.2-generic",
                    package_version="7.0.2-1",
                ),
            ]
        )
        pending = copy.deepcopy(base)
        del pending["streams"]["ubuntu-stable-generic"]["architectures"][
            "x86_64"
        ][0]

        result = self.reconcile(
            copy.deepcopy(base),
            pending_base=base,
            pending_head=pending,
            succeeds=False,
        )

        self.assertIn("cannot drop retained kernels below 2", result.stderr)

    def test_reconcile_allows_fedora_key_rollover_only_once(self):
        old_fingerprint = "a" * 40
        new_fingerprint = "b" * 40
        kernel_nvr = "7.1.5-200.fc44"
        source_rpm = f"kernel-{kernel_nvr}.src.rpm"
        base = {
            "schema_version": 3,
            "streams": {
                "fedora-44-kernel-core": {
                    "provider": "fedora",
                    "release": "44",
                    "builder": f"fedora:44@sha256:{DIGEST}",
                    "signing_fingerprint": old_fingerprint,
                    "architectures": {
                        "x86_64": [
                            {
                                "nvr": f"kernel-{kernel_nvr}",
                                "signing_fingerprint": old_fingerprint,
                                "artifacts": fedora_artifacts(
                                    kernel_nvr,
                                    "1.97.1-1.fc44",
                                ),
                            }
                        ]
                    },
                }
            },
        }
        configured = copy.deepcopy(base)
        stream = configured["streams"]["fedora-44-kernel-core"]
        stream["signing_fingerprint"] = new_fingerprint
        resigned = copy.deepcopy(configured)
        resigned_lock = resigned["streams"]["fedora-44-kernel-core"][
            "architectures"
        ]["x86_64"][0]
        resigned_lock["signing_fingerprint"] = new_fingerprint
        resigned_lock["artifacts"][source_rpm] = "1" * 64
        self.assertEqual(
            self.reconcile(
                copy.deepcopy(configured),
                pending_base=configured,
                pending_head=resigned,
            ),
            resigned,
        )

        same_key_mutation = copy.deepcopy(resigned)
        same_key_mutation["streams"]["fedora-44-kernel-core"][
            "architectures"
        ]["x86_64"][0]["artifacts"][source_rpm] = "2" * 64
        result = self.reconcile(
            copy.deepcopy(resigned),
            pending_base=resigned,
            pending_head=same_key_mutation,
            succeeds=False,
        )
        self.assertIn("cannot change an existing locked package", result.stderr)


if __name__ == "__main__":
    unittest.main()
