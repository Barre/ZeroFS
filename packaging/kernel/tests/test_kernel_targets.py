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


def channel(
    channel_id="ubuntu-stable-generic-x86-64",
    *,
    arch="x86_64",
):
    return {
        "id": channel_id,
        "distro": "ubuntu",
        "release": "stable",
        "family": "deb",
        "arch": arch,
        "flavor": "generic",
        "apt": {
            "codename": "stable",
            "suite": "stable",
            "component": "main",
        },
        "discovery": {
            "kind": "ubuntu-snapshot",
            "builder_image": f"ubuntu:test@sha256:{DIGEST}",
            "selector": "linux-image-generic",
            "suite": "stable",
        },
    }


def target(
    target_id="ubuntu-kernel-r1",
    *,
    channel_id="ubuntu-stable-generic-x86-64",
    revision=1,
    kernel_release="7.0.1-generic",
    package_version="7.0.1-1",
    selector_version=None,
    publish=False,
):
    return {
        "id": target_id,
        "enabled": True,
        "ci": True,
        "publish": publish,
        "channel_id": channel_id,
        "package_revision": str(revision),
        "kernel_release": kernel_release,
        "kernel_package_name": f"linux-image-{kernel_release}",
        "kernel_package_version": package_version,
        "kernel_selector_version": selector_version or package_version,
        "builder_image": f"ubuntu:test@sha256:{DIGEST}",
        "source": {
            "kind": "apt-snapshot",
            "identity": f"ubuntu:linux@{package_version}",
            "snapshot": "20260729T000000Z",
        },
    }


def manifest(channels=None, targets=None):
    return {
        "schema_version": 1,
        "channels": channels or [channel()],
        "targets": targets or [target()],
        "unsupported_targets": [],
    }


def candidate(
    *,
    channel_id="ubuntu-stable-generic-x86-64",
    base_target_id="ubuntu-kernel-r1",
    kernel_release="7.0.1-generic",
    package_version="7.0.1-1",
    selector_version=None,
    snapshot="20260730T000000Z",
):
    return {
        "schema_version": 1,
        "channel_id": channel_id,
        "base_target_id": base_target_id,
        "kernel_release": kernel_release,
        "kernel_package_name": f"linux-image-{kernel_release}",
        "kernel_package_version": package_version,
        "kernel_selector_version": selector_version or package_version,
        "source": {
            "kind": "apt-snapshot",
            "identity": f"ubuntu:linux@{package_version}",
            "snapshot": snapshot,
        },
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

    def run_controller(self, manifest_path, *arguments, succeeds=True):
        result = subprocess.run(
            [
                sys.executable,
                str(CONTROLLER),
                "--manifest",
                str(manifest_path),
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

    def apply(self, document, *candidates):
        manifest_path = self.write_json("targets.json", document)
        candidate_paths = [
            self.write_json(f"candidate-{index}.json", value)
            for index, value in enumerate(candidates)
        ]
        self.run_controller(manifest_path, "apply", *candidate_paths)
        return (
            json.loads(manifest_path.read_text(encoding="utf-8")),
            manifest_path.read_text(encoding="utf-8"),
        )

    def test_apply_promotes_current_without_snapshot_churn(self):
        document = manifest()
        original = copy.deepcopy(document["targets"][0])

        updated, _ = self.apply(document, candidate())

        self.assertEqual(len(updated["targets"]), 1)
        promoted = updated["targets"][0]
        self.assertTrue(promoted["enabled"])
        self.assertTrue(promoted["ci"])
        self.assertTrue(promoted["publish"])
        self.assertEqual(promoted["source"], original["source"])
        self.assertNotIn("family", promoted)
        self.assertNotIn("kernel_dependency", promoted)

    def test_apply_is_idempotent_for_published_target(self):
        document = manifest(targets=[target(publish=True)])

        _, first = self.apply(document, candidate())
        _, second = self.apply(json.loads(first), candidate())

        self.assertEqual(first, second)

    def test_apply_appends_same_uname_package_rebuild(self):
        updated, _ = self.apply(
            manifest(),
            candidate(package_version="7.0.1-2"),
        )

        old, new = updated["targets"]
        self.assertTrue(old["enabled"])
        self.assertFalse(old["ci"])
        self.assertFalse(old["publish"])
        self.assertEqual(
            new["id"],
            "ubuntu-stable-generic-x86-64-7.0.1-generic-r2",
        )
        self.assertEqual(new["package_revision"], "2")
        self.assertEqual(new["kernel_release"], old["kernel_release"])
        self.assertEqual(new["kernel_package_version"], "7.0.1-2")
        self.assertEqual(
            new["builder_image"],
            updated["channels"][0]["discovery"]["builder_image"],
        )
        self.assertTrue(new["publish"])

    def test_apply_persists_selector_only_update(self):
        updated, _ = self.apply(
            manifest(),
            candidate(selector_version="7.0.1-2"),
        )

        self.assertEqual(len(updated["targets"]), 2)
        self.assertEqual(
            updated["targets"][-1]["kernel_selector_version"],
            "7.0.1-2",
        )
        self.assertEqual(
            updated["targets"][-1]["kernel_package_version"],
            "7.0.1-1",
        )

    def test_apply_appends_when_builder_image_changes(self):
        document = manifest()
        builder_image = f"ubuntu:new@sha256:{'1' * 64}"
        document["channels"][0]["discovery"]["builder_image"] = builder_image

        updated, _ = self.apply(document, candidate())

        old, new = updated["targets"]
        self.assertFalse(old["ci"])
        self.assertFalse(old["publish"])
        self.assertEqual(new["package_revision"], "2")
        self.assertEqual(
            new["kernel_package_version"],
            old["kernel_package_version"],
        )
        self.assertEqual(new["builder_image"], builder_image)
        self.assertTrue(new["publish"])

    def test_runtime_matrix_rejects_builder_image_drift(self):
        document = manifest()
        builder_image = f"ubuntu:new@sha256:{'1' * 64}"
        document["channels"][0]["discovery"]["builder_image"] = builder_image
        path = self.write_json("builder-image-drift.json", document)

        result = self.run_controller(
            path,
            "matrix",
            "--scope",
            "ci",
            succeeds=False,
        )

        self.assertIn("newest target 'ubuntu-kernel-r1'", result.stderr)
        self.assertIn("apply a candidate", result.stderr)

        discovery = self.run_controller(
            path,
            "matrix",
            "--scope",
            "discover",
        )
        entry = json.loads(discovery.stdout)["include"][0]
        self.assertEqual(entry["id"], "ubuntu-stable-generic-x86-64")

    def test_apply_orders_channels_independently_of_arguments(self):
        second_channel = channel(
            "ubuntu-stable-generic-aarch64",
            arch="aarch64",
        )
        second_target = target(
            "ubuntu-arm-kernel-r1",
            channel_id=second_channel["id"],
        )
        document = manifest(
            channels=[second_channel, channel()],
            targets=[second_target, target()],
        )
        first_candidate = candidate(package_version="7.0.2-1")
        second_candidate = candidate(
            channel_id=second_channel["id"],
            base_target_id=second_target["id"],
            package_version="7.0.2-1",
        )

        _, forward = self.apply(
            copy.deepcopy(document),
            first_candidate,
            second_candidate,
        )
        _, reverse = self.apply(
            copy.deepcopy(document),
            second_candidate,
            first_candidate,
        )

        self.assertEqual(forward, reverse)
        appended = [
            item["channel_id"] for item in json.loads(forward)["targets"][2:]
        ]
        self.assertEqual(appended, sorted(appended))

    def test_apply_rejects_stale_and_duplicate_candidates(self):
        path = self.write_json("targets.json", manifest())
        stale = candidate(base_target_id="old-target")
        stale_path = self.write_json("stale.json", stale)
        result = self.run_controller(
            path,
            "apply",
            stale_path,
            succeeds=False,
        )
        self.assertIn("stale candidate", result.stderr)

        current_path = self.write_json("current.json", candidate())
        result = self.run_controller(
            path,
            "apply",
            current_path,
            current_path,
            succeeds=False,
        )
        self.assertIn("multiple candidates", result.stderr)

    def test_apply_rejects_retired_package(self):
        old = target(
            "ubuntu-kernel-r1",
            revision=1,
            package_version="7.0.1-1",
        )
        current = target(
            "ubuntu-kernel-r2",
            revision=2,
            package_version="7.0.2-1",
        )
        old["ci"] = False
        document = manifest(targets=[old, current])
        rollback = candidate(
            base_target_id=current["id"],
            package_version=old["kernel_package_version"],
        )

        path = self.write_json("targets.json", document)
        candidate_path = self.write_json("rollback.json", rollback)
        result = self.run_controller(
            path,
            "apply",
            candidate_path,
            succeeds=False,
        )

        self.assertIn("matches retired target", result.stderr)

    def test_apply_rejects_malformed_candidate(self):
        malformed = candidate()
        malformed["builder_image"] = f"ubuntu:test@sha256:{DIGEST}"
        path = self.write_json("targets.json", manifest())
        candidate_path = self.write_json("candidate.json", malformed)

        result = self.run_controller(
            path,
            "apply",
            candidate_path,
            succeeds=False,
        )

        self.assertIn("expected fields", result.stderr)

    def test_apply_enforces_apt_package_name(self):
        malformed = candidate()
        malformed["kernel_package_name"] = "linux-image-other"
        path = self.write_json("targets.json", manifest())
        candidate_path = self.write_json("candidate.json", malformed)

        result = self.run_controller(
            path,
            "apply",
            candidate_path,
            succeeds=False,
        )

        self.assertIn("kernel_package_name: expected", result.stderr)

    def test_fedora_candidate_must_use_signed_koji_source(self):
        fingerprint = "a" * 40
        builder_image = f"fedora:44@sha256:{DIGEST}"
        channel_id = "fedora-44-kernel-core-x86-64"
        kernel_nvr = "7.1.5-200.fc44"
        rust_nvr = "1.97.1-1.fc44"
        artifacts = {
            f"kernel-{kernel_nvr}.src.rpm": DIGEST,
            f"rust-src-{rust_nvr}.noarch.rpm": DIGEST,
        }
        artifacts.update(
            {
                f"{name}-{kernel_nvr}.x86_64.rpm": DIGEST
                for name in (
                    "kernel-core",
                    "kernel-devel",
                    "kernel-modules-core",
                )
            }
        )
        artifacts.update(
            {
                f"{name}-{rust_nvr}.x86_64.rpm": DIGEST
                for name in (
                    "cargo",
                    "rust",
                    "rust-std-static",
                    "rustfmt",
                )
            }
        )
        fedora_channel = {
            "id": channel_id,
            "distro": "fedora",
            "release": "44",
            "family": "rpm",
            "arch": "x86_64",
            "flavor": "kernel-core",
            "rpm": {"repo_id": "zerofs-fedora-44"},
            "discovery": {
                "kind": "fedora-koji",
                "builder_image": builder_image,
                "selector": "kernel-core",
                "signing_fingerprint": fingerprint,
            },
        }
        fedora_target = {
            "id": "fedora-kernel-r1",
            "enabled": True,
            "ci": True,
            "publish": False,
            "channel_id": channel_id,
            "package_revision": "1",
            "kernel_release": f"{kernel_nvr}.x86_64",
            "kernel_package_name": "kernel-core-uname-r",
            "kernel_package_version": f"{kernel_nvr}.x86_64",
            "kernel_selector_version": kernel_nvr,
            "builder_image": builder_image,
            "source": {
                "kind": "koji",
                "identity": f"kernel-{kernel_nvr}",
                "snapshot": "koji-download-build:x86_64,noarch,src",
                "artifacts": artifacts,
            },
        }
        base = manifest(
            channels=[fedora_channel],
            targets=[fedora_target],
        )
        wrong_selector = copy.deepcopy(base)
        wrong_selector["targets"][0]["kernel_selector_version"] = "1"
        wrong_selector_path = self.write_json(
            "wrong-fedora-selector.json",
            wrong_selector,
        )
        result = self.run_controller(
            wrong_selector_path,
            "matrix",
            "--scope",
            "ci",
            succeeds=False,
        )
        self.assertIn("expected Fedora kernel NVR", result.stderr)

        signed = {
            "schema_version": 1,
            "channel_id": channel_id,
            "base_target_id": fedora_target["id"],
            "kernel_release": fedora_target["kernel_release"],
            "kernel_package_name": fedora_target["kernel_package_name"],
            "kernel_package_version": fedora_target[
                "kernel_package_version"
            ],
            "kernel_selector_version": fedora_target[
                "kernel_selector_version"
            ],
            "source": copy.deepcopy(fedora_target["source"]),
        }
        path = self.write_json("targets.json", base)
        unsigned_path = self.write_json("unsigned.json", signed)
        result = self.run_controller(
            path,
            "apply",
            unsigned_path,
            succeeds=False,
        )
        self.assertIn("source.snapshot: expected", result.stderr)

        signed["source"]["snapshot"] = (
            f"koji-signed-build:{fingerprint}:x86_64,noarch,src"
        )
        signed_base = copy.deepcopy(base)
        signed_base["targets"][0]["source"] = copy.deepcopy(signed["source"])
        changed_hash = copy.deepcopy(signed)
        artifact = next(iter(changed_hash["source"]["artifacts"]))
        changed_hash["source"]["artifacts"][artifact] = "1" * 64
        manifest_path = self.write_json("signed-base.json", signed_base)
        candidate_path = self.write_json("changed-hash.json", changed_hash)
        result = self.run_controller(
            manifest_path,
            "apply",
            candidate_path,
            succeeds=False,
        )
        self.assertIn("artifact hashes changed", result.stderr)

        updated, _ = self.apply(copy.deepcopy(base), signed)
        self.assertEqual(len(updated["targets"]), 2)
        self.assertEqual(
            updated["targets"][-1]["source"]["snapshot"],
            signed["source"]["snapshot"],
        )

    def test_opensuse_upgrade_gate_uses_kernel_default_version(self):
        channel_id = "opensuse-tumbleweed-default-x86-64"
        builder_image = "opensuse/tumbleweed:latest"
        packages = [
            "kernel-default",
            "kernel-default-devel",
            "kernel-devel",
            "kernel-source",
            "kernel-syms",
        ]
        channel_value = {
            "id": channel_id,
            "distro": "opensuse",
            "release": "tumbleweed",
            "family": "rpm",
            "arch": "x86_64",
            "flavor": "default",
            "rpm": {"repo_id": "zerofs-opensuse"},
            "discovery": {
                "kind": "opensuse-history",
                "builder_image": builder_image,
                "selector": "kernel-default",
                "packages": packages,
            },
        }
        version = "7.1.4-1.1"
        target_value = {
            "id": "opensuse-kernel-r1",
            "enabled": True,
            "ci": True,
            "publish": False,
            "channel_id": channel_id,
            "package_revision": "1",
            "kernel_release": "7.1.4-1-default",
            "kernel_package_name": "kernel-default",
            "kernel_package_version": version,
            "kernel_selector_version": version,
            "builder_image": builder_image,
            "source": {
                "kind": "opensuse-history",
                "identity": ",".join(
                    f"{package}@{version}" for package in packages
                ),
                "snapshot": "20260729",
            },
        }
        path = self.write_json(
            "opensuse.json",
            manifest(channels=[channel_value], targets=[target_value]),
        )

        result = self.run_controller(
            path,
            "field",
            target_value["id"],
            "kernel_upgrade_conflict",
        )
        self.assertEqual(result.stdout.strip(), f"kernel-default > {version}")

        target_value["kernel_selector_version"] = "1"
        path = self.write_json(
            "opensuse-wrong-selector.json",
            manifest(channels=[channel_value], targets=[target_value]),
        )
        result = self.run_controller(
            path,
            "matrix",
            "--scope",
            "ci",
            succeeds=False,
        )
        self.assertIn("must match kernel-default version", result.stderr)

    def test_builder_image_must_be_an_oci_name(self):
        document = manifest()
        document["channels"][0]["discovery"]["builder_image"] = (
            f"--privileged@sha256:{DIGEST}"
        )
        path = self.write_json("targets.json", document)

        result = self.run_controller(
            path,
            "matrix",
            "--scope",
            "ci",
            succeeds=False,
        )

        self.assertIn("digest-pinned OCI image name", result.stderr)

    def test_tagged_builder_image_is_rejected_outside_opensuse(self):
        document = manifest()
        document["channels"][0]["discovery"]["builder_image"] = "ubuntu:test"
        path = self.write_json("tagged-builder.json", document)

        result = self.run_controller(
            path,
            "matrix",
            "--scope",
            "ci",
            succeeds=False,
        )

        self.assertIn("digest-pinned OCI image name", result.stderr)

    def test_fields_expose_upgrade_gate(self):
        document = manifest(
            targets=[
                target(
                    package_version="7.0.1-1",
                    selector_version="7.0.1.2",
                )
            ]
        )
        path = self.write_json("targets.json", document)

        field_result = self.run_controller(
            path,
            "field",
            "ubuntu-kernel-r1",
            "kernel_upgrade_conflict",
        )
        self.assertEqual(
            field_result.stdout.strip(),
            "linux-image-generic (>> 7.0.1.2)",
        )

    def test_kind_type_errors_are_reported_without_tracebacks(self):
        for location in ("discovery", "source"):
            with self.subTest(location=location):
                document = manifest()
                if location == "discovery":
                    document["channels"][0]["discovery"]["kind"] = []
                else:
                    document["targets"][0]["source"]["kind"] = []
                path = self.write_json(f"{location}.json", document)

                result = self.run_controller(
                    path,
                    "matrix",
                    "--scope",
                    "ci",
                    succeeds=False,
                )

                self.assertIn(".kind: must be a non-empty", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_published_tolerates_reviewed_channel_changes(self):
        base = manifest()
        current = copy.deepcopy(base)
        current["channels"][0]["apt"]["suite"] = "kernel"
        current["targets"][0]["publish"] = True
        base_path = self.write_json("base.json", base)
        current_path = self.write_json("current.json", current)

        result = self.run_controller(
            current_path,
            "published",
            "--base",
            base_path,
        )

        self.assertEqual(
            json.loads(result.stdout)["include"][0]["id"],
            "ubuntu-kernel-r1",
        )

    def test_published_rejects_existing_target_changes(self):
        base = manifest(targets=[target(publish=True)])
        current = copy.deepcopy(base)
        builder_image = f"ubuntu:changed@sha256:{'1' * 64}"
        current["channels"][0]["discovery"]["builder_image"] = builder_image
        current["targets"][0]["builder_image"] = builder_image
        base_path = self.write_json("base.json", base)
        current_path = self.write_json("current.json", current)

        result = self.run_controller(
            current_path,
            "published",
            "--base",
            base_path,
            succeeds=False,
        )

        self.assertIn("existing target cannot be changed", result.stderr)

    def test_published_accepts_replacement_for_drifted_base(self):
        base = manifest(targets=[target(publish=True)])
        builder_image = f"ubuntu:new@sha256:{'1' * 64}"
        base["channels"][0]["discovery"]["builder_image"] = builder_image
        current = copy.deepcopy(base)
        current["targets"][0]["ci"] = False
        current["targets"][0]["publish"] = False
        replacement = target(
            "ubuntu-kernel-r2",
            revision=2,
            publish=True,
        )
        replacement["builder_image"] = builder_image
        current["targets"].append(replacement)
        base_path = self.write_json("drifted-base.json", base)
        current_path = self.write_json("replacement-current.json", current)

        result = self.run_controller(
            current_path,
            "published",
            "--base",
            base_path,
        )

        entries = json.loads(result.stdout)["include"]
        self.assertEqual([entry["id"] for entry in entries], ["ubuntu-kernel-r2"])

    def test_published_rejects_removed_targets(self):
        old = target("ubuntu-kernel-r1", revision=1)
        old["ci"] = False
        current = target(
            "ubuntu-kernel-r2",
            revision=2,
            kernel_release="7.0.2-generic",
            package_version="7.0.2-1",
            publish=True,
        )
        base = manifest(targets=[old, current])
        changed = manifest(targets=[current])
        base_path = self.write_json("base.json", base)
        current_path = self.write_json("current.json", changed)

        result = self.run_controller(
            current_path,
            "published",
            "--base",
            base_path,
            succeeds=False,
        )

        self.assertIn("existing targets cannot be removed", result.stderr)

    def test_discovery_matrix_comes_from_channels(self):
        path = self.write_json("targets.json", manifest())

        result = self.run_controller(
            path,
            "matrix",
            "--scope",
            "discover",
        )

        self.assertEqual(
            json.loads(result.stdout),
            {
                "include": [
                    {
                        "id": "ubuntu-stable-generic-x86-64",
                        "runner": "ubuntu-26.04",
                        "image": f"ubuntu:test@sha256:{DIGEST}",
                    }
                ]
            },
        )


if __name__ == "__main__":
    unittest.main()
