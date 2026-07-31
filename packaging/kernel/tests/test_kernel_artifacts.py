import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from test_kernel_targets import channel, manifest, target


REPOSITORY = Path(__file__).resolve().parents[3]
STAGER = REPOSITORY / "packaging/kernel/kernel-artifacts.py"
COMMIT = "a" * 40
TOOLING_COMMIT = "b" * 40
VERSION = "1.4.1"


class KernelArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def artifact(self, target_value, channel_value, *, files=None):
        """Write a build output directory that matches the catalog target."""
        directory = self.root / f"artifact-{target_value['id']}"
        directory.mkdir()
        family = channel_value["family"]
        architecture = {
            ("deb", "x86_64"): "amd64",
            ("deb", "aarch64"): "arm64",
            ("rpm", "x86_64"): "x86_64",
            ("rpm", "aarch64"): "aarch64",
        }[(family, channel_value["arch"])]
        slug = target_value["id"]
        kernel_slug = target_value["kernel_release"].replace("_", "-")
        full_version = f"{VERSION}-{target_value['package_revision']}"
        if family == "deb":
            payload_base = f"zerofs-kernel-client-{slug}-{kernel_slug}"
            payload_name = f"{payload_base}_{full_version}_{architecture}.deb"
            selector_name = (
                f"zerofs-kernel-client_{full_version}_{slug}"
                f"_{kernel_slug}_{architecture}.deb"
            )
        else:
            payload_base = f"zerofs-kernel-client-{slug}-{kernel_slug}"
            payload_name = f"{payload_base}-{full_version}.{architecture}.rpm"
            selector_name = (
                f"zerofs-kernel-client-{full_version}.{slug}"
                f".{kernel_slug}.{architecture}.rpm"
            )

        payload = {
            payload_name: b"payload package\n",
            selector_name: b"selector package\n",
            "zerofs.ko": b"module\n",
            "vmlinuz": b"kernel image\n",
            "kernel.config": b"kernel config\n",
            "Module.symvers": b"symbol versions\n",
            "build-info": b"build information\n",
            "busybox": b"busybox\n",
            "module-dependencies/netfs.ko": b"netfs module\n",
            "boot-modules/virtio.ko": b"virtio module\n",
            "zerofs-module-signing-cert.der": b"certificate\n",
        }
        payload.update(files or {})
        for name, data in payload.items():
            path = directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        digests = {
            name: hashlib.sha256(data).hexdigest()
            for name, data in payload.items()
        }

        artifact = {
            "schema_version": 2,
            "target_id": target_value["id"],
            "kernel_release": target_value["kernel_release"],
            "kernel_package_version": target_value["kernel_package_version"],
            "kernel_selector_version": target_value[
                "kernel_selector_version"
            ],
            "channel_id": target_value["channel_id"],
            "package_revision": target_value["package_revision"],
            "zerofs_version": VERSION,
            "family": family,
            "arch": channel_value["arch"],
            "builder_image": target_value["builder_image"],
            "source": target_value["source"],
            "source_commit": COMMIT,
            "source_tree_state": "clean",
            "tooling_commit": TOOLING_COMMIT,
            "tooling_tree_state": "clean",
            "module": "zerofs.ko",
            "payload_package": payload_name,
            "selector_package": selector_name,
            "kernel_image": "vmlinuz",
            "kernel_config": "kernel.config",
            "module_symvers": "Module.symvers",
            "build_info": "build-info",
            "boot_busybox": "busybox",
            "module_dependencies": ["module-dependencies/netfs.ko"],
            "boot_modules": ["boot-modules/virtio.ko"],
            "module_signing": {
                "certificate": "zerofs-module-signing-cert.der",
                "certificate_sha256": digests["zerofs-module-signing-cert.der"],
                "signature_id": "PKCS#7",
                "signer": "ZeroFS module signing",
                "key": "00:11",
                "hash_algorithm": "sha256",
            },
            "sha256": digests,
        }
        (directory / "artifact.json").write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        return directory, artifact

    def edit_artifact(self, directory, **changes):
        path = directory / "artifact.json"
        artifact = json.loads(path.read_text(encoding="utf-8"))
        artifact.update(changes)
        path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")

    def stage(self, document, artifacts, *, succeeds=True, version=VERSION):
        manifest_path = self.root / "targets.json"
        manifest_path.write_text(
            json.dumps(document, indent=2) + "\n",
            encoding="utf-8",
        )
        output = self.root / "staged"
        arguments = [
            sys.executable,
            str(STAGER),
            "--manifest",
            str(manifest_path),
            "--version",
            version,
            "--source-commit",
            COMMIT,
            "--tooling-commit",
            TOOLING_COMMIT,
            "--output",
            str(output),
        ]
        for target_id, directory in artifacts.items():
            arguments.extend(("--artifact", f"{target_id}={directory}"))
        result = subprocess.run(
            arguments,
            cwd=REPOSITORY,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if succeeds and result.returncode:
            self.fail(result.stderr)
        if not succeeds and not result.returncode:
            self.fail("staging unexpectedly succeeded")
        return result, output

    def published(self, **overrides):
        channel_value = channel()
        target_value = target(publish=True, **overrides)
        document = manifest(channels=[channel_value], targets=[target_value])
        directory, _ = self.artifact(target_value, channel_value)
        return document, target_value, directory

    def test_stages_packages_and_emits_channel_matrix(self):
        document, target_value, directory = self.published()
        result, output = self.stage(document, {target_value["id"]: directory})

        matrix = json.loads(result.stdout)
        self.assertEqual(len(matrix["include"]), 1)
        entry = matrix["include"][0]
        self.assertEqual(entry["id"], target_value["id"])
        self.assertEqual(entry["family"], "deb")
        self.assertEqual(
            entry["prefix"],
            "kernel/apt/ubuntu/stable/generic/x86_64",
        )
        self.assertEqual(entry["codename"], "stable")
        self.assertEqual(entry["component"], "main")
        self.assertEqual(entry["architectures"], "amd64")
        self.assertEqual(entry["probe_package"], "zerofs-kernel-client")
        self.assertEqual(entry["probe_version"], f"{VERSION}-1")
        self.assertEqual(
            entry["descriptor_key"],
            "kernel/apt/ubuntu/stable/generic/x86_64/zerofs-kernel.list",
        )

        staged = sorted(path.name for path in (output / target_value["id"]).iterdir())
        self.assertEqual(len(staged), 2)
        self.assertTrue(all(name.endswith(".deb") for name in staged))

    def test_emits_rpm_version_and_architecture(self):
        channel_id = "opensuse-tumbleweed-default-x86-64"
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
                "builder_image": f"opensuse:test@sha256:{'0' * 64}",
                "selector": "kernel-default",
                "packages": [
                    "kernel-default",
                    "kernel-default-devel",
                    "kernel-devel",
                    "kernel-source",
                    "kernel-syms",
                ],
            },
        }
        target_value = target(
            target_id="opensuse-kernel-r7",
            channel_id=channel_id,
            revision=7,
            kernel_release="7.1.4-1-default",
            package_version="7.1.4-1",
            publish=True,
        )
        target_value["builder_image"] = channel_value["discovery"][
            "builder_image"
        ]
        target_value["source"] = {
            "kind": "opensuse-history",
            "identity": "opensuse:kernel-default@7.1.4-1",
            "snapshot": "20260728",
        }
        document = manifest(
            channels=[channel_value],
            targets=[target_value],
        )
        directory, _ = self.artifact(target_value, channel_value)

        result, _ = self.stage(document, {target_value["id"]: directory})

        entry = json.loads(result.stdout)["include"][0]
        self.assertEqual(entry["family"], "rpm")
        self.assertEqual(entry["architectures"], "x86_64")
        self.assertEqual(entry["probe_version"], f"{VERSION}-7")

    def test_publishes_module_signing_material(self):
        document, target_value, directory = self.published()
        _, output = self.stage(document, {target_value["id"]: directory})

        certificate = output / "zerofs-module-signing-cert.der"
        self.assertEqual(certificate.read_bytes(), b"certificate\n")
        fingerprint = (
            output / "zerofs-module-signing-cert.fingerprint"
        ).read_text(encoding="ascii").strip()
        expected = hashlib.sha256(b"certificate\n").hexdigest().upper()
        self.assertEqual(fingerprint.replace(":", ""), expected)

    def test_rejects_unpublished_target(self):
        channel_value = channel()
        target_value = target(publish=False)
        document = manifest(channels=[channel_value], targets=[target_value])
        directory, _ = self.artifact(target_value, channel_value)
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("not authorized for publication", result.stderr)

    def test_rejects_artifact_from_another_release(self):
        document, target_value, directory = self.published()
        self.edit_artifact(directory, zerofs_version="9.9.9")
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.zerofs_version", result.stderr)

    def test_rejects_artifact_from_another_commit(self):
        document, target_value, directory = self.published()
        self.edit_artifact(directory, source_commit="c" * 40)
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.source_commit", result.stderr)

    def test_rejects_artifact_from_another_tooling_commit(self):
        document, target_value, directory = self.published()
        self.edit_artifact(directory, tooling_commit="c" * 40)
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.tooling_commit", result.stderr)

    def test_rejects_missing_payload_package_digest(self):
        document, target_value, directory = self.published()
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        del artifact["sha256"][artifact["payload_package"]]
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("must cover every artifact path exactly", result.stderr)

    def test_rejects_missing_selector_package_digest(self):
        document, target_value, directory = self.published()
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        del artifact["sha256"][artifact["selector_package"]]
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("must cover every artifact path exactly", result.stderr)

    def test_rejects_missing_signing_certificate_digest(self):
        document, target_value, directory = self.published()
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        certificate = artifact["module_signing"]["certificate"]
        del artifact["sha256"][certificate]
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("must cover every artifact path exactly", result.stderr)

    def test_rejects_extra_digest_record(self):
        document, target_value, directory = self.published()
        extra = directory / "unreferenced"
        extra.write_bytes(b"unreferenced\n")
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        artifact["sha256"][extra.name] = hashlib.sha256(
            extra.read_bytes()
        ).hexdigest()
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("must cover every artifact path exactly", result.stderr)

    def test_rejects_artifact_for_another_kernel(self):
        document, target_value, directory = self.published()
        self.edit_artifact(directory, kernel_release="7.0.2-generic")
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.kernel_release", result.stderr)

    def test_rejects_dirty_source_tree(self):
        document, target_value, directory = self.published()
        self.edit_artifact(directory, source_tree_state="dirty")
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.source_tree_state", result.stderr)

    def test_rejects_tampered_package(self):
        document, target_value, directory = self.published()
        package = next(directory.glob("zerofs-kernel-client_*.deb"))
        package.write_bytes(b"replaced payload\n")
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("file digest does not match", result.stderr)

    def test_rejects_missing_recorded_file(self):
        document, target_value, directory = self.published()
        (directory / "zerofs.ko").unlink()
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("is not a regular file", result.stderr)

    def test_rejects_path_escaping_the_artifact_directory(self):
        document, target_value, directory = self.published()
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        digest = artifact["sha256"].pop(artifact["module"])
        artifact["module"] = "../escape"
        artifact["sha256"]["../escape"] = digest
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("normalized relative path", result.stderr)

    def test_rejects_symlinked_payload(self):
        document, target_value, directory = self.published()
        secret = self.root / "secret"
        secret.write_bytes(b"module\n")
        (directory / "zerofs.ko").unlink()
        (directory / "zerofs.ko").symlink_to(secret)
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("symbolic links are not allowed", result.stderr)

    def test_rejects_unsigned_artifact(self):
        document, target_value, directory = self.published()
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        del artifact["module_signing"]
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("must be signed", result.stderr)

    def test_rejects_mismatched_package_filename(self):
        document, target_value, directory = self.published()
        # A package built for revision 2 must not publish as revision 1.
        original = next(directory.glob("zerofs-kernel-client_*.deb"))
        renamed = directory / original.name.replace(f"{VERSION}-1", f"{VERSION}-2")
        original.rename(renamed)
        artifact_path = directory / "artifact.json"
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        artifact["sha256"][renamed.name] = artifact["sha256"].pop(original.name)
        artifact["selector_package"] = renamed.name
        artifact_path.write_text(
            json.dumps(artifact, indent=2) + "\n",
            encoding="utf-8",
        )
        result, _ = self.stage(
            document,
            {target_value["id"]: directory},
            succeeds=False,
        )
        self.assertIn("artifact.selector_package", result.stderr)

    def test_rejects_divergent_signing_certificates(self):
        first_channel = channel()
        second_channel = channel("ubuntu-stable-generic-arm64", arch="aarch64")
        first = target(publish=True)
        second = target(
            "ubuntu-kernel-arm-r1",
            channel_id="ubuntu-stable-generic-arm64",
            publish=True,
        )
        document = manifest(
            channels=[first_channel, second_channel],
            targets=[first, second],
        )
        first_directory, _ = self.artifact(first, first_channel)
        second_directory, _ = self.artifact(
            second,
            second_channel,
            files={"zerofs-module-signing-cert.der": b"different certificate\n"},
        )
        result, _ = self.stage(
            document,
            {
                first["id"]: first_directory,
                second["id"]: second_directory,
            },
            succeeds=False,
        )
        self.assertIn("different module-signing certificates", result.stderr)


if __name__ == "__main__":
    unittest.main()
