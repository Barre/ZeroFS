import importlib.util
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
PACKAGER = REPOSITORY / "packaging/kernel/kernel-source-package.py"
RESOLVER = REPOSITORY / "packaging/kernel/scripts/dkms-find-kernel-source.sh"
FETCHER = REPOSITORY / "packaging/kernel/scripts/dkms-fetch-module.sh"
STAGER = REPOSITORY / "packaging/kernel/stage-prebuilt-modules.py"
SPEC = importlib.util.spec_from_file_location("kernel_source_package", PACKAGER)
assert SPEC is not None and SPEC.loader is not None
kernel_source_package = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kernel_source_package)
sys.path.insert(0, str(STAGER.parent))
STAGER_SPEC = importlib.util.spec_from_file_location("stage_prebuilt_modules", STAGER)
assert STAGER_SPEC is not None and STAGER_SPEC.loader is not None
stage_prebuilt_modules = importlib.util.module_from_spec(STAGER_SPEC)
STAGER_SPEC.loader.exec_module(stage_prebuilt_modules)


class KernelSourcePackageTest(unittest.TestCase):
    def write_executable(self, path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8")
        path.chmod(0o755)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fake_bin = self.root / "bin"
        self.fake_bin.mkdir()
        fake_nfpm = self.fake_bin / "nfpm"
        self.write_executable(
            fake_nfpm,
            """#!/usr/bin/env python3
import os
import pathlib
import sys
import tarfile

if os.environ.get("SOURCE_DATE_EPOCH") != "42":
    raise SystemExit("SOURCE_DATE_EPOCH was not preserved")
arguments = sys.argv[1:]
target = pathlib.Path(arguments[arguments.index("-t") + 1])
root = pathlib.Path.cwd()
with tarfile.open(target, "w") as archive:
    for path in sorted(root.rglob("*")):
        if path == target:
            continue
        archive.add(path, arcname=path.relative_to(root), recursive=False)
""",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_package_revision_overrides_are_explicit(self):
        self.assertEqual(kernel_source_package.package_revision("2.2.3"), 2)
        self.assertEqual(kernel_source_package.package_revision("9.8.7"), 1)

    def test_builds_one_architecture_independent_package_per_family(self):
        output = self.root / "output"
        version = kernel_source_package.repository_version(REPOSITORY)
        dkms_version = kernel_source_package.package_version(version)
        environment = {
            **os.environ,
            "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SOURCE_DATE_EPOCH": "42",
        }
        subprocess.run(
            [
                sys.executable,
                str(PACKAGER),
                "--output",
                str(output),
            ],
            cwd=REPOSITORY,
            env=environment,
            check=True,
            text=True,
            stdout=subprocess.DEVNULL,
        )
        packages = {
            output / f"deb/zerofs-kernel-client_{dkms_version}_all.deb",
            output / f"rpm/zerofs-kernel-client-{dkms_version}.noarch.rpm",
        }
        self.assertTrue(all(package.is_file() for package in packages))

        deb = next(package for package in packages if package.suffix == ".deb")
        with tarfile.open(deb) as archive:
            self.assertTrue(all(member.mtime == 42 for member in archive.getmembers()))
            names = set(archive.getnames())
            prefix = "content/source"
            self.assertIn(f"{prefix}/dkms.conf", names)
            self.assertIn(f"{prefix}/dkms-build", names)
            self.assertIn(f"{prefix}/dkms-fetch-module", names)
            self.assertIn(f"{prefix}/dkms-find-kernel-source", names)
            self.assertIn(
                f"{prefix}/zerofs-module-signing-cert.pem",
                names,
            )
            self.assertIn(f"{prefix}/zerofs-module-layout", names)
            self.assertIn(f"{prefix}/kernel/Makefile", names)
            self.assertIn(f"{prefix}/kernel/self_contained/build.sh", names)
            self.assertIn(f"{prefix}/zerofs/ninep-proto/src/lib.rs", names)
            self.assertNotIn(f"{prefix}/.zerofs-module-source", names)
            self.assertFalse(any(name.endswith(".ko") for name in names))
            self.assertFalse(any(name.endswith("kernels.lock.json") for name in names))
            self.assertNotIn("content/provenance.json", names)
            manifest_file = archive.extractfile("nfpm.yaml")
            self.assertIsNotNone(manifest_file)
            manifest = manifest_file.read().decode()
            self.assertIn('license: "AGPL-3.0"\n', manifest)
            self.assertIn(
                "  - src: ./content/source\n"
                f'    dst: "/usr/src/zerofs-{dkms_version}"\n'
                "    type: tree\n",
                manifest,
            )
            self.assertNotIn("./content/source/kernel/Makefile", manifest)
            self.assertNotIn("zerofs-kernel-module", manifest)
            self.assertNotIn("replaces:", manifest)
            self.assertNotIn("conflicts:", manifest)
            config = archive.extractfile(f"{prefix}/dkms.conf")
            self.assertIsNotNone(config)
            configuration = config.read().decode()
            self.assertIn('AUTOINSTALL="yes"', configuration)
            self.assertIn(
                f'MAKE[0]="./dkms-build $kernelver {dkms_version}"',
                configuration,
            )
            self.assertNotRegex(configuration, r"(?m)^\s*PRE_BUILD=")
            self.assertIn('CLEAN="/bin/true"', configuration)
            self.assertIn('STRIP[0]="no"', configuration)
            self.assertIn(
                'BUILD_EXCLUSIVE_KERNEL_MIN="6.18"',
                configuration,
            )
            kernel_policy = [
                line
                for line in configuration.splitlines()
                if "BUILD_EXCLUSIVE_KERNEL" in line or "OBSOLETE_BY" in line
            ]
            self.assertEqual(
                kernel_policy,
                ['BUILD_EXCLUSIVE_KERNEL_MIN="6.18"'],
            )
            self.assertIn('  - "dkms (>= 3.0.11)"\n', manifest)
            self.assertIn('  - "ca-certificates"\n', manifest)
            self.assertIn('  - "curl"\n', manifest)

    def test_prebuilt_paths_match_distribution_header_identities(self):
        cases = (
            (
                {
                    "id": "ubuntu-test",
                    "distro": "ubuntu",
                    "arch": "x86_64",
                    "kernel_release": "7.0.0-1-generic",
                    "kernel_package_version": "7.0.0-1.1",
                },
                ("ubuntu", "amd64", "linux-headers-7.0.0-1-generic",
                 "7.0.0-1.1", "7.0.0-1-generic"),
            ),
            (
                {
                    "id": "debian-test",
                    "distro": "debian",
                    "arch": "x86_64",
                    "kernel_release": "7.1.0+deb13-amd64",
                    "kernel_package_version": "7.1.0-1~bpo13+1",
                },
                ("debian", "amd64", "linux-headers-7.1.0+deb13-amd64",
                 "7.1.0-1~bpo13+1", "7.1.0+deb13-amd64"),
            ),
            (
                {
                    "id": "fedora-test",
                    "distro": "fedora",
                    "arch": "x86_64",
                    "kernel_release": "7.1.0-1.fc44.x86_64",
                    "kernel_package_version": "7.1.0-1.fc44.x86_64",
                },
                ("fedora", "x86_64", "kernel-devel",
                 "0@7.1.0-1.fc44", "7.1.0-1.fc44.x86_64"),
            ),
            (
                {
                    "id": "opensuse-test",
                    "distro": "opensuse",
                    "arch": "x86_64",
                    "kernel_release": "7.1.0-1-default",
                    "kernel_package_version": "7.1.0-1.1",
                    "source": {
                        "identity": "kernel-default-devel@7.1.0-1.1"
                    },
                },
                ("opensuse", "x86_64", "kernel-default-devel",
                 "0@7.1.0-1.1", "7.1.0-1-default"),
            ),
        )
        for target, identity in cases:
            with self.subTest(distro=target["distro"]):
                self.assertEqual(
                    stage_prebuilt_modules.publication_identity(target),
                    identity,
                )
                self.assertEqual(
                    stage_prebuilt_modules.relative_module_path(
                        target, "2.2.3-2"
                    ).parts,
                    ("kernel-modules", "v1", *identity, "2.2.3-2", "zerofs.ko.xz"),
                )
                self.assertNotIn(
                    ":",
                    stage_prebuilt_modules.relative_module_path(
                        target, "2.2.3-2"
                    ).as_posix(),
                )

    def test_fetcher_installs_only_the_exact_signed_module(self):
        catalog = stage_prebuilt_modules.load_catalog(
            REPOSITORY / "packaging/kernel/kernels.lock.json"
        )
        target = next(
            target
            for target in reversed(catalog.targets)
            if target["distro"] == "ubuntu"
            and target["release"] == "26.04"
            and target["arch"] == "x86_64"
        )
        kernel_release = target["kernel_release"]
        package_version = "2.2.3-2"
        _, _, header_package, header_version, _ = (
            stage_prebuilt_modules.publication_identity(target)
        )
        module_base_url = "https://modules.test/kernel-modules"
        relative_module = stage_prebuilt_modules.relative_module_path(
            target, package_version
        )
        expected_url = f"{module_base_url}/{'/'.join(relative_module.parts[1:])}"

        modules_root = self.root / "lib/modules"
        kernel_build = self.root / "headers"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n", encoding="utf-8"
        )
        kernel_link = modules_root / kernel_release / "build"
        kernel_link.parent.mkdir(parents=True)
        kernel_link.symlink_to(kernel_build, target_is_directory=True)
        os_release = self.root / "os-release"
        os_release.write_text("ID=ubuntu\n", encoding="utf-8")

        artifact = self.root / "artifact"
        artifact.mkdir()
        module_source = artifact / "module.c"
        unsigned_module = artifact / "zerofs.ko"
        module_source.write_text(
            "\n".join(
                (
                    '#define MODINFO(name, value) static const char name[] '
                    '__attribute__((section(".modinfo"), used, aligned(1))) = value',
                    'MODINFO(module_name, "name=zerofs");',
                    f'MODINFO(vermagic, "vermagic={kernel_release} SMP");',
                    "",
                )
            ),
            encoding="utf-8",
        )
        subprocess.run(
            ["cc", "-c", "-o", unsigned_module, module_source],
            check=True,
        )

        signing_key = self.root / "signing-key.pem"
        signing_certificate = self.root / "signing-cert.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-new",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=ZeroFS fetcher test",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout",
                signing_key,
                "-out",
                signing_certificate,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        staged = self.root / "staged"
        subprocess.run(
            [
                sys.executable,
                str(STAGER),
                "--manifest",
                str(REPOSITORY / "packaging/kernel/kernels.lock.json"),
                "--package-version",
                package_version,
                "--artifact",
                f"{target['id']}={artifact}",
                "--signer",
                "kmodsign",
                "--sign-key",
                str(signing_key),
                "--trusted-cert",
                str(signing_certificate),
                "--strip-tool",
                "strip",
                "--objcopy-tool",
                "objcopy",
                "--output",
                str(staged),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        compressed_module = staged.joinpath(*relative_module.parts)
        signed_module = self.root / "zerofs.signed.ko"
        with signed_module.open("wb") as output:
            subprocess.run(
                ["xz", "--decompress", "--stdout", compressed_module],
                check=True,
                stdout=output,
            )

        def compress(module: Path, name: str) -> Path:
            compressed = self.root / name
            with compressed.open("wb") as output:
                subprocess.run(
                    ["xz", "--threads=1", "--stdout", module],
                    check=True,
                    stdout=output,
                )
            return compressed

        tampered_module = self.root / "zerofs.tampered.ko"
        tampered_bytes = bytearray(signed_module.read_bytes())
        tampered_bytes[64] ^= 1
        tampered_module.write_bytes(tampered_bytes)
        tampered_compressed = compress(
            tampered_module,
            "zerofs.tampered.ko.xz",
        )

        trailer_module = self.root / "zerofs.bad-trailer.ko"
        trailer_bytes = bytearray(signed_module.read_bytes())
        signature_magic = b"~Module signature appended~\n"
        trailer_start = (
            len(trailer_bytes)
            - len(signature_magic)
            - stage_prebuilt_modules.SIGNATURE_TRAILER.size
        )
        trailer_bytes[trailer_start + 2] = 1
        trailer_module.write_bytes(trailer_bytes)
        trailer_compressed = compress(
            trailer_module,
            "zerofs.bad-trailer.ko.xz",
        )

        curl_log = self.root / "curl.log"
        self.write_executable(
            self.fake_bin / "curl",
            """#!/usr/bin/env python3
import os
import pathlib
import shutil
import sys

arguments = sys.argv[1:]
if not arguments or arguments[0] != "--disable":
    raise SystemExit(2)
status = int(os.environ.get("FAKE_CURL_STATUS", "0"))
if status:
    raise SystemExit(status)
output = pathlib.Path(arguments[arguments.index("--output") + 1])
url = arguments[-1]
with pathlib.Path(os.environ["CURL_LOG"]).open("a", encoding="utf-8") as log:
    log.write(f"{url}\\n")
shutil.copyfile(os.environ["FAKE_MODULE"], output)
""",
        )
        self.write_executable(
            self.fake_bin / "dpkg",
            """#!/bin/sh
[ "$#" -eq 1 ] && [ "$1" = --print-architecture ] || exit 2
printf '%s\n' amd64
""",
        )
        self.write_executable(
            self.fake_bin / "dpkg-query",
            f"""#!/bin/sh
case $1 in
    --search)
        printf '%s\\n' 'diversion by local-kernel from: '"$2"
        printf '%s\\n' 'diversion by local-kernel to: '"$2"
        printf '%s: %s\\n' {header_package} "$2"
        ;;
    --show) printf '%s\\t%s\\n' {header_package} "$FAKE_HEADER_VERSION" ;;
    *) exit 2 ;;
esac
""",
        )
        temporary_directory = self.root / "tmp"
        destination_directory = self.root / "destination"
        temporary_directory.mkdir()
        destination_directory.mkdir()

        def fetch(
            header_version_value: str,
            destination: Path,
            *,
            curl_status: int = 0,
            module: Path = compressed_module,
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [str(FETCHER), kernel_release, package_version, destination],
                env={
                    **os.environ,
                    "CURL_LOG": str(curl_log),
                    "FAKE_CURL_STATUS": str(curl_status),
                    "FAKE_HEADER_VERSION": header_version_value,
                    "FAKE_MODULE": str(module),
                    "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                    "TMPDIR": str(temporary_directory),
                    "ZEROFS_MODULE_BASE_URL": module_base_url,
                    "ZEROFS_MODULE_CERT_FILE": str(signing_certificate),
                    "ZEROFS_MODULES_ROOT": str(modules_root),
                    "ZEROFS_OS_RELEASE_FILE": str(os_release),
                },
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        installed = destination_directory / "installed.ko"
        result = fetch(header_version, installed)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(installed.read_bytes(), signed_module.read_bytes())
        self.assertEqual(curl_log.read_text(encoding="utf-8"), f"{expected_url}\n")

        os_release.write_text(
            'ID=linuxmint\nID_LIKE="ubuntu debian"\n', encoding="utf-8"
        )
        curl_log.write_text("", encoding="utf-8")
        derivative = destination_directory / "derivative.ko"
        result = fetch(header_version, derivative)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(derivative.read_bytes(), signed_module.read_bytes())
        self.assertEqual(curl_log.read_text(encoding="utf-8"), f"{expected_url}\n")

        rejected = destination_directory / "rejected.ko"
        rejected.write_bytes(b"unchanged")
        result = fetch(f"{header_version}+wrong", rejected)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("different publication identity", result.stderr)
        self.assertEqual(rejected.read_bytes(), b"unchanged")
        self.assertFalse(list(destination_directory.glob(".zerofs.ko.*")))

        tampered = destination_directory / "tampered.ko"
        result = fetch(header_version, tampered, module=tampered_compressed)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("valid ZeroFS signature", result.stderr)
        self.assertFalse(tampered.exists())

        bad_trailer = destination_directory / "bad-trailer.ko"
        result = fetch(header_version, bad_trailer, module=trailer_compressed)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("signature is not PKCS#7", result.stderr)
        self.assertFalse(bad_trailer.exists())

        unavailable = destination_directory / "unavailable.ko"
        result = fetch(header_version, unavailable, curl_status=60)
        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertFalse(unavailable.exists())

    def test_delayed_builds_with_the_same_epoch_are_identical(self):
        environment = {
            **os.environ,
            "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SOURCE_DATE_EPOCH": "42",
        }
        outputs = (self.root / "first", self.root / "second")
        for index, output in enumerate(outputs):
            if index:
                time.sleep(1.1)
            subprocess.run(
                [sys.executable, str(PACKAGER), "--output", str(output)],
                cwd=REPOSITORY,
                env=environment,
                check=True,
                text=True,
                stdout=subprocess.DEVNULL,
            )

        version = kernel_source_package.repository_version(REPOSITORY)
        dkms_version = kernel_source_package.package_version(version)
        filenames = (
            f"deb/zerofs-kernel-client_{dkms_version}_all.deb",
            f"rpm/zerofs-kernel-client-{dkms_version}.noarch.rpm",
        )
        for filename in filenames:
            self.assertEqual(
                outputs[0].joinpath(filename).read_bytes(),
                outputs[1].joinpath(filename).read_bytes(),
            )

    def test_fetcher_serializes_rpm_epoch_without_a_colon(self):
        kernel_release = "7.1.0-1.fc44.x86_64"
        modules_root = self.root / "lib/modules"
        kernel_build = self.root / "headers"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n", encoding="utf-8"
        )
        kernel_link = modules_root / kernel_release / "build"
        kernel_link.parent.mkdir(parents=True)
        kernel_link.symlink_to(kernel_build, target_is_directory=True)
        os_release = self.root / "os-release"
        os_release.write_text("ID=fedora\n", encoding="utf-8")
        curl_log = self.root / "rpm-curl.log"
        self.write_executable(
            self.fake_bin / "curl",
            """#!/bin/sh
for argument do last=$argument; done
printf '%s\n' "$last" >"$CURL_LOG"
exit 22
""",
        )
        self.write_executable(
            self.fake_bin / "rpm",
            """#!/bin/sh
case " $* " in
    *' --query --file '*)
        printf '%s\t%s\t%s\t%s\n' kernel-devel 0 7.1.0 1.fc44
        ;;
    *' --eval '*) printf '%s\n' x86_64 ;;
    *) exit 2 ;;
esac
""",
        )
        destination = self.root / "destination/zerofs.ko"
        destination.parent.mkdir()
        result = subprocess.run(
            [str(FETCHER), kernel_release, "2.2.3-2", destination],
            env={
                **os.environ,
                "CURL_LOG": str(curl_log),
                "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                "ZEROFS_MODULE_BASE_URL": "https://modules.test/kernel-modules",
                "ZEROFS_MODULE_CERT_FILE": str(
                    REPOSITORY
                    / "packaging/kernel/zerofs-module-signing-cert.pem"
                ),
                "ZEROFS_MODULES_ROOT": str(modules_root),
                "ZEROFS_OS_RELEASE_FILE": str(os_release),
            },
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(result.returncode, 75, result.stderr)
        self.assertEqual(
            curl_log.read_text(encoding="utf-8"),
            "https://modules.test/kernel-modules/v1/fedora/x86_64/"
            "kernel-devel/0@7.1.0-1.fc44/7.1.0-1.fc44.x86_64/"
            "2.2.3-2/zerofs.ko.xz\n",
        )
        self.assertFalse(destination.exists())

    def test_timestamp_normalization_covers_files_and_directories(self):
        staging = self.root / "staging"
        nested = staging / "nested"
        nested.mkdir(parents=True)
        payload = nested / "payload"
        payload.write_text("payload\n", encoding="utf-8")

        kernel_source_package.normalize_staging_timestamps(staging, 42)

        for path in (staging, nested, payload):
            self.assertEqual(path.stat().st_mtime_ns, 42_000_000_000)

    def test_source_date_epoch_defaults_to_checked_out_commit(self):
        expected = int(
            subprocess.check_output(
                ["git", "show", "-s", "--format=%ct", "HEAD"],
                cwd=REPOSITORY,
                text=True,
            ).strip()
        )
        previous = os.environ.pop("SOURCE_DATE_EPOCH", None)
        try:
            actual = kernel_source_package.source_date_epoch(REPOSITORY)
        finally:
            if previous is not None:
                os.environ["SOURCE_DATE_EPOCH"] = previous

        self.assertEqual(actual, expected)

    def test_staged_configuration_mode_is_independent_of_umask(self):
        destination = self.root / "staged-source"
        previous_umask = os.umask(0o077)
        try:
            kernel_source_package.stage_source_tree(
                REPOSITORY,
                destination,
                "1.4.1-3",
            )
        finally:
            os.umask(previous_umask)

        self.assertEqual(
            destination.joinpath("dkms.conf").stat().st_mode & 0o777,
            0o644,
        )

    def test_source_tree_preflight_rejects_symbolic_links(self):
        source = self.root / "source-tree"
        source.mkdir()
        (source / "link").symlink_to(self.root)

        with self.assertRaises(kernel_source_package.PackageError):
            kernel_source_package.validate_source_tree(source)

    def write_kernel_source(
        self,
        root: Path,
        *,
        package_version: str,
    ) -> Path:
        source = root / "linux-source-7.0.1"
        (source / "rust/kernel").mkdir(parents=True)
        (source / "scripts").mkdir()
        (source / "debian.master").mkdir()
        (source / "Makefile").write_text("# kernel source\n", encoding="utf-8")
        (source / "rust/Makefile").write_text("# rust\n", encoding="utf-8")
        (source / "rust/kernel/lib.rs").write_text("// rust\n", encoding="utf-8")
        (source / "scripts/Makefile.build").write_text("# build\n", encoding="utf-8")
        (source / "debian.master/changelog").write_text(
            f"linux ({package_version}) stable; urgency=medium\n",
            encoding="utf-8",
        )
        return source

    def source_archive(self, *, package_version: str) -> Path:
        staging = self.root / f"source-{package_version}"
        staging.mkdir()
        source = self.write_kernel_source(
            staging,
            package_version=package_version,
        )
        archive = self.root / f"linux-{package_version}.tar.xz"
        with tarfile.open(archive, "w:xz") as output:
            output.add(source, arcname=source.name)
        return archive

    def resolve(
        self,
        archive: Path,
        *,
        succeeds: bool = True,
        trusted_version: str | None = None,
        trusted_source_version: str | None = None,
        trusted_header_version: str | None = None,
        kernel_release: str = "7.0.1-28-generic",
    ):
        kernel_build = self.root / "headers"
        kernel_build.mkdir(exist_ok=True)
        extraction = self.root / "extracted"
        environment = {
            **os.environ,
            "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
            "ZEROFS_KERNEL_SOURCE": str(archive),
        }
        if trusted_version is not None:
            self.assertIsNone(trusted_source_version)
            self.assertIsNone(trusted_header_version)
            trusted_source_version = trusted_version
            trusted_header_version = trusted_version
        if (
            trusted_source_version is not None
            or trusted_header_version is not None
        ):
            self.assertIsNotNone(trusted_source_version)
            self.assertIsNotNone(trusted_header_version)
            environment.update(
                {
                    "ZEROFS_KERNEL_SOURCE_PACKAGE_VERSION": trusted_source_version,
                    "ZEROFS_KERNEL_HEADERS_PACKAGE_VERSION": trusted_header_version,
                }
            )
        result = subprocess.run(
            [
                str(RESOLVER),
                kernel_release,
                str(kernel_build),
                str(extraction),
            ],
            env=environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(result.returncode == 0, succeeds, result.stderr)
        return result

    def test_source_resolver_selects_archive_root_not_rust_directory(self):
        archive = self.source_archive(package_version="7.0.1-28.28")

        result = self.resolve(archive, trusted_version="7.0.1-28.28")

        source, provenance = result.stdout.rstrip("\n").split("\t")
        self.assertEqual(Path(source).name, "linux-source-7.0.1")
        self.assertTrue((Path(source) / "rust/Makefile").is_file())
        self.assertEqual(provenance, f"archive:{archive}")

    def test_source_resolver_accepts_distinct_trusted_package_revisions(self):
        source_version = "0:7.0.1-3.1"
        archive = self.source_archive(package_version=source_version)

        result = self.resolve(
            archive,
            trusted_source_version=source_version,
            trusted_header_version="0:7.0.1-2.2",
        )

        self.assertEqual(result.returncode, 0)

    def test_source_resolver_ignores_failed_rpm_ownership_diagnostic(self):
        archive = self.source_archive(package_version="7.0.1-28.28")
        self.write_executable(
            self.fake_bin / "rpm",
            """#!/bin/sh
printf 'file %s is not owned by any package\n' "$4"
exit 1
""",
        )

        result = self.resolve(archive, trusted_version="7.0.1-28.28")

        self.assertNotIn("not owned by any package", result.stderr)

    def test_source_resolver_rejects_wrong_ubuntu_abi(self):
        archive = self.source_archive(package_version="7.0.1-29.29")

        result = self.resolve(
            archive,
            succeeds=False,
            trusted_version="7.0.1-29.29",
        )

        self.assertEqual(result.returncode, 75)
        self.assertIn("does not match 7.0.1-28-generic", result.stderr)

    def test_source_resolver_rejects_unknown_package_revision(self):
        archive = self.source_archive(package_version="7.0.1-28.28")

        result = self.resolve(archive, succeeds=False)

        self.assertIn(
            "cannot prove the source and header package revisions",
            result.stderr,
        )

    def test_source_resolver_skips_wrong_candidate_before_exact_one(self):
        bad_root = self.root / "bad-candidate"
        good_root = self.root / "good-candidate"
        bad_root.mkdir()
        good_root.mkdir()
        bad = self.write_kernel_source(
            bad_root,
            package_version="7.0.1-29.29",
        )
        good = self.write_kernel_source(
            good_root,
            package_version="7.0.1-28.28",
        )
        archive = self.root / "candidate-selection.tar.xz"
        with tarfile.open(archive, "w:xz") as output:
            output.add(bad, arcname=f"bad/{bad.name}")
            output.add(good, arcname=f"good/{good.name}")

        result = self.resolve(
            archive,
            trusted_version="7.0.1-28.28",
        )

        source, _ = result.stdout.rstrip("\n").split("\t")
        self.assertIn("/good/", source)

    def test_source_resolver_skips_diversions_then_checks_package_revision(self):
        archive = self.source_archive(package_version="7.0.1-28.28")
        fake_dpkg = self.fake_bin / "dpkg-query"
        self.write_executable(
            fake_dpkg,
            """#!/bin/sh
case $1 in
    -S)
        printf 'diversion by local-kernel from: %s\n' "$2"
        printf 'diversion by local-kernel to: %s\n' "$2"
        case $2 in
            */include/config/kernel.release)
                printf 'linux-headers-test: %s\\n' "$2" ;;
            *) printf 'linux-source-test: %s\\n' "$2" ;;
        esac
        ;;
    -W)
        case $3 in
            linux-headers-test) printf '7.0.1-29.29\\n' ;;
            linux-source-test) printf '7.0.1-28.28\\n' ;;
            *) exit 1 ;;
        esac
        ;;
    *) exit 1 ;;
esac
""",
        )
        kernel_build = self.root / "headers"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/kernel.release").write_text(
            "7.0.1-28-generic\n",
            encoding="utf-8",
        )
        result = subprocess.run(
            [
                str(RESOLVER),
                "7.0.1-28-generic",
                str(kernel_build),
                str(self.root / "revision-extracted"),
            ],
            env={
                **os.environ,
                "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                "ZEROFS_KERNEL_SOURCE": str(archive),
            },
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "source package 7.0.1-28.28 does not match headers 7.0.1-29.29",
            result.stderr,
        )

    def render_postinstall(self) -> Path:
        script = self.root / "postinstall.sh"
        kernel_source_package.render_script(
            REPOSITORY / "packaging/kernel/scripts/source-postinstall.sh.in",
            script,
            "1.4.1-3",
        )
        replacements = {
            "/usr/src": str(self.root / "usr/src"),
            "/var/lib/dkms": str(self.root / "var/lib/dkms"),
            "/lib/modules": str(self.root / "lib/modules"),
        }
        text = script.read_text(encoding="utf-8")
        for original, replacement in replacements.items():
            text = text.replace(original, replacement)
        script.write_text(text, encoding="utf-8")
        (self.root / "usr/src/zerofs-1.4.1-3").mkdir(parents=True)
        return script

    def run_postinstall(
        self,
        script: Path,
        *,
        cwd: Path | None = None,
        extra_environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            **os.environ,
            "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
        }
        if extra_environment is not None:
            environment.update(extra_environment)
        return subprocess.run(
            [str(script), "configure"],
            cwd=cwd,
            env=environment,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def write_fake_dkms(self) -> None:
        fake_dkms = self.fake_bin / "dkms"
        self.write_executable(
            fake_dkms,
            """#!/bin/sh
printf '%s\\n' "$*" >>"$DKMS_LOG"
case $1 in
    status) exit 0 ;;
    add|build|install) exit 0 ;;
    *) exit 2 ;;
esac
""",
        )

    def test_postinstall_builds_only_selected_module_and_kernel(self):
        script = self.render_postinstall()
        self.write_fake_dkms()
        log = self.root / "dkms.log"

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
                "ZEROFS_DKMS_KERNEL": "7.0.1-28-generic",
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)

        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn("add -m zerofs -v 1.4.1-3", commands)
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 7.0.1-28-generic",
            commands,
        )
        self.assertIn(
            "install -m zerofs -v 1.4.1-3 -k 7.0.1-28-generic",
            commands,
        )
        self.assertFalse(any("autoinstall" in command for command in commands))
        self.assertFalse(any("remove" in command for command in commands))

    def test_postinstall_does_not_split_or_expand_selected_kernel(self):
        script = self.render_postinstall()
        self.write_fake_dkms()
        log = self.root / "dkms-unsafe-kernel.log"
        (self.root / "7.0.1-first").mkdir()
        (self.root / "7.0.2-second").mkdir()

        for kernel in ("7.0.1-first 7.0.2-second", "*"):
            with self.subTest(kernel=kernel):
                log.unlink(missing_ok=True)
                result = self.run_postinstall(
                    script,
                    cwd=self.root,
                    extra_environment={
                        "DKMS_LOG": str(log),
                        "ZEROFS_DKMS_KERNEL": kernel,
                    },
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"unsafe DKMS kernel release: {kernel}",
                    result.stderr,
                )
                commands = log.read_text(encoding="utf-8").splitlines()
                self.assertFalse(
                    any(command.startswith("build ") for command in commands)
                )
                self.assertFalse(
                    any(command.startswith("install ") for command in commands)
                )

    def test_postinstall_registers_without_installed_headers(self):
        script = self.render_postinstall()
        self.write_fake_dkms()
        log = self.root / "dkms-no-headers.log"

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DKMS will build when headers become available", result.stderr)
        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn("add -m zerofs -v 1.4.1-3", commands)
        self.assertFalse(any(command.startswith("build ") for command in commands))
        self.assertFalse(any(command.startswith("install ") for command in commands))

    def test_postinstall_rejects_stale_same_version_dkms_state(self):
        script = self.render_postinstall()
        self.write_fake_dkms()
        state = self.root / "var/lib/dkms/zerofs/1.4.1-3"
        state.mkdir(parents=True)
        wrong_source = self.root / "wrong-source"
        wrong_source.mkdir()
        source_link = state / "source"
        source_link.symlink_to(wrong_source, target_is_directory=True)
        log = self.root / "dkms-link-repair.log"

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
            },
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("stale DKMS registration", result.stderr)
        self.assertEqual(source_link.resolve(), wrong_source.resolve())
        self.assertFalse(log.exists())

    def test_postinstall_stops_on_running_kernel_build_failure(self):
        script = self.render_postinstall()
        for kernel in ("7.0.1-old", "7.0.2-new"):
            (self.root / f"lib/modules/{kernel}/build").mkdir(parents=True)
        fake_uname = self.fake_bin / "uname"
        self.write_executable(
            fake_uname,
            "#!/bin/sh\nprintf '%s\\n' 7.0.1-old\n",
        )
        fake_dkms = self.fake_bin / "dkms"
        self.write_executable(
            fake_dkms,
            """#!/bin/sh
printf '%s\\n' "$*" >>"$DKMS_LOG"
case $1 in
    status) exit 0 ;;
    add|install) exit 0 ;;
    build)
        case " $* " in
            *' -k 7.0.1-old '*) exit 1 ;;
            *) exit 0 ;;
        esac
        ;;
    *) exit 2 ;;
esac
""",
        )
        log = self.root / "dkms-old-new.log"

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
            },
        )

        self.assertNotEqual(result.returncode, 0)
        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 7.0.1-old",
            commands,
        )
        self.assertNotIn(
            "build -m zerofs -v 1.4.1-3 -k 7.0.2-new",
            commands,
        )
        self.assertNotIn(
            "install -m zerofs -v 1.4.1-3 -k 7.0.1-old",
            commands,
        )

    def test_postinstall_skips_kernel_below_floor_and_builds_next(self):
        script = self.render_postinstall()
        for kernel in ("6.17.12-old", "99.0.1-new"):
            (self.root / f"lib/modules/{kernel}/build").mkdir(parents=True)
        fake_uname = self.fake_bin / "uname"
        self.write_executable(
            fake_uname,
            "#!/bin/sh\nprintf '%s\\n' 6.17.12-old\n",
        )
        fake_dkms = self.fake_bin / "dkms"
        self.write_executable(
            fake_dkms,
            """#!/bin/sh
printf '%s\\n' "$*" >>"$DKMS_LOG"
case $1 in
    status) exit 0 ;;
    add|install) exit 0 ;;
    build)
        case " $* " in
            *' -k 6.17.12-old '*) exit 77 ;;
            *) exit 0 ;;
        esac
        ;;
    *) exit 2 ;;
esac
""",
        )
        log = self.root / "dkms-minimum.log"

        result = self.run_postinstall(
            script,
            extra_environment={"DKMS_LOG": str(log)},
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("requires Linux 6.18 or newer", result.stderr)
        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 6.17.12-old",
            commands,
        )
        self.assertNotIn(
            "install -m zerofs -v 1.4.1-3 -k 6.17.12-old",
            commands,
        )
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 99.0.1-new",
            commands,
        )
        self.assertIn(
            "install -m zerofs -v 1.4.1-3 -k 99.0.1-new",
            commands,
        )

    def test_postinstall_keeps_selected_kernel_failure_fatal(self):
        script = self.render_postinstall()
        fake_dkms = self.fake_bin / "dkms"
        self.write_executable(
            fake_dkms,
            """#!/bin/sh
case $1 in
    status|add) exit 0 ;;
    build) exit 77 ;;
    *) exit 2 ;;
esac
""",
        )

        result = self.run_postinstall(
            script,
            extra_environment={
                "ZEROFS_DKMS_KERNEL": "7.0.2-new",
            },
        )

        self.assertNotEqual(result.returncode, 0)

    def test_postinstall_keeps_selected_kernel_install_failure_fatal(self):
        script = self.render_postinstall()
        fake_dkms = self.fake_bin / "dkms"
        self.write_executable(
            fake_dkms,
            """#!/bin/sh
case $1 in
    add) exit 0 ;;
    status) printf '%s\n' 'zerofs/1.4.1-3, 7.0.2-new, x86_64: built' ;;
    install) exit 1 ;;
    *) exit 2 ;;
esac
""",
        )

        result = self.run_postinstall(
            script,
            extra_environment={
                "ZEROFS_DKMS_KERNEL": "7.0.2-new",
            },
        )

        self.assertNotEqual(result.returncode, 0)

    def test_arm64_metadata_build_skips_unrelated_compat_vdso(self):
        wrapper_root = self.root / "wrapper"
        wrapper_root.mkdir()
        wrapper = wrapper_root / "dkms-build"
        self.write_executable(
            wrapper,
            (REPOSITORY / "packaging/kernel/scripts/dkms-build.sh")
            .read_text(encoding="utf-8")
            .replace("/lib/modules", str(self.root / "lib/modules")),
        )
        (wrapper_root / "kernel").mkdir()

        kernel_release = "7.0.1-arm64-test"
        kernel_build = self.root / f"lib/modules/{kernel_release}/build"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/auto.conf").write_text(
            "\n".join(
                (
                    "CONFIG_ARM64=y",
                    "CONFIG_CC_IS_GCC=y",
                    "CONFIG_COMPAT_VDSO=y",
                    "CONFIG_MODULES=y",
                    "CONFIG_RUST=y",
                    "",
                )
            ),
            encoding="utf-8",
        )
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n",
            encoding="utf-8",
        )
        (kernel_build / "Module.symvers").write_text("symbols\n", encoding="utf-8")
        kernel_source = self.root / "kernel-source"
        kernel_source.mkdir()

        resolver = wrapper_root / "dkms-find-kernel-source"
        self.write_executable(
            resolver,
            "#!/bin/sh\nprintf '%s\\tdirectory:test\\n' \"$FAKE_KERNEL_SOURCE\"\n",
        )

        for tool in ("rustc", "rustfmt", "bindgen", "gcc"):
            self.write_executable(self.fake_bin / tool, "#!/bin/sh\nexit 0\n")
        self.write_executable(
            self.fake_bin / "modinfo",
            f"""#!/bin/sh
[ "$1" = -F ] || exit 2
case $2 in
    name) printf '%s\n' zerofs ;;
    vermagic) printf '%s\n' '{kernel_release} SMP' ;;
    *) exit 2 ;;
esac
""",
        )
        fake_make = self.fake_bin / "make"
        self.write_executable(
            fake_make,
            """#!/bin/sh
printf '%s\n' "$*" >>"$MAKE_LOG"
metadata=
module_output=
for argument in "$@"; do
    case $argument in
        O=*) metadata=${argument#O=} ;;
        MO=*) module_output=${argument#MO=} ;;
    esac
done
case " $* " in
    *' rust/kernel.o '*)
        mkdir -p "$metadata/rust"
        printf '%s\n' metadata >"$metadata/rust/libkernel.rmeta"
        ;;
    *' modules '*)
        mkdir -p "$module_output"
        printf '%s\n' module >"$module_output/zerofs.ko"
        ;;
esac
""",
        )
        make_log = self.root / "make.log"

        subprocess.run(
            [str(wrapper), kernel_release, "1.4.1-3"],
            env={
                **os.environ,
                "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                "FAKE_KERNEL_SOURCE": str(kernel_source),
                "MAKE_LOG": str(make_log),
                "ZEROFS_BINDGEN": str(self.fake_bin / "bindgen"),
                "ZEROFS_BUILD_JOBS": "1",
                "ZEROFS_DISABLE_PREBUILT": "1",
                "ZEROFS_RUSTC": str(self.fake_bin / "rustc"),
                "ZEROFS_RUSTFMT": str(self.fake_bin / "rustfmt"),
                "ZEROFS_TARGET_CC": str(self.fake_bin / "gcc"),
            },
            check=True,
        )

        commands = make_log.read_text(encoding="utf-8").splitlines()
        metadata_command = next(
            command for command in commands if "rust/kernel.o" in command
        )
        module_command = next(command for command in commands if " modules" in command)
        self.assertIn("CONFIG_COMPAT_VDSO=", metadata_command)
        self.assertNotIn("CONFIG_COMPAT_VDSO=", module_command)

    def test_unpublished_module_without_source_fallback_is_fatal(self):
        wrapper_root = self.root / "missing-tool-wrapper"
        wrapper_root.mkdir()
        wrapper = wrapper_root / "dkms-build"
        self.write_executable(
            wrapper,
            (REPOSITORY / "packaging/kernel/scripts/dkms-build.sh")
            .read_text(encoding="utf-8")
            .replace("/lib/modules", str(self.root / "lib/modules")),
        )
        (wrapper_root / "kernel").mkdir()
        self.write_executable(
            wrapper_root / "dkms-fetch-module",
            "#!/bin/sh\nexit 75\n",
        )

        kernel_release = "7.0.3-missing-tool"
        kernel_build = self.root / f"lib/modules/{kernel_release}/build"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/auto.conf").write_text(
            "CONFIG_MODULES=y\n"
            "CONFIG_RUST=y\n"
            "CONFIG_RUSTC_VERSION_TEXT=\"rustc 99.99.99 test\"\n",
            encoding="utf-8",
        )
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n",
            encoding="utf-8",
        )
        (kernel_build / "Module.symvers").write_text(
            "symbols\n", encoding="utf-8"
        )

        result = subprocess.run(
            [str(wrapper), kernel_release, "1.4.1-3"],
            env={
                **os.environ,
                "PATH": "/usr/bin:/bin",
            },
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("source fallback unavailable", result.stderr)

    def test_prebuilt_wrapper_succeeds_without_source_build_inputs(self):
        wrapper_root = self.root / "prebuilt-wrapper"
        wrapper_root.mkdir()
        wrapper = wrapper_root / "dkms-build"
        self.write_executable(
            wrapper,
            (REPOSITORY / "packaging/kernel/scripts/dkms-build.sh")
            .read_text(encoding="utf-8")
            .replace("/lib/modules", str(self.root / "lib/modules")),
        )
        (wrapper_root / "kernel").mkdir()
        self.write_executable(
            wrapper_root / "dkms-fetch-module",
            """#!/bin/sh
printf '%s\n' prebuilt >"$3"
""",
        )

        kernel_release = "7.0.4-prebuilt"
        kernel_build = self.root / f"lib/modules/{kernel_release}/build"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n",
            encoding="utf-8",
        )
        runtime_bin = self.root / "prebuilt-runtime-bin"
        runtime_bin.mkdir()
        for command in ("bash", "dirname", "install", "realpath", "rm"):
            executable = shutil.which(command)
            self.assertIsNotNone(executable)
            (runtime_bin / command).symlink_to(executable)

        result = subprocess.run(
            [str(wrapper), kernel_release, "1.4.1-3"],
            env={**os.environ, "PATH": str(runtime_bin)},
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (wrapper_root / "dkms-output/zerofs.ko").read_text(encoding="utf-8"),
            "prebuilt\n",
        )

    def test_failed_build_removes_a_partial_module(self):
        wrapper_root = self.root / "partial-wrapper"
        wrapper_root.mkdir()
        wrapper = wrapper_root / "dkms-build"
        self.write_executable(
            wrapper,
            (REPOSITORY / "packaging/kernel/scripts/dkms-build.sh")
            .read_text(encoding="utf-8")
            .replace("/lib/modules", str(self.root / "lib/modules")),
        )
        (wrapper_root / "kernel").mkdir()
        self.write_executable(
            wrapper_root / "dkms-fetch-module",
            "#!/bin/sh\nexit 75\n",
        )

        kernel_release = "7.0.4-partial"
        kernel_build = self.root / f"lib/modules/{kernel_release}/build"
        (kernel_build / "include/config").mkdir(parents=True)
        (kernel_build / "rust").mkdir()
        (kernel_build / "include/config/auto.conf").write_text(
            "CONFIG_CC_IS_GCC=y\nCONFIG_MODULES=y\nCONFIG_RUST=y\n",
            encoding="utf-8",
        )
        (kernel_build / "include/config/kernel.release").write_text(
            f"{kernel_release}\n",
            encoding="utf-8",
        )
        (kernel_build / "Module.symvers").write_text(
            "symbols\n", encoding="utf-8"
        )
        (kernel_build / "rust/libkernel.rmeta").write_text(
            "metadata\n", encoding="utf-8"
        )
        for tool in ("rustc", "rustfmt", "bindgen", "gcc"):
            self.write_executable(self.fake_bin / tool, "#!/bin/sh\nexit 0\n")
        self.write_executable(
            self.fake_bin / "make",
            """#!/bin/sh
for argument in "$@"; do
    case $argument in MO=*) output=${argument#MO=} ;; esac
done
mkdir -p "$output"
printf '%s\n' partial >"$output/zerofs.ko"
exit 1
""",
        )

        result = subprocess.run(
            [str(wrapper), kernel_release, "1.4.1-3"],
            env={
                **os.environ,
                "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                "ZEROFS_BINDGEN": str(self.fake_bin / "bindgen"),
                "ZEROFS_BUILD_JOBS": "1",
                "ZEROFS_DISABLE_PREBUILT": "1",
                "ZEROFS_RUSTC": str(self.fake_bin / "rustc"),
                "ZEROFS_RUSTFMT": str(self.fake_bin / "rustfmt"),
                "ZEROFS_TARGET_CC": str(self.fake_bin / "gcc"),
            },
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((wrapper_root / "dkms-output/zerofs.ko").exists())


if __name__ == "__main__":
    unittest.main()
