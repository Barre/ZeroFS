import importlib.util
import os
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
SPEC = importlib.util.spec_from_file_location("kernel_source_package", PACKAGER)
assert SPEC is not None and SPEC.loader is not None
kernel_source_package = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kernel_source_package)


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
            self.assertIn(f"{prefix}/dkms-find-kernel-source", names)
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
            self.assertIn('MAKE[0]="./dkms-build $kernelver"', configuration)
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
        self.assertEqual(
            (self.root / "extracted.source-unavailable").read_text(
                encoding="utf-8"
            ),
            "7.0.1-28-generic\n",
        )

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

    def test_source_resolver_rejects_header_package_revision_mismatch(self):
        archive = self.source_archive(package_version="7.0.1-28.28")
        fake_dpkg = self.fake_bin / "dpkg-query"
        self.write_executable(
            fake_dpkg,
            """#!/bin/sh
case $1 in
    -S)
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

    def test_postinstall_skips_kernel_below_floor_and_builds_newer_kernel(self):
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

    def test_postinstall_skips_kernel_without_exact_source(self):
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
            *' -k 7.0.1-old '*)
                mkdir -p "${SOURCE_UNAVAILABLE_MARKER%/*}"
                printf '%s\\n' 7.0.1-old >"$SOURCE_UNAVAILABLE_MARKER"
                exit 10
                ;;
            *) exit 0 ;;
        esac
        ;;
    *) exit 2 ;;
esac
""",
        )
        log = self.root / "dkms-old-source.log"
        marker = (
            self.root
            / "var/lib/dkms/zerofs/1.4.1-3/build/dkms-kernel-source"
            / "7.0.1-old.source-unavailable"
        )

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
                "SOURCE_UNAVAILABLE_MARKER": str(marker),
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipping kernel 7.0.1-old", result.stderr)
        self.assertFalse(marker.exists())
        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 7.0.1-old",
            commands,
        )
        self.assertNotIn(
            "install -m zerofs -v 1.4.1-3 -k 7.0.1-old",
            commands,
        )
        self.assertIn(
            "build -m zerofs -v 1.4.1-3 -k 7.0.2-new",
            commands,
        )
        self.assertIn(
            "install -m zerofs -v 1.4.1-3 -k 7.0.2-new",
            commands,
        )

    def test_postinstall_skips_current_kernel_without_exact_source(self):
        script = self.render_postinstall()
        kernel = "7.0.2-current"
        (self.root / f"lib/modules/{kernel}/build").mkdir(parents=True)
        fake_uname = self.fake_bin / "uname"
        self.write_executable(
            fake_uname,
            f"#!/bin/sh\nprintf '%s\\n' {kernel}\n",
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
        mkdir -p "${SOURCE_UNAVAILABLE_MARKER%/*}"
        printf '%s\\n' 7.0.2-current >"$SOURCE_UNAVAILABLE_MARKER"
        exit 10
        ;;
    *) exit 2 ;;
esac
""",
        )
        log = self.root / "dkms-current-source.log"
        marker = (
            self.root
            / "var/lib/dkms/zerofs/1.4.1-3/build/dkms-kernel-source"
            / f"{kernel}.source-unavailable"
        )

        result = self.run_postinstall(
            script,
            extra_environment={
                "DKMS_LOG": str(log),
                "SOURCE_UNAVAILABLE_MARKER": str(marker),
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"skipping kernel {kernel}", result.stderr)
        self.assertIn("run dkms autoinstall", result.stderr)
        self.assertFalse(marker.exists())
        commands = log.read_text(encoding="utf-8").splitlines()
        self.assertIn(
            f"build -m zerofs -v 1.4.1-3 -k {kernel}",
            commands,
        )
        self.assertNotIn(
            f"install -m zerofs -v 1.4.1-3 -k {kernel}",
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
    build)
        mkdir -p "${SOURCE_UNAVAILABLE_MARKER%/*}"
        printf '%s\\n' 7.0.2-new >"$SOURCE_UNAVAILABLE_MARKER"
        exit 10
        ;;
    *) exit 2 ;;
esac
""",
        )

        result = self.run_postinstall(
            script,
            extra_environment={
                "ZEROFS_DKMS_KERNEL": "7.0.2-new",
                "SOURCE_UNAVAILABLE_MARKER": str(
                    self.root
                    / "var/lib/dkms/zerofs/1.4.1-3/build/dkms-kernel-source"
                    / "7.0.2-new.source-unavailable"
                ),
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
            [str(wrapper), kernel_release],
            env={
                **os.environ,
                "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
                "FAKE_KERNEL_SOURCE": str(kernel_source),
                "MAKE_LOG": str(make_log),
                "ZEROFS_BINDGEN": str(self.fake_bin / "bindgen"),
                "ZEROFS_BUILD_JOBS": "1",
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


if __name__ == "__main__":
    unittest.main()
