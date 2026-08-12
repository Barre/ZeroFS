import hashlib
import re
import tempfile
from pathlib import Path
from typing import Any

from ..catalog import fail
from .common import Runner, base_candidate, rpm_compare


FEDORA_RUST_NVR = re.compile(
    r"^CONFIG_RUSTC_VERSION_TEXT=\"?"
    r"[^\"\n]* \(Fedora ([0-9A-Za-z.+~_]+-[0-9A-Za-z.+~_]+)\)\"?$",
    re.MULTILINE,
)


def _verify_rpm(
    runner: Runner,
    path: Path,
    fingerprint: str,
    name: str,
    nvr: str,
    arch: str,
    source_rpm: str | None,
    *,
    verify_arch: bool = True,
) -> None:
    signature = runner.run(
        ["rpmkeys", "--checksig", "--verbose", str(path)]
    )
    expected = f"key fingerprint: {fingerprint}: OK"
    if expected not in signature:
        fail(f"{path.name} has an unexpected Fedora signature")
    query = runner.run(
        [
            "rpm",
            "-qp",
            "--queryformat",
            "%{NAME}\\t%{VERSION}-%{RELEASE}\\t%{ARCH}"
            "\\t%{SOURCERPM}\\n",
            str(path),
        ]
    ).strip().split("\t")
    if (
        len(query) != 4
        or query[:2] != [name, nvr]
        or (verify_arch and query[2] != arch)
    ):
        fail(f"{path.name} has unexpected RPM metadata")
    if source_rpm is not None and query[3] != source_rpm:
        fail(f"{path.name} has an unexpected source RPM")


def _download_rpm(
    runner: Runner,
    url: str,
    directory: Path,
    fingerprint: str,
    name: str,
    nvr: str,
    arch: str,
    source_rpm: str | None,
    *,
    verify_arch: bool = True,
) -> Path:
    filename = f"{name}-{nvr}.{arch}.rpm"
    path = directory / filename
    unsigned = directory / f"{filename}.unsigned"
    sighdr = directory / f"{filename}.sig"
    # koji garbage-collects the materialized signed copies once a build is
    # untagged, but keeps the original rpm and the detached signature header
    # for the life of the build. Splicing the two reproduces the signed rpm
    # byte for byte.
    runner.download(f"{url}/{arch}/{filename}", unsigned)
    runner.download(
        f"{url}/data/sigcache/{fingerprint[-8:]}/{arch}/{filename}.sig",
        sighdr,
    )
    runner.splice_rpm_sighdr(sighdr, unsigned, path)
    unsigned.unlink()
    sighdr.unlink()
    _verify_rpm(
        runner,
        path,
        fingerprint,
        name,
        nvr,
        arch,
        source_rpm,
        verify_arch=verify_arch,
    )
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def observe(
    channel: dict[str, Any],
    current: dict[str, Any],
    runner: Runner,
) -> str:
    target_arch = channel["arch"]
    package_name = "kernel-core"
    runner.run(["dnf", "-y", "install", "ca-certificates", "rpm"])
    query = runner.run(
        [
            "dnf",
            "-q",
            "repoquery",
            "--refresh",
            "--available",
            f"--arch={target_arch}",
            "--latest-limit=1",
            "--queryformat",
            "%{name}\t%{epoch}\t%{version}\t%{release}"
            "\t%{arch}\t%{sourcerpm}",
            package_name,
        ]
    )
    rows = [line.split("\t") for line in query.splitlines() if line.strip()]
    if len(rows) != 1 or len(rows[0]) != 6:
        fail("cannot select one Fedora kernel-core build")
    name, epoch, version, release, package_arch, source_rpm = rows[0]
    if (
        name != package_name
        or epoch not in {"0", "(none)"}
        or package_arch != target_arch
        or not release.endswith(f".fc{channel['release']}")
    ):
        fail("Fedora kernel query returned an unexpected package")
    kernel_nvr = f"{version}-{release}"
    if source_rpm != f"kernel-{kernel_nvr}.src.rpm":
        fail("Fedora kernel-core has an unexpected source build")
    old_nvr = current["source"]["identity"].removeprefix("kernel-")
    if rpm_compare(runner, kernel_nvr, old_nvr) < 0:
        fail(
            f"{channel['id']}: discovered kernel {kernel_nvr} "
            f"is older than {old_nvr}"
        )

    return f"kernel-{kernel_nvr}"


def discover(
    channel: dict[str, Any],
    current: dict[str, Any],
    current_lock: dict[str, Any],
    runner: Runner,
) -> dict[str, Any]:
    kernel_identity = observe(channel, current, runner)
    fingerprint = channel["discovery"]["signing_fingerprint"]
    if (
        kernel_identity == current_lock["nvr"]
        and fingerprint == current_lock["signing_fingerprint"]
    ):
        return base_candidate(current, current_lock)

    target_arch = channel["arch"]
    kernel_nvr = kernel_identity.removeprefix("kernel-")
    version, separator, release = kernel_nvr.rpartition("-")
    if not separator:
        fail("invalid Fedora kernel build")
    runner.run(["dnf", "-y", "install", "cpio", "python3-koji"])
    kernel_base = (
        "https://kojipkgs.fedoraproject.org/packages/kernel/"
        f"{version}/{release}"
    )
    with tempfile.TemporaryDirectory(prefix="zerofs-discovery-") as raw:
        directory = Path(raw)
        paths: list[Path] = []
        kernel_source = f"kernel-{kernel_nvr}.src.rpm"
        for package in ("kernel-core", "kernel-modules-core", "kernel-devel"):
            paths.append(
                _download_rpm(
                    runner,
                    kernel_base,
                    directory,
                    fingerprint,
                    package,
                    kernel_nvr,
                    target_arch,
                    kernel_source,
                )
            )
        paths.append(
            _download_rpm(
                runner,
                kernel_base,
                directory,
                fingerprint,
                "kernel",
                kernel_nvr,
                "src",
                None,
                # Koji files the SRPM below src/, while its RPM header records
                # whichever architecture built it. The signature, name, and
                # NVR provide the stable identity shared by every target arch.
                verify_arch=False,
            )
        )
        devel = next(
            path for path in paths if path.name.startswith("kernel-devel-")
        )
        auto_conf = runner.kernel_auto_conf(devel, directory / "extract")
        rust_match = FEDORA_RUST_NVR.search(auto_conf)
        if rust_match is None:
            fail("cannot derive Fedora Rust build from kernel-devel")
        rust_nvr = rust_match.group(1)
        rust_version, separator, rust_release = rust_nvr.partition("-")
        if not separator:
            fail("invalid Fedora Rust build")
        rust_base = (
            "https://kojipkgs.fedoraproject.org/packages/rust/"
            f"{rust_version}/{rust_release}"
        )
        rust_source = f"rust-{rust_nvr}.src.rpm"
        for package in ("cargo", "rust", "rust-std-static", "rustfmt"):
            paths.append(
                _download_rpm(
                    runner,
                    rust_base,
                    directory,
                    fingerprint,
                    package,
                    rust_nvr,
                    target_arch,
                    rust_source,
                )
            )
        paths.append(
            _download_rpm(
                runner,
                rust_base,
                directory,
                fingerprint,
                "rust-src",
                rust_nvr,
                "noarch",
                rust_source,
            )
        )
        artifacts = {
            path.name: _sha256(path)
            for path in sorted(paths, key=lambda item: item.name)
        }

    lock = {
        "nvr": kernel_identity,
        "signing_fingerprint": fingerprint,
        "artifacts": artifacts,
    }
    return base_candidate(current, lock)
