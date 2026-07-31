import copy
import re
from datetime import datetime
from typing import Any

from ..catalog import fail
from ..observation import KernelObservation, compare_observation
from .common import Runner, base_candidate


def _control_paragraphs(value: str) -> list[dict[str, str]]:
    paragraphs: list[dict[str, str]] = []
    current: dict[str, str] = {}
    field = ""
    for line in [*value.splitlines(), ""]:
        if not line:
            if current:
                paragraphs.append(current)
                current = {}
                field = ""
            continue
        if line[0].isspace():
            if not field:
                fail("invalid continuation in APT package metadata")
            current[field] += " " + line.strip()
            continue
        field, separator, content = line.partition(":")
        if not separator or not field:
            fail("invalid APT package metadata")
        if field in current:
            current[field] += ", " + content.strip()
        else:
            current[field] = content.strip()
    return paragraphs


def _select_record(
    records: list[dict[str, str]],
    package: str,
    version: str,
) -> dict[str, str]:
    exact = [
        record
        for record in records
        if record.get("Package") == package
        and record.get("Version") == version
    ]
    identities = {
        (
            record.get("Depends", ""),
            record.get("Pre-Depends", ""),
            record.get("Source", ""),
        )
        for record in exact
    }
    if not exact or len(identities) != 1:
        fail(f"cannot read unambiguous APT metadata for {package}={version}")
    return exact[0]


def _show_exact(
    runner: Runner,
    package: str,
    version: str,
    suite: str,
) -> dict[str, str]:
    records = _control_paragraphs(
        runner.run(
            [
                "apt-cache",
                "-o",
                f"APT::Default-Release={suite}",
                "show",
                f"{package}={version}",
            ]
        )
    )
    return _select_record(records, package, version)


def _candidate(
    runner: Runner,
    package: str,
    suite: str,
) -> tuple[str, dict[str, str]]:
    policy = runner.run(
        [
            "apt-cache",
            "-o",
            f"APT::Default-Release={suite}",
            "policy",
            package,
        ]
    )
    versions = re.findall(r"^\s*Candidate:\s+(\S+)\s*$", policy, re.MULTILINE)
    if len(versions) != 1 or versions[0] == "(none)":
        fail(f"cannot select an APT candidate for {package}")
    version = versions[0]
    return version, _show_exact(runner, package, version, suite)


def _versioned_image(
    record: dict[str, str],
    suffix: str,
) -> tuple[str, str | None]:
    dependencies = record.get("Depends", "")
    matches = re.findall(
        r"(linux-image-([0-9][0-9A-Za-z.+~_-]*))"
        r"(?:\s*\(=\s*([0-9A-Za-z][0-9A-Za-z.+:~_-]*)\))?",
        dependencies,
    )
    selected = [
        (name, version)
        for name, release, version in matches
        if release.endswith(f"-{suffix}")
    ]
    if len(selected) != 1:
        fail("kernel selector does not have one exact versioned image")
    return selected[0]


def _source_field(record: dict[str, str]) -> tuple[str, str]:
    value = record.get("Source", "")
    match = re.fullmatch(
        r"([a-z0-9][a-z0-9+.-]*)(?: "
        r"\(([0-9A-Za-z][0-9A-Za-z.+:~_-]*)\))?",
        value,
    )
    if match is None:
        fail("kernel headers do not identify their source package")
    return match.group(1), match.group(2) or record["Version"]


def _is_older(runner: Runner, candidate: str, current: str) -> bool:
    return runner.status(
        ["dpkg", "--compare-versions", candidate, "lt", current]
    ) == 0


def observe(
    channel: dict[str, Any],
    current: dict[str, Any],
    as_of: datetime,
    runner: Runner,
) -> KernelObservation:
    discovery = channel["discovery"]
    suite = discovery["suite"]
    stamp = as_of.strftime("%Y%m%dT%H%M%SZ")
    runner.run(
        [
            "apt-get",
            "-o",
            "Acquire::Check-Valid-Until=false",
            "-o",
            "Acquire::https::Verify-Peer=false",
            "update",
        ]
    )
    runner.run(
        [
            "apt-get",
            "-o",
            "Acquire::https::Verify-Peer=false",
            "install",
            "-y",
            "--no-install-recommends",
            "ca-certificates",
        ]
    )
    if channel["distro"] == "ubuntu":
        source_list = (
            "Types: deb\n"
            f"URIs: https://snapshot.ubuntu.com/ubuntu/{stamp}/\n"
            f"Suites: {suite} {suite}-updates {suite}-security\n"
            "Components: main universe\n"
            "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n"
        )
        runner.replace_apt_sources("zerofs.sources", source_list)
    else:
        source_list = (
            "deb [check-valid-until=no "
            "signed-by=/usr/share/keyrings/debian-archive-keyring.gpg] "
            f"https://snapshot.debian.org/archive/debian/{stamp} "
            "trixie main\n"
            "deb [check-valid-until=no "
            "signed-by=/usr/share/keyrings/debian-archive-keyring.gpg] "
            f"https://snapshot.debian.org/archive/debian/{stamp} "
            f"{suite} main\n"
        )
        runner.replace_apt_sources("sources.list", source_list)
    runner.run(
        [
            "apt-get",
            "-o",
            "Acquire::Check-Valid-Until=false",
            "update",
        ]
    )

    selector_version, selector = _candidate(
        runner,
        discovery["selector"],
        suite,
    )
    suffix = current["kernel_release"].rsplit("-", 1)[-1]
    package_name, package_version = _versioned_image(selector, suffix)
    if not package_version:
        package_version, _ = _candidate(runner, package_name, suite)
    kernel_release = package_name.removeprefix("linux-image-")
    _show_exact(runner, package_name, package_version, suite)
    headers = _show_exact(
        runner,
        f"linux-headers-{kernel_release}",
        package_version,
        suite,
    )
    source_name, source_version = _source_field(headers)

    if channel["distro"] == "ubuntu":
        _show_exact(
            runner,
            f"linux-modules-{kernel_release}",
            package_version,
            suite,
        )
        identity = f"ubuntu:{source_name}@{source_version}"
    else:
        if source_name != "linux":
            fail(f"unexpected Debian kernel source package: {source_name}")
        source_release = kernel_release.split("+", 1)[0]
        if source_release.count(".") < 2:
            fail(f"cannot derive Debian source series from {kernel_release}")
        source_series = source_release.rsplit(".", 1)[0]
        _show_exact(
            runner,
            f"linux-source-{source_series}",
            source_version,
            suite,
        )
        identity = f"debian:linux@{source_version}:{suite}"

    if _is_older(runner, package_version, current["kernel_package_version"]):
        fail(
            f"{channel['id']}: discovered kernel {package_version} "
            f"is older than {current['kernel_package_version']}"
        )
    if _is_older(
        runner,
        selector_version,
        current["kernel_selector_version"],
    ):
        fail(
            f"{channel['id']}: discovered kernel selector "
            f"{selector_version} is older than "
            f"{current['kernel_selector_version']}"
        )
    return KernelObservation(
        kernel_release=kernel_release,
        kernel_package_name=package_name,
        kernel_package_version=package_version,
        kernel_selector_version=selector_version,
        source_kind="apt-snapshot",
        source_identity=identity,
        source_snapshot=stamp,
    )


def discover(
    channel: dict[str, Any],
    current: dict[str, Any],
    as_of: datetime,
    runner: Runner,
) -> dict[str, Any]:
    observation = observe(channel, current, as_of, runner)
    if compare_observation(current, observation).update_available:
        source = {
            "kind": observation.source_kind,
            "identity": observation.source_identity,
            "snapshot": observation.source_snapshot,
        }
    else:
        source = copy.deepcopy(current["source"])
    return base_candidate(
        channel,
        current,
        observation.kernel_release,
        observation.kernel_package_name,
        observation.kernel_package_version,
        source,
        selector_version=observation.kernel_selector_version,
    )
