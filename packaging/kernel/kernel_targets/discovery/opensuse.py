import copy
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta
from typing import Any

from ..catalog import PACKAGE_VERSION_PATTERN, fail
from ..observation import KernelObservation, compare_observation
from ..updates import parse_opensuse_identity
from .common import Runner, base_candidate, rpm_compare


def _snapshot(as_of: datetime, runner: Runner) -> str:
    start = as_of.date() - timedelta(days=1)
    for offset in range(31):
        snapshot = (start - timedelta(days=offset)).strftime("%Y%m%d")
        url = (
            "https://download.opensuse.org/history/"
            f"{snapshot}/tumbleweed/repo/oss/repodata/repomd.xml"
        )
        if runner.url_exists(url):
            return snapshot
    fail("cannot find an openSUSE history snapshot in the previous 31 days")


def _versions(packages: list[str], runner: Runner) -> dict[str, str]:
    versions = {}
    for package in packages:
        output = runner.run(
            [
                "zypper",
                "--non-interactive",
                "--xmlout",
                "--no-refresh",
                "search",
                "--details",
                "--match-exact",
                "--type",
                "package",
                package,
            ]
        )
        try:
            root = ElementTree.fromstring(output)
        except ElementTree.ParseError as error:
            fail(f"invalid zypper XML for {package}: {error}")
        editions = {
            item.attrib["edition"]
            for item in root.iter()
            if item.tag.rsplit("}", 1)[-1] == "solvable"
            and item.attrib.get("name") == package
            and item.attrib.get("repository", item.attrib.get("repo"))
            == "zerofs-snapshot"
            and item.attrib.get("arch") in {"x86_64", "noarch"}
            and "edition" in item.attrib
        }
        if not editions:
            fail(f"openSUSE snapshot does not contain {package}")
        selected = None
        for edition in sorted(editions):
            if selected is None or rpm_compare(runner, edition, selected) > 0:
                selected = edition
        assert selected is not None
        versions[package] = selected
    return versions


def observe(
    channel: dict[str, Any],
    current: dict[str, Any],
    as_of: datetime,
    runner: Runner,
) -> KernelObservation:
    runner.run(
        [
            "zypper",
            "--non-interactive",
            "install",
            "--no-recommends",
            "ca-certificates",
        ]
    )
    snapshot = _snapshot(as_of, runner)
    repository = (
        "https://download.opensuse.org/history/"
        f"{snapshot}/tumbleweed/repo/oss/"
    )
    runner.run(["zypper", "--non-interactive", "removerepo", "--all"])
    runner.run(
        [
            "zypper",
            "--non-interactive",
            "addrepo",
            "--check",
            repository,
            "zerofs-snapshot",
        ]
    )
    runner.run(["zypper", "--non-interactive", "refresh"])
    packages = channel["discovery"]["packages"]
    versions = _versions(packages, runner)
    old_versions = parse_opensuse_identity(
        current["source"]["identity"],
        packages,
        current["kernel_package_version"],
        f"target {current['id']!r}",
    )
    for package in packages:
        old = old_versions.get(package)
        if old is None:
            fail(f"current openSUSE identity omits {package}")
        if rpm_compare(runner, versions[package], old) < 0:
            fail(
                f"{channel['id']}: {package} {versions[package]} "
                f"is older than {old}"
            )

    identity = ",".join(f"{package}@{versions[package]}" for package in packages)
    edition = versions[channel["discovery"]["selector"]]
    version, separator, release = edition.rpartition("-")
    if (
        not separator
        or "." not in release
        or not PACKAGE_VERSION_PATTERN.fullmatch(edition)
    ):
        fail(f"cannot derive openSUSE uname from {edition}")
    kernel_release = (
        f"{version}-{release.rsplit('.', 1)[0]}-{channel['flavor']}"
    )
    return KernelObservation(
        kernel_release=kernel_release,
        kernel_package_name=channel["discovery"]["selector"],
        kernel_package_version=edition,
        kernel_selector_version=edition,
        source_kind="opensuse-history",
        source_identity=identity,
        source_snapshot=snapshot,
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
