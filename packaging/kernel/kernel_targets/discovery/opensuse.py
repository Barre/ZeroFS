import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta
from typing import Any

from ..catalog import OPENSUSE_PACKAGES, fail
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


def _versions(runner: Runner) -> dict[str, str]:
    versions = {}
    for package in OPENSUSE_PACKAGES:
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
    current_lock: dict[str, Any],
    as_of: datetime,
    runner: Runner,
) -> dict[str, Any]:
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
    versions = _versions(runner)
    old_versions = current_lock["packages"]
    for package in OPENSUSE_PACKAGES:
        old = old_versions[package]
        if rpm_compare(runner, versions[package], old) < 0:
            fail(
                f"{channel['id']}: {package} {versions[package]} "
                f"is older than {old}"
            )

    return {"snapshot": snapshot, "packages": versions}


def discover(
    channel: dict[str, Any],
    current: dict[str, Any],
    current_lock: dict[str, Any],
    as_of: datetime,
    runner: Runner,
) -> dict[str, Any]:
    lock = observe(channel, current_lock, as_of, runner)
    if lock["packages"] == current_lock["packages"]:
        lock = current_lock
    return base_candidate(current, lock)
