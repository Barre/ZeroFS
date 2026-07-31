from pathlib import Path
from typing import Any, Protocol

from ..catalog import PACKAGE_VERSION_PATTERN, fail


class Runner(Protocol):
    def run(self, arguments: list[str]) -> str: ...

    def status(self, arguments: list[str]) -> int: ...

    def replace_apt_sources(self, filename: str, contents: str) -> None: ...

    def download(self, url: str, destination: Path) -> None: ...

    def url_exists(self, url: str) -> bool: ...

    def kernel_auto_conf(self, package: Path, destination: Path) -> str: ...


def require_native_arch(channel: dict[str, Any], runner: Runner) -> None:
    expected = channel["arch"]
    actual = runner.run(["uname", "-m"]).strip()
    if actual != expected:
        fail(
            f"channel {channel['id']!r} requires native {expected}, "
            f"not {actual or 'an unknown architecture'}"
        )


def base_candidate(
    channel: dict[str, Any],
    current: dict[str, Any],
    kernel_release: str,
    package_name: str,
    package_version: str,
    source: dict[str, Any],
    *,
    selector_version: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "channel_id": channel["id"],
        "base_target_id": current["id"],
        "kernel_release": kernel_release,
        "kernel_package_name": package_name,
        "kernel_package_version": package_version,
        "kernel_selector_version": selector_version or package_version,
        "source": source,
    }


def rpm_compare(runner: Runner, left: str, right: str) -> int:
    for label, value in (("left", left), ("right", right)):
        if not PACKAGE_VERSION_PATTERN.fullmatch(value):
            fail(f"invalid RPM version for {label} operand: {value!r}")
    expression = (
        "%{lua:print(rpm.vercmp([[" + left + "]],[[" + right + "]]))}"
    )
    output = runner.run(["rpm", "--eval", expression]).strip()
    if output not in {"-1", "0", "1"}:
        fail(f"rpm returned an invalid version comparison: {output!r}")
    return int(output)
