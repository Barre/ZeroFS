from datetime import datetime
from typing import Any

from ..catalog import Catalog, fail
from ..observation import KernelObservation, availability_entry
from ..updates import latest_targets, validate_candidate
from .apt import discover as discover_apt
from .apt import observe as observe_apt
from .common import Runner, require_native_arch
from .fedora import discover as discover_fedora
from .fedora import observe as observe_fedora
from .opensuse import discover as discover_opensuse
from .opensuse import observe as observe_opensuse
from .runtime import SystemRunner


def _context(
    catalog: Catalog,
    channel_id: str,
    runner: Runner | None,
) -> tuple[dict[str, Any], dict[str, Any], Runner]:
    channel = catalog.channels.get(channel_id)
    if channel is None:
        fail(f"unknown channel id: {channel_id}")
    current = latest_targets(catalog)[channel_id]
    active_runner = runner or SystemRunner()
    require_native_arch(channel, active_runner)
    return channel, current, active_runner


def observe_kernel(
    catalog: Catalog,
    channel_id: str,
    as_of: datetime,
    runner: Runner | None = None,
) -> tuple[dict[str, Any], dict[str, Any], KernelObservation]:
    channel, current, active_runner = _context(catalog, channel_id, runner)
    kind = channel["discovery"]["kind"]
    if kind in {"ubuntu-snapshot", "debian-snapshot"}:
        observation = observe_apt(channel, current, as_of, active_runner)
    elif kind == "fedora-koji":
        observation = observe_fedora(channel, current, active_runner)
    else:
        observation = observe_opensuse(
            channel,
            current,
            as_of,
            active_runner,
        )
    return channel, current, observation


def check_channel(
    catalog: Catalog,
    channel_id: str,
    as_of: datetime,
    runner: Runner | None = None,
) -> dict[str, Any]:
    channel, current, observation = observe_kernel(
        catalog,
        channel_id,
        as_of,
        runner,
    )
    return availability_entry(channel, current, observation)


def discover_candidate(
    catalog: Catalog,
    channel_id: str,
    as_of: datetime,
    runner: Runner | None = None,
) -> dict[str, Any]:
    channel, current, active_runner = _context(catalog, channel_id, runner)
    kind = channel["discovery"]["kind"]
    if kind in {"ubuntu-snapshot", "debian-snapshot"}:
        candidate = discover_apt(channel, current, as_of, active_runner)
    elif kind == "fedora-koji":
        candidate = discover_fedora(channel, current, active_runner)
    else:
        candidate = discover_opensuse(
            channel,
            current,
            as_of,
            active_runner,
        )
    return validate_candidate(candidate, "discovered candidate", catalog.channels)
