from datetime import datetime
from typing import Any

from ..catalog import Catalog, fail
from ..updates import latest_targets, validate_candidate
from .apt import discover as discover_apt
from .common import Runner, require_native_arch
from .fedora import discover as discover_fedora
from .opensuse import discover as discover_opensuse
from .runtime import SystemRunner


def _context(
    catalog: Catalog,
    channel_id: str,
    runner: Runner | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], Runner]:
    channel = catalog.channels.get(channel_id)
    if channel is None:
        fail(f"unknown channel id: {channel_id}")
    current = latest_targets(catalog)[channel_id]
    current_lock = catalog.raw_lock(current["id"])
    active_runner = runner or SystemRunner()
    require_native_arch(channel, active_runner)
    return channel, current, current_lock, active_runner


def discover_candidate(
    catalog: Catalog,
    channel_id: str,
    as_of: datetime,
    runner: Runner | None = None,
) -> dict[str, Any]:
    channel, current, current_lock, active_runner = _context(
        catalog, channel_id, runner
    )
    provider = channel["distro"]
    if provider in {"ubuntu", "debian"}:
        candidate = discover_apt(
            channel, current, current_lock, as_of, active_runner
        )
    elif provider == "fedora":
        candidate = discover_fedora(channel, current, current_lock, active_runner)
    else:
        candidate = discover_opensuse(
            channel,
            current,
            current_lock,
            as_of,
            active_runner,
        )
    return validate_candidate(candidate, "discovered candidate", catalog)
