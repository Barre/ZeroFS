import copy
from pathlib import Path
from typing import Any

from .catalog import (
    ID_PATTERN,
    KERNEL_RETENTION,
    Catalog,
    fail,
    read_json,
    target_from_lock,
    validate_lock,
    validate_catalog,
    validate_string,
)


CANDIDATE_FIELDS = {
    "base_target_id",
    "lock",
}


def validate_candidate(
    value: Any,
    label: str,
    catalog: Catalog,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != CANDIDATE_FIELDS:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(CANDIDATE_FIELDS))}"
        )
    base_target_id = value["base_target_id"]
    validate_string(base_target_id, f"{label}.base_target_id")
    if not ID_PATTERN.fullmatch(base_target_id):
        fail(f"{label}.base_target_id: unsupported target id")

    base_target = catalog.targets_by_id.get(base_target_id)
    if base_target is None:
        fail(
            f"{label}: stale candidate based on unknown target "
            f"{base_target_id!r}"
        )
    channel = catalog.channels[base_target["channel_id"]]
    provider = channel["distro"]
    lock = validate_lock(
        value["lock"],
        f"{label}.lock",
        provider,
        channel["arch"],
    )
    if provider == "fedora":
        fingerprint = channel["discovery"]["signing_fingerprint"]
        if lock["signing_fingerprint"] != fingerprint:
            fail(
                f"{label}.lock.signing_fingerprint: expected {fingerprint!r}"
            )
    return {
        "base_target_id": base_target_id,
        "lock": lock,
    }


def load_candidate(
    path: Path,
    catalog: Catalog,
) -> dict[str, Any]:
    return validate_candidate(read_json(path, "candidate"), str(path), catalog)


def latest_targets(catalog: Catalog) -> dict[str, dict[str, Any]]:
    latest = {}
    for target in catalog.targets:
        latest[target["channel_id"]] = target
    return latest


def package_identity(value: dict[str, Any]) -> tuple[str, ...]:
    source = value["source"]
    return (
        value["kernel_release"],
        value["kernel_package_version"],
        source["identity"],
    )


def _channel_targets(catalog: Catalog) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for target in catalog.targets:
        result.setdefault(target["channel_id"], []).append(target)
    return result


def update_candidates(
    catalog: Catalog,
    candidates: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    by_channel = {}
    for candidate in candidates:
        base_target_id = candidate["base_target_id"]
        base_target = catalog.targets_by_id.get(base_target_id)
        if base_target is None:
            fail(f"stale candidate based on unknown target {base_target_id!r}")
        channel_id = base_target["channel_id"]
        if channel_id in by_channel:
            fail(f"multiple candidates for channel {channel_id!r}")
        by_channel[channel_id] = candidate

    current = latest_targets(catalog)
    targets_by_channel = _channel_targets(catalog)

    updates = []
    for channel_id in sorted(by_channel):
        candidate = by_channel[channel_id]
        current_target = current[channel_id]
        if candidate["base_target_id"] != current_target["id"]:
            fail(
                f"{channel_id}: stale candidate based on "
                f"{candidate['base_target_id']!r}; current target is "
                f"{current_target['id']!r}"
            )

        location = catalog.target_locations[current_target["id"]]
        stream = catalog.document["streams"][location.stream_id]
        candidate_target = target_from_lock(
            catalog.channels[channel_id],
            stream,
            candidate["lock"],
        )
        for retained_target in targets_by_channel[channel_id][:-1]:
            if (
                candidate_target["kernel_release"]
                == retained_target["kernel_release"]
            ):
                fail(
                    f"{channel_id}: cannot replace non-latest retained kernel "
                    f"{retained_target['kernel_release']!r}"
                )
        package_changed = package_identity(candidate_target) != package_identity(
            current_target
        )
        current_lock = catalog.raw_lock(current_target["id"])
        signed_source_changed = (
            stream["provider"] == "fedora"
            and candidate["lock"]["signing_fingerprint"]
            != current_lock["signing_fingerprint"]
        )
        update_required = package_changed or signed_source_changed
        if (
            not update_required
            and candidate["lock"].get("artifacts")
            != current_lock.get("artifacts")
        ):
            fail(
                f"{channel_id}: artifact hashes changed for "
                f"{candidate_target['source']['identity']}"
            )
        if update_required:
            updates.append((channel_id, candidate, candidate_target))
    return updates


def _channel_locks(catalog: Catalog) -> dict[str, list[dict[str, Any]]]:
    result = {}
    for channel_id, targets in _channel_targets(catalog).items():
        location = catalog.target_locations[targets[-1]["id"]]
        result[channel_id] = catalog.document["streams"][location.stream_id][
            "architectures"
        ][location.arch]
    return result


def _apply_candidates(
    catalog: Catalog,
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any], set[str]]:
    updates = update_candidates(catalog, candidates)
    document = copy.deepcopy(catalog.document)
    targets_by_channel = _channel_targets(catalog)

    for channel_id, candidate, candidate_target in updates:
        targets = targets_by_channel[channel_id]
        current_target = targets[-1]
        current_location = catalog.target_locations[targets[-1]["id"]]
        locks = document["streams"][current_location.stream_id]["architectures"][
            current_location.arch
        ]
        candidate_lock = copy.deepcopy(candidate["lock"])
        if candidate_target["kernel_release"] == current_target["kernel_release"]:
            locks[-1] = candidate_lock
        else:
            locks.append(candidate_lock)
            del locks[:-KERNEL_RETENTION]
    updated_channels = {
        channel_id for channel_id, _candidate, _candidate_target in updates
    }
    return document, updated_channels


def _validate_lock_transition(
    channel_id: str,
    base_locks: list[dict[str, Any]],
    current_locks: list[dict[str, Any]],
    base_targets: list[dict[str, Any]],
    current_targets: list[dict[str, Any]],
    current_signing_fingerprint: str | None = None,
) -> None:
    minimum = min(len(base_locks), KERNEL_RETENTION)
    if len(current_locks) < minimum:
        fail(
            f"channel {channel_id!r} cannot drop retained kernels "
            f"below {minimum}; found {len(current_locks)}"
        )
    added = [lock for lock in current_locks if lock not in base_locks]
    if not added:
        if current_locks != base_locks:
            fail(f"channel {channel_id!r} cannot remove or reorder retained kernels")
        return
    if len(added) != 1:
        fail(f"channel {channel_id!r} can add only one kernel per update")

    added_index = current_locks.index(added[0])
    new_target = current_targets[added_index]
    matching_old = [
        (target, lock)
        for target, lock in zip(base_targets, base_locks)
        if target["kernel_release"] == new_target["kernel_release"]
    ]
    if (
        matching_old
        and matching_old[-1][0]["id"] != base_targets[-1]["id"]
    ):
        fail(
            f"channel {channel_id!r} cannot replace non-latest retained kernel "
            f"{new_target['kernel_release']!r}"
        )
    same_package = matching_old and package_identity(
        matching_old[-1][0]
    ) == package_identity(new_target)
    old_lock = matching_old[-1][1] if matching_old else {}
    signed_again = (
        current_signing_fingerprint is not None
        and old_lock.get("signing_fingerprint") is not None
        and old_lock["signing_fingerprint"]
        != added[0].get("signing_fingerprint")
        and added[0].get("signing_fingerprint")
        == current_signing_fingerprint
    )
    if same_package and not signed_again:
        fail(
            f"channel {channel_id!r} cannot change an existing locked package"
        )

    expected = [
        lock
        for lock, target in zip(base_locks, base_targets)
        if target["kernel_release"] != new_target["kernel_release"]
    ]
    expected.append(added[0])
    expected = expected[-KERNEL_RETENTION:]
    if current_locks != expected:
        fail(
            f"channel {channel_id!r} may only replace the same kernel or "
            "append one kernel and prune the oldest"
        )


def pending_update_channels(
    pending_base: Catalog,
    pending_head: Catalog,
) -> set[str]:
    if pending_base.channels != pending_head.channels:
        fail("pending update changes non-lock configuration")
    base_locks = _channel_locks(pending_base)
    head_locks = _channel_locks(pending_head)
    changed_channels = {
        channel_id
        for channel_id in base_locks
        if base_locks[channel_id] != head_locks[channel_id]
    }
    if not changed_channels:
        fail("pending update does not change any kernel locks")
    base_targets = _channel_targets(pending_base)
    head_targets = _channel_targets(pending_head)
    for channel_id in sorted(changed_channels):
        location = pending_base.target_locations[
            base_targets[channel_id][-1]["id"]
        ]
        stream = pending_head.document["streams"][location.stream_id]
        signing_fingerprint = (
            stream["signing_fingerprint"]
            if stream["provider"] == "fedora"
            else None
        )
        _validate_lock_transition(
            channel_id,
            base_locks[channel_id],
            head_locks[channel_id],
            base_targets[channel_id],
            head_targets[channel_id],
            signing_fingerprint,
        )
    return changed_channels


def reconcile_candidates(
    catalog: Catalog,
    candidates: list[dict[str, Any]],
    *,
    pending_base: Catalog | None = None,
    pending_head: Catalog | None = None,
) -> dict[str, Any]:
    if (pending_base is None) != (pending_head is None):
        fail("pending base and head manifests must be provided together")

    document, updated_channels = _apply_candidates(catalog, candidates)

    if pending_base is not None and pending_head is not None:
        changed_channels = pending_update_channels(pending_base, pending_head)
        current_locks = _channel_locks(catalog)
        current_targets = latest_targets(catalog)
        pending_base_locks = _channel_locks(pending_base)
        pending_head_locks = _channel_locks(pending_head)
        for channel_id in sorted(changed_channels - updated_channels):
            current = current_locks.get(channel_id)
            if current is None:
                continue
            previous = pending_base_locks[channel_id]
            proposed = pending_head_locks[channel_id]
            if current == proposed:
                continue
            if current != previous:
                continue
            if catalog.channels.get(channel_id) != pending_base.channels.get(
                channel_id
            ):
                continue
            location = catalog.target_locations[current_targets[channel_id]["id"]]
            document["streams"][location.stream_id]["architectures"][
                location.arch
            ] = copy.deepcopy(proposed)

    validate_catalog(document, "reconciled manifest")
    return document
