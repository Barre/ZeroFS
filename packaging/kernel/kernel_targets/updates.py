import copy
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .catalog import (
    DISCOVERY_PROVIDERS,
    ID_PATTERN,
    PACKAGE_VERSION_PATTERN,
    REPOSITORY_TOKEN_PATTERN,
    TARGET_LIFECYCLE_FIELDS,
    Catalog,
    fail,
    read_json,
    validate_catalog,
    validate_source,
    validate_string,
)
from .observation import (
    ObservationComparison,
    candidate_observation,
    compare_observation,
)


CANDIDATE_FIELDS = {
    "schema_version",
    "channel_id",
    "base_target_id",
    "kernel_release",
    "kernel_package_name",
    "kernel_package_version",
    "kernel_selector_version",
    "source",
}
RUNNERS = {
    "x86_64": "ubuntu-26.04",
    "aarch64": "ubuntu-26.04-arm",
}
APT_SNAPSHOT_PATTERN = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
OPENSUSE_SNAPSHOT_PATTERN = re.compile(r"^[0-9]{8}$")


@dataclass(frozen=True)
class CandidateAssessment:
    candidate: dict[str, Any]
    current_target: dict[str, Any]
    builder_image: str
    comparison: ObservationComparison
    builder_image_changed: bool

    @property
    def target_update_required(self) -> bool:
        return self.comparison.update_available or self.builder_image_changed


def validate_candidate(
    value: Any,
    label: str,
    channels: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != CANDIDATE_FIELDS:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(CANDIDATE_FIELDS))}"
        )
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        fail(f"{label}.schema_version: must be 1")
    for field in (
        "channel_id",
        "base_target_id",
        "kernel_release",
        "kernel_package_name",
        "kernel_package_version",
        "kernel_selector_version",
    ):
        validate_string(value[field], f"{label}.{field}")
    if not ID_PATTERN.fullmatch(value["base_target_id"]):
        fail(f"{label}.base_target_id: unsupported target id")
    if not PACKAGE_VERSION_PATTERN.fullmatch(value["kernel_release"]):
        fail(f"{label}.kernel_release: unsupported kernel release")
    if not REPOSITORY_TOKEN_PATTERN.fullmatch(
        value["kernel_package_name"]
    ):
        fail(f"{label}.kernel_package_name: unsupported package name")
    if not PACKAGE_VERSION_PATTERN.fullmatch(
        value["kernel_package_version"]
    ):
        fail(f"{label}.kernel_package_version: unsupported package version")
    if not PACKAGE_VERSION_PATTERN.fullmatch(
        value["kernel_selector_version"]
    ):
        fail(f"{label}.kernel_selector_version: unsupported package version")

    channel_id = value["channel_id"]
    channel = channels.get(channel_id)
    if channel is None:
        fail(f"{label}.channel_id: unknown channel {channel_id!r}")
    source = validate_source(value["source"], f"{label}.source")
    expected_source_kind = DISCOVERY_PROVIDERS[
        channel["discovery"]["kind"]
    ]["source_kind"]
    if source["kind"] != expected_source_kind:
        fail(
            f"{label}.source.kind: expected {expected_source_kind} "
            f"for channel {channel_id}"
        )

    discovery = channel["discovery"]
    discovery_kind = discovery["kind"]
    if discovery_kind in {"ubuntu-snapshot", "debian-snapshot"}:
        expected_name = f"linux-image-{value['kernel_release']}"
        if value["kernel_package_name"] != expected_name:
            fail(
                f"{label}.kernel_package_name: expected {expected_name!r}"
            )
        if not APT_SNAPSHOT_PATTERN.fullmatch(source["snapshot"]):
            fail(f"{label}.source.snapshot: expected a UTC APT snapshot")
        if discovery_kind == "ubuntu-snapshot":
            identity_match = re.fullmatch(
                r"ubuntu:([a-z0-9][a-z0-9+.-]*)@"
                r"([0-9A-Za-z][0-9A-Za-z.+:~_-]*)",
                source["identity"],
            )
            if identity_match is None:
                fail(
                    f"{label}.source.identity: invalid "
                    f"{discovery_kind} identity"
                )
            if "-hwe-" in discovery["selector"]:
                kernel_series = ".".join(
                    value["kernel_release"].split("-", 1)[0].split(".")[:2]
                )
                expected_source = f"linux-hwe-{kernel_series}"
            else:
                expected_source = "linux"
            if (
                identity_match.group(1) != expected_source
                or identity_match.group(2)
                != value["kernel_package_version"]
            ):
                fail(
                    f"{label}.source.identity: expected "
                    f"ubuntu:{expected_source}@"
                    f"{value['kernel_package_version']}"
                )
        else:
            identity_pattern = (
                r"debian:linux@"
                + re.escape(value["kernel_package_version"])
                + r":"
                + re.escape(discovery["suite"])
            )
            if not re.fullmatch(identity_pattern, source["identity"]):
                fail(
                    f"{label}.source.identity: invalid "
                    f"{discovery_kind} identity"
                )
    elif discovery_kind == "fedora-koji":
        arch = channel["arch"]
        if value["kernel_package_name"] != "kernel-core-uname-r":
            fail(
                f"{label}.kernel_package_name: "
                "expected 'kernel-core-uname-r'"
            )
        expected_snapshot = (
            "koji-signed-build:"
            f"{discovery['signing_fingerprint']}:{arch},noarch,src"
        )
        if source["snapshot"] != expected_snapshot:
            fail(
                f"{label}.source.snapshot: expected "
                f"{expected_snapshot!r}"
            )
        if not source["identity"].startswith("kernel-"):
            fail(f"{label}.source.identity: expected a kernel NVR")
        kernel_nvr = source["identity"].removeprefix("kernel-")
        expected_version = f"{kernel_nvr}.{arch}"
        if (
            value["kernel_package_version"] != expected_version
            or value["kernel_release"] != expected_version
        ):
            fail(
                f"{label}: Fedora kernel release and package version "
                f"must be {expected_version!r}"
            )
        if value["kernel_selector_version"] != kernel_nvr:
            fail(
                f"{label}.kernel_selector_version: "
                f"must be {kernel_nvr!r}"
            )
        validate_fedora_artifacts(
            source["artifacts"],
            kernel_nvr,
            arch,
            label,
        )
    else:
        if value["kernel_package_name"] != "kernel-default":
            fail(f"{label}.kernel_package_name: expected 'kernel-default'")
        if not OPENSUSE_SNAPSHOT_PATTERN.fullmatch(source["snapshot"]):
            fail(f"{label}.source.snapshot: expected an openSUSE date")
        parse_opensuse_identity(
            source["identity"],
            discovery["packages"],
            value["kernel_package_version"],
            label,
        )
        if value["kernel_selector_version"] != value["kernel_package_version"]:
            fail(
                f"{label}.kernel_selector_version: must match "
                "kernel-default version"
            )

    candidate = copy.deepcopy(value)
    candidate["source"] = source
    return candidate


def validate_fedora_artifacts(
    artifacts: dict[str, str],
    kernel_nvr: str,
    arch: str,
    label: str,
) -> None:
    rpm_suffix = f".{arch}.rpm"
    kernel_artifacts = {
        f"kernel-{kernel_nvr}.src.rpm",
        *(
            f"{name}-{kernel_nvr}.{arch}.rpm"
            for name in ("kernel-core", "kernel-devel", "kernel-modules-core")
        ),
    }
    rust_packages = ("cargo", "rust", "rust-std-static", "rustfmt")
    rust_binaries = [
        filename
        for filename in artifacts
        if filename.startswith("rust-")
        and not filename.startswith(("rust-src-", "rust-std-static-"))
        and filename.endswith(rpm_suffix)
    ]
    if len(rust_binaries) != 1:
        fail(f"{label}.source.artifacts: cannot select the Rust NVR")
    rust_nvr = rust_binaries[0][len("rust-") : -len(rpm_suffix)]
    rust_artifacts = {
        f"{name}-{rust_nvr}.{arch}.rpm" for name in rust_packages
    }
    rust_artifacts.add(f"rust-src-{rust_nvr}.noarch.rpm")
    if set(artifacts) != kernel_artifacts | rust_artifacts:
        fail(f"{label}.source.artifacts: unexpected Fedora artifact set")


def parse_opensuse_identity(
    identity: str,
    packages: list[str],
    kernel_package_version: str,
    label: str,
) -> dict[str, str]:
    entries = identity.split(",")
    if len(entries) != len(packages):
        fail(f"{label}.source.identity: unexpected openSUSE package count")
    versions = {}
    for entry, package in zip(entries, packages):
        prefix = f"{package}@"
        if not entry.startswith(prefix):
            fail(
                f"{label}.source.identity: expected package {package!r}"
            )
        version = entry.removeprefix(prefix)
        if not PACKAGE_VERSION_PATTERN.fullmatch(version):
            fail(
                f"{label}.source.identity: invalid version for {package!r}"
            )
        versions[package] = version
    if versions[packages[0]] != kernel_package_version:
        fail(
            f"{label}.source.identity: kernel-default version does not match"
        )
    return versions


def load_candidate(
    path: Path,
    channels: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return validate_candidate(
        read_json(path, "candidate"),
        str(path),
        channels,
    )


def latest_targets(catalog: Catalog) -> dict[str, dict[str, Any]]:
    latest = {}
    for target in catalog.targets:
        latest[target["channel_id"]] = target
    return latest


def package_identity(value: dict[str, Any]) -> tuple[str, ...]:
    source = value["source"]
    return (
        value["kernel_release"],
        value["kernel_package_name"],
        value["kernel_package_version"],
        value["kernel_selector_version"],
        source["kind"],
        source["identity"],
    )


def target_id(channel_id: str, kernel_release: str, revision: int) -> str:
    release = re.sub(r"[^a-z0-9.]+", "-", kernel_release.lower()).strip(".-")
    if not release:
        fail(f"cannot derive target id from kernel release {kernel_release!r}")
    return f"{channel_id}-{release}-r{revision}"


def retire_channel_targets(
    raw_targets: list[dict[str, Any]],
    channel_id: str,
) -> None:
    for target in raw_targets:
        if target["channel_id"] == channel_id:
            target["ci"] = False
            target["publish"] = False


def assess_candidates(
    catalog: Catalog,
    candidates: list[dict[str, Any]],
) -> list[CandidateAssessment]:
    by_channel = {}
    for candidate in candidates:
        channel_id = candidate["channel_id"]
        if channel_id in by_channel:
            fail(f"multiple candidates for channel {channel_id!r}")
        by_channel[channel_id] = candidate

    current = latest_targets(catalog)
    targets_by_channel: dict[str, list[dict[str, Any]]] = {}
    for target in catalog.targets:
        targets_by_channel.setdefault(target["channel_id"], []).append(target)

    assessments = []
    for channel_id in sorted(by_channel):
        candidate = by_channel[channel_id]
        current_target = current[channel_id]
        if candidate["base_target_id"] != current_target["id"]:
            fail(
                f"{channel_id}: stale candidate based on "
                f"{candidate['base_target_id']!r}; current target is "
                f"{current_target['id']!r}"
            )

        observation = candidate_observation(candidate)
        comparison = compare_observation(
            current_target,
            observation,
        )
        if not comparison.update_available:
            if (
                candidate["source"].get("artifacts")
                != current_target["source"].get("artifacts")
            ):
                fail(
                    f"{channel_id}: artifact hashes changed for "
                    f"{candidate['source']['identity']}"
                )
        builder_image = catalog.channels[channel_id]["discovery"][
            "builder_image"
        ]
        builder_image_changed = current_target["builder_image"] != builder_image
        if comparison.package_changed:
            identity = package_identity(candidate)
            for known_target in targets_by_channel[channel_id][:-1]:
                if identity == package_identity(known_target):
                    fail(
                        f"{channel_id}: candidate matches retired target "
                        f"{known_target['id']!r}"
                    )

        assessments.append(
            CandidateAssessment(
                candidate=candidate,
                current_target=current_target,
                builder_image=builder_image,
                comparison=comparison,
                builder_image_changed=builder_image_changed,
            )
        )
    return assessments


def apply_candidates(
    catalog: Catalog,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    assessments = assess_candidates(catalog, candidates)
    document = copy.deepcopy(catalog.document)
    raw_targets = document["targets"]
    known_ids = set(catalog.targets_by_id)

    for assessment in assessments:
        candidate = assessment.candidate
        current_target = assessment.current_target
        channel_id = candidate["channel_id"]
        if not assessment.target_update_required:
            retire_channel_targets(raw_targets, channel_id)
            for target in raw_targets:
                if target["id"] == current_target["id"]:
                    target["enabled"] = True
                    target["ci"] = True
                    target["publish"] = True
                    break
            continue

        revision = int(current_target["package_revision"]) + 1
        new_id = target_id(
            channel_id,
            candidate["kernel_release"],
            revision,
        )
        if new_id in known_ids:
            fail(f"{channel_id}: generated target id already exists: {new_id}")
        known_ids.add(new_id)
        retire_channel_targets(raw_targets, channel_id)
        raw_targets.append(
            {
                "id": new_id,
                "enabled": True,
                "ci": True,
                "publish": True,
                "channel_id": channel_id,
                "package_revision": str(revision),
                "kernel_release": candidate["kernel_release"],
                "kernel_package_name": candidate["kernel_package_name"],
                "kernel_package_version": candidate[
                    "kernel_package_version"
                ],
                "kernel_selector_version": candidate[
                    "kernel_selector_version"
                ],
                "builder_image": assessment.builder_image,
                "source": copy.deepcopy(candidate["source"]),
            }
        )

    validate_catalog(document, "updated manifest")
    return document


def newly_published_targets(
    base: Catalog,
    current: Catalog,
) -> list[dict[str, Any]]:
    removed = sorted(base.targets_by_id.keys() - current.targets_by_id.keys())
    if removed:
        fail(f"existing targets cannot be removed: {', '.join(removed)}")
    existing = base.targets_by_id.keys() & current.targets_by_id.keys()
    for target_id in sorted(existing):
        old = base.targets_by_id[target_id]
        new = current.targets_by_id[target_id]
        old_identity = {
            key: value
            for key, value in old.items()
            if key not in TARGET_LIFECYCLE_FIELDS
        }
        new_identity = {
            key: value
            for key, value in new.items()
            if key not in TARGET_LIFECYCLE_FIELDS
        }
        if old_identity != new_identity:
            fail(f"existing target cannot be changed: {target_id}")

    return sorted(
        (
            target
            for target in current.targets
            if target["publish"]
            and (
                target["id"] not in base.targets_by_id
                or not base.targets_by_id[target["id"]]["publish"]
            )
        ),
        key=lambda target: target["id"],
    )


def target_matrix_entry(target: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": target["id"],
        "family": target["family"],
        "arch": target["arch"],
        "runner": RUNNERS[target["arch"]],
    }


def discovery_matrix_entry(channel: dict[str, Any]) -> dict[str, Any]:
    discovery = channel["discovery"]
    return {
        "id": channel["id"],
        "runner": RUNNERS[channel["arch"]],
        "image": discovery["builder_image"],
    }
