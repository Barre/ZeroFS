import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TARGET_FIELDS = {
    "id",
    "enabled",
    "ci",
    "publish",
    "channel_id",
    "package_revision",
    "kernel_release",
    "kernel_package_name",
    "kernel_package_version",
    "kernel_selector_version",
    "builder_image",
    "source",
}
TARGET_LIFECYCLE_FIELDS = {"enabled", "ci", "publish"}
TARGET_STRING_FIELDS = TARGET_FIELDS - TARGET_LIFECYCLE_FIELDS - {"source"}
UNSUPPORTED_FIELDS = {
    "id",
    "distro",
    "release",
    "arch",
    "flavor",
    "kernel_release",
    "reason",
}
CHANNEL_FIELDS = {
    "id",
    "distro",
    "release",
    "family",
    "arch",
    "flavor",
}
CHANNEL_IDENTITY_FIELDS = (
    "distro",
    "release",
    "family",
    "arch",
    "flavor",
)
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]*$")
PACKAGE_REVISION_PATTERN = re.compile(r"^[1-9][0-9]*$")
PATH_TOKEN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._+-]*$")
REPOSITORY_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
PACKAGE_VERSION_PATTERN = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+:~_-]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
OCI_REPOSITORY_PATTERN = (
    r"(?:(?:[a-z0-9]+(?:[.-][a-z0-9]+)*)(?::([1-9][0-9]{0,4}))?/)?"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
)
OCI_TAG_PATTERN = r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
OCI_DIGEST_IMAGE_PATTERN = re.compile(
    rf"{OCI_REPOSITORY_PATTERN}(?::{OCI_TAG_PATTERN})?"
    r"@sha256:[0-9a-f]{64}$"
)
OCI_TAGGED_IMAGE_PATTERN = re.compile(
    rf"{OCI_REPOSITORY_PATTERN}:{OCI_TAG_PATTERN}$"
)
FAMILIES = {"deb", "rpm"}
ARCHITECTURES = {"x86_64", "aarch64"}
DISTROS = {"debian", "fedora", "opensuse", "rocky", "ubuntu"}
SOURCE_KINDS = {"apt-snapshot", "koji", "opensuse-history"}
DISCOVERY_PROVIDERS = {
    "ubuntu-snapshot": {
        "source_kind": "apt-snapshot",
        "distro": "ubuntu",
        "family": "deb",
        "architectures": ARCHITECTURES,
    },
    "debian-snapshot": {
        "source_kind": "apt-snapshot",
        "distro": "debian",
        "family": "deb",
        "architectures": ARCHITECTURES,
    },
    "fedora-koji": {
        "source_kind": "koji",
        "distro": "fedora",
        "family": "rpm",
        "architectures": ARCHITECTURES,
    },
    "opensuse-history": {
        "source_kind": "opensuse-history",
        "distro": "opensuse",
        "family": "rpm",
        "architectures": {"x86_64"},
    },
}
OPENSUSE_PACKAGES = (
    "kernel-default",
    "kernel-default-devel",
    "kernel-devel",
    "kernel-source",
    "kernel-syms",
)


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class Catalog:
    document: dict[str, Any]
    channels: dict[str, dict[str, Any]]
    targets: list[dict[str, Any]]
    targets_by_id: dict[str, dict[str, Any]]


def fail(message: str) -> None:
    raise ManifestError(message)


def read_json(path: Path, description: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        fail(f"{path}: cannot read {description}: {error}")
    except json.JSONDecodeError as error:
        fail(f"{path}: invalid JSON: {error}")


def validate_string(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        fail(f"{label}: must be a non-empty trimmed string")
    if any(ord(character) < 32 for character in value):
        fail(f"{label}: contains control characters")


def validate_builder_image(
    value: Any,
    label: str,
    *,
    allow_tagged: bool = False,
) -> None:
    validate_string(value, label)
    match = OCI_DIGEST_IMAGE_PATTERN.fullmatch(value)
    if match is None and allow_tagged:
        match = OCI_TAGGED_IMAGE_PATTERN.fullmatch(value)
    if match is None:
        requirement = "a digest-pinned OCI image name"
        if allow_tagged:
            requirement = "a tagged or digest-pinned OCI image name"
        fail(f"{label}: must be {requirement}")
    port = match.group(1)
    if port is not None and int(port) > 65535:
        fail(f"{label}: registry port is out of range")


def validate_discovery(
    value: Any,
    label: str,
    distro: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label}: must be an object")
    kind = value.get("kind")
    validate_string(kind, f"{label}.kind")
    common_fields = {"kind", "builder_image", "selector"}
    if kind in {"ubuntu-snapshot", "debian-snapshot"}:
        expected_fields = common_fields | {"suite"}
    elif kind == "fedora-koji":
        expected_fields = common_fields | {"signing_fingerprint"}
    elif kind == "opensuse-history":
        expected_fields = common_fields | {"packages"}
    else:
        fail(f"{label}.kind: unsupported value {kind!r}")
    provider = DISCOVERY_PROVIDERS[kind]
    if set(value) != expected_fields:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(expected_fields))}"
        )
    if distro != provider["distro"]:
        fail(f"{label}.kind: {kind} does not support {distro}")

    validate_builder_image(
        value["builder_image"],
        f"{label}.builder_image",
        allow_tagged=kind == "opensuse-history",
    )
    validate_string(value["selector"], f"{label}.selector")
    if not REPOSITORY_TOKEN_PATTERN.fullmatch(value["selector"]):
        fail(f"{label}.selector: unsupported package name")

    if kind in {"ubuntu-snapshot", "debian-snapshot"}:
        validate_string(value["suite"], f"{label}.suite")
        if not PATH_TOKEN_PATTERN.fullmatch(value["suite"]):
            fail(f"{label}.suite: unsupported suite")
    elif kind == "fedora-koji":
        fingerprint = value["signing_fingerprint"]
        if (
            not isinstance(fingerprint, str)
            or not FINGERPRINT_PATTERN.fullmatch(fingerprint)
        ):
            fail(
                f"{label}.signing_fingerprint: "
                "must be a lowercase SHA-1 fingerprint"
            )
    else:
        packages = value["packages"]
        if not isinstance(packages, list) or tuple(packages) != OPENSUSE_PACKAGES:
            fail(
                f"{label}.packages: expected "
                f"{', '.join(OPENSUSE_PACKAGES)}"
            )

    return copy.deepcopy(value)


def validate_channel(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label}: channel must be an object")
    missing = sorted(CHANNEL_FIELDS - value.keys())
    if missing:
        fail(f"{label}: missing fields: {', '.join(missing)}")
    for field in CHANNEL_FIELDS:
        validate_string(value[field], f"{label}.{field}")

    channel_id = value["id"]
    if not ID_PATTERN.fullmatch(channel_id):
        fail(f"{label}.id: unsupported channel id: {channel_id!r}")
    family = value["family"]
    if family not in FAMILIES:
        fail(f"{label}.family: expected one of {sorted(FAMILIES)}")
    if value["arch"] not in ARCHITECTURES:
        fail(f"{label}.arch: expected one of {sorted(ARCHITECTURES)}")
    if value["distro"] not in DISTROS:
        fail(f"{label}.distro: expected one of {sorted(DISTROS)}")
    for field in ("release", "flavor"):
        if (
            value[field] in {".", ".."}
            or not PATH_TOKEN_PATTERN.fullmatch(value[field])
        ):
            fail(f"{label}.{field}: unsupported channel path token")

    family_field = "apt" if family == "deb" else "rpm"
    expected_fields = CHANNEL_FIELDS | {family_field, "discovery"}
    if set(value) != expected_fields:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(expected_fields))}"
        )

    repository = value[family_field]
    if not isinstance(repository, dict):
        fail(f"{label}.{family_field}: must be an object")
    if family == "deb":
        repository_fields = {"codename", "suite", "component"}
    else:
        repository_fields = {"repo_id"}
    if set(repository) != repository_fields:
        fail(
            f"{label}.{family_field}: expected fields "
            f"{', '.join(sorted(repository_fields))}"
        )
    for field, item in repository.items():
        validate_string(item, f"{label}.{family_field}.{field}")
        if item in {".", ".."} or not REPOSITORY_TOKEN_PATTERN.fullmatch(item):
            fail(
                f"{label}.{family_field}.{field}: unsupported repository token "
                f"{item!r}"
            )

    channel = copy.deepcopy(value)
    channel["discovery"] = validate_discovery(
        value["discovery"],
        f"{label}.discovery",
        value["distro"],
    )
    discovery_kind = channel["discovery"]["kind"]
    provider = DISCOVERY_PROVIDERS[discovery_kind]
    if family != provider["family"]:
        fail(
            f"{label}.family: {discovery_kind} requires "
            f"{provider['family']}"
        )
    if value["arch"] not in provider["architectures"]:
        fail(
            f"{label}.arch: {discovery_kind} does not support "
            f"{value['arch']}"
        )
    repository_kind = "apt" if family == "deb" else "rpm"
    channel["prefix"] = "/".join(
        (
            "kernel",
            repository_kind,
            value["distro"],
            value["release"],
            value["flavor"],
            value["arch"],
        )
    )
    return channel


def validate_source(
    value: Any,
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label}: must be an object")
    kind = value.get("kind")
    validate_string(kind, f"{label}.kind")
    expected_fields = {"kind", "identity", "snapshot"}
    if kind == "koji":
        expected_fields.add("artifacts")
    if set(value) != expected_fields:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(expected_fields))}"
        )
    if kind not in SOURCE_KINDS:
        fail(f"{label}.kind: unsupported value {kind!r}")
    validate_string(value["identity"], f"{label}.identity")
    snapshot = value["snapshot"]
    if snapshot is not None:
        validate_string(snapshot, f"{label}.snapshot")
    else:
        fail(f"{label}.snapshot: supported target must be immutable")

    source = {
        "kind": kind,
        "identity": value["identity"],
        "snapshot": snapshot,
    }
    if kind == "koji":
        artifacts = value["artifacts"]
        if not isinstance(artifacts, dict) or not artifacts:
            fail(f"{label}.artifacts: must be a non-empty object")
        normalized_artifacts = {}
        for filename, digest in sorted(artifacts.items()):
            if (
                not isinstance(filename, str)
                or filename in {".", ".."}
                or not REPOSITORY_TOKEN_PATTERN.fullmatch(filename)
            ):
                fail(f"{label}.artifacts: unsupported filename {filename!r}")
            if (
                not isinstance(digest, str)
                or not SHA256_PATTERN.fullmatch(digest)
            ):
                fail(
                    f"{label}.artifacts.{filename}: "
                    "must be a lowercase SHA-256 digest"
                )
            normalized_artifacts[filename] = digest
        source["artifacts"] = normalized_artifacts
    return source


def validate_target(
    value: Any,
    label: str,
    channels: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label}: target must be an object")
    if set(value) != TARGET_FIELDS:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(TARGET_FIELDS))}"
        )
    for field in TARGET_STRING_FIELDS:
        validate_string(value[field], f"{label}.{field}")
    for field in TARGET_LIFECYCLE_FIELDS:
        if not isinstance(value[field], bool):
            fail(f"{label}.{field}: must be a boolean")

    if not ID_PATTERN.fullmatch(value["id"]):
        fail(f"{label}.id: unsupported target id: {value['id']!r}")
    if value["ci"] and not value["enabled"]:
        fail(f"{label}: ci=true requires enabled=true")
    if value["publish"] and not (value["enabled"] and value["ci"]):
        fail(f"{label}: publish=true requires enabled=true and ci=true")
    if not PACKAGE_REVISION_PATTERN.fullmatch(value["package_revision"]):
        fail(f"{label}.package_revision: unsupported package revision")
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
    discovery_kind = channel["discovery"]["kind"]
    validate_builder_image(
        value["builder_image"],
        f"{label}.builder_image",
        allow_tagged=discovery_kind == "opensuse-history",
    )
    source = validate_source(value["source"], f"{label}.source")
    expected_source_kind = DISCOVERY_PROVIDERS[
        channel["discovery"]["kind"]
    ]["source_kind"]
    if source["kind"] != expected_source_kind:
        fail(
            f"{label}.source.kind: expected {expected_source_kind} "
            f"for channel {channel_id}"
        )
    if discovery_kind == "fedora-koji":
        expected_selector_version = source["identity"].removeprefix("kernel-")
        if (
            not source["identity"].startswith("kernel-")
            or value["kernel_selector_version"] != expected_selector_version
        ):
            fail(
                f"{label}.kernel_selector_version: expected Fedora "
                f"kernel NVR {expected_selector_version!r}"
            )
    elif discovery_kind == "opensuse-history":
        if value["kernel_selector_version"] != value["kernel_package_version"]:
            fail(
                f"{label}.kernel_selector_version: must match "
                "kernel-default version"
            )

    target = copy.deepcopy(value)
    target["source"] = source
    for field in CHANNEL_IDENTITY_FIELDS:
        target[field] = channel[field]
    if target["family"] == "deb":
        target["kernel_dependency"] = (
            f"{target['kernel_package_name']} "
            f"(= {target['kernel_package_version']})"
        )
        target["kernel_upgrade_conflict"] = (
            f"{channel['discovery']['selector']} "
            f"(>> {target['kernel_selector_version']})"
        )
    else:
        target["kernel_dependency"] = (
            f"{target['kernel_package_name']} = "
            f"{target['kernel_package_version']}"
        )
        if channel["distro"] == "fedora":
            capability = "kernel-core-uname-r"
        else:
            capability = channel["discovery"]["selector"]
        target["kernel_upgrade_conflict"] = (
            f"{capability} > {target['kernel_package_version']}"
        )
    return target


def validate_unsupported_target(value: Any, label: str) -> None:
    if not isinstance(value, dict) or set(value) != UNSUPPORTED_FIELDS:
        fail(
            f"{label}: expected fields "
            f"{', '.join(sorted(UNSUPPORTED_FIELDS))}"
        )
    for field in UNSUPPORTED_FIELDS:
        validate_string(value[field], f"{label}.{field}")
    if not ID_PATTERN.fullmatch(value["id"]):
        fail(f"{label}.id: unsupported target id: {value['id']!r}")
    if value["distro"] not in DISTROS:
        fail(f"{label}.distro: expected one of {sorted(DISTROS)}")
    if value["arch"] not in ARCHITECTURES:
        fail(f"{label}.arch: expected one of {sorted(ARCHITECTURES)}")
    for field in ("release", "flavor"):
        if (
            value[field] in {".", ".."}
            or not PATH_TOKEN_PATTERN.fullmatch(value[field])
        ):
            fail(f"{label}.{field}: unsupported path token")


def validate_catalog(
    value: Any,
    label: str,
    *,
    allow_builder_image_drift: bool = False,
) -> Catalog:
    if not isinstance(value, dict):
        fail(f"{label}: top level must be an object")
    expected_top_level = {
        "schema_version",
        "channels",
        "targets",
        "unsupported_targets",
    }
    if set(value) != expected_top_level:
        fail(f"{label}: unexpected top-level fields")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        fail(f"{label}: schema_version must be 1")
    if not isinstance(value["channels"], list):
        fail(f"{label}: channels must be an array")
    if not isinstance(value["targets"], list):
        fail(f"{label}: targets must be an array")
    if not isinstance(value["unsupported_targets"], list):
        fail(f"{label}: unsupported_targets must be an array")

    channels: dict[str, dict[str, Any]] = {}
    prefixes: dict[str, str] = {}
    rpm_repo_ids: dict[str, str] = {}
    for index, raw_channel in enumerate(value["channels"]):
        channel = validate_channel(raw_channel, f"channels[{index}]")
        channel_id = channel["id"]
        if channel_id in channels:
            fail(f"{label}: duplicate channel id: {channel_id}")
        prefix = channel["prefix"]
        if prefix in prefixes:
            fail(
                f"{label}: channel prefix {prefix!r} is shared by "
                f"{prefixes[prefix]!r} and {channel_id!r}"
            )
        channels[channel_id] = channel
        prefixes[prefix] = channel_id
        if channel["family"] == "rpm":
            repo_id = channel["rpm"]["repo_id"]
            if repo_id in rpm_repo_ids:
                fail(
                    f"{label}: RPM repo id {repo_id!r} is shared by "
                    f"{rpm_repo_ids[repo_id]!r} and {channel_id!r}"
                )
            rpm_repo_ids[repo_id] = channel_id

    targets = []
    targets_by_id = {}
    revisions: dict[str, tuple[int, str]] = {}
    publishing: dict[str, str] = {}
    seen_ids: set[str] = set()
    for index, raw_target in enumerate(value["targets"]):
        target = validate_target(
            raw_target,
            f"targets[{index}]",
            channels,
        )
        target_id = target["id"]
        if target_id in seen_ids:
            fail(f"{label}: duplicate target id: {target_id}")
        seen_ids.add(target_id)
        channel_id = target["channel_id"]
        revision = int(target["package_revision"])
        previous = revisions.get(channel_id)
        if previous is not None and revision <= previous[0]:
            fail(
                f"{label}: target {target_id!r} package revision {revision} "
                f"must be greater than {previous[0]} from {previous[1]!r} "
                f"in channel {channel_id!r}"
            )
        revisions[channel_id] = (revision, target_id)
        if target["publish"]:
            if channel_id in publishing:
                fail(
                    f"{label}: channel {channel_id!r} has multiple publish "
                    f"targets: {publishing[channel_id]!r} and {target_id!r}"
                )
            publishing[channel_id] = target_id
        targets.append(target)
        targets_by_id[target_id] = target

    for index, target in enumerate(value["unsupported_targets"]):
        validate_unsupported_target(target, f"unsupported_targets[{index}]")
        target_id = target["id"]
        if target_id in seen_ids:
            fail(f"{label}: duplicate target id: {target_id}")
        seen_ids.add(target_id)

    unreferenced = sorted(channels.keys() - revisions.keys())
    if unreferenced:
        fail(f"{label}: channels without targets: {', '.join(unreferenced)}")
    for channel_id, target_id in publishing.items():
        if revisions[channel_id][1] != target_id:
            fail(
                f"{label}: publish target {target_id!r} is not the newest "
                f"target in channel {channel_id!r}"
            )
    if not allow_builder_image_drift:
        for channel_id, (_, target_id) in revisions.items():
            target_image = targets_by_id[target_id]["builder_image"]
            channel_image = channels[channel_id]["discovery"]["builder_image"]
            if target_image != channel_image:
                fail(
                    f"{label}: newest target {target_id!r} uses builder image "
                    f"{target_image!r}, but channel {channel_id!r} configures "
                    f"{channel_image!r}; apply a candidate to create a "
                    "replacement target"
                )

    return Catalog(
        document=copy.deepcopy(value),
        channels=channels,
        targets=targets,
        targets_by_id=targets_by_id,
    )


def load_catalog(
    path: Path,
    *,
    allow_builder_image_drift: bool = False,
) -> Catalog:
    return validate_catalog(
        read_json(path, "manifest"),
        str(path),
        allow_builder_image_drift=allow_builder_image_drift,
    )
