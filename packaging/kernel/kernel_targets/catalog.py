import copy
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


KERNEL_RETENTION = 2
CHANNEL_IDENTITY_FIELDS = (
    "distro",
    "release",
    "family",
    "arch",
)
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]*$")
PATH_TOKEN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._+-]*$")
REPOSITORY_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
PACKAGE_VERSION_PATTERN = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+:~_-]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
APT_SNAPSHOT_PATTERN = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")
OPENSUSE_SNAPSHOT_PATTERN = re.compile(r"^[0-9]{8}$")
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
ARCHITECTURES = {"x86_64", "aarch64"}
ARCH_ID_TOKENS = {"x86_64": "x86-64", "aarch64": "aarch64"}
OPENSUSE_PACKAGES = (
    "kernel-default",
    "kernel-default-devel",
    "kernel-devel",
    "kernel-source",
    "kernel-syms",
)
PROVIDERS = {
    "ubuntu": {
        "family": "deb",
        "architectures": ARCHITECTURES,
        "fields": {"suite", "selector"},
    },
    "debian": {
        "family": "deb",
        "architectures": ARCHITECTURES,
        "fields": {"suite"},
    },
    "fedora": {
        "family": "rpm",
        "architectures": ARCHITECTURES,
        "fields": {"signing_fingerprint"},
    },
    "opensuse": {
        "family": "rpm",
        "architectures": {"x86_64"},
        "fields": set(),
    },
}


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class LockLocation:
    """Location of a normalized target in Catalog.document."""

    stream_id: str
    arch: str
    index: int


@dataclass(frozen=True)
class Catalog:
    """Validated lock document plus its derived build-facing view.

    Callers that update the lockfile should edit `document`, using
    `target_locations` to find a normalized target's provider-native lock.
    Channels and targets are derived build views and must never be serialized
    back into the lockfile.
    """

    document: dict[str, Any]
    channels: dict[str, dict[str, Any]]
    targets: list[dict[str, Any]]
    targets_by_id: dict[str, dict[str, Any]]
    target_locations: dict[str, LockLocation]

    def raw_lock(self, target_id: str) -> dict[str, Any]:
        location = self.target_locations[target_id]
        return copy.deepcopy(
            self.document["streams"][location.stream_id]["architectures"]
            [location.arch][location.index]
        )


def fail(message: str) -> None:
    raise ManifestError(message)


def read_json(path: Path, description: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        fail(f"{path}: cannot read {description}: {error}")
    except UnicodeError as error:
        fail(f"{path}: {description} is not valid UTF-8: {error}")
    except json.JSONDecodeError as error:
        fail(f"{path}: invalid JSON: {error}")


def validate_string(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        fail(f"{label}: must be a non-empty trimmed string")
    if any(ord(character) < 32 for character in value):
        fail(f"{label}: contains control characters")


def validate_builder_image(value: Any, label: str) -> None:
    validate_string(value, label)
    match = OCI_DIGEST_IMAGE_PATTERN.fullmatch(value)
    if match is None:
        fail(f"{label}: must be a digest-pinned OCI image name")
    port = match.group(1)
    if port is not None and int(port) > 65535:
        fail(f"{label}: registry port is out of range")


def validate_token(value: Any, label: str, pattern: re.Pattern[str]) -> None:
    validate_string(value, label)
    if not pattern.fullmatch(value):
        fail(f"{label}: unsupported value {value!r}")


def validate_artifacts(value: Any, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        fail(f"{label}: must be a non-empty object")
    artifacts = {}
    for filename, digest in value.items():
        validate_token(filename, f"{label} filename", REPOSITORY_TOKEN_PATTERN)
        if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
            fail(f"{label}.{filename}: must be a lowercase SHA-256 digest")
        artifacts[filename] = digest
    return artifacts


def validate_fedora_artifacts(
    artifacts: dict[str, str],
    kernel_nvr: str,
    arch: str,
    label: str,
) -> None:
    rpm_suffix = f".{arch}.rpm"
    expected = {
        f"kernel-{kernel_nvr}.src.rpm",
        *(
            f"{name}-{kernel_nvr}.{arch}.rpm"
            for name in ("kernel-core", "kernel-devel", "kernel-modules-core")
        ),
    }
    rust_binaries = [
        filename
        for filename in artifacts
        if filename.startswith("rust-")
        and not filename.startswith(("rust-src-", "rust-std-static-"))
        and filename.endswith(rpm_suffix)
    ]
    if len(rust_binaries) != 1:
        fail(f"{label}.artifacts: cannot select the Rust NVR")
    rust_nvr = rust_binaries[0][len("rust-") : -len(rpm_suffix)]
    expected.update(
        f"{name}-{rust_nvr}.{arch}.rpm"
        for name in ("cargo", "rust", "rust-std-static", "rustfmt")
    )
    expected.add(f"rust-src-{rust_nvr}.noarch.rpm")
    if set(artifacts) != expected:
        fail(f"{label}.artifacts: unexpected Fedora artifact set")


def validate_apt_lock(
    value: Any,
    label: str,
    provider: str,
) -> dict[str, Any]:
    expected = {"kernel", "version", "source_version", "snapshot"}
    if provider == "ubuntu":
        expected_with_optional = (expected, expected | {"source_name"})
    else:
        expected_with_optional = (expected,)
    if not isinstance(value, dict) or set(value) not in expected_with_optional:
        optional = {"source_name"} if provider == "ubuntu" else set()
        allowed = ", ".join(sorted(expected | optional))
        fail(f"{label}: expected fields {allowed}")
    for field in ("kernel", "version", "source_version"):
        validate_token(value[field], f"{label}.{field}", PACKAGE_VERSION_PATTERN)
    snapshot = value["snapshot"]
    if not isinstance(snapshot, str) or not APT_SNAPSHOT_PATTERN.fullmatch(snapshot):
        fail(f"{label}.snapshot: expected a UTC APT snapshot")
    if "source_name" in value:
        validate_token(
            value["source_name"],
            f"{label}.source_name",
            REPOSITORY_TOKEN_PATTERN,
        )
    return copy.deepcopy(value)


def validate_fedora_lock(
    value: Any,
    label: str,
    arch: str,
) -> dict[str, Any]:
    expected = {"nvr", "signing_fingerprint", "artifacts"}
    if not isinstance(value, dict) or set(value) != expected:
        fail(f"{label}: expected fields {', '.join(sorted(expected))}")
    validate_token(value["nvr"], f"{label}.nvr", REPOSITORY_TOKEN_PATTERN)
    if not value["nvr"].startswith("kernel-"):
        fail(f"{label}.nvr: expected a Fedora kernel NVR")
    fingerprint = value["signing_fingerprint"]
    if (
        not isinstance(fingerprint, str)
        or not FINGERPRINT_PATTERN.fullmatch(fingerprint)
    ):
        fail(f"{label}.signing_fingerprint: must be a lowercase SHA-1 fingerprint")
    lock = {
        "nvr": value["nvr"],
        "signing_fingerprint": fingerprint,
        "artifacts": validate_artifacts(value["artifacts"], f"{label}.artifacts"),
    }
    validate_fedora_artifacts(
        lock["artifacts"],
        lock["nvr"].removeprefix("kernel-"),
        arch,
        label,
    )
    return lock


def validate_opensuse_lock(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"snapshot", "packages"}:
        fail(f"{label}: expected fields packages, snapshot")
    snapshot = value["snapshot"]
    if (
        not isinstance(snapshot, str)
        or not OPENSUSE_SNAPSHOT_PATTERN.fullmatch(snapshot)
    ):
        fail(f"{label}.snapshot: expected an openSUSE date")
    packages = value["packages"]
    if not isinstance(packages, dict) or set(packages) != set(OPENSUSE_PACKAGES):
        fail(f"{label}.packages: expected {', '.join(OPENSUSE_PACKAGES)}")
    normalized_packages = {}
    for package in OPENSUSE_PACKAGES:
        version = packages[package]
        validate_token(
            version,
            f"{label}.packages.{package}",
            PACKAGE_VERSION_PATTERN,
        )
        normalized_packages[package] = version
    return {"snapshot": snapshot, "packages": normalized_packages}


def validate_lock(
    value: Any,
    label: str,
    provider: str,
    arch: str,
) -> dict[str, Any]:
    if provider in {"ubuntu", "debian"}:
        return validate_apt_lock(value, label, provider)
    if provider == "fedora":
        return validate_fedora_lock(value, label, arch)
    return validate_opensuse_lock(value, label)


def rpm_version_compare(left: str, right: str) -> int:
    """Compare epochless RPM versions without requiring RPM Python bindings."""

    left_index = 0
    right_index = 0
    while left_index < len(left) or right_index < len(right):
        while (
            left_index < len(left)
            and not left[left_index].isalnum()
            and left[left_index] not in "~^"
        ):
            left_index += 1
        while (
            right_index < len(right)
            and not right[right_index].isalnum()
            and right[right_index] not in "~^"
        ):
            right_index += 1

        left_tilde = left_index < len(left) and left[left_index] == "~"
        right_tilde = right_index < len(right) and right[right_index] == "~"
        if left_tilde or right_tilde:
            if left_tilde != right_tilde:
                return -1 if left_tilde else 1
            left_index += 1
            right_index += 1
            continue

        left_caret = left_index < len(left) and left[left_index] == "^"
        right_caret = right_index < len(right) and right[right_index] == "^"
        if left_caret or right_caret:
            if left_caret and right_caret:
                left_index += 1
                right_index += 1
                continue
            if left_caret:
                return 1 if right_index == len(right) else -1
            return -1 if left_index == len(left) else 1

        if left_index == len(left) or right_index == len(right):
            break

        numeric = left[left_index].isdigit()
        if numeric != right[right_index].isdigit():
            return 1 if numeric else -1
        left_end = left_index
        right_end = right_index
        if numeric:
            while left_end < len(left) and left[left_end].isdigit():
                left_end += 1
            while right_end < len(right) and right[right_end].isdigit():
                right_end += 1
            left_segment = left[left_index:left_end].lstrip("0") or "0"
            right_segment = right[right_index:right_end].lstrip("0") or "0"
            if len(left_segment) != len(right_segment):
                return 1 if len(left_segment) > len(right_segment) else -1
        else:
            while left_end < len(left) and left[left_end].isalpha():
                left_end += 1
            while right_end < len(right) and right[right_end].isalpha():
                right_end += 1
            left_segment = left[left_index:left_end]
            right_segment = right[right_index:right_end]
        if left_segment != right_segment:
            return 1 if left_segment > right_segment else -1
        left_index = left_end
        right_index = right_end

    return (left_index < len(left)) - (right_index < len(right))


def debian_version_compare(left: str, right: str) -> int:
    """Compare Debian versions without requiring python-apt."""

    def split(value: str) -> tuple[int, str, str]:
        epoch_text, separator, remainder = value.partition(":")
        if separator:
            if not epoch_text.isdigit() or ":" in remainder:
                fail(f"invalid Debian package version {value!r}")
            epoch = int(epoch_text)
        else:
            epoch = 0
            remainder = epoch_text
        upstream, separator, revision = remainder.rpartition("-")
        if not separator:
            upstream = remainder
            revision = "0"
        return epoch, upstream, revision

    def character_order(character: str) -> int:
        if character == "~":
            return -1
        if not character or character.isdigit():
            return 0
        if character.isalpha():
            return ord(character)
        return ord(character) + 256

    def compare_part(left_part: str, right_part: str) -> int:
        left_index = 0
        right_index = 0
        while left_index < len(left_part) or right_index < len(right_part):
            while (
                left_index < len(left_part)
                and not left_part[left_index].isdigit()
            ) or (
                right_index < len(right_part)
                and not right_part[right_index].isdigit()
            ):
                left_character = (
                    left_part[left_index]
                    if left_index < len(left_part)
                    else ""
                )
                right_character = (
                    right_part[right_index]
                    if right_index < len(right_part)
                    else ""
                )
                left_order = character_order(left_character)
                right_order = character_order(right_character)
                if left_order != right_order:
                    return 1 if left_order > right_order else -1
                if left_character:
                    left_index += 1
                if right_character:
                    right_index += 1

            while (
                left_index < len(left_part) and left_part[left_index] == "0"
            ):
                left_index += 1
            while (
                right_index < len(right_part) and right_part[right_index] == "0"
            ):
                right_index += 1

            first_difference = 0
            while (
                left_index < len(left_part)
                and right_index < len(right_part)
                and left_part[left_index].isdigit()
                and right_part[right_index].isdigit()
            ):
                if not first_difference:
                    first_difference = ord(left_part[left_index]) - ord(
                        right_part[right_index]
                    )
                left_index += 1
                right_index += 1
            if (
                left_index < len(left_part)
                and left_part[left_index].isdigit()
            ):
                return 1
            if (
                right_index < len(right_part)
                and right_part[right_index].isdigit()
            ):
                return -1
            if first_difference:
                return 1 if first_difference > 0 else -1
        return 0

    left_epoch, left_upstream, left_revision = split(left)
    right_epoch, right_upstream, right_revision = split(right)
    if left_epoch != right_epoch:
        return 1 if left_epoch > right_epoch else -1
    upstream_order = compare_part(left_upstream, right_upstream)
    if upstream_order:
        return upstream_order
    return compare_part(left_revision, right_revision)


def compare_lock_order(
    provider: str,
    left: dict[str, Any],
    right: dict[str, Any],
) -> int:
    if provider in {"ubuntu", "debian"}:
        return debian_version_compare(left["version"], right["version"])
    if provider == "fedora":
        return rpm_version_compare(
            left["nvr"].removeprefix("kernel-"),
            right["nvr"].removeprefix("kernel-"),
        )
    return rpm_version_compare(
        left["packages"]["kernel-default"],
        right["packages"]["kernel-default"],
    )


def validate_stream(
    stream_id: str,
    value: Any,
    label: str,
) -> dict[str, Any]:
    if not ID_PATTERN.fullmatch(stream_id):
        fail(f"{label}: unsupported stream id {stream_id!r}")
    if not isinstance(value, dict):
        fail(f"{label}: must be an object")
    provider = value.get("provider")
    if provider not in PROVIDERS:
        fail(f"{label}.provider: unsupported value {provider!r}")
    details = PROVIDERS[provider]
    expected = {
        "provider",
        "release",
        "builder",
        "architectures",
        *details["fields"],
    }
    if set(value) != expected:
        fail(f"{label}: expected fields {', '.join(sorted(expected))}")
    validate_token(value["release"], f"{label}.release", PATH_TOKEN_PATTERN)
    validate_builder_image(value["builder"], f"{label}.builder")
    if provider in {"ubuntu", "debian"}:
        validate_token(value["suite"], f"{label}.suite", PATH_TOKEN_PATTERN)
    if provider == "debian" and not value["suite"].endswith("-backports"):
        fail(f"{label}.suite: expected a Debian backports suite")
    if provider == "ubuntu":
        validate_token(
            value["selector"],
            f"{label}.selector",
            REPOSITORY_TOKEN_PATTERN,
        )
        if not value["selector"].startswith("linux-image-"):
            fail(f"{label}.selector: expected an Ubuntu linux-image selector")
    if provider == "fedora":
        fingerprint = value["signing_fingerprint"]
        if (
            not isinstance(fingerprint, str)
            or not FINGERPRINT_PATTERN.fullmatch(fingerprint)
        ):
            fail(f"{label}.signing_fingerprint: must be a lowercase SHA-1 fingerprint")
    if provider == "opensuse" and value["release"] != "tumbleweed":
        fail(f"{label}.release: only tumbleweed is supported")

    architectures = value["architectures"]
    if not isinstance(architectures, dict) or not architectures:
        fail(f"{label}.architectures: must be a non-empty object")
    unsupported = set(architectures) - details["architectures"]
    if unsupported:
        fail(
            f"{label}.architectures: unsupported architecture "
            f"{sorted(unsupported)[0]!r}"
        )

    stream = {
        key: copy.deepcopy(item)
        for key, item in value.items()
        if key != "architectures"
    }
    stream["architectures"] = {}
    for arch, raw_locks in architectures.items():
        arch_label = f"{label}.architectures.{arch}"
        if not isinstance(raw_locks, list) or not raw_locks:
            fail(f"{arch_label}: must be a non-empty array")
        if len(raw_locks) > KERNEL_RETENTION:
            fail(f"{arch_label}: exceeds retention={KERNEL_RETENTION}")
        locks = [
            validate_lock(raw_lock, f"{arch_label}[{index}]", provider, arch)
            for index, raw_lock in enumerate(raw_locks)
        ]
        for index, (older, newer) in enumerate(zip(locks, locks[1:]), start=1):
            if compare_lock_order(provider, older, newer) >= 0:
                fail(
                    f"{arch_label}[{index}]: retained locks must be ordered "
                    "oldest to newest"
                )
        stream["architectures"][arch] = locks
    return stream


def discovery_from_stream(stream: dict[str, Any], arch: str) -> dict[str, Any]:
    provider = stream["provider"]
    discovery = {"builder_image": stream["builder"]}
    if provider == "ubuntu":
        discovery.update(selector=stream["selector"], suite=stream["suite"])
    elif provider == "debian":
        package_arch = "amd64" if arch == "x86_64" else "arm64"
        discovery.update(selector=f"linux-image-{package_arch}", suite=stream["suite"])
    elif provider == "fedora":
        discovery["signing_fingerprint"] = stream["signing_fingerprint"]
    return discovery


def channel_from_stream(
    stream_id: str,
    stream: dict[str, Any],
    arch: str,
) -> dict[str, Any]:
    provider = stream["provider"]
    return {
        "id": f"{stream_id}-{ARCH_ID_TOKENS[arch]}",
        "distro": provider,
        "release": stream["release"],
        "family": PROVIDERS[provider]["family"],
        "arch": arch,
        "discovery": discovery_from_stream(stream, arch),
    }


def opensuse_kernel_release(version: str, label: str) -> str:
    package_version, separator, package_release = version.rpartition("-")
    if not separator or "." not in package_release:
        fail(f"{label}: cannot derive openSUSE kernel release from {version!r}")
    return f"{package_version}-{package_release.rsplit('.', 1)[0]}-default"


def source_from_lock(
    stream: dict[str, Any],
    arch: str,
    lock: dict[str, Any],
) -> dict[str, Any]:
    provider = stream["provider"]
    if provider == "ubuntu":
        source_name = lock.get("source_name", "linux")
        return {
            "identity": f"ubuntu:{source_name}@{lock['source_version']}",
            "snapshot": lock["snapshot"],
        }
    if provider == "debian":
        return {
            "identity": f"debian:linux@{lock['source_version']}:{stream['suite']}",
            "snapshot": lock["snapshot"],
        }
    if provider == "fedora":
        fingerprint = lock["signing_fingerprint"]
        return {
            "identity": lock["nvr"],
            "snapshot": f"koji-signed-build:{fingerprint}:{arch},noarch,src",
            "artifacts": copy.deepcopy(lock["artifacts"]),
        }
    return {
        "identity": ",".join(
            f"{package}@{lock['packages'][package]}"
            for package in OPENSUSE_PACKAGES
        ),
        "snapshot": lock["snapshot"],
    }


def lock_identity_hash(
    stream: dict[str, Any],
    arch: str,
    lock: dict[str, Any],
) -> str:
    effective_stream = {
        key: value
        for key, value in stream.items()
        if key != "architectures"
        and not (stream["provider"] == "fedora" and key == "signing_fingerprint")
    }
    canonical = json.dumps(
        {"stream": effective_stream, "arch": arch, "lock": lock},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:12]


def target_from_lock(
    channel: dict[str, Any],
    stream: dict[str, Any],
    lock: dict[str, Any],
) -> dict[str, Any]:
    provider = stream["provider"]
    arch = channel["arch"]
    if provider in {"ubuntu", "debian"}:
        kernel_release = lock["kernel"]
        package_version = lock["version"]
    elif provider == "fedora":
        kernel_release = f"{lock['nvr'].removeprefix('kernel-')}.{arch}"
        package_version = kernel_release
    else:
        package_version = lock["packages"]["kernel-default"]
        kernel_release = opensuse_kernel_release(
            package_version,
            f"target in {channel['id']}",
        )
    release_token = re.sub(r"[^a-z0-9.]+", "-", kernel_release.lower()).strip(".-")
    target = {
        "id": (
            f"{channel['id']}-{release_token}-"
            f"{lock_identity_hash(stream, arch, lock)}"
        ),
        "channel_id": channel["id"],
        "kernel_release": kernel_release,
        "kernel_package_version": package_version,
        "builder_image": stream["builder"],
        "source": source_from_lock(stream, arch, lock),
    }
    if provider in {"ubuntu", "debian"}:
        target["suite"] = stream["suite"]
    for field in CHANNEL_IDENTITY_FIELDS:
        target[field] = channel[field]
    return target


def validate_catalog(
    value: Any,
    label: str,
) -> Catalog:
    if not isinstance(value, dict):
        fail(f"{label}: top level must be an object")
    if set(value) != {"schema_version", "streams"}:
        fail(f"{label}: expected fields schema_version, streams")
    if type(value["schema_version"]) is not int or value["schema_version"] != 3:
        fail(f"{label}: schema_version must be 3")
    raw_streams = value["streams"]
    if not isinstance(raw_streams, dict) or not raw_streams:
        fail(f"{label}: streams must be a non-empty object")

    channels = {}
    targets = []
    targets_by_id = {}
    target_locations = {}
    for stream_id, raw_stream in raw_streams.items():
        stream = validate_stream(
            stream_id,
            raw_stream,
            f"{label}.streams.{stream_id}",
        )
        for arch, locks in stream["architectures"].items():
            channel = channel_from_stream(stream_id, stream, arch)
            channel_id = channel["id"]
            if channel_id in channels:
                fail(f"{label}: duplicate derived channel id {channel_id!r}")
            channels[channel_id] = channel
            kernel_releases = set()
            for index, lock in enumerate(locks):
                target = target_from_lock(channel, stream, lock)
                target_id = target["id"]
                kernel_release = target["kernel_release"]
                if kernel_release in kernel_releases:
                    fail(
                        f"{label}: channel {channel_id!r} has multiple locks "
                        f"for kernel release {kernel_release!r}"
                    )
                if target_id in targets_by_id:
                    fail(f"{label}: duplicate derived target id {target_id!r}")
                kernel_releases.add(kernel_release)
                targets.append(target)
                targets_by_id[target_id] = target
                target_locations[target_id] = LockLocation(stream_id, arch, index)

    return Catalog(
        document=copy.deepcopy(value),
        channels=channels,
        targets=targets,
        targets_by_id=targets_by_id,
        target_locations=target_locations,
    )


def load_catalog(path: Path) -> Catalog:
    return validate_catalog(
        read_json(path, "kernel lock"),
        str(path),
    )
