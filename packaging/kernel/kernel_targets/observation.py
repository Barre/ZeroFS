from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class KernelObservation:
    kernel_release: str
    kernel_package_name: str
    kernel_package_version: str
    kernel_selector_version: str
    source_kind: str
    source_identity: str
    source_snapshot: str


@dataclass(frozen=True)
class ObservationComparison:
    package_changed: bool
    signed_source_changed: bool

    @property
    def update_available(self) -> bool:
        return self.package_changed or self.signed_source_changed


def candidate_observation(candidate: dict[str, Any]) -> KernelObservation:
    source = candidate["source"]
    return KernelObservation(
        kernel_release=candidate["kernel_release"],
        kernel_package_name=candidate["kernel_package_name"],
        kernel_package_version=candidate["kernel_package_version"],
        kernel_selector_version=candidate["kernel_selector_version"],
        source_kind=source["kind"],
        source_identity=source["identity"],
        source_snapshot=source["snapshot"],
    )


def compare_observation(
    current: dict[str, Any],
    observation: KernelObservation,
) -> ObservationComparison:
    source = current["source"]
    package_changed = (
        observation.kernel_release,
        observation.kernel_package_name,
        observation.kernel_package_version,
        observation.kernel_selector_version,
        observation.source_kind,
        observation.source_identity,
    ) != (
        current["kernel_release"],
        current["kernel_package_name"],
        current["kernel_package_version"],
        current["kernel_selector_version"],
        source["kind"],
        source["identity"],
    )
    signed_source_changed = (
        observation.source_kind == "koji"
        and observation.source_snapshot != source["snapshot"]
    )
    return ObservationComparison(
        package_changed=package_changed,
        signed_source_changed=signed_source_changed,
    )


def availability_entry(
    channel: dict[str, Any],
    current: dict[str, Any],
    observation: KernelObservation,
) -> dict[str, Any]:
    comparison = compare_observation(current, observation)
    return {
        "channel_id": channel["id"],
        "current_target_id": current["id"],
        "current_kernel_release": current["kernel_release"],
        "current_kernel_package_version": current["kernel_package_version"],
        "current_kernel_selector_version": current["kernel_selector_version"],
        "current_source_identity": current["source"]["identity"],
        "current_source_snapshot": current["source"]["snapshot"],
        "candidate_kernel_release": observation.kernel_release,
        "candidate_kernel_package_version": observation.kernel_package_version,
        "candidate_kernel_selector_version": observation.kernel_selector_version,
        "candidate_source_identity": observation.source_identity,
        "candidate_source_snapshot": observation.source_snapshot,
        "update_available": comparison.update_available,
    }
