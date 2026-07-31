#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable


TAG_REPOSITORIES = (
    "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
    "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git",
)
VERSION_PATTERN = re.compile(
    r"^v?(?P<major>[1-9][0-9]*)\.(?P<minor>[0-9]+)"
    r"(?:\.(?P<patch>[0-9]+))?(?:-rc(?P<rc>[1-9][0-9]*))?$"
)


class ResolutionError(ValueError):
    pass


@dataclass(frozen=True)
class KernelVersion:
    text: str
    major: int
    minor: int
    patch: int
    rc: int | None

    @classmethod
    def parse(cls, value: str) -> "KernelVersion":
        match = VERSION_PATTERN.fullmatch(value)
        if match is None:
            raise ResolutionError(f"unsupported Linux version: {value!r}")
        patch = match.group("patch")
        rc = match.group("rc")
        text = value.removeprefix("v")
        return cls(
            text=text,
            major=int(match.group("major")),
            minor=int(match.group("minor")),
            patch=int(patch) if patch is not None else 0,
            rc=int(rc) if rc is not None else None,
        )

    @property
    def final_key(self) -> tuple[int, int, int]:
        return self.major, self.minor, self.patch

    @property
    def series_key(self) -> tuple[int, int]:
        return self.major, self.minor


def parse_tag_refs(output: str) -> set[str]:
    tags: set[str] = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) != 2 or not fields[1].startswith("refs/tags/"):
            raise ResolutionError("git ls-remote returned an invalid tag record")
        tag = fields[1].removeprefix("refs/tags/")
        if VERSION_PATTERN.fullmatch(tag):
            tags.add(tag)
    return tags


def resolve_matrix(tags: Iterable[str]) -> dict[str, list[dict[str, object]]]:
    versions = {KernelVersion.parse(tag) for tag in tags}
    finals = [version for version in versions if version.rc is None]
    if not finals:
        raise ResolutionError("no final release tag was found")

    latest_final = max(finals, key=lambda version: version.final_key)
    release_candidates = [
        version
        for version in versions
        if version.rc is not None
        and version.series_key > latest_final.series_key
    ]
    selected = [(latest_final, "latest-final")]
    if release_candidates:
        latest_rc = max(
            release_candidates,
            key=lambda version: (*version.final_key, version.rc or 0),
        )
        selected.append((latest_rc, "latest-rc"))

    matrix: list[dict[str, object]] = []
    for version, role in selected:
        for architecture in ("x86_64", "aarch64"):
            matrix.append(
                {
                    "name": (
                        f"native, Linux {version.text}, "
                        f"{architecture} ({role.replace('-', ' ')})"
                    ),
                    "client": "native",
                    "guest": 1,
                    "arch": architecture,
                    "kernel": version.text,
                }
            )
    return {"include": matrix}


def fetch_tags() -> set[str]:
    tags: set[str] = set()
    for repository in TAG_REPOSITORIES:
        result = subprocess.run(
            ["git", "ls-remote", "--refs", "--tags", repository],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
            timeout=120,
        )
        tags.update(parse_tag_refs(result.stdout))
    return tags


def main() -> int:
    try:
        matrix = resolve_matrix(fetch_tags())
    except (
        OSError,
        ResolutionError,
        subprocess.SubprocessError,
    ) as error:
        print(f"upstream_kernels.py: {error}", file=sys.stderr)
        return 1

    json.dump(matrix, sys.stdout, separators=(",", ":"), sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
