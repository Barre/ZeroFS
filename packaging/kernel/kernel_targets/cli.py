import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from .catalog import ManifestError, fail, load_catalog
from .discovery import discover_candidate, parse_as_of
from .updates import load_candidate, reconcile_candidates


RUNNERS = {
    "x86_64": "ubuntu-26.04",
    "aarch64": "ubuntu-26.04-arm",
}


def print_matrix(entries: list[dict[str, Any]]) -> None:
    print(json.dumps({"include": entries}, separators=(",", ":")))


def print_value(value: Any) -> None:
    if isinstance(value, (dict, list)):
        print(json.dumps(value, separators=(",", ":"), sort_keys=True))
    else:
        print(value)


def serialized_manifest(document: dict[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2) + "\n"


def write_manifest(path: Path, document: dict[str, Any]) -> None:
    text = serialized_manifest(document)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o644
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_name = temporary.name
        os.chmod(temporary_name, mode)
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("matrix")
    subparsers.add_parser("discovery-matrix")

    builder_image = subparsers.add_parser("builder-image")
    builder_image.add_argument("provider")
    builder_image.add_argument("release")

    field = subparsers.add_parser("field")
    field.add_argument("target_id")
    field.add_argument("field")

    discover = subparsers.add_parser("discover")
    discover.add_argument("channel_id")
    discover.add_argument("--as-of", required=True)
    discover.add_argument("--output", type=Path)

    reconcile = subparsers.add_parser("reconcile")
    reconcile.add_argument("--pending-base", type=Path)
    reconcile.add_argument("--pending-head", type=Path)
    reconcile.add_argument("candidates", type=Path, nargs="*")

    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    catalog = load_catalog(arguments.manifest)

    if arguments.command == "matrix":
        entries = [
            {
                "id": target["id"],
                "arch": target["arch"],
                "runner": RUNNERS[target["arch"]],
            }
            for target in catalog.targets
        ]
        print_matrix(entries)
        return 0

    if arguments.command == "discovery-matrix":
        entries = [
            {
                "id": channel["id"],
                "runner": RUNNERS[channel["arch"]],
                "image": channel["discovery"]["builder_image"],
            }
            for channel in catalog.channels.values()
        ]
        print_matrix(entries)
        return 0

    if arguments.command == "builder-image":
        images = {
            stream["builder"]
            for stream in catalog.document["streams"].values()
            if stream["provider"] == arguments.provider
            and stream["release"] == arguments.release
        }
        description = (
            f"provider {arguments.provider!r}, release {arguments.release!r}"
        )
        if not images:
            fail(f"no builder image matches {description}")
        if len(images) != 1:
            fail(f"multiple builder images match {description}")
        print_value(next(iter(images)))
        return 0

    if arguments.command == "field":
        target = catalog.targets_by_id.get(arguments.target_id)
        if target is None:
            fail(f"unknown target id: {arguments.target_id}")
        if arguments.field not in target:
            fail(f"{arguments.target_id}: unknown field: {arguments.field}")
        print_value(target[arguments.field])
        return 0

    if arguments.command == "discover":
        candidate = discover_candidate(
            catalog,
            arguments.channel_id,
            parse_as_of(arguments.as_of),
        )
        if arguments.output is not None:
            write_manifest(arguments.output, candidate)
        else:
            sys.stdout.write(serialized_manifest(candidate))
        return 0

    if arguments.command == "reconcile":
        if (arguments.pending_base is None) != (
            arguments.pending_head is None
        ):
            fail("--pending-base and --pending-head must be used together")
        candidates = [
            load_candidate(path, catalog)
            for path in arguments.candidates
        ]
        pending_base = None
        pending_head = None
        if arguments.pending_base is not None:
            pending_base = load_catalog(arguments.pending_base)
            pending_head = load_catalog(arguments.pending_head)
        document = reconcile_candidates(
            catalog,
            candidates,
            pending_base=pending_base,
            pending_head=pending_head,
        )
        write_manifest(arguments.manifest, document)
        return 0

    fail(f"unsupported command: {arguments.command}")


def run() -> None:
    try:
        raise SystemExit(main())
    except ManifestError as error:
        print(f"kernel-targets.py: {error}", file=sys.stderr)
        raise SystemExit(1)
