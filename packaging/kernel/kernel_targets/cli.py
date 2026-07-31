import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from .catalog import ManifestError, fail, load_catalog
from .discovery import check_channel, discover_candidate, parse_as_of
from .updates import (
    apply_candidates,
    discovery_matrix_entry,
    load_candidate,
    newly_published_targets,
    target_matrix_entry,
)


def print_matrix(entries: list[dict[str, Any]]) -> None:
    print(json.dumps({"include": entries}, separators=(",", ":")))


def print_value(value: Any) -> None:
    if isinstance(value, (dict, list)):
        print(json.dumps(value, separators=(",", ":"), sort_keys=True))
    elif value is None:
        print("null")
    elif isinstance(value, bool):
        print("true" if value else "false")
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

    matrix = subparsers.add_parser("matrix")
    matrix.add_argument(
        "--scope",
        choices=("ci", "publish", "discover"),
        required=True,
    )

    field = subparsers.add_parser("field")
    field.add_argument("target_id")
    field.add_argument("field")

    discover = subparsers.add_parser("discover")
    discover.add_argument("channel_id")
    discover.add_argument("--as-of", required=True)
    discover.add_argument("--output", type=Path)

    check = subparsers.add_parser("check")
    check.add_argument("channel_id")
    check.add_argument("--as-of", required=True)

    apply = subparsers.add_parser("apply")
    apply.add_argument("candidates", type=Path, nargs="+")

    published = subparsers.add_parser("published")
    published.add_argument("--base", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    catalog = load_catalog(arguments.manifest)

    if arguments.command == "matrix":
        if arguments.scope == "discover":
            entries = [
                discovery_matrix_entry(channel)
                for channel in catalog.channels.values()
            ]
        elif arguments.scope == "ci":
            entries = [
                target_matrix_entry(target)
                for target in catalog.targets
                if target["enabled"] and target["ci"]
            ]
        else:
            entries = [
                target_matrix_entry(target)
                for target in catalog.targets
                if target["publish"]
            ]
        print_matrix(entries)
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

    if arguments.command == "check":
        print_value(
            check_channel(
                catalog,
                arguments.channel_id,
                parse_as_of(arguments.as_of),
            )
        )
        return 0

    if arguments.command == "apply":
        candidates = [
            load_candidate(path, catalog.channels)
            for path in arguments.candidates
        ]
        document = apply_candidates(catalog, candidates)
        write_manifest(arguments.manifest, document)
        return 0

    if arguments.command != "published":
        fail(f"unsupported command: {arguments.command}")
    base = load_catalog(arguments.base)
    entries = [
        target_matrix_entry(target)
        for target in newly_published_targets(base, catalog)
    ]
    print_matrix(entries)
    return 0


def run() -> None:
    try:
        raise SystemExit(main())
    except ManifestError as error:
        print(f"kernel-targets.py: {error}", file=sys.stderr)
        raise SystemExit(1)
