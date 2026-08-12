#!/usr/bin/env python3

"""Validate, sign, and stage exact-kernel ZeroFS modules for publication."""

import argparse
import json
import os
import re
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from kernel_targets.catalog import ManifestError, load_catalog


VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+-[1-9][0-9]*$")
IDENTITY_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+~:-]*$")
ARCHITECTURES = {
    "x86_64": {"native_deb": "amd64", "native_rpm": "x86_64", "machine": 62},
    "aarch64": {"native_deb": "arm64", "native_rpm": "aarch64", "machine": 183},
}
SIGNATURE_MAGIC = b"~Module signature appended~\n"
SIGNATURE_TRAILER = struct.Struct(">BBBBBBBBI")
IDENTITY_FIELD = "zerofs_identity"
OPENSSL = "openssl"
MODINFO = "modinfo"
XZ = "xz"


class StageError(ValueError):
    pass


def fail(message: str) -> None:
    raise StageError(message)


def regular_path(path: Path, label: str, *, directory: bool = False) -> Path:
    try:
        metadata = path.lstat()
    except OSError as error:
        fail(f"{label}: cannot inspect {path}: {error}")
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    kind = "directory" if directory else "file"
    if not expected(metadata.st_mode):
        fail(f"{label}: must be a regular {kind}, not a symlink: {path}")
    return path.resolve(strict=True)


def run(arguments: list[str | Path], label: str) -> bytes:
    try:
        result = subprocess.run(
            [str(argument) for argument in arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        fail(f"{label}: {error}")
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        fail(f"{label}: {detail or f'exited with status {result.returncode}'}")
    return result.stdout


def modinfo(modinfo_tool: str | Path, module: Path, field: str) -> str:
    value = run(
        [modinfo_tool, "-F", field, module],
        f"cannot read module {field}",
    ).decode("utf-8", errors="strict").strip()
    if "\n" in value or "\r" in value:
        fail(f"module {field} contains multiple values")
    return value


def module_metadata_fields(
    objcopy_tool: str | Path, module: Path, temporary: Path
) -> list[bytes]:
    section = temporary / "modinfo.bin"
    section.unlink(missing_ok=True)
    run(
        [objcopy_tool, "--dump-section", f".modinfo={section}", module],
        "cannot extract module metadata",
    )
    data = section.read_bytes()
    if not data or not data.endswith(b"\0"):
        fail(f"module metadata has invalid framing: {module}")
    fields = data[:-1].split(b"\0")
    if not fields or any(not field for field in fields):
        fail(f"module metadata contains an empty entry: {module}")
    return fields


def validate_elf(module: Path, expected_machine: int) -> None:
    with module.open("rb") as stream:
        header = stream.read(64)
    if len(header) < 20 or header[:4] != b"\x7fELF":
        fail(f"module is not an ELF object: {module}")
    if header[4] != 2:
        fail(f"module is not a 64-bit ELF object: {module}")
    if header[5] != 1:
        fail(f"module is not a little-endian ELF object: {module}")
    elf_type = int.from_bytes(header[16:18], "little")
    machine = int.from_bytes(header[18:20], "little")
    if elf_type != 1:
        fail(f"module is not a relocatable ELF object: {module}")
    if machine != expected_machine:
        fail(
            f"module ELF machine {machine} does not match expected "
            f"machine {expected_machine}: {module}"
        )


def validate_unsigned_module(
    module: Path,
    target: dict[str, Any],
    objcopy_tool: str | Path,
    temporary: Path,
) -> list[bytes]:
    validate_elf(module, ARCHITECTURES[target["arch"]]["machine"])
    name = modinfo(MODINFO, module, "name")
    if name != "zerofs":
        fail(f"expected module name 'zerofs', found {name!r}: {module}")
    vermagic = modinfo(MODINFO, module, "vermagic")
    kernel_release = target["kernel_release"]
    if vermagic != kernel_release and not vermagic.startswith(f"{kernel_release} "):
        fail(
            f"module vermagic {vermagic!r} does not target "
            f"{kernel_release!r}: {module}"
        )
    if module.read_bytes().endswith(SIGNATURE_MAGIC):
        fail(f"input module is already signed: {module}")
    fields = module_metadata_fields(objcopy_tool, module, temporary)
    prefix = f"{IDENTITY_FIELD}=".encode("ascii")
    if any(field.startswith(prefix) for field in fields):
        fail(f"input module already declares {IDENTITY_FIELD}: {module}")
    return fields


def verify_signature(
    module: Path,
    trusted_cert: Path,
    temporary: Path,
) -> None:
    data = module.read_bytes()
    if not data.endswith(SIGNATURE_MAGIC):
        fail("signed module has no appended Linux module signature")
    structure_end = len(data) - len(SIGNATURE_MAGIC)
    structure_start = structure_end - SIGNATURE_TRAILER.size
    if structure_start < 0:
        fail("signed module has a truncated signature trailer")
    fields = SIGNATURE_TRAILER.unpack(data[structure_start:structure_end])
    identity_type, signer_length, key_id_length = fields[2:5]
    signature_length = fields[8]
    if identity_type != 2:
        fail("signed module signature is not PKCS#7")
    if any(fields[:2]) or signer_length or key_id_length or any(fields[5:8]):
        fail("signed module uses an unsupported signature trailer")
    signature_start = structure_start - signature_length
    if signature_length == 0 or signature_start < 64:
        fail("signed module has an invalid signature length")
    unsigned = temporary / "unsigned.ko"
    signature = temporary / "signature.p7s"
    unsigned.write_bytes(data[:signature_start])
    signature.write_bytes(data[signature_start:structure_start])
    run(
        [
            OPENSSL, "cms", "-verify", "-binary",
            "-inform", "DER", "-in", signature,
            "-content", unsigned,
            "-certfile", trusted_cert,
            "-nointern", "-noverify",
            "-out", os.devnull,
        ],
        "cannot verify appended module signature",
    )


def artifact_directories(values: list[str]) -> dict[str, Path]:
    if not values:
        fail("at least one --artifact TARGET_ID=DIRECTORY is required")
    result: dict[str, Path] = {}
    for value in values:
        target_id, separator, directory = value.partition("=")
        if not separator or not target_id or not directory:
            fail(f"invalid --artifact value: {value!r}")
        if target_id in result:
            fail(f"duplicate artifact target: {target_id}")
        result[target_id] = Path(directory)
    return result


def publication_identity(target: dict[str, Any]) -> tuple[str, ...]:
    distro = target["distro"]
    arch = target["arch"]
    if distro in {"ubuntu", "debian"}:
        native_arch = ARCHITECTURES[arch]["native_deb"]
        header_package = f"linux-headers-{target['kernel_release']}"
        header_version = target["kernel_package_version"]
    elif distro == "fedora":
        native_arch = ARCHITECTURES[arch]["native_rpm"]
        header_package = "kernel-devel"
        suffix = f".{native_arch}"
        rpm_version = target["kernel_package_version"]
        if not rpm_version.endswith(suffix):
            fail(
                f"{target['id']}: Fedora kernel package version does not "
                f"end in {suffix!r}"
            )
        header_version = f"0:{rpm_version.removesuffix(suffix)}"
    elif distro == "opensuse":
        native_arch = ARCHITECTURES[arch]["native_rpm"]
        header_package = "kernel-default-devel"
        source_identity = target["source"]["identity"]
        matches = [
            entry.removeprefix("kernel-default-devel@")
            for entry in source_identity.split(",")
            if entry.startswith("kernel-default-devel@")
        ]
        if len(matches) != 1 or not matches[0]:
            fail(f"{target['id']}: cannot derive kernel-default-devel version")
        header_version = f"0:{matches[0]}"
    else:
        fail(f"{target['id']}: unsupported distribution {distro!r}")

    components = (
        distro,
        native_arch,
        header_package,
        header_version,
        target["kernel_release"],
    )
    for component in components:
        if not IDENTITY_COMPONENT_PATTERN.fullmatch(component):
            fail(f"{target['id']}: unsafe publication path component {component!r}")
    # GitHub's artifact service rejects colons in uploaded paths. RPM uses a
    # colon between epoch and version, so serialize it with an otherwise
    # forbidden '@'. This is injective because raw identity components cannot
    # contain '@'.
    return tuple(component.replace(":", "@") for component in components)


def relative_module_path(target: dict[str, Any], version: str) -> PurePosixPath:
    return PurePosixPath(
        "kernel-modules",
        "v1",
        *publication_identity(target),
        version,
        "zerofs.ko.xz",
    )


def stage_module(
    *,
    target: dict[str, Any],
    artifact_directory: Path,
    version: str,
    staging: Path,
    signer: str | Path,
    sign_key: Path,
    signer_certificate: Path,
    trusted_cert: Path,
    strip_tool: str | Path,
    objcopy_tool: str | Path,
) -> dict[str, Any]:
    root = regular_path(
        artifact_directory, f"{target['id']} artifact", directory=True
    )
    source = regular_path(root / "zerofs.ko", f"{target['id']} module")

    relative = relative_module_path(target, version)
    identity = relative.parent.as_posix()
    destination = staging.joinpath(*relative.parts)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sign-", dir=staging) as temporary_text:
        temporary = Path(temporary_text)
        prepared = temporary / "zerofs.ko"
        shutil.copyfile(source, prepared)
        metadata_fields = validate_unsigned_module(
            prepared, target, objcopy_tool, temporary
        )
        run(
            [strip_tool, "--strip-debug", prepared],
            f"{target['id']}: cannot strip module",
        )
        modinfo_section = temporary / "modinfo.bin"
        modinfo_section.write_bytes(
            b"\0".join(
                metadata_fields + [f"{IDENTITY_FIELD}={identity}".encode("ascii")]
            )
            + b"\0"
        )
        run(
            [
                objcopy_tool,
                "--update-section",
                f".modinfo={modinfo_section}",
                prepared,
            ],
            f"{target['id']}: cannot bind the publication identity",
        )
        metadata_fields = module_metadata_fields(objcopy_tool, prepared, temporary)
        identity_field = f"{IDENTITY_FIELD}={identity}".encode("ascii")
        if metadata_fields.count(identity_field) != 1:
            fail(f"{target['id']}: module has ambiguous publication identity")
        if modinfo(MODINFO, prepared, IDENTITY_FIELD) != identity:
            fail(f"{target['id']}: module publication identity was not bound")
        run(
            [signer, "sha256", sign_key, signer_certificate, prepared],
            f"{target['id']}: cannot sign module",
        )
        verify_signature(prepared, trusted_cert, temporary)
        compressed = run(
            [XZ, "-9e", "--threads=1", "--stdout", prepared],
            f"{target['id']}: cannot compress module",
        )
        with destination.open("xb") as output:
            output.write(compressed)
        destination.chmod(0o644)

    return {
        "target_id": target["id"],
        "path": relative.as_posix(),
    }


def stage(arguments: argparse.Namespace) -> int:
    if not VERSION_PATTERN.fullmatch(arguments.package_version):
        fail("--package-version must be a full X.Y.Z-N DKMS package version")
    catalog = load_catalog(arguments.manifest)
    artifacts = artifact_directories(arguments.artifact)

    unknown = sorted(set(artifacts) - set(catalog.targets_by_id))
    if unknown:
        fail(f"unknown target id: {unknown[0]}")
    sign_key = regular_path(arguments.sign_key, "--sign-key")
    trusted_cert = regular_path(arguments.trusted_cert, "--trusted-cert")

    output = arguments.output.absolute()
    if output.exists() or output.is_symlink():
        fail(f"--output must not exist: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    parent = regular_path(output.parent, "output parent", directory=True)

    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.stage-", dir=parent))
    try:
        signer_certificate = staging / ".signing-cert.der"
        signer_certificate.write_bytes(
            run(
                [OPENSSL, "x509", "-in", trusted_cert, "-outform", "DER"],
                f"cannot parse X.509 certificate {trusted_cert}",
            )
        )
        entries = [
            stage_module(
                target=catalog.targets_by_id[target_id],
                artifact_directory=artifacts[target_id],
                version=arguments.package_version,
                staging=staging,
                signer=arguments.signer,
                sign_key=sign_key,
                signer_certificate=signer_certificate,
                trusted_cert=trusted_cert,
                strip_tool=arguments.strip_tool,
                objcopy_tool=arguments.objcopy_tool,
            )
            for target_id in sorted(artifacts)
        ]
        signer_certificate.unlink()
        manifest = {
            "schema_version": 1,
            "modules": entries,
        }
        (staging / "manifest.json").write_text(
            json.dumps(
                manifest,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n",
            encoding="ascii",
        )
        os.rename(staging, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    print(output)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sign and stage exact-kernel modules for object publication.",
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--package-version", required=True)
    parser.add_argument(
        "--artifact", action="append", default=[], metavar="TARGET_ID=DIRECTORY"
    )
    parser.add_argument("--signer", required=True)
    parser.add_argument("--sign-key", type=Path, required=True)
    parser.add_argument("--trusted-cert", type=Path, required=True)
    parser.add_argument("--strip-tool", required=True)
    parser.add_argument("--objcopy-tool", required=True)
    parser.add_argument("--output", type=Path, required=True)
    try:
        return stage(parser.parse_args())
    except (StageError, ManifestError, OSError, UnicodeError) as error:
        print(f"stage-prebuilt-modules.py: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
