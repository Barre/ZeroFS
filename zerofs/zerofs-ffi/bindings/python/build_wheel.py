#!/usr/bin/env python3
"""Build an installable wheel for the ZeroFS Python bindings, stdlib only.

The FFI crate lives inside the main Cargo workspace, where maturin's
`cargo run --bin uniffi-bindgen` cannot resolve the bin (the workspace root is
the `zerofs` package). So this builder drives generation itself with an explicit
`-p zerofs-ffi` and assembles the wheel by hand:

  * `zerofs_ffi/`: the uniffi-generated low-level module + the bundled cdylib,
  * `zerofs/`: the idiomatic facade (async context managers + iteration),
  * `py.typed`: PEP 561 markers in both packages so type checkers honor the
                    inline annotations,
  * `*.dist-info/`: METADATA, WHEEL, RECORD.

Usage:
    python3 build_wheel.py [out_dir]        # default out_dir: target/wheels

The wheel is tagged for the host platform. For PyPI, run the Linux wheel through
`auditwheel repair` to produce a portable manylinux wheel.
"""

import base64
import hashlib
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import zipfile

NAME = "zerofs"
VERSION = "0.1.0"
SUMMARY = "Async, path-based client for ZeroFS over 9P, with idiomatic Python ergonomics"

HERE = os.path.dirname(os.path.abspath(__file__))
FFI_DIR = os.path.normpath(os.path.join(HERE, "..", ".."))   # zerofs-ffi/
WS_DIR = os.path.normpath(os.path.join(FFI_DIR, ".."))       # zerofs/ (workspace)
FACADE = os.path.join(HERE, "zerofs.py")


def run(cmd, **kw):
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, **kw)


def cdylib_path(profile: str) -> str:
    sub = "release" if profile == "release" else "debug"
    for name in ("libzerofs_ffi.so", "libzerofs_ffi.dylib", "zerofs_ffi.dll"):
        p = os.path.join(WS_DIR, "target", sub, name)
        if os.path.exists(p):
            return p
    raise SystemExit(f"{profile} cdylib not found; build failed?")


def platform_tag() -> str:
    return sysconfig.get_platform().replace("-", "_").replace(".", "_")


def record_line(arcname: str, data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    return f"{arcname},sha256={digest},{len(data)}"


def main() -> None:
    out_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(WS_DIR, "target", "wheels")
    os.makedirs(out_dir, exist_ok=True)

    # Build both: the debug lib keeps the uniffi metadata symbols that bindgen
    # reads (release strips them); the release lib is what we bundle and run.
    run(["cargo", "build", "-p", "zerofs-ffi"], cwd=WS_DIR)
    run(["cargo", "build", "--release", "-p", "zerofs-ffi"], cwd=WS_DIR)
    with tempfile.TemporaryDirectory() as gen:
        run(
            ["cargo", "run", "-q", "-p", "zerofs-ffi", "--bin", "uniffi-bindgen", "--",
             "generate", "--library", cdylib_path("debug"), "--language", "python",
             "--out-dir", gen],
            cwd=WS_DIR,
        )
        generated = os.path.join(gen, "zerofs_ffi.py")
        lib = cdylib_path("release")
        libname = os.path.basename(lib)

        members: dict[str, bytes] = {}
        with open(generated, "rb") as f:
            members[f"zerofs_ffi/__init__.py"] = f.read()
        with open(lib, "rb") as f:
            members[f"zerofs_ffi/{libname}"] = f.read()
        with open(FACADE, "rb") as f:
            members["zerofs/__init__.py"] = f.read()

        # PEP 561 markers: make both packages typed so checkers honor the inline
        # annotations on the facade (and the generated module if it ships none).
        members["zerofs/py.typed"] = b""
        members["zerofs_ffi/py.typed"] = b""

        tag = f"py3-none-{platform_tag()}"
        dist = f"{NAME}-{VERSION}.dist-info"
        members[f"{dist}/METADATA"] = (
            f"Metadata-Version: 2.1\n"
            f"Name: {NAME}\n"
            f"Version: {VERSION}\n"
            f"Summary: {SUMMARY}\n"
            f"License: AGPL-3.0\n"
            f"Requires-Python: >=3.9\n"
        ).encode()
        members[f"{dist}/WHEEL"] = (
            f"Wheel-Version: 1.0\n"
            f"Generator: zerofs build_wheel.py\n"
            f"Root-Is-Purelib: false\n"
            f"Tag: {tag}\n"
        ).encode()

        record = "\n".join(record_line(n, d) for n, d in members.items())
        record += f"\n{dist}/RECORD,,\n"
        members[f"{dist}/RECORD"] = record.encode()

        wheel = os.path.join(out_dir, f"{NAME}-{VERSION}-{tag}.whl")
        with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as z:
            for arcname, data in members.items():
                z.writestr(arcname, data)

    print(f"\nbuilt {wheel}")


if __name__ == "__main__":
    main()
