"""Verify the Python ergonomics: streaming, PathLike, str path helpers, sync API.

Invoked with the server target as argv[1].
"""

import asyncio
import importlib.util
import os
import pathlib
import sys
import typing

import zerofs


def typing_part() -> None:
    # The facade imports cleanly with the new annotations (deferred via
    # `from __future__ import annotations`, so they never run at import time).
    assert hasattr(zerofs.File, "write_from"), "write_from not installed on File"
    assert hasattr(zerofs.File, "read_chunks")

    # get_type_hints resolves the deferred annotations against the generated
    # types, proving they are valid type objects, not unresolved strings.
    hints = typing.get_type_hints(zerofs.File.write_from)
    assert hints["return"] is int, hints
    assert hints["offset"] is int, hints

    # PEP 561: the installed package ships a py.typed marker so checkers honor
    # the inline annotations.
    pkg_dir = os.path.dirname(zerofs.__file__)
    assert os.path.exists(os.path.join(pkg_dir, "py.typed")), "missing zerofs/py.typed"

    # The generated low-level module is also marked typed.
    spec = importlib.util.find_spec("zerofs_ffi")
    ffi_dir = os.path.dirname(spec.origin)
    assert os.path.exists(os.path.join(ffi_dir, "py.typed")), "missing zerofs_ffi/py.typed"
    print("TYPING OK")


async def async_part(target: str) -> None:
    fs = await zerofs.Client.connect(target)

    # PathLike: pathlib.Path works anywhere a path is taken.
    await fs.create_dir_all(pathlib.Path("/erg/a/b"), 0o755)
    assert await fs.exists(pathlib.Path("/erg/a/b"))

    # Streaming reads in chunks.
    big = bytes((i % 251) for i in range(250_000))
    await fs.write("/erg/big.bin", big)
    f = await fs.open("/erg/big.bin", zerofs.OpenOptions(read=True))
    chunks = [chunk async for chunk in f.read_chunks(64 * 1024)]
    await f.close()
    assert b"".join(chunks) == big
    assert len(chunks) >= 2, "expected a multi-chunk stream"

    # Streaming writes: drain an async generator of chunks, then read it back.
    parts = [bytes((i % 251) for i in range(70_000)) for _ in range(4)]
    expected = b"".join(parts)

    async def gen():
        for part in parts:
            yield part
        yield b""  # empty chunks are skipped, not written

    async with await fs.open(
        "/erg/written.bin", zerofs.OpenOptions(write=True, create=True, truncate=True)
    ) as wf:
        total = await wf.write_from(gen())
    assert total == len(expected), (total, len(expected))
    assert await fs.read("/erg/written.bin") == expected

    # write_from honors a starting offset: leave a 5-byte gap, then write.
    async def one():
        yield b"tail"

    async with await fs.open(
        "/erg/offset.bin", zerofs.OpenOptions(write=True, create=True, truncate=True)
    ) as of:
        await of.write_at(0, b"head_")
        n = await of.write_from(one(), offset=5)
    assert n == 4, n
    assert await fs.read("/erg/offset.bin") == b"head_tail"

    # String path helpers (decode the lossless bytes for the UTF-8 case).
    await fs.symlink("big.bin", "/erg/link")
    assert await fs.read_link_str("/erg/link") == "big.bin"
    await fs.symlink("/erg/a", "/erg/cur")
    assert await fs.canonicalize_str("/erg/cur/b") == "/erg/a/b"

    # Metadata predicates (stat does not follow symlinks).
    fmd = await fs.stat("/erg/big.bin")
    assert fmd.is_file() and not fmd.is_dir() and not fmd.is_symlink()
    assert fmd.permissions() == fmd.mode & 0o7777
    assert (await fs.stat("/erg/a")).is_dir()
    assert (await fs.stat("/erg/link")).is_symlink()

    # Text convenience (non-ASCII round-trips via UTF-8).
    await fs.write_text("/erg/t.txt", "héllo")
    assert await fs.read_text("/erg/t.txt") == "héllo"

    await fs.remove_dir_all("/erg")
    await fs.close()
    print("ASYNC ERGONOMICS OK")


def sync_part(target: str) -> None:
    # Blocking API for non-async code; `with` closes it (and stops the loop).
    with zerofs.connect_sync(target) as fs:
        fs.create_dir_all("/sync", 0o755)
        fs.write("/sync/a.txt", b"sync hello")
        assert fs.read("/sync/a.txt") == b"sync hello"
        for i in range(3):
            fs.write(f"/sync/f{i}", b"x")
        d = fs.open_dir("/sync")
        names = sorted(e.name for e in d)  # blocking iteration over the directory
        d.close()
        assert names == ["a.txt", "f0", "f1", "f2"], names
        fs.remove_dir_all("/sync")
    print("SYNC ERGONOMICS OK")


if __name__ == "__main__":
    target = sys.argv[1]
    typing_part()
    asyncio.run(async_part(target))
    sync_part(target)
    print("ALL PYTHON ERGONOMICS PASSED")
