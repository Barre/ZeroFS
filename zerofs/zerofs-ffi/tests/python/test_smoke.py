"""Integration smoke test for the generated Python bindings.

Run by the CI harness, which generates the bindings, starts a real ZeroFS 9P
server, and invokes this with the target as argv[1].
"""

import asyncio
import datetime
import sys

import zerofs_ffi as z


async def main(target: str) -> None:
    # connect_with: explicit options (record with defaults) reach the server.
    fs = await z.Client.connect_with(target, z.ConnectOptions(msize=512 * 1024))
    caps = fs.capabilities()
    assert caps.msize <= 512 * 1024, caps.msize
    print(f"connected: msize={caps.msize} v1={caps.extensions_v1} v2={caps.extensions_v2}")

    await fs.create_dir_all("/ffi/logs", 0o755)

    # One-shot write / read / read_range / append.
    await fs.write("/ffi/a.txt", b"hello world")
    assert await fs.read("/ffi/a.txt") == b"hello world"
    assert await fs.read_range("/ffi/a.txt", 6, 5) == b"world"
    off = await fs.append("/ffi/a.txt", b"!!!")
    assert off == 11, off
    assert await fs.read("/ffi/a.txt") == b"hello world!!!"

    # Metadata family.
    meta = await fs.stat("/ffi/a.txt")
    assert meta.size == 14 and meta.file_type == z.FileType.FILE
    m2 = await fs.chmod("/ffi/a.txt", 0o600)
    assert m2.mode & 0o777 == 0o600
    m3 = await fs.truncate("/ffi/a.txt", 5)
    assert m3.size == 5
    assert await fs.exists("/ffi/a.txt") and not await fs.exists("/ffi/nope")
    sf = await fs.statfs()
    assert sf.block_size > 0
    await fs.sync()

    # set_times with the SetTime enum; times cross as native datetime.
    when = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc)
    updated = await fs.set_times("/ffi/a.txt", None, z.SetTime.AT(when))
    assert isinstance(updated.mtime, datetime.datetime), updated.mtime
    assert int(updated.mtime.timestamp()) == int(when.timestamp()), updated.mtime

    # Symlinks (lossless byte target back).
    await fs.symlink("a.txt", "/ffi/link")
    assert await fs.read_link("/ffi/link") == b"a.txt"
    assert (await fs.stat("/ffi/link")).file_type == z.FileType.SYMLINK

    # File handle: positioned I/O.
    f = await fs.open("/ffi/big.bin", z.OpenOptions(read=True, write=True, create=True, truncate=True))
    payload = bytes((i % 251) for i in range(300_000))
    await f.write_at(0, payload)
    assert await f.read_at(0, 5) == payload[:5]
    assert (await f.metadata()).size == len(payload)
    await f.sync_all()
    await f.close()

    # Dir handle + *_at suite, including a non-UTF-8 name.
    d = await fs.open_dir("/ffi")
    await d.create_dir_at(b"sub", 0o755)
    weird = b"\xff\xfe-bytes"
    child = await d.open_at(weird, z.OpenOptions(write=True, create=True))
    await child.write_at(0, b"raw")
    await child.close()
    names = sorted(e.name_bytes for e in await d.next_batch(None))
    assert weird in names, names
    assert await d.read_link_at(b"link") == b"a.txt"
    await d.close()

    # read_dir returns inline metadata (readdirplus).
    entries = {e.name: e for e in await fs.read_dir("/ffi")}
    assert "a.txt" in entries and "sub" in entries
    assert entries["sub"].file_type == z.FileType.DIR

    # Structured error.
    try:
        await fs.read("/ffi/missing")
        raise SystemExit("expected NotFound")
    except z.ZeroFsError.NotFound:
        pass

    await fs.remove_dir_all("/ffi")
    assert not await fs.exists("/ffi")
    await fs.close()
    print("ALL FFI PYTHON CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
