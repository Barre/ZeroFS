"""Verify the idiomatic Python facade: async context managers + async iteration.

Invoked by CI with the server target as argv[1] (PYTHONPATH includes both the
generated bindings and bindings/python/).
"""

import asyncio
import sys

import zerofs


async def main(target: str) -> None:
    # Client as an async context manager; closed on block exit.
    async with await zerofs.Client.connect(target) as fs:
        await fs.create_dir_all("/facade", 0o755)
        for i in range(5):
            await fs.write(f"/facade/f{i}", b"x")

        # File as an async context manager.
        async with await fs.open(
            "/facade/handle", zerofs.OpenOptions(write=True, create=True)
        ) as f:
            await f.write_at(0, b"via context manager")

        # Dir as both a context manager and an async iterator.
        names = []
        async with await fs.open_dir("/facade") as d:
            async for entry in d:
                names.append(entry.name)
        assert sorted(names) == ["f0", "f1", "f2", "f3", "f4", "handle"], names

        await fs.remove_dir_all("/facade")

    # The client is closed now: further calls raise Closed.
    try:
        await fs.read("/facade/f0")
        raise SystemExit("expected Closed after context exit")
    except zerofs.ZeroFsError.Closed:
        pass

    print("FACADE PYTHON CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
