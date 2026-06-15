# zerofs-client

Async, path-based Python client for [ZeroFS](https://github.com/Barre/ZeroFS)
over 9P, with idiomatic ergonomics: async context managers, async iteration,
streaming, `PathLike` arguments, and a blocking (sync) API.

```python
import zerofs_client

async with await zerofs_client.Client.connect("unix:/run/zerofs/9p.sock") as fs:
    await fs.write("/hello.txt", b"hi")
    print(await fs.read("/hello.txt"))
```

The wheel bundles the native library, so `pip install zerofs-client` is self-contained.
The low-level uniffi bindings remain available as `import zerofs_ffi`.
