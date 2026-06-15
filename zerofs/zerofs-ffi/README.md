# zerofs-ffi

Foreign-language bindings for [`zerofs-client`](../zerofs-client), generated with
[uniffi](https://mozilla.github.io/uniffi-rs/) (Python),
[uniffi-bindgen-node-js](https://github.com/criccomini/uniffi-bindgen-node-js)
(runnable Node/TypeScript over koffi), and
[uniffi-bindgen-go](https://github.com/NordSecurity/uniffi-bindgen-go) (runnable
Go over cgo). It is a thin, FFI-shaped re-export of the Rust client:

- paths cross as `String` (and byte-exact child names in the `Dir` `*_at` suite
  as `bytes`/`Uint8Array` for non-UTF-8 entries),
- byte payloads as owned byte arrays,
- handles (`Client`, `File`, `Dir`) as reference-counted objects,
- errors as a flat, exhaustive `ZeroFsError`.

Every method is async and maps to the host language's native async (Python
`await`, a TS `Promise`); in Go the async calls cross as ordinary synchronous
calls that block on the cgo thread and return `(value, error)`. The crate opts
out of `zerofs-client`'s default features; the Rust-only sugar (`tokio-io`
cursor, `Stream`, serde) does not cross the FFI boundary.

## Generating bindings

The bindings are generated from the compiled `cdylib`, not checked in. Build
once, then generate per language:

```sh
cargo build -p zerofs-ffi
./zerofs-ffi/bindings/generate.sh python  out/python
cargo install uniffi-bindgen-node-js      # once, for the Node/TypeScript package
./zerofs-ffi/bindings/generate.sh typescript out/ts   # bundles the lib + facade
cargo install uniffi-bindgen-go \
  --git https://github.com/NordSecurity/uniffi-bindgen-go \
  --tag v0.7.0+v0.31.0                     # once, for the Go package
./zerofs-ffi/bindings/generate.sh go out/go           # generated pkg + facade
```

For Python, also copy the cdylib next to the generated module (the wheel builder
does this for you):

```sh
cp target/debug/libzerofs_ffi.so out/python/
```

CI (`.github/workflows/ffi.yml`) generates all three. Python, Node/TypeScript,
and Go run end-to-end against a real ZeroFS server.

## Packaging

**Python** ships as an installable wheel that bundles the cdylib, the generated
`zerofs_ffi` module, and the `zerofs` facade:

```sh
python3 zerofs-ffi/bindings/python/build_wheel.py   # -> target/wheels/zerofs-*.whl
pip install target/wheels/zerofs-*.whl
python3 -c "import asyncio, zerofs; ..."
```

The builder is a stdlib-only script rather than maturin: the FFI crate lives in
the main Cargo workspace, where maturin's `cargo run --bin uniffi-bindgen`
cannot resolve the bin (the workspace root *is* the `zerofs` package). The
script drives generation with an explicit `-p zerofs-ffi`, generates from the
debug lib (the release profile strips the uniffi metadata symbols bindgen reads)
and bundles the release lib. The wheel is host-tagged; for PyPI, run it through
`auditwheel repair` to produce a portable `manylinux` wheel. CI builds the
wheel, `pip install`s it, and runs the bindings against a real server.

**Node/TypeScript** is itself an npm package: `generate.sh typescript` emits a
self-contained ESM package (via `uniffi-bindgen-node-js`) that loads the cdylib
through [koffi](https://koffi.dev) (no wasm, so sockets work), bundles the
native library, and drops in the `zerofs.ts` facade. It runs end-to-end in CI.
Two notes: the generator keeps method names in `snake_case` (e.g. `open_dir`,
`next_batch`); the facade adds `camelCase` aliases beside them, and
`generate.sh` patches one upstream bug in its timestamp converter.

**Go** is a runnable cgo binding: `generate.sh go` emits a `zerofs_ffi/` Go
package (via `uniffi-bindgen-go`) with a C header, and copies `facade.go` beside
it. The async Rust calls cross as ordinary blocking Go calls returning
`(value, error)`; `SystemTime` crosses as `time.Time` and errors are matchable
with `errors.Is(err, zerofs_ffi.ErrZeroFsErrorNotFound)`. Build with
`CGO_CFLAGS=-I<pkg dir>` and `CGO_LDFLAGS="-L target/debug -lzerofs_ffi"`, and
run with `LD_LIBRARY_PATH=target/debug`. `generate.sh` patches one upstream bug
in this generator version (it lifts the return buffer even on the error path, so
value-returning fallible calls would panic; the patch gates the lift on the
call status). It runs end-to-end in CI. The Go package name keeps the
underscore (`zerofs_ffi`): in this generator version the `package` identifier is
hardcoded to the Rust crate's namespace and the `package_name` config key only
renames the output dir/header, not the `package` line, so there is no clean
config-only way to make it idiomatic without touching the crate's lib name.

## Idiomatic facades

uniffi generates a faithful but mechanical surface (async methods, raw handle
objects). Each language also ships a small hand-written **facade** in
`bindings/<lang>/` that re-exports the generated code and adds the niceties
uniffi cannot express. Distribute the facade alongside the generated module.

| Language   | Facade                          | Adds |
|------------|---------------------------------|------|
| Python     | `bindings/python/zerofs.py`     | `async with`; `async for entry in dir`; streaming `file.read_chunks()` and `file.write_from(async_iterable, offset=0)`; `PathLike` args; `read_text`/`write_text`; `canonicalize_str`/`read_link_str`; metadata `meta.is_dir()`/`is_file()`/`is_symlink()`/`permissions()`; blocking `connect_sync()`; full type hints + `py.typed` markers in both packages |
| TypeScript | `bindings/typescript/zerofs.ts` | `await using` (`Symbol.asyncDispose`); `for await (const e of dir)`; Web Streams: `file.readable(chunkSize=1MiB)` (`ReadableStream`) and `file.writable(offset=0)` (`WritableStream`, backpressure-honouring); `camelCase` aliases (`openDir`, `nextBatch`, `readAt`, …) beside the kept `snake_case` names; `canonicalizeStr`/`readLinkStr`; metadata predicate helpers `isDir(m)`/`isFile(m)`/`isSymlink(m)`/`permissions(m)` |
| Go         | `bindings/go/facade.go`         | `dir.Entries()` as a range-over-func `iter.Seq2[DirEntry, error]` (one batch at a time); `file.Reader()`/`file.ReaderAt(off)` as `io.Reader` (also `io.WriterTo`, so `io.Copy` streams with no extra buffer); `file.Writer()`/`file.WriterAt(off)` as `io.Writer`; `CanonicalizeStr`/`ReadLinkStr`; metadata `meta.IsDir()`/`IsFile()`/`IsSymlink()`/`Permissions()`; `Call(ctx, fn)`/`Do(ctx, fn)` context wrappers around the blocking calls |

```python
import zerofs
async with await zerofs.Client.connect(target) as fs:
    async with await fs.open_dir("/") as d:
        async for entry in d:
            print(entry.name)

# ...or from synchronous code:
with zerofs.connect_sync(target) as fs:
    print(fs.read("/hello.txt"))
    for entry in fs.open_dir("/"):
        print(entry.name)
```

```typescript
import { Client } from "./zerofs.ts";
await using fs = await Client.connect(target);
await using dir = await fs.open_dir("/");
for await (const entry of dir) console.log(entry.name);
```

The CI jobs exercise these: Python and Node run the facades against a real
server. The TS facade also exports `openOptions({ ... })` to fill `OpenOptions`
defaults (the Node generator doesn't apply uniffi record defaults).

## Per-language notes

- **`File` name collision.** `File` shadows the DOM `File`. Import with an
  alias: `import { File as ZfsFile } from "..."` (TypeScript).
- **Cancellation.** None of the bindings cancel server-side work; uniffi does
  not propagate host-side cancellation to Rust. You can only bound *your* wait,
  with the host's own facilities: `asyncio.wait_for` (Python), `Promise.race`
  (TypeScript), or the `Call(ctx, fn)`/`Do(ctx, fn)` helpers (Go). All three
  *abandon the wait*: they return as soon as your timeout/context fires, while
  the in-flight Rust op keeps running until the server answers, then discards
  its result and self-cleans (see the client's lifecycle docs). Pass a deadline
  to bound how long you wait, not how long the server works.
- **Exhaustive errors.** `ZeroFsError` is intentionally exhaustive: a new
  upstream variant fails this crate's build rather than silently degrading.
- **Matching errors idiomatically.** Python raises typed exceptions
  (`except zerofs.ZeroFsError.NotFound`); Go exposes sentinels for
  `errors.Is(err, zerofs_ffi.ErrZeroFsErrorNotFound)`; TypeScript throws
  subclasses of `ZeroFsError` for `instanceof ZeroFsErrorNotFound`.

## Example (Python)

```python
import asyncio, zerofs_ffi as z

async def main():
    fs = await z.Client.connect("unix:/run/zerofs/9p.sock")
    await fs.write("/hello.txt", b"hi")
    print(await fs.read("/hello.txt"))
    for e in await fs.read_dir("/"):
        print(e.name, e.metadata.size if e.metadata else "?")
    await fs.close()

asyncio.run(main())
```

## License

AGPL-3.0
