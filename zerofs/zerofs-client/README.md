# zerofs-client

An async, path-based Rust client for a [ZeroFS](https://github.com/Barre/ZeroFS)
server. It speaks 9P2000.L over TCP or a unix socket, and uses the ZeroFS
fast-path extensions automatically when the server offers them.

The primary surface is one-shot path operations on a shared `Client` (`read`,
`write`, `stat`, `rename`, `mkdir`), with `File` and `Dir` handles where
statefulness pays off (chunked I/O, incremental listing, `openat`-style child
operations). Paths are bytes, as on POSIX and the 9P wire: every path parameter
is `impl AsRef<Path>`, so non-UTF-8 names work with no separate API.

This is a trusted-network client: you assert your uid at connect time,
NFS-style.

## Usage

```rust
use zerofs_client::{Client, OpenOptions};

#[tokio::main]
async fn main() -> Result<(), zerofs_client::ZeroFsError> {
    let fs = Client::connect("unix:/run/zerofs/9p.sock").await?;

    fs.create_dir_all("/projects/demo", 0o755).await?;
    fs.write("/projects/demo/hello.txt", b"hello from zerofs").await?;

    let data = fs.read("/projects/demo/hello.txt").await?;
    assert_eq!(&data[..], b"hello from zerofs");

    for entry in fs.read_dir("/projects/demo").await? {
        let size = entry.metadata.as_ref().map_or(0, |m| m.size);
        println!("{size:>8}  {}", entry.name);
    }

    let file = fs
        .open("/projects/demo/big.bin", OpenOptions::read_write().create(true))
        .await?;
    file.write_at(0, &vec![0u8; 4 << 20]).await?;
    file.sync_all().await?;
    file.close().await;
    Ok(())
}
```

## Connection targets

`Client::connect` accepts:

- `unix:/path/to/sock`: unix socket
- `tcp://host:port`: TCP
- `host:port` / `host`: TCP (default port 5564)
- a bare filesystem path (`/run/zerofs/9p.sock`, `./sock`): unix socket

Use `Client::connect_with` for explicit identity (`uid`/`gid`/`uname`), the
attach subtree (`aname`), `msize`, and `connect_timeout_ms`.

## Lifecycles

- **Close & drop.** `close()` marks the handle closed immediately (later calls
  return `Closed`); it always succeeds, is idempotent, and never hangs. A
  handle's server-side fid is released and its number recycled when the handle
  is dropped (for scope-bound use, right after `close()`); dropping a handle you
  never closed does the same. Fid numbers are always reused, so a long-running
  client never exhausts them.
- **Cancellation.** Every public future is cancel-safe: dropping it at any
  await point leaks nothing. There is no per-operation timeout parameter; bound
  waits with `tokio::time::timeout`.
- **Connection loss.** The underlying session reconnects forever with backoff
  and replays its state; while the server is unreachable, calls block rather
  than fail. An op in flight at a disconnect is resent, so a non-idempotent op
  (rename, create, unlink) can apply twice across a reconnect.

## Reads return `Bytes`

`read`, `read_range`, and `File::read_at` return [`bytes::Bytes`] (re-exported
as `zerofs_client::Bytes`): cheap to clone and slice, derefs to `&[u8]`, and a
read served by a single round trip comes back with no copy. `ZeroFsError`
converts to `std::io::Error` (`impl From`), so client calls propagate with `?`
in functions returning `io::Result`.

## Features

All feature-gated additions are Rust-only and never cross the FFI boundary.

- `tokio-io` *(off by default)*: `File::cursor()` returning an `io::FileCursor`
  that implements `AsyncRead + AsyncWrite + AsyncSeek` over the positioned API,
  so `tokio::io::copy` and friends work.
- `stream` *(off by default)*: `Dir::entries()` returning a
  `futures::Stream` of `DirEntry`, for `StreamExt` consumers.
- `serde` *(off by default)*: `Serialize`/`Deserialize` on the plain data
  records (`Metadata`, `DirEntry`, `StatFs`, `Timestamp`, ...).

`SystemTime` converts to/from `Timestamp` and into `SetTime`, so the standard
time types work directly with `set_times` and the metadata accessors.

## License

AGPL-3.0
