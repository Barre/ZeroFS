# zerofs-client

An async, path-based Rust client for a [ZeroFS](https://github.com/Barre/ZeroFS)
server. It speaks the private `9P2000.L.Z` dialect over TCP or a unix
socket.

The primary surface is one-shot path operations on a shared `Client` (`read`,
`write`, `stat`, `rename`, `mkdir`), with `File` and `Dir` handles where
statefulness pays off (chunked I/O, incremental listing, `openat`-style child
operations). Paths are bytes, as on POSIX and the 9P wire: every path parameter
is `impl AsRef<Path>`, so non-UTF-8 names work with no separate API.

The client asserts its uid at connect time and assumes a trusted network.

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
        let size = entry.metadata.size;
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

For an HA pair, pass both nodes as one comma-separated string:

```rust
let fs = Client::connect("node-a:5564,node-b:5564").await?;
```

The client races the targets to find the serving leader and re-probes the set
after a disconnect. Rust callers that already hold separate strings can use
`Client::connect_multi(&targets)`.

Use `Client::connect_with` for explicit identity (`uid`/`gid`/`uname`), the
attach subtree (`aname`), `msize`, and `connect_timeout_ms`.

## Lifecycles

- **Close and drop.** `close()` marks the handle closed and is idempotent and
  non-blocking. Dropping the handle schedules its server fid for release and
  recycling. Dropping an open handle performs the same cleanup.
- **Cancellation.** Dropping a public future releases its client-side fids. A
  dispatched mutation may still complete, leaving an ambiguous outcome.
  Per-operation timeouts are supplied by the caller when this ambiguity is
  acceptable.
- **Connection loss.** The session reconnects with backoff and restores fids
  and locks before accepting requests. Undispatched calls wait while no server
  is reachable. In-flight mutations retain their operation envelope across
  resends. Resends stop at the 120-second protocol horizon; unresolved outcomes
  return an ambiguous connection failure. Open-unlinked handles are
  connection-local and produce `ESTALE` after connection loss.

## Reads return `Bytes`

`read`, `read_range`, and `File::read_at` return [`bytes::Bytes`] (re-exported
as `zerofs_client::Bytes`): cheap to clone and slice, derefs to `&[u8]`, and a
read served by a single round trip comes back with no copy. `ZeroFsError`
converts to `std::io::Error` (`impl From`), so client calls propagate with `?`
in functions returning `io::Result`.

## Features

Feature-gated additions are Rust-only and are excluded from the FFI boundary.

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
