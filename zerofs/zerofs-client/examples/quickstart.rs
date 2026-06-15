//! End-to-end tour of the `zerofs-client` API against a running ZeroFS server.
//!
//! Point it at a 9P endpoint (unix socket or TCP); the target may be given as
//! the first argument or the `ZEROFS_TARGET` env var, defaulting to a local
//! unix socket:
//!
//! ```text
//! cargo run --example quickstart -- unix:/run/zerofs/9p.sock
//! ZEROFS_TARGET=tcp://127.0.0.1:5564 cargo run --example quickstart
//! ```
//!
//! It is self-contained and idempotent: it works inside `/quickstart-demo`,
//! removing it first, and cleans up on the way out.

use futures::StreamExt;
use std::error::Error;
use zerofs_client::{Client, OpenOptions};

const DIR: &str = "/quickstart-demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let target = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("ZEROFS_TARGET").ok())
        .unwrap_or_else(|| "unix:/run/zerofs/9p.sock".to_string());

    let fs = Client::connect(&target).await?;
    let caps = fs.capabilities();
    println!(
        "connected to {target}  (msize={}, v1={}, v2={})",
        caps.msize, caps.extensions_v1, caps.extensions_v2
    );

    // Start from a clean slate.
    if fs.exists(DIR).await? {
        fs.remove_dir_all(DIR).await?;
    }
    fs.create_dir_all(format!("{DIR}/logs"), 0o755).await?;

    // One-shot write/read. Reads return `Bytes` (cheap to slice/clone).
    fs.write(format!("{DIR}/hello.txt"), b"hello from zerofs")
        .await?;
    let data = fs.read(format!("{DIR}/hello.txt")).await?;
    println!("read back {} bytes: {:?}", data.len(), &data[..]);

    // Positioned I/O through a File handle, then a cursor + tokio::io::copy.
    let big: Vec<u8> = (0..(1 << 20)).map(|i| (i % 251) as u8).collect();
    let src = fs
        .open(
            format!("{DIR}/big.bin"),
            OpenOptions::read_write().create(true).truncate(true),
        )
        .await?;
    src.write_at(0, &big).await?;
    src.sync_all().await?;

    let dst = fs.create(format!("{DIR}/big.copy")).await?;
    let copied = tokio::io::copy(&mut src.cursor(), &mut dst.cursor()).await?;
    println!("copied {copied} bytes via FileCursor");
    src.close().await;
    dst.close().await;

    // Stream the directory listing.
    let dir = fs.open_dir(DIR).await?;
    print!("entries: ");
    let mut entries = dir.entries();
    while let Some(entry) = entries.next().await {
        let entry = entry?;
        let size = entry.metadata.as_ref().map_or(0, |m| m.size);
        print!("{} ({size}B)  ", entry.name);
    }
    println!();
    dir.close().await;

    // Filesystem usage.
    let sf = fs.statfs().await?;
    println!(
        "statfs: {} blocks of {}B, {} free",
        sf.blocks, sf.block_size, sf.blocks_free
    );

    fs.remove_dir_all(DIR).await?;
    fs.close().await;
    println!("done");
    Ok(())
}
