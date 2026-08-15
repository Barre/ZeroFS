use anyhow::{Context, Result};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_eager_driver_handoff()
        .build()
        .context("Failed to build Tokio runtime")?
        .block_on(zerofs::run_cli())
}
