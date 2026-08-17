pub mod block_transformer;
pub mod config;
pub mod db;
pub mod dedup;
pub mod frame_codec;
pub mod fs;
pub mod length_checked_object_store;
pub mod metadata_digest;
pub mod object_store_prefetch;
pub mod object_trace;
pub mod replication;
pub mod retrying_object_store;
pub mod segment;
pub mod segment_extractor;
pub mod segment_store;
pub mod storage_class_object_store;
pub mod task;

mod app;
mod bucket_identity;
mod checkpoint_manager;
mod cli;
mod key_management;
#[cfg(target_os = "linux")]
mod mount;
mod nbd;
mod net_util;
mod nfs;
mod ninep;
mod parse_object_store;
mod prometheus;
mod redis_conditional_store;
mod rpc;
mod secrets;
mod storage_compatibility;
mod telemetry;
#[cfg(feature = "webui")]
mod webui;

#[cfg(feature = "failpoints")]
pub mod failpoints;

#[cfg(test)]
pub mod fault_store;

#[cfg(test)]
pub mod test_helpers;

#[cfg(test)]
mod posix_tests;

#[cfg(test)]
mod zerofs_client_tests;

/// Run the ZeroFS command-line application.
///
/// This is public only so the package's thin binary target can enter the
/// library-owned implementation module graph.
#[doc(hidden)]
pub async fn run_cli() -> anyhow::Result<()> {
    app::run().await
}
