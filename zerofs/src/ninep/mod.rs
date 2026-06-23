pub mod errors;
pub mod handler;
pub mod lock_manager;
pub mod server;

pub use server::NinePServer;

// End-to-end test of the handler's op-id dedup path (in-process server + the real
// client over a unix socket). Lives here, not in `tests/`, because it needs the
// crate-internal `NinePServer`.
#[cfg(test)]
mod op_id_dedup_tests;
