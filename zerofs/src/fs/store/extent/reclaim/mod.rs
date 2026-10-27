//! Segment-space reclamation: durable counter scanning, repacking,
//! dead-segment deletion, and orphan sweeping.

mod activity;
pub mod cycle;
pub(crate) mod driver;
pub mod repack;
mod select;

pub(super) use activity::Activity;

/// Dense outputs below this size would immediately qualify for another repack.
pub(super) const SMALL_SEGMENT_BYTES: u64 = 1 << 20; // 1 MiB

/// Nominal stored-live-byte budget per repack job; the first source may exceed it.
pub(super) const REPACK_JOB_BYTES: u64 = 256 << 20; // 256 MiB

/// Exercise the production checkpoint query and protection policy in DST.
#[cfg(dst)]
pub async fn run_with_checkpoints(
    store: &super::ExtentStore,
    admin: &slatedb::admin::Admin,
    policy: cycle::CyclePolicy,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<cycle::ReclaimOutcome, crate::fs::FsError> {
    cycle::run(
        store,
        || driver::checkpoint_protection(Some(admin)),
        policy,
        cancel,
    )
    .await
}
