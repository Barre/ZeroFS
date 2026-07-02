//! One-line digests of metadata (LSM) compaction activity.
//!
//! The default log filter drops the LSM engine's own per-compaction lines
//! (one per compaction, in its internal vocabulary; see `init_logging`). This
//! task retells that story in ZeroFS's voice: at most one line per interval,
//! emitted only when compaction actually ran, mirroring the segment GC's
//! one-line-per-pass summary. The L0 depth rides along as the early-warning
//! signal: flushes stack up in L0 until compaction folds them down, and a
//! full backlog (`l0_max_ssts`) stalls writers.

use std::sync::Arc;
use std::time::Duration;

use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::fs::store::extent::human_bytes;

const DIGEST_INTERVAL: Duration = Duration::from_secs(60);

/// Warn when L0 crosses this percentage of `l0_max_ssts` from below: a full
/// L0 stalls writers, so the operator hears about it before it fills.
const L0_WARN_PERCENT: usize = 80;

/// Counters the embedded compaction worker maintains. Summed across label
/// sets: there is one embedded worker, but the metrics carry its worker id.
const BYTES_COMPACTED: &str = "slatedb.compactor.bytes_compacted";
const SSTS_WRITTEN: &str = "slatedb.compactor.ssts_written";

fn compactor_counters(recorder: &DefaultMetricsRecorder) -> (u64, u64) {
    let snap = recorder.snapshot();
    let sum = |name: &str| -> u64 {
        snap.by_name(name)
            .iter()
            .map(|m| match &m.value {
                MetricValue::Counter(v) => *v,
                _ => 0,
            })
            .sum()
    };
    (sum(BYTES_COMPACTED), sum(SSTS_WRITTEN))
}

/// The digest line, or `None` when the interval saw no compaction: idle is
/// the common case and not worth a line.
fn digest_line(bytes_delta: u64, ssts_delta: u64, l0: usize, l0_max: usize) -> Option<String> {
    if bytes_delta == 0 && ssts_delta == 0 {
        return None;
    }
    Some(format!(
        "metadata compaction: {} compacted into {} SSTs over the last {}s; L0 {l0}/{l0_max}",
        human_bytes(bytes_delta),
        ssts_delta,
        DIGEST_INTERVAL.as_secs(),
    ))
}

pub fn spawn(
    recorder: Arc<DefaultMetricsRecorder>,
    status: tokio::sync::watch::Receiver<slatedb::DbStatus>,
    l0_max_ssts: usize,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    crate::task::spawn_named("metadata-compaction-digest", async move {
        let (mut prev_bytes, mut prev_ssts) = compactor_counters(&recorder);
        let warn_at = (l0_max_ssts * L0_WARN_PERCENT).div_ceil(100).max(1);
        let mut was_high = false;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(DIGEST_INTERVAL) => {}
            }
            let (bytes, ssts) = compactor_counters(&recorder);
            let l0 = status.borrow().current_manifest.l0().len();
            if let Some(line) = digest_line(
                bytes.saturating_sub(prev_bytes),
                ssts.saturating_sub(prev_ssts),
                l0,
                l0_max_ssts,
            ) {
                info!("{line}");
            }
            // Warn only on the crossing, not every tick, so a persistently
            // deep backlog doesn't turn the warning into noise.
            let high = l0 >= warn_at;
            if high && !was_high {
                warn!(
                    "metadata L0 backlog at {l0}/{l0_max_ssts} SSTs; writes stall if it fills \
                     (compaction is not keeping up)"
                );
            }
            was_high = high;
            prev_bytes = bytes;
            prev_ssts = ssts;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_silent_when_idle() {
        assert_eq!(digest_line(0, 0, 3, 256), None);
    }

    #[test]
    fn digest_reports_activity_and_l0() {
        let line = digest_line(64 << 20, 3, 12, 256).unwrap();
        assert!(line.contains("64.0 MiB"), "{line}");
        assert!(line.contains("3 SSTs"), "{line}");
        assert!(line.contains("L0 12/256"), "{line}");
    }
}
