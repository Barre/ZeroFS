//! Whole-store accounting invariants: segment counters and footprint gauges.

use futures::StreamExt;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::Ordering::Relaxed;
use zerofs::fs::ZeroFS;
use zerofs::fs::key_codec::{KeyCodec, KeyPrefix};
use zerofs::fs::metrics::SegmentFootprint;
use zerofs::segment::{FrameLoc, Segid};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentCounter {
    live_bytes: u64,
    appended_bytes: u64,
}

impl SegmentCounter {
    fn decode(value: &[u8]) -> Option<Self> {
        KeyCodec::decode_segcount(value).map(|(live_bytes, appended_bytes)| Self {
            live_bytes,
            appended_bytes,
        })
    }
}

#[derive(Debug, Default)]
struct SegmentAccounting {
    referenced_bytes: BTreeMap<Segid, u64>,
    counters: BTreeMap<Segid, SegmentCounter>,
}

impl SegmentAccounting {
    async fn load(fs: &ZeroFS) -> Self {
        let codec = KeyCodec::new();
        let mut accounting = Self::default();

        let (extent_start, extent_end) = codec.prefix_range(KeyPrefix::Extent);
        let mut extents = fs
            .db
            .scan(extent_start..extent_end)
            .await
            .expect("extent scan");
        while let Some(item) = extents.next().await {
            let (_, value) = item.expect("extent scan item");
            let loc = FrameLoc::decode(&value).expect("FrameLoc decode");
            *accounting.referenced_bytes.entry(loc.segid).or_default() += loc.byte_len as u64;
        }

        let (counter_start, counter_end) = codec.segcount_prefix_range();
        let mut counters = fs
            .db
            .scan(counter_start..counter_end)
            .await
            .expect("segcount scan");
        while let Some(item) = counters.next().await {
            let (key, value) = item.expect("segcount scan item");
            let Some((epoch, counter)) = codec.parse_segcount_key(&key) else {
                continue;
            };
            let segid = Segid::new(epoch, counter);
            let counter = SegmentCounter::decode(&value).expect("segcount decode");
            assert!(
                accounting.counters.insert(segid, counter).is_none(),
                "duplicate segcount row for {segid:?}"
            );
        }

        accounting
    }

    fn validate(&self) -> Result<(), SegmentAccountingViolations> {
        let mut violations = Vec::new();

        for (&segid, counter) in &self.counters {
            let referenced_bytes = self.referenced_bytes.get(&segid).copied().unwrap_or(0);
            if counter.live_bytes != referenced_bytes {
                violations.push(SegmentAccountingViolation::LiveBytesMismatch {
                    segid,
                    referenced_bytes,
                    recorded_live_bytes: counter.live_bytes,
                });
            }
            if counter.live_bytes > counter.appended_bytes {
                violations.push(SegmentAccountingViolation::LiveExceedsAppended {
                    segid,
                    live_bytes: counter.live_bytes,
                    appended_bytes: counter.appended_bytes,
                });
            }
        }

        for (&segid, &referenced_bytes) in &self.referenced_bytes {
            if !self.counters.contains_key(&segid) {
                violations.push(SegmentAccountingViolation::MissingCounter {
                    segid,
                    referenced_bytes,
                });
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(SegmentAccountingViolations(violations))
        }
    }
}

#[derive(Debug)]
enum SegmentAccountingViolation {
    MissingCounter {
        segid: Segid,
        referenced_bytes: u64,
    },
    LiveBytesMismatch {
        segid: Segid,
        referenced_bytes: u64,
        recorded_live_bytes: u64,
    },
    LiveExceedsAppended {
        segid: Segid,
        live_bytes: u64,
        appended_bytes: u64,
    },
}

impl fmt::Display for SegmentAccountingViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCounter {
                segid,
                referenced_bytes,
            } => write!(
                f,
                "{segid:?} has {referenced_bytes} referenced bytes but no counter row"
            ),
            Self::LiveBytesMismatch {
                segid,
                referenced_bytes,
                recorded_live_bytes,
            } => write!(
                f,
                "{segid:?} records {recorded_live_bytes} live bytes but FrameLocs reference \
                 {referenced_bytes}"
            ),
            Self::LiveExceedsAppended {
                segid,
                live_bytes,
                appended_bytes,
            } => write!(
                f,
                "{segid:?} records {live_bytes} live bytes but only {appended_bytes} appended bytes"
            ),
        }
    }
}

#[derive(Debug)]
struct SegmentAccountingViolations(Vec<SegmentAccountingViolation>);

impl fmt::Display for SegmentAccountingViolations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for violation in &self.0 {
            writeln!(f, "  - {violation}")?;
        }
        Ok(())
    }
}

pub(crate) struct Checks<'a> {
    fs: &'a ZeroFS,
    seed: u64,
    round: usize,
}

impl<'a> Checks<'a> {
    pub(crate) fn new(fs: &'a ZeroFS, seed: u64, round: usize) -> Self {
        Self { fs, seed, round }
    }

    pub(crate) async fn verify(&self) {
        self.verify_segment_accounting().await;
        self.verify_footprint().await;
    }

    async fn verify_segment_accounting(&self) {
        SegmentAccounting::load(self.fs)
            .await
            .validate()
            .unwrap_or_else(|violations| {
                panic!(
                    "segment accounting violations (seed {}, round {}):\n{}",
                    self.seed, self.round, violations
                )
            });
    }

    async fn verify_footprint(&self) {
        let scanned = self
            .fs
            .extent_store
            .sample_footprint()
            .await
            .expect("footprint scan");
        let gauges = self.fs.extent_store.segment_gc_stats();
        let gauged = SegmentFootprint {
            segment_count: gauges.segment_count.load(Relaxed),
            appended_bytes: gauges.appended_bytes.load(Relaxed),
            live_bytes: gauges.live_bytes.load(Relaxed),
            reclaimable_bytes: gauges.reclaimable_bytes.load(Relaxed),
        };
        assert_eq!(
            scanned, gauged,
            "footprint gauges diverged from authoritative scan (seed {}, round {})",
            self.seed, self.round
        );
    }
}
