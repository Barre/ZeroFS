//! Validated scalar values shared by the HA replication protocol.
//!
//! Zero represents an absent epoch, ship, or durable watermark. Constructors
//! validate values before they enter receiver state.

use std::num::NonZeroU64;

/// A data-db writer epoch observed on the replication protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WriterEpoch(NonZeroU64);

impl WriterEpoch {
    pub(crate) fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for WriterEpoch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(formatter)
    }
}

/// A term-local sequence number assigned to a shipped batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ShipSeqno(NonZeroU64);

impl ShipSeqno {
    pub(crate) fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl std::fmt::Display for ShipSeqno {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(formatter)
    }
}

/// A sequence number returned by SlateDB for a locally committed batch.
///
/// Zero is a valid initial durability frontier. This type is distinct from
/// term-local ship sequences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SlateDbSeqno(u64);

impl SlateDbSeqno {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Leader-provided durable frontier. `None` is wire value zero; a present
/// watermark must precede the batch carrying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PruneWatermark(Option<ShipSeqno>);

impl PruneWatermark {
    pub(crate) fn for_ship(value: u64, current: ShipSeqno) -> Option<Self> {
        let watermark = ShipSeqno::new(value);
        if watermark.is_some_and(|watermark| watermark >= current) {
            return None;
        }
        Some(Self(watermark))
    }

    pub(crate) const fn get(self) -> u64 {
        match self.0 {
            Some(value) => value.get(),
            None => 0,
        }
    }
}

/// Locally applied and durably flushed ship-attempt frontiers. The durable
/// frontier cannot exceed the applied frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoverageFrontier {
    applied_through: Option<ShipSeqno>,
    durable_through: Option<ShipSeqno>,
}

impl CoverageFrontier {
    pub(crate) const fn new(
        applied_through: Option<ShipSeqno>,
        durable_through: Option<ShipSeqno>,
    ) -> Option<Self> {
        match (applied_through, durable_through) {
            (None, Some(_)) => None,
            (Some(applied), Some(durable)) if durable.get() > applied.get() => None,
            _ => Some(Self {
                applied_through,
                durable_through,
            }),
        }
    }

    pub(crate) fn from_wire(applied_through: u64, durable_through: u64) -> Option<Self> {
        Self::new(
            ShipSeqno::new(applied_through),
            ShipSeqno::new(durable_through),
        )
    }

    pub(crate) const fn applied_through(self) -> Option<ShipSeqno> {
        self.applied_through
    }

    pub(crate) const fn durable_through(self) -> Option<ShipSeqno> {
        self.durable_through
    }
}

/// Solo history for a writer term and the number of Solo commits since its last
/// acknowledged ship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SoloHistory {
    Never,
    Ever { commits_since_last_ship: u64 },
}

impl SoloHistory {
    /// Validates an untyped `(count, latch)` representation.
    pub(crate) const fn from_parts(commits_since_last_ship: u64, ran_solo: bool) -> Option<Self> {
        match (commits_since_last_ship, ran_solo) {
            (0, false) => Some(Self::Never),
            (_, true) => Some(Self::Ever {
                commits_since_last_ship,
            }),
            (_, false) => None,
        }
    }

    pub(crate) const fn never() -> Self {
        Self::Never
    }

    pub(crate) const fn ever(commits_since_last_ship: u64) -> Self {
        Self::Ever {
            commits_since_last_ship,
        }
    }

    pub(crate) const fn commits_since_last_ship(self) -> u64 {
        match self {
            Self::Never => 0,
            Self::Ever {
                commits_since_last_ship,
            } => commits_since_last_ship,
        }
    }

    pub(crate) const fn ran_solo(self) -> bool {
        matches!(self, Self::Ever { .. })
    }
}

/// Durable writer-term provenance. Optional ship frontiers encode zero as
/// `None`; the applied frontier must cover every acknowledged ship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HaStamp {
    writer_epoch: WriterEpoch,
    last_shipped: Option<ShipSeqno>,
    solo_history: SoloHistory,
    applied_through: Option<ShipSeqno>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InvalidHaStamp {
    #[error("HA applied frontier {applied_through} is behind acknowledged ship {last_shipped}")]
    AppliedFrontierBehindShip {
        last_shipped: u64,
        applied_through: u64,
    },
    #[error(
        "HA applied frontier {applied_through} exceeds acknowledged ship {last_shipped} without \
         Solo/Ambiguous history"
    )]
    UnacknowledgedApplyWithoutSoloHistory {
        last_shipped: u64,
        applied_through: u64,
    },
}

impl HaStamp {
    pub(crate) fn new(
        writer_epoch: WriterEpoch,
        last_shipped: Option<ShipSeqno>,
        solo_history: SoloHistory,
        applied_through: Option<ShipSeqno>,
    ) -> Result<Self, InvalidHaStamp> {
        let last_shipped_value = last_shipped.map_or(0, ShipSeqno::get);
        let applied_through_value = applied_through.map_or(0, ShipSeqno::get);
        if applied_through_value < last_shipped_value {
            return Err(InvalidHaStamp::AppliedFrontierBehindShip {
                last_shipped: last_shipped_value,
                applied_through: applied_through_value,
            });
        }
        if applied_through_value > last_shipped_value && !solo_history.ran_solo() {
            return Err(InvalidHaStamp::UnacknowledgedApplyWithoutSoloHistory {
                last_shipped: last_shipped_value,
                applied_through: applied_through_value,
            });
        }
        Ok(Self {
            writer_epoch,
            last_shipped,
            solo_history,
            applied_through,
        })
    }

    pub(crate) const fn writer_epoch(self) -> WriterEpoch {
        self.writer_epoch
    }

    pub(crate) const fn last_shipped(self) -> Option<ShipSeqno> {
        self.last_shipped
    }

    pub(crate) const fn solo_history(self) -> SoloHistory {
        self.solo_history
    }

    pub(crate) const fn applied_through(self) -> Option<ShipSeqno> {
        self.applied_through
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_identifiers_are_nonzero() {
        assert!(WriterEpoch::new(0).is_none());
        assert_eq!(WriterEpoch::new(7).unwrap().get(), 7);
        assert!(ShipSeqno::new(0).is_none());
        assert_eq!(ShipSeqno::new(3).unwrap().get(), 3);
    }

    #[test]
    fn watermark_must_precede_its_ship() {
        let current = ShipSeqno::new(3).unwrap();
        assert_eq!(PruneWatermark::for_ship(0, current).unwrap().get(), 0);
        assert_eq!(PruneWatermark::for_ship(2, current).unwrap().get(), 2);
        assert!(PruneWatermark::for_ship(3, current).is_none());
        assert!(PruneWatermark::for_ship(4, current).is_none());
    }

    #[test]
    fn durable_coverage_cannot_outrun_applied_attempts() {
        assert_eq!(
            CoverageFrontier::from_wire(3, 2),
            CoverageFrontier::new(ShipSeqno::new(3), ShipSeqno::new(2))
        );
        assert!(CoverageFrontier::from_wire(0, 1).is_none());
        assert!(CoverageFrontier::from_wire(2, 3).is_none());
    }

    #[test]
    fn ha_stamp_rejects_an_applied_frontier_behind_an_acknowledged_ship() {
        let error = HaStamp::new(
            WriterEpoch::new(7).unwrap(),
            ShipSeqno::new(3),
            SoloHistory::never(),
            ShipSeqno::new(2),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "HA applied frontier 2 is behind acknowledged ship 3"
        );
    }

    #[test]
    fn ha_stamp_requires_solo_history_for_an_applied_unacknowledged_attempt() {
        let error = HaStamp::new(
            WriterEpoch::new(7).unwrap(),
            ShipSeqno::new(2),
            SoloHistory::never(),
            ShipSeqno::new(3),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "HA applied frontier 3 exceeds acknowledged ship 2 without Solo/Ambiguous history"
        );
    }

    #[test]
    fn solo_history_rejects_count_without_latch() {
        assert_eq!(SoloHistory::from_parts(0, false), Some(SoloHistory::Never));
        assert_eq!(SoloHistory::from_parts(2, false), None);
        assert_eq!(SoloHistory::from_parts(0, true), Some(SoloHistory::ever(0)));
        assert_eq!(SoloHistory::never().commits_since_last_ship(), 0);
        assert!(!SoloHistory::never().ran_solo());
        assert_eq!(SoloHistory::ever(0).commits_since_last_ship(), 0);
        assert!(SoloHistory::ever(0).ran_solo());
        assert_eq!(SoloHistory::ever(4).commits_since_last_ship(), 4);
    }
}
