//! The replication transport: the receiver-side role state machine and the
//! leader's gRPC sender.

use crate::dedup::DedupEntry;
use crate::replication::tail::{ReplOp, TailBuffer};
use crate::replication::types::{CoverageFrontier, PruneWatermark, ShipSeqno, WriterEpoch};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify, watch};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("zerofs.replication");
}

use proto::replication_service_client::ReplicationServiceClient;
use proto::replication_service_server::{ReplicationService, ReplicationServiceServer};
use proto::{
    DedupEntry as ProtoDedupEntry, HeartbeatRequest, HeartbeatResponse, HelloRequest,
    HelloResponse, ReplOp as ProtoOp, ReplicateDisposition, ReplicateRequest, ReplicateResponse,
};

/// Timeout for gRPC connection establishment, including the HTTP/2 handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Timeout for unary control RPCs.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// Delay after a heartbeat transport failure.
const HEARTBEAT_RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// Decode ceiling for an incoming `ReplicateRequest`. tonic defaults to 4 MiB.
const MAX_SHIP_DECODE_BYTES: usize = usize::MAX;
/// Version 2 uses unary heartbeats with exact-epoch acknowledgements. Version 1
/// used an unacknowledged client stream; zero is the protobuf default.
pub(crate) const HA_PROTOCOL_VERSION: u32 = 2;
/// Terminal liveness value after an incompatible heartbeat.
pub(crate) const INCOMPATIBLE_HEARTBEAT_LIVENESS: u64 = u64::MAX;

/// One decoded replication batch at the message boundary.
struct IncomingShip {
    seqno: ShipSeqno,
    ops: Vec<ReplOp>,
    dedup_entries: Vec<DedupEntry>,
    prune_watermark: PruneWatermark,
    writer_epoch: WriterEpoch,
}

/// Sender-facing result of a replication ship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShipResult {
    /// The batch entered the standby tail.
    Accepted,
    /// The sender epoch is not above the receiver's local or observed epoch.
    SenderDeposed,
    /// The receiver cannot account for every earlier ship in this writer term.
    BaseNotCovered,
}

/// Receiver-local outcome, including receiver staleness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShipDisposition {
    Result(ShipResult),
    /// The sender is newer than this process's local writer epoch.
    ReceiverStale {
        local_epoch: u64,
        incoming_epoch: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeartbeatDisposition {
    Covered,
    Ignored,
    BaseNotCovered,
}

const HEARTBEAT_BASE_NOT_COVERED: &str = "HA receiver base is not covered";
const HEARTBEAT_PROTOCOL_INCOMPATIBLE: &str = "HA heartbeat protocol is incompatible";

#[derive(Debug, thiserror::Error)]
#[error(
    "peer answered Hello with HA protocol version {received}; this binary requires version \
     {required}"
)]
struct IncompatibleHelloProtocol {
    received: u32,
    required: u32,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HeartbeatExit {
    #[error("receiver heartbeat base is not covered")]
    BaseNotCovered(#[source] Status),
    #[error("heartbeat protocol is incompatible")]
    ProtocolIncompatible(#[source] Status),
}

impl HeartbeatExit {
    fn from_status(status: Status) -> Result<Self, Status> {
        if status.code() == tonic::Code::FailedPrecondition {
            match status.message() {
                HEARTBEAT_BASE_NOT_COVERED => return Ok(Self::BaseNotCovered(status)),
                message if message.starts_with(HEARTBEAT_PROTOCOL_INCOMPATIBLE) => {
                    return Ok(Self::ProtocolIncompatible(status));
                }
                _ => {}
            }
        }
        Err(status)
    }
}

fn ship_rpc_response(disposition: ShipDisposition) -> Result<Response<ReplicateResponse>, Status> {
    match disposition {
        ShipDisposition::Result(result) => {
            let disposition = match result {
                ShipResult::Accepted => ReplicateDisposition::Accepted,
                ShipResult::SenderDeposed => ReplicateDisposition::SenderDeposed,
                ShipResult::BaseNotCovered => ReplicateDisposition::BaseNotCovered,
            };
            Ok(Response::new(ReplicateResponse {
                disposition: disposition.into(),
            }))
        }
        ShipDisposition::ReceiverStale {
            local_epoch,
            incoming_epoch,
        } => Err(Status::unavailable(format!(
            "receiver is a stale writer at epoch {local_epoch}; incoming writer \
             epoch is {incoming_epoch}"
        ))),
    }
}

struct StandbyState {
    observed_epoch: u64,
    tail: TailBuffer,
    /// Shares the phase lock with heartbeat admission.
    heartbeat_acks_enabled: bool,
}

struct FencedState {
    writer_epoch: u64,
    /// Highest peer epoch observed after acquiring the local writer.
    observed_epoch: u64,
}

impl FencedState {
    fn observe(&mut self, core: &ReceiverCore, incoming_epoch: u64) -> bool {
        self.observed_epoch = self.observed_epoch.max(incoming_epoch);
        core.publish_superseding_epoch(self.writer_epoch, incoming_epoch);
        incoming_epoch > self.writer_epoch
    }

    fn ship_disposition(&mut self, core: &ReceiverCore, incoming_epoch: u64) -> ShipDisposition {
        if !self.observe(core, incoming_epoch) {
            return ShipDisposition::Result(ShipResult::SenderDeposed);
        }
        ShipDisposition::ReceiverStale {
            local_epoch: self.writer_epoch,
            incoming_epoch,
        }
    }
}

enum ReceiverPhase {
    Standby(StandbyState),
    /// Local writer acquired; takeover reconciliation owns the detached tail.
    /// Newer peer epochs remain recorded for completion fencing.
    Promoting(FencedState),
    Leading(FencedState),
}

impl ReceiverPhase {
    fn standby(observed_epoch: u64, tail: TailBuffer, heartbeat_acks_enabled: bool) -> Self {
        Self::Standby(StandbyState {
            observed_epoch,
            tail,
            heartbeat_acks_enabled,
        })
    }

    fn promotion(&self, writer_epoch: u64) -> anyhow::Result<&FencedState> {
        match self {
            Self::Promoting(state) if state.writer_epoch == writer_epoch => Ok(state),
            Self::Standby(_) => anyhow::bail!("receiver returned to standby during promotion"),
            Self::Promoting(state) | Self::Leading(state) => anyhow::bail!(
                "receiver fenced at epoch {}; cannot complete promotion at epoch {writer_epoch}",
                state.writer_epoch
            ),
        }
    }
}

struct ReceiverCore {
    phase: Mutex<ReceiverPhase>,
    dedup: Arc<crate::dedup::DedupCache>,
    /// Highest peer epoch superseding the local runtime.
    superseding_epoch: watch::Sender<u64>,
    /// Serving lease revoked synchronously when an RPC observes a newer epoch.
    serving_lease: std::sync::Mutex<Option<(u64, std::sync::Weak<crate::replication::Lease>)>>,
}

impl ReceiverCore {
    fn publish_superseding_epoch(&self, local_epoch: u64, incoming_epoch: u64) {
        if incoming_epoch <= local_epoch {
            return;
        }
        self.superseding_epoch
            .send_modify(|observed| *observed = (*observed).max(incoming_epoch));
        let lease = self
            .serving_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|(epoch, lease)| {
                (incoming_epoch > *epoch).then(|| lease.upgrade()).flatten()
            });
        if let Some(lease) = lease {
            lease.revoke();
        }
    }
}

/// Handle for atomically freezing a receiver's standby state during promotion.
#[derive(Clone)]
pub struct ReceiverControl {
    core: Arc<ReceiverCore>,
}

/// Tail and observed epoch detached by one promotion transition.
#[must_use = "a promotion snapshot must be reconciled before serving"]
pub struct PromotionSnapshot {
    control: ReceiverControl,
    writer_epoch: u64,
    observed_epoch: u64,
    tail: TailBuffer,
}

/// The receiver observed a writer newer than the local promotion.
#[derive(Debug, thiserror::Error)]
#[error(
    "receiver observed newer writer epoch {observed_epoch}; local promotion epoch is {writer_epoch}"
)]
pub(crate) struct PromotionSuperseded {
    pub(crate) writer_epoch: u64,
    pub(crate) observed_epoch: u64,
}

impl ReceiverControl {
    /// Subscribes to the highest peer epoch superseding this runtime.
    // Binary-only API.
    #[allow(dead_code)]
    pub(crate) fn superseding_epochs(&self) -> watch::Receiver<u64> {
        self.core.superseding_epoch.subscribe()
    }

    /// Stops heartbeat acknowledgements before durable takeover. Ship admission
    /// remains enabled.
    #[allow(dead_code)] // Called by the binary-only failover supervisor.
    pub(crate) async fn quiesce_heartbeat_acks(&self) {
        let mut phase = self.core.phase.lock().await;
        if let ReceiverPhase::Standby(standby) = &mut *phase {
            standby.heartbeat_acks_enabled = false;
        }
    }

    /// Rearms heartbeat acknowledgements after returning to role election.
    #[allow(dead_code)] // Called by the binary-only failover supervisor.
    pub(crate) async fn resume_heartbeat_acks(&self) {
        let mut phase = self.core.phase.lock().await;
        if let ReceiverPhase::Standby(standby) = &mut *phase {
            standby.heartbeat_acks_enabled = true;
        }
    }

    /// Attaches the serving lease and applies previously observed supersession.
    #[allow(dead_code)]
    pub(crate) fn attach_serving_lease(
        &self,
        writer_epoch: u64,
        lease: &Arc<crate::replication::Lease>,
    ) {
        *self
            .core
            .serving_lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((writer_epoch, Arc::downgrade(lease)));
        if *self.core.superseding_epoch.borrow() > writer_epoch {
            lease.revoke();
        }
    }

    /// Fence receiver admission and take ownership of the standby tail.
    pub async fn begin_promotion(&self, writer_epoch: u64) -> anyhow::Result<PromotionSnapshot> {
        anyhow::ensure!(writer_epoch > 0, "receiver writer epoch must be nonzero");
        let mut phase = self.core.phase.lock().await;
        match &*phase {
            ReceiverPhase::Standby(standby) => {
                if standby.observed_epoch > writer_epoch {
                    return Err(PromotionSuperseded {
                        writer_epoch,
                        observed_epoch: standby.observed_epoch,
                    }
                    .into());
                }
            }
            ReceiverPhase::Promoting(state) | ReceiverPhase::Leading(state) => anyhow::bail!(
                "receiver already fenced at writer epoch {}; cannot promote \
                 at epoch {writer_epoch}",
                state.writer_epoch
            ),
        }
        let ReceiverPhase::Standby(standby) = std::mem::replace(
            &mut *phase,
            ReceiverPhase::Promoting(FencedState {
                writer_epoch,
                observed_epoch: writer_epoch,
            }),
        ) else {
            unreachable!("standby phase checked above")
        };
        Ok(PromotionSnapshot {
            control: self.clone(),
            writer_epoch,
            observed_epoch: standby.observed_epoch,
            tail: standby.tail,
        })
    }

    async fn complete_promotion(
        &self,
        writer_epoch: u64,
        snapshot_observed_epoch: u64,
        tail: TailBuffer,
    ) -> anyhow::Result<u64> {
        let mut phase = self.core.phase.lock().await;
        let observed_epoch = phase
            .promotion(writer_epoch)?
            .observed_epoch
            .max(snapshot_observed_epoch);
        if observed_epoch > writer_epoch {
            // Keep acknowledgements quiesced until role election restarts.
            *phase = ReceiverPhase::standby(observed_epoch, tail, false);
            return Err(PromotionSuperseded {
                writer_epoch,
                observed_epoch,
            }
            .into());
        }
        *phase = ReceiverPhase::Leading(FencedState {
            writer_epoch,
            observed_epoch,
        });
        Ok(writer_epoch)
    }

    /// Returns a failed promotion's tail to receiver admission. `observed_epoch`
    /// may include a database fence not observed over replication.
    pub(crate) async fn return_to_standby(
        &self,
        writer_epoch: u64,
        snapshot_observed_epoch: u64,
        observed_epoch: u64,
        tail: TailBuffer,
    ) -> anyhow::Result<u64> {
        let mut phase = self.core.phase.lock().await;
        let observed_epoch = observed_epoch
            .max(snapshot_observed_epoch)
            .max(phase.promotion(writer_epoch)?.observed_epoch);
        // Returning the tail does not restart Claiming grace; ACKs remain quiesced.
        *phase = ReceiverPhase::standby(observed_epoch, tail, false);
        Ok(observed_epoch)
    }

    /// Demotes an unexposed local leader to an empty, quiesced standby.
    #[allow(dead_code)]
    pub(crate) async fn demote_leader(
        &self,
        writer_epoch: u64,
        observed_epoch: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            observed_epoch >= writer_epoch,
            "receiver demotion epoch {observed_epoch} is below local writer epoch {writer_epoch}"
        );
        let mut phase = self.core.phase.lock().await;
        self.core
            .publish_superseding_epoch(writer_epoch, observed_epoch);
        match &mut *phase {
            ReceiverPhase::Leading(state) if state.writer_epoch == writer_epoch => {
                let observed_epoch = observed_epoch.max(state.observed_epoch);
                *phase = ReceiverPhase::standby(observed_epoch, TailBuffer::new(), false);
                Ok(())
            }
            ReceiverPhase::Standby(standby) => {
                standby.observed_epoch = standby.observed_epoch.max(observed_epoch);
                Ok(())
            }
            ReceiverPhase::Promoting(state) | ReceiverPhase::Leading(state) => anyhow::bail!(
                "receiver fenced at epoch {}; cannot demote writer epoch {writer_epoch}",
                state.writer_epoch
            ),
        }
    }

    #[cfg(test)]
    pub(crate) async fn inspect_standby_for_tests<R>(
        &self,
        inspect: impl FnOnce(&TailBuffer, u64) -> R,
    ) -> R {
        let phase = self.core.phase.lock().await;
        let ReceiverPhase::Standby(standby) = &*phase else {
            panic!("receiver is no longer a standby")
        };
        inspect(&standby.tail, standby.observed_epoch)
    }

    #[cfg(test)]
    pub(crate) async fn inspect_phase_epochs_for_tests(&self) -> (&'static str, u64, u64) {
        let phase = self.core.phase.lock().await;
        match &*phase {
            ReceiverPhase::Standby(standby) => ("standby", 0, standby.observed_epoch),
            ReceiverPhase::Promoting(state) => {
                ("promoting", state.writer_epoch, state.observed_epoch)
            }
            ReceiverPhase::Leading(state) => ("leading", state.writer_epoch, state.observed_epoch),
        }
    }
}

impl PromotionSnapshot {
    pub(crate) fn observed_epoch(&self) -> u64 {
        self.observed_epoch
    }

    pub(crate) fn tail_mut(&mut self) -> &mut TailBuffer {
        &mut self.tail
    }

    pub(crate) fn dedup(&self) -> Arc<crate::dedup::DedupCache> {
        self.control.core.dedup.clone()
    }

    pub(crate) fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Restores the snapshot after local writer closure.
    pub(crate) async fn return_to_standby(self, observed_epoch: u64) -> anyhow::Result<u64> {
        let Self {
            control,
            writer_epoch,
            observed_epoch: snapshot_observed_epoch,
            tail,
        } = self;
        control
            .return_to_standby(writer_epoch, snapshot_observed_epoch, observed_epoch, tail)
            .await
    }

    pub(crate) async fn complete(self) -> anyhow::Result<u64> {
        let Self {
            control,
            writer_epoch,
            observed_epoch,
            tail,
        } = self;
        control
            .complete_promotion(writer_epoch, observed_epoch, tail)
            .await
    }
}

fn to_repl_ops(ops: Vec<ProtoOp>) -> Vec<ReplOp> {
    ops.into_iter()
        .map(|o| {
            if o.delete {
                ReplOp::Delete(Bytes::from(o.key))
            } else if !o.frame.is_empty() {
                ReplOp::PutFrame(
                    Bytes::from(o.key),
                    Bytes::from(o.value),
                    Bytes::from(o.frame),
                )
            } else {
                ReplOp::Put(Bytes::from(o.key), Bytes::from(o.value))
            }
        })
        .collect()
}

fn to_proto_ops(ops: &[ReplOp]) -> Vec<ProtoOp> {
    ops.iter()
        .map(|o| match o {
            ReplOp::Put(k, v) => ProtoOp {
                key: k.to_vec(),
                value: v.to_vec(),
                delete: false,
                frame: Vec::new(),
            },
            ReplOp::PutFrame(k, v, f) => ProtoOp {
                key: k.to_vec(),
                value: v.to_vec(),
                delete: false,
                frame: f.to_vec(),
            },
            ReplOp::Delete(k) => ProtoOp {
                key: k.to_vec(),
                value: Vec::new(),
                delete: true,
                frame: Vec::new(),
            },
        })
        .collect()
}

fn decode_dedup_entries(entries: Vec<ProtoDedupEntry>) -> Result<Vec<DedupEntry>, Status> {
    entries
        .into_iter()
        .map(|entry| {
            DedupEntry::from_wire(&entry.op_id, &entry.result).map_err(|error| {
                Status::invalid_argument(format!("invalid replication dedup entry: {error}"))
            })
        })
        .collect()
}

fn encode_dedup_entries(entries: &[DedupEntry]) -> anyhow::Result<Vec<ProtoDedupEntry>> {
    let mut encoded_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let (op_id, result) = entry.to_wire_parts()?;
        encoded_entries.push(ProtoDedupEntry { op_id, result });
    }
    Ok(encoded_entries)
}

/// Receives replication into a term-fenced standby tail.
pub struct ReplicationReceiver {
    core: Arc<ReceiverCore>,
    /// Accepted-heartbeat counter. The reserved maximum value latches protocol
    /// incompatibility.
    heartbeats: watch::Sender<u64>,
    /// Signals Hello from a peer while this standby holds a covered tail.
    takeover_trigger: Option<Arc<Notify>>,
    /// Node identity returned by Hello.
    node_id: String,
    /// Test-only gate between the fast checks and the phase lock.
    #[cfg(test)]
    before_phase_lock_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
    /// Test-only gate after heartbeat admission acquires the phase lock.
    #[cfg(test)]
    heartbeat_phase_lock_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
    /// Test gate after fencing and before append.
    #[cfg(test)]
    before_append_pause: Option<(u64, Arc<Notify>, Arc<Notify>)>,
}

impl ReplicationReceiver {
    pub fn new(
        dedup: Arc<crate::dedup::DedupCache>,
        takeover_trigger: Option<Arc<Notify>>,
        node_id: String,
    ) -> Self {
        dedup.start_expiry_reaper();
        Self {
            core: Arc::new(ReceiverCore {
                phase: Mutex::new(ReceiverPhase::standby(0, TailBuffer::new(), true)),
                dedup,
                superseding_epoch: watch::channel(0u64).0,
                serving_lease: std::sync::Mutex::new(None),
            }),
            heartbeats: watch::channel(0u64).0,
            takeover_trigger,
            node_id,
            #[cfg(test)]
            before_phase_lock_pause: None,
            #[cfg(test)]
            heartbeat_phase_lock_pause: None,
            #[cfg(test)]
            before_append_pause: None,
        }
    }

    #[cfg(test)]
    fn pause_epoch_before_phase_lock(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.before_phase_lock_pause = Some((epoch, reached, resume));
        self
    }

    #[cfg(test)]
    fn pause_heartbeat_with_phase_lock(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.heartbeat_phase_lock_pause = Some((epoch, reached, resume));
        self
    }

    #[cfg(test)]
    pub(crate) fn pause_epoch_before_append(
        mut self,
        epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> Self {
        self.before_append_pause = Some((epoch, reached, resume));
        self
    }

    /// Subscribe before [`Self::into_server`] consumes the receiver.
    pub fn liveness(&self) -> watch::Receiver<u64> {
        self.heartbeats.subscribe()
    }

    /// Returns the promotion control handle.
    pub fn control(&self) -> ReceiverControl {
        ReceiverControl {
            core: self.core.clone(),
        }
    }

    /// Applies fencing, tail mutation, pruning, and dedup publication under the
    /// receiver phase lock.
    async fn receive_ship(&self, ship: IncomingShip) -> ShipDisposition {
        let IncomingShip {
            seqno,
            ops,
            dedup_entries,
            prune_watermark,
            writer_epoch,
        } = ship;
        let writer_epoch_value = writer_epoch.get();

        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.before_phase_lock_pause
            && writer_epoch_value == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        let mut phase = self.core.phase.lock().await;
        match &mut *phase {
            ReceiverPhase::Standby(standby) => {
                if writer_epoch_value < standby.observed_epoch {
                    return ShipDisposition::Result(ShipResult::SenderDeposed);
                }
                standby.observed_epoch = standby.observed_epoch.max(writer_epoch_value);
                if !standby
                    .tail
                    .covers_predecessors(writer_epoch, seqno, prune_watermark)
                {
                    // Record the predecessor frontier from this pre-append rejection.
                    let predecessor = ShipSeqno::new(seqno.get() - 1)
                        .expect("an uncovered ship must have a predecessor");
                    let frontier = CoverageFrontier::new(
                        Some(predecessor),
                        ShipSeqno::new(prune_watermark.get()),
                    )
                    .expect("a validated ship watermark cannot exceed its predecessor");
                    let (_, durable_entries) =
                        standby.tail.observe_coverage(writer_epoch, frontier);
                    standby
                        .tail
                        .publish_durable(&self.core.dedup, durable_entries);
                    return ShipDisposition::Result(ShipResult::BaseNotCovered);
                }
                #[cfg(test)]
                if let Some((epoch, reached, resume)) = &self.before_append_pause
                    && writer_epoch_value == *epoch
                {
                    reached.notify_one();
                    resume.notified().await;
                }
                // Tail epoch, not observed heartbeat epoch, controls replacement.
                standby
                    .tail
                    .accept_validated(writer_epoch, seqno, ops, dedup_entries);
                if prune_watermark.get() > 0 {
                    let durable_entries = standby.tail.prune(prune_watermark.get());
                    standby
                        .tail
                        .publish_durable(&self.core.dedup, durable_entries);
                }
                ShipDisposition::Result(ShipResult::Accepted)
            }
            ReceiverPhase::Promoting(state) | ReceiverPhase::Leading(state) => {
                state.ship_disposition(&self.core, writer_epoch_value)
            }
        }
    }

    /// Applies heartbeat epoch and coverage under the receiver phase lock.
    async fn receive_heartbeat(
        &self,
        writer_epoch: WriterEpoch,
        frontier: CoverageFrontier,
    ) -> HeartbeatDisposition {
        let writer_epoch_value = writer_epoch.get();
        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.before_phase_lock_pause
            && writer_epoch_value == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        let mut phase = self.core.phase.lock().await;
        #[cfg(test)]
        if let Some((epoch, reached, resume)) = &self.heartbeat_phase_lock_pause
            && writer_epoch_value == *epoch
        {
            reached.notify_one();
            resume.notified().await;
        }
        match &mut *phase {
            ReceiverPhase::Standby(standby) if !standby.heartbeat_acks_enabled => {
                HeartbeatDisposition::Ignored
            }
            ReceiverPhase::Standby(standby) if writer_epoch_value >= standby.observed_epoch => {
                standby.observed_epoch = writer_epoch_value;
                let (covered, durable_entries) =
                    standby.tail.observe_coverage(writer_epoch, frontier);
                standby
                    .tail
                    .publish_durable(&self.core.dedup, durable_entries);
                if covered {
                    HeartbeatDisposition::Covered
                } else {
                    HeartbeatDisposition::BaseNotCovered
                }
            }
            ReceiverPhase::Promoting(state) | ReceiverPhase::Leading(state) => {
                // Record supersession without acknowledging from a fenced phase.
                state.observe(&self.core, writer_epoch_value);
                HeartbeatDisposition::Ignored
            }
            ReceiverPhase::Standby(_) => HeartbeatDisposition::Ignored,
        }
    }

    /// Validates a heartbeat before updating coverage and liveness.
    async fn process_heartbeat(
        &self,
        protocol_version: u32,
        writer_epoch: u64,
        applied_through: u64,
        durable_through: u64,
    ) -> Result<bool, Status> {
        if protocol_version != HA_PROTOCOL_VERSION {
            let newly_latched = self.heartbeats.send_if_modified(|liveness| {
                let newly_latched = *liveness != INCOMPATIBLE_HEARTBEAT_LIVENESS;
                *liveness = INCOMPATIBLE_HEARTBEAT_LIVENESS;
                newly_latched
            });
            if newly_latched {
                tracing::error!(
                    "incompatible HA heartbeat protocol; received={protocol_version}; \
                     required={HA_PROTOCOL_VERSION}; takeover disabled"
                );
            }
            return Err(Status::failed_precondition(format!(
                "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: received version {protocol_version}, \
                 required {HA_PROTOCOL_VERSION}"
            )));
        }
        if *self.heartbeats.borrow() == INCOMPATIBLE_HEARTBEAT_LIVENESS {
            return Err(Status::failed_precondition(format!(
                "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: an incompatible heartbeat was already \
                 observed; restart after upgrading both peers"
            )));
        }
        let writer_epoch = WriterEpoch::new(writer_epoch)
            .ok_or_else(|| Status::invalid_argument("writer_epoch must be nonzero"))?;
        let frontier =
            CoverageFrontier::from_wire(applied_through, durable_through).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "durable_through {durable_through} must not exceed applied_through \
                     {applied_through}"
                ))
            })?;
        match self.receive_heartbeat(writer_epoch, frontier).await {
            HeartbeatDisposition::Covered => {
                let ticked = self.heartbeats.send_if_modified(|liveness| {
                    if *liveness == INCOMPATIBLE_HEARTBEAT_LIVENESS {
                        return false;
                    }
                    *liveness = match *liveness {
                        tick if tick == INCOMPATIBLE_HEARTBEAT_LIVENESS - 1 => 1,
                        tick => tick + 1,
                    };
                    true
                });
                if !ticked && *self.heartbeats.borrow() == INCOMPATIBLE_HEARTBEAT_LIVENESS {
                    return Err(Status::failed_precondition(format!(
                        "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: an incompatible heartbeat raced \
                        this request; restart after upgrading both peers"
                    )));
                }
                Ok(true)
            }
            HeartbeatDisposition::Ignored => Ok(false),
            HeartbeatDisposition::BaseNotCovered => {
                Err(Status::failed_precondition(HEARTBEAT_BASE_NOT_COVERED))
            }
        }
    }

    pub fn into_server(self) -> ReplicationServiceServer<Self> {
        ReplicationServiceServer::new(self).max_decoding_message_size(MAX_SHIP_DECODE_BYTES)
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationReceiver {
    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        let writer_epoch = WriterEpoch::new(req.writer_epoch)
            .ok_or_else(|| Status::invalid_argument("writer_epoch must be nonzero"))?;
        let seqno = ShipSeqno::new(req.seqno)
            .ok_or_else(|| Status::invalid_argument("seqno must be nonzero"))?;
        let prune_watermark =
            PruneWatermark::for_ship(req.prune_watermark, seqno).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "prune_watermark {} must be below current seqno {}",
                    req.prune_watermark, req.seqno
                ))
            })?;
        let dedup_entries = decode_dedup_entries(req.dedup_entries)?;
        let ship = IncomingShip {
            seqno,
            ops: to_repl_ops(req.ops),
            dedup_entries,
            prune_watermark,
            writer_epoch,
        };
        ship_rpc_response(self.receive_ship(ship).await)
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let beat = request.into_inner();
        // Only an accepted standby heartbeat returns a nonzero epoch.
        let accepted = self
            .process_heartbeat(
                beat.protocol_version,
                beat.writer_epoch,
                beat.applied_through,
                beat.durable_through,
            )
            .await?;
        Ok(Response::new(HeartbeatResponse {
            accepted_writer_epoch: if accepted { beat.writer_epoch } else { 0 },
            protocol_version: HA_PROTOCOL_VERSION,
        }))
    }

    async fn hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        let protocol_version = request.into_inner().protocol_version;
        if protocol_version != HA_PROTOCOL_VERSION {
            tracing::warn!(
                "incompatible HA Hello protocol; received={protocol_version}; \
                 required={HA_PROTOCOL_VERSION}; reporting active"
            );
            return Ok(Response::new(HelloResponse {
                peer_active: true,
                node_id: self.node_id.clone(),
                protocol_version: HA_PROTOCOL_VERSION,
            }));
        }
        let (peer_active, trigger_takeover) = {
            let phase = self.core.phase.lock().await;
            match &*phase {
                ReceiverPhase::Standby(standby) => {
                    let has_tail = !standby.tail.is_empty();
                    let has_uncovered_lineage =
                        standby.tail.has_uncovered_lineage(standby.observed_epoch);
                    (
                        has_tail || has_uncovered_lineage,
                        has_tail && !has_uncovered_lineage,
                    )
                }
                ReceiverPhase::Promoting(_) => (true, false),
                ReceiverPhase::Leading(state) => {
                    (state.observed_epoch <= state.writer_epoch, false)
                }
            }
        };
        // Only a covered nonempty tail triggers immediate takeover.
        if trigger_takeover && let Some(trigger) = &self.takeover_trigger {
            trigger.notify_one();
        }
        Ok(Response::new(HelloResponse {
            peer_active,
            node_id: self.node_id.clone(),
            protocol_version: HA_PROTOCOL_VERSION,
        }))
    }
}

/// Sends replication batches and returns the peer disposition.
pub struct ReplicationSender {
    client: Mutex<ReplicationServiceClient<Channel>>,
}

impl ReplicationSender {
    pub async fn connect(endpoint: String) -> anyhow::Result<Self> {
        let client =
            tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
                .await
                .map_err(|_| anyhow::anyhow!("replication dial timed out"))??;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    pub(crate) async fn ship(
        &self,
        seqno: ShipSeqno,
        ops: &[ReplOp],
        dedup_entries: &[crate::dedup::DedupEntry],
        prune_watermark: PruneWatermark,
        writer_epoch: WriterEpoch,
    ) -> anyhow::Result<ShipResult> {
        let dedup_entries = encode_dedup_entries(dedup_entries)?;
        let req = ReplicateRequest {
            seqno: seqno.get(),
            ops: to_proto_ops(ops),
            prune_watermark: prune_watermark.get(),
            writer_epoch: writer_epoch.get(),
            dedup_entries,
        };
        let resp = self.client.lock().await.replicate(req).await?;
        let disposition = ReplicateDisposition::try_from(resp.into_inner().disposition)
            .map_err(|value| anyhow::anyhow!("unknown replicate disposition {value}"))?;
        match disposition {
            ReplicateDisposition::Accepted => Ok(ShipResult::Accepted),
            ReplicateDisposition::SenderDeposed => Ok(ShipResult::SenderDeposed),
            ReplicateDisposition::BaseNotCovered => Ok(ShipResult::BaseNotCovered),
            ReplicateDisposition::Unspecified => {
                anyhow::bail!("peer omitted the replicate disposition")
            }
        }
    }
}

/// Hello response used for startup role election. `peer_active` covers
/// Promoting, Leading, and retained-tail states.
#[derive(Debug)]
pub struct HelloAnswer {
    pub peer_active: bool,
    pub node_id: String,
}

fn decode_hello_answer(resp: HelloResponse) -> anyhow::Result<HelloAnswer> {
    if resp.protocol_version != HA_PROTOCOL_VERSION {
        return Err(anyhow::Error::new(IncompatibleHelloProtocol {
            received: resp.protocol_version,
            required: HA_PROTOCOL_VERSION,
        }));
    }
    anyhow::ensure!(
        !resp.node_id.trim().is_empty(),
        "Hello peer returned an empty node_id"
    );
    Ok(HelloAnswer {
        peer_active: resp.peer_active,
        node_id: resp.node_id,
    })
}

/// Returns whether Hello completed with a protocol-version mismatch.
#[allow(dead_code)] // Used by the binary CLI; the library target has no role election.
pub(crate) fn hello_protocol_incompatible(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<IncompatibleHelloProtocol>().is_some())
}

pub async fn hello_peer(endpoint: String) -> anyhow::Result<HelloAnswer> {
    let mut client =
        tokio::time::timeout(CONNECT_TIMEOUT, ReplicationServiceClient::connect(endpoint))
            .await
            .map_err(|_| anyhow::anyhow!("Hello dial timed out"))??;
    let resp = tokio::time::timeout(
        RPC_TIMEOUT,
        client.hello(HelloRequest {
            protocol_version: HA_PROTOCOL_VERSION,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Hello RPC timed out"))??;
    decode_hello_answer(resp.into_inner())
}

/// Sends epoch-acknowledged heartbeats at `interval`. Transport failures
/// reconnect; missing base coverage and protocol incompatibility return
/// terminal session results to the caller.
#[allow(dead_code)] // Used by the binary CLI; library tests exercise it directly.
pub(crate) async fn run_heartbeat_sender(
    endpoint: String,
    epoch: WriterEpoch,
    interval: Duration,
    control: Arc<crate::replication::replicator::ReplicationControl>,
) -> HeartbeatExit {
    loop {
        match run_heartbeat_session_once(&endpoint, epoch, interval, control.clone()).await {
            Ok(exit) => return exit,
            Err(error) => {
                tracing::debug!(
                    "HA heartbeat connection ended; reconnect_delay_s={}; error={error:#}",
                    HEARTBEAT_RECONNECT_DELAY.as_secs()
                );
            }
        }
        tokio::time::sleep(HEARTBEAT_RECONNECT_DELAY).await;
    }
}

async fn run_heartbeat_session_once(
    endpoint: &str,
    epoch: WriterEpoch,
    interval: Duration,
    control: Arc<crate::replication::replicator::ReplicationControl>,
) -> anyhow::Result<HeartbeatExit> {
    let mut client = tokio::time::timeout(
        CONNECT_TIMEOUT,
        ReplicationServiceClient::connect(endpoint.to_owned()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("heartbeat dial timed out"))??;
    let mut heartbeats = tokio::time::interval(interval);
    heartbeats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // The first tick is immediate; missed ticks are skipped without overlap.
        heartbeats.tick().await;
        let frontier = control.coverage_frontier();
        let request = HeartbeatRequest {
            writer_epoch: epoch.get(),
            applied_through: frontier.applied_through().map_or(0, ShipSeqno::get),
            durable_through: frontier.durable_through().map_or(0, ShipSeqno::get),
            protocol_version: HA_PROTOCOL_VERSION,
        };
        let started = tokio::time::Instant::now();
        let response = match tokio::time::timeout(RPC_TIMEOUT, client.heartbeat(request)).await {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                return match HeartbeatExit::from_status(status) {
                    Ok(exit) => Ok(exit),
                    Err(status) => Err(status.into()),
                };
            }
            Err(_) => anyhow::bail!("heartbeat RPC timed out"),
        };
        if response.protocol_version != HA_PROTOCOL_VERSION {
            return Ok(HeartbeatExit::ProtocolIncompatible(
                Status::failed_precondition(format!(
                    "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: response version {}, required {}",
                    response.protocol_version, HA_PROTOCOL_VERSION
                )),
            ));
        }
        match response.accepted_writer_epoch {
            accepted if accepted == epoch.get() => control.acknowledge_heartbeat(epoch, started),
            0 => {}
            accepted => {
                return Ok(HeartbeatExit::ProtocolIncompatible(
                    Status::failed_precondition(format!(
                        "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: response acknowledged writer epoch \
                         {accepted}, request used {}",
                        epoch.get()
                    )),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use std::time::Instant;
    use tokio::net::TcpListener;

    /// Hello response decoded by an unversioned peer.
    #[derive(Clone, PartialEq, Message)]
    struct LegacyHelloResponse {
        #[prost(bool, tag = "1")]
        peer_active: bool,
    }

    fn new_receiver(dedup: Arc<crate::dedup::DedupCache>) -> ReplicationReceiver {
        ReplicationReceiver::new(dedup, None, "standby-under-test".to_string())
    }

    fn heartbeat_control(
        writer_epoch: u64,
    ) -> Arc<crate::replication::replicator::ReplicationControl> {
        let (_sequencer, control) = crate::replication::replicator::Replicator::new(
            "unused-test-peer".to_string(),
            test_writer_epoch(writer_epoch),
        );
        control
    }

    fn test_writer_epoch(writer_epoch: u64) -> WriterEpoch {
        WriterEpoch::new(writer_epoch).expect("test writer epoch must be nonzero")
    }

    /// Test service that fails its first heartbeat RPC.
    struct FailFirstHeartbeat {
        inner: ReplicationReceiver,
        status: Status,
        process_first_beat: bool,
        heartbeat_calls: Arc<AtomicU64>,
        first_failure: Arc<Notify>,
    }

    /// Test service that delays heartbeat responses.
    struct DelayedHeartbeat {
        inner: ReplicationReceiver,
        delay: Duration,
    }

    /// Test service with a fixed heartbeat response.
    struct FixedHeartbeatReply {
        inner: ReplicationReceiver,
        accepted_writer_epoch: u64,
        protocol_version: u32,
        heartbeat_calls: Arc<AtomicU64>,
    }

    #[tonic::async_trait]
    impl ReplicationService for FailFirstHeartbeat {
        async fn replicate(
            &self,
            request: Request<ReplicateRequest>,
        ) -> Result<Response<ReplicateResponse>, Status> {
            ReplicationService::replicate(&self.inner, request).await
        }

        async fn heartbeat(
            &self,
            request: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            if self.heartbeat_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                if self.process_first_beat {
                    let beat = request.into_inner();
                    self.inner
                        .process_heartbeat(
                            beat.protocol_version,
                            beat.writer_epoch,
                            beat.applied_through,
                            beat.durable_through,
                        )
                        .await?;
                } else {
                    drop(request);
                }
                self.first_failure.notify_one();
                return Err(self.status.clone());
            }
            ReplicationService::heartbeat(&self.inner, request).await
        }

        async fn hello(
            &self,
            request: Request<HelloRequest>,
        ) -> Result<Response<HelloResponse>, Status> {
            ReplicationService::hello(&self.inner, request).await
        }
    }

    #[tonic::async_trait]
    impl ReplicationService for DelayedHeartbeat {
        async fn replicate(
            &self,
            request: Request<ReplicateRequest>,
        ) -> Result<Response<ReplicateResponse>, Status> {
            ReplicationService::replicate(&self.inner, request).await
        }

        async fn heartbeat(
            &self,
            request: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            tokio::time::sleep(self.delay).await;
            ReplicationService::heartbeat(&self.inner, request).await
        }

        async fn hello(
            &self,
            request: Request<HelloRequest>,
        ) -> Result<Response<HelloResponse>, Status> {
            ReplicationService::hello(&self.inner, request).await
        }
    }

    #[tonic::async_trait]
    impl ReplicationService for FixedHeartbeatReply {
        async fn replicate(
            &self,
            request: Request<ReplicateRequest>,
        ) -> Result<Response<ReplicateResponse>, Status> {
            ReplicationService::replicate(&self.inner, request).await
        }

        async fn heartbeat(
            &self,
            _request: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            self.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(HeartbeatResponse {
                accepted_writer_epoch: self.accepted_writer_epoch,
                protocol_version: self.protocol_version,
            }))
        }

        async fn hello(
            &self,
            request: Request<HelloRequest>,
        ) -> Result<Response<HelloResponse>, Status> {
            ReplicationService::hello(&self.inner, request).await
        }
    }

    struct TestSender(ReplicationSender);
    type ShipCoordinates = (u64, u64, u64); // seqno, prune watermark, writer epoch

    impl TestSender {
        async fn send(
            &self,
            (seqno, prune_watermark, writer_epoch): ShipCoordinates,
            ops: &[ReplOp],
            dedup_entries: &[crate::dedup::DedupEntry],
        ) -> anyhow::Result<ShipResult> {
            let seqno = ShipSeqno::new(seqno)
                .ok_or_else(|| anyhow::anyhow!("test ship sequence must be nonzero"))?;
            let prune_watermark = PruneWatermark::for_ship(prune_watermark, seqno)
                .ok_or_else(|| anyhow::anyhow!("test prune watermark must precede the ship"))?;
            let writer_epoch = WriterEpoch::new(writer_epoch)
                .ok_or_else(|| anyhow::anyhow!("test writer epoch must be nonzero"))?;
            self.0
                .ship(seqno, ops, dedup_entries, prune_watermark, writer_epoch)
                .await
        }

        async fn ship(
            &self,
            coordinates: ShipCoordinates,
            ops: &[ReplOp],
            dedup_entries: &[crate::dedup::DedupEntry],
        ) -> bool {
            matches!(
                self.send(coordinates, ops, dedup_entries).await.unwrap(),
                ShipResult::Accepted
            )
        }

        async fn put_result(
            &self,
            coordinates: ShipCoordinates,
            key: &[u8],
            value: &[u8],
        ) -> ShipResult {
            self.send(coordinates, &[put(key, value)], &[])
                .await
                .unwrap()
        }

        async fn put(
            &self,
            coordinates: ShipCoordinates,
            key: &[u8],
            value: &[u8],
            dedup_entries: &[crate::dedup::DedupEntry],
        ) -> bool {
            self.ship(coordinates, &[put(key, value)], dedup_entries)
                .await
        }

        async fn try_put(
            &self,
            coordinates: ShipCoordinates,
            key: &[u8],
            value: &[u8],
            dedup_entries: &[crate::dedup::DedupEntry],
        ) -> anyhow::Result<bool> {
            Ok(matches!(
                self.send(coordinates, &[put(key, value)], dedup_entries)
                    .await?,
                ShipResult::Accepted
            ))
        }
    }

    async fn serve_service(service: impl ReplicationService) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    ReplicationServiceServer::new(service)
                        .max_decoding_message_size(MAX_SHIP_DECODE_BYTES),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    async fn serve(receiver: ReplicationReceiver) -> String {
        serve_service(receiver).await
    }

    async fn connect(endpoint: &str) -> TestSender {
        for _ in 0..100 {
            if let Ok(sender) = ReplicationSender::connect(endpoint.to_string()).await {
                return TestSender(sender);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("could not connect to receiver");
    }

    async fn spawn_receiver() -> (String, ReceiverControl) {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    async fn spawn_receiver_with_dedup(
        dedup: Arc<crate::dedup::DedupCache>,
    ) -> (String, ReceiverControl) {
        let receiver = new_receiver(dedup);
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    /// Receiver paused after fencing and before append for `paused_epoch`.
    async fn spawn_receiver_paused_before_append(
        paused_epoch: u64,
        reached: Arc<Notify>,
        resume: Arc<Notify>,
    ) -> (String, ReceiverControl) {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()))
            .pause_epoch_before_append(paused_epoch, reached, resume);
        let control = receiver.control();
        (serve(receiver).await, control)
    }

    async fn spawn_receiver_paused_before_phase_lock(
        paused_epoch: u64,
    ) -> (
        String,
        ReceiverControl,
        Arc<crate::dedup::DedupCache>,
        Arc<Notify>,
        Arc<Notify>,
    ) {
        let dedup = Arc::new(crate::dedup::DedupCache::new());
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let receiver = new_receiver(dedup.clone()).pause_epoch_before_phase_lock(
            paused_epoch,
            reached.clone(),
            resume.clone(),
        );
        let control = receiver.control();
        (serve(receiver).await, control, dedup, reached, resume)
    }

    fn put(k: &[u8], v: &[u8]) -> ReplOp {
        ReplOp::Put(Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
    }

    fn applied(op_id: crate::dedup::OpId) -> DedupEntry {
        DedupEntry {
            op_id,
            result: crate::dedup::DedupResult::Applied,
        }
    }

    async fn receive_put(
        receiver: &ReplicationReceiver,
        (seqno, prune_watermark, writer_epoch): ShipCoordinates,
        key: &[u8],
        value: &[u8],
    ) -> ShipDisposition {
        let seqno = ShipSeqno::new(seqno).unwrap();
        receiver
            .receive_ship(IncomingShip {
                seqno,
                ops: vec![put(key, value)],
                dedup_entries: Vec::new(),
                prune_watermark: PruneWatermark::for_ship(prune_watermark, seqno).unwrap(),
                writer_epoch: WriterEpoch::new(writer_epoch).unwrap(),
            })
            .await
    }

    async fn tail_len(control: &ReceiverControl) -> usize {
        control
            .inspect_standby_for_tests(|tail, _| tail.len())
            .await
    }

    async fn tail_state(control: &ReceiverControl) -> (u64, Vec<u64>, u64) {
        control
            .inspect_standby_for_tests(|tail, observed_epoch| {
                (
                    tail.epoch(),
                    tail.batches_in_order().map(|(seqno, _)| seqno).collect(),
                    observed_epoch,
                )
            })
            .await
    }

    async fn wait_for_epoch(control: &ReceiverControl, epoch: u64, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            while control
                .inspect_standby_for_tests(|_, observed_epoch| observed_epoch)
                .await
                < epoch
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    #[test]
    fn malformed_dedup_wire_entries_are_rejected() {
        let valid_result = crate::dedup::DedupResult::Applied.encode_wire().unwrap();
        for entry in [
            ProtoDedupEntry {
                op_id: vec![1; 15],
                result: valid_result.clone(),
            },
            ProtoDedupEntry {
                op_id: vec![1; 16],
                result: vec![99, 0],
            },
            ProtoDedupEntry {
                op_id: vec![0; 16],
                result: valid_result,
            },
        ] {
            let error = decode_dedup_entries(vec![entry])
                .expect_err("a malformed entry must reject the whole ship");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }

    #[test]
    fn dedup_payload_round_trips_exact_results() {
        let entries = [applied([7; 16])];
        let encoded_entries = encode_dedup_entries(&entries).unwrap();
        let decoded = decode_dedup_entries(encoded_entries).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].op_id, [7; 16]);
        assert!(matches!(
            &decoded[0].result,
            crate::dedup::DedupResult::Applied
        ));
    }

    #[tokio::test]
    async fn fresh_receiver_requires_the_sender_to_cover_a_missing_prefix() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert_eq!(
            sender.put_result((2, 0, 7), b"b", b"2").await,
            ShipResult::BaseNotCovered
        );
        let (tail_is_empty, has_uncovered_lineage, observed_epoch) = control
            .inspect_standby_for_tests(|tail, observed_epoch| {
                (
                    tail.is_empty(),
                    tail.has_uncovered_lineage(observed_epoch),
                    observed_epoch,
                )
            })
            .await;
        assert!(tail_is_empty, "an uncovered ship must not enter the tail");
        assert!(
            has_uncovered_lineage,
            "the rejected ship must leave promotion fail-closed"
        );
        assert_eq!(observed_epoch, 7);
        assert!(
            hello_peer(endpoint.clone()).await.unwrap().peer_active,
            "a peer must defer to known missing data even when the tail is empty"
        );

        assert_eq!(
            sender.put_result((2, 1, 7), b"b", b"2").await,
            ShipResult::Accepted
        );
        assert_eq!(tail_state(&control).await.1, vec![2]);
    }

    #[tokio::test]
    async fn hello_does_not_trigger_takeover_for_an_uncovered_tail() {
        let takeover = Arc::new(Notify::new());
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            Some(takeover.clone()),
            "standby-under-test".to_string(),
        );

        assert!(matches!(
            receive_put(&receiver, (1, 0, 7), b"a", b"1").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));
        assert!(matches!(
            receive_put(&receiver, (3, 0, 7), b"c", b"3").await,
            ShipDisposition::Result(ShipResult::BaseNotCovered)
        ));

        let answer = receiver
            .hello(Request::new(HelloRequest {
                protocol_version: HA_PROTOCOL_VERSION,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(answer.peer_active);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), takeover.notified())
                .await
                .is_err(),
            "an uncovered tail cannot authorize immediate takeover"
        );
    }

    #[test]
    fn current_hello_client_rejects_an_unversioned_response() {
        let error = decode_hello_answer(HelloResponse {
            peer_active: false,
            node_id: "legacy-peer".to_string(),
            protocol_version: 0,
        })
        .expect_err("an old server response must not participate in role election");

        assert!(hello_protocol_incompatible(&error));
        assert!(error.to_string().contains("protocol version 0"));
        assert!(!hello_protocol_incompatible(&anyhow::anyhow!(
            "Hello dial timed out"
        )));
    }

    #[tokio::test]
    async fn legacy_hello_caller_is_forced_to_defer_without_triggering_takeover() {
        let takeover = Arc::new(Notify::new());
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            Some(takeover.clone()),
            "new-peer".to_string(),
        );

        // An empty legacy request decodes `protocol_version` as zero.
        let answer = receiver
            .hello(Request::new(HelloRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(answer.protocol_version, HA_PROTOCOL_VERSION);
        assert!(answer.peer_active);

        // A legacy decoder sees the incompatible response as active.
        let legacy = LegacyHelloResponse::decode(answer.encode_to_vec().as_slice()).unwrap();
        assert!(legacy.peer_active);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), takeover.notified())
                .await
                .is_err(),
            "an incompatible Hello must defer its caller, not launch local takeover"
        );
    }

    #[tokio::test]
    async fn ships_buffer_and_prune_the_durable_prefix() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert!(sender.put((1, 0, 1), b"a", b"1", &[]).await);
        assert!(sender.put((2, 0, 1), b"b", b"2", &[]).await);
        assert_eq!(tail_len(&control).await, 2);

        // Sequence 4 declares sequences through 2 durable.
        assert!(sender.put((3, 1, 1), b"c", b"3", &[]).await);
        let seqnos = tail_state(&control).await.1;
        assert_eq!(seqnos, vec![2, 3], "watermark 1 prunes seqno 1");
    }

    #[tokio::test]
    async fn malformed_ship_identity_cannot_mutate_leading_receiver_state() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        let superseding = control.superseding_epochs();
        control
            .begin_promotion(8)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        let lease = crate::replication::Lease::new();
        lease.renew(Duration::from_secs(30));
        control.attach_serving_lease(8, &lease);

        for (seqno, prune_watermark, writer_epoch) in [
            (1, 0, 0), // zero epoch
            (0, 0, 9), // zero ship sequence
            (3, 3, 9), // watermark covers the current ship
            (3, 4, 9), // watermark is ahead of the current ship
        ] {
            let error = receiver
                .replicate(Request::new(ReplicateRequest {
                    seqno,
                    ops: Vec::new(),
                    prune_watermark,
                    writer_epoch,
                    dedup_entries: Vec::new(),
                }))
                .await
                .expect_err("malformed ship coordinates must be rejected");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(
                control.inspect_phase_epochs_for_tests().await,
                ("leading", 8, 8),
                "validation must run before admission observes epoch 9"
            );
            assert_eq!(*superseding.borrow(), 0);
            assert!(lease.is_valid(), "malformed input must not revoke serving");
        }
    }

    #[tokio::test]
    async fn malformed_heartbeats_do_not_mutate_receiver_state() {
        for (name, epoch, applied, durable) in [
            ("zero writer epoch", 0, 0, 0),
            ("durable frontier ahead of applied", 7, 1, 2),
        ] {
            let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
            let liveness = receiver.liveness();
            let error = receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, epoch, applied, durable)
                .await
                .expect_err(name);
            assert_eq!(error.code(), tonic::Code::InvalidArgument, "{name}");
            assert_eq!(*liveness.borrow(), 0, "{name}");
            assert_eq!(
                receiver.control().inspect_phase_epochs_for_tests().await,
                ("standby", 0, 0),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn legacy_heartbeat_latches_protocol_incompatibility() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let liveness = receiver.liveness();

        let error = receiver
            .process_heartbeat(0, 7, 0, 0)
            .await
            .expect_err("an unversioned heartbeat must be rejected");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(HEARTBEAT_PROTOCOL_INCOMPATIBLE));
        assert_eq!(*liveness.borrow(), INCOMPATIBLE_HEARTBEAT_LIVENESS);
        assert_eq!(
            receiver.control().inspect_phase_epochs_for_tests().await,
            ("standby", 0, 0),
            "legacy liveness must not publish an epoch or coverage frontier"
        );

        let error = receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 0, 0)
            .await
            .expect_err("the incompatibility latch must survive later valid frames");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(*liveness.borrow(), INCOMPATIBLE_HEARTBEAT_LIVENESS);
    }

    #[tokio::test]
    async fn fresh_receiver_ticks_liveness_only_after_the_applied_base_is_durable() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let liveness = receiver.liveness();

        let error = receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 1, 0)
            .await
            .expect_err("a fresh receiver does not hold applied attempt 1");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(error.message(), HEARTBEAT_BASE_NOT_COVERED);
        assert_eq!(
            *liveness.borrow(),
            0,
            "an uncovered receiver must not advertise takeover readiness"
        );
        assert!(
            !receiver
                .control()
                .inspect_standby_for_tests(|tail, _| tail.preserves_lineage(7))
                .await
        );

        receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 1, 1)
            .await
            .expect("a durable watermark covering attempt 1 repairs the base");
        assert_eq!(*liveness.borrow(), 1);
        assert!(
            receiver
                .control()
                .inspect_standby_for_tests(|tail, _| tail.preserves_lineage(7))
                .await,
            "a liveness tick must imply safe data-prefix coverage"
        );
    }

    #[tokio::test]
    async fn reordered_lower_heartbeat_cannot_hide_a_missing_frontier() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let liveness = receiver.liveness();

        receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 2, 0)
            .await
            .expect_err("the receiver is missing the exposed two-attempt prefix");
        receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 1, 1)
            .await
            .expect_err("a reordered lower heartbeat cannot erase missing attempt 2");
        assert_eq!(
            *liveness.borrow(),
            0,
            "liveness must remain fail-closed through the highest observed frontier"
        );

        receiver
            .process_heartbeat(HA_PROTOCOL_VERSION, 7, 2, 2)
            .await
            .expect("durability through the highest observed frontier repairs coverage");
        assert_eq!(*liveness.borrow(), 1);
    }

    #[tokio::test]
    async fn heartbeat_ack_quiesce_preserves_ship_and_tail_admission() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        let liveness = receiver.liveness();
        assert!(matches!(
            receive_put(&receiver, (1, 0, 7), b"first", b"one").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));

        control.quiesce_heartbeat_acks().await;
        assert!(
            !receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, 7, 1, 0)
                .await
                .expect("a quiesced heartbeat is valid but ignored"),
            "quiescing must suppress receiver and sender liveness"
        );
        assert_eq!(*liveness.borrow(), 0);
        assert!(matches!(
            receive_put(&receiver, (2, 0, 7), b"second", b"two").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, observed_epoch| { (tail.len(), observed_epoch) })
                .await,
            (2, 7),
            "quiescing heartbeat ACKs must not freeze or discard the replicated tail"
        );

        control.resume_heartbeat_acks().await;
        assert!(
            receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, 7, 2, 0)
                .await
                .expect("rearmed heartbeat coverage is valid")
        );
        assert_eq!(*liveness.borrow(), 1);
    }

    #[tokio::test]
    async fn reconstructed_standby_stays_quiesced_until_role_election_rearms_it() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        control.quiesce_heartbeat_acks().await;
        control
            .begin_promotion(3)
            .await
            .unwrap()
            .return_to_standby(3)
            .await
            .unwrap();

        assert!(
            !receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, 3, 0, 0)
                .await
                .expect("a reconstructed standby must ignore heartbeats while quiesced")
        );
        assert_eq!(*receiver.liveness().borrow(), 0);

        control.resume_heartbeat_acks().await;
        assert!(
            receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, 3, 0, 0)
                .await
                .expect("role election must be able to rearm the reconstructed standby")
        );
        assert_eq!(*receiver.liveness().borrow(), 1);
    }

    #[tokio::test]
    async fn pending_dedup_results_publish_only_when_pruned_as_durable() {
        let dedup = Arc::new(crate::dedup::DedupCache::new());
        let (endpoint, _control) = spawn_receiver_with_dedup(dedup.clone()).await;
        let sender = connect(&endpoint).await;
        let durable = [1u8; 16];
        let pending = [2u8; 16];
        let original_attrs = crate::fs::types::FileAttributes {
            mode: 0o640,
            size: 1234,
            fileid: 77,
            ..Default::default()
        };
        let durable_entry = DedupEntry {
            op_id: durable,
            result: crate::dedup::DedupResult::Create {
                inode_id: 77,
                attrs: original_attrs,
            },
        };

        assert!(sender.put((1, 0, 1), b"a", b"1", &[durable_entry]).await);
        assert!(
            dedup.get(&durable).is_none(),
            "an accepted-but-unpruned tail batch is not known applied yet"
        );

        assert!(sender.put((2, 1, 1), b"b", b"2", &[applied(pending)]).await);
        match dedup.get(&durable) {
            Some(crate::dedup::DedupResult::Create { inode_id, attrs }) => {
                assert_eq!(inode_id, 77);
                assert_eq!(attrs.mode, 0o640);
                assert_eq!(attrs.size, 1234);
                assert_eq!(attrs.fileid, 77);
            }
            other => panic!(
                "watermark pruning must publish the exact typed create result, got {other:?}"
            ),
        }
        assert!(
            dedup.get(&pending).is_none(),
            "the unpruned suffix must remain pending"
        );
    }

    #[tokio::test]
    async fn live_prune_preserves_the_oldest_result_expiry_for_promotion() {
        let start = Instant::now();
        let elapsed = Arc::new(AtomicU64::new(0));
        let clock = Arc::clone(&elapsed);
        let dedup = Arc::new(crate::dedup::DedupCache::new_for_test(
            Duration::from_secs(10),
            move || start + Duration::from_secs(clock.load(Ordering::SeqCst)),
        ));
        let (endpoint, control) = spawn_receiver_with_dedup(Arc::clone(&dedup)).await;
        let sender = connect(&endpoint).await;
        let old = [0x31; 16];
        let newer = [0x32; 16];

        assert!(sender.put((1, 0, 7), b"old", b"1", &[applied(old)]).await);
        assert!(sender.put((2, 1, 7), b"new", b"2", &[applied(newer)]).await);
        let old_deadline = control
            .inspect_standby_for_tests(|tail, _| tail.dedup_coverage_deadline())
            .await
            .expect("watermark publication must contribute its result expiry");
        assert_eq!(old_deadline, start + Duration::from_secs(10));

        // Later publication retains the earliest result expiry.
        elapsed.store(9, Ordering::SeqCst);
        assert!(sender.put((3, 2, 7), b"latest", b"3", &[]).await);
        let bounded_deadline = control
            .inspect_standby_for_tests(|tail, _| tail.dedup_coverage_deadline())
            .await;
        assert_eq!(bounded_deadline, Some(old_deadline));
        assert!(dedup.get(&old).is_some());

        elapsed.store(10, Ordering::SeqCst);
        assert!(dedup.get(&old).is_none());
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| tail.dedup_coverage_deadline())
                .await,
            Some(old_deadline),
            "expired coverage must remain visibly expired, not gain a fresh window"
        );
    }

    #[tokio::test]
    async fn cross_term_tail_replacement_drops_pending_dedup_results() {
        let dedup = Arc::new(crate::dedup::DedupCache::new());
        let (endpoint, _control) = spawn_receiver_with_dedup(dedup.clone()).await;
        let sender = connect(&endpoint).await;
        let durable_old = [3u8; 16];
        let old = [4u8; 16];
        let durable_new = [5u8; 16];
        let pending_new = [6u8; 16];

        assert!(
            sender
                .put(
                    (49, 48, 5),
                    b"durable-old",
                    b"value",
                    &[applied(durable_old)]
                )
                .await
        );
        assert!(
            sender
                .put((50, 49, 5), b"old", b"value", &[applied(old)])
                .await
        );
        assert!(
            sender
                .put((1, 0, 6), b"new", b"value", &[applied(durable_new)])
                .await
        );
        assert!(
            sender
                .put((2, 1, 6), b"next", b"value", &[applied(pending_new)])
                .await
        );

        assert!(
            dedup.get(&durable_old).is_some(),
            "an already-pruned old-term id remains valid across term replacement"
        );
        assert!(
            dedup.get(&old).is_none(),
            "an old-term dedup result must disappear with its replaced tail batch"
        );
        assert!(
            dedup.get(&durable_new).is_some(),
            "the pruned new-term batch is proven durable"
        );
        assert!(
            dedup.get(&pending_new).is_none(),
            "the unpruned new-term suffix remains pending"
        );
    }

    #[tokio::test]
    async fn ship_larger_than_the_default_decode_limit_is_accepted() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        let big = ReplOp::Put(
            Bytes::from_static(b"k"),
            Bytes::from(vec![0u8; 6 * 1024 * 1024]),
        );
        assert!(
            sender.ship((1, 0, 1), &[big], &[]).await,
            "a ship past the 4 MiB default decode limit must be accepted, not rejected"
        );
        assert_eq!(tail_len(&control).await, 1, "the large batch must buffer");
    }

    #[tokio::test]
    async fn hello_reports_the_responders_node_id() {
        let (endpoint, _control) = spawn_receiver().await;
        let answer = loop {
            match hello_peer(endpoint.clone()).await {
                Ok(a) => break a,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        assert_eq!(answer.node_id, "standby-under-test");
        assert!(!answer.peer_active, "idle standby: empty tail, not leading");
    }

    #[tokio::test]
    async fn hello_rejects_an_empty_peer_identity() {
        let endpoint = serve(ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            String::new(),
        ))
        .await;
        let error = loop {
            match hello_peer(endpoint.clone()).await {
                Err(error) if error.to_string().contains("empty node_id") => break error,
                Err(_) | Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        assert!(error.to_string().contains("empty node_id"));
    }

    #[tokio::test]
    async fn heartbeats_tick_liveness_and_stop_on_break() {
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "standby-under-test".to_string(),
        );
        let mut liveness = receiver.liveness();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(receiver.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let endpoint = format!("http://{addr}");

        let sender = tokio::spawn(async move {
            let _ = run_heartbeat_sender(
                endpoint,
                test_writer_epoch(1),
                Duration::from_millis(20),
                heartbeat_control(1),
            )
            .await;
        });

        tokio::time::timeout(Duration::from_secs(3), liveness.changed())
            .await
            .expect("a heartbeat must tick liveness")
            .unwrap();
        assert!(*liveness.borrow() > 0, "liveness advanced on heartbeats");

        sender.abort();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = *liveness.borrow();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            *liveness.borrow(),
            after,
            "no more ticks after the sender stops"
        );
    }

    #[tokio::test]
    async fn leader_ack_is_exact_epoch_and_anchored_at_request_start() {
        let response_delay = Duration::from_millis(100);
        let endpoint = serve_service(DelayedHeartbeat {
            inner: new_receiver(Arc::new(crate::dedup::DedupCache::new())),
            delay: response_delay,
        })
        .await;
        let control = heartbeat_control(7);
        let mut acknowledgements = control.heartbeat_acks();
        let not_before = tokio::time::Instant::now();
        let sender = tokio::spawn(run_heartbeat_sender(
            endpoint,
            test_writer_epoch(7),
            Duration::from_secs(3600),
            control,
        ));

        tokio::time::timeout(Duration::from_secs(3), acknowledgements.changed())
            .await
            .expect("the standby must acknowledge the exact writer epoch")
            .unwrap();
        let received = tokio::time::Instant::now();
        let ack = (*acknowledgements.borrow_and_update())
            .expect("a watch change must carry an acknowledgement");
        assert!(ack.started >= not_before);
        assert!(
            received.duration_since(ack.started) >= response_delay,
            "authority must be anchored at request start, not delayed ACK receipt"
        );
        sender.abort();
    }

    #[tokio::test]
    async fn fenced_receiver_does_not_acknowledge_heartbeat_liveness() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        receiver
            .control()
            .begin_promotion(2)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        let endpoint = serve(receiver).await;
        let control = heartbeat_control(1);
        let mut acknowledgements = control.heartbeat_acks();
        let sender = tokio::spawn(run_heartbeat_sender(
            endpoint,
            test_writer_epoch(1),
            Duration::from_millis(20),
            control,
        ));

        assert!(
            tokio::time::timeout(Duration::from_millis(250), acknowledgements.changed())
                .await
                .is_err(),
            "a fenced receiver must return no leader-side liveness ACK"
        );
        assert!(acknowledgements.borrow().is_none());
        sender.abort();
    }

    #[tokio::test]
    async fn heartbeat_sender_reconnects_after_an_established_transport_failure() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let mut liveness = receiver.liveness();
        let heartbeat_calls = Arc::new(AtomicU64::new(0));
        let first_failure = Arc::new(Notify::new());
        let endpoint = serve_service(FailFirstHeartbeat {
            inner: receiver,
            status: Status::unavailable("injected heartbeat RPC failure"),
            process_first_beat: true,
            heartbeat_calls: heartbeat_calls.clone(),
            first_failure: first_failure.clone(),
        })
        .await;

        let sender = tokio::spawn(run_heartbeat_sender(
            endpoint,
            test_writer_epoch(1),
            Duration::from_millis(20),
            heartbeat_control(1),
        ));
        tokio::time::timeout(Duration::from_secs(3), first_failure.notified())
            .await
            .expect("the first established stream must receive the injected failure");
        let first_tick = *liveness.borrow_and_update();
        assert!(first_tick > 0, "the first stream must accept a heartbeat");

        tokio::time::timeout(crate::replication::TAKEOVER_HINT_AFTER, async {
            loop {
                liveness.changed().await.unwrap();
                if *liveness.borrow_and_update() > first_tick {
                    break;
                }
            }
        })
        .await
        .expect("the sender must reconnect and beat before the takeover hint deadline");
        assert!(
            heartbeat_calls.load(Ordering::SeqCst) >= 2,
            "a generic heartbeat failure must open a replacement heartbeat RPC"
        );
        assert!(
            !sender.is_finished(),
            "generic transport failures stay inside the heartbeat sender"
        );
        sender.abort();
    }

    #[tokio::test]
    async fn heartbeat_sender_surfaces_base_not_covered_without_retrying() {
        let heartbeat_calls = Arc::new(AtomicU64::new(0));
        let first_failure = Arc::new(Notify::new());
        let endpoint = serve_service(FailFirstHeartbeat {
            inner: new_receiver(Arc::new(crate::dedup::DedupCache::new())),
            status: Status::failed_precondition(HEARTBEAT_BASE_NOT_COVERED),
            process_first_beat: false,
            heartbeat_calls: heartbeat_calls.clone(),
            first_failure,
        })
        .await;

        let exit = tokio::time::timeout(
            Duration::from_secs(3),
            run_heartbeat_sender(
                endpoint,
                test_writer_epoch(1),
                Duration::from_millis(20),
                heartbeat_control(1),
            ),
        )
        .await
        .expect("the semantic rejection must not enter the reconnect backoff");
        assert!(matches!(exit, HeartbeatExit::BaseNotCovered(_)));
        assert_eq!(
            heartbeat_calls.load(Ordering::SeqCst),
            1,
            "BaseNotCovered reopened the heartbeat stream"
        );
    }

    #[tokio::test]
    async fn heartbeat_sender_surfaces_protocol_incompatibility_without_retrying() {
        let heartbeat_calls = Arc::new(AtomicU64::new(0));
        let endpoint = serve_service(FailFirstHeartbeat {
            inner: new_receiver(Arc::new(crate::dedup::DedupCache::new())),
            status: Status::failed_precondition(format!(
                "{HEARTBEAT_PROTOCOL_INCOMPATIBLE}: injected version mismatch"
            )),
            process_first_beat: false,
            heartbeat_calls: heartbeat_calls.clone(),
            first_failure: Arc::new(Notify::new()),
        })
        .await;

        let exit = tokio::time::timeout(
            Duration::from_secs(3),
            run_heartbeat_sender(
                endpoint,
                test_writer_epoch(1),
                Duration::from_millis(20),
                heartbeat_control(1),
            ),
        )
        .await
        .expect("protocol incompatibility must not enter the reconnect backoff");
        let HeartbeatExit::ProtocolIncompatible(status) = exit else {
            panic!("protocol incompatibility returned the wrong heartbeat exit")
        };
        assert!(status.message().contains("injected version mismatch"));
        assert_eq!(
            heartbeat_calls.load(Ordering::SeqCst),
            1,
            "protocol incompatibility reopened the heartbeat stream"
        );
    }

    #[tokio::test]
    async fn malformed_heartbeat_response_is_terminal_without_ack() {
        for (name, protocol_version, accepted_writer_epoch, expected_message) in [
            (
                "wrong protocol version",
                HA_PROTOCOL_VERSION + 1,
                7,
                "response version",
            ),
            (
                "wrong accepted epoch",
                HA_PROTOCOL_VERSION,
                8,
                "acknowledged writer epoch",
            ),
        ] {
            let heartbeat_calls = Arc::new(AtomicU64::new(0));
            let endpoint = serve_service(FixedHeartbeatReply {
                inner: new_receiver(Arc::new(crate::dedup::DedupCache::new())),
                accepted_writer_epoch,
                protocol_version,
                heartbeat_calls: heartbeat_calls.clone(),
            })
            .await;
            let control = heartbeat_control(7);
            let acknowledgements = control.heartbeat_acks();

            let exit = tokio::time::timeout(
                Duration::from_secs(3),
                run_heartbeat_sender(
                    endpoint,
                    test_writer_epoch(7),
                    Duration::from_millis(20),
                    control,
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("{name} must terminate without reconnecting"));
            let HeartbeatExit::ProtocolIncompatible(status) = exit else {
                panic!("{name} returned the wrong heartbeat exit")
            };
            assert!(status.message().contains(expected_message), "{name}");
            assert_eq!(heartbeat_calls.load(Ordering::SeqCst), 1, "{name}");
            assert!(
                acknowledgements.borrow().is_none(),
                "{name} must not publish leader-side liveness"
            );
        }
    }

    #[tokio::test]
    async fn newer_term_replaces_the_old_tail_with_or_without_a_prior_heartbeat() {
        for (name, heartbeat_first) in [("first ship", false), ("heartbeat first", true)] {
            let (endpoint, control) = spawn_receiver().await;
            let sender = connect(&endpoint).await;
            assert!(sender.put((50, 49, 5), b"/f", b"old", &[]).await);
            assert!(sender.put((51, 49, 5), b"/f", b"old", &[]).await);

            let heartbeat = heartbeat_first.then(|| {
                tokio::spawn(run_heartbeat_sender(
                    endpoint,
                    test_writer_epoch(6),
                    Duration::from_secs(3600),
                    heartbeat_control(6),
                ))
            });
            if heartbeat_first {
                assert!(
                    wait_for_epoch(&control, 6, Duration::from_secs(5)).await,
                    "{name}"
                );
                assert_eq!(
                    tail_state(&control).await,
                    (5, vec![50, 51], 6),
                    "heartbeat cannot discard the predecessor's only tail copy"
                );
            }

            assert!(sender.put((1, 0, 6), b"/f", b"new", &[]).await, "{name}");
            if let Some(heartbeat) = heartbeat {
                heartbeat.abort();
            }
            let (epoch, seqnos, _) = tail_state(&control).await;
            assert_eq!((epoch, seqnos), (6, vec![1]), "{name}");
        }
    }

    #[tokio::test]
    async fn promoting_receiver_rejects_zombies_and_yields_to_newer_writer() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert!(sender.put((1, 0, 7), b"a", b"1", &[]).await);
        assert_eq!(tail_len(&control).await, 1);

        let promotion = control.begin_promotion(7).await.unwrap();

        assert!(
            !sender.put((2, 0, 7), b"b", b"2", &[applied([9; 16])]).await,
            "a fenced node must reject an equal-epoch zombie's ship"
        );
        assert_eq!(
            promotion.tail.len(),
            1,
            "a rejected zombie ship must not enter the frozen promotion tail"
        );

        assert!(
            !sender.put((3, 0, 6), b"c", b"3", &[]).await,
            "a lower-epoch zombie must also be authoritatively rejected"
        );

        let err = sender
            .try_put((1, 0, 8), b"new", b"writer", &[])
            .await
            .expect_err("a newer writer must see this stale receiver as unavailable");
        assert_eq!(
            err.downcast_ref::<tonic::Status>().map(tonic::Status::code),
            Some(tonic::Code::Unavailable)
        );
        assert_eq!(
            promotion.tail.len(),
            1,
            "the newer writer must not enter a stale process's frozen tail"
        );
        let error = promotion
            .complete()
            .await
            .expect_err("the recorded epoch-8 writer must prevent epoch-7 completion");
        let superseded = error
            .downcast_ref::<PromotionSuperseded>()
            .expect("completion must report recoverable supersession");
        assert_eq!(superseded.writer_epoch, 7);
        assert_eq!(superseded.observed_epoch, 8);
        let (len, observed_epoch) = control
            .inspect_standby_for_tests(|tail, observed_epoch| (tail.len(), observed_epoch))
            .await;
        assert_eq!(len, 1, "the frozen epoch-7 tail must be restored");
        assert_eq!(observed_epoch, 8);
    }

    #[tokio::test]
    async fn stale_promotion_entry_preserves_the_live_standby_tail() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        assert!(matches!(
            receive_put(&receiver, (1, 0, 9), b"newer", b"one").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));

        let error = match control.begin_promotion(8).await {
            Ok(_) => panic!("epoch 8 must not freeze a tail already known to be at epoch 9"),
            Err(error) => error,
        };
        let superseded = error
            .downcast_ref::<PromotionSuperseded>()
            .expect("stale entry must be a recoverable role-election result");
        assert_eq!(superseded.writer_epoch, 8);
        assert_eq!(superseded.observed_epoch, 9);
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, observed_epoch| {
                    (tail.epoch(), tail.len(), observed_epoch)
                })
                .await,
            (9, 1, 9),
            "entry refusal must leave the standby tail live"
        );

        assert!(matches!(
            receive_put(&receiver, (2, 0, 9), b"newer", b"two").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));
        assert_eq!(
            tail_len(&control).await,
            2,
            "the receiver must continue buffering after rejecting stale promotion"
        );
    }

    #[tokio::test]
    async fn newer_ship_deposes_leading_runtime_without_accepting_a_tail() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        let superseding = control.superseding_epochs();
        control
            .begin_promotion(8)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        let lease = crate::replication::Lease::new();
        lease.renew(Duration::from_secs(30));
        control.attach_serving_lease(8, &lease);
        assert_eq!(
            control.inspect_phase_epochs_for_tests().await,
            ("leading", 8, 8)
        );

        assert!(matches!(
            receive_put(&receiver, (1, 0, 9), b"epoch-9", b"value").await,
            ShipDisposition::ReceiverStale {
                local_epoch: 8,
                incoming_epoch: 9
            }
        ));
        assert_eq!(
            control.inspect_phase_epochs_for_tests().await,
            ("leading", 8, 9)
        );
        assert_eq!(*superseding.borrow(), 9);
        assert!(
            !lease.is_valid(),
            "the RPC that observes a newer writer must synchronously revoke serving"
        );
    }

    #[tokio::test]
    async fn control_plane_conflict_demotes_completed_leader() {
        let receiver = new_receiver(Arc::new(crate::dedup::DedupCache::new()));
        let control = receiver.control();
        control
            .begin_promotion(8)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();

        control.demote_leader(8, 10).await.unwrap();
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, observed_epoch| { (tail.len(), observed_epoch) })
                .await,
            (0, 10)
        );
    }

    #[tokio::test]
    async fn dropped_promotion_snapshot_leaves_receiver_fail_closed() {
        let (endpoint, control) = spawn_receiver().await;
        let sender = connect(&endpoint).await;

        assert!(sender.put((1, 0, 7), b"a", b"1", &[]).await);
        let promotion = control.begin_promotion(8).await.unwrap();
        drop(promotion);

        assert!(
            !sender.put((2, 0, 7), b"late", b"write", &[]).await,
            "dropping the replay capability must not reopen receiver admission"
        );
        assert!(
            control.begin_promotion(9).await.is_err(),
            "a failed promotion is terminal for this receiver incarnation"
        );
        assert!(
            hello_peer(endpoint).await.unwrap().peer_active,
            "a fail-closed promoter must still make a booting peer defer"
        );
    }

    #[tokio::test]
    async fn ship_losing_the_promotion_race_is_rejected() {
        let (endpoint, control, dedup, reached, resume) =
            spawn_receiver_paused_before_phase_lock(7).await;
        let sender = connect(&endpoint).await;
        let op_id = [9u8; 16];
        let ship = tokio::spawn(async move {
            sender
                .put((1, 0, 7), b"late", b"write", &[applied(op_id)])
                .await
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the ship must reach the phase-lock boundary");

        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let accepted = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the ship must finish after promotion releases the phase lock")
            .unwrap();
        assert!(
            !accepted,
            "a ship that loses the phase-lock race to promotion must be rejected"
        );
        assert!(
            promotion.tail.is_empty(),
            "the post-replay ship must not enter the tail"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a rejected post-replay ship must not poison dedup"
        );
        promotion.complete().await.unwrap();
    }

    #[tokio::test]
    async fn newer_ship_losing_the_promotion_race_sees_stale_receiver() {
        let (endpoint, control, dedup, reached, resume) =
            spawn_receiver_paused_before_phase_lock(9).await;
        let sender = connect(&endpoint).await;
        let op_id = [10u8; 16];
        let ship = tokio::spawn(async move {
            sender
                .try_put((1, 0, 9), b"newer", b"write", &[applied(op_id)])
                .await
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the newer ship must reach the phase-lock boundary");

        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let err = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the newer ship must finish after promotion releases the phase lock")
            .unwrap()
            .expect_err("the stale promoter must report itself unavailable");
        assert_eq!(
            err.downcast_ref::<tonic::Status>().map(tonic::Status::code),
            Some(tonic::Code::Unavailable)
        );
        assert!(
            promotion.tail.is_empty(),
            "a stale promoter must not append after its one-time replay"
        );
        assert!(
            dedup.get(&op_id).is_none(),
            "a stale promoter must not publish the rejected op-id"
        );
        let error = promotion
            .complete()
            .await
            .expect_err("a newer ship observed during replay must abort stale completion");
        assert!(error.downcast_ref::<PromotionSuperseded>().is_some());
        let (epoch, seqnos, observed_epoch) = tail_state(&control).await;
        assert_eq!(epoch, 0);
        assert!(seqnos.is_empty());
        assert_eq!(observed_epoch, 9);
    }

    #[tokio::test]
    async fn heartbeat_epoch_update_waits_for_ship_admission() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let (endpoint, control) =
            spawn_receiver_paused_before_append(5, reached.clone(), resume.clone()).await;
        let sender = connect(&endpoint).await;

        let ship = tokio::spawn(async move { sender.put((50, 49, 5), b"f", b"old", &[]).await });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the ship must reach the admission critical section");

        let hb = tokio::spawn(run_heartbeat_sender(
            endpoint.clone(),
            test_writer_epoch(6),
            Duration::from_secs(3600),
            heartbeat_control(6),
        ));
        let published_mid_admission = wait_for_epoch(&control, 6, Duration::from_millis(500)).await;
        assert!(
            !published_mid_admission,
            "a heartbeat published a newer epoch inside another ship's admission \
             critical section"
        );

        resume.notify_one();
        let accepted = tokio::time::timeout(Duration::from_secs(3), ship)
            .await
            .expect("the ship must finish once resumed")
            .unwrap();
        assert!(accepted);

        assert!(
            wait_for_epoch(&control, 6, Duration::from_secs(5)).await,
            "the heartbeat must publish after the admission releases"
        );
        hb.abort();
        let sender2 = connect(&endpoint).await;
        assert!(
            !sender2.put((51, 0, 5), b"f", b"old2", &[]).await,
            "a stale ship after the published beat must be fenced"
        );
    }

    #[tokio::test]
    async fn heartbeat_paused_before_phase_lock_cannot_ack_after_quiesce() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let receiver =
            Arc::new(
                new_receiver(Arc::new(crate::dedup::DedupCache::new()))
                    .pause_epoch_before_phase_lock(8, reached.clone(), resume.clone()),
            );
        let control = receiver.control();
        let liveness = receiver.liveness();
        let heartbeat = tokio::spawn({
            let receiver = receiver.clone();
            async move {
                receiver
                    .process_heartbeat(HA_PROTOCOL_VERSION, 8, 0, 0)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("heartbeat must pause before acquiring the phase lock");
        control.quiesce_heartbeat_acks().await;
        resume.notify_one();

        let accepted = tokio::time::timeout(Duration::from_secs(3), heartbeat)
            .await
            .expect("heartbeat must finish after quiescing")
            .unwrap()
            .expect("heartbeat frame remains valid");
        assert!(!accepted, "no ACK may linearize after quiesce returns");
        assert_eq!(*liveness.borrow(), 0);
        assert_eq!(
            control.inspect_phase_epochs_for_tests().await,
            ("standby", 0, 0),
            "an ignored quiesced heartbeat must not publish its epoch"
        );
    }

    #[tokio::test]
    async fn heartbeat_holding_phase_lock_linearizes_before_quiesce() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let receiver = Arc::new(
            new_receiver(Arc::new(crate::dedup::DedupCache::new()))
                .pause_heartbeat_with_phase_lock(8, reached.clone(), resume.clone()),
        );
        let control = receiver.control();
        let liveness = receiver.liveness();
        let heartbeat = tokio::spawn({
            let receiver = receiver.clone();
            async move {
                receiver
                    .process_heartbeat(HA_PROTOCOL_VERSION, 8, 0, 0)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("heartbeat must hold the phase lock");
        let mut quiesce = tokio::spawn({
            let control = control.clone();
            async move { control.quiesce_heartbeat_acks().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut quiesce)
                .await
                .is_err(),
            "quiesce must wait for the heartbeat admission critical section"
        );

        resume.notify_one();
        let accepted = tokio::time::timeout(Duration::from_secs(3), heartbeat)
            .await
            .expect("heartbeat must finish after releasing the phase lock")
            .unwrap()
            .expect("heartbeat frame remains valid");
        assert!(accepted, "the pre-quiesce heartbeat must be accepted");
        tokio::time::timeout(Duration::from_secs(3), quiesce)
            .await
            .expect("quiesce must finish after heartbeat admission")
            .unwrap();
        assert_eq!(*liveness.borrow(), 1);

        assert!(
            !receiver
                .process_heartbeat(HA_PROTOCOL_VERSION, 9, 0, 0)
                .await
                .expect("later heartbeat remains a valid frame"),
            "heartbeats linearizing after quiesce must be ignored"
        );
        assert_eq!(*liveness.borrow(), 1);
        assert_eq!(
            control.inspect_phase_epochs_for_tests().await,
            ("standby", 0, 8),
            "the quiesced later heartbeat must not publish its epoch"
        );
    }

    #[tokio::test]
    async fn heartbeat_losing_the_promotion_race_cannot_change_replay_snapshot() {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let receiver =
            Arc::new(
                new_receiver(Arc::new(crate::dedup::DedupCache::new()))
                    .pause_epoch_before_phase_lock(8, reached.clone(), resume.clone()),
            );
        let control = receiver.control();

        assert!(matches!(
            receive_put(&receiver, (1, 0, 7), b"covered", b"write").await,
            ShipDisposition::Result(ShipResult::Accepted)
        ));

        let heartbeat = tokio::spawn({
            let receiver = receiver.clone();
            async move {
                receiver
                    .receive_heartbeat(
                        WriterEpoch::new(8).unwrap(),
                        CoverageFrontier::from_wire(0, 0).unwrap(),
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the heartbeat must reach the phase-lock boundary");

        let promotion = control.begin_promotion(8).await.unwrap();
        resume.notify_one();

        let observed_epoch = promotion.observed_epoch();
        assert_eq!(observed_epoch, 7);
        assert!(
            promotion.tail.preserves_lineage(observed_epoch),
            "takeover must freeze the tail and observed epoch in one owned value"
        );

        let disposition = tokio::time::timeout(Duration::from_secs(3), heartbeat)
            .await
            .expect("the heartbeat must finish after promotion releases the phase lock")
            .unwrap();
        assert_eq!(disposition, HeartbeatDisposition::Ignored);
        assert_eq!(
            promotion.observed_epoch(),
            7,
            "the late heartbeat has no path to the frozen snapshot"
        );
        promotion.complete().await.unwrap();
    }

    #[tokio::test]
    async fn stale_ship_losing_the_phase_lock_race_is_rejected() {
        let (endpoint, control, dedup, reached, resume) =
            spawn_receiver_paused_before_phase_lock(5).await;

        let stale_sender = connect(&endpoint).await;
        let current_sender = connect(&endpoint).await;

        let stale_op_id = [5u8; 16];
        let stale_ship = tokio::spawn(async move {
            stale_sender
                .put((50, 0, 5), b"stale", b"write", &[applied(stale_op_id)])
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), reached.notified())
            .await
            .expect("the stale ship must reach the phase-lock boundary");

        assert!(
            current_sender
                .put((1, 0, 6), b"current", b"write", &[])
                .await,
            "the newer epoch ship must be accepted first"
        );
        resume.notify_one();

        let stale_accepted = tokio::time::timeout(Duration::from_secs(3), stale_ship)
            .await
            .expect("the stale ship must finish after it resumes")
            .unwrap();
        assert!(
            !stale_accepted,
            "a stale ship must be re-fenced if a newer epoch won the phase lock"
        );
        let (epoch, seqnos, _) = tail_state(&control).await;
        assert_eq!(epoch, 6);
        assert_eq!(
            seqnos,
            vec![1],
            "the stale epoch batch must not enter the newer term's tail"
        );
        assert!(
            dedup.get(&stale_op_id).is_none(),
            "a re-fenced stale ship must not poison dedup"
        );
    }
}
