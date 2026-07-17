//! Durable HA ownership coordination beside the data database.
//!
//! Version 1 is a plain-text `<writer_epoch> <node_id>` marker. Version 2 uses
//! the following conditional-write state machine:
//!
//! `Active/legacy/absent -> Claiming -> Opening -> Active`.
//!
//! Claiming and Opening contain renewable revisions. They are recoverable only
//! after the record and object version remain unchanged for the staleness
//! interval. Claiming then waits [`CLAIM_GRACE`] before entering Opening. Every
//! transition uses the object version returned by the preceding operation.
//! Generation and incarnation are logical fencing tokens; ETag/object version
//! is the storage compare-and-swap token.

use serde::{Deserialize, Serialize};
use slatedb::object_store::{
    Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path,
};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

const LEADER_RECORD_MARKER: &str = ".zerofs_ha_leader";
const FORMAT_VERSION: u8 = 2;

/// Covers the previous owner's cached authority, final validation, and response drain.
pub const CLAIM_GRACE: Duration = crate::replication::AUTHORITY_TTL
    .saturating_add(crate::replication::FINAL_MARKER_RECOVERY_TIMEOUT)
    .saturating_add(crate::replication::RESPONSE_DRAIN_TIMEOUT)
    .saturating_add(Duration::from_secs(1));

/// Renewal interval for Claiming and Opening records.
const HANDOFF_RENEW_INTERVAL: Duration = Duration::from_secs(1);
const HANDOFF_TRANSITION_RETRY: Duration = Duration::from_millis(100);

/// Required unchanged interval before a Claiming or Opening record is recoverable.
pub const HANDOFF_STALE_AFTER: Duration = HANDOFF_RENEW_INTERVAL.saturating_mul(5);

fn record_path(db_path: &str) -> Path {
    Path::from(db_path).join(LEADER_RECORD_MARKER)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LegacyRecord {
    epoch: u64,
    node_id: String,
}

impl LegacyRecord {
    fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("invalid HA leader record: {e}"))?;
        // Preserve the complete durable node identity after the delimiter.
        let (epoch, node_id) = text
            .split_once(' ')
            .ok_or_else(|| anyhow::anyhow!("invalid HA leader record: {text:?}"))?;
        let epoch = epoch
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid HA leader record epoch: {e}"))?;
        Ok(Self {
            epoch,
            node_id: node_id.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    node_id: String,
    incarnation: String,
}

impl Owner {
    fn new(node_id: String) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !node_id.trim().is_empty(),
            "HA leader-record owner node_id must not be empty"
        );
        Ok(Self {
            node_id,
            incarnation: Uuid::new_v4().to_string(),
        })
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.node_id.trim().is_empty(),
            "invalid v2 HA leader record: owner node_id is empty"
        );
        let incarnation = Uuid::parse_str(&self.incarnation)
            .map_err(|e| anyhow::anyhow!("invalid v2 HA leader incarnation: {e}"))?;
        anyhow::ensure!(
            !incarnation.is_nil(),
            "invalid v2 HA leader record: owner incarnation is nil"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalOwner {
    node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incarnation: Option<String>,
}

impl HistoricalOwner {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.node_id.trim().is_empty(),
            "invalid v2 HA leader record: last_active node_id is empty"
        );
        if let Some(incarnation) = &self.incarnation {
            let incarnation = Uuid::parse_str(incarnation)
                .map_err(|e| anyhow::anyhow!("invalid v2 HA last-active incarnation: {e}"))?;
            anyhow::ensure!(
                !incarnation.is_nil(),
                "invalid v2 HA leader record: last-active incarnation is nil"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LastActive {
    generation: u64,
    owner: HistoricalOwner,
    writer_epoch: u64,
}

impl LastActive {
    fn from_legacy(record: &LegacyRecord) -> Self {
        Self {
            // V1 writer epochs seed the monotonic V2 generation.
            generation: record.epoch,
            owner: HistoricalOwner {
                node_id: record.node_id.clone(),
                incarnation: None,
            },
            writer_epoch: record.epoch,
        }
    }

    fn from_active(generation: u64, owner: &Owner, writer_epoch: u64) -> Self {
        Self {
            generation,
            owner: HistoricalOwner {
                node_id: owner.node_id.clone(),
                incarnation: Some(owner.incarnation.clone()),
            },
            writer_epoch,
        }
    }

    fn validate(&self, current_generation: u64, writer_epoch_floor: u64) -> anyhow::Result<()> {
        self.owner.validate()?;
        anyhow::ensure!(
            self.generation < current_generation,
            "invalid v2 HA leader record: last_active generation {} is not below current generation {}",
            self.generation,
            current_generation
        );
        anyhow::ensure!(
            self.writer_epoch <= writer_epoch_floor,
            "invalid v2 HA leader record: last_active writer epoch {} exceeds floor {}",
            self.writer_epoch,
            writer_epoch_floor
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Handoff {
    owner: Owner,
    writer_epoch_floor: u64,
    /// Monotonic liveness revision for this exact generation/incarnation.
    /// Zero is accepted for markers written by binaries predating leases.
    #[serde(default)]
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_active: Option<LastActive>,
}

impl Handoff {
    fn validate(&self, generation: u64) -> anyhow::Result<()> {
        self.owner.validate()?;
        if let Some(last_active) = &self.last_active {
            last_active.validate(generation, self.writer_epoch_floor)?;
        }
        Ok(())
    }

    fn is_same_attempt_as(&self, current: &Self) -> bool {
        self.owner == current.owner
            && self.writer_epoch_floor == current.writer_epoch_floor
            && self.last_active == current.last_active
            && current.revision >= self.revision
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum V2State {
    Claiming(Handoff),
    Opening(Handoff),
    Active { owner: Owner, writer_epoch: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct V2Record {
    version: u8,
    generation: u64,
    state: V2State,
}

impl V2Record {
    fn encode(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        let record: Self = serde_json::from_str(text)
            .map_err(|e| anyhow::anyhow!("invalid v2 HA leader record: {e}"))?;
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.version == FORMAT_VERSION,
            "unsupported HA leader record version {}",
            self.version
        );
        anyhow::ensure!(
            self.generation > 0,
            "invalid v2 HA leader record: generation is zero"
        );
        match &self.state {
            V2State::Claiming(handoff) | V2State::Opening(handoff) => {
                handoff.validate(self.generation)?
            }
            V2State::Active {
                owner,
                writer_epoch,
            } => {
                owner.validate()?;
                anyhow::ensure!(
                    *writer_epoch > 0,
                    "invalid v2 HA leader record: active writer epoch is zero"
                );
            }
        }
        Ok(())
    }

    fn renewed_handoff(&self) -> anyhow::Result<Self> {
        let mut renewed = self.clone();
        let revision = match &mut renewed.state {
            V2State::Claiming(handoff) | V2State::Opening(handoff) => &mut handoff.revision,
            V2State::Active { .. } => {
                anyhow::bail!("cannot renew an Active HA leader record as an in-progress handoff")
            }
        };
        *revision = revision.checked_add(1).ok_or_else(|| {
            anyhow::anyhow!(
                "cannot renew HA handoff generation {}: revision exhausted",
                self.generation
            )
        })?;
        Ok(renewed)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StoredRecord {
    Legacy(LegacyRecord),
    V2(V2Record),
}

impl StoredRecord {
    fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("invalid HA leader record: {e}"))?;
        if text.trim_start().starts_with('{') {
            Ok(Self::V2(V2Record::parse(text)?))
        } else {
            Ok(Self::Legacy(LegacyRecord::parse(bytes)?))
        }
    }

    fn phase(&self) -> LeaderRecordPhase {
        match self {
            Self::Legacy(_) => LeaderRecordPhase::Legacy,
            Self::V2(record) => match record.state {
                V2State::Claiming(_) => LeaderRecordPhase::Claiming,
                V2State::Opening(_) => LeaderRecordPhase::Opening,
                V2State::Active { .. } => LeaderRecordPhase::Active,
            },
        }
    }

    fn generation(&self) -> u64 {
        match self {
            Self::Legacy(record) => record.epoch,
            Self::V2(record) => record.generation,
        }
    }

    fn owner_node(&self) -> &str {
        match self {
            Self::Legacy(record) => &record.node_id,
            Self::V2(record) => match &record.state {
                V2State::Claiming(Handoff { owner, .. })
                | V2State::Opening(Handoff { owner, .. })
                | V2State::Active { owner, .. } => &owner.node_id,
            },
        }
    }

    fn latest_writer(&self) -> Option<(u64, &str)> {
        match self {
            Self::Legacy(record) => Some((record.epoch, &record.node_id)),
            Self::V2(record) => match &record.state {
                V2State::Active {
                    owner,
                    writer_epoch,
                } => Some((*writer_epoch, &owner.node_id)),
                V2State::Claiming(handoff) | V2State::Opening(handoff) => handoff
                    .last_active
                    .as_ref()
                    .map(|active| (active.writer_epoch, active.owner.node_id.as_str())),
            },
        }
    }

    fn startup_blockers(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        match self {
            Self::Legacy(record) => blockers.push(record.node_id.clone()),
            Self::V2(record) => match &record.state {
                V2State::Active { owner, .. } => blockers.push(owner.node_id.clone()),
                V2State::Claiming(handoff) | V2State::Opening(handoff) => {
                    blockers.push(handoff.owner.node_id.clone());
                    if let Some(last_active) = &handoff.last_active
                        && last_active.owner.node_id != handoff.owner.node_id
                    {
                        blockers.push(last_active.owner.node_id.clone());
                    }
                }
            },
        }
        blockers
    }

    fn claim_base(&self) -> (u64, u64, Option<LastActive>) {
        match self {
            Self::Legacy(record) => (
                record.epoch,
                record.epoch,
                Some(LastActive::from_legacy(record)),
            ),
            Self::V2(record) => match &record.state {
                V2State::Active {
                    owner,
                    writer_epoch,
                } => (
                    record.generation,
                    *writer_epoch,
                    Some(LastActive::from_active(
                        record.generation,
                        owner,
                        *writer_epoch,
                    )),
                ),
                V2State::Claiming(handoff) | V2State::Opening(handoff) => (
                    record.generation,
                    handoff.writer_epoch_floor,
                    handoff.last_active.clone(),
                ),
            },
        }
    }
}

#[derive(Clone, Debug)]
struct RecordSnapshot {
    record: StoredRecord,
    version: UpdateVersion,
}

fn same_snapshot(left: &RecordSnapshot, right: &RecordSnapshot) -> bool {
    left.record == right.record
        && left.version.e_tag == right.version.e_tag
        && left.version.version == right.version.version
}

fn has_version(version: &UpdateVersion) -> bool {
    version.e_tag.is_some() || version.version.is_some()
}

async fn read_snapshot(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> anyhow::Result<Option<RecordSnapshot>> {
    match object_store.get(path).await {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result.bytes().await?;
            Ok(Some(RecordSnapshot {
                record: StoredRecord::parse(&bytes)?,
                version,
            }))
        }
        Err(Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("reading the HA leader record failed: {e}")),
    }
}

/// Public durable phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaderRecordPhase {
    Legacy,
    Claiming,
    Opening,
    Active,
}

/// Read-only, owned view used by startup role election.
#[derive(Clone, Debug)]
pub struct Inspection {
    record: Option<StoredRecord>,
}

#[allow(dead_code)] // Some diagnostics are currently exercised only by tests.
impl Inspection {
    pub fn phase(&self) -> Option<LeaderRecordPhase> {
        self.record.as_ref().map(StoredRecord::phase)
    }

    pub fn generation(&self) -> Option<u64> {
        self.record.as_ref().map(StoredRecord::generation)
    }

    /// Node identities that block startup on peer silence.
    pub fn startup_blockers(&self) -> Vec<String> {
        self.record
            .as_ref()
            .map(StoredRecord::startup_blockers)
            .unwrap_or_default()
    }

    /// Last term that reached the legacy marker or v2 Active phase.
    pub fn latest_writer(&self) -> Option<(u64, String)> {
        self.record
            .as_ref()
            .and_then(StoredRecord::latest_writer)
            .map(|(epoch, node)| (epoch, node.to_string()))
    }
}

pub async fn inspect(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<Inspection> {
    Ok(Inspection {
        record: read_snapshot(object_store, &record_path(db_path))
            .await?
            .map(|snapshot| snapshot.record),
    })
}

/// Compatibility view of the last legacy or V2 Active term.
#[allow(dead_code)] // Compatibility view retained for callers during migration.
pub async fn read(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<Option<(u64, String)>> {
    Ok(inspect(object_store, db_path).await?.latest_writer())
}

/// The requested claim was not installed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("HA leader claim rejected by {phase:?} generation {generation:?} owned by {owner:?}")]
pub struct ClaimRejected {
    phase: Option<LeaderRecordPhase>,
    generation: Option<u64>,
    owner: Option<String>,
}

#[allow(dead_code)] // Structured diagnostics are useful to external callers/tests.
impl ClaimRejected {
    fn from_snapshot(snapshot: Option<&RecordSnapshot>) -> Self {
        Self {
            phase: snapshot.map(|snapshot| snapshot.record.phase()),
            generation: snapshot.map(|snapshot| snapshot.record.generation()),
            owner: snapshot.map(|snapshot| snapshot.record.owner_node().to_string()),
        }
    }

    pub fn phase(&self) -> Option<LeaderRecordPhase> {
        self.phase
    }

    pub fn generation(&self) -> Option<u64> {
        self.generation
    }

    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }
}

/// Loss of the generation/incarnation capability being advanced.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "lost HA leader-record ownership for generation {generation} node {node_id:?} incarnation {incarnation}"
)]
pub struct OwnershipLost {
    generation: u64,
    node_id: String,
    incarnation: String,
}

impl OwnershipLost {
    fn new(generation: u64, owner: &Owner) -> Self {
        Self {
            generation,
            node_id: owner.node_id.clone(),
            incarnation: owner.incarnation.clone(),
        }
    }
}

#[derive(Debug)]
enum ExactWriteOutcome {
    Applied(RecordSnapshot),
    Changed(Option<RecordSnapshot>),
}

/// Performs one storage CAS. A reread accepts only the exact desired value;
/// otherwise it reports the observed record.
async fn write_v2_exact(
    object_store: &Arc<dyn ObjectStore>,
    path: &Path,
    desired: &V2Record,
    mode: PutMode,
    require_version: bool,
) -> anyhow::Result<ExactWriteOutcome> {
    desired.validate()?;
    let payload = desired.encode()?;
    match object_store
        .put_opts(path, payload.into(), PutOptions::from(mode))
        .await
    {
        Ok(result) => {
            let version = UpdateVersion {
                e_tag: result.e_tag,
                version: result.version,
            };
            if !require_version || has_version(&version) {
                return Ok(ExactWriteOutcome::Applied(RecordSnapshot {
                    record: StoredRecord::V2(desired.clone()),
                    version,
                }));
            }

            let current = read_snapshot(object_store, path).await?;
            match current {
                Some(snapshot) if snapshot.record == StoredRecord::V2(desired.clone()) => {
                    anyhow::ensure!(
                        has_version(&snapshot.version),
                        "HA leader record update returned neither an ETag nor an object version"
                    );
                    Ok(ExactWriteOutcome::Applied(snapshot))
                }
                other => Ok(ExactWriteOutcome::Changed(other)),
            }
        }
        Err(write_error) => {
            // Reconcile a conditional write whose response may have been lost.
            let current = read_snapshot(object_store, path).await.map_err(|read_error| {
                anyhow::anyhow!(
                    "writing the HA leader record failed: {write_error}; rereading it to reconcile the ambiguous result also failed: {read_error:#}"
                )
            })?;
            if let Some(snapshot) = current.as_ref()
                && snapshot.record == StoredRecord::V2(desired.clone())
            {
                if require_version && !has_version(&snapshot.version) {
                    anyhow::bail!(
                        "HA leader record update returned neither an ETag nor an object version"
                    );
                }
                return Ok(ExactWriteOutcome::Applied(
                    current.expect("matched snapshot exists"),
                ));
            }

            if matches!(
                write_error,
                Error::AlreadyExists { .. } | Error::Precondition { .. } | Error::NotFound { .. }
            ) {
                Ok(ExactWriteOutcome::Changed(current))
            } else {
                Err(anyhow::anyhow!(
                    "writing the HA leader record failed before the desired transition was visible: {write_error}"
                ))
            }
        }
    }
}

struct HandoffState {
    record: V2Record,
    version: UpdateVersion,
    ownership_lost: bool,
    renewal_failure: Option<String>,
}

/// Reconciles a non-regressing revision for the same generation and incarnation.
fn reconcile_own_handoff_revision(
    state: &mut HandoffState,
    snapshot: RecordSnapshot,
) -> anyhow::Result<bool> {
    let StoredRecord::V2(current) = &snapshot.record else {
        return Ok(false);
    };
    let same_attempt = current.generation == state.record.generation
        && match (&state.record.state, &current.state) {
            (V2State::Claiming(expected), V2State::Claiming(current))
            | (V2State::Opening(expected), V2State::Opening(current)) => {
                expected.is_same_attempt_as(current)
            }
            _ => false,
        };
    if !same_attempt {
        return Ok(false);
    }
    anyhow::ensure!(
        has_version(&snapshot.version),
        "HA handoff record returned neither an ETag nor an object version"
    );
    state.record = current.clone();
    state.version = snapshot.version;
    Ok(true)
}

/// Renewable Claiming/Opening state. Renewals and transitions serialize on
/// `state` and use its latest object version.
struct HandoffLease {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    state: Arc<tokio::sync::Mutex<HandoffState>>,
    renewal_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    renewal_task: Option<tokio::task::JoinHandle<()>>,
    generation: u64,
    owner: Owner,
    claimed_at: tokio::time::Instant,
}

impl HandoffLease {
    fn start(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        record: V2Record,
        version: UpdateVersion,
    ) -> Self {
        let owner = match &record.state {
            V2State::Claiming(handoff) | V2State::Opening(handoff) => handoff.owner.clone(),
            V2State::Active { .. } => unreachable!("handoff lease cannot contain Active"),
        };
        let generation = record.generation;
        let state = Arc::new(tokio::sync::Mutex::new(HandoffState {
            record,
            version,
            ownership_lost: false,
            renewal_failure: None,
        }));
        let (renewal_shutdown, mut shutdown) = tokio::sync::oneshot::channel();
        let renewal_store = Arc::clone(&object_store);
        let renewal_path = path.clone();
        let renewal_state = Arc::clone(&state);
        let renewal_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(HANDOFF_RENEW_INTERVAL) => {}
                }

                // Serialize renewal with phase transitions and validation.
                let mut state = renewal_state.lock().await;
                if state.ownership_lost || state.renewal_failure.is_some() {
                    return;
                }
                let desired = match state.record.renewed_handoff() {
                    Ok(desired) => desired,
                    Err(error) => {
                        state.renewal_failure = Some(error.to_string());
                        return;
                    }
                };
                let mode = PutMode::Update(state.version.clone());
                match write_v2_exact(&renewal_store, &renewal_path, &desired, mode, true).await {
                    Ok(ExactWriteOutcome::Applied(snapshot)) => {
                        state.record = desired;
                        state.version = snapshot.version;
                    }
                    Ok(ExactWriteOutcome::Changed(actual)) => {
                        if let Some(snapshot) = actual {
                            match reconcile_own_handoff_revision(&mut state, snapshot) {
                                Ok(true) => continue,
                                Ok(false) => {}
                                Err(error) => {
                                    state.renewal_failure = Some(error.to_string());
                                    return;
                                }
                            }
                        }
                        state.ownership_lost = true;
                        return;
                    }
                    Err(error) => {
                        // Retry the same revision and object version.
                        tracing::warn!(
                            "HA handoff marker renewal failed; generation={generation}; \
                             retrying: {error:#}"
                        );
                    }
                }
            }
        });

        Self {
            object_store,
            path,
            state,
            renewal_shutdown: Some(renewal_shutdown),
            renewal_task: Some(renewal_task),
            generation,
            owner,
            claimed_at: tokio::time::Instant::now(),
        }
    }

    fn ownership_lost(&self) -> OwnershipLost {
        OwnershipLost::new(self.generation, &self.owner)
    }

    async fn stop_renewing(&mut self) -> anyhow::Result<()> {
        if let Some(shutdown) = self.renewal_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.renewal_task.take() {
            task.await.map_err(|error| {
                anyhow::anyhow!(
                    "HA handoff generation {} renewal task failed: {error}",
                    self.generation
                )
            })?;
        }
        Ok(())
    }
}

impl Drop for HandoffLease {
    fn drop(&mut self) {
        if let Some(shutdown) = self.renewal_shutdown.take() {
            let _ = shutdown.send(());
        }
        // Detach any bounded in-flight request; aborting would make its CAS result ambiguous.
        self.renewal_task.take();
    }
}

/// Shared representation for typed Claiming and Opening capabilities.
#[doc(hidden)]
pub struct HandoffToken<const OPENING: bool> {
    lease: HandoffLease,
}

#[allow(dead_code)] // Optional token diagnostics.
impl<const OPENING: bool> HandoffToken<OPENING> {
    pub fn generation(&self) -> u64 {
        self.lease.generation
    }

    pub fn node_id(&self) -> &str {
        &self.lease.owner.node_id
    }

    pub fn incarnation(&self) -> &str {
        &self.lease.owner.incarnation
    }
}

/// Opaque renewable capability for the exact durable Claiming generation.
pub type ClaimToken = HandoffToken<false>;

/// Opaque renewable capability for the exact Opening generation.
pub type OpeningToken = HandoffToken<true>;

/// Validates Opening ownership and refreshes its object version.
pub async fn validate_opening(token: &OpeningToken) -> anyhow::Result<bool> {
    let mut state = token.lease.state.lock().await;
    if state.ownership_lost || state.renewal_failure.is_some() {
        return Ok(false);
    }
    let current = read_snapshot(&token.lease.object_store, &token.lease.path).await?;
    let Some(snapshot) = current else {
        state.ownership_lost = true;
        return Ok(false);
    };
    if !reconcile_own_handoff_revision(&mut state, snapshot)? {
        state.ownership_lost = true;
        return Ok(false);
    }
    Ok(true)
}

/// Exact V2 Active identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveOwnership {
    record: V2Record,
}

#[allow(dead_code)] // Identity diagnostics are not needed by the serving path.
impl ActiveOwnership {
    fn active(&self) -> (&Owner, u64) {
        let V2State::Active {
            owner,
            writer_epoch,
        } = &self.record.state
        else {
            unreachable!("ActiveOwnership always contains an Active record")
        };
        (owner, *writer_epoch)
    }

    pub fn generation(&self) -> u64 {
        self.record.generation
    }

    pub fn node_id(&self) -> &str {
        self.active().0.node_id.as_str()
    }

    pub fn incarnation(&self) -> &str {
        self.active().0.incarnation.as_str()
    }

    pub fn writer_epoch(&self) -> u64 {
        self.active().1
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        generation: u64,
        node_id: &str,
        incarnation: &str,
        writer_epoch: u64,
    ) -> Self {
        Self {
            record: V2Record {
                version: FORMAT_VERSION,
                generation,
                state: V2State::Active {
                    owner: Owner {
                        node_id: node_id.to_string(),
                        incarnation: incarnation.to_string(),
                    },
                    writer_epoch,
                },
            },
        }
    }
}

/// Installs a Claiming record by conditional write. Without `force`, an
/// unchanged Claiming/Opening record is recoverable after
/// [`HANDOFF_STALE_AFTER`]. Force skips that observation interval. A failed CAS
/// returns [`ClaimRejected`] without retrying against the observed generation.
pub async fn claim(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    node_id: &str,
    force: bool,
) -> anyhow::Result<ClaimToken> {
    claim_inner(object_store, db_path, node_id, force, false).await
}

/// Recovers only a Claiming or Opening record. Active, legacy, and absent
/// records return [`ClaimRejected`].
pub async fn recover_handoff(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    node_id: &str,
) -> anyhow::Result<ClaimToken> {
    claim_inner(object_store, db_path, node_id, false, true).await
}

async fn claim_inner(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    node_id: &str,
    force: bool,
    recovery_only: bool,
) -> anyhow::Result<ClaimToken> {
    debug_assert!(!(force && recovery_only));
    let path = record_path(db_path);
    let mut current = read_snapshot(object_store, &path).await?;
    let handoff_in_progress = current.as_ref().is_some_and(|snapshot| {
        matches!(
            snapshot.record.phase(),
            LeaderRecordPhase::Claiming | LeaderRecordPhase::Opening
        )
    });

    if recovery_only && !handoff_in_progress {
        return Err(ClaimRejected::from_snapshot(current.as_ref()).into());
    }

    if !force && handoff_in_progress {
        let observed = current
            .as_ref()
            .expect("phase check requires an in-progress snapshot")
            .clone();
        tokio::time::sleep(HANDOFF_STALE_AFTER).await;
        let refreshed = read_snapshot(object_store, &path).await?;
        if !refreshed
            .as_ref()
            .is_some_and(|snapshot| same_snapshot(&observed, snapshot))
        {
            return Err(ClaimRejected::from_snapshot(refreshed.as_ref()).into());
        }
        current = refreshed;
    }

    let (generation, writer_epoch_floor, last_active) = match current.as_ref() {
        Some(snapshot) => snapshot.record.claim_base(),
        None => (0, 0, None),
    };
    let generation = generation.checked_add(1).ok_or_else(|| {
        anyhow::anyhow!("cannot claim HA leadership: leader-record generation exhausted")
    })?;
    let owner = Owner::new(node_id.to_string())?;
    let desired = V2Record {
        version: FORMAT_VERSION,
        generation,
        state: V2State::Claiming(Handoff {
            owner,
            writer_epoch_floor,
            revision: 1,
            last_active,
        }),
    };
    let mode = match current.as_ref() {
        Some(snapshot) => {
            anyhow::ensure!(
                has_version(&snapshot.version),
                "HA leader claim returned neither an ETag nor an object version"
            );
            PutMode::Update(snapshot.version.clone())
        }
        None => PutMode::Create,
    };

    match write_v2_exact(object_store, &path, &desired, mode, true).await? {
        ExactWriteOutcome::Applied(snapshot) => {
            let lease =
                HandoffLease::start(Arc::clone(object_store), path, desired, snapshot.version);
            Ok(HandoffToken { lease })
        }
        ExactWriteOutcome::Changed(actual) => {
            Err(ClaimRejected::from_snapshot(actual.as_ref()).into())
        }
    }
}

pub async fn begin_open(token: ClaimToken) -> anyhow::Result<OpeningToken> {
    begin_open_after(token, CLAIM_GRACE).await
}

async fn begin_open_after(token: ClaimToken, grace: Duration) -> anyhow::Result<OpeningToken> {
    if let Some(remaining) = grace.checked_sub(token.lease.claimed_at.elapsed()) {
        tokio::time::sleep(remaining).await;
    }

    let ownership_lost = token.lease.ownership_lost();
    let mut state = token.lease.state.lock().await;
    if state.ownership_lost {
        return Err(ownership_lost.into());
    }
    if let Some(error) = &state.renewal_failure {
        anyhow::bail!(
            "HA handoff generation {} cannot enter Opening after renewal failure: {error}",
            token.lease.generation
        );
    }
    loop {
        let renewed = state.record.renewed_handoff()?;
        let V2State::Claiming(handoff) = renewed.state else {
            unreachable!("ClaimToken always contains Claiming");
        };
        let desired = V2Record {
            version: FORMAT_VERSION,
            generation: token.lease.generation,
            state: V2State::Opening(handoff),
        };
        match write_v2_exact(
            &token.lease.object_store,
            &token.lease.path,
            &desired,
            PutMode::Update(state.version.clone()),
            true,
        )
        .await
        {
            Ok(ExactWriteOutcome::Applied(snapshot)) => {
                state.record = desired;
                state.version = snapshot.version;
                drop(state);
                return Ok(HandoffToken { lease: token.lease });
            }
            Ok(ExactWriteOutcome::Changed(actual)) => {
                if let Some(snapshot) = actual
                    && reconcile_own_handoff_revision(&mut state, snapshot)?
                {
                    continue;
                }
                state.ownership_lost = true;
                return Err(ownership_lost.into());
            }
            Err(error) => {
                tracing::warn!(
                    "HA Claiming-to-Opening transition failed; generation={}; \
                     retrying: {error:#}",
                    token.lease.generation
                );
                tokio::time::sleep(HANDOFF_TRANSITION_RETRY).await;
            }
        }
    }
}

/// Atomically publishes the writer epoch and activates the Opening owner.
pub async fn activate(
    mut token: OpeningToken,
    writer_epoch: u64,
) -> anyhow::Result<ActiveOwnership> {
    let ownership_lost = token.lease.ownership_lost();
    token.lease.stop_renewing().await?;
    let mut state = token.lease.state.lock().await;
    if state.ownership_lost {
        return Err(ownership_lost.into());
    }
    if let Some(error) = &state.renewal_failure {
        anyhow::bail!(
            "HA handoff generation {} cannot activate after renewal failure: {error}",
            token.lease.generation
        );
    }
    let V2State::Opening(handoff) = &state.record.state else {
        unreachable!("OpeningToken always contains Opening");
    };
    anyhow::ensure!(
        writer_epoch > handoff.writer_epoch_floor,
        "refusing to activate HA writer epoch {writer_epoch}: it does not advance the recorded floor {}",
        handoff.writer_epoch_floor
    );
    let desired = V2Record {
        version: FORMAT_VERSION,
        generation: token.lease.generation,
        state: V2State::Active {
            owner: handoff.owner.clone(),
            writer_epoch,
        },
    };
    let ownership = ActiveOwnership {
        record: desired.clone(),
    };
    loop {
        match write_v2_exact(
            &token.lease.object_store,
            &token.lease.path,
            &desired,
            PutMode::Update(state.version.clone()),
            false,
        )
        .await
        {
            Ok(ExactWriteOutcome::Applied(snapshot)) => {
                state.record = desired;
                state.version = snapshot.version;
                return Ok(ownership);
            }
            Ok(ExactWriteOutcome::Changed(actual)) => {
                if let Some(snapshot) = actual
                    && reconcile_own_handoff_revision(&mut state, snapshot)?
                {
                    continue;
                }
                state.ownership_lost = true;
                return Err(ownership_lost.into());
            }
            Err(error) => {
                // Retry the same Active identity; a reread reconciles a lost response.
                tracing::warn!(
                    "HA Opening-to-Active transition failed; generation={}; \
                     retrying: {error:#}",
                    token.lease.generation
                );
                tokio::time::sleep(HANDOFF_TRANSITION_RETRY).await;
            }
        }
    }
}

/// Returns true only for the exact V2 Active generation, owner, incarnation,
/// and writer epoch. Read and parse failures propagate.
pub async fn validate_active(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    ownership: &ActiveOwnership,
) -> anyhow::Result<bool> {
    Ok(read_snapshot(object_store, &record_path(db_path))
        .await?
        .is_some_and(|snapshot| snapshot.record == StoredRecord::V2(ownership.record.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault_store::{FaultControls, FaultStore};
    use crate::retrying_object_store::RetryingObjectStore;
    use slatedb::object_store::memory::InMemory;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn retrying_fault_store() -> (Arc<dyn ObjectStore>, Arc<FaultControls>) {
        let (store, faults) = FaultStore::new(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = store;
        (
            Arc::new(RetryingObjectStore::new(store)) as Arc<dyn ObjectStore>,
            faults,
        )
    }

    async fn seed_legacy(store: &Arc<dyn ObjectStore>, db_path: &str, epoch: u64, node_id: &str) {
        store
            .put(&record_path(db_path), format!("{epoch} {node_id}").into())
            .await
            .unwrap();
    }

    async fn open_now(token: ClaimToken) -> OpeningToken {
        begin_open_after(token, Duration::ZERO).await.unwrap()
    }

    #[tokio::test]
    async fn legacy_decodes_and_preserves_identity_whitespace() {
        let store = store();
        assert_eq!(read(&store, "db").await.unwrap(), None);

        seed_legacy(&store, "db", 7, "node-a \t").await;
        assert_eq!(
            read(&store, "db").await.unwrap(),
            Some((7, "node-a \t".to_string()))
        );
        let inspection = inspect(&store, "db").await.unwrap();
        assert_eq!(inspection.phase(), Some(LeaderRecordPhase::Legacy));
        assert_eq!(inspection.startup_blockers(), vec!["node-a \t"]);
    }

    #[tokio::test]
    async fn legacy_migrates_through_claiming_opening_and_active() {
        let store = store();
        seed_legacy(&store, "db", 7, "node-a").await;

        let claim = claim(&store, "db", "node-b", false).await.unwrap();
        assert_eq!(claim.generation(), 8);
        let inspection = inspect(&store, "db").await.unwrap();
        assert_eq!(inspection.phase(), Some(LeaderRecordPhase::Claiming));
        assert_eq!(
            inspection.startup_blockers(),
            vec!["node-b".to_string(), "node-a".to_string()]
        );
        assert_eq!(inspection.latest_writer(), Some((7, "node-a".into())));

        let opening = open_now(claim).await;
        let inspection = inspect(&store, "db").await.unwrap();
        assert_eq!(inspection.phase(), Some(LeaderRecordPhase::Opening));

        let active = activate(opening, 8).await.unwrap();
        assert_eq!(active.generation(), 8);
        assert_eq!(active.node_id(), "node-b");
        assert_eq!(active.writer_epoch(), 8);
        assert!(validate_active(&store, "db", &active).await.unwrap());
        assert_eq!(
            read(&store, "db").await.unwrap(),
            Some((8, "node-b".to_string()))
        );
    }

    #[tokio::test]
    async fn simultaneous_claim_loser_does_not_leapfrog() {
        let store = store();
        let first = claim(&store, "db", "node-a", false).await.unwrap();

        let error = claim(&store, "db", "node-b", false)
            .await
            .err()
            .expect("second claim must be rejected");
        let rejected = error.downcast_ref::<ClaimRejected>().unwrap();
        assert_eq!(rejected.phase(), Some(LeaderRecordPhase::Claiming));
        assert_eq!(rejected.owner(), Some("node-a"));
        assert_eq!(rejected.generation(), Some(1));

        let opening = open_now(first).await;
        let active = activate(opening, 1).await.unwrap();
        assert!(validate_active(&store, "db", &active).await.unwrap());
    }

    #[tokio::test]
    async fn retrying_store_preserves_inspection_and_claim_across_transient_errors() {
        let (store, faults) = retrying_fault_store();

        faults.fail_gets(1);
        assert_eq!(inspect(&store, "db").await.unwrap().phase(), None);

        faults.fail_gets(1);
        faults.fail_puts(1);
        let claim = claim(&store, "db", "node-a", false)
            .await
            .expect("transient marker errors must not abort the claim");
        assert_eq!(claim.generation(), 1);
        assert_eq!(
            inspect(&store, "db").await.unwrap().phase(),
            Some(LeaderRecordPhase::Claiming)
        );
    }

    #[tokio::test]
    async fn retrying_store_reconciles_an_applied_claim_with_a_lost_response() {
        let (store, faults) = retrying_fault_store();
        faults.fail_puts_after_apply(1);

        let claim = claim(&store, "db", "node-a", false)
            .await
            .expect("the exact applied claim must survive a lost response");
        assert_eq!(claim.generation(), 1);
        assert_eq!(faults.put_count(), 2);
        assert_eq!(
            inspect(&store, "db").await.unwrap().phase(),
            Some(LeaderRecordPhase::Claiming)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn opening_waits_full_claim_grace() {
        let store = store();
        let predecessor_lease = crate::replication::Lease::new();
        assert!(predecessor_lease.activate_from(
            tokio::time::Instant::now(),
            crate::replication::AUTHORITY_TTL,
        ));
        let claim = claim(&store, "db", "node-b", false).await.unwrap();
        let opening = tokio::spawn(begin_open(claim));
        tokio::task::yield_now().await;

        let covered = crate::replication::AUTHORITY_TTL
            + crate::replication::FINAL_MARKER_RECOVERY_TIMEOUT
            + crate::replication::RESPONSE_DRAIN_TIMEOUT;
        tokio::time::advance(covered + Duration::from_millis(1)).await;
        assert!(
            !predecessor_lease.is_valid(),
            "the predecessor's cached authority must expire during Claiming"
        );
        assert!(
            !opening.is_finished(),
            "Opening must remain unavailable after authority, final recovery, and drain"
        );

        tokio::time::advance(CLAIM_GRACE - covered - Duration::from_millis(1)).await;
        opening
            .await
            .expect("Opening task")
            .expect("exact claimant should enter Opening after the grace");
    }

    #[tokio::test]
    async fn live_opening_renews_and_force_recovery_can_supersede_it() {
        let store = store();
        seed_legacy(&store, "db", 7, "node-a").await;
        let old_opening = open_now(claim(&store, "db", "node-b", false).await.unwrap()).await;
        assert!(validate_opening(&old_opening).await.unwrap());

        let error = claim(&store, "db", "node-c", false)
            .await
            .err()
            .expect("Opening must reject a normal claim");
        let rejected = error.downcast_ref::<ClaimRejected>().unwrap();
        assert_eq!(rejected.phase(), Some(LeaderRecordPhase::Opening));
        assert_eq!(rejected.owner(), Some("node-b"));

        let recovery = claim(&store, "db", "node-c", true).await.unwrap();
        assert!(!validate_opening(&old_opening).await.unwrap());
        assert_eq!(recovery.generation(), 9);
        assert_eq!(
            inspect(&store, "db").await.unwrap().startup_blockers(),
            vec!["node-c".to_string(), "node-a".to_string()]
        );

        let error = activate(old_opening, 8).await.unwrap_err();
        assert!(error.downcast_ref::<OwnershipLost>().is_some());

        let recovered_opening = open_now(recovery).await;
        let error = activate(recovered_opening, 7).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not advance the recorded floor 7")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_claiming_is_replaced_after_exact_stale_observation() {
        let store = store();
        let abandoned = claim(&store, "db", "node-a", false).await.unwrap();
        let abandoned_generation = abandoned.generation();
        drop(abandoned);
        tokio::task::yield_now().await;

        let contender_store = Arc::clone(&store);
        let contender =
            tokio::spawn(async move { recover_handoff(&contender_store, "db", "node-b").await });
        tokio::task::yield_now().await;
        assert!(!contender.is_finished());

        tokio::time::advance(HANDOFF_STALE_AFTER).await;
        let replacement = contender
            .await
            .expect("contender task")
            .expect("an abandoned exact Claiming snapshot must be recoverable");
        assert_eq!(replacement.generation(), abandoned_generation + 1);
        assert_eq!(replacement.node_id(), "node-b");
    }

    #[tokio::test(start_paused = true)]
    async fn only_one_contender_can_recover_an_abandoned_handoff_snapshot() {
        let (store, faults) = retrying_fault_store();
        drop(claim(&store, "db", "node-a", false).await.unwrap());
        tokio::task::yield_now().await;

        let first_store = Arc::clone(&store);
        let first =
            tokio::spawn(async move { recover_handoff(&first_store, "db", "node-b").await });
        let second_store = Arc::clone(&store);
        let second =
            tokio::spawn(async move { recover_handoff(&second_store, "db", "node-c").await });
        tokio::task::yield_now().await;
        assert!(!first.is_finished() && !second.is_finished());

        faults.fail_puts(1);
        tokio::time::advance(HANDOFF_STALE_AFTER).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let first = first.await.expect("first contender task");
        let second = second.await.expect("second contender task");
        assert_eq!(
            usize::from(first.is_ok()) + usize::from(second.is_ok()),
            1,
            "the exact snapshot CAS must admit one replacement"
        );
        let loser = if first.is_err() { first } else { second };
        assert!(
            loser
                .err()
                .expect("one contender must lose")
                .downcast_ref::<ClaimRejected>()
                .is_some()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_opening_is_replaced_after_exact_stale_observation() {
        let store = store();
        let abandoned = open_now(claim(&store, "db", "node-a", false).await.unwrap()).await;
        let abandoned_generation = abandoned.generation();
        drop(abandoned);
        tokio::task::yield_now().await;

        let contender_store = Arc::clone(&store);
        let contender =
            tokio::spawn(async move { recover_handoff(&contender_store, "db", "node-b").await });
        tokio::task::yield_now().await;
        assert!(!contender.is_finished());

        tokio::time::advance(HANDOFF_STALE_AFTER).await;
        let replacement = contender
            .await
            .expect("contender task")
            .expect("an abandoned exact Opening snapshot must be recoverable");
        assert_eq!(replacement.generation(), abandoned_generation + 1);
        assert_eq!(replacement.node_id(), "node-b");
    }

    #[tokio::test(start_paused = true)]
    async fn handoff_recovery_cannot_replace_a_marker_that_reached_active() {
        let store = store();
        let original = claim(&store, "db", "node-a", false).await.unwrap();

        let contender_store = Arc::clone(&store);
        let recovery =
            tokio::spawn(async move { recover_handoff(&contender_store, "db", "node-b").await });
        tokio::task::yield_now().await;
        assert!(
            !recovery.is_finished(),
            "recovery must have observed Claiming and entered stale observation"
        );

        let active = activate(open_now(original).await, 1).await.unwrap();
        tokio::time::advance(HANDOFF_STALE_AFTER).await;
        let error = recovery
            .await
            .expect("recovery task")
            .err()
            .expect("Active must not be replaced by recovery-only claim");
        let rejected = error.downcast_ref::<ClaimRejected>().unwrap();
        assert_eq!(rejected.phase(), Some(LeaderRecordPhase::Active));
        assert!(validate_active(&store, "db", &active).await.unwrap());
    }

    #[tokio::test]
    async fn exact_active_validation_rejects_every_other_state_or_identity() {
        let store = store();
        let active = activate(
            open_now(claim(&store, "db", "node-a", false).await.unwrap()).await,
            1,
        )
        .await
        .unwrap();
        assert!(validate_active(&store, "db", &active).await.unwrap());

        let wrong = ActiveOwnership::for_test(
            active.generation(),
            active.node_id(),
            "00000000-0000-4000-8000-000000000001",
            active.writer_epoch(),
        );
        assert!(!validate_active(&store, "db", &wrong).await.unwrap());
        assert!(!validate_active(&store, "other-db", &active).await.unwrap());

        let _claim = claim(&store, "db", "node-b", false).await.unwrap();
        assert!(!validate_active(&store, "db", &active).await.unwrap());
    }

    #[tokio::test]
    async fn stale_claim_token_cannot_enter_opening_after_force_recovery() {
        let store = store();
        let stale = claim(&store, "db", "node-a", false).await.unwrap();
        let replacement = claim(&store, "db", "node-b", true).await.unwrap();

        let error = begin_open_after(stale, Duration::ZERO)
            .await
            .err()
            .expect("stale token must not enter Opening");
        assert!(error.downcast_ref::<OwnershipLost>().is_some());
        assert_eq!(
            inspect(&store, "db").await.unwrap().generation(),
            Some(replacement.generation())
        );
    }
}
