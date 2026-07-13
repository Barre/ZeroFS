//! Takeover reconciliation of the standby's in-memory replication tail.

use crate::frame_codec::FrameCodec;
use crate::fs::key_codec::KeyCodec;
use crate::replication::tail::{ReplOp, ReplayDecision, TailBuffer, replay_decision};
use crate::replication::transport::PromotionSnapshot;
use crate::segment::{FrameLoc, Segid};
use crate::segment_store::{ReconFrame, materialize_segment_if_absent};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

const MATERIALIZE_ATTEMPTS: u32 = 5;

/// Capability proving that takeover reconciliation covered the prior durability
/// lineage for this exact writer open. Only this module can construct one.
#[doc(hidden)]
pub struct LineageProof {
    writer_epoch: u64,
    raw_db: Arc<slatedb::Db>,
}

impl LineageProof {
    pub(crate) fn authorizes(self, raw_db: &Arc<slatedb::Db>, writer_epoch: u64) -> bool {
        self.writer_epoch == writer_epoch && Arc::ptr_eq(&self.raw_db, raw_db)
    }
}

impl PromotionSnapshot {
    /// Reconcile this frozen tail with durable state and finish promotion.
    /// Returns a proof only when every observed ship is accounted for.
    pub async fn reconcile_into(
        mut self,
        raw_db: &Arc<slatedb::Db>,
        segment_object_store: &Arc<dyn ObjectStore>,
        segment_codec: &FrameCodec,
    ) -> Result<Option<LineageProof>> {
        let observed_epoch = self.observed_epoch();
        let dedup = self.dedup();
        let key_codec = KeyCodec::new();
        let tail = self.tail_mut();

        reconcile_durable_head(raw_db, &key_codec, tail, &dedup).await;
        let preserves_lineage = tail.preserves_lineage(observed_epoch);
        if !tail.is_empty() {
            replay_remaining_tail(
                raw_db,
                &key_codec,
                tail,
                segment_object_store,
                segment_codec,
            )
            .await?;
            // The flush made the remaining suffix durable; only now publish its
            // op-ids.
            for op_id in tail.finish_replay() {
                dedup.record(op_id, Bytes::new());
            }
        }

        let writer_epoch = self
            .complete()
            .await
            .context("HA takeover: completing receiver promotion failed")?;
        Ok(preserves_lineage.then_some(LineageProof {
            writer_epoch,
            raw_db: raw_db.clone(),
        }))
    }
}

async fn reconcile_durable_head(
    raw_db: &slatedb::Db,
    key_codec: &KeyCodec,
    tail: &mut TailBuffer,
    dedup: &crate::dedup::DedupCache,
) {
    if tail.is_empty() {
        return;
    }

    // Replaying blind could regress durable state. An unreadable stamp therefore
    // discards the volatile tail, forfeiting only writes that were not fsynced.
    let decision = match raw_db.get(&key_codec.ha_seqno_key()).await {
        Ok(None) => replay_decision(None, tail.epoch()),
        Ok(Some(value)) => match KeyCodec::decode_ha_stamp(&value) {
            Some(stamp) => replay_decision(Some(stamp), tail.epoch()),
            None => {
                tracing::warn!("HA takeover: undecodable HA stamp; dropping the tail");
                ReplayDecision::Discard
            }
        },
        Err(error) => {
            tracing::warn!(
                "HA takeover: reading the HA stamp failed ({error}); dropping the tail rather \
                 than replaying over unseen durable state"
            );
            ReplayDecision::Discard
        }
    };

    match decision {
        ReplayDecision::ReplayAll => info!(
            "HA takeover: nothing of the tail's term (epoch {}) is durable; \
             replaying the full tail",
            tail.epoch()
        ),
        ReplayDecision::PruneTo(seqno) => {
            let before = tail.len();
            for op_id in tail.prune(seqno) {
                dedup.record(op_id, Bytes::new());
            }
            info!(
                "HA takeover: db holds epoch {} through seqno {}; pruned {} of {} buffered batches",
                tail.epoch(),
                seqno,
                before - tail.len(),
                before
            );
        }
        ReplayDecision::Discard => {
            info!(
                "HA takeover: durable state supersedes epoch {}'s {} buffered \
                 batches; dropping them",
                tail.epoch(),
                tail.len()
            );
            tail.discard();
        }
    }
}

async fn replay_remaining_tail(
    raw_db: &slatedb::Db,
    key_codec: &KeyCodec,
    tail: &TailBuffer,
    segment_object_store: &Arc<dyn ObjectStore>,
    segment_codec: &FrameCodec,
) -> Result<()> {
    info!(
        "HA takeover: replaying {} buffered batch(es) into the data db",
        tail.len()
    );
    let segments = write_tail_batches(raw_db, key_codec, tail).await?;
    materialize_segments(segment_object_store, segment_codec, &segments).await?;
    raw_db
        .flush()
        .await
        .context("HA takeover replay flush failed")
}

async fn write_tail_batches(
    raw_db: &slatedb::Db,
    key_codec: &KeyCodec,
    tail: &TailBuffer,
) -> Result<HashMap<Segid, Vec<ReconFrame>>> {
    let mut segments: HashMap<Segid, Vec<ReconFrame>> = HashMap::new();
    for (_seqno, ops) in tail.batches_in_order() {
        let mut batch = slatedb::WriteBatch::new();
        for op in ops {
            match op {
                ReplOp::Put(key, value) => batch.put_bytes(key.clone(), value.clone()),
                ReplOp::Delete(key) => batch.delete(key.clone()),
                ReplOp::PutFrame(key, value, frame) => {
                    batch.put_bytes(key.clone(), value.clone());
                    if let Some((inode, extent)) = key_codec.parse_extent_key_full(key)
                        && let Some(location) = FrameLoc::decode(value)
                    {
                        segments
                            .entry(location.segid)
                            .or_default()
                            .push(ReconFrame {
                                frame_index: location.frame_index,
                                byte_offset: location.byte_offset,
                                byte_len: location.byte_len,
                                inode,
                                extent,
                                bytes: frame.clone(),
                            });
                    }
                }
            }
        }
        raw_db
            .write_with_options(
                batch,
                &slatedb::config::WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .context("HA takeover tail replay failed")?;
    }
    Ok(segments)
}

async fn materialize_segments(
    segment_object_store: &Arc<dyn ObjectStore>,
    segment_codec: &FrameCodec,
    segments: &HashMap<Segid, Vec<ReconFrame>>,
) -> Result<()> {
    let mut materialized = 0;
    for (segid, frames) in segments {
        let mut attempt = 0;
        loop {
            match materialize_segment_if_absent(segment_object_store, segment_codec, *segid, frames)
                .await
            {
                Ok(created) => {
                    materialized += usize::from(created);
                    break;
                }
                Err(error) => {
                    attempt += 1;
                    if attempt >= MATERIALIZE_ATTEMPTS {
                        return Err(anyhow!(
                            "HA takeover: reconstructing segment {segid:?} failed after \
                             {MATERIALIZE_ATTEMPTS} attempts: {error}; refusing to commit \
                             dangling FrameLocs"
                        ));
                    }
                    tracing::warn!(
                        "HA takeover: reconstructing segment {segid:?} failed (attempt \
                         {attempt}/{MATERIALIZE_ATTEMPTS}): {error}; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt)))
                        .await;
                }
            }
        }
    }
    if materialized > 0 {
        info!("HA takeover: materialized {materialized} segment(s) from the replayed tail");
    }
    Ok(())
}
