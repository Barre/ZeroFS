//! Takeover reconciliation of the standby's in-memory replication tail.

use crate::frame_codec::FrameCodec;
use crate::fs::key_codec::KeyCodec;
use crate::replication::tail::{ReplOp, ReplayDecision, TailBuffer, replay_decision};
use crate::replication::transport::PromotionSnapshot;
use crate::replication::types::HaStamp;
use crate::segment::{FrameLoc, Segid};
use crate::segment_store::{ReconFrame, materialize_segment_if_absent};
use anyhow::{Context, Result, anyhow};
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

const MATERIALIZE_ATTEMPTS: u32 = 5;
const MATERIALIZE_RETRY_BASE: Duration = Duration::from_millis(200);

/// Proof that takeover reconciled lineage for this writer open.
#[doc(hidden)]
pub struct LineageProof {
    writer_epoch: u64,
    raw_db: Arc<slatedb::Db>,
}

/// Proof that direct-predecessor takeover retained every mutation result and
/// that the predecessor made no unreplicated commits.
#[doc(hidden)]
pub struct PromotionRetryGraceProof {
    dedup: Arc<crate::dedup::DedupCache>,
    predecessor_epoch: u64,
    deadline: Instant,
}

/// Takeover reconciliation result. `RetryRole` returns the tail to the receiver.
#[doc(hidden)]
pub enum ReconcileOutcome {
    Promoted {
        lineage_proof: Option<LineageProof>,
        retry_grace_proof: Option<PromotionRetryGraceProof>,
    },
    RetryRole {
        writer_epoch: u64,
        observed_epoch: u64,
    },
}

impl LineageProof {
    pub(crate) fn authorizes(self, raw_db: &Arc<slatedb::Db>, writer_epoch: u64) -> bool {
        self.writer_epoch == writer_epoch && Arc::ptr_eq(&self.raw_db, raw_db)
    }
}

impl PromotionRetryGraceProof {
    /// Arms the bounded retry window. Returns false after its deadline.
    pub fn arm(self) -> bool {
        self.dedup
            .arm_promotion_retry_grace_until(self.predecessor_epoch, self.deadline)
    }
}

fn proves_promotion_retry_grace(
    tail: &TailBuffer,
    observed_epoch: u64,
    durable_stamp: Option<HaStamp>,
) -> bool {
    let predecessor_ran_solo = durable_stamp.is_some_and(|stamp| {
        stamp.writer_epoch().get() == observed_epoch && stamp.solo_history().ran_solo()
    });
    tail.preserves_dedup_lineage(observed_epoch) && !predecessor_ran_solo
}

impl PromotionSnapshot {
    /// Reconciles a frozen tail with durable state and completes promotion.
    pub async fn reconcile_into(
        mut self,
        raw_db: &Arc<slatedb::Db>,
        segment_object_store: &Arc<dyn ObjectStore>,
        segment_codec: &FrameCodec,
    ) -> Result<ReconcileOutcome> {
        let observed_epoch = self.observed_epoch();
        let dedup = self.dedup();
        let key_codec = KeyCodec::new();
        // Retry grace is scoped to this promotion attempt.
        dedup.clear_promotion_retry_grace();

        // Check writer status even when the tail is empty.
        if let Some(reason) = raw_db.status().close_reason {
            return self.retry_role_after_close(reason).await;
        }

        let durable_stamp =
            match reconcile_durable_head(raw_db, &key_codec, self.tail_mut(), &dedup).await {
                Ok(stamp) => stamp,
                Err(error) => return self.retry_role_on_closed(error).await,
            };
        if self.tail_mut().has_uncovered_lineage(observed_epoch) {
            anyhow::bail!(
                "HA takeover: incomplete applied replication prefix for writer epoch \
                 {observed_epoch}"
            );
        }
        let preserves_lineage = self.tail_mut().preserves_lineage(observed_epoch);
        let retry_grace_lineage_proven =
            proves_promotion_retry_grace(self.tail_mut(), observed_epoch, durable_stamp);
        let mut retry_grace_deadline = dedup.promotion_retry_deadline();
        if let Some(deadline) = self.tail_mut().dedup_coverage_deadline() {
            retry_grace_deadline = retry_grace_deadline.min(deadline);
        }
        if !self.tail_mut().is_empty() {
            if let Err(error) = replay_remaining_tail(
                raw_db,
                &key_codec,
                self.tail_mut(),
                segment_object_store,
                segment_codec,
            )
            .await
            {
                return self.retry_role_on_closed(error).await;
            }
            // Publish dedup results after the replay flush.
            for entry in self.tail_mut().finish_replay() {
                if let Some(deadline) = dedup.record_entry_with_expiry(entry) {
                    retry_grace_deadline = retry_grace_deadline.min(deadline);
                }
            }
        }
        let retry_grace_proven = retry_grace_lineage_proven
            && dedup.promotion_retry_deadline_is_live(retry_grace_deadline);

        match self.complete().await {
            Ok(writer_epoch) => Ok(ReconcileOutcome::Promoted {
                lineage_proof: preserves_lineage.then_some(LineageProof {
                    writer_epoch,
                    raw_db: raw_db.clone(),
                }),
                retry_grace_proof: retry_grace_proven.then_some(PromotionRetryGraceProof {
                    dedup,
                    predecessor_epoch: observed_epoch,
                    deadline: retry_grace_deadline,
                }),
            }),
            Err(error) => {
                if let Some(superseded) =
                    error.downcast_ref::<crate::replication::transport::PromotionSuperseded>()
                {
                    Ok(ReconcileOutcome::RetryRole {
                        writer_epoch: superseded.writer_epoch,
                        observed_epoch: superseded.observed_epoch,
                    })
                } else {
                    Err(error).context("HA takeover: completing receiver promotion failed")
                }
            }
        }
    }

    /// Returns the tail to the receiver when the writer closes during replay.
    async fn retry_role_on_closed(self, error: anyhow::Error) -> Result<ReconcileOutcome> {
        let Some(reason) = closed_reason(&error) else {
            return Err(error);
        };
        self.retry_role_after_close(reason).await
    }

    async fn retry_role_after_close(
        self,
        reason: slatedb::CloseReason,
    ) -> Result<ReconcileOutcome> {
        let writer_epoch = self.writer_epoch();
        let observed_floor = match reason {
            slatedb::CloseReason::Fenced => writer_epoch.saturating_add(1),
            _ => writer_epoch,
        };
        let observed_epoch = self
            .return_to_standby(observed_floor)
            .await
            .context("HA takeover: restoring the tail after writer closure failed")?;
        tracing::warn!(
            "HA takeover writer closed; writer_epoch={writer_epoch}; reason={reason:?}; \
             observed_epoch={observed_epoch}; tail restored"
        );
        Ok(ReconcileOutcome::RetryRole {
            writer_epoch,
            observed_epoch,
        })
    }
}

fn closed_reason(error: &anyhow::Error) -> Option<slatedb::CloseReason> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<slatedb::Error>()
            .and_then(|error| match error.kind() {
                slatedb::ErrorKind::Closed(reason) => Some(reason),
                _ => None,
            })
    })
}

async fn reconcile_durable_head(
    raw_db: &slatedb::Db,
    key_codec: &KeyCodec,
    tail: &mut TailBuffer,
    dedup: &crate::dedup::DedupCache,
) -> Result<Option<HaStamp>> {
    // The durable stamp determines the replay boundary and writer status.
    let durable_stamp = match raw_db.get(&key_codec.ha_seqno_key()).await {
        Ok(None) => None,
        Ok(Some(value)) => match KeyCodec::decode_ha_stamp(&value) {
            Some(stamp) => Some(stamp),
            None => {
                anyhow::bail!("HA takeover: invalid durable HA stamp");
            }
        },
        Err(error) => {
            return Err(error).context("HA takeover: reading the durable HA stamp failed");
        }
    };
    let decision = replay_decision(durable_stamp.as_ref(), tail.epoch());

    match decision {
        ReplayDecision::PruneTo(seqno) => {
            let before = tail.len();
            let durable_entries = tail.prune(seqno);
            tail.publish_durable(dedup, durable_entries);
            if seqno == 0 {
                info!("HA takeover replaying full tail; epoch={}", tail.epoch());
            } else {
                info!(
                    "HA takeover tail pruned; epoch={}; applied_through={}; pruned={}; \
                     buffered={}; replay={}",
                    tail.epoch(),
                    seqno,
                    before - tail.len(),
                    before,
                    tail.len()
                );
            }
        }
        ReplayDecision::Discard => {
            info!(
                "HA takeover discarding superseded tail; epoch={}; batches={}",
                tail.epoch(),
                tail.len()
            );
            tail.discard();
        }
    }
    Ok(durable_stamp)
}

async fn replay_remaining_tail(
    raw_db: &slatedb::Db,
    key_codec: &KeyCodec,
    tail: &TailBuffer,
    segment_object_store: &Arc<dyn ObjectStore>,
    segment_codec: &FrameCodec,
) -> Result<()> {
    info!("HA takeover replay starting; batches={}", tail.len());
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
        // Empty batches carry dedup results but no database operations.
        if ops.is_empty() {
            continue;
        }
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
                            "HA takeover segment materialization failed; segment={segid:?}; \
                             attempts={MATERIALIZE_ATTEMPTS}: {error}"
                        ));
                    }
                    tracing::warn!(
                        "HA takeover segment materialization failed; segment={segid:?}; \
                         attempt={attempt}/{MATERIALIZE_ATTEMPTS}; retrying: {error}"
                    );
                    tokio::time::sleep(MATERIALIZE_RETRY_BASE.saturating_mul(attempt)).await;
                }
            }
        }
    }
    if materialized > 0 {
        info!("HA takeover segments materialized; count={materialized}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionConfig;
    use crate::dedup::{DedupEntry, DedupResult};
    use crate::fs::types::FileAttributes;
    use crate::replication::transport::ReplicationReceiver;
    use crate::replication::types::{ShipSeqno, SoloHistory, WriterEpoch};
    use crate::segment::SEGMENT_INFO;
    use bytes::Bytes;
    use slatedb::config::{PutOptions, WriteOptions};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn ha_stamp(
        writer_epoch: u64,
        last_shipped: u64,
        solo_history: SoloHistory,
        applied_through: u64,
    ) -> HaStamp {
        HaStamp::new(
            WriterEpoch::new(writer_epoch).unwrap(),
            ShipSeqno::new(last_shipped),
            solo_history,
            ShipSeqno::new(applied_through),
        )
        .unwrap()
    }

    fn encoded_ha_stamp(
        writer_epoch: u64,
        last_shipped: u64,
        solo_history: SoloHistory,
        applied_through: u64,
    ) -> Bytes {
        KeyCodec::encode_ha_stamp(&ha_stamp(
            writer_epoch,
            last_shipped,
            solo_history,
            applied_through,
        ))
    }

    fn create_entry(op: u8, inode_id: u64, mode: u32) -> DedupEntry {
        DedupEntry {
            op_id: [op; 16],
            result: DedupResult::Create {
                inode_id,
                attrs: FileAttributes {
                    mode,
                    fileid: inode_id,
                    ..FileAttributes::default()
                },
            },
        }
    }

    async fn open_db_with_durable_stamp(
        name: &str,
        stamp: Bytes,
    ) -> (Arc<dyn ObjectStore>, slatedb::Db, KeyCodec) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new(object_store::path::Path::from(name), store.clone())
            .build()
            .await
            .unwrap();
        let key_codec = KeyCodec::new();
        db.put_with_options(
            &key_codec.ha_seqno_key(),
            &stamp,
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        (store, db, key_codec)
    }

    #[tokio::test]
    async fn solo_takeover_replays_suffix_accepted_after_the_durable_head() {
        let (_store, db, key_codec) = open_db_with_durable_stamp(
            "solo-shipped-dedup",
            encoded_ha_stamp(7, 1, SoloHistory::ever(1), 1),
        )
        .await;
        db.put_with_options(
            &Bytes::from_static(b"shared-key"),
            &Bytes::from_static(b"newer-solo-value"),
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        let mut tail = TailBuffer::new();
        tail.accept(
            7,
            1,
            vec![ReplOp::Put(
                Bytes::from_static(b"shared-key"),
                Bytes::from_static(b"older-shipped-value"),
            )],
            vec![create_entry(1, 41, 0o640)],
        );
        tail.accept(
            7,
            2,
            vec![ReplOp::Put(
                Bytes::from_static(b"post-solo-suffix"),
                Bytes::from_static(b"recovered"),
            )],
            vec![create_entry(2, 42, 0o600)],
        );
        let dedup = crate::dedup::DedupCache::new();

        reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .unwrap();

        assert_eq!(
            tail.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![2],
            "only the suffix accepted after the durable Solo head must remain"
        );
        match dedup.get(&[1; 16]) {
            Some(DedupResult::Create { inode_id, attrs }) => {
                assert_eq!((inode_id, attrs.fileid, attrs.mode), (41, 41, 0o640));
            }
            other => panic!("takeover lost the acknowledged create result: {other:?}"),
        }
        assert!(
            dedup.get(&[2; 16]).is_none(),
            "the suffix result must wait for successful replay"
        );

        write_tail_batches(&db, &key_codec, &tail).await.unwrap();
        db.flush().await.unwrap();
        for entry in tail.finish_replay() {
            dedup.record_entry(entry);
        }
        assert_eq!(
            db.get(&Bytes::from_static(b"shared-key"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"newer-solo-value"[..]),
            "the applied prefix must not replay over newer Solo state"
        );
        assert_eq!(
            db.get(&Bytes::from_static(b"post-solo-suffix"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"recovered"[..]),
            "the accepted post-stamp suffix is the sole copy and must replay"
        );
        assert!(
            dedup.get(&[2; 16]).is_some(),
            "successful suffix replay must publish its exact result"
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn solo_takeover_publishes_applied_ambiguous_result_through_frontier() {
        let (_store, db, key_codec) = open_db_with_durable_stamp(
            "solo-ambiguous-dedup",
            encoded_ha_stamp(7, 1, SoloHistory::ever(2), 2),
        )
        .await;
        let mut tail = TailBuffer::new();
        for (seqno, entry) in [
            (1, create_entry(1, 51, 0o644)),
            (2, create_entry(2, 52, 0o600)),
            (3, create_entry(3, 53, 0o400)),
        ] {
            tail.accept(
                7,
                seqno,
                vec![ReplOp::Put(
                    Bytes::from(format!("tail-{seqno}")),
                    Bytes::from_static(b"buffered"),
                )],
                vec![entry],
            );
        }
        let dedup = crate::dedup::DedupCache::new();

        reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .unwrap();

        assert_eq!(
            tail.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![3],
            "the later suffix remains pending for replay"
        );
        match dedup.get(&[2; 16]) {
            Some(DedupResult::Create { inode_id, attrs }) => {
                assert_eq!((inode_id, attrs.fileid, attrs.mode), (52, 52, 0o600));
            }
            other => panic!("takeover lost the applied ambiguous result: {other:?}"),
        }
        assert!(
            dedup.get(&[1; 16]).is_some(),
            "the acknowledged prefix is also proven applied"
        );
        assert!(
            dedup.get(&[3; 16]).is_none(),
            "an accepted attempt above the durable frontier publishes only after replay"
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn start_solo_takeover_replays_the_first_reconnect_ship() {
        let (_store, db, key_codec) = open_db_with_durable_stamp(
            "start-solo-reconnect",
            encoded_ha_stamp(7, 0, SoloHistory::ever(1), 0),
        )
        .await;
        db.put_with_options(
            &Bytes::from_static(b"solo-base"),
            &Bytes::from_static(b"durable"),
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        let mut tail = TailBuffer::new();
        tail.accept(
            7,
            1,
            vec![ReplOp::Put(
                Bytes::from_static(b"first-reconnect-ship"),
                Bytes::from_static(b"replayed"),
            )],
            Vec::new(),
        );
        let dedup = crate::dedup::DedupCache::new();

        let stamp = reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .unwrap();
        assert_eq!(stamp, Some(ha_stamp(7, 0, SoloHistory::ever(1), 0)));
        assert_eq!(
            tail.batches_in_order()
                .map(|(seqno, _)| seqno)
                .collect::<Vec<_>>(),
            vec![1]
        );

        write_tail_batches(&db, &key_codec, &tail).await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(
            db.get(&Bytes::from_static(b"solo-base"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"durable"[..])
        );
        assert_eq!(
            db.get(&Bytes::from_static(b"first-reconnect-ship"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"replayed"[..])
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn dedup_only_tail_batch_replays_without_empty_db_write() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::DbBuilder::new(object_store::path::Path::from("dedup-only-tail"), store)
            .build()
            .await
            .unwrap();
        let mut tail = TailBuffer::new();
        tail.accept(
            3,
            1,
            Vec::new(),
            vec![DedupEntry {
                op_id: [0x61; 16],
                result: DedupResult::Error {
                    errno: libc::EEXIST as u32,
                },
            }],
        );

        let segments = write_tail_batches(&db, &KeyCodec::new(), &tail)
            .await
            .expect("logical-only replay must not submit an empty WriteBatch");
        assert!(segments.is_empty());
        db.flush().await.unwrap();
        assert!(matches!(
            tail.finish_replay().as_slice(),
            [DedupEntry {
                result: DedupResult::Error { errno },
                ..
            }] if *errno == libc::EEXIST as u32
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_durable_stamp_fails_without_discarding_the_tail() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let db =
            slatedb::DbBuilder::new(object_store::path::Path::from("malformed-ha-stamp"), store)
                .build()
                .await
                .unwrap();
        let key_codec = KeyCodec::new();
        db.put_with_options(
            &key_codec.ha_seqno_key(),
            &Bytes::from_static(b"not-a-valid-ha-stamp"),
            &PutOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        db.flush().await.unwrap();

        let mut tail = TailBuffer::new();
        tail.accept(
            1,
            1,
            vec![ReplOp::Put(
                Bytes::from_static(b"acked"),
                Bytes::from_static(b"tail"),
            )],
            Vec::new(),
        );
        let dedup = crate::dedup::DedupCache::new();

        let error = reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .expect_err("unknown durable lineage must stop promotion");
        assert!(error.to_string().contains("invalid durable HA stamp"));
        assert_eq!(
            (tail.epoch(), tail.len()),
            (1, 1),
            "the malformed stamp must not clear the acknowledged tail"
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_durable_stamp_does_not_block_idle_takeover() {
        let current = encoded_ha_stamp(7, 4, SoloHistory::never(), 4);
        let legacy = Bytes::copy_from_slice(&current[..24]);
        let (_store, db, key_codec) =
            open_db_with_durable_stamp("empty-tail-legacy-ha-stamp", legacy).await;
        let mut tail = TailBuffer::new();
        let dedup = crate::dedup::DedupCache::new();

        let stamp = reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .expect("an empty volatile tail still has to inspect durable provenance");
        assert_eq!(stamp, Some(ha_stamp(7, 4, SoloHistory::ever(0), 4)));
        assert!(tail.is_empty());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn older_legacy_stamp_allows_a_new_epoch_tail_to_replay() {
        let current = encoded_ha_stamp(7, 4, SoloHistory::never(), 4);
        let legacy = Bytes::copy_from_slice(&current[..24]);
        let (_store, db, key_codec) =
            open_db_with_durable_stamp("older-legacy-ha-stamp", legacy).await;
        let mut tail = TailBuffer::new();
        tail.accept(
            8,
            1,
            vec![ReplOp::Put(
                Bytes::from_static(b"post-upgrade"),
                Bytes::from_static(b"tail"),
            )],
            Vec::new(),
        );
        let dedup = crate::dedup::DedupCache::new();

        let stamp = reconcile_durable_head(&db, &key_codec, &mut tail, &dedup)
            .await
            .expect("the upgraded writer's fresh epoch makes the whole tail safe to replay");
        assert_eq!(stamp, Some(ha_stamp(7, 4, SoloHistory::ever(0), 4)));
        assert_eq!((tail.epoch(), tail.len()), (8, 1));

        db.close().await.unwrap();
    }

    #[test]
    fn retry_grace_requires_complete_predecessor_history() {
        let mut complete = TailBuffer::new();
        complete.accept(7, 1, Vec::new(), vec![create_entry(1, 41, 0o755)]);
        complete.accept(7, 2, Vec::new(), vec![create_entry(2, 42, 0o755)]);
        assert!(proves_promotion_retry_grace(
            &complete,
            7,
            Some(ha_stamp(7, 2, SoloHistory::never(), 2))
        ));

        complete.prune(2);
        assert!(proves_promotion_retry_grace(
            &complete,
            7,
            Some(ha_stamp(7, 2, SoloHistory::never(), 2))
        ));

        let mut missing_prefix = TailBuffer::new();
        missing_prefix.accept(7, 2, Vec::new(), vec![create_entry(2, 42, 0o755)]);
        missing_prefix.prune(1);
        assert!(
            !proves_promotion_retry_grace(
                &missing_prefix,
                7,
                Some(ha_stamp(7, 1, SoloHistory::never(), 1))
            ),
            "a watermark cannot replace an unseen seqno-1 exact result"
        );
        assert!(
            !proves_promotion_retry_grace(
                &complete,
                8,
                Some(ha_stamp(7, 2, SoloHistory::never(), 2))
            ),
            "coverage for epoch 7 cannot authorize an epoch-8 origin"
        );
    }

    #[test]
    fn retry_grace_rejects_a_latched_solo_term_after_shipping_recovers() {
        let mut complete = TailBuffer::new();
        complete.accept(7, 1, Vec::new(), vec![create_entry(1, 41, 0o755)]);
        complete.accept(7, 2, Vec::new(), vec![create_entry(2, 42, 0o755)]);

        assert!(
            !proves_promotion_retry_grace(
                &complete,
                7,
                // The term latch remains set when the current count is zero.
                Some(ha_stamp(7, 2, SoloHistory::ever(0), 2))
            ),
            "later shipping must not erase evidence of an earlier Solo/Ambiguous commit"
        );
    }

    #[tokio::test]
    async fn pruned_result_expiry_bounds_promotion_retry_grace() {
        let start = Instant::now();
        let elapsed = Arc::new(AtomicU64::new(0));
        let clock = Arc::clone(&elapsed);
        let dedup = Arc::new(crate::dedup::DedupCache::new_for_test(
            std::time::Duration::from_secs(10),
            move || start + std::time::Duration::from_secs(clock.load(Ordering::SeqCst)),
        ));
        let mut tail = TailBuffer::new();
        tail.accept(7, 1, Vec::new(), vec![create_entry(1, 41, 0o755)]);
        let durable_entries = tail.prune(1);
        tail.publish_durable(&dedup, durable_entries);
        assert!(proves_promotion_retry_grace(
            &tail,
            7,
            Some(ha_stamp(7, 1, SoloHistory::never(), 1))
        ));
        let deadline = tail
            .dedup_coverage_deadline()
            .expect("pruned exact-result history supplies the proof deadline");

        // Arming retains the original result deadline.
        elapsed.store(9, Ordering::SeqCst);
        assert!(
            PromotionRetryGraceProof {
                dedup: Arc::clone(&dedup),
                predecessor_epoch: 7,
                deadline,
            }
            .arm()
        );
        elapsed.store(10, Ordering::SeqCst);
        assert!(matches!(
            dedup.begin_attempt_from_origin([1; 16], true, 7).await,
            Err(crate::dedup::DedupAdmissionError::UnseenRetry)
        ));

        assert!(
            !PromotionRetryGraceProof {
                dedup,
                predecessor_epoch: 7,
                deadline,
            }
            .arm()
        );
    }

    #[tokio::test]
    async fn fenced_stamp_reader_restores_tail_and_retries_role() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = object_store::path::Path::from("fenced-replay");
        let stale = Arc::new(
            slatedb::DbBuilder::new(path.clone(), store.clone())
                .build()
                .await
                .unwrap(),
        );
        stale
            .put_with_options(
                &Bytes::from_static(b"volatile"),
                &Bytes::from_static(b"old"),
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: false,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let current = slatedb::DbBuilder::new(path, store.clone())
            .build()
            .await
            .unwrap();
        let fence_error = stale
            .flush()
            .await
            .expect_err("the old writer's flush must be fenced");
        assert!(matches!(
            fence_error.kind(),
            slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
        ));

        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "fenced-promoter".to_string(),
        );
        let control = receiver.control();
        let mut promotion = control.begin_promotion(1).await.unwrap();
        promotion.tail_mut().accept(
            1,
            1,
            vec![ReplOp::Put(
                Bytes::from_static(b"acked"),
                Bytes::from_static(b"tail"),
            )],
            Vec::new(),
        );

        let codec = FrameCodec::new(&[7u8; 32], SEGMENT_INFO, CompressionConfig::Lz4);
        let outcome = promotion
            .reconcile_into(&stale, &store, &codec)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            ReconcileOutcome::RetryRole {
                writer_epoch: 1,
                observed_epoch: 2
            }
        ));
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, observed_epoch| {
                    (tail.epoch(), tail.len(), observed_epoch)
                })
                .await,
            (1, 1, 2),
            "fencing must restore the acknowledged tail to live standby admission"
        );

        current.close().await.unwrap();
    }
}
