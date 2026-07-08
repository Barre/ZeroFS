//! The durable latest-leader record: a marker object beside the db naming the
//! last node to open it as writer. A booting node that finds its own name must
//! not elect itself from Hello silence: a silent peer may be live behind a
//! partition holding that term's acked writes in its RAM tail, or already
//! promoted, and electing would abandon the tail or fence the live writer.
//!
//! Advisory only: the writer-epoch CAS on the data db stays the single-writer
//! guarantee. The record is written after that CAS and before serving, so its
//! only staleness (a crash in between) names a previous writer, which biases a
//! booting node toward blocking, never toward electing.
//!
//! Callers pass the shared retry-wrapped store: a transient backend error must
//! not fail a boot (read) or a takeover (write).

use slatedb::object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::sync::Arc;

const LEADER_RECORD_MARKER: &str = ".zerofs_ha_leader";

fn record_path(db_path: &str) -> Path {
    Path::from(db_path).join(LEADER_RECORD_MARKER)
}

/// `(writer_epoch, node_id)` of the last writer; `None` before any HA writer.
/// Read errors other than absence propagate: deciding a boot role without the
/// record could elect a latest-leader from silence.
pub async fn read(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<Option<(u64, String)>> {
    match object_store.get(&record_path(db_path)).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let text = String::from_utf8(bytes.to_vec())
                .map_err(|e| anyhow::anyhow!("invalid HA leader record: {e}"))?;
            let (epoch, node) = text
                .trim()
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("invalid HA leader record: {text:?}"))?;
            let epoch: u64 = epoch
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid HA leader record epoch: {e}"))?;
            Ok(Some((epoch, node.to_string())))
        }
        Err(slatedb::object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("reading the HA leader record failed: {e}")),
    }
}

/// Overwrite the record with this writer's term, before it serves. Last write
/// wins: the writer-epoch CAS already serialized the writers, and a retried
/// PUT rewrites the same value.
pub async fn write(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
    epoch: u64,
    node_id: &str,
) -> anyhow::Result<()> {
    object_store
        .put(&record_path(db_path), format!("{epoch} {node_id}").into())
        .await
        .map_err(|e| anyhow::anyhow!("writing the HA leader record failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn roundtrips_and_reads_absent_as_none() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(read(&store, "db").await.unwrap(), None);

        write(&store, "db", 7, "node-a").await.unwrap();
        assert_eq!(
            read(&store, "db").await.unwrap(),
            Some((7, "node-a".to_string()))
        );

        // Last write wins across takeovers.
        write(&store, "db", 8, "node-b").await.unwrap();
        assert_eq!(
            read(&store, "db").await.unwrap(),
            Some((8, "node-b".to_string()))
        );

        // Only the first space splits, so a node id containing spaces round-trips.
        write(&store, "db", 9, "node b").await.unwrap();
        assert_eq!(
            read(&store, "db").await.unwrap(),
            Some((9, "node b".to_string()))
        );
    }
}
