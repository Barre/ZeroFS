use futures::StreamExt;
use object_store::{Error, ObjectStore, PutMode, PutOptions, path::Path};
use std::sync::Arc;
use uuid::Uuid;

const TEST_FILE_PREFIX: &str = ".zerofs_compatibility_test_";

pub async fn check_if_match_support(
    object_store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> anyhow::Result<()> {
    // Clean up any old test files from previous runs (best effort)
    let prefix_path = Path::from(db_path).child(TEST_FILE_PREFIX);
    let mut list = object_store.list(Some(&prefix_path));
    while let Some(Ok(meta)) = list.next().await {
        let _ = object_store.delete(&meta.location).await;
    }

    let test_id = Uuid::new_v4();
    let test_path = Path::from(db_path).child(format!("{}{}", TEST_FILE_PREFIX, test_id));

    tracing::info!("Checking storage provider compatibility (conditional writes for fencing)...");

    object_store
        .put(&test_path, "initial".into())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write test file: {e:#?}"))?;

    let result = object_store
        .put_opts(
            &test_path,
            "should_fail".into(),
            PutOptions::from(PutMode::Create),
        )
        .await;

    let _ = object_store.delete(&test_path).await;

    match result {
        Err(Error::AlreadyExists { .. }) => {
            tracing::info!("Storage provider compatibility check passed");
            Ok(())
        }
        Ok(_) => Err(anyhow::anyhow!(
            "Storage provider does not support conditional writes. \
            PutMode::Create succeeded when it should have failed. \
            This feature is required for fencing in ZeroFS."
        )),
        Err(Error::NotImplemented) => Err(anyhow::anyhow!(
            "Storage provider does not support conditional writes (PutMode::Create). \
            This feature is required for fencing in ZeroFS."
        )),
        Err(e) => {
            let error_str = e.to_string().to_lowercase();
            if error_str.contains("501") || error_str.contains("not implemented") {
                Err(anyhow::anyhow!(
                    "Storage provider does not support conditional writes. \
                    This feature is required for fencing in ZeroFS.\n\n{e}"
                ))
            } else {
                Err(anyhow::anyhow!(
                    "Storage provider precondition check failed: {e}"
                ))
            }
        }
    }
}
