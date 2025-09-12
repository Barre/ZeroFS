use slatedb::object_store::{ObjectStore, path::Path};
use std::sync::Arc;
use uuid::Uuid;

const BUCKET_ID_MARKER: &str = ".zerofs_bucket_id";

/// Manages bucket identity to ensure cache isolation between different bucket instances
#[derive(Debug, Clone)]
pub struct BucketIdentity {
    id: Uuid,
}

impl BucketIdentity {
    /// Gets or creates a unique bucket ID for the given bucket
    /// This ID persists with the bucket and changes if the bucket is recreated
    pub async fn get_or_create(
        object_store: &Arc<dyn ObjectStore>,
        db_path: &str,
    ) -> anyhow::Result<Self> {
        let marker_path = Path::from(db_path).child(BUCKET_ID_MARKER);

        tracing::debug!("Checking for bucket ID at: {}", marker_path);

        let id = match object_store.get(&marker_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let id_str = String::from_utf8(bytes.to_vec())?;
                let uuid = Uuid::parse_str(id_str.trim())
                    .map_err(|e| anyhow::anyhow!("Invalid bucket ID format: {e:#?}"))?;
                tracing::info!("Found existing bucket ID: {}", uuid);
                uuid
            }
            Err(_) => {
                let new_id = Uuid::new_v4();
                tracing::info!("Creating new bucket ID: {}", new_id);

                object_store
                    .put(&marker_path, new_id.to_string().into())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to write bucket ID marker: {e:#?}"))?;

                new_id
            }
        };

        Ok(Self { id })
    }

    /// Gets the bucket ID
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Generates a cache-friendly directory name for this bucket
    pub fn cache_directory_name(&self) -> String {
        // Use the first 8 characters of the UUID for readability
        format!("bucket_{}", &self.id.to_string()[..8])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_directory_name() {
        let bucket = BucketIdentity {
            id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        };
        assert_eq!(bucket.cache_directory_name(), "bucket_550e8400");
    }

    #[test]
    fn test_cache_directory_name_with_new_uuid() {
        let uuid = Uuid::new_v4();
        let bucket = BucketIdentity { id: uuid };
        let cache_name = bucket.cache_directory_name();

        assert!(cache_name.starts_with("bucket_"));
        assert_eq!(cache_name.len(), 15);
        let expected = format!("bucket_{}", &uuid.to_string()[..8]);
        assert_eq!(cache_name, expected);
    }
}
