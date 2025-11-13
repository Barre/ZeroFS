use async_trait::async_trait;
use bytes::Bytes;
use slatedb::config::{PutOptions, WriteOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Failed to allocate inode: {0}")]
    AllocationError(String),
    
    #[error("Coordination backend error: {0}")]
    BackendError(String),
    
    #[error("Maximum inode ID exceeded")]
    InodeIdExhausted,
}

#[async_trait]
pub trait MetadataCoordinator: Send + Sync {
    async fn allocate_inode(&self) -> Result<u64, CoordinatorError>;
}

pub struct LocalCoordinator {
    next_inode: Arc<AtomicU64>,
    max_inode_id: u64,
}

impl LocalCoordinator {
    pub fn new(initial_inode: u64, max_inode_id: u64) -> Self {
        Self {
            next_inode: Arc::new(AtomicU64::new(initial_inode)),
            max_inode_id,
        }
    }
}

#[async_trait]
impl MetadataCoordinator for LocalCoordinator {
    async fn allocate_inode(&self) -> Result<u64, CoordinatorError> {
        let id = self.next_inode.fetch_add(1, Ordering::SeqCst);
        
        if id > self.max_inode_id {
            self.next_inode.store(self.max_inode_id + 2, Ordering::SeqCst);
            return Err(CoordinatorError::InodeIdExhausted);
        }
        
        Ok(id)
    }
}

pub struct DistributedCoordinator {
    db: Arc<slatedb::Db>,
    cache: Arc<AtomicU64>,
    cache_batch_size: u64,
    max_inode_id: u64,
}

impl DistributedCoordinator {
    pub fn new(
        db: Arc<slatedb::Db>,
        initial_inode: u64,
        max_inode_id: u64,
        cache_batch_size: u64,
    ) -> Self {
        Self {
            db,
            cache: Arc::new(AtomicU64::new(initial_inode)),
            cache_batch_size,
            max_inode_id,
        }
    }
    
    async fn allocate_batch(&self) -> Result<u64, CoordinatorError> {
        const MAX_RETRIES: u32 = 10;
        const COUNTER_KEY: &[u8] = b"__metadata_coord_inode_counter";
        
        for attempt in 0..MAX_RETRIES {
            let current = match self.db.get(COUNTER_KEY).await {
                Ok(Some(data)) => {
                    u64::from_be_bytes(
                        data.as_ref()
                            .try_into()
                            .map_err(|_| CoordinatorError::BackendError("Invalid counter format".to_string()))?
                    )
                }
                Ok(None) => {
                    let initial = 1u64;
                    let initial_bytes = initial.to_be_bytes();
                    
                    match self.db.put_with_options(
                        &Bytes::from_static(COUNTER_KEY),
                        &initial_bytes,
                        &PutOptions::default(),
                        &WriteOptions {
                            await_durable: true,
                        },
                    ).await {
                        Ok(_) => initial,
                        Err(_) => continue,
                    }
                }
                Err(e) => return Err(CoordinatorError::BackendError(e.to_string())),
            };
            
            if current > self.max_inode_id {
                return Err(CoordinatorError::InodeIdExhausted);
            }
            
            let next = current.saturating_add(self.cache_batch_size);
            let next_bytes = next.to_be_bytes();
            
            match self.db.put_with_options(
                &Bytes::from_static(COUNTER_KEY),
                &next_bytes,
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: true,
                },
            ).await {
                Ok(_) => {
                    tracing::debug!(
                        "Allocated inode batch: {}-{} (attempt {})",
                        current,
                        next - 1,
                        attempt + 1
                    );
                    return Ok(current);
                }
                Err(e) => {
                    tracing::debug!("Batch allocation conflict on attempt {}: {}", attempt + 1, e);
                    
                    if attempt < MAX_RETRIES - 1 {
                        let backoff_ms = 10u64 << attempt.min(5);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }
        
        Err(CoordinatorError::AllocationError(
            format!("Failed to allocate inode batch after {} retries", MAX_RETRIES)
        ))
    }
}

#[async_trait]
impl MetadataCoordinator for DistributedCoordinator {
    async fn allocate_inode(&self) -> Result<u64, CoordinatorError> {
        loop {
            let current = self.cache.load(Ordering::Acquire);
            let next = current + 1;
            
            if next % self.cache_batch_size == 0 {
                let batch_start = self.allocate_batch().await?;
                self.cache.store(batch_start, Ordering::Release);
                return Ok(batch_start);
            }
            
            if self.cache.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Acquire
            ).is_ok() {
                if current > self.max_inode_id {
                    return Err(CoordinatorError::InodeIdExhausted);
                }
                return Ok(current);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_local_coordinator() {
        let coord = LocalCoordinator::new(1, 1000);
        
        let id1 = coord.allocate_inode().await.unwrap();
        let id2 = coord.allocate_inode().await.unwrap();
        let id3 = coord.allocate_inode().await.unwrap();
        
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }
    
    #[tokio::test]
    async fn test_local_coordinator_exhaustion() {
        let coord = LocalCoordinator::new(998, 1000);
        
        assert!(coord.allocate_inode().await.is_ok());
        assert!(coord.allocate_inode().await.is_ok());
        assert!(coord.allocate_inode().await.is_ok());
        
        // Should fail when exceeding max
        let result = coord.allocate_inode().await;
        assert!(matches!(result, Err(CoordinatorError::InodeIdExhausted)));
    }
}

