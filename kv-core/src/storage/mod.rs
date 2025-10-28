//! Storage implementations for the KV service
//! 
//! Provides different storage backends: in-memory, disk persistence, and AOF.

pub mod memory;
pub mod disk;
pub mod aof;

pub use memory::MemoryStorage;
pub use disk::DiskStorage;
pub use aof::AOFStorage;

use async_trait::async_trait;
use crate::{KVResult, Key, Entry, DatabaseId, PersistenceMode};

/// Trait for storage backends
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get an entry by key
    async fn get(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<Entry>>;
    
    /// Set an entry
    async fn set(&self, database_id: DatabaseId, key: Key, entry: Entry) -> KVResult<()>;
    
    /// Delete an entry
    async fn delete(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool>;
    
    /// Check if a key exists
    async fn exists(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool>;
    
    /// Get all keys in a database
    async fn keys(&self, database_id: DatabaseId) -> KVResult<Vec<Key>>;
    
    /// Get keys matching a pattern
    async fn keys_pattern(&self, database_id: DatabaseId, pattern: &str) -> KVResult<Vec<Key>>;
    
    /// Clear all data in a database
    async fn clear_database(&self, database_id: DatabaseId) -> KVResult<()>;
    
    /// Get database statistics
    async fn get_stats(&self, database_id: DatabaseId) -> KVResult<StorageStats>;
    
    /// Flush all pending writes
    async fn flush(&self) -> KVResult<()>;
    
    /// Close the storage backend
    async fn close(&self) -> KVResult<()>;
}

/// Statistics for storage usage
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_keys: u64,
    pub memory_usage: u64,
    pub disk_usage: Option<u64>,
    pub last_flush: Option<chrono::DateTime<chrono::Utc>>,
}

/// Storage factory for creating storage backends
pub struct StorageFactory;

impl StorageFactory {
    /// Create a storage backend based on persistence mode
    /// 
    /// # Errors
    /// Returns error if storage creation fails
    pub async fn create(
        mode: PersistenceMode,
        data_dir: &str,
        database_id: DatabaseId,
    ) -> KVResult<Box<dyn Storage>> {
        match mode {
            PersistenceMode::Memory => {
                Ok(Box::new(MemoryStorage::new()))
            }
            PersistenceMode::AOF => {
                let aof_path = format!("{data_dir}/db_{database_id}.aof");
                Ok(Box::new(AOFStorage::new(&aof_path).await?))
            }
            PersistenceMode::Full => {
                let db_path = format!("{data_dir}/db_{database_id}");
                Ok(Box::new(DiskStorage::new(&db_path)?))
            }
            PersistenceMode::Hybrid => {
                // For hybrid mode, we'll use memory storage with AOF logging
                let aof_path = format!("{data_dir}/db_{database_id}.aof");
                Ok(Box::new(AOFStorage::new(&aof_path).await?))
            }
        }
    }
}
