//! Disk-based storage implementation using Sled

use std::path::Path;
use sled::{Db, Tree};
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::{
    KVError, KVResult, Key, Entry, DatabaseId, Storage, StorageStats,
};

/// Disk-based storage using Sled
pub struct DiskStorage {
    /// Sled database instance
    db: Db,
    /// Database trees (one per logical database)
    trees: RwLock<Vec<Option<Tree>>>,
}

impl DiskStorage {
    /// Create a new disk storage
    /// 
    /// # Errors
    /// Returns error if directory creation or database opening fails
    pub fn new<P: AsRef<Path>>(path: P) -> KVResult<Self> {
        let db_path = path.as_ref();
        std::fs::create_dir_all(db_path)
            .map_err(|e| KVError::Storage(format!("Failed to create data directory: {e}")))?;

        let db = sled::open(db_path)
            .map_err(|e| KVError::Storage(format!("Failed to open sled database: {e}")))?;

        // Initialize trees for databases 0-15
        let mut trees = Vec::with_capacity(16);
        for i in 0..16 {
            let tree_name = format!("db_{i}");
            let tree = db.open_tree(&tree_name)
                .map_err(|e| KVError::Storage(format!("Failed to open tree {tree_name}: {e}")))?;
            trees.push(Some(tree));
        }

        Ok(Self {
            db,
            trees: RwLock::new(trees),
        })
    }

    /// Get tree for a database
    async fn get_tree(&self, database_id: DatabaseId) -> KVResult<Tree> {
        if database_id >= 16 {
            return Err(KVError::InvalidKey(format!("Database ID {database_id} out of range (0-15)")));
        }

        let trees = self.trees.read().await;
        trees[database_id as usize]
            .as_ref()
            .ok_or_else(|| KVError::Storage("Database tree not found".to_string())).cloned()
    }

    /// Serialize entry to bytes
    fn serialize_entry(entry: &Entry) -> KVResult<Vec<u8>> {
        serde_json::to_vec(entry)
            .map_err(|e| KVError::Internal(format!("Serialization failed: {e}")))
    }

    /// Deserialize entry from bytes
    fn deserialize_entry(data: &[u8]) -> KVResult<Entry> {
        serde_json::from_slice(data)
            .map_err(|e| KVError::Internal(format!("Deserialization failed: {e}")))
    }
}

#[async_trait::async_trait]
impl Storage for DiskStorage {
    async fn get(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<Entry>> {
        let tree = self.get_tree(database_id).await?;
        
        match tree.get(key.as_bytes()) {
            Ok(Some(data)) => {
                let entry = Self::deserialize_entry(&data)?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(KVError::Storage(format!("Failed to get key: {e}"))),
        }
    }

    async fn set(&self, database_id: DatabaseId, key: Key, entry: Entry) -> KVResult<()> {
        let tree = self.get_tree(database_id).await?;
        let data = Self::serialize_entry(&entry)?;
        
        tree.insert(key.as_bytes(), data)
            .map_err(|e| KVError::Storage(format!("Failed to set key: {e}")))?;
        
        Ok(())
    }

    async fn delete(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        let tree = self.get_tree(database_id).await?;
        
        match tree.remove(key.as_bytes()) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(KVError::Storage(format!("Failed to delete key: {e}"))),
        }
    }

    async fn exists(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        let tree = self.get_tree(database_id).await?;
        
        match tree.contains_key(key.as_bytes()) {
            Ok(exists) => Ok(exists),
            Err(e) => Err(KVError::Storage(format!("Failed to check key existence: {e}"))),
        }
    }

    async fn keys(&self, database_id: DatabaseId) -> KVResult<Vec<Key>> {
        let tree = self.get_tree(database_id).await?;
        let mut keys = Vec::new();
        
        for result in &tree {
            match result {
                Ok((key_bytes, _)) => {
                    let key = String::from_utf8(key_bytes.to_vec())
                        .map_err(|e| KVError::Storage(format!("Invalid key encoding: {e}")))?;
                    keys.push(key);
                }
                Err(e) => {
                    error!("Error iterating keys: {}", e);
                    return Err(KVError::Storage(format!("Failed to iterate keys: {e}")));
                }
            }
        }
        
        Ok(keys)
    }

    async fn keys_pattern(&self, database_id: DatabaseId, pattern: &str) -> KVResult<Vec<Key>> {
        let all_keys = self.keys(database_id).await?;
        let matching_keys: Vec<Key> = all_keys
            .into_iter()
            .filter(|key| matches_pattern(key, pattern))
            .collect();
        
        Ok(matching_keys)
    }

    async fn clear_database(&self, database_id: DatabaseId) -> KVResult<()> {
        let tree = self.get_tree(database_id).await?;
        
        tree.clear()
            .map_err(|e| KVError::Storage(format!("Failed to clear database: {e}")))?;
        
        Ok(())
    }

    async fn get_stats(&self, database_id: DatabaseId) -> KVResult<StorageStats> {
        let tree = self.get_tree(database_id).await?;
        
        let total_keys = tree.len() as u64;
        let memory_usage = 0; // Sled doesn't provide size_on_disk method
        
        Ok(StorageStats {
            total_keys,
            memory_usage,
            disk_usage: Some(memory_usage),
            last_flush: None, // Sled handles flushing automatically
        })
    }

    async fn flush(&self) -> KVResult<()> {
        self.db.flush()
            .map_err(|e| KVError::Storage(format!("Failed to flush database: {e}")))?;
        debug!("Disk storage flushed");
        Ok(())
    }

    async fn close(&self) -> KVResult<()> {
        self.db.flush()
            .map_err(|e| KVError::Storage(format!("Failed to flush on close: {e}")))?;
        debug!("Disk storage closed");
        Ok(())
    }
}

/// Simple pattern matching (supports * wildcard)
fn matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    
    if !pattern.contains('*') {
        return key == pattern;
    }
    
    // Simple wildcard matching
    let pattern_parts: Vec<&str> = pattern.split('*').collect();
    if pattern_parts.len() == 2 {
        let prefix = pattern_parts[0];
        let suffix = pattern_parts[1];
        
        if prefix.is_empty() {
            key.ends_with(suffix)
        } else if suffix.is_empty() {
            key.starts_with(prefix)
        } else {
            key.starts_with(prefix) && key.ends_with(suffix)
        }
    } else {
        // More complex pattern - for now, just do simple contains
        key.contains(pattern.trim_matches('*'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Value, Entry};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_disk_storage_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DiskStorage::new(temp_dir.path()).unwrap();
        let database_id = 0;
        
        // Test set and get
        let entry = Entry::new(Value::String("test_value".to_string()), None);
        storage.set(database_id, "test_key".to_string(), entry.clone()).await.unwrap();
        
        let retrieved = storage.get(database_id, &"test_key".to_string()).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value.as_string().unwrap(), "test_value");
        
        // Test exists
        let exists = storage.exists(database_id, &"test_key".to_string()).await.unwrap();
        assert!(exists);
        
        // Test delete
        let deleted = storage.delete(database_id, &"test_key".to_string()).await.unwrap();
        assert!(deleted);
        
        let exists_after = storage.exists(database_id, &"test_key".to_string()).await.unwrap();
        assert!(!exists_after);
    }

    #[tokio::test]
    async fn test_disk_storage_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path();
        
        // Create storage and add data
        {
            let storage = DiskStorage::new(storage_path).unwrap();
            let entry = Entry::new(Value::String("persistent_value".to_string()), None);
            storage.set(0, "persistent_key".to_string(), entry).await.unwrap();
            storage.flush().await.unwrap();
        }
        
        // Reopen storage and check data persists
        {
            let storage = DiskStorage::new(storage_path).unwrap();
            let retrieved = storage.get(0, &"persistent_key".to_string()).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value.as_string().unwrap(), "persistent_value");
        }
    }

    #[tokio::test]
    async fn test_disk_storage_multiple_databases() {
        let temp_dir = TempDir::new().unwrap();
        let storage = DiskStorage::new(temp_dir.path()).unwrap();
        
        // Add data to different databases
        let entry1 = Entry::new(Value::String("db0_value".to_string()), None);
        let entry2 = Entry::new(Value::String("db1_value".to_string()), None);
        
        storage.set(0, "key".to_string(), entry1).await.unwrap();
        storage.set(1, "key".to_string(), entry2).await.unwrap();
        
        // Check data is isolated
        let db0_value = storage.get(0, &"key".to_string()).await.unwrap().unwrap();
        let db1_value = storage.get(1, &"key".to_string()).await.unwrap().unwrap();
        
        assert_eq!(db0_value.value.as_string().unwrap(), "db0_value");
        assert_eq!(db1_value.value.as_string().unwrap(), "db1_value");
    }
}
