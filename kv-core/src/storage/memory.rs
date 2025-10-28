//! In-memory storage implementation

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::{
    KVResult, Key, Entry, DatabaseId, Storage, StorageStats,
};

/// In-memory storage using `HashMap`
pub struct MemoryStorage {
    /// Map of `database_id` -> (key -> entry)
    databases: Arc<RwLock<HashMap<DatabaseId, HashMap<Key, Entry>>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new in-memory storage
    #[must_use] 
    pub fn new() -> Self {
        Self {
            databases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create database
    async fn _get_or_create_database(&self, database_id: DatabaseId) -> HashMap<Key, Entry> {
        let mut databases = self.databases.write().await;
        databases.entry(database_id).or_insert_with(HashMap::new).clone()
    }
}

#[async_trait::async_trait]
impl Storage for MemoryStorage {
    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn get(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<Entry>> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&database_id) {
            Ok(db.get(key).cloned())
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn set(&self, database_id: DatabaseId, key: Key, entry: Entry) -> KVResult<()> {
        let mut databases = self.databases.write().await;
        let db = databases.entry(database_id).or_insert_with(HashMap::new);
        db.insert(key, entry);
        Ok(())
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn delete(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        let mut databases = self.databases.write().await;
        if let Some(db) = databases.get_mut(&database_id) {
            Ok(db.remove(key).is_some())
        } else {
            Ok(false)
        }
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn exists(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&database_id) {
            Ok(db.contains_key(key))
        } else {
            Ok(false)
        }
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn keys(&self, database_id: DatabaseId) -> KVResult<Vec<Key>> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&database_id) {
            Ok(db.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn keys_pattern(&self, database_id: DatabaseId, pattern: &str) -> KVResult<Vec<Key>> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&database_id) {
            let keys: Vec<Key> = db.keys()
                .filter(|key| matches_pattern(key, pattern))
                .cloned()
                .collect();
            Ok(keys)
        } else {
            Ok(Vec::new())
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn clear_database(&self, database_id: DatabaseId) -> KVResult<()> {
        let mut databases = self.databases.write().await;
        if let Some(db) = databases.get_mut(&database_id) {
            db.clear();
        }
        Ok(())
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn get_stats(&self, database_id: DatabaseId) -> KVResult<StorageStats> {
        let databases = self.databases.read().await;
        if let Some(db) = databases.get(&database_id) {
            let total_keys = db.len() as u64;
            let memory_usage = std::mem::size_of_val(db) as u64;
            
            Ok(StorageStats {
                total_keys,
                memory_usage,
                disk_usage: None,
                last_flush: None,
            })
        } else {
            Ok(StorageStats {
                total_keys: 0,
                memory_usage: 0,
                disk_usage: None,
                last_flush: None,
            })
        }
    }

    async fn flush(&self) -> KVResult<()> {
        // No-op for in-memory storage
        debug!("Memory storage flush (no-op)");
        Ok(())
    }

    async fn close(&self) -> KVResult<()> {
        debug!("Closing memory storage");
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

    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();
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
    async fn test_memory_storage_keys() {
        let storage = MemoryStorage::new();
        let database_id = 0;
        
        // Add some keys
        let entry = Entry::new(Value::String("value".to_string()), None);
        storage.set(database_id, "key1".to_string(), entry.clone()).await.unwrap();
        storage.set(database_id, "key2".to_string(), entry.clone()).await.unwrap();
        storage.set(database_id, "test_key".to_string(), entry.clone()).await.unwrap();
        
        // Test get all keys
        let keys = storage.keys(database_id).await.unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"test_key".to_string()));
        
        // Test pattern matching
        let test_keys = storage.keys_pattern(database_id, "key*").await.unwrap();
        assert_eq!(test_keys.len(), 2);
        assert!(test_keys.contains(&"key1".to_string()));
        assert!(test_keys.contains(&"key2".to_string()));
    }

    #[tokio::test]
    async fn test_memory_storage_clear() {
        let storage = MemoryStorage::new();
        let database_id = 0;
        
        // Add some keys
        let entry = Entry::new(Value::String("value".to_string()), None);
        storage.set(database_id, "key1".to_string(), entry.clone()).await.unwrap();
        storage.set(database_id, "key2".to_string(), entry.clone()).await.unwrap();
        
        // Clear database
        storage.clear_database(database_id).await.unwrap();
        
        // Check keys are gone
        let keys = storage.keys(database_id).await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_memory_storage_stats() {
        let storage = MemoryStorage::new();
        let database_id = 0;
        
        // Add some keys
        let entry = Entry::new(Value::String("value".to_string()), None);
        storage.set(database_id, "key1".to_string(), entry.clone()).await.unwrap();
        storage.set(database_id, "key2".to_string(), entry.clone()).await.unwrap();
        
        let stats = storage.get_stats(database_id).await.unwrap();
        assert_eq!(stats.total_keys, 2);
        assert!(stats.memory_usage > 0);
        assert!(stats.disk_usage.is_none());
    }
}
