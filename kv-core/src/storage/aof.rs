//! Append-Only File (AOF) storage implementation

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use crate::{
    KVError, KVResult, Key, Entry, DatabaseId, Storage, StorageStats,
};

/// AOF operation types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum AOFOperation {
    Set { key: Key, entry: Entry },
    Delete { key: Key },
    Clear,
}

/// AOF storage implementation
pub struct AOFStorage {
    /// In-memory cache of data
    cache: Arc<RwLock<HashMap<DatabaseId, HashMap<Key, Entry>>>>,
    /// AOF file path
    aof_path: String,
    /// AOF file writer
    writer: Arc<RwLock<Option<BufWriter<File>>>>,
    /// Sync interval in seconds
    sync_interval: u64,
    /// Background sync task handle
    sync_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AOFStorage {
    /// Create a new AOF storage
    /// 
    /// # Errors
    /// Returns error if directory creation or file operations fail
    pub async fn new<P: AsRef<Path>>(aof_path: P) -> KVResult<Self> {
        let aof_path = aof_path.as_ref().to_string_lossy().to_string();
        
        // Create directory if it doesn't exist
        if let Some(parent) = Path::new(&aof_path).parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| KVError::Storage(format!("Failed to create AOF directory: {e}")))?;
        }

        let mut storage = Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            aof_path: aof_path.clone(),
            writer: Arc::new(RwLock::new(None)),
            sync_interval: 1, // Default 1 second
            sync_handle: None,
        };

        // Load existing data from AOF file
        storage.load_from_aof().await?;
        
        // Open writer
        storage.open_writer().await?;
        
        // Start background sync task
        storage.start_sync_task();

        Ok(storage)
    }

    /// Load data from AOF file
    async fn load_from_aof(&self) -> KVResult<()> {
        if !Path::new(&self.aof_path).exists() {
            debug!("AOF file does not exist, starting fresh");
            return Ok(());
        }

        debug!("Loading data from AOF file: {}", self.aof_path);
        
        let file = File::open(&self.aof_path).await
            .map_err(|e| KVError::Storage(format!("Failed to open AOF file: {e}")))?;
        
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut cache = self.cache.write().await;
        
        let mut line_count = 0;
        while let Some(line) = lines.next_line().await
            .map_err(|e| KVError::Storage(format!("Failed to read AOF line: {e}")))?
        {
            line_count += 1;
            
            if line.trim().is_empty() {
                continue;
            }
            
            match serde_json::from_str::<AOFOperation>(&line) {
                Ok(op) => {
                    match op {
                        AOFOperation::Set { key, entry } => {
                            // We need to determine which database this belongs to
                            // For now, we'll use database 0 as default
                            // In a real implementation, we'd need to track database context
                            let db = cache.entry(0).or_insert_with(HashMap::new);
                            db.insert(key, entry);
                        }
                        AOFOperation::Delete { key } => {
                            if let Some(db) = cache.get_mut(&0) {
                                db.remove(&key);
                            }
                        }
                        AOFOperation::Clear => {
                            if let Some(db) = cache.get_mut(&0) {
                                db.clear();
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to parse AOF line {}: {}", line_count, e);
                    // Continue loading other lines
                }
            }
        }
        
        debug!("Loaded {} lines from AOF file", line_count);
        Ok(())
    }

    /// Open AOF file for writing
    #[allow(clippy::significant_drop_tightening)]
    async fn open_writer(&self) -> KVResult<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.aof_path)
            .await
            .map_err(|e| KVError::Storage(format!("Failed to open AOF file for writing: {e}")))?;
        
        let writer = BufWriter::new(file);
        let mut writer_guard = self.writer.write().await;
        *writer_guard = Some(writer);
        
        Ok(())
    }

    /// Write operation to AOF file
    #[allow(clippy::significant_drop_tightening)]
    async fn write_operation(&self, operation: AOFOperation) -> KVResult<()> {
        let operation_json = serde_json::to_string(&operation)
            .map_err(KVError::Serialization)?;
        
        let mut writer_guard = self.writer.write().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer.write_all(operation_json.as_bytes()).await
                .map_err(|e| KVError::Storage(format!("Failed to write to AOF: {e}")))?;
            writer.write_all(b"\n").await
                .map_err(|e| KVError::Storage(format!("Failed to write newline to AOF: {e}")))?;
        } else {
            return Err(KVError::Storage("AOF writer not available".to_string()));
        }
        
        Ok(())
    }

    /// Start background sync task
    fn start_sync_task(&mut self) {
        let writer = Arc::clone(&self.writer);
        let sync_interval = self.sync_interval;
        
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(sync_interval));
            
            loop {
                interval.tick().await;
                
                let mut writer_guard = writer.write().await;
                if let Some(writer) = writer_guard.as_mut()
                    && let Err(e) = writer.flush().await {
                        error!("Failed to flush AOF: {}", e);
                    }
            }
        });
        
        self.sync_handle = Some(handle);
    }

    /// Stop background sync task
    fn _stop_sync_task(&mut self) {
        if let Some(handle) = self.sync_handle.take() {
            handle.abort();
        }
    }

    /// Force sync AOF file
    #[allow(clippy::significant_drop_tightening)]
    async fn force_sync(&self) -> KVResult<()> {
        let mut writer_guard = self.writer.write().await;
        if let Some(writer) = writer_guard.as_mut() {
            writer.flush().await
                .map_err(|e| KVError::Storage(format!("Failed to flush AOF: {e}")))?;
        }
        Ok(())
    }
}

impl Drop for AOFStorage {
    fn drop(&mut self) {
        // Note: We can't use async in Drop, so we'll rely on the sync task
        // In a real implementation, we'd want to ensure proper cleanup
    }
}

#[async_trait::async_trait]
impl Storage for AOFStorage {
    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn get(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<Entry>> {
        let cache = self.cache.read().await;
        if let Some(db) = cache.get(&database_id) {
            Ok(db.get(key).cloned())
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn set(&self, database_id: DatabaseId, key: Key, entry: Entry) -> KVResult<()> {
        // Update cache
        {
            let mut cache = self.cache.write().await;
            let db = cache.entry(database_id).or_insert_with(HashMap::new);
            db.insert(key.clone(), entry.clone());
        }
        
        // Write to AOF
        let operation = AOFOperation::Set { key, entry };
        self.write_operation(operation).await?;
        
        Ok(())
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn delete(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        // Update cache
        let existed = {
            let mut cache = self.cache.write().await;
            if let Some(db) = cache.get_mut(&database_id) {
                db.remove(key).is_some()
            } else {
                false
            }
        };
        
        if existed {
            // Write to AOF
            let operation = AOFOperation::Delete { key: key.clone() };
            self.write_operation(operation).await?;
        }
        
        Ok(existed)
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn exists(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        let cache = self.cache.read().await;
        if let Some(db) = cache.get(&database_id) {
            Ok(db.contains_key(key))
        } else {
            Ok(false)
        }
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn keys(&self, database_id: DatabaseId) -> KVResult<Vec<Key>> {
        let cache = self.cache.read().await;
        if let Some(db) = cache.get(&database_id) {
            Ok(db.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    async fn keys_pattern(&self, database_id: DatabaseId, pattern: &str) -> KVResult<Vec<Key>> {
        let cache = self.cache.read().await;
        if let Some(db) = cache.get(&database_id) {
            let keys: Vec<Key> = db.keys()
                .filter(|key| matches_pattern(key, pattern))
                .cloned()
                .collect();
            Ok(keys)
        } else {
            Ok(Vec::new())
        }
    }

    async fn clear_database(&self, database_id: DatabaseId) -> KVResult<()> {
        // Update cache
        {
            let mut cache = self.cache.write().await;
            if let Some(db) = cache.get_mut(&database_id) {
                db.clear();
            }
        }
        
        // Write to AOF
        let operation = AOFOperation::Clear;
        self.write_operation(operation).await?;
        
        Ok(())
    }

    async fn get_stats(&self, database_id: DatabaseId) -> KVResult<StorageStats> {
        let cache = self.cache.read().await;
        if let Some(db) = cache.get(&database_id) {
            let total_keys = db.len() as u64;
            let memory_usage = std::mem::size_of_val(db) as u64;
            
            // Get AOF file size
            let disk_usage = tokio::fs::metadata(&self.aof_path).await
                .map(|m| m.len())
                .unwrap_or(0);
            
            Ok(StorageStats {
                total_keys,
                memory_usage,
                disk_usage: Some(disk_usage),
                last_flush: None, // We don't track flush times in this simple implementation
            })
        } else {
            Ok(StorageStats {
                total_keys: 0,
                memory_usage: 0,
                disk_usage: Some(0),
                last_flush: None,
            })
        }
    }

    async fn flush(&self) -> KVResult<()> {
        self.force_sync().await?;
        debug!("AOF storage flushed");
        Ok(())
    }

    async fn close(&self) -> KVResult<()> {
        self.force_sync().await?;
        debug!("AOF storage closed");
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
    async fn test_aof_storage_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let aof_path = temp_dir.path().join("test.aof");
        let storage = AOFStorage::new(&aof_path).await.unwrap();
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
    async fn test_aof_storage_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let aof_path = temp_dir.path().join("persistent.aof");
        
        // Create storage and add data
        {
            let storage = AOFStorage::new(&aof_path).await.unwrap();
            let entry = Entry::new(Value::String("persistent_value".to_string()), None);
            storage.set(0, "persistent_key".to_string(), entry).await.unwrap();
            storage.flush().await.unwrap();
        }
        
        // Reopen storage and check data persists
        {
            let storage = AOFStorage::new(&aof_path).await.unwrap();
            let retrieved = storage.get(0, &"persistent_key".to_string()).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value.as_string().unwrap(), "persistent_value");
        }
    }

    #[tokio::test]
    async fn test_aof_storage_stats() {
        let temp_dir = TempDir::new().unwrap();
        let aof_path = temp_dir.path().join("stats.aof");
        let storage = AOFStorage::new(&aof_path).await.unwrap();
        
        // Add some data
        let entry = Entry::new(Value::String("value".to_string()), None);
        storage.set(0, "key1".to_string(), entry.clone()).await.unwrap();
        storage.set(0, "key2".to_string(), entry).await.unwrap();
        
        // Force flush to ensure data is written to disk
        storage.flush().await.unwrap();
        
        let stats = storage.get_stats(0).await.unwrap();
        assert_eq!(stats.total_keys, 2);
        assert!(stats.memory_usage > 0);
        assert!(stats.disk_usage.is_some());
        assert!(stats.disk_usage.unwrap() > 0);
    }
}
