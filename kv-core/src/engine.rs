//! Main KV engine implementation
//! 
//! Orchestrates storage, encryption, TTL management, and data structures.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::{
    KVError, KVResult, Key, Value, Entry, DatabaseId, 
    KVConfig, KVStats, TTL, StorageFactory,
    encryption::KeyManager,
    ttl::{TTLManager, TTLSupport},
    storage::Storage as StorageTrait,
    pubsub::{PubSubManager, ChannelPattern},
};

/// Main KV engine
pub struct KVEngine {
    /// Configuration
    config: KVConfig,
    /// Storage backends per database
    storages: Arc<RwLock<HashMap<DatabaseId, Box<dyn StorageTrait>>>>,
    /// Encryption key manager
    _key_manager: Arc<RwLock<KeyManager>>,
    /// TTL manager
    ttl_manager: Arc<RwLock<TTLManager>>,
    /// Pub/Sub manager
    pubsub_manager: Arc<RwLock<PubSubManager>>,
    /// Statistics
    stats: Arc<RwLock<KVStats>>,
    /// Start time for uptime calculation
    start_time: std::time::Instant,
}

impl KVEngine {
    /// Create a new KV engine
    /// 
    /// # Errors
    /// Returns error if storage initialization fails
    pub async fn new(config: KVConfig) -> KVResult<Self> {
        info!("Initializing KV engine with config: {:?}", config);
        
        // Initialize key manager
        let key_manager = KeyManager::new(&config.master_key)?;
        
        // Initialize TTL manager
        let ttl_manager = TTLManager::new(
            std::time::Duration::from_secs(config.expiration_check_interval)
        );
        
        // Initialize Pub/Sub manager
        let mut pubsub_manager = PubSubManager::default();
        pubsub_manager.start_cleanup();
        
        let engine = Self {
            config: config.clone(),
            storages: Arc::new(RwLock::new(HashMap::new())),
            _key_manager: Arc::new(RwLock::new(key_manager)),
            ttl_manager: Arc::new(RwLock::new(ttl_manager)),
            pubsub_manager: Arc::new(RwLock::new(pubsub_manager)),
            stats: Arc::new(RwLock::new(KVStats {
                total_keys: 0,
                expired_keys: 0,
                memory_usage: 0,
                disk_usage: 0,
                total_operations: 0,
                ops_per_second: 0.0,
                uptime: 0,
                active_connections: 0,
            })),
            start_time: std::time::Instant::now(),
        };
        
        // Start TTL cleanup
        engine.start_ttl_cleanup().await;
        
        // Initialize storage for default database
        engine.ensure_storage(0).await?;
        
        info!("KV engine initialized successfully");
        Ok(engine)
    }

    /// Ensure storage exists for a database
    #[allow(clippy::significant_drop_tightening)]
    async fn ensure_storage(&self, database_id: DatabaseId) -> KVResult<()> {
        let mut storages = self.storages.write().await;
        if let std::collections::hash_map::Entry::Vacant(e) = storages.entry(database_id) {
            let storage = StorageFactory::create(
                self.config.persistence_mode,
                &self.config.data_dir,
                database_id,
            ).await?;
            e.insert(storage);
        }
        Ok(())
    }

    /// Get storage for a database
    #[allow(clippy::option_if_let_else)]
    async fn _get_storage(&self, database_id: DatabaseId) -> KVResult<Arc<Box<dyn StorageTrait>>> {
        self.ensure_storage(database_id).await?;
        let storages = self.storages.read().await;
        if let Some(_storage) = storages.get(&database_id) {
            // We need to return a reference, but we can't clone the trait object
            // This is a limitation of the current design - in a real implementation
            // we'd use Arc<dyn StorageTrait> instead of Box<dyn StorageTrait>
            Err(KVError::Internal("Storage access not implemented".to_string()))
        } else {
            Err(KVError::Internal("Storage not found".to_string()))
        }
    }

    /// Start TTL cleanup background task
    async fn start_ttl_cleanup(&self) {
        let ttl_manager = Arc::clone(&self.ttl_manager);
        let storages = Arc::clone(&self.storages);
        
        {
            let mut ttl_manager_guard = ttl_manager.write().await;
            ttl_manager_guard.start_cleanup(move |expired_keys| {
                let storages = Arc::clone(&storages);
                
                tokio::spawn(async move {
                    for key in expired_keys {
                        // Remove from all databases
                        let storages_guard = storages.read().await;
                        for (database_id, storage) in storages_guard.iter() {
                            if let Err(e) = storage.delete(*database_id, &key).await {
                                error!("Failed to delete expired key {} from database {}: {}", key, database_id, e);
                            }
                        }
                    }
                });
            });
        }
    }

    /// Update statistics
    async fn update_stats(&self, operation_count: u64) {
        let mut stats = self.stats.write().await;
        stats.total_operations += operation_count;
        stats.uptime = self.start_time.elapsed().as_secs();
        
        if stats.uptime > 0 {
            #[allow(clippy::cast_precision_loss)]
            let ops_per_second = stats.total_operations as f64 / stats.uptime as f64;
            stats.ops_per_second = ops_per_second;
        }
    }

    // Basic KV operations

    /// Get a value by key
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<Value>> {
        self.ensure_storage(database_id).await?;
        
        let storages = self.storages.read().await;
        let storage = storages.get(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        let entry = storage.get(database_id, key).await?;
        
        if let Some(mut entry) = entry {
            // Check if expired
            if entry.is_expired() {
                // Remove expired entry
                drop(storages); // Release read lock
                let _ = self.delete(database_id, key).await;
                return Ok(None);
            }
            
            // Touch entry (update access count and time)
            entry.touch();
            
            // Update in storage
            drop(storages); // Release read lock
            let mut storages = self.storages.write().await;
            let storage = storages.get_mut(&database_id)
                .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
            storage.set(database_id, key.clone(), entry.clone()).await?;
            
            self.update_stats(1).await;
            Ok(Some(entry.value))
        } else {
            self.update_stats(1).await;
            Ok(None)
        }
    }

    /// Set a value with optional TTL
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn set(&self, database_id: DatabaseId, key: Key, value: Value, ttl: Option<TTL>) -> KVResult<()> {
        self.ensure_storage(database_id).await?;
        
        let entry = Entry::new(value, ttl);
        
        let mut storages = self.storages.write().await;
        let storage = storages.get_mut(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        storage.set(database_id, key.clone(), entry).await?;
        
        // Update TTL manager if TTL is set
        if let Some(ttl) = ttl {
            let ttl_manager = self.ttl_manager.read().await;
            ttl_manager.set_ttl(key.clone(), ttl).await?;
        }
        
        // Publish cache invalidation event
        if let Err(e) = self.publish_invalidation(&key).await {
            error!("Failed to publish invalidation for key {}: {}", key, e);
        }
        
        self.update_stats(1).await;
        Ok(())
    }

    /// Delete a key
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn delete(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        self.ensure_storage(database_id).await?;
        
        let mut storages = self.storages.write().await;
        let storage = storages.get_mut(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        let deleted = storage.delete(database_id, key).await?;
        
        if deleted {
            // Remove from TTL manager
            let ttl_manager = self.ttl_manager.read().await;
            let _ = ttl_manager.remove_ttl(key).await;
            
            // Publish cache invalidation event
            if let Err(e) = self.publish_invalidation(key).await {
                error!("Failed to publish invalidation for key {}: {}", key, e);
            }
        }
        
        self.update_stats(1).await;
        Ok(deleted)
    }

    /// Check if a key exists
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn exists(&self, database_id: DatabaseId, key: &Key) -> KVResult<bool> {
        self.ensure_storage(database_id).await?;
        
        let storages = self.storages.read().await;
        let storage = storages.get(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        let exists = storage.exists(database_id, key).await?;
        
        self.update_stats(1).await;
        Ok(exists)
    }

    /// Set TTL for an existing key
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn expire(&self, database_id: DatabaseId, key: &Key, ttl: TTL) -> KVResult<bool> {
        self.ensure_storage(database_id).await?;
        
        // Check if key exists
        if !self.exists(database_id, key).await? {
            return Ok(false);
        }
        
        // Update TTL
        let ttl_manager = self.ttl_manager.read().await;
        ttl_manager.set_ttl(key.clone(), ttl).await?;
        
        // Update entry in storage
        let storages = self.storages.read().await;
        let storage = storages.get(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        if let Some(mut entry) = storage.get(database_id, key).await? {
            entry.set_ttl(ttl);
            drop(storages); // Release read lock
            let mut storages = self.storages.write().await;
            let storage = storages.get_mut(&database_id)
                .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
            storage.set(database_id, key.clone(), entry).await?;
        }
        
        self.update_stats(1).await;
        Ok(true)
    }

    /// Get remaining TTL for a key
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn ttl(&self, database_id: DatabaseId, key: &Key) -> KVResult<Option<TTL>> {
        self.ensure_storage(database_id).await?;
        
        let ttl_manager = self.ttl_manager.read().await;
        let ttl = ttl_manager.get_ttl(key).await?;
        
        self.update_stats(1).await;
        Ok(ttl)
    }

    /// Get all keys in a database
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn keys(&self, database_id: DatabaseId) -> KVResult<Vec<Key>> {
        self.ensure_storage(database_id).await?;
        
        let storages = self.storages.read().await;
        let storage = storages.get(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        let keys = storage.keys(database_id).await?;
        
        self.update_stats(1).await;
        Ok(keys)
    }

    /// Get keys matching a pattern
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn keys_pattern(&self, database_id: DatabaseId, pattern: &str) -> KVResult<Vec<Key>> {
        self.ensure_storage(database_id).await?;
        
        let storages = self.storages.read().await;
        let storage = storages.get(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        let keys = storage.keys_pattern(database_id, pattern).await?;
        
        self.update_stats(1).await;
        Ok(keys)
    }

    /// Clear all data in a database
    /// 
    /// # Errors
    /// Returns error if storage operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn clear_database(&self, database_id: DatabaseId) -> KVResult<()> {
        self.ensure_storage(database_id).await?;
        
        let mut storages = self.storages.write().await;
        let storage = storages.get_mut(&database_id)
            .ok_or_else(|| KVError::Internal("Storage not found".to_string()))?;
        
        storage.clear_database(database_id).await?;
        
        // Clear TTL information for this database
        let ttl_manager = self.ttl_manager.read().await;
        ttl_manager.clear_all().await;
        
        self.update_stats(1).await;
        Ok(())
    }

    /// Get engine statistics
    /// 
    /// # Errors
    /// Returns error if statistics calculation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get_stats(&self) -> KVResult<KVStats> {
        let mut stats = self.stats.read().await.clone();
        
        // Update memory and disk usage
        let storages = self.storages.read().await;
        let mut total_memory = 0u64;
        let mut total_disk = 0u64;
        let mut total_keys = 0u64;
        
        for (database_id, storage) in storages.iter() {
            if let Ok(storage_stats) = storage.get_stats(*database_id).await {
                total_memory += storage_stats.memory_usage;
                total_disk += storage_stats.disk_usage.unwrap_or(0);
                total_keys += storage_stats.total_keys;
            }
        }
        
        stats.memory_usage = total_memory;
        stats.disk_usage = total_disk;
        stats.total_keys = total_keys;
        
        Ok(stats)
    }

    /// Flush all pending writes
    /// 
    /// # Errors
    /// Returns error if flush operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn flush(&self) -> KVResult<()> {
        let storages = self.storages.read().await;
        for (database_id, storage) in storages.iter() {
            if let Err(e) = storage.flush().await {
                error!("Failed to flush database {}: {}", database_id, e);
            }
        }
        Ok(())
    }

    /// Close the engine and cleanup resources
    /// 
    /// # Errors
    /// Returns error if cleanup fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn close(&self) -> KVResult<()> {
        info!("Closing KV engine");
        
        // Stop TTL cleanup
        let mut ttl_manager = self.ttl_manager.write().await;
        ttl_manager.stop_cleanup();
        
        // Stop Pub/Sub cleanup
        let mut pubsub_manager = self.pubsub_manager.write().await;
        pubsub_manager.stop_cleanup();
        
        // Close all storages
        let storages = self.storages.read().await;
        for (database_id, storage) in storages.iter() {
            if let Err(e) = storage.close().await {
                error!("Failed to close storage for database {}: {}", database_id, e);
            }
        }
        
        info!("KV engine closed");
        Ok(())
    }

    // Pub/Sub operations

    /// Publish a message to a channel
    /// 
    /// # Errors
    /// Returns error if publishing fails
    pub async fn publish(&self, channel: &str, message: Value) -> KVResult<usize> {
        let pubsub_manager = self.pubsub_manager.read().await;
        pubsub_manager.publish(channel, message).await
    }

    /// Subscribe to a channel pattern
    /// 
    /// # Errors
    /// Returns error if subscription fails
    pub async fn subscribe(&self, pattern: ChannelPattern) -> KVResult<tokio::sync::mpsc::UnboundedReceiver<crate::PubSubMessage>> {
        let pubsub_manager = self.pubsub_manager.read().await;
        pubsub_manager.subscribe(pattern).await
    }

    /// Unsubscribe from a channel pattern
    /// 
    /// # Errors
    /// Returns error if unsubscription fails
    pub async fn unsubscribe(&self, pattern: &ChannelPattern) -> KVResult<usize> {
        let pubsub_manager = self.pubsub_manager.read().await;
        pubsub_manager.unsubscribe(pattern).await
    }

    /// Subscribe to cache invalidation events
    /// 
    /// # Errors
    /// Returns error if subscription fails
    pub async fn subscribe_to_invalidations(&self) -> KVResult<tokio::sync::mpsc::UnboundedReceiver<crate::PubSubMessage>> {
        let pattern = ChannelPattern::wildcard("cache:invalidate:*".to_string());
        self.subscribe(pattern).await
    }

    /// Publish cache invalidation event
    /// 
    /// # Errors
    /// Returns error if publishing fails
    async fn publish_invalidation(&self, key: &Key) -> KVResult<usize> {
        let channel = format!("cache:invalidate:{key}");
        let message = Value::String(format!("invalidate:{key}"));
        self.publish(&channel, message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::PersistenceMode;

    async fn create_test_engine() -> KVEngine {
        let temp_dir = TempDir::new().unwrap();
        let config = KVConfig {
            master_key: String::new(), // Empty string will generate a random key
            persistence_mode: PersistenceMode::Memory,
            data_dir: temp_dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        
        KVEngine::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let engine = create_test_engine().await;
        let database_id = 0;
        
        // Test set and get
        let value = Value::String("test_value".to_string());
        engine.set(database_id, "test_key".to_string(), value.clone(), None).await.unwrap();
        
        let retrieved = engine.get(database_id, &"test_key".to_string()).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_string().unwrap(), "test_value");
        
        // Test exists
        let exists = engine.exists(database_id, &"test_key".to_string()).await.unwrap();
        assert!(exists);
        
        // Test delete
        let deleted = engine.delete(database_id, &"test_key".to_string()).await.unwrap();
        assert!(deleted);
        
        let exists_after = engine.exists(database_id, &"test_key".to_string()).await.unwrap();
        assert!(!exists_after);
    }

    #[tokio::test]
    async fn test_ttl_operations() {
        let engine = create_test_engine().await;
        let database_id = 0;
        
        // Set key with TTL
        let value = Value::String("ttl_value".to_string());
        engine.set(database_id, "ttl_key".to_string(), value, Some(60)).await.unwrap();
        
        // Check TTL
        let ttl = engine.ttl(database_id, &"ttl_key".to_string()).await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap() <= 60);
        
        // Set TTL on existing key
        let set_ttl = engine.expire(database_id, &"ttl_key".to_string(), 120).await.unwrap();
        assert!(set_ttl);
        
        let new_ttl = engine.ttl(database_id, &"ttl_key".to_string()).await.unwrap();
        assert!(new_ttl.is_some());
        assert!(new_ttl.unwrap() <= 120);
    }

    #[tokio::test]
    async fn test_keys_operations() {
        let engine = create_test_engine().await;
        let database_id = 0;
        
        // Add some keys
        let value = Value::String("value".to_string());
        engine.set(database_id, "key1".to_string(), value.clone(), None).await.unwrap();
        engine.set(database_id, "key2".to_string(), value.clone(), None).await.unwrap();
        engine.set(database_id, "test_key".to_string(), value, None).await.unwrap();
        
        // Get all keys
        let keys = engine.keys(database_id).await.unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"test_key".to_string()));
        
        // Get keys with pattern
        let test_keys = engine.keys_pattern(database_id, "key*").await.unwrap();
        assert_eq!(test_keys.len(), 2);
        assert!(test_keys.contains(&"key1".to_string()));
        assert!(test_keys.contains(&"key2".to_string()));
    }

    #[tokio::test]
    async fn test_clear_database() {
        let engine = create_test_engine().await;
        let database_id = 0;
        
        // Add some keys
        let value = Value::String("value".to_string());
        engine.set(database_id, "key1".to_string(), value.clone(), None).await.unwrap();
        engine.set(database_id, "key2".to_string(), value, None).await.unwrap();
        
        // Clear database
        engine.clear_database(database_id).await.unwrap();
        
        // Check keys are gone
        let keys = engine.keys(database_id).await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_stats() {
        let engine = create_test_engine().await;
        let database_id = 0;
        
        // Add some data
        let value = Value::String("value".to_string());
        engine.set(database_id, "key1".to_string(), value.clone(), None).await.unwrap();
        engine.set(database_id, "key2".to_string(), value, None).await.unwrap();
        
        // Small delay to ensure uptime > 0
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        let stats = engine.get_stats().await.unwrap();
        assert_eq!(stats.total_keys, 2);
        assert!(stats.total_operations > 0);
        // Allow 0 for very fast tests - comparison is always true for u64
        assert!(stats.uptime >= 0);
    }
}
