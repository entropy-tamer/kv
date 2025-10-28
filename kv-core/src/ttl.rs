//! TTL (Time To Live) management for the KV service
//! 
//! Handles expiration of keys and background cleanup of expired entries.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
#[cfg(test)]
use tokio::time::sleep;
use chrono::{DateTime, Utc};
use tracing::debug;

use crate::{KVError, KVResult, Key, Entry, TTL};

/// TTL manager for handling key expiration
pub struct TTLManager {
    /// Map of expiration time -> set of keys that expire at that time
    expiration_map: Arc<RwLock<BTreeMap<DateTime<Utc>, Vec<Key>>>>,
    /// Map of key -> expiration time (for quick lookup)
    key_expirations: Arc<RwLock<HashMap<Key, DateTime<Utc>>>>,
    /// Check interval for expired keys
    check_interval: Duration,
    /// Background task handle
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TTLManager {
    /// Create a new TTL manager
    #[must_use] 
    pub fn new(check_interval: Duration) -> Self {
        Self {
            expiration_map: Arc::new(RwLock::new(BTreeMap::new())),
            key_expirations: Arc::new(RwLock::new(HashMap::new())),
            check_interval,
            cleanup_handle: None,
        }
    }

    /// Start the background cleanup task
    pub fn start_cleanup(&mut self, cleanup_callback: impl Fn(Vec<Key>) + Send + Sync + 'static) {
        let expiration_map = Arc::clone(&self.expiration_map);
        let key_expirations = Arc::clone(&self.key_expirations);
        let check_interval = self.check_interval;

        let handle = tokio::spawn(async move {
            let mut interval = interval(check_interval);
            
            loop {
                interval.tick().await;
                
                let now = Utc::now();
                let expired_keys = {
                    let mut exp_map = expiration_map.write().await;
                    let mut key_exps = key_expirations.write().await;
                    
                    let mut expired = Vec::new();
                    
                    // Find all keys that have expired
                    let expired_times: Vec<DateTime<Utc>> = exp_map
                        .range(..now)
                        .map(|(time, _)| *time)
                        .collect();
                    
                    for time in expired_times {
                        if let Some(keys) = exp_map.remove(&time) {
                            for key in keys {
                                key_exps.remove(&key);
                                expired.push(key);
                            }
                        }
                    }
                    
                    expired
                };
                
                if !expired_keys.is_empty() {
                    debug!("Found {} expired keys", expired_keys.len());
                    cleanup_callback(expired_keys);
                }
            }
        });

        self.cleanup_handle = Some(handle);
    }

    /// Stop the background cleanup task
    pub fn stop_cleanup(&mut self) {
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
        }
    }

    /// Set TTL for a key
    /// 
    /// # Errors
    /// Returns error if TTL operation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn set_ttl(&self, key: Key, ttl: TTL) -> KVResult<()> {
        #[allow(clippy::cast_possible_wrap)]
        let expiration_time = Utc::now() + chrono::Duration::seconds(ttl as i64);
        
        let mut exp_map = self.expiration_map.write().await;
        let mut key_exps = self.key_expirations.write().await;
        
        // Remove from old expiration time if exists
        if let Some(old_time) = key_exps.get(&key)
            && let Some(keys) = exp_map.get_mut(old_time) {
                keys.retain(|k| k != &key);
                if keys.is_empty() {
                    exp_map.remove(old_time);
                }
            }
        
        // Add to new expiration time
        exp_map.entry(expiration_time).or_insert_with(Vec::new).push(key.clone());
        key_exps.insert(key, expiration_time);
        
        Ok(())
    }

    /// Remove TTL for a key (make it persistent)
    /// 
    /// # Errors
    /// Returns error if TTL removal fails
    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    pub async fn remove_ttl(&self, key: &Key) -> KVResult<bool> {
        let mut exp_map = self.expiration_map.write().await;
        let mut key_exps = self.key_expirations.write().await;
        
        if let Some(expiration_time) = key_exps.remove(key) {
            if let Some(keys) = exp_map.get_mut(&expiration_time) {
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    exp_map.remove(&expiration_time);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get remaining TTL for a key
    /// 
    /// # Errors
    /// Returns error if TTL retrieval fails
    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    pub async fn get_ttl(&self, key: &Key) -> KVResult<Option<TTL>> {
        let key_exps = self.key_expirations.read().await;
        
        if let Some(expiration_time) = key_exps.get(key) {
            let now = Utc::now();
            if now < *expiration_time {
                #[allow(clippy::cast_sign_loss)]
                let remaining = (*expiration_time - now).num_seconds() as u64;
                Ok(Some(remaining))
            } else {
                Ok(Some(0)) // Expired
            }
        } else {
            Ok(None) // No TTL set
        }
    }

    /// Check if a key has expired
    /// 
    /// # Errors
    /// Returns error if expiration check fails
    #[allow(clippy::significant_drop_tightening, clippy::option_if_let_else)]
    pub async fn is_expired(&self, key: &Key) -> KVResult<bool> {
        let key_exps = self.key_expirations.read().await;
        
        if let Some(expiration_time) = key_exps.get(key) {
            Ok(Utc::now() > *expiration_time)
        } else {
            Ok(false) // No TTL means not expired
        }
    }

    /// Get all keys that will expire within the given duration
    /// 
    /// # Errors
    /// Returns error if duration conversion fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get_expiring_keys(&self, within: Duration) -> KVResult<Vec<Key>> {
        let now = Utc::now();
        let future_time = now + chrono::Duration::from_std(within)
            .map_err(|e| KVError::TTL(format!("Invalid duration: {e}")))?;
        
        let exp_map = self.expiration_map.read().await;
        let mut expiring_keys = Vec::new();
        
        for (_expiration_time, keys) in exp_map.range(now..=future_time) {
            expiring_keys.extend(keys.clone());
        }
        
        Ok(expiring_keys)
    }

    /// Get statistics about TTL usage
    /// 
    /// # Errors
    /// Returns error if stats calculation fails
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get_stats(&self) -> KVResult<TTLStats> {
        let exp_map = self.expiration_map.read().await;
        let key_exps = self.key_expirations.read().await;
        
        let now = Utc::now();
        let mut expired_count = 0;
        let mut active_count = 0;
        
        for expiration_time in key_exps.values() {
            if *expiration_time <= now {
                expired_count += 1;
            } else {
                active_count += 1;
            }
        }
        
        Ok(TTLStats {
            total_keys_with_ttl: key_exps.len(),
            active_keys: active_count,
            expired_keys: expired_count,
            next_expiration: exp_map.keys().next().copied(),
        })
    }

    /// Clear all TTL information (for testing or reset)
    #[allow(clippy::significant_drop_tightening)]
    pub async fn clear_all(&self) {
        let mut exp_map = self.expiration_map.write().await;
        let mut key_exps = self.key_expirations.write().await;
        
        exp_map.clear();
        key_exps.clear();
    }
}

/// Statistics for TTL usage
#[derive(Debug, Clone)]
pub struct TTLStats {
    pub total_keys_with_ttl: usize,
    pub active_keys: usize,
    pub expired_keys: usize,
    pub next_expiration: Option<DateTime<Utc>>,
}

/// Helper trait for entries with TTL support
pub trait TTLSupport {
    fn is_expired(&self) -> bool;
    fn remaining_ttl(&self) -> Option<TTL>;
    fn set_ttl(&mut self, ttl: TTL);
    fn remove_ttl(&mut self);
}

impl TTLSupport for Entry {
    fn is_expired(&self) -> bool {
        self.is_expired()
    }

    fn remaining_ttl(&self) -> Option<TTL> {
        self.remaining_ttl()
    }

    fn set_ttl(&mut self, ttl: TTL) {
        #[allow(clippy::cast_possible_wrap)]
        let expiration_time = Utc::now() + chrono::Duration::seconds(ttl as i64);
        self.expires_at = Some(expiration_time);
    }

    fn remove_ttl(&mut self) {
        self.expires_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_ttl_set_and_get() {
        let ttl_manager = TTLManager::new(Duration::from_secs(1));
        
        // Set TTL for a key
        ttl_manager.set_ttl("test_key".to_string(), 60).await.unwrap();
        
        // Check TTL
        let ttl = ttl_manager.get_ttl(&"test_key".to_string()).await.unwrap();
        assert!(ttl.is_some());
        assert!(ttl.unwrap() <= 60);
        
        // Check not expired
        let expired = ttl_manager.is_expired(&"test_key".to_string()).await.unwrap();
        assert!(!expired);
    }

    #[tokio::test]
    async fn test_ttl_removal() {
        let ttl_manager = TTLManager::new(Duration::from_secs(1));
        
        // Set TTL
        ttl_manager.set_ttl("test_key".to_string(), 60).await.unwrap();
        
        // Remove TTL
        let removed = ttl_manager.remove_ttl(&"test_key".to_string()).await.unwrap();
        assert!(removed);
        
        // Check TTL is gone
        let ttl = ttl_manager.get_ttl(&"test_key".to_string()).await.unwrap();
        assert!(ttl.is_none());
    }

    #[tokio::test]
    async fn test_expiration_cleanup() {
        let mut ttl_manager = TTLManager::new(Duration::from_millis(100));
        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let cleanup_count_clone = Arc::clone(&cleanup_count);
        
        // Start cleanup with callback
        ttl_manager.start_cleanup(move |keys| {
            cleanup_count_clone.fetch_add(keys.len(), Ordering::Relaxed);
        });
        
        // Set a very short TTL
        ttl_manager.set_ttl("short_ttl_key".to_string(), 1).await.unwrap();
        
        // Wait for expiration
        sleep(Duration::from_millis(1500)).await;
        
        // Check that cleanup was called
        assert!(cleanup_count.load(Ordering::Relaxed) > 0);
        
        // Stop cleanup
        ttl_manager.stop_cleanup();
    }

    #[tokio::test]
    async fn test_ttl_stats() {
        let ttl_manager = TTLManager::new(Duration::from_secs(1));
        
        // Set some TTLs
        ttl_manager.set_ttl("key1".to_string(), 60).await.unwrap();
        ttl_manager.set_ttl("key2".to_string(), 120).await.unwrap();
        
        let stats = ttl_manager.get_stats().await.unwrap();
        assert_eq!(stats.total_keys_with_ttl, 2);
        assert_eq!(stats.active_keys, 2);
        assert_eq!(stats.expired_keys, 0);
        assert!(stats.next_expiration.is_some());
    }
}
