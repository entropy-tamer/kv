//! Pub/Sub system for the KV service
//! 
//! Provides channel-based publish/subscribe functionality with pattern matching
//! and thread-safe message broadcasting for cache invalidation events.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Duration;
use chrono::{DateTime, Utc};
use tracing::{debug, warn};

use crate::{KVResult, Value, PubSubMessage};

/// Pattern matching for channel subscriptions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChannelPattern {
    /// Exact channel match
    Exact(String),
    /// Wildcard pattern (e.g., "cache:*" matches "cache:invalidate:user123")
    Wildcard(String),
}

impl ChannelPattern {
    /// Create a new exact channel pattern
    #[must_use]
    pub const fn exact(channel: String) -> Self {
        Self::Exact(channel)
    }

    /// Create a new wildcard pattern
    #[must_use]
    pub const fn wildcard(pattern: String) -> Self {
        Self::Wildcard(pattern)
    }

    /// Check if a channel matches this pattern
    #[must_use]
    pub fn matches(&self, channel: &str) -> bool {
        match self {
            Self::Exact(exact) => exact == channel,
            Self::Wildcard(pattern) => {
                // Simple wildcard matching: * matches any characters
                if pattern.contains('*') {
                    let parts: Vec<&str> = pattern.split('*').collect();
                    if parts.len() == 2 {
                        // Single wildcard: prefix*suffix
                        channel.starts_with(parts[0]) && channel.ends_with(parts[1])
                    } else {
                        // Multiple wildcards - for now, just check if it's a prefix
                        channel.starts_with(parts[0])
                    }
                } else {
                    pattern == channel
                }
            }
        }
    }
}

/// Subscription information
#[derive(Debug)]
struct Subscription {
    #[allow(dead_code)]
    pattern: ChannelPattern,
    sender: mpsc::UnboundedSender<PubSubMessage>,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    last_activity: DateTime<Utc>,
}

/// Pub/Sub manager for handling channel subscriptions and message broadcasting
pub struct PubSubManager {
    /// Map of channel patterns to subscribers
    subscriptions: Arc<RwLock<HashMap<ChannelPattern, Vec<Subscription>>>>,
    /// Cleanup interval for inactive subscriptions
    cleanup_interval: Duration,
    /// Subscription timeout
    subscription_timeout: Duration,
    /// Background cleanup task handle
    cleanup_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PubSubManager {
    /// Create a new pub/sub manager
    #[must_use]
    pub fn new(cleanup_interval: Duration, subscription_timeout: Duration) -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            cleanup_interval,
            subscription_timeout,
            cleanup_handle: None,
        }
    }

    /// Start the background cleanup task
    pub fn start_cleanup(&mut self) {
        let subscriptions = Arc::clone(&self.subscriptions);
        let cleanup_interval = self.cleanup_interval;
        let subscription_timeout = self.subscription_timeout;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            
            loop {
                interval.tick().await;
                
                let now = Utc::now();
                let mut subs = subscriptions.write().await;
                
                // Remove inactive subscriptions
                for (pattern, subs_list) in subs.iter_mut() {
                    subs_list.retain(|sub| {
                        let is_active = (now - sub.last_activity).to_std()
                            .map(|d| d < subscription_timeout)
                            .unwrap_or(false);
                        
                        if !is_active {
                            debug!("Removing inactive subscription for pattern: {:?}", pattern);
                        }
                        
                        is_active
                    });
                }
                
                // Remove empty pattern entries
                subs.retain(|_, subs_list| !subs_list.is_empty());
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

    /// Subscribe to a channel pattern
    /// 
    /// # Errors
    /// Returns error if subscription setup fails
    pub async fn subscribe(&self, pattern: ChannelPattern) -> KVResult<mpsc::UnboundedReceiver<PubSubMessage>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let pattern_clone = pattern.clone();
        
        let subscription = Subscription {
            pattern: pattern.clone(),
            sender,
            created_at: Utc::now(),
            last_activity: Utc::now(),
        };

        self.subscriptions.write().await.entry(pattern).or_insert_with(Vec::new).push(subscription);
        
        debug!("New subscription created for pattern: {:?}", pattern_clone);
        Ok(receiver)
    }

    /// Unsubscribe from a channel pattern
    /// 
    /// # Errors
    /// Returns error if unsubscription fails
    pub async fn unsubscribe(&self, pattern: &ChannelPattern) -> KVResult<usize> {
        let mut subscriptions = self.subscriptions.write().await;
        
        if let Some(subs_list) = subscriptions.get_mut(pattern) {
            let count = subs_list.len();
            subs_list.clear();
            
            if subs_list.is_empty() {
                subscriptions.remove(pattern);
            }
            
            debug!("Unsubscribed {} subscribers from pattern: {:?}", count, pattern);
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Publish a message to a channel
    /// 
    /// # Errors
    /// Returns error if publishing fails
    pub async fn publish(&self, channel: &str, message: Value) -> KVResult<usize> {
        let pubsub_message = PubSubMessage {
            channel: channel.to_string(),
            message,
            timestamp: Utc::now(),
        };

        let subscriptions = self.subscriptions.read().await;
        let mut delivered_count = 0;
        let mut failed_deliveries = Vec::new();

        // Find all matching subscriptions
        for (pattern, subs_list) in subscriptions.iter() {
            if pattern.matches(channel) {
                for (index, subscription) in subs_list.iter().enumerate() {
                    if let Err(e) = subscription.sender.send(pubsub_message.clone()) {
                        warn!("Failed to deliver message to subscriber: {}", e);
                        failed_deliveries.push((pattern.clone(), index));
                    } else {
                        delivered_count += 1;
                    }
                }
            }
        }

        // Clean up failed deliveries
        if !failed_deliveries.is_empty() {
            drop(subscriptions);
            let mut subs = self.subscriptions.write().await;
            
            for (pattern, index) in failed_deliveries {
                if let Some(subs_list) = subs.get_mut(&pattern) {
                    if index < subs_list.len() {
                        subs_list.remove(index);
                    }
                    if subs_list.is_empty() {
                        subs.remove(&pattern);
                    }
                }
            }
        }

        debug!("Published message to channel '{}', delivered to {} subscribers", channel, delivered_count);
        Ok(delivered_count)
    }

    /// Get statistics about subscriptions
    /// 
    /// # Errors
    /// Returns error if stats calculation fails
    pub async fn get_stats(&self) -> KVResult<PubSubStats> {
        let mut total_subscriptions = 0;
        let mut pattern_count = 0;
        let mut exact_patterns = 0;
        let mut wildcard_patterns = 0;

        for (pattern, subs_list) in self.subscriptions.read().await.iter() {
            pattern_count += 1;
            total_subscriptions += subs_list.len();
            
            match pattern {
                ChannelPattern::Exact(_) => exact_patterns += 1,
                ChannelPattern::Wildcard(_) => wildcard_patterns += 1,
            }
        }

        Ok(PubSubStats {
            total_subscriptions,
            pattern_count,
            exact_patterns,
            wildcard_patterns,
        })
    }

    /// Get all active channel patterns
    /// 
    /// # Errors
    /// Returns error if pattern retrieval fails
    pub async fn get_active_patterns(&self) -> KVResult<Vec<ChannelPattern>> {
        let subscriptions = self.subscriptions.read().await;
        Ok(subscriptions.keys().cloned().collect())
    }
}

/// Statistics for pub/sub system
#[derive(Debug, Clone)]
pub struct PubSubStats {
    pub total_subscriptions: usize,
    pub pattern_count: usize,
    pub exact_patterns: usize,
    pub wildcard_patterns: usize,
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(300), // 5 minutes cleanup interval
            Duration::from_secs(3600), // 1 hour subscription timeout
        )
    }
}

impl Drop for PubSubManager {
    fn drop(&mut self) {
        self.stop_cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    async fn create_test_manager() -> PubSubManager {
        PubSubManager::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
    }

    #[tokio::test]
    async fn test_exact_channel_subscription() {
        let manager = create_test_manager().await;
        
        // Subscribe to exact channel
        let pattern = ChannelPattern::exact("test:channel".to_string());
        let mut receiver = manager.subscribe(pattern).await.unwrap();
        
        // Publish message
        let message = Value::String("Hello, World!".to_string());
        let delivered = manager.publish("test:channel", message.clone()).await.unwrap();
        assert_eq!(delivered, 1);
        
        // Receive message
        let received = timeout(Duration::from_millis(100), receiver.recv()).await.unwrap().unwrap();
        assert_eq!(received.channel, "test:channel");
        assert_eq!(received.message, message);
    }

    #[tokio::test]
    async fn test_wildcard_channel_subscription() {
        let manager = create_test_manager().await;
        
        // Subscribe to wildcard pattern
        let pattern = ChannelPattern::wildcard("cache:*".to_string());
        let mut receiver = manager.subscribe(pattern).await.unwrap();
        
        // Publish message to matching channel
        let message = Value::String("Invalidate user123".to_string());
        let delivered = manager.publish("cache:invalidate:user123", message.clone()).await.unwrap();
        assert_eq!(delivered, 1);
        
        // Receive message
        let received = timeout(Duration::from_millis(100), receiver.recv()).await.unwrap().unwrap();
        assert_eq!(received.channel, "cache:invalidate:user123");
        assert_eq!(received.message, message);
        
        // Publish to non-matching channel
        let delivered = manager.publish("other:channel", Value::String("test".to_string())).await.unwrap();
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let manager = create_test_manager().await;
        
        // Create multiple subscribers
        let pattern = ChannelPattern::exact("broadcast".to_string());
        let mut receiver1 = manager.subscribe(pattern.clone()).await.unwrap();
        let mut receiver2 = manager.subscribe(pattern).await.unwrap();
        
        // Publish message
        let message = Value::String("Broadcast message".to_string());
        let delivered = manager.publish("broadcast", message.clone()).await.unwrap();
        assert_eq!(delivered, 2);
        
        // Both should receive the message
        let received1 = timeout(Duration::from_millis(100), receiver1.recv()).await.unwrap().unwrap();
        let received2 = timeout(Duration::from_millis(100), receiver2.recv()).await.unwrap().unwrap();
        
        assert_eq!(received1.message, message);
        assert_eq!(received2.message, message);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let manager = create_test_manager().await;
        
        // Subscribe
        let pattern = ChannelPattern::exact("test:unsub".to_string());
        let _receiver = manager.subscribe(pattern.clone()).await.unwrap();
        
        // Publish before unsubscribe
        let delivered = manager.publish("test:unsub", Value::String("test".to_string())).await.unwrap();
        assert_eq!(delivered, 1);
        
        // Unsubscribe
        let unsub_count = manager.unsubscribe(&pattern).await.unwrap();
        assert_eq!(unsub_count, 1);
        
        // Publish after unsubscribe
        let delivered = manager.publish("test:unsub", Value::String("test2".to_string())).await.unwrap();
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = create_test_manager().await;
        
        // Initial stats
        let stats = manager.get_stats().await.unwrap();
        assert_eq!(stats.total_subscriptions, 0);
        assert_eq!(stats.pattern_count, 0);
        
        // Add subscriptions
        let _receiver1 = manager.subscribe(ChannelPattern::exact("exact1".to_string())).await.unwrap();
        let _receiver2 = manager.subscribe(ChannelPattern::exact("exact2".to_string())).await.unwrap();
        let _receiver3 = manager.subscribe(ChannelPattern::wildcard("wild:*".to_string())).await.unwrap();
        
        let stats = manager.get_stats().await.unwrap();
        assert_eq!(stats.total_subscriptions, 3);
        assert_eq!(stats.pattern_count, 3);
        assert_eq!(stats.exact_patterns, 2);
        assert_eq!(stats.wildcard_patterns, 1);
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        // Test exact matching
        let exact = ChannelPattern::exact("test:channel".to_string());
        assert!(exact.matches("test:channel"));
        assert!(!exact.matches("test:other"));
        
        // Test wildcard matching
        let wildcard = ChannelPattern::wildcard("cache:*".to_string());
        assert!(wildcard.matches("cache:invalidate"));
        assert!(wildcard.matches("cache:invalidate:user123"));
        assert!(!wildcard.matches("other:invalidate"));
        
        // Test prefix wildcard
        let prefix = ChannelPattern::wildcard("auth:*".to_string());
        assert!(prefix.matches("auth:login"));
        assert!(prefix.matches("auth:logout"));
        assert!(!prefix.matches("cache:login"));
    }
}
