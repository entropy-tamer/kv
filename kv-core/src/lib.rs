//! # KV Core
//! 
//! Core storage engine for the Reynard KV service.
//! Provides secure, encrypted key-value storage with TTL support,
//! multiple data structures, and various persistence options.

pub mod engine;
pub mod storage;
pub mod encryption;
pub mod ttl;
pub mod types;
pub mod error;
pub mod pubsub;

pub use engine::KVEngine;
pub use types::*;
pub use error::{KVError, KVResult};
pub use storage::{Storage, StorageFactory, StorageStats};

/// Re-export commonly used types
pub use serde::{Deserialize, Serialize};
pub use uuid::Uuid;
pub use chrono::{DateTime, Utc};
