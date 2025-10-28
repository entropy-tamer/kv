//! Error types for the KV service

use thiserror::Error;

/// Result type for KV operations
pub type KVResult<T> = Result<T, KVError>;

/// Error types for the KV service
#[derive(Error, Debug)]
pub enum KVError {
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid key: {0}")]
    InvalidKey(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("TTL error: {0}")]
    TTL(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("Database error: {0}")]
    Database(#[from] sled::Error),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Rate limit exceeded")]
    RateLimit,

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<&str> for KVError {
    fn from(msg: &str) -> Self {
        Self::Internal(msg.to_string())
    }
}

impl From<String> for KVError {
    fn from(msg: String) -> Self {
        Self::Internal(msg)
    }
}
