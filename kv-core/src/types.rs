//! Core types for the KV service

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, BTreeMap};
use chrono::{DateTime, Utc};

/// Database identifier (0-15 for compatibility with Redis)
pub type DatabaseId = u8;

/// Key type
pub type Key = String;

/// Value type (can be any serializable data)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Value {
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl Value {
    #[must_use] 
    pub const fn as_string(&self) -> Option<&String> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    #[must_use] 
    pub const fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    #[must_use] 
    pub const fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Json(j) => Some(j),
            _ => None,
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Self::Bytes(b)
    }
}

impl From<serde_json::Value> for Value {
    fn from(j: serde_json::Value) -> Self {
        Self::Json(j)
    }
}

/// TTL (Time To Live) in seconds
pub type TTL = u64;

/// Entry with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub access_count: u64,
    pub last_accessed: DateTime<Utc>,
}

impl Entry {
    #[must_use] 
    pub fn new(value: Value, ttl: Option<TTL>) -> Self {
        let now = Utc::now();
        #[allow(clippy::cast_possible_wrap)]
        let expires_at = ttl.map(|ttl| now + chrono::Duration::seconds(ttl as i64));
        
        Self {
            value,
            created_at: now,
            expires_at,
            access_count: 0,
            last_accessed: now,
        }
    }

    #[must_use] 
    #[allow(clippy::option_if_let_else)]
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Utc::now() > expires_at
        } else {
            false
        }
    }

    #[must_use] 
    #[allow(clippy::option_if_let_else)]
    pub fn remaining_ttl(&self) -> Option<TTL> {
        if let Some(expires_at) = self.expires_at {
            let now = Utc::now();
            if now < expires_at {
                Some((expires_at - now).num_seconds().try_into().unwrap_or(0))
            } else {
                Some(0) // Expired
            }
        } else {
            None // No TTL
        }
    }

    pub fn touch(&mut self) {
        self.access_count += 1;
        self.last_accessed = Utc::now();
    }
}

/// List data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct List {
    pub items: Vec<Value>,
}

impl Default for List {
    fn default() -> Self {
        Self::new()
    }
}

impl List {
    #[must_use]
    pub const fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push_left(&mut self, value: Value) {
        self.items.insert(0, value);
    }

    pub fn push_right(&mut self, value: Value) {
        self.items.push(value);
    }

    pub fn pop_left(&mut self) -> Option<Value> {
        if self.items.is_empty() {
            None
        } else {
            Some(self.items.remove(0))
        }
    }

    pub fn pop_right(&mut self) -> Option<Value> {
        self.items.pop()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    #[must_use]
    pub fn get(&self, index: isize) -> Option<&Value> {
        if index < 0 {
            let abs_index = (-index - 1).try_into().unwrap_or(0);
            self.items.get(self.items.len().saturating_sub(abs_index + 1))
        } else {
            self.items.get(index.try_into().unwrap_or(0))
        }
    }

    #[must_use]
    pub fn range(&self, start: isize, end: isize) -> Vec<&Value> {
        let len = self.items.len().try_into().unwrap_or(0);
        let start = if start < 0 { len + start } else { start };
        let end = if end < 0 { len + end } else { end };
        
        let start = start.max(0).try_into().unwrap_or(0);
        let end = end.min(len - 1).try_into().unwrap_or(0);
        
        if start > end {
            return Vec::new();
        }
        
        self.items[start..=end].iter().collect()
    }
}

/// Set data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Set {
    pub items: HashSet<Value>,
}

impl Default for Set {
    fn default() -> Self {
        Self::new()
    }
}

impl Set {
    #[must_use]
    pub fn new() -> Self {
        Self { items: HashSet::new() }
    }

    pub fn add(&mut self, value: Value) -> bool {
        self.items.insert(value)
    }

    pub fn remove(&mut self, value: &Value) -> bool {
        self.items.remove(value)
    }

    #[must_use]
    pub fn contains(&self, value: &Value) -> bool {
        self.items.contains(value)
    }

    #[must_use]
    pub fn members(&self) -> Vec<&Value> {
        self.items.iter().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// Sorted set entry
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SortedSetEntry {
    pub member: Value,
    pub score: f64,
}

impl SortedSetEntry {
    #[must_use]
    pub const fn new(member: Value, score: f64) -> Self {
        Self { member, score }
    }
}

/// Sorted set data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedSet {
    pub entries: BTreeMap<OrderedFloat<f64>, Vec<Value>>, // score -> members with that score
    pub member_scores: HashMap<Value, f64>, // member -> score
}

/// Wrapper for f64 to make it Ord
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderedFloat<F>(pub F);

impl<F: PartialEq> Eq for OrderedFloat<F> {}

impl<F: PartialOrd> PartialOrd for OrderedFloat<F> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<F: PartialOrd> Ord for OrderedFloat<F> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl<F: std::hash::Hash> std::hash::Hash for OrderedFloat<F> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            member_scores: HashMap::new(),
        }
    }

    pub fn add(&mut self, member: Value, score: f64) -> bool {
        // Remove from old score if exists
        if let Some(old_score) = self.member_scores.remove(&member) {
            let ordered_old_score = OrderedFloat(old_score);
            if let Some(members) = self.entries.get_mut(&ordered_old_score) {
                members.retain(|m| m != &member);
                if members.is_empty() {
                    self.entries.remove(&ordered_old_score);
                }
            }
        }

        // Add to new score
        let ordered_score = OrderedFloat(score);
        self.entries.entry(ordered_score).or_default().push(member.clone());
        self.member_scores.insert(member, score);
        true
    }

    pub fn remove(&mut self, member: &Value) -> bool {
        if let Some(score) = self.member_scores.remove(member) {
            let ordered_score = OrderedFloat(score);
            if let Some(members) = self.entries.get_mut(&ordered_score) {
                members.retain(|m| m != member);
                if members.is_empty() {
                    self.entries.remove(&ordered_score);
                }
            }
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn score(&self, member: &Value) -> Option<f64> {
        self.member_scores.get(member).copied()
    }

    #[must_use]
    pub fn range(&self, start: isize, end: isize) -> Vec<&Value> {
        let all_entries: Vec<_> = self.entries
            .iter()
            .flat_map(|(_, members)| members.iter())
            .collect();
        
        let len = all_entries.len().try_into().unwrap_or(0);
        let start = if start < 0 { len + start } else { start };
        let end = if end < 0 { len + end } else { end };
        
        let start = start.max(0).try_into().unwrap_or(0);
        let end = end.min(len - 1).try_into().unwrap_or(0);
        
        if start > end {
            return Vec::new();
        }
        
        all_entries[start..=end].to_vec()
    }

    #[must_use]
    pub fn range_by_score(&self, min_score: f64, max_score: f64) -> Vec<&Value> {
        let min_ordered = OrderedFloat(min_score);
        let max_ordered = OrderedFloat(max_score);
        self.entries
            .range(min_ordered..=max_ordered)
            .flat_map(|(_, members)| members.iter())
            .collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.member_scores.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.member_scores.is_empty()
    }
}

/// Hash data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hash {
    pub fields: HashMap<String, Value>,
}

impl Default for Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash {
    #[must_use]
    pub fn new() -> Self {
        Self { fields: HashMap::new() }
    }

    pub fn set(&mut self, field: String, value: Value) -> bool {
        self.fields.insert(field, value).is_some()
    }

    #[must_use]
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.fields.get(field)
    }

    pub fn delete(&mut self, field: &str) -> bool {
        self.fields.remove(field).is_some()
    }

    #[must_use]
    pub fn exists(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    #[must_use]
    pub fn keys(&self) -> Vec<&String> {
        self.fields.keys().collect()
    }

    #[must_use]
    pub fn values(&self) -> Vec<&Value> {
        self.fields.values().collect()
    }

    #[must_use]
    pub const fn all(&self) -> &HashMap<String, Value> {
        &self.fields
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Data structure types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataStructure {
    String(Value),
    List(List),
    Set(Set),
    SortedSet(SortedSet),
    Hash(Hash),
}

impl DataStructure {
    #[must_use]
    pub const fn as_string(&self) -> Option<&Value> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_list(&self) -> Option<&List> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub const fn as_list_mut(&mut self) -> Option<&mut List> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_set(&self) -> Option<&Set> {
        match self {
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub const fn as_set_mut(&mut self) -> Option<&mut Set> {
        match self {
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_sorted_set(&self) -> Option<&SortedSet> {
        match self {
            Self::SortedSet(s) => Some(s),
            _ => None,
        }
    }

    pub const fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSet> {
        match self {
            Self::SortedSet(s) => Some(s),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_hash(&self) -> Option<&Hash> {
        match self {
            Self::Hash(h) => Some(h),
            _ => None,
        }
    }

    pub const fn as_hash_mut(&mut self) -> Option<&mut Hash> {
        match self {
            Self::Hash(h) => Some(h),
            _ => None,
        }
    }
}

/// Persistence mode
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum PersistenceMode {
    /// In-memory only (no persistence)
    Memory,
    /// Append-only file
    AOF,
    /// Full durability (synchronous writes)
    Full,
    /// Hybrid (memory + AOF)
    Hybrid,
}

/// Configuration for the KV engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVConfig {
    /// Master encryption key (base64 encoded)
    pub master_key: String,
    /// Persistence mode
    pub persistence_mode: PersistenceMode,
    /// Data directory for persistence
    pub data_dir: String,
    /// Snapshot interval in seconds
    pub snapshot_interval: u64,
    /// AOF sync interval in seconds
    pub aof_sync_interval: u64,
    /// Maximum memory usage in bytes
    pub max_memory: Option<u64>,
    /// Enable compression
    pub enable_compression: bool,
    /// Key expiration check interval in seconds
    pub expiration_check_interval: u64,
}

impl Default for KVConfig {
    fn default() -> Self {
        Self {
            master_key: String::new(),
            persistence_mode: PersistenceMode::Hybrid,
            data_dir: "./data".to_string(),
            snapshot_interval: 300, // 5 minutes
            aof_sync_interval: 1,   // 1 second
            max_memory: None,
            enable_compression: true,
            expiration_check_interval: 60, // 1 minute
        }
    }
}

/// Statistics for the KV engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KVStats {
    /// Total number of keys
    pub total_keys: u64,
    /// Number of expired keys
    pub expired_keys: u64,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Disk usage in bytes
    pub disk_usage: u64,
    /// Total operations performed
    pub total_operations: u64,
    /// Operations per second
    pub ops_per_second: f64,
    /// Uptime in seconds
    pub uptime: u64,
    /// Number of active connections
    pub active_connections: u32,
}

/// Pub/Sub message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: Value,
    pub timestamp: DateTime<Utc>,
}

/// Pub/Sub subscription
#[derive(Debug, Clone)]
pub struct PubSubSubscription {
    pub channel: String,
    pub sender: tokio::sync::mpsc::UnboundedSender<PubSubMessage>,
}
