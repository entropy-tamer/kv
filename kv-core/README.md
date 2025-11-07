# kv-core

Core storage engine for the KV service - a secure, encrypted key-value store designed to replace Redis with enhanced security and performance.

## 🚀 Features

- **🔐 AES-256-GCM Encryption**: All data encrypted at rest with database-specific keys
- **⚡ High Performance**: Built with Rust for maximum speed and memory safety
- **💾 Persistent Storage**: Configurable persistence modes using Sled backend
- **🔄 Pub/Sub Support**: Real-time messaging with pattern-based subscriptions
- **🧵 Thread Safe**: Concurrent access with DashMap for in-memory operations
- **📊 Metrics**: Built-in performance monitoring and health checks

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kv-core = "0.1.0"
```

## 🚀 Quick Start

```rust
use kv_core::{KVEngine, Config, PersistenceMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = Config {
        master_key: "your-base64-encoded-key".to_string(),
        persistence_mode: PersistenceMode::Hybrid,
        data_dir: "./data/kv".to_string(),
        max_memory_size: 100 * 1024 * 1024, // 100MB
        compression: true,
    };

    // Initialize the engine
    let mut engine = KVEngine::new(config).await?;

    // Basic operations
    engine.set(0, "user:123", "john_doe", None).await?;
    let value = engine.get(0, "user:123").await?;
    println!("User: {:?}", value);

    // Pub/Sub operations
    engine.publish("notifications", "Hello World!").await?;

    // Cleanup
    engine.shutdown().await?;
    Ok(())
}
```

## 🔧 Configuration

### Config Structure

```rust
pub struct Config {
    pub master_key: String,           // Base64-encoded encryption key
    pub persistence_mode: PersistenceMode, // Storage mode
    pub data_dir: String,             // Data directory path
    pub max_memory_size: usize,       // Memory cache limit
    pub compression: bool,            // Enable compression
}
```

### Persistence Modes

- **`Memory`**: Data stored only in memory (fastest, not persistent)
- **`Disk`**: Data stored only on disk (persistent, slower)
- **`Hybrid`**: Hot data in memory, cold data on disk (recommended)

## 🔐 Security Features

### Encryption

- **Master Key**: Base64-encoded 256-bit encryption key
- **Key Derivation**: HKDF-based derivation for database-specific keys
- **AES-256-GCM**: Authenticated encryption for data integrity
- **Secure Random**: Cryptographically secure random number generation

### Key Management

```rust
// Generate a new master key
let master_key = EncryptionManager::generate_master_key()?;

// Rotate master key
let new_key = encryption_manager.rotate_master_key()?;
```

## 📊 Performance

### Benchmarks

| Operation | Memory Mode | Hybrid Mode | Disk Mode |
| --------- | ----------- | ----------- | --------- |
| Set       | ~100ns      | ~200ns      | ~500ns    |
| Get       | ~50ns       | ~100ns      | ~300ns    |
| Delete    | ~80ns       | ~150ns      | ~400ns    |

### Memory Usage

- **Memory Mode**: ~1.2x data size
- **Hybrid Mode**: ~0.3x data size + disk storage
- **Disk Mode**: ~0.1x data size + disk storage

## 🧪 Testing

```bash
# Run unit tests
cargo test

# Run integration tests
cargo test --test integration

# Run benchmarks
cargo bench

# Run with specific log level
RUST_LOG=kv_core=debug cargo test
```

## 📚 API Reference

### Core Engine

#### `KVEngine`

Main engine for key-value operations.

```rust
impl KVEngine {
    pub async fn new(config: Config) -> KVResult<Self>
    pub async fn set(&mut self, db: u8, key: &str, value: &str, ttl: Option<u64>) -> KVResult<()>
    pub async fn get(&self, db: u8, key: &str) -> KVResult<Option<String>>
    pub async fn delete(&mut self, db: u8, key: &str) -> KVResult<bool>
    pub async fn exists(&self, db: u8, key: &str) -> KVResult<bool>
    pub async fn keys(&self, db: u8, pattern: Option<&str>) -> KVResult<Vec<String>>
    pub async fn clear_database(&mut self, db: u8) -> KVResult<()>
    pub async fn expire(&mut self, db: u8, key: &str, ttl: u64) -> KVResult<bool>
    pub async fn ttl(&self, db: u8, key: &str) -> KVResult<Option<u64>>
    pub async fn shutdown(&mut self) -> KVResult<()>
}
```

#### Pub/Sub Operations

```rust
impl KVEngine {
    pub async fn publish(&self, channel: &str, message: &str) -> KVResult<usize>
    pub async fn subscribe(&self, pattern: &str) -> KVResult<Subscription>
    pub async fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> KVResult<()>
}
```

### Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum KVError {
    #[error("Encryption error: {0}")]
    Encryption(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```

## 🔍 Monitoring

### Health Checks

```rust
// Check engine health
let health = engine.health_check().await?;
println!("Status: {:?}", health.status);
println!("Memory usage: {} bytes", health.memory_usage);
println!("Key count: {}", health.key_count);
```

### Metrics

```rust
// Get performance metrics
let metrics = engine.get_metrics().await?;
println!("Operations per second: {}", metrics.ops_per_second);
println!("Average latency: {}μs", metrics.avg_latency_us);
```

## 🛠️ Development

### Building from Source

```bash
git clone https://github.com/entropy-tamer/kv.git
cd kv/kv-core
cargo build --release
```

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_encryption

# With logging
RUST_LOG=debug cargo test
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../../LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📞 Support

- 📖 [Documentation](https://github.com/entropy-tamer/kv/wiki)
- 🐛 [Issue Tracker](https://github.com/entropy-tamer/kv/issues)
- 💬 [Discussions](https://github.com/entropy-tamer/kv/discussions)

---

**Part of the [KV](https://github.com/entropy-tamer/kv) ecosystem**
