# KV - Secure Key-Value Store

A high-performance, encrypted key-value store designed to replace Redis with enhanced security and pub/sub capabilities. Built in Rust with Python bindings for seamless integration.

## 🚀 Features

- **🔐 End-to-End Encryption**: AES-256-GCM encryption for all data at rest
- **⚡ High Performance**: Built with Rust for maximum speed and memory safety
- **🔄 Pub/Sub Support**: Real-time messaging with pattern-based subscriptions
- **💾 Persistent Storage**: Configurable persistence modes with Sled backend
- **🌐 Multi-Language**: Rust core with Python bindings
- **🔧 Production Ready**: Comprehensive error handling and logging
- **📊 Monitoring**: Built-in metrics and health checks

## 📦 Components

### Core Libraries

- **[kv-core](./kv-core/)** - Core storage engine and encryption layer
- **[kv-client](./kv-client/)** - Client library for connecting to KV servers
- **[kv-python](./kv-python/)** - Python bindings for easy integration

### Server

- **[kv-server](./kv-server/)** - HTTP server for network access to KV service

## 🛠️ Installation

### Rust Crates

Add to your `Cargo.toml`:

```toml
[dependencies]
kv-core = "0.1.0"
kv-client = "0.1.0"
```

### Python Package

```bash
pip install kv-python
```

## 🚀 Quick Start

### Rust Example

```rust
use kv_core::{KVEngine, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let mut engine = KVEngine::new(config).await?;
    
    // Set a value
    engine.set(0, "user:123", "john_doe", None).await?;
    
    // Get a value
    let value = engine.get(0, "user:123").await?;
    println!("User: {:?}", value);
    
    // Publish a message
    engine.publish("notifications", "Hello World!").await?;
    
    Ok(())
}
```

### Python Example

```python
import asyncio
from kv_python import PyKVEngine

async def main():
    engine = PyKVEngine(
        master_key="your-base64-key",
        persistence_mode="hybrid",
        data_dir="./data"
    )
    
    # Set a value
    await engine.set(0, "user:123", "john_doe")
    
    # Get a value
    value = await engine.get(0, "user:123")
    print(f"User: {value}")
    
    # Publish a message
    await engine.publish("notifications", "Hello World!")

asyncio.run(main())
```

## 🔧 Configuration

### Environment Variables

```bash
# Master encryption key (base64 encoded)
KV_MASTER_KEY="your-base64-encoded-key"

# Data directory for persistence
KV_DATA_DIR="./data/kv"

# Persistence mode: memory, disk, hybrid
KV_PERSISTENCE_MODE="hybrid"

# Log level
RUST_LOG="kv=info"
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `master_key` | String | Generated | Base64-encoded encryption key |
| `persistence_mode` | String | `hybrid` | Storage mode: `memory`, `disk`, `hybrid` |
| `data_dir` | String | `./data/kv` | Directory for persistent storage |
| `max_memory_size` | usize | `100MB` | Maximum memory cache size |
| `compression` | bool | `true` | Enable data compression |

## 🔐 Security

- **AES-256-GCM Encryption**: All data encrypted at rest
- **Key Derivation**: HKDF-based key derivation for database-specific keys
- **Secure Random**: Cryptographically secure random number generation
- **Memory Safety**: Rust's ownership system prevents common vulnerabilities

## 📊 Performance

- **Memory Efficient**: Optimized data structures with configurable limits
- **Fast Lookups**: O(1) average case for key operations
- **Concurrent Access**: Thread-safe operations with minimal locking
- **Compression**: Optional data compression to reduce storage footprint

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run benchmarks
cargo bench

# Run Python tests
cd kv-python
python -m pytest
```

## 📚 API Reference

### Core Operations

- `set(db, key, value, ttl?)` - Store a key-value pair
- `get(db, key)` - Retrieve a value by key
- `delete(db, key)` - Remove a key
- `exists(db, key)` - Check if key exists
- `keys(db, pattern?)` - List keys with optional pattern matching

### Pub/Sub Operations

- `publish(channel, message)` - Publish a message to a channel
- `subscribe(pattern)` - Subscribe to messages matching a pattern
- `unsubscribe(subscription_id)` - Unsubscribe from a channel

### Database Management

- `clear_database(db)` - Clear all keys in a database
- `expire(db, key, ttl)` - Set expiration for a key
- `ttl(db, key)` - Get time-to-live for a key

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Rust](https://rust-lang.org/) for performance and safety
- Uses [Sled](https://github.com/spacejam/sled) for persistent storage
- Python bindings powered by [PyO3](https://pyo3.rs/)
- Encryption provided by [AES-GCM](https://crates.io/crates/aes-gcm)

## 📞 Support

- 📖 [Documentation](https://github.com/entropy-tamer/kv/wiki)
- 🐛 [Issue Tracker](https://github.com/entropy-tamer/kv/issues)
- 💬 [Discussions](https://github.com/entropy-tamer/kv/discussions)

---

**Made with ❤️ by the Reynard Team**
