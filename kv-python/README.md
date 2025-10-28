# kv-python

Python bindings for the KV service - a high-performance, encrypted key-value store with pub/sub capabilities. Easy-to-use Python API for the Rust-based KV engine.

## 🚀 Features

- **🐍 Python Native**: Seamless Python integration with async/await support
- **🔐 End-to-End Encryption**: AES-256-GCM encryption for all data
- **⚡ High Performance**: Rust-powered backend with Python convenience
- **🔄 Pub/Sub Support**: Real-time messaging with pattern subscriptions
- **💾 Persistent Storage**: Configurable persistence modes
- **🧵 Thread Safe**: Safe concurrent access from multiple Python threads
- **📊 Monitoring**: Built-in metrics and health checks

## 📦 Installation

### From PyPI (Recommended)

```bash
pip install kv-python
```

### From Source

```bash
git clone https://github.com/entropy-tamer/kv.git
cd kv/kv-python
pip install -e .
```

### Requirements

- Python 3.8+
- Rust toolchain (for building from source)

## 🚀 Quick Start

### Basic Usage

```python
import asyncio
from kv_python import PyKVEngine

async def main():
    # Initialize the engine
    engine = PyKVEngine(
        master_key="your-base64-encoded-key",
        persistence_mode="hybrid",
        data_dir="./data"
    )
    
    # Basic operations
    await engine.set(0, "user:123", "john_doe")
    value = await engine.get(0, "user:123")
    print(f"User: {value}")
    
    # Pub/Sub operations
    await engine.publish("notifications", "Hello World!")
    
    # Cleanup
    await engine.close()

# Run the example
asyncio.run(main())
```

### Context Manager

```python
import asyncio
from kv_python import PyKVEngine

async def main():
    async with PyKVEngine(
        master_key="your-key",
        persistence_mode="hybrid"
    ) as engine:
        await engine.set(0, "key", "value")
        value = await engine.get(0, "key")
        print(f"Value: {value}")

asyncio.run(main())
```

## 🔧 Configuration

### PyKVEngine Parameters

```python
engine = PyKVEngine(
    master_key="base64-encoded-key",    # Encryption key
    persistence_mode="hybrid",          # Storage mode
    data_dir="./data",                  # Data directory
    max_memory_size=100 * 1024 * 1024, # 100MB memory limit
    compression=True,                   # Enable compression
    log_level="info"                   # Log level
)
```

### Persistence Modes

- **`"memory"`**: Data stored only in memory (fastest, not persistent)
- **`"disk"`**: Data stored only on disk (persistent, slower)
- **`"hybrid"`**: Hot data in memory, cold data on disk (recommended)

### Environment Variables

```bash
# Set default configuration
export KV_MASTER_KEY="your-base64-key"
export KV_DATA_DIR="./data/kv"
export KV_PERSISTENCE_MODE="hybrid"
export KV_LOG_LEVEL="info"
```

## 📚 API Reference

### Core Operations

#### `set(database_id, key, value, ttl=None)`

Store a key-value pair with optional expiration.

```python
# Set without expiration
await engine.set(0, "user:123", "john_doe")

# Set with TTL (seconds)
await engine.set(0, "session:abc", "active", ttl=3600)
```

#### `get(database_id, key)`

Retrieve a value by key.

```python
value = await engine.get(0, "user:123")
if value is not None:
    print(f"User: {value}")
```

#### `delete(database_id, key)`

Remove a key.

```python
deleted = await engine.delete(0, "user:123")
print(f"Key deleted: {deleted}")
```

#### `exists(database_id, key)`

Check if a key exists.

```python
exists = await engine.exists(0, "user:123")
print(f"Key exists: {exists}")
```

#### `keys(database_id, pattern=None)`

List keys with optional pattern matching.

```python
# List all keys
all_keys = await engine.keys(0)

# List keys matching pattern
user_keys = await engine.keys(0, "user:*")
```

#### `clear_database(database_id)`

Clear all keys in a database.

```python
await engine.clear_database(0)
```

#### `expire(database_id, key, ttl)`

Set expiration for a key.

```python
success = await engine.expire(0, "user:123", 3600)
print(f"TTL set: {success}")
```

#### `ttl(database_id, key)`

Get time-to-live for a key.

```python
ttl_seconds = await engine.ttl(0, "user:123")
if ttl_seconds is not None:
    print(f"TTL: {ttl_seconds} seconds")
```

### Pub/Sub Operations

#### `publish(channel, message)`

Publish a message to a channel.

```python
subscribers = await engine.publish("notifications", "Hello World!")
print(f"Message sent to {subscribers} subscribers")
```

#### `subscribe(pattern)`

Subscribe to messages matching a pattern.

```python
subscription = await engine.subscribe("notifications:*")

# Listen for messages
async for message in subscription:
    print(f"Received: {message}")
```

### Utility Methods

#### `health_check()`

Check engine health.

```python
health = await engine.health_check()
print(f"Status: {health['status']}")
print(f"Memory usage: {health['memory_usage']} bytes")
print(f"Key count: {health['key_count']}")
```

#### `get_metrics()`

Get performance metrics.

```python
metrics = await engine.get_metrics()
print(f"Operations: {metrics['total_operations']}")
print(f"Avg latency: {metrics['avg_latency_us']}μs")
```

#### `close()`

Close the engine and cleanup resources.

```python
await engine.close()
```

## 🔐 Security

### Key Management

```python
# Generate a new master key
import base64
import secrets

def generate_master_key():
    key = secrets.token_bytes(32)
    return base64.b64encode(key).decode('utf-8')

master_key = generate_master_key()
print(f"Master key: {master_key}")
```

### Encryption

All data is automatically encrypted using:

- **AES-256-GCM**: Authenticated encryption
- **HKDF**: Key derivation for database-specific keys
- **Secure Random**: Cryptographically secure random generation

## 📊 Performance

### Benchmarks

| Operation | Memory Mode | Hybrid Mode | Disk Mode |
|-----------|-------------|-------------|-----------|
| Set | ~200ns | ~400ns | ~800ns |
| Get | ~100ns | ~200ns | ~500ns |
| Delete | ~150ns | ~300ns | ~600ns |

### Memory Usage

- **Memory Mode**: ~1.2x data size
- **Hybrid Mode**: ~0.3x data size + disk storage
- **Disk Mode**: ~0.1x data size + disk storage

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
python -m pytest

# Run specific test
python -m pytest test_basic_operations.py

# Run with coverage
python -m pytest --cov=kv_python
```

### Integration Tests

```bash
# Run integration tests
python -m pytest tests/integration/

# Run with specific log level
KV_LOG_LEVEL=debug python -m pytest
```

## 🛠️ Development

### Building from Source

```bash
# Clone repository
git clone https://github.com/entropy-tamer/kv.git
cd kv/kv-python

# Install in development mode
pip install -e .

# Build wheel
maturin build --release
```

### Running Examples

```bash
# Basic example
python examples/basic_usage.py

# Pub/Sub example
python examples/pubsub_example.py

# Performance benchmark
python examples/benchmark.py
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

## 🙏 Acknowledgments

- Built with [PyO3](https://pyo3.rs/) for Python-Rust integration
- Powered by the [kv-core](https://crates.io/crates/kv-core) Rust library
- Uses [Tokio](https://tokio.rs/) for async runtime

---

**Part of the [KV](https://github.com/entropy-tamer/kv) ecosystem**
