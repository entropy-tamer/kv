# Reynard KV Python Bindings

Python bindings for the Reynard KV service, providing high-performance, encrypted key-value storage with pub/sub capabilities.

## Features

- **High Performance**: Rust-based engine with async Python bindings
- **Encryption**: Built-in encryption for all stored data
- **TTL Support**: Automatic expiration of keys with configurable TTL
- **Pub/Sub**: Real-time message broadcasting and subscription
- **Multiple Data Types**: Support for strings, bytes, and JSON objects
- **Context Manager**: Python context manager support for resource cleanup
- **Async Support**: Full async/await support using pyo3-asyncio

## Installation

```bash
# Development install
maturin develop

# Production install
maturin build --release
pip install target/wheels/reynard_kv-*.whl
```

## Usage

### Basic Operations

```python
import asyncio
from reynard_kv import PyKVEngine

async def main():
    # Create engine with encryption
    engine = PyKVEngine(
        master_key="your-base64-key",
        persistence_mode="hybrid",
        data_dir="./data",
        expiration_check_interval=60
    )

    # Set a value with TTL
    engine.set(0, "user:123", "John Doe", ttl=300)  # 5 minutes

    # Get a value
    value = engine.get(0, "user:123")
    print(f"User: {value}")

    # Check if key exists
    exists = engine.exists(0, "user:123")
    print(f"Key exists: {exists}")

    # Delete a key
    deleted = engine.delete(0, "user:123")
    print(f"Deleted: {deleted}")

    # Close engine
    engine.close()

asyncio.run(main())
```

### Context Manager

```python
from reynard_kv import PyKVEngine

with PyKVEngine() as engine:
    engine.set(0, "key", "value")
    value = engine.get(0, "key")
    print(value)
# Engine automatically closed
```

### Pub/Sub

```python
import asyncio
from reynard_kv import PyKVEngine

async def main():
    engine = PyKVEngine()

    # Subscribe to cache invalidation events
    async for message in engine.subscribe_to_invalidations():
        print(f"Cache invalidated: {message.channel}")

    # Publish a message
    subscribers = engine.publish("cache:invalidate:user123", "invalidate")
    print(f"Message sent to {subscribers} subscribers")

asyncio.run(main())
```

### Advanced Features

```python
# Set TTL for existing key
engine.expire(0, "key", 60)  # 60 seconds

# Get remaining TTL
ttl = engine.ttl(0, "key")
print(f"TTL: {ttl} seconds")

# Get all keys
keys = engine.keys(0)
print(f"All keys: {keys}")

# Get keys matching pattern
user_keys = engine.keys_pattern(0, "user:*")
print(f"User keys: {user_keys}")

# Clear entire database
engine.clear_database(0)

# Get engine statistics
stats = engine.get_stats()
print(f"Stats: {stats}")

# Flush pending writes
engine.flush()
```

## Configuration

### Persistence Modes

- `memory`: In-memory only (no persistence)
- `aof`: Append-only file (durable writes)
- `full`: Full persistence with snapshots
- `hybrid`: Combination of AOF and snapshots (recommended)

### Master Key

The master key is used for encryption. If empty, a key will be auto-generated.

**Security Note**: Store the master key securely and never commit it to version control.

## Error Handling

All operations can raise `RuntimeError` for various failure conditions:

```python
try:
    engine.set(0, "key", "value")
except RuntimeError as e:
    print(f"Operation failed: {e}")
```

## Performance

The Rust-based engine provides:

- **Sub-millisecond** read/write operations
- **Memory efficient** storage with compression
- **Concurrent** operations with thread safety
- **Low latency** pub/sub messaging

## License

MIT License - see LICENSE file for details.
