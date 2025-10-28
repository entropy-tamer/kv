# kv-core

Core storage engine for KV service - a secure, encrypted key-value store to replace Redis.

## Features

- **Encrypted Storage**: AES-GCM encryption for all data at rest
- **High Performance**: Built with Rust for maximum speed and memory safety
- **Persistent Storage**: Sled-based persistent storage with configurable modes
- **Async/Await**: Full async support with Tokio runtime
- **Thread Safe**: Concurrent access with DashMap for in-memory operations

## Usage

```rust
use kv_core::{KVEngine, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let mut engine = KVEngine::new(config).await?;
    
    // Set a value
    engine.set(0, "key1", "value1", None).await?;
    
    // Get a value
    let value = engine.get(0, "key1").await?;
    println!("Value: {:?}", value);
    
    Ok(())
}
```

## License

MIT
