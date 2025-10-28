# kv-client

Client library for KV service - a secure, encrypted key-value store to replace Redis.

## Features

- **Easy Integration**: Simple client API for connecting to KV servers
- **Async Support**: Full async/await support with Tokio
- **Connection Pooling**: Efficient connection management
- **Error Handling**: Comprehensive error types and handling

## Usage

```rust
use kv_client::{KVClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new("localhost:8080");
    let mut client = KVClient::connect(config).await?;
    
    // Set a value
    client.set(0, "key1", "value1", None).await?;
    
    // Get a value
    let value = client.get(0, "key1").await?;
    println!("Value: {:?}", value);
    
    Ok(())
}
```

## License

MIT
