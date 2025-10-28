# kv-client

Client library for the KV service - a high-performance client for connecting to KV servers with built-in connection pooling and error handling.

## 🚀 Features

- **🔌 Connection Pooling**: Efficient connection management with automatic reconnection
- **⚡ Async/Await**: Full async support with Tokio runtime
- **🔄 Auto-Retry**: Automatic retry logic with exponential backoff
- **📊 Health Monitoring**: Built-in health checks and connection monitoring
- **🔐 Secure**: TLS support for encrypted connections
- **📈 Metrics**: Client-side performance metrics and monitoring

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kv-client = "0.1.0"
```

## 🚀 Quick Start

### Basic Usage

```rust
use kv_client::{KVClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client configuration
    let config = ClientConfig {
        server_url: "http://localhost:8080".to_string(),
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(10),
        retry_attempts: 3,
        tls_config: None, // Optional TLS configuration
    };
    
    // Connect to server
    let mut client = KVClient::connect(config).await?;
    
    // Basic operations
    client.set(0, "user:123", "john_doe", None).await?;
    let value = client.get(0, "user:123").await?;
    println!("User: {:?}", value);
    
    // Pub/Sub operations
    client.publish("notifications", "Hello World!").await?;
    
    // Cleanup
    client.disconnect().await?;
    Ok(())
}
```

### Connection Pooling

```rust
use kv_client::{KVClient, ClientConfig, ConnectionPool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::default();
    let pool = ConnectionPool::new(config, 5).await?; // 5 connections
    
    // Get client from pool
    let mut client = pool.get_client().await?;
    
    // Use client
    client.set(0, "key", "value", None).await?;
    
    // Client is automatically returned to pool when dropped
    Ok(())
}
```

## 🔧 Configuration

### ClientConfig

```rust
pub struct ClientConfig {
    pub server_url: String,           // Server URL
    pub max_connections: usize,       // Max connections in pool
    pub connection_timeout: Duration, // Connection timeout
    pub request_timeout: Duration,    // Request timeout
    pub retry_attempts: usize,        // Retry attempts
    pub tls_config: Option<TlsConfig>, // Optional TLS config
    pub keep_alive: bool,             // Keep connections alive
    pub compression: bool,            // Enable compression
}
```

### TLS Configuration

```rust
use kv_client::{ClientConfig, TlsConfig};

let config = ClientConfig {
    server_url: "https://kv.example.com:8443".to_string(),
    tls_config: Some(TlsConfig {
        ca_cert_path: Some("./certs/ca.pem".to_string()),
        client_cert_path: Some("./certs/client.pem".to_string()),
        client_key_path: Some("./certs/client.key".to_string()),
        verify_hostname: true,
    }),
    ..Default::default()
};
```

## 📊 Performance

### Connection Pooling

- **Automatic Management**: Connections are created and destroyed as needed
- **Health Monitoring**: Unhealthy connections are automatically replaced
- **Load Balancing**: Requests are distributed across available connections
- **Backpressure**: Automatic backpressure when pool is exhausted

### Retry Logic

```rust
// Custom retry configuration
let config = ClientConfig {
    retry_attempts: 5,
    retry_delay: Duration::from_millis(100),
    retry_multiplier: 2.0,
    max_retry_delay: Duration::from_secs(30),
    ..Default::default()
};
```

## 🧪 Testing

```bash
# Run unit tests
cargo test

# Run integration tests (requires running server)
cargo test --test integration

# Run with specific log level
RUST_LOG=kv_client=debug cargo test
```

## 📚 API Reference

### KVClient

Main client for KV operations.

```rust
impl KVClient {
    pub async fn connect(config: ClientConfig) -> KVResult<Self>
    pub async fn set(&mut self, db: u8, key: &str, value: &str, ttl: Option<u64>) -> KVResult<()>
    pub async fn get(&self, db: u8, key: &str) -> KVResult<Option<String>>
    pub async fn delete(&mut self, db: u8, key: &str) -> KVResult<bool>
    pub async fn exists(&self, db: u8, key: &str) -> KVResult<bool>
    pub async fn keys(&self, db: u8, pattern: Option<&str>) -> KVResult<Vec<String>>
    pub async fn clear_database(&mut self, db: u8) -> KVResult<()>
    pub async fn expire(&mut self, db: u8, key: &str, ttl: u64) -> KVResult<bool>
    pub async fn ttl(&self, db: u8, key: &str) -> KVResult<Option<u64>>
    pub async fn publish(&self, channel: &str, message: &str) -> KVResult<usize>
    pub async fn subscribe(&self, pattern: &str) -> KVResult<Subscription>
    pub async fn health_check(&self) -> KVResult<HealthStatus>
    pub async fn disconnect(&mut self) -> KVResult<()>
}
```

### ConnectionPool

Connection pool for managing multiple clients.

```rust
impl ConnectionPool {
    pub async fn new(config: ClientConfig, pool_size: usize) -> KVResult<Self>
    pub async fn get_client(&self) -> KVResult<PooledClient>
    pub async fn health_check(&self) -> KVResult<PoolHealth>
    pub fn stats(&self) -> PoolStats
}
```

### Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Request timeout: {0}")]
    Timeout(String),
    #[error("Server error: {0}")]
    Server(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("Pool exhausted")]
    PoolExhausted,
}
```

## 🔍 Monitoring

### Health Checks

```rust
// Check client health
let health = client.health_check().await?;
println!("Status: {:?}", health.status);
println!("Latency: {}ms", health.latency_ms);
println!("Last error: {:?}", health.last_error);
```

### Metrics

```rust
// Get client metrics
let metrics = client.get_metrics().await?;
println!("Requests: {}", metrics.total_requests);
println!("Errors: {}", metrics.total_errors);
println!("Avg latency: {}ms", metrics.avg_latency_ms);
```

### Pool Statistics

```rust
// Get pool statistics
let stats = pool.stats();
println!("Active connections: {}", stats.active_connections);
println!("Idle connections: {}", stats.idle_connections);
println!("Total requests: {}", stats.total_requests);
```

## 🛠️ Development

### Building from Source

```bash
git clone https://github.com/entropy-tamer/kv.git
cd kv/kv-client
cargo build --release
```

### Running Tests

```bash
# All tests
cargo test

# Integration tests (requires server)
cargo test --test integration

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
