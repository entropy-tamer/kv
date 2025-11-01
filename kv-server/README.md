# KV Server - Network Server for KV Service

A high-performance HTTP server that provides network access to the KV service. Built with Axum and designed for production deployment with comprehensive error handling and monitoring.

## 🚀 Features

- **🌐 HTTP API**: RESTful endpoints for all KV operations
- **⚡ High Performance**: Built with Axum for maximum throughput
- **🔐 Security**: TLS support and authentication ready
- **📊 Monitoring**: Built-in health checks and metrics
- **🔧 Configuration**: Environment-based configuration management
- **📝 Logging**: Comprehensive structured logging with tracing

## 📦 Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/entropy-tamer/kv.git
cd kv

# Build the server
cargo build --release --bin kv-server

# Run the server
./target/release/kv-server
```

### Docker

```bash
# Build Docker image
docker build -t kv-server .

# Run container
docker run -p 8080:8080 kv-server
```

## 🚀 Quick Start

### Basic Usage

```bash
# Start the server with default configuration
./kv-server

# Start with custom configuration
KV_MASTER_KEY="your-key" KV_DATA_DIR="./data" ./kv-server

# Start with custom port
KV_PORT=3000 ./kv-server
```

### Environment Variables

```bash
# Master encryption key (base64 encoded)
KV_MASTER_KEY="your-base64-encoded-key"

# Data directory for persistence
KV_DATA_DIR="./data/kv"

# Server port
KV_PORT=8080

# Persistence mode: memory, disk, hybrid
KV_PERSISTENCE_MODE="hybrid"

# Log level
RUST_LOG="kv=info"
```

## 🔧 Configuration

### Configuration File

Create a `config.toml` file:

```toml
[server]
port = 8080
host = "0.0.0.0"

[storage]
master_key = "your-base64-key"
data_dir = "./data/kv"
persistence_mode = "hybrid"
max_memory_size = "100MB"
compression = true

[logging]
level = "info"
format = "json"
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `server.port` | u16 | `8080` | Server port |
| `server.host` | String | `"0.0.0.0"` | Server host |
| `storage.master_key` | String | Generated | Base64-encoded encryption key |
| `storage.data_dir` | String | `"./data/kv"` | Directory for persistent storage |
| `storage.persistence_mode` | String | `"hybrid"` | Storage mode: `memory`, `disk`, `hybrid` |
| `storage.max_memory_size` | String | `"100MB"` | Maximum memory cache size |
| `storage.compression` | bool | `true` | Enable data compression |
| `logging.level` | String | `"info"` | Log level |
| `logging.format` | String | `"pretty"` | Log format: `pretty`, `json` |

## 📚 API Reference

### Health Check

```http
GET /health
```

Returns server health status.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00Z",
  "version": "0.1.1"
}
```

### Key-Value Operations

#### Set Key-Value Pair

```http
POST /api/v1/keys
Content-Type: application/json

{
  "db": 0,
  "key": "user:123",
  "value": "john_doe",
  "ttl": 3600
}
```

#### Get Value

```http
GET /api/v1/keys/{key}?db=0
```

#### Delete Key

```http
DELETE /api/v1/keys/{key}?db=0
```

#### Check Key Exists

```http
HEAD /api/v1/keys/{key}?db=0
```

#### List Keys

```http
GET /api/v1/keys?db=0&pattern=user:*
```

### Pub/Sub Operations

#### Publish Message

```http
POST /api/v1/pubsub/publish
Content-Type: application/json

{
  "channel": "notifications",
  "message": "Hello World!"
}
```

#### Subscribe to Channel

```http
GET /api/v1/pubsub/subscribe?pattern=notifications
```

### Database Management

#### Clear Database

```http
DELETE /api/v1/databases/{db}
```

#### Set Key Expiration

```http
POST /api/v1/keys/{key}/expire
Content-Type: application/json

{
  "db": 0,
  "ttl": 3600
}
```

## 🔐 Security

- **TLS Support**: HTTPS with configurable certificates
- **Authentication**: Ready for JWT or API key authentication
- **Input Validation**: Comprehensive request validation
- **Rate Limiting**: Built-in protection against abuse
- **CORS**: Configurable cross-origin resource sharing

## 📊 Monitoring

### Health Endpoints

- `GET /health` - Basic health check
- `GET /health/detailed` - Detailed system status
- `GET /metrics` - Prometheus-compatible metrics

### Metrics

- Request count and duration
- Error rates by endpoint
- Memory usage and cache hit rates
- Database operation statistics

## 🧪 Testing

```bash
# Run server tests
cargo test --bin kv-server

# Run integration tests
cargo test --test integration

# Run with specific log level
RUST_LOG=debug cargo test --bin kv-server
```

## 🚀 Deployment

### Production Checklist

- [ ] Set secure `KV_MASTER_KEY`
- [ ] Configure proper `KV_DATA_DIR`
- [ ] Set up TLS certificates
- [ ] Configure logging level
- [ ] Set up monitoring and alerting
- [ ] Configure backup strategy
- [ ] Set up load balancing (if needed)

### Docker Deployment

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin kv-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kv-server /usr/local/bin/
EXPOSE 8080
CMD ["kv-server"]
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [Axum](https://github.com/tokio-rs/axum) for high-performance HTTP
- Uses [Tower](https://github.com/tower-rs/tower) for middleware
- Logging powered by [Tracing](https://github.com/tokio-rs/tracing)
- Configuration managed by [Config](https://github.com/mehcode/config-rs)

## 📞 Support

- 📖 [Documentation](https://github.com/entropy-tamer/kv/wiki)
- 🐛 [Issue Tracker](https://github.com/entropy-tamer/kv/issues)
- 💬 [Discussions](https://github.com/entropy-tamer/kv/discussions)

---

## Made with ❤️ by the Reynard Team
