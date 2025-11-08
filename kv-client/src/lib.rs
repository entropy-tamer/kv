//! KV Client - Client library for the KV service

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::net::UnixStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Client configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Unix socket path (default: /var/run/reynard/kv.sock)
    pub socket_path: Option<String>,
    /// HTTP URL (e.g., http://localhost:8004)
    pub http_url: Option<String>,
    /// Database ID (0-15)
    pub database: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: Some("/var/run/reynard/kv.sock".to_string()),
            http_url: None,
            database: 0,
        }
    }
}

impl Config {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let socket_path = std::env::var("KV_SOCKET_PATH")
            .ok()
            .filter(|s| !s.is_empty());
        let http_url = std::env::var("KV_HTTP_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let database = std::env::var("KV_DATABASE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        
        Self {
            socket_path: socket_path.or_else(|| Some("/var/run/reynard/kv.sock".to_string())),
            http_url,
            database,
        }
    }
    
    /// Use Unix socket
    pub fn with_socket<P: AsRef<Path>>(path: P) -> Self {
        Self {
            socket_path: Some(path.as_ref().to_string_lossy().to_string()),
            http_url: None,
            database: 0,
        }
    }
    
    /// Use HTTP
    pub fn with_http(url: impl Into<String>) -> Self {
        Self {
            socket_path: None,
            http_url: Some(url.into()),
            database: 0,
        }
    }
    
    /// Set database ID
    pub fn with_database(mut self, database: u8) -> Self {
        self.database = database;
        self
    }
}

/// KV Client
pub struct Client {
    config: Config,
}

#[derive(Serialize)]
struct SetRequest {
    database_id: u8,
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct GetResponse {
    database_id: u8,
    key: String,
    value: Option<String>,
    found: bool,
}

#[derive(Deserialize)]
struct HealthResponse {
    status: String,
    service: String,
}

impl Client {
    /// Create a new client with default configuration
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }
    
    /// Create a new client with custom configuration
    pub fn with_config(config: Config) -> Self {
        Self { config }
    }
    
    /// Create a client from environment variables
    pub fn from_env() -> Self {
        Self {
            config: Config::from_env(),
        }
    }
    
    /// Send a request via Unix socket
    async fn request_unix(&self, method: &str, path: &str, body: Option<&[u8]>) -> Result<Vec<u8>> {
        let socket_path = self.config.socket_path.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Unix socket path not configured"))?;
        
        let mut stream = UnixStream::connect(socket_path).await
            .with_context(|| format!("Failed to connect to Unix socket: {}", socket_path))?;
        
        // Build HTTP request
        let mut request = format!("{} {} HTTP/1.1\r\n", method, path);
        request.push_str("Host: localhost\r\n");
        request.push_str("Content-Type: application/json\r\n");
        
        if let Some(body) = body {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");
        
        // Send request
        stream.write_all(request.as_bytes()).await?;
        if let Some(body) = body {
            stream.write_all(body).await?;
        }
        stream.flush().await?;
        
        // Read response
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        
        // Parse HTTP response (simple parser)
        let response_str = String::from_utf8_lossy(&response);
        let body_start = response_str.find("\r\n\r\n")
            .map(|i| i + 4)
            .unwrap_or(0);
        
        Ok(response[body_start..].to_vec())
    }
    
    /// Send a request via HTTP
    async fn request_http(&self, method: &str, path: &str, body: Option<&[u8]>) -> Result<Vec<u8>> {
        let url = self.config.http_url.as_ref()
            .ok_or_else(|| anyhow::anyhow!("HTTP URL not configured"))?;
        
        let client = reqwest::Client::new();
        let url = format!("{}{}", url.trim_end_matches('/'), path);
        
        let response = match method {
            "GET" => client.get(&url).send().await?,
            "POST" => {
                let mut req = client.post(&url);
                if let Some(body) = body {
                    req = req.body(body.to_vec());
                }
                req.send().await?
            }
            _ => return Err(anyhow::anyhow!("Unsupported method: {}", method)),
        };
        
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }
    
    /// Send a request (auto-detects socket vs HTTP)
    async fn request(&self, method: &str, path: &str, body: Option<&[u8]>) -> Result<Vec<u8>> {
        if self.config.socket_path.is_some() {
            self.request_unix(method, path, body).await
        } else if self.config.http_url.is_some() {
            self.request_http(method, path, body).await
        } else {
            Err(anyhow::anyhow!("No connection method configured"))
        }
    }
    
    /// Check server health
    pub async fn health(&self) -> Result<HealthResponse> {
        let response = self.request("GET", "/health", None).await?;
        let health: HealthResponse = serde_json::from_slice(&response)?;
        Ok(health)
    }
    
    /// Set a key-value pair
    pub async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let request = SetRequest {
            database_id: self.config.database,
            key: key.into(),
            value: value.into(),
        };
        let body = serde_json::to_vec(&request)?;
        self.request("POST", "/set", Some(&body)).await?;
        Ok(())
    }
    
    /// Get a value by key
    pub async fn get(&self, key: impl Into<String>) -> Result<Option<String>> {
        let key = key.into();
        let path = format!("/get/{}/{}", self.config.database, urlencoding::encode(&key));
        let response = self.request("GET", &path, None).await?;
        let get_response: GetResponse = serde_json::from_slice(&response)?;
        Ok(get_response.value)
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}
