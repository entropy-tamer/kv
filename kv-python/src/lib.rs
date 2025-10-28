//! Python bindings for the KV service
//! 
//! Provides async Python bindings for the Rust KV engine with pub/sub support,
//! context manager functionality, and proper error handling.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
use pyo3_asyncio::tokio::future_into_py;
use std::sync::Arc;
use serde_json;

use kv_core::{
    KVEngine, KVConfig, Value, PersistenceMode,
    pubsub::ChannelPattern,
    PubSubMessage,
};

/// Python wrapper for the KV engine
#[pyclass]
pub struct PyKVEngine {
    engine: Arc<KVEngine>,
}

#[pymethods]
impl PyKVEngine {
    /// Create a new KV engine
    /// 
    /// # Arguments
    /// * `master_key` - Base64 encoded master encryption key (empty string for auto-generation)
    /// * `persistence_mode` - Persistence mode: "memory", "aof", "full", "hybrid"
    /// * `data_dir` - Data directory for persistence
    /// * `expiration_check_interval` - TTL check interval in seconds
    /// 
    /// # Returns
    /// New PyKVEngine instance
    /// 
    /// # Errors
    /// Raises ValueError if configuration is invalid
    #[new]
    #[pyo3(signature = (master_key="", persistence_mode="hybrid", data_dir="./data", expiration_check_interval=60))]
    fn new(
        master_key: &str,
        persistence_mode: &str,
        data_dir: &str,
        expiration_check_interval: u64,
    ) -> PyResult<Self> {
        let persistence = match persistence_mode {
            "memory" => PersistenceMode::Memory,
            "aof" => PersistenceMode::AOF,
            "full" => PersistenceMode::Full,
            "hybrid" => PersistenceMode::Hybrid,
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Invalid persistence mode: {}", persistence_mode)
            )),
        };

        let config = KVConfig {
            master_key: master_key.to_string(),
            persistence_mode: persistence,
            data_dir: data_dir.to_string(),
            snapshot_interval: 300,
            aof_sync_interval: 1,
            max_memory: None,
            enable_compression: true,
            expiration_check_interval,
        };

        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create tokio runtime: {}", e)
            ))?;

        let engine = rt.block_on(async {
            KVEngine::new(config).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to create KV engine: {}", e)
        ))?;
        
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Get a value by key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to retrieve
    /// 
    /// # Returns
    /// Value if found, None if not found or expired
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn get(&self, database_id: u8, key: &str) -> PyResult<Option<PyObject>> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.get(database_id, &key).await
        });

        match result {
            Ok(Some(value)) => {
                let py_value = Python::with_gil(|py| value_to_py_object(value, py))?;
                Ok(Some(py_value))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Get operation failed: {}", e)
            )),
        }
    }

    /// Set a value with optional TTL
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to set
    /// * `value` - Value to store (string, bytes, or dict)
    /// * `ttl` - Optional TTL in seconds
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn set(&self, database_id: u8, key: &str, value: PyObject, ttl: Option<u64>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        let value = py_object_to_value(value)?;
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.set(database_id, key, value, ttl).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Set operation failed: {}", e)
        ))
    }

    /// Delete a key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to delete
    /// 
    /// # Returns
    /// True if key was deleted, False if key didn't exist
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn delete(&self, database_id: u8, key: &str) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.delete(database_id, &key).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Delete operation failed: {}", e)
        ))
    }

    /// Check if a key exists
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to check
    /// 
    /// # Returns
    /// True if key exists, False otherwise
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn exists(&self, database_id: u8, key: &str) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.exists(database_id, &key).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Exists operation failed: {}", e)
        ))
    }

    /// Set TTL for an existing key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to set TTL for
    /// * `ttl` - TTL in seconds
    /// 
    /// # Returns
    /// True if TTL was set, False if key doesn't exist
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn expire(&self, database_id: u8, key: &str, ttl: u64) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.expire(database_id, &key, ttl).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Expire operation failed: {}", e)
        ))
    }

    /// Get remaining TTL for a key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `key` - Key to check TTL for
    /// 
    /// # Returns
    /// Remaining TTL in seconds, None if no TTL set
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn ttl(&self, database_id: u8, key: &str) -> PyResult<Option<u64>> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.ttl(database_id, &key).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("TTL operation failed: {}", e)
        ))
    }

    /// Get all keys in a database
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// 
    /// # Returns
    /// List of all keys
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn keys(&self, database_id: u8) -> PyResult<Vec<String>> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.keys(database_id).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Keys operation failed: {}", e)
        ))
    }

    /// Get keys matching a pattern
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// * `pattern` - Pattern to match (supports * wildcard)
    /// 
    /// # Returns
    /// List of matching keys
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn keys_pattern(&self, database_id: u8, pattern: &str) -> PyResult<Vec<String>> {
        let engine = Arc::clone(&self.engine);
        let pattern = pattern.to_string();
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.keys_pattern(database_id, &pattern).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Keys pattern operation failed: {}", e)
        ))
    }

    /// Clear all data in a database
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-15)
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn clear_database(&self, database_id: u8) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.clear_database(database_id).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Clear database operation failed: {}", e)
        ))
    }

    /// Publish a message to a channel
    /// 
    /// # Arguments
    /// * `channel` - Channel name
    /// * `message` - Message to publish (string, bytes, or dict)
    /// 
    /// # Returns
    /// Number of subscribers that received the message
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn publish(&self, channel: &str, message: PyObject) -> PyResult<usize> {
        let engine = Arc::clone(&self.engine);
        let channel = channel.to_string();
        let message = py_object_to_value(message)?;
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.publish(&channel, message).await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Publish operation failed: {}", e)
        ))
    }

    /// Subscribe to a channel pattern
    /// 
    /// # Arguments
    /// * `pattern` - Channel pattern (supports * wildcard)
    /// 
    /// # Returns
    /// Async generator that yields PubSubMessage objects
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn subscribe(&self, pattern: &str) -> PyResult<PyObject> {
        let engine = Arc::clone(&self.engine);
        let pattern = if pattern.contains('*') {
            ChannelPattern::wildcard(pattern.to_string())
        } else {
            ChannelPattern::exact(pattern.to_string())
        };
        
        let rt = tokio::runtime::Handle::current();
        let receiver = rt.block_on(async move {
            engine.subscribe(pattern).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Subscribe operation failed: {}", e)
        ))?;

        // Create async generator for receiving messages
        Python::with_gil(|py| {
            let future = future_into_py(py, async move {
                let mut receiver = receiver;
                let mut messages = Vec::new();
                
                while let Some(message) = receiver.recv().await {
                    messages.push(message);
                }
                
                let py_messages: Vec<PyPubSubMessage> = messages.into_iter().map(|msg| msg.into()).collect();
                Ok::<Vec<PyPubSubMessage>, PyErr>(py_messages)
            })?;
            
            Ok(future.into())
        })
    }

    /// Subscribe to cache invalidation events
    /// 
    /// # Returns
    /// Async generator that yields cache invalidation messages
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn subscribe_to_invalidations(&self) -> PyResult<PyObject> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let receiver = rt.block_on(async move {
            engine.subscribe_to_invalidations().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Subscribe to invalidations operation failed: {}", e)
        ))?;

        // Create async generator for receiving invalidation messages
        Python::with_gil(|py| {
            let future = future_into_py(py, async move {
                let mut receiver = receiver;
                let mut messages = Vec::new();
                
                while let Some(message) = receiver.recv().await {
                    messages.push(message);
                }
                
                let py_messages: Vec<PyPubSubMessage> = messages.into_iter().map(|msg| msg.into()).collect();
                Ok::<Vec<PyPubSubMessage>, PyErr>(py_messages)
            })?;
            
            Ok(future.into())
        })
    }

    /// Get engine statistics
    /// 
    /// # Returns
    /// Dictionary with engine statistics
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn get_stats(&self) -> PyResult<PyObject> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.get_stats().await
        });

        let stats = result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Get stats operation failed: {}", e)
        ))?;

        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("total_keys", stats.total_keys)?;
            dict.set_item("expired_keys", stats.expired_keys)?;
            dict.set_item("memory_usage", stats.memory_usage)?;
            dict.set_item("disk_usage", stats.disk_usage)?;
            dict.set_item("total_operations", stats.total_operations)?;
            dict.set_item("ops_per_second", stats.ops_per_second)?;
            dict.set_item("uptime", stats.uptime)?;
            dict.set_item("active_connections", stats.active_connections)?;
            Ok(dict.into())
        })
    }

    /// Flush all pending writes
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn flush(&self) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.flush().await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Flush operation failed: {}", e)
        ))
    }

    /// Close the engine and cleanup resources
    /// 
    /// # Errors
    /// Raises RuntimeError if operation fails
    fn close(&self) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        
        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(async move {
            engine.close().await
        });

        result.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Close operation failed: {}", e)
        ))
    }

    /// Context manager entry
    fn __enter__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    /// Context manager exit
    fn __exit__(&self, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> PyResult<()> {
        self.close()
    }
}

impl Clone for PyKVEngine {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
        }
    }
}

/// Python wrapper for PubSubMessage
#[pyclass]
pub struct PyPubSubMessage {
    #[pyo3(get)]
    pub channel: String,
    #[pyo3(get)]
    pub message: PyObject,
    #[pyo3(get)]
    pub timestamp: String,
}

impl From<PubSubMessage> for PyPubSubMessage {
    fn from(msg: PubSubMessage) -> Self {
        let message = Python::with_gil(|py| {
            value_to_py_object(msg.message, py).unwrap_or_else(|_| py.None().into())
        });
        
        Self {
            channel: msg.channel,
            message,
            timestamp: msg.timestamp.to_rfc3339(),
        }
    }
}

/// Convert Python object to KV Value
fn py_object_to_value(obj: PyObject) -> PyResult<Value> {
    Python::with_gil(|py| {
        if let Ok(string) = obj.extract::<String>(py) {
            Ok(Value::String(string))
        } else if let Ok(bytes) = obj.extract::<Vec<u8>>(py) {
            Ok(Value::Bytes(bytes))
        } else if let Ok(dict) = obj.downcast::<PyDict>(py) {
            // Convert dict to string representation
            let dict_string = format!("{:?}", dict);
            Ok(Value::String(dict_string))
        } else {
            // Try to convert to string as fallback
            let string = obj.to_string();
            Ok(Value::String(string))
        }
    })
}

/// Convert KV Value to Python object
fn value_to_py_object(value: Value, py: Python) -> PyResult<PyObject> {
    match value {
        Value::String(s) => Ok(PyString::new(py, &s).into()),
        Value::Bytes(b) => Ok(b.into_py(py)),
        Value::Json(j) => {
            // Convert JSON to string for now
            let json_string = serde_json::to_string(&j).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Failed to serialize JSON: {}", e)
                )
            })?;
            Ok(PyString::new(py, &json_string).into())
        }
    }
}

/// Python module for Reynard KV
#[pymodule]
fn reynard_kv(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyKVEngine>()?;
    m.add_class::<PyPubSubMessage>()?;
    
    // Add version info
    m.add("__version__", "0.1.0")?;
    
    Ok(())
}