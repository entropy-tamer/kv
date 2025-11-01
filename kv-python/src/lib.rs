//! Python bindings for the KV service
//! 
//! Provides Python bindings for the Rust KV engine with basic functionality.

use pyo3::prelude::*;
use std::sync::Arc;

use kv_core::{
    KVEngine, KVConfig, PersistenceMode,
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
            expiration_check_interval,
            snapshot_interval: 300, // 5 minutes default
            aof_sync_interval: 1,   // 1 second default
            max_memory: Some(100 * 1024 * 1024), // 100MB default
            enable_compression: true,
        };

        let engine = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create runtime: {}", e)
            ))?
            .block_on(async {
                KVEngine::new(config)
                    .await
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Failed to create KV engine: {}", e)
                    ))
            })?;

        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Get a value by key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    /// * `key` - Key to retrieve
    /// 
    /// # Returns
    /// Value if found, None otherwise
    fn get(&self, database_id: u8, key: &str) -> PyResult<Option<String>> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            match engine.get(database_id, &key).await {
                Ok(Some(value)) => {
                    // Convert Value to string
                    let value_str = match value {
                        kv_core::Value::String(s) => s,
                        kv_core::Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
                        kv_core::Value::Json(j) => j.to_string(),
                    };
                    Ok(Some(value_str))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// Set a key-value pair
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    /// * `key` - Key to set
    /// * `value` - Value to set
    /// * `ttl` - Optional time-to-live in seconds
    fn set(&self, database_id: u8, key: &str, value: &str, ttl: Option<u64>) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        let value = value.to_string();
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            let kv_value = kv_core::Value::String(value);
            match engine.set(database_id, key, kv_value, ttl).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// Delete a key
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    /// * `key` - Key to delete
    /// 
    /// # Returns
    /// True if key was deleted, False if key didn't exist
    fn delete(&self, database_id: u8, key: &str) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            match engine.delete(database_id, &key).await {
                Ok(deleted) => Ok(deleted),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// Check if a key exists
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    /// * `key` - Key to check
    /// 
    /// # Returns
    /// True if key exists, False otherwise
    fn exists(&self, database_id: u8, key: &str) -> PyResult<bool> {
        let engine = Arc::clone(&self.engine);
        let key = key.to_string();
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            match engine.exists(database_id, &key).await {
                Ok(exists) => Ok(exists),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// List all keys in a database
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    /// 
    /// # Returns
    /// List of all keys in the database
    fn keys(&self, database_id: u8) -> PyResult<Vec<String>> {
        let engine = Arc::clone(&self.engine);
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            match engine.keys(database_id).await {
                Ok(keys) => Ok(keys.into_iter().map(|k| k.to_string()).collect()),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// Clear all keys in a database
    /// 
    /// # Arguments
    /// * `database_id` - Database ID (0-255)
    fn clear_database(&self, database_id: u8) -> PyResult<()> {
        let engine = Arc::clone(&self.engine);
        
        // Try to get current runtime, create new one if none exists
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                // Create a new runtime for this operation
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create Tokio runtime: {}", e)
                    )
                })?;
                rt.handle().clone()
            }
        };
        
        rt.block_on(async move {
            match engine.clear_database(database_id).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("KV error: {}", e)
                )),
            }
        })
    }

    /// Close the engine and cleanup resources
    fn close(&self) -> PyResult<()> {
        // For now, just return Ok since we don't have a shutdown method
        Ok(())
    }

}

/// Python module for KV
#[pymodule]
fn kv_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyKVEngine>()?;
    
    // Add version info
    m.add("__version__", "0.1.0")?;
    
    Ok(())
}