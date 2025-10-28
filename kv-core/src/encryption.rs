//! Encryption layer for the KV service
//! 
//! Provides AES-256-GCM encryption for data at rest with key derivation
//! using HKDF and secure key management.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce, Key,
};
use hkdf::Hkdf;
use sha2::Sha256;
use base64::{Engine as _, engine::general_purpose};
use getrandom;
use std::collections::HashMap;

use crate::{KVError, KVResult, DatabaseId};

/// Encryption key for a specific database
#[derive(Debug, Clone)]
pub struct DatabaseKey {
    pub database_id: DatabaseId,
    pub key: Key<Aes256Gcm>,
    pub nonce: [u8; 12], // GCM nonce size
}

impl DatabaseKey {
    /// Generate a new database key from master key
    /// 
    /// # Errors
    /// Returns error if master key is too short or key derivation fails
    pub fn derive(master_key: &[u8], database_id: DatabaseId) -> KVResult<Self> {
        if master_key.len() < 32 {
            return Err(KVError::Encryption("Master key must be at least 32 bytes".to_string()));
        }

        // Use HKDF to derive database-specific key
        let hk = Hkdf::<Sha256>::new(None, master_key);
        let mut derived_key = [0u8; 32];
        let info = format!("kv-database-{database_id}");
        hk.expand(info.as_bytes(), &mut derived_key)
            .map_err(|e| KVError::Encryption(format!("Key derivation failed: {e}")))?;

        // Generate random nonce for this database
        let mut nonce = [0u8; 12];
        getrandom::getrandom(&mut nonce).map_err(|e| KVError::Encryption(format!("Failed to generate random nonce: {e}")))?;

        let key = Key::<Aes256Gcm>::from_slice(&derived_key);
        
        Ok(Self {
            database_id,
            key: *key,
            nonce,
        })
    }

    /// Encrypt data using this database key
    /// 
    /// # Errors
    /// Returns error if encryption fails
    pub fn encrypt(&self, data: &[u8]) -> KVResult<Vec<u8>> {
        let cipher = Aes256Gcm::new(&self.key);
        let nonce = Nonce::from_slice(&self.nonce);
        
        cipher.encrypt(nonce, data)
            .map_err(|e| KVError::Encryption(format!("Encryption failed: {e}")))
    }

    /// Decrypt data using this database key
    /// 
    /// # Errors
    /// Returns error if decryption fails
    pub fn decrypt(&self, encrypted_data: &[u8]) -> KVResult<Vec<u8>> {
        let cipher = Aes256Gcm::new(&self.key);
        let nonce = Nonce::from_slice(&self.nonce);
        
        cipher.decrypt(nonce, encrypted_data)
            .map_err(|e| KVError::Encryption(format!("Decryption failed: {e}")))
    }
}

/// Key manager for handling encryption keys
pub struct KeyManager {
    master_key: Vec<u8>,
    database_keys: HashMap<DatabaseId, DatabaseKey>,
}

impl KeyManager {
    /// Create a new key manager with master key
    /// 
    /// # Errors
    /// Returns error if key generation or decoding fails
    pub fn new(master_key: &str) -> KVResult<Self> {
        let master_key = if master_key.is_empty() {
            // Generate a new master key
            let mut key = [0u8; 32];
            getrandom::getrandom(&mut key).map_err(|e| KVError::Encryption(format!("Failed to generate random key: {e}")))?;
            key.to_vec()
        } else {
            // Decode base64 master key
            general_purpose::STANDARD.decode(master_key)
                .map_err(|e| KVError::Encryption(format!("Invalid base64 master key: {e}")))?
        };

        Ok(Self {
            master_key,
            database_keys: HashMap::new(),
        })
    }

    /// Get or create encryption key for a database
    /// 
    /// # Errors
    /// Returns error if key derivation fails
    pub fn get_database_key(&mut self, database_id: DatabaseId) -> KVResult<&DatabaseKey> {
        if !self.database_keys.contains_key(&database_id) {
            let db_key = DatabaseKey::derive(&self.master_key, database_id)?;
            self.database_keys.insert(database_id, db_key);
        }
        
        self.database_keys.get(&database_id)
            .ok_or_else(|| KVError::Encryption("Failed to get database key".to_string()))
    }

    /// Get master key as base64 string (for configuration)
    #[must_use] 
    pub fn master_key_base64(&self) -> String {
        general_purpose::STANDARD.encode(&self.master_key)
    }

    /// Rotate master key (generates new key and invalidates all database keys)
    /// 
    /// # Errors
    /// Returns error if new key generation fails
    pub fn rotate_master_key(&mut self) -> KVResult<String> {
        // Generate new master key
        let mut new_master_key = [0u8; 32];
        getrandom::getrandom(&mut new_master_key).map_err(|e| KVError::Encryption(format!("Failed to generate random key: {e}")))?;
        
        // Update master key
        self.master_key = new_master_key.to_vec();
        
        // Clear all database keys (they will be regenerated on next access)
        self.database_keys.clear();
        
        Ok(self.master_key_base64())
    }

    /// Encrypt data for a specific database
    /// 
    /// # Errors
    /// Returns error if encryption fails
    pub fn encrypt(&mut self, database_id: DatabaseId, data: &[u8]) -> KVResult<Vec<u8>> {
        let db_key = self.get_database_key(database_id)?;
        db_key.encrypt(data)
    }

    /// Decrypt data for a specific database
    /// 
    /// # Errors
    /// Returns error if decryption fails
    pub fn decrypt(&mut self, database_id: DatabaseId, encrypted_data: &[u8]) -> KVResult<Vec<u8>> {
        let db_key = self.get_database_key(database_id)?;
        db_key.decrypt(encrypted_data)
    }
}

/// Encrypted storage wrapper
#[derive(Debug, Clone)]
pub struct EncryptedData {
    pub database_id: DatabaseId,
    pub encrypted_data: Vec<u8>,
    pub nonce: [u8; 12],
}

impl EncryptedData {
    /// Create encrypted data from raw data
    /// 
    /// # Errors
    /// Returns error if encryption fails
    pub fn encrypt(data: &[u8], database_id: DatabaseId, key_manager: &mut KeyManager) -> KVResult<Self> {
        let encrypted_data = key_manager.encrypt(database_id, data)?;
        
        Ok(Self {
            database_id,
            encrypted_data,
            nonce: [0u8; 12], // Will be set by the database key
        })
    }

    /// Decrypt this data
    /// 
    /// # Errors
    /// Returns error if decryption fails
    pub fn decrypt(&self, key_manager: &mut KeyManager) -> KVResult<Vec<u8>> {
        key_manager.decrypt(self.database_id, &self.encrypted_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_derivation() {
        // Create a 32-byte master key
        let master_key = b"0123456789abcdef0123456789abcdef"; // 32 bytes
        let db_key1 = DatabaseKey::derive(master_key, 0).unwrap();
        let db_key2 = DatabaseKey::derive(master_key, 1).unwrap();
        
        // Different databases should have different keys
        assert_ne!(db_key1.key, db_key2.key);
    }

    #[test]
    fn test_encryption_decryption() {
        // Use empty string to generate a random master key
        let mut key_manager = KeyManager::new("").unwrap();
        
        let data = b"Hello, encrypted world!";
        let encrypted = key_manager.encrypt(0, data).unwrap();
        let decrypted = key_manager.decrypt(0, &encrypted).unwrap();
        
        assert_eq!(data, &decrypted[..]);
    }

    #[test]
    fn test_key_rotation() {
        let mut key_manager = KeyManager::new("").unwrap();
        let old_master_key = key_manager.master_key_base64();
        
        let new_master_key = key_manager.rotate_master_key().unwrap();
        
        assert_ne!(old_master_key, new_master_key);
        assert!(key_manager.database_keys.is_empty());
    }

    #[test]
    fn test_encrypted_data_wrapper() {
        let mut key_manager = KeyManager::new("").unwrap();
        let data = b"Test data for encryption wrapper";
        
        let encrypted = EncryptedData::encrypt(data, 0, &mut key_manager).unwrap();
        let decrypted = encrypted.decrypt(&mut key_manager).unwrap();
        
        assert_eq!(data, &decrypted[..]);
    }
}
