use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use kv_core::{
    KVEngine, KVConfig, PersistenceMode, Key, Value, DatabaseId,
    encryption::{KeyManager, DatabaseKey, EncryptedData}
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

async fn create_test_engine() -> KVEngine {
    let temp_dir = TempDir::new().unwrap();
    let config = KVConfig {
        master_key: String::new(),
        persistence_mode: PersistenceMode::Memory,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    KVEngine::new(config).await.unwrap()
}

fn bench_key_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_derivation");
    
    for size in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("derive_keys", size), size, |b, &size| {
            b.iter(|| {
                let master_key = b"0123456789abcdef0123456789abcdef";
                let mut keys = Vec::new();
                
                for i in 0..*size {
                    let database_id = DatabaseId::from(i);
                    let key = DatabaseKey::derive(master_key, database_id).unwrap();
                    keys.push(key);
                }
                
                black_box(keys);
            });
        });
    }
    
    group.finish();
}

fn bench_encryption_decryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("encryption_decryption");
    
    let master_key = b"0123456789abcdef0123456789abcdef";
    let database_id = DatabaseId::default();
    let db_key = DatabaseKey::derive(master_key, database_id).unwrap();
    
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("encrypt", size), size, |b, &size| {
            b.iter(|| {
                let data = vec![0u8; *size];
                let encrypted = db_key.encrypt(&data).unwrap();
                black_box(encrypted);
            });
        });
        
        group.bench_with_input(BenchmarkId::new("decrypt", size), size, |b, &size| {
            let data = vec![0u8; *size];
            let encrypted = db_key.encrypt(&data).unwrap();
            
            b.iter(|| {
                let decrypted = db_key.decrypt(&encrypted).unwrap();
                black_box(decrypted);
            });
        });
    }
    
    group.finish();
}

fn bench_key_manager_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_manager_operations");
    
    for size in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("get_database_key", size), size, |b, &size| {
            b.iter(|| {
                let mut key_manager = KeyManager::new("").unwrap();
                let mut keys = Vec::new();
                
                for i in 0..*size {
                    let database_id = DatabaseId::from(i);
                    let key = key_manager.get_database_key(database_id).unwrap();
                    keys.push(key);
                }
                
                black_box(keys);
            });
        });
        
        group.bench_with_input(BenchmarkId::new("encrypt_data", size), size, |b, &size| {
            b.iter(|| {
                let mut key_manager = KeyManager::new("").unwrap();
                let database_id = DatabaseId::default();
                let data = vec![0u8; 100];
                
                for _i in 0..*size {
                    let encrypted = key_manager.encrypt(database_id, &data).unwrap();
                    black_box(encrypted);
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("decrypt_data", size), size, |b, &size| {
            let mut key_manager = KeyManager::new("").unwrap();
            let database_id = DatabaseId::default();
            let data = vec![0u8; 100];
            let encrypted = key_manager.encrypt(database_id, &data).unwrap();
            
            b.iter(|| {
                for _i in 0..*size {
                    let decrypted = key_manager.decrypt(database_id, &encrypted).unwrap();
                    black_box(decrypted);
                }
            });
        });
    }
    
    group.finish();
}

fn bench_encrypted_data_wrapper(c: &mut Criterion) {
    let mut group = c.benchmark_group("encrypted_data_wrapper");
    
    for size in [1, 10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("encrypt_wrapper", size), size, |b, &size| {
            b.iter(|| {
                let mut key_manager = KeyManager::new("").unwrap();
                let database_id = DatabaseId::default();
                let data = vec![0u8; 100];
                
                for _i in 0..*size {
                    let encrypted = EncryptedData::encrypt(&data, database_id, &mut key_manager).unwrap();
                    black_box(encrypted);
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("decrypt_wrapper", size), size, |b, &size| {
            let mut key_manager = KeyManager::new("").unwrap();
            let database_id = DatabaseId::default();
            let data = vec![0u8; 100];
            let encrypted = EncryptedData::encrypt(&data, database_id, &mut key_manager).unwrap();
            
            b.iter(|| {
                for _i in 0..*size {
                    let decrypted = encrypted.decrypt(&mut key_manager).unwrap();
                    black_box(decrypted);
                }
            });
        });
    }
    
    group.finish();
}

fn bench_key_rotation(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_rotation");
    
    group.bench_function("rotate_master_key", |b| {
        b.iter(|| {
            let mut key_manager = KeyManager::new("").unwrap();
            let new_key = key_manager.rotate_master_key().unwrap();
            black_box(new_key);
        });
    });
    
    group.bench_function("rotate_with_existing_keys", |b| {
        b.iter(|| {
            let mut key_manager = KeyManager::new("").unwrap();
            
            // Create some database keys
            for i in 0..100 {
                let database_id = DatabaseId::from(i);
                key_manager.get_database_key(database_id).unwrap();
            }
            
            // Rotate the master key
            let new_key = key_manager.rotate_master_key().unwrap();
            black_box(new_key);
        });
    });
    
    group.finish();
}

fn bench_encryption_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("encryption_overhead");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("with_encryption", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    engine.set(database_id, key, value, None).await.unwrap();
                }
                
                black_box(&engine);
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_key_derivation,
    bench_encryption_decryption,
    bench_key_manager_operations,
    bench_encrypted_data_wrapper,
    bench_key_rotation,
    bench_encryption_overhead
);
criterion_main!(benches);

