use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use kv_core::{
    KVEngine, KVConfig, PersistenceMode, Key, Value, DatabaseId
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

async fn create_test_engine(mode: PersistenceMode) -> KVEngine {
    let temp_dir = TempDir::new().unwrap();
    let config = KVConfig {
        master_key: String::new(),
        persistence_mode: mode,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    KVEngine::new(config).await.unwrap()
}

fn bench_storage_backends_set(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("storage_backends_set");
    
    let backends = [
        ("memory", PersistenceMode::Memory),
        ("aof", PersistenceMode::AOF),
        ("full", PersistenceMode::Full),
    ];
    
    for (name, mode) in backends.iter() {
        for size in [100, 1000, 10000].iter() {
            group.bench_with_input(BenchmarkId::new(name, size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let engine = create_test_engine(*mode).await;
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
    }
    
    group.finish();
}

fn bench_storage_backends_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("storage_backends_get");
    
    let backends = [
        ("memory", PersistenceMode::Memory),
        ("aof", PersistenceMode::AOF),
        ("full", PersistenceMode::Full),
    ];
    
    for (name, mode) in backends.iter() {
        for size in [100, 1000, 10000].iter() {
            group.bench_with_input(BenchmarkId::new(name, size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let engine = create_test_engine(*mode).await;
                    let database_id = DatabaseId::default();
                    
                    // Pre-populate with data
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let value = Value::String(format!("value_{}", i));
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Benchmark get operations
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let _result = engine.get(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        }
    }
    
    group.finish();
}

fn bench_storage_backends_persistence(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("storage_backends_persistence");
    
    let backends = [
        ("aof", PersistenceMode::AOF),
        ("full", PersistenceMode::Full),
    ];
    
    for (name, mode) in backends.iter() {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine(*mode).await;
                let database_id = DatabaseId::default();
                
                // Write data
                for i in 0..1000 {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    engine.set(database_id, key, value, None).await.unwrap();
                }
                
                // Flush to disk
                engine.flush().await.unwrap();
                
                black_box(&engine);
            });
        });
    }
    
    group.finish();
}

fn bench_storage_backends_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("storage_backends_memory_usage");
    
    let backends = [
        ("memory", PersistenceMode::Memory),
        ("aof", PersistenceMode::AOF),
        ("full", PersistenceMode::Full),
    ];
    
    for (name, mode) in backends.iter() {
        for size in [1000, 10000, 100000].iter() {
            group.bench_with_input(BenchmarkId::new(name, size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let engine = create_test_engine(*mode).await;
                    let database_id = DatabaseId::default();
                    
                    // Fill with data
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let value = Value::String(format!("value_{}", i));
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Get stats to measure memory usage
                    let stats = engine.get_stats().await.unwrap();
                    black_box(stats.memory_usage);
                });
            });
        }
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_storage_backends_set,
    bench_storage_backends_get,
    bench_storage_backends_persistence,
    bench_storage_backends_memory_usage
);
criterion_main!(benches);

