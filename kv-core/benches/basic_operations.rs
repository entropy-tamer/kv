use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use kv_core::{
    KVEngine, KVConfig, PersistenceMode, Value, DatabaseId, TTL
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

fn bench_set_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("set_operations");
    
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("memory", size), size, |b, size| {
            b.iter(|| {
                rt.block_on(async {
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
        });
    }
    
    group.finish();
}

fn bench_get_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("get_operations");
    
    for size in [1, 10, 100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("memory", size), size, |b, size| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = create_test_engine().await;
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
        });
    }
    
    group.finish();
}

fn bench_mixed_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("mixed_operations");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("memory", size), size, |b, size| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = create_test_engine().await;
                    let database_id = DatabaseId::default();
                    
                    // Mix of set, get, delete operations
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let value = Value::String(format!("value_{}", i));
                        
                        // Set
                        engine.set(database_id, key.clone(), value, None).await.unwrap();
                        
                        // Get
                        let _result = engine.get(database_id, &key).await.unwrap();
                        
                        // Delete every 10th key
                        if i % 10 == 0 {
                            engine.delete(database_id, &key).await.unwrap();
                        }
                    }
                    
                    black_box(&engine);
                });
            });
        });
    }
    
    group.finish();
}

fn bench_ttl_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_operations");
    
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("memory", size), size, |b, size| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = create_test_engine().await;
                    let database_id = DatabaseId::default();
                    
                    // Set keys with TTL
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let value = Value::String(format!("value_{}", i));
                        let ttl = TTL(3600); // 1 hour
                        engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                    }
                    
                    // Check TTL for all keys
                    for i in 0..*size {
                        let key = format!("key_{}", i);
                        let _ttl = engine.ttl(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
    }
    
    group.finish();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_operations");
    
    for num_tasks in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("memory", num_tasks), num_tasks, |b, num_tasks| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = create_test_engine().await;
                    let database_id = DatabaseId::default();
                    
                    let mut handles = vec![];
                    
                    for task_id in 0..*num_tasks {
                        let engine_ref = &engine;
                        let handle = tokio::spawn(async move {
                            for i in 0..100 {
                                let key = format!("task_{}_key_{}", task_id, i);
                                let value = Value::String(format!("task_{}_value_{}", task_id, i));
                                engine_ref.set(database_id, key, value, None).await.unwrap();
                            }
                        });
                        handles.push(handle);
                    }
                    
                    // Wait for all tasks to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_set_operations,
    bench_get_operations,
    bench_mixed_operations,
    bench_ttl_operations,
    bench_concurrent_operations
);
criterion_main!(benches);
