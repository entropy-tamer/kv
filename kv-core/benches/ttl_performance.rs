use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use kv_core::{
    KVEngine, KVConfig, PersistenceMode, Key, Value, DatabaseId, TTL
};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use std::time::Duration;

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

fn bench_ttl_set_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_set_operations");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::new("set_ttl", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                // Pre-populate with data
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    engine.set(database_id, key, value, None).await.unwrap();
                }
                
                // Set TTL for all keys
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let ttl = TTL::from_secs(3600); // 1 hour
                    engine.expire(database_id, &key, ttl).await.unwrap();
                }
                
                black_box(&engine);
            });
        });
    }
    
    group.finish();
}

fn bench_ttl_get_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_get_operations");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::new("get_ttl", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                // Pre-populate with data and TTL
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    let ttl = TTL::from_secs(3600); // 1 hour
                    engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                }
                
                // Get TTL for all keys
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let _ttl = engine.ttl(database_id, &key).await.unwrap();
                }
                
                black_box(&engine);
            });
        });
    }
    
    group.finish();
}

fn bench_ttl_cleanup_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_cleanup_performance");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::new("cleanup_expired", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                // Create keys with different TTLs
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    
                    // Some keys expire immediately, some in 1 hour
                    let ttl = if i % 2 == 0 {
                        TTL::from_secs(0) // Expired
                    } else {
                        TTL::from_secs(3600) // 1 hour
                    };
                    
                    engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                }
                
                // Wait a bit for cleanup to process
                tokio::time::sleep(Duration::from_millis(100)).await;
                
                // Check how many keys are still valid
                let stats = engine.get_stats().await.unwrap();
                black_box(stats);
            });
        });
    }
    
    group.finish();
}

fn bench_ttl_mixed_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_mixed_operations");
    
    for size in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("mixed_ttl_ops", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                // Mix of operations with TTL
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    
                    // Set with TTL
                    let ttl = TTL::from_secs(3600);
                    engine.set(database_id, key.clone(), value, Some(ttl)).await.unwrap();
                    
                    // Get TTL
                    let _ttl = engine.ttl(database_id, &key).await.unwrap();
                    
                    // Update TTL every 10th key
                    if i % 10 == 0 {
                        let new_ttl = TTL::from_secs(7200); // 2 hours
                        engine.expire(database_id, &key, new_ttl).await.unwrap();
                    }
                    
                    // Remove TTL every 20th key
                    if i % 20 == 0 {
                        // This would require a remove_ttl method, which we don't have yet
                        // For now, just set a very long TTL
                        let long_ttl = TTL::from_secs(86400 * 365); // 1 year
                        engine.expire(database_id, &key, long_ttl).await.unwrap();
                    }
                }
                
                black_box(&engine);
            });
        });
    }
    
    group.finish();
}

fn bench_ttl_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_concurrent_operations");
    
    for num_tasks in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("concurrent_ttl", num_tasks), num_tasks, |b, &num_tasks| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                let mut handles = vec![];
                
                for task_id in 0..*num_tasks {
                    let engine_clone = engine.clone();
                    let handle = tokio::spawn(async move {
                        for i in 0..100 {
                            let key = format!("task_{}_key_{}", task_id, i);
                            let value = Value::String(format!("task_{}_value_{}", task_id, i));
                            let ttl = TTL::from_secs(3600);
                            
                            // Set with TTL
                            engine_clone.set(database_id, key.clone(), value, Some(ttl)).await.unwrap();
                            
                            // Get TTL
                            let _ttl = engine_clone.ttl(database_id, &key).await.unwrap();
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
    }
    
    group.finish();
}

fn bench_ttl_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_memory_usage");
    
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::new("ttl_memory", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let engine = create_test_engine().await;
                let database_id = DatabaseId::default();
                
                // Create keys with TTL
                for i in 0..*size {
                    let key = format!("key_{}", i);
                    let value = Value::String(format!("value_{}", i));
                    let ttl = TTL::from_secs(3600);
                    engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                }
                
                // Get stats to measure memory usage
                let stats = engine.get_stats().await.unwrap();
                black_box(stats.memory_usage);
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_ttl_set_operations,
    bench_ttl_get_operations,
    bench_ttl_cleanup_performance,
    bench_ttl_mixed_operations,
    bench_ttl_concurrent_operations,
    bench_ttl_memory_usage
);
criterion_main!(benches);

