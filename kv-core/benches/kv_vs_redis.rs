use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::hint::black_box;
use kv_core::{
    KVEngine, KVConfig, PersistenceMode, Value, DatabaseId, TTL
};
use redis::{AsyncCommands, aio::MultiplexedConnection};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use std::sync::Arc;

// Redis connection configuration
const REDIS_URL: &str = "redis://127.0.0.1:6379";
const REDIS_DB: i64 = 15; // Use DB 15 for testing

// Test data sizes
const SIZES: &[usize] = &[1, 10, 100, 1000, 10000];
const CONCURRENT_TASKS: &[usize] = &[2, 4, 8, 16, 32];

// Value sizes
const SMALL_VALUE_SIZE: usize = 100;   // 100 bytes
const MEDIUM_VALUE_SIZE: usize = 1024; // 1 KB
const LARGE_VALUE_SIZE: usize = 10240; // 10 KB

/// Setup KV engine for benchmarking
async fn setup_kv() -> KVEngine {
    let temp_dir = TempDir::new().unwrap();
    let config = KVConfig {
        master_key: String::new(),
        persistence_mode: PersistenceMode::Memory,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    KVEngine::new(config).await.unwrap()
}

/// Setup Redis connection for benchmarking
async fn setup_redis() -> MultiplexedConnection {
    let client = redis::Client::open(REDIS_URL).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    
    // Select test database
    redis::cmd("SELECT").arg(REDIS_DB).query_async::<()>(&mut conn).await.unwrap();
    
    // Flush database before each benchmark
    redis::cmd("FLUSHDB").query_async::<()>(&mut conn).await.unwrap();
    
    conn
}

/// Generate test data of specified size
fn generate_test_data(size: usize, value_size: usize) -> Vec<(String, String)> {
    (0..size)
        .map(|i| {
            let key = format!("key_{}", i);
            let value = "x".repeat(value_size);
            (key, value)
        })
        .collect()
}

/// Generate test data for KV engine
fn generate_kv_test_data(size: usize, value_size: usize) -> Vec<(String, Value)> {
    (0..size)
        .map(|i| {
            let key = format!("key_{}", i);
            let value = Value::String("x".repeat(value_size));
            (key, value)
        })
        .collect()
}

// ============================================================================
// BASIC OPERATIONS BENCHMARKS
// ============================================================================

fn bench_basic_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("basic_operations");
    
    for size in SIZES {
        // SET operations
        group.bench_function(BenchmarkId::new("kv_set", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(*size, SMALL_VALUE_SIZE);
                    
                    for (key, value) in test_data {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_set", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(*size, SMALL_VALUE_SIZE);
                    
                    for (key, value) in test_data {
                        let _: () = conn.set(&key, &value).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // GET operations (with pre-populated data)
        group.bench_function(BenchmarkId::new("kv_get", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Benchmark GET operations
                    for (key, _) in test_data {
                        let _result = engine.get(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_get", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    // Benchmark GET operations
                    for (key, _) in test_data {
                        let _result: Option<String> = conn.get(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // DELETE operations (with pre-populated data)
        group.bench_function(BenchmarkId::new("kv_delete", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Benchmark DELETE operations
                    for (key, _) in test_data {
                        let _result = engine.delete(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_delete", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    // Benchmark DELETE operations
                    for (key, _) in test_data {
                        let _result: i32 = conn.del(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // EXISTS operations (with pre-populated data)
        group.bench_function(BenchmarkId::new("kv_exists", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Benchmark EXISTS operations
                    for (key, _) in test_data {
                        let _result = engine.exists(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_exists", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(*size, SMALL_VALUE_SIZE);
                    
                    // Pre-populate
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    // Benchmark EXISTS operations
                    for (key, _) in test_data {
                        let _result: bool = conn.exists(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// TTL OPERATIONS BENCHMARKS
// ============================================================================

fn bench_ttl_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("ttl_operations");
    
    for size in [100, 1000, 10000] {
        // SETEX operations (set with TTL)
        group.bench_function(BenchmarkId::new("kv_setex", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    let ttl: TTL = 3600; // 1 hour
                    
                    for (key, value) in test_data {
                        engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_setex", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    let ttl = 3600; // 1 hour
                    
                    for (key, value) in test_data {
                        let _: () = conn.set_ex(&key, &value, ttl).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // EXPIRE operations (set TTL on existing keys)
        group.bench_function(BenchmarkId::new("kv_expire", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    let ttl: TTL = 3600; // 1 hour
                    
                    // Pre-populate without TTL
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Benchmark EXPIRE operations
                    for (key, _) in test_data {
                        let _result = engine.expire(database_id, &key, ttl).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_expire", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    let ttl = 3600; // 1 hour
                    
                    // Pre-populate without TTL
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    // Benchmark EXPIRE operations
                    for (key, _) in test_data {
                        let _result: bool = conn.expire(&key, ttl).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // TTL operations (check remaining TTL)
        group.bench_function(BenchmarkId::new("kv_ttl", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    let ttl: TTL = 3600; // 1 hour
                    
                    // Pre-populate with TTL
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, Some(ttl)).await.unwrap();
                    }
                    
                    // Benchmark TTL operations
                    for (key, _) in test_data {
                        let _result = engine.ttl(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_ttl", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    let ttl = 3600; // 1 hour
                    
                    // Pre-populate with TTL
                    for (key, value) in &test_data {
                        let _: () = conn.set_ex(key, value, ttl).await.unwrap();
                    }
                    
                    // Benchmark TTL operations
                    for (key, _) in test_data {
                        let _result: i64 = conn.ttl(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// DATA STRUCTURE BENCHMARKS
// ============================================================================

fn bench_data_structures(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("data_structures");
    
    for size in [100, 1000, 10000] {
        // List operations - LPUSH
        group.bench_function(BenchmarkId::new("kv_list_lpush", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let list_key = "test_list".to_string();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    
                    // Note: KV service doesn't have direct list operations yet,
                    // so we'll simulate with string operations for now
                    for (i, (_, value)) in test_data.iter().enumerate() {
                        let key = format!("{}:{}", list_key, i);
                        engine.set(database_id, key, value.clone(), None).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_list_lpush", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let list_key = "test_list";
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    
                    for (_, value) in test_data {
                        let _: usize = conn.lpush(list_key, &value).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // Set operations - SADD
        group.bench_function(BenchmarkId::new("kv_set_sadd", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    
                    // Note: KV service doesn't have direct set operations yet,
                    // so we'll simulate with string operations for now
                    for (key, value) in test_data {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_set_sadd", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let set_key = "test_set";
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    
                    for (_, value) in test_data {
                        let _: usize = conn.sadd(set_key, &value).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // Hash operations - HSET
        group.bench_function(BenchmarkId::new("kv_hash_hset", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(size, SMALL_VALUE_SIZE);
                    
                    // Note: KV service doesn't have direct hash operations yet,
                    // so we'll simulate with string operations for now
                    for (key, value) in test_data {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_hash_hset", size), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let hash_key = "test_hash";
                    let test_data = generate_test_data(size, SMALL_VALUE_SIZE);
                    
                    for (key, value) in test_data {
                        let _: bool = conn.hset(hash_key, &key, &value).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// CONCURRENT OPERATIONS BENCHMARKS
// ============================================================================

fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_operations");
    
    for num_tasks in CONCURRENT_TASKS {
        // Concurrent SET operations
        group.bench_function(BenchmarkId::new("kv_concurrent_set", num_tasks), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = Arc::new(setup_kv().await);
                    let database_id = DatabaseId::default();
                    let operations_per_task = 100;
                    
                    let mut handles = vec![];
                    
                    for task_id in 0..*num_tasks {
                        let engine_ref = Arc::clone(&engine);
                        let handle = tokio::spawn(async move {
                            for i in 0..operations_per_task {
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
        
        group.bench_function(BenchmarkId::new("redis_concurrent_set", num_tasks), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let operations_per_task = 100;
                    
                    let mut handles = vec![];
                    
                    for task_id in 0..*num_tasks {
                        let handle = tokio::spawn(async move {
                            let mut conn = setup_redis().await;
                            for i in 0..operations_per_task {
                                let key = format!("task_{}_key_{}", task_id, i);
                                let value = format!("task_{}_value_{}", task_id, i);
                                let _: () = conn.set(&key, &value).await.unwrap();
                            }
                        });
                        handles.push(handle);
                    }
                    
                    // Wait for all tasks to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
        
        // Concurrent GET operations (with pre-populated data)
        group.bench_function(BenchmarkId::new("kv_concurrent_get", num_tasks), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = Arc::new(setup_kv().await);
                    let database_id = DatabaseId::default();
                    let operations_per_task = 100;
                    
                    // Pre-populate data
                    for task_id in 0..*num_tasks {
                        for i in 0..operations_per_task {
                            let key = format!("task_{}_key_{}", task_id, i);
                            let value = Value::String(format!("task_{}_value_{}", task_id, i));
                            engine.set(database_id, key, value, None).await.unwrap();
                        }
                    }
                    
                    let mut handles = vec![];
                    
                    for task_id in 0..*num_tasks {
                        let engine_ref = Arc::clone(&engine);
                        let handle = tokio::spawn(async move {
                            for i in 0..operations_per_task {
                                let key = format!("task_{}_key_{}", task_id, i);
                                let _result = engine_ref.get(database_id, &key).await.unwrap();
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
        
        group.bench_function(BenchmarkId::new("redis_concurrent_get", num_tasks), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let operations_per_task = 100;
                    
                    // Pre-populate data
                    let mut conn = setup_redis().await;
                    for task_id in 0..*num_tasks {
                        for i in 0..operations_per_task {
                            let key = format!("task_{}_key_{}", task_id, i);
                            let value = format!("task_{}_value_{}", task_id, i);
                            let _: () = conn.set(&key, &value).await.unwrap();
                        }
                    }
                    
                    let mut handles = vec![];
                    
                    for task_id in 0..*num_tasks {
                        let handle = tokio::spawn(async move {
                            let mut conn = setup_redis().await;
                            for i in 0..operations_per_task {
                                let key = format!("task_{}_key_{}", task_id, i);
                                let _result: Option<String> = conn.get(&key).await.unwrap();
                            }
                        });
                        handles.push(handle);
                    }
                    
                    // Wait for all tasks to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// MIXED WORKLOAD BENCHMARKS
// ============================================================================

fn bench_mixed_workloads(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("mixed_workloads");
    
    for total_ops in [1000, 10000] {
        // Read-heavy workload (80% GET, 20% SET)
        group.bench_function(BenchmarkId::new("kv_read_heavy", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    let read_ops = (total_ops * 8) / 10;
                    let write_ops = total_ops - read_ops;
                    
                    // Read operations
                    for i in 0..read_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result = engine.get(database_id, &key).await.unwrap();
                    }
                    
                    // Write operations
                    for i in 0..write_ops {
                        let key = format!("new_key_{}", i);
                        let value = Value::String(format!("new_value_{}", i));
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_read_heavy", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    let read_ops = (total_ops * 8) / 10;
                    let write_ops = total_ops - read_ops;
                    
                    // Read operations
                    for i in 0..read_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result: Option<String> = conn.get(&key).await.unwrap();
                    }
                    
                    // Write operations
                    for i in 0..write_ops {
                        let key = format!("new_key_{}", i);
                        let value = format!("new_value_{}", i);
                        let _: () = conn.set(&key, &value).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // Write-heavy workload (80% SET, 20% GET)
        group.bench_function(BenchmarkId::new("kv_write_heavy", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    let write_ops = (total_ops * 8) / 10;
                    let read_ops = total_ops - write_ops;
                    
                    // Write operations
                    for i in 0..write_ops {
                        let key = format!("new_key_{}", i);
                        let value = Value::String(format!("new_value_{}", i));
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Read operations
                    for i in 0..read_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result = engine.get(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_write_heavy", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    let write_ops = (total_ops * 8) / 10;
                    let read_ops = total_ops - write_ops;
                    
                    // Write operations
                    for i in 0..write_ops {
                        let key = format!("new_key_{}", i);
                        let value = format!("new_value_{}", i);
                        let _: () = conn.set(&key, &value).await.unwrap();
                    }
                    
                    // Read operations
                    for i in 0..read_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result: Option<String> = conn.get(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
        
        // Balanced workload (50% SET, 50% GET)
        group.bench_function(BenchmarkId::new("kv_balanced", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let engine = setup_kv().await;
                    let database_id = DatabaseId::default();
                    let test_data = generate_kv_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in test_data.clone() {
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    let half_ops = total_ops / 2;
                    
                    // Write operations
                    for i in 0..half_ops {
                        let key = format!("new_key_{}", i);
                        let value = Value::String(format!("new_value_{}", i));
                        engine.set(database_id, key, value, None).await.unwrap();
                    }
                    
                    // Read operations
                    for i in 0..half_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result = engine.get(database_id, &key).await.unwrap();
                    }
                    
                    black_box(&engine);
                });
            });
        });
        
        group.bench_function(BenchmarkId::new("redis_balanced", total_ops), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = setup_redis().await;
                    let test_data = generate_test_data(total_ops / 2, SMALL_VALUE_SIZE);
                    
                    // Pre-populate some data
                    for (key, value) in &test_data {
                        let _: () = conn.set(key, value).await.unwrap();
                    }
                    
                    let half_ops = total_ops / 2;
                    
                    // Write operations
                    for i in 0..half_ops {
                        let key = format!("new_key_{}", i);
                        let value = format!("new_value_{}", i);
                        let _: () = conn.set(&key, &value).await.unwrap();
                    }
                    
                    // Read operations
                    for i in 0..half_ops {
                        let key = format!("key_{}", i % (total_ops / 2));
                        let _result: Option<String> = conn.get(&key).await.unwrap();
                    }
                    
                    black_box(&conn);
                });
            });
        });
    }
    
    group.finish();
}

// ============================================================================
// BENCHMARK GROUPS
// ============================================================================

criterion_group!(
    benches,
    bench_basic_operations,
    bench_ttl_operations,
    bench_data_structures,
    bench_concurrent_operations,
    bench_mixed_workloads
);
criterion_main!(benches);


