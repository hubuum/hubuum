//! Hash-only comparison; fixture generation and allocation reporting are untimed.
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hubuum_storage_postgres::source_data_sha256;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

struct AllocationCounter;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

fn record_allocation(size: usize) {
    if COUNTING.load(Ordering::Relaxed) {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    }
}

// The wrapper preserves System's allocation/deallocation contract. Counters are
// enabled only for a separate synchronous hash call, never Criterion's timing.
unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation(size);
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: AllocationCounter = AllocationCounter;

// Exact pre-change canonicalization at 61182b17866e14b5705c760316496518aa70b907.
fn legacy_canonical_json(value: &Value, output: &mut String) {
    match value {
        Value::Object(values) => {
            output.push('{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).unwrap());
                output.push(':');
                legacy_canonical_json(&values[key], output);
            }
            output.push('}');
        }
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                legacy_canonical_json(value, output);
            }
            output.push(']');
        }
        _ => output.push_str(&serde_json::to_string(value).unwrap()),
    }
}

fn legacy_hash(data: &Value) -> String {
    let mut canonical = String::new();
    legacy_canonical_json(data, &mut canonical);
    Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn streamed_hash(data: &Value) -> String {
    source_data_sha256(data).unwrap()
}

fn allocation_sample(hash: fn(&Value) -> String, data: &Value) -> (usize, usize) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    let digest = hash(data);
    COUNTING.store(false, Ordering::Relaxed);
    black_box(digest);
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    )
}

fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

fn benchmark_hash(c: &mut Criterion) {
    let cpu_hz = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|output| output.trim().parse::<u64>().ok());
    let mut group = c.benchmark_group("computed_source_hash");
    group
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    for bytes in [1024, 262_144, 1_048_576] {
        for shape in ["string", "escaped", "wide"] {
            let data = match shape {
                "string" => json!({"padding":"x".repeat(bytes),"value":"benchmark.example"}),
                "escaped" => json!({"padding":"\n\\\"ø".repeat(bytes/5),"value":null}),
                _ => Value::Object(
                    (0..bytes / 32)
                        .map(|index| (format!("key_{index:06}"), json!([index, true, null])))
                        .collect(),
                ),
            };
            assert_eq!(legacy_hash(&data), streamed_hash(&data));
            let serialized_bytes = serde_json::to_vec(&data).unwrap().len();
            for (name, hash) in [
                ("legacy", legacy_hash as fn(&Value) -> String),
                ("streamed", streamed_hash),
            ] {
                let (allocations, allocated_bytes) = allocation_sample(hash, &data);
                eprintln!(
                    "hash_allocations implementation={name} shape={shape} fixture_bytes={bytes} serialized_bytes={serialized_bytes} allocations={allocations} allocated_bytes={allocated_bytes}"
                );
                if let Some((hz, before)) = cpu_hz.zip(process_cpu_ticks()) {
                    let iterations = if bytes == 1024 { 10_000 } else { 100 };
                    for _ in 0..iterations {
                        black_box(hash(black_box(&data)));
                    }
                    if let Some(after) = process_cpu_ticks() {
                        let cpu_us =
                            (after - before) as f64 * 1_000_000.0 / hz as f64 / iterations as f64;
                        eprintln!(
                            "hash_cpu implementation={name} shape={shape} fixture_bytes={bytes} iterations={iterations} cpu_us={cpu_us:.2} tick_hz={hz}"
                        );
                    }
                }
                group.bench_with_input(
                    BenchmarkId::new(format!("{shape}/{name}"), bytes),
                    &data,
                    |b, data| b.iter(|| black_box(hash(black_box(data)))),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_hash);
criterion_main!(benches);
