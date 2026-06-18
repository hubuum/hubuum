use hubuum::tasks::request_hash;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::sync::LazyLock;

// Idempotency request hashing canonicalises a JSON payload (recursively sorting
// object keys) and SHA-256s the result. It runs for every import/report request
// that carries an Idempotency-Key, so the payload shape is deliberately nested
// and unsorted to exercise the canonicalisation path.
static PAYLOAD: LazyLock<serde_json::Value> = LazyLock::new(|| {
    serde_json::json!({
        "mode": "upsert",
        "atomicity": "all_or_nothing",
        "classes": [
            { "name": "asset", "namespace": "platform", "schema": { "type": "object" } },
            { "name": "service", "namespace": "platform", "schema": { "type": "object" } }
        ],
        "objects": [
            {
                "name": "asset-001",
                "class": "asset",
                "data": { "region": "eu-north-1", "tags": ["critical", "customer-facing"], "cpu": 8 }
            },
            {
                "name": "asset-002",
                "class": "asset",
                "data": { "region": "eu-west-1", "tags": ["batch"], "cpu": 2 }
            }
        ],
        "collision_policy": "replace"
    })
});

#[library_benchmark]
fn bench_request_hash() -> usize {
    let digest = request_hash(black_box(&*PAYLOAD)).expect("benchmark payload should hash");

    black_box(digest.len())
}

library_benchmark_group!(name = benches; benchmarks = bench_request_hash);
main!(library_benchmark_groups = benches);
