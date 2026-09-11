use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hubuum_domain::CompiledSchema;
use serde_json::json;

fn schema_validation(c: &mut Criterion) {
    let schema = CompiledSchema::try_new(json!({
        "type": "object", "required": ["payload", "samples"],
        "properties": {"payload": {"type": "string", "minLength": 1},
            "samples": {"type": "array", "items": {"type": "integer"}}}
    }))
    .unwrap();
    let mut group = c.benchmark_group("schema_validation_batches");
    group.sample_size(20);
    for bytes in [16 * 1024, 256 * 1024, 1024 * 1024] {
        let document =
            json!({"payload": "x".repeat(bytes), "samples": (0..128).collect::<Vec<_>>()});
        let count = 8 * 1024 * 1024 / serde_json::to_vec(&document).unwrap().len();
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(bytes),
            &document,
            |b, document| {
                b.iter(|| {
                    for _ in 0..count {
                        black_box(schema.inspect(black_box(document))).unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, schema_validation);
criterion_main!(benches);
