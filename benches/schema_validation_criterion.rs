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
    // Admission accounts for schema work and conservatively escaped JSON size.
    // Exercise the original production-sized fixtures under the deployment defaults.
    for (name, payload_sizes, expected_valid) in [
        (
            "schema_validation_accepted_batches",
            [16 * 1024, 256 * 1024, 1024 * 1024],
            true,
        ),
        (
            "schema_validation_rejected_batches",
            [2 * 1024 * 1024, 3 * 1024 * 1024, 4 * 1024 * 1024],
            false,
        ),
    ] {
        let mut group = c.benchmark_group(name);
        group.sample_size(20);
        for bytes in payload_sizes {
            let document =
                json!({"payload": "x".repeat(bytes), "samples": (0..128).collect::<Vec<_>>()});
            assert_eq!(
                schema.inspect(&document).is_ok(),
                expected_valid,
                "{name}/{bytes} no longer has its intended validation outcome"
            );
            // Match the default worker's row and serialized-byte batch bounds.
            let count = (8 * 1024 * 1024 / serde_json::to_vec(&document).unwrap().len()).min(64);
            group.throughput(Throughput::Elements(count as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(bytes),
                &document,
                |b, document| {
                    b.iter(|| {
                        for _ in 0..count {
                            let _ = black_box(schema.inspect(black_box(document)));
                        }
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, schema_validation);
criterion_main!(benches);
