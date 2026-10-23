use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use hubuum_domain::CompiledSchema;
use serde_json::json;
use std::hint::black_box;

fn schema(c: &mut Criterion) {
    let source = json!({"type": "object", "required": ["name", "samples"],
        "properties": {"name": {"type": "string", "minLength": 1},
            "samples": {"type": "array", "maxItems": 512,
                "items": {"type": "integer", "minimum": 0, "maximum": 1000}}}});
    let compiled = CompiledSchema::try_new(source).unwrap();
    let document = json!({"name": "deterministic-host", "samples": (0..256).collect::<Vec<_>>()});
    assert!(compiled.inspect(&document).is_ok());
    let mut group = c.benchmark_group("release_schema");
    group.throughput(Throughput::Elements(256));
    group.bench_function("validate", |b| {
        b.iter(|| black_box(compiled.inspect(black_box(&document))))
    });
    group.finish();
}
criterion_group!(benches, schema);
criterion_main!(benches);
