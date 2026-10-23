use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde_json::json;
use std::hint::black_box;

fn serialization(c: &mut Criterion) {
    let objects: Vec<_> = (0..1024)
        .map(|id| {
            json!({
                "id": id, "name": format!("host-{id:04}"),
                "data": {"addresses": ["192.0.2.1", "2001:db8::1"],
                    "samples": [1, 2, 3, 4], "description": "quoted \"value\"\n".repeat(8)}
            })
        })
        .collect();
    let bytes = serde_json::to_vec(&objects).unwrap();
    let mut group = c.benchmark_group("release_serialization");
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("encode", |b| {
        b.iter(|| black_box(serde_json::to_vec(black_box(&objects)).unwrap()))
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            black_box(serde_json::from_slice::<serde_json::Value>(black_box(&bytes)).unwrap())
        })
    });
    group.finish();
}
criterion_group!(benches, serialization);
criterion_main!(benches);
