use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hubuum::utilities::db::DatabaseUrlComponents;
use std::hint::black_box;

fn benchmark_database_url_parsing(c: &mut Criterion) {
    let urls = [
        "postgres://postgres:postgres@localhost:5432/hubuum_rust_test",
        "postgres://bench:secret@example.internal/hubuum_bench",
        "mysql://bench:secret@example.internal/hubuum_bench",
    ];

    let mut group = c.benchmark_group("database_url_components");

    for url in urls {
        group.bench_with_input(BenchmarkId::from_parameter(url), &url, |b, url| {
            b.iter(|| {
                black_box(url)
                    .parse::<DatabaseUrlComponents>()
                    .expect("URL should parse")
            })
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_database_url_parsing);
criterion_main!(benches);
