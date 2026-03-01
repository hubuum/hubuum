use hubuum::models::NewHubuumObject;
use hubuum::tests::constants::{get_schema, SchemaType};
use hubuum::traits::ValidateAgainstSchema;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use once_cell::sync::Lazy;
use std::hint::black_box;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio runtime"));

#[library_benchmark]
fn bench_validate_geo_schema() -> usize {
    let object = NewHubuumObject {
        name: "geo-object".to_string(),
        namespace_id: 1,
        hubuum_class_id: 1,
        data: serde_json::json!({
            "latitude": 40.7128,
            "longitude": -74.0060
        }),
        description: "Benchmark validation payload".to_string(),
    };

    RUNTIME
        .block_on(object.validate_against_schema(black_box(get_schema(SchemaType::Geo))))
        .expect("validation should succeed");

    black_box(object.data.as_object().map_or(0, |data| data.len()))
}

library_benchmark_group!(name = benches; benchmarks = bench_validate_geo_schema);
main!(library_benchmark_groups = benches);
