use hubuum::models::NewHubuumObject;
use hubuum::traits::ValidateAgainstSchema;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::sync::LazyLock;
use tokio::runtime::Runtime;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));
static GEO_SCHEMA: LazyLock<serde_json::Value> = LazyLock::new(|| {
    serde_json::json!({
        "$id": "https://example.com/geographical-location.schema.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Geographical Location",
        "description": "A geographical location",
        "type": "object",
        "required": ["latitude", "longitude"],
        "properties": {
            "latitude": {
                "type": "number",
                "minimum": -90,
                "maximum": 90
            },
            "longitude": {
                "type": "number",
                "minimum": -180,
                "maximum": 180
            }
        }
    })
});

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
        .block_on(object.validate_against_schema(black_box(&*GEO_SCHEMA)))
        .expect("validation should succeed");

    black_box(object.data.as_object().map_or(0, |data| data.len()))
}

library_benchmark_group!(name = benches; benchmarks = bench_validate_geo_schema);
main!(library_benchmark_groups = benches);
