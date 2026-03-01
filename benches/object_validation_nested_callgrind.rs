use hubuum::models::NewHubuumObject;
use hubuum::traits::ValidateAgainstSchema;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use once_cell::sync::Lazy;
use std::hint::black_box;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("tokio runtime"));

static NESTED_SCHEMA: Lazy<serde_json::Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "required": ["service", "environment", "metadata"],
        "properties": {
            "service": { "type": "string" },
            "environment": { "type": "string" },
            "metadata": {
                "type": "object",
                "required": ["owner", "tags", "limits"],
                "properties": {
                    "owner": { "type": "string" },
                    "tags": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 3
                    },
                    "limits": {
                        "type": "object",
                        "required": ["cpu", "memory"],
                        "properties": {
                            "cpu": { "type": "integer", "minimum": 1 },
                            "memory": { "type": "integer", "minimum": 256 }
                        }
                    }
                }
            },
            "endpoints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "port", "protocol"],
                    "properties": {
                        "name": { "type": "string" },
                        "port": { "type": "integer", "minimum": 1 },
                        "protocol": { "type": "string" }
                    }
                }
            }
        }
    })
});

static NESTED_DATA: Lazy<serde_json::Value> = Lazy::new(|| {
    serde_json::json!({
        "service": "asset-api",
        "environment": "production",
        "metadata": {
            "owner": "platform",
            "tags": ["critical", "customer-facing", "benchmark"],
            "limits": {
                "cpu": 8,
                "memory": 4096
            }
        },
        "endpoints": [
            { "name": "http", "port": 8080, "protocol": "tcp" },
            { "name": "https", "port": 8443, "protocol": "tcp" },
            { "name": "metrics", "port": 9090, "protocol": "tcp" }
        ]
    })
});

#[library_benchmark]
fn bench_validate_nested_schema() -> usize {
    let object = NewHubuumObject {
        name: "nested-object".to_string(),
        namespace_id: 1,
        hubuum_class_id: 1,
        data: NESTED_DATA.clone(),
        description: "Benchmark nested validation payload".to_string(),
    };

    RUNTIME
        .block_on(object.validate_against_schema(black_box(&*NESTED_SCHEMA)))
        .expect("validation should succeed");

    black_box(
        object
            .data
            .get("endpoints")
            .and_then(serde_json::Value::as_array)
            .map_or(0, |endpoints| endpoints.len()),
    )
}

library_benchmark_group!(name = benches; benchmarks = bench_validate_nested_schema);
main!(library_benchmark_groups = benches);
