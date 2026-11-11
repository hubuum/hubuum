use hubuum_templates::{MAX_WORKER_HEAP_BYTES, TemplateExecution, TemplateLimits};
use rstest::rstest;
use serde_json::{Value, json};

#[tokio::test]
async fn oversized_output_is_rejected_without_allocating_the_complete_output() {
    let error = TemplateExecution::new(
        "output",
        "{% for _ in range(10000) %}0123456789{% endfor %}",
        TemplateLimits::new(64, 500_000),
    )
    .max_output_bytes(1024)
    .render(&json!({}))
    .await
    .err()
    .expect("output limit should fail");
    assert!(error.to_string().contains("output limit"));
}

#[tokio::test]
async fn intermediate_allocation_failure_is_contained_in_the_worker() {
    // Each repeated string is legal to MiniJinja individually; retaining several
    // captures exceeds the whole-worker budget before anything reaches stdout.
    let source =
        "{% set a = 'x' * 60000000 %}{% set b = a ~ a %}{% set c = b ~ b %}{{ c | length }}";
    // Let allocation reach the independent heap guard before fuel admission.
    let error = TemplateExecution::new("allocation", source, TemplateLimits::new(64, u64::MAX))
        .render(&json!({}))
        .await
        .err()
        .expect("intermediate allocation must be bounded");
    assert!(error.to_string().contains("heap budget"), "{error}");
    let healthy =
        TemplateExecution::new("healthy", "still running", TemplateLimits::new(64, 50_000))
            .render(&json!({}))
            .await
            .unwrap();
    assert!(healthy.peak_heap_bytes() < MAX_WORKER_HEAP_BYTES);
    assert_eq!(healthy.into_parts().0, "still running");
}

#[tokio::test]
async fn macro_capture_failure_is_contained_in_the_worker() {
    let source = "{% set chunk = 'x' * 60000000 %}{% macro large() %}{% for _ in range(10) %}{{ chunk }}{% endfor %}{% endmacro %}{{ large() | length }}";
    // Let capture reach the independent heap guard before fuel admission.
    let error = TemplateExecution::new("capture", source, TemplateLimits::new(64, u64::MAX))
        .render(&json!({}))
        .await
        .err()
        .expect("macro capture must be bounded");
    assert!(error.to_string().contains("heap budget"), "{error}");
}

#[rstest]
#[case::filter_input("{{ payload | length }}", json!({ "payload": "x".repeat(64 * 1024) }))]
#[case::string_allocation("{{ 'x' * 65536 }}", json!({}))]
#[tokio::test]
async fn data_processing_is_bounded_by_fuel(#[case] source: &str, #[case] context: Value) {
    let error = TemplateExecution::new("data", source, TemplateLimits::new(64, 100))
        .render(&context)
        .await
        .err()
        .expect("data work must consume fuel");

    assert!(error.to_string().contains("fuel"), "{error}");
}
