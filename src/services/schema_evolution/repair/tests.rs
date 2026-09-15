use super::*;
use rstest::rstest;

fn legacy(status: &str) -> SchemaWorkResponse {
    serde_json::from_value(json!({
        "task_id":1,"target":{"class_id":1,"revision":2},"kind":"impact","status":status,
        "start_epoch":1,"end_epoch":1,"upper_bound":10,"cursor":7,"examined":7,"valid":0,"invalid":7,"not_required":0,"uninspectable":0,"stale":0,"invalid_samples":[1,2,3,4,5],"elapsed_millis":1,"batches":1,"created_at":"2026-09-14T00:00:00Z",
        "impact":{"baseline":{"class_id":1,"revision":1},"counts":{"newly_invalid":7,"newly_valid":0,"still_invalid":0,"still_valid":0,"newly_required_valid":0,"no_longer_required":0,"unchanged_not_required":0,"uninspectable":0},"failures":[{"reason":{"keyword":"required","schema_path":"/required","missing_property":"hostname"},"objects":7,"samples":[1,2,3,4,5]}],"ungrouped_failures":0},
        "readiness":"inconclusive","current_epoch":2,"current_active_schema":{"class_id":1,"revision":1}
    })).unwrap()
}

fn context(analysis: &SchemaWorkResponse) -> Value {
    report_context(
        analysis,
        "Class",
        &SchemaObjectUrlTemplate::try_new("https://example.test/objects/{object_id}".into())
            .unwrap(),
        Utc::now(),
    )
    .unwrap()
}

#[actix_web::test]
async fn legacy_sampled_reports_explain_missing_ids_and_diagnostics() {
    let html = render_content(
        &context(&legacy("complete")),
        TemplateLimits::new(32, 1_000_000).with_max_output_bytes(262_144),
    )
    .await
    .unwrap();
    assert!(html.contains("2 affected object IDs were not retained"));
    assert!(html.contains("only the first failure or a sampled ID"));
    assert_eq!(html.matches("<article ").count(), 5);
}

#[rstest]
#[case("running")]
#[case("cancelled")]
#[case("failed")]
#[case("superseded")]
#[actix_web::test]
async fn unfinished_reports_are_explicitly_partial(#[case] status: &str) {
    let html = render_content(
        &context(&legacy(status)),
        TemplateLimits::new(32, 1_000_000).with_max_output_bytes(262_144),
    )
    .await
    .unwrap();
    assert!(html.contains(&format!("Partial analysis: {status}")));
    assert!(html.contains("Compatibility has not been established"));
}

#[actix_web::test]
async fn large_report_rendering_fails_instead_of_truncating() {
    let result = render_content(
        &context(&legacy("complete")),
        TemplateLimits::new(32, 1_000_000).with_max_output_bytes(128),
    )
    .await;
    assert!(matches!(result, Err(ApiError::PayloadTooLarge(_))));
}

#[test]
fn document_wrapper_counts_toward_the_output_limit() {
    assert!(matches!(
        wrap_document("x".repeat(100), 100),
        Err(ApiError::PayloadTooLarge(_))
    ));
}

#[test]
fn expanded_object_urls_cannot_exceed_the_context_assembly_budget() {
    let mut analysis = legacy("complete");
    let impact = analysis.impact.as_mut().unwrap();
    impact.failures[0].samples = (1..=3000).collect();
    let urls = SchemaObjectUrlTemplate::try_new(format!(
        "https://example.test/{}/{{object_id}}",
        "x".repeat(1900),
    ))
    .unwrap();
    let result = report_context(&analysis, "Class", &urls, Utc::now());
    assert!(matches!(result, Err(ApiError::PayloadTooLarge(_))));
}
