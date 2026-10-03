use super::tests::{envelope, settings, spawn_https_server};
use super::*;
use hubuum_event_sinks_common::SinkFailure;
use rstest::rstest;
use serde_json::json;

#[rstest]
#[case(json!({"body_template":""}))]
#[case(json!({"url_secret_ref":" "}))]
#[case(json!({"max_request_bytes":0}))]
#[case(json!({"headers":{"X-Test":42}}))]
#[case(json!({"response":{"success_statuses":[400]}}))]
#[case(json!({"response":{"retry_statuses":[200]}}))]
#[case(json!({"response":{"body":{"kind":"json_equals","pointer":"bad","value":true}}}))]
#[case(json!({"response":{"body":{"kind":"json_equals","pointer":"/~2","value":true}}}))]
fn rejects_invalid_configuration(#[case] value: Value) {
    assert!(Configuration::parse(&value).is_err());
}

#[tokio::test]
async fn rejects_invalid_template_without_exposing_source() {
    let config =
        Configuration::parse(&json!({"body_template":"private-source {{ broken"})).unwrap();
    let error = config.validate().await.unwrap_err();
    assert!(!error.to_string().contains("private-source"));
}

#[rstest]
#[case(false, "")]
#[case(true, "[TEST] ")]
#[tokio::test]
async fn preview_escapes_event_values_and_exposes_test_context(
    #[case] test: bool,
    #[case] marker: &str,
) {
    let event = envelope();
    let config = json!({"url_secret_ref":"unresolved_secret", "body_template":r#"{"text":{{ (test_marker ~ summary ~ '\n"quoted"') | tojson }},"test":{{ test | tojson }},"id":{{ event.event_id | tojson }}}"#});
    let routing = json!({});
    let prepared = WebhookSink::new(settings())
        .prepare(
            &event,
            SinkDelivery::new(&config, &routing, None).for_test(test),
        )
        .await
        .unwrap();
    assert_eq!(
        prepared.payload(),
        &json!({"text":format!("{marker}{}\n\"quoted\"",event.summary()), "test":test, "id":event.event_id()})
    );
}

#[tokio::test]
async fn legacy_payload_remains_the_event_envelope() {
    let event = envelope();
    let config = json!({});
    let routing = json!({"url":"https://example.invalid/events"});
    let prepared = WebhookSink::new(settings())
        .prepare(&event, SinkDelivery::new(&config, &routing, None))
        .await
        .unwrap();
    assert_eq!(prepared.payload(), &serde_json::to_value(&event).unwrap());
}

#[rstest]
#[case("not json")]
#[case(r#"{"text":"unterminated}"#)]
#[tokio::test]
async fn rejects_invalid_rendered_json(#[case] source: &str) {
    let config = json!({"url_secret_ref":"unresolved_secret","body_template":source});
    let routing = json!({});
    let error = WebhookSink::new(settings())
        .prepare(&envelope(), SinkDelivery::new(&config, &routing, None))
        .await
        .unwrap_err();
    assert_eq!(error.failure(), SinkFailure::Permanent);
}

#[tokio::test]
async fn template_output_obeys_request_limit() {
    let config = json!({"url_secret_ref":"unresolved_secret","body_template":r#"{"text":"too long"}"#, "max_request_bytes":10});
    let routing = json!({});
    assert!(
        WebhookSink::new(settings())
            .prepare(&envelope(), SinkDelivery::new(&config, &routing, None))
            .await
            .is_err()
    );
}

#[test]
fn secret_destination_rejects_routing_override() {
    let config = Configuration::parse(&json!({"url_secret_ref":"destination"})).unwrap();
    assert!(
        config
            .validate_routing(&json!({"url":"https://example.invalid/override"}))
            .is_err()
    );
}

#[tokio::test]
async fn url_secret_and_bearer_secret_are_independent() {
    let (port, request) = spawn_https_server("200 OK").await;
    let url =
        SecretValue::new(format!("https://localhost:{port}/secret-path").into_bytes()).unwrap();
    let token = SecretValue::new(b"separate-token".to_vec()).unwrap();
    let config = json!({"url_secret_ref":"destination", "body_template":r#"{"text":{{ (test_marker ~ summary) | tojson }}}"#});
    let routing = json!({});
    let sink = WebhookSink::new(settings());
    let prepared = sink
        .prepare(
            &envelope(),
            SinkDelivery::new(&config, &routing, None).for_test(true),
        )
        .await
        .unwrap();
    sink.send(&prepared, Some(&token), Some(&url))
        .await
        .unwrap();
    let request = request.await.unwrap();
    assert!(request.starts_with("POST /secret-path HTTP/1.1"));
    assert!(request.contains("authorization: Bearer separate-token"));
    assert!(request.contains("x-hubuum-delivery-purpose: test"));
    assert!(request.contains("[TEST] collection created"));
}

#[rstest]
#[case("ok", 1024, true)]
#[case("different", 1024, false)]
#[case("ok", 2, false)]
#[tokio::test]
async fn response_checks_use_the_acknowledgement_and_reject_possible_truncation(
    #[case] expected: &str,
    #[case] limit: usize,
    #[case] success: bool,
) {
    let (port, request) = spawn_https_server("200 OK").await;
    let config = json!({"max_response_bytes":limit,"response":{"body":{"kind":"text_equals","value":expected}}});
    let routing = json!({"url":format!("https://localhost:{port}/events")});
    let result = WebhookSink::new(settings())
        .deliver(&envelope(), SinkDelivery::new(&config, &routing, None))
        .await;
    let _ = request.await.unwrap();
    assert_eq!(result.is_ok(), success);
}

#[rstest]
#[case(false, 1, None)]
#[case(true, 1, Some("-test-1"))]
#[case(true, 2, Some("-test-2"))]
#[tokio::test]
async fn queued_tests_have_delivery_specific_idempotency_keys(
    #[case] test: bool,
    #[case] delivery_id: i64,
    #[case] suffix: Option<&str>,
) {
    let event = envelope();
    let config = json!({});
    let routing = json!({"url":"https://example.invalid/events"});
    let prepared = WebhookSink::new(settings())
        .prepare(
            &event,
            SinkDelivery::new(&config, &routing, None).for_test(test),
        )
        .await
        .unwrap()
        .for_delivery(EventDeliveryId::new(delivery_id).unwrap());
    assert_eq!(
        prepared.idempotency_key.as_deref(),
        Some(format!("{}{}", event.event_id(), suffix.unwrap_or("")).as_str())
    );
}
