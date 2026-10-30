//! Raw-envelope compatibility probe for server #550. This does not implement
//! lifecycle interpretation, authorization, delivery reconciliation or approval.
#![cfg(all(feature = "async", feature = "blocking"))]

use std::{env, fs, sync::Arc};

use hubuum_client::{
    BaseUrl, Client, EventResponse, MockTransport, Token, TransportResponse, blocking,
};
use reqwest::StatusCode;
use rstest::rstest;
use serde_json::{Value, json};

fn fixtures() -> Value {
    let path = env::var("HUBUUM_LIFECYCLE_FIXTURES").expect("set the shared examples.json path");
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn read_events(input: &Value, asynchronous: bool) -> Vec<EventResponse> {
    let transport = MockTransport::default();
    transport.push_response(TransportResponse::json(StatusCode::OK, input).unwrap());
    let base = BaseUrl::new("https://example.test").unwrap();
    let events = if asynchronous {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                Client::builder(base)
                    .with_transport(Arc::new(transport.clone()))
                    .build()
                    .unwrap()
                    .authenticate(Token::new("fixture-token"))
                    .events()
                    .list()
                    .await
                    .unwrap()
            })
    } else {
        blocking::Client::builder(base)
            .with_transport(Arc::new(transport.clone()))
            .build()
            .unwrap()
            .authenticate(Token::new("fixture-token"))
            .events()
            .list()
            .unwrap()
    };
    assert_eq!(transport.requests().len(), 1);
    assert_eq!(transport.requests()[0].url.path(), "/api/v1/events");
    events
}

fn assert_lifecycle_preserved(input: &Value, event: &EventResponse) {
    let output = serde_json::to_value(event).unwrap();
    for field in [
        "id",
        "event_id",
        "entity_type",
        "entity_id",
        "collection_id",
        "action",
        "actor_kind",
        "actor_user_id",
        "provenance",
        "before",
        "after",
        "metadata",
        "schema_version",
    ] {
        assert_eq!(output[field], input[field], "field {field}");
    }
}

#[rstest]
#[case("examples", "published_audit")]
#[case("examples", "published_sink")]
#[case("examples", "same_tier_migration_audit")]
#[case("examples", "source_retired_audit")]
#[case("examples", "erasure_restricted_audit")]
#[case("examples", "erasure_completed_audit")]
#[case("examples", "erasure_blocked_audit")]
#[case("examples", "ready_audit")]
#[case("examples", "ready_sink")]
#[case("examples", "system_policy_audit")]
#[case("examples", "verified_audit_only")]
#[case("examples", "delivery_gap_acknowledged_audit")]
#[case("reader_cases", "unknown_optional_field")]
#[case("reader_cases", "unknown_optional_reason")]
#[case("reader_cases", "unknown_name")]
#[case("reader_cases", "unknown_major_version")]
fn event_list_preserves_raw_lifecycle_data(
    #[case] section: &str,
    #[case] name: &str,
    #[values(false, true)] asynchronous: bool,
) {
    let document = fixtures();
    let input = if section == "reader_cases" {
        &document[section][name]["input"]
    } else {
        &document[section][name]
    };
    assert_eq!(input["entity_type"], "lifecycle");
    let events = read_events(&json!([input]), asynchronous);
    assert_eq!(events.len(), 1);
    assert_lifecycle_preserved(input, &events[0]);
}

#[rstest]
fn event_list_preserves_duplicate_and_out_of_order_wire_entries(
    #[values(false, true)] asynchronous: bool,
) {
    let document = fixtures();
    let input = &document["delivery_cases"]["duplicate_out_of_order"]["input"];
    let entries = input.as_array().unwrap();
    let events = read_events(input, asynchronous);
    assert_eq!(events.len(), entries.len());
    for (entry, event) in entries.iter().zip(&events) {
        assert_lifecycle_preserved(entry, event);
    }
}
