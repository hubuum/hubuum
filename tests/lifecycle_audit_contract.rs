//! Validate #550's proposed interchange examples, not unimplemented lifecycle
//! transitions. Backend/provider qualification remains in the scenario ledger.

use std::sync::OnceLock;

use hubuum_domain::{JsonSchemaErrorKind, validate_json_schema, validate_json_value};
use hubuum_events_core::EventEnvelope;
use rstest::rstest;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn schema(view: &str) -> Value {
    let document = match view {
        "audit" => include_str!("../docs/contracts/lifecycle-audit/audit-event.schema.json"),
        "sink" => include_str!("../docs/contracts/lifecycle-audit/sink-event.schema.json"),
        "membership" => {
            include_str!("../docs/contracts/lifecycle-audit/membership-page.schema.json")
        }
        "unavailable" => include_str!("../docs/contracts/lifecycle-audit/unavailable.schema.json"),
        "delivery" => include_str!("../docs/contracts/lifecycle-audit/delivery-status.schema.json"),
        "capture" => include_str!("../docs/contracts/lifecycle-audit/capture-receipt.schema.json"),
        _ => panic!("unknown contract view"),
    };
    serde_json::from_str(document).unwrap()
}

fn assert_invalid(view: &str, value: &Value) {
    let error = validate_json_value(&schema(view), value).unwrap_err();
    assert_eq!(
        error.kind(),
        JsonSchemaErrorKind::InvalidValue,
        "the schema must compile before a value can be rejected: {error}"
    );
}

fn fixtures() -> &'static Value {
    static EXAMPLES: OnceLock<Value> = OnceLock::new();
    EXAMPLES.get_or_init(|| {
        serde_json::from_str(include_str!(
            "../docs/contracts/lifecycle-audit/examples.json"
        ))
        .unwrap()
    })
}

fn example(name: &str) -> Value {
    fixtures()["examples"][name]
        .as_object()
        .expect("named shared fixture must exist")
        .clone()
        .into()
}

#[rstest]
#[case("audit")]
#[case("sink")]
#[case("membership")]
#[case("unavailable")]
#[case("delivery")]
#[case("capture")]
fn proposal_schema_is_valid_json_schema(#[case] view: &str) {
    validate_json_schema(&schema(view)).unwrap();
}

#[rstest]
#[case("published_audit")]
#[case("same_tier_migration_audit")]
#[case("published_sink")]
#[case("source_retired_audit")]
#[case("erasure_restricted_audit")]
#[case("erasure_completed_audit")]
#[case("erasure_blocked_audit")]
#[case("ready_audit")]
#[case("ready_sink")]
#[case("system_policy_audit")]
#[case("verified_audit_only")]
#[case("membership_page")]
#[case("empty_membership_page")]
#[case("expired_evidence")]
#[case("minimized_evidence")]
#[case("corrupt_evidence")]
#[case("delivery_configured")]
#[case("delivery_unconfigured")]
#[case("delivery_draining")]
#[case("delivery_gap_open")]
#[case("delivery_gap_unknown_count")]
#[case("delivery_gap_recovery_unknown")]
#[case("delivery_gap_acknowledged_expired_source")]
#[case("delivery_history_expired")]
#[case("delivery_gap_acknowledged_audit")]
#[case("capture_ready_without_task")]
#[case("capture_ready_expired_source")]
#[case("capture_expired")]
#[case("capture_unavailable")]
fn shared_interchange_example_satisfies_the_proposal(#[case] name: &str) {
    let value = example(name);
    let view = match value["kind"].as_str() {
        Some("lifecycle_membership") => "membership",
        Some("lifecycle_evidence_unavailable") => "unavailable",
        Some("lifecycle_delivery_status") => "delivery",
        Some("lifecycle_capture_receipt") => "capture",
        _ => value["metadata"]["lifecycle"]["projection"]
            .as_str()
            .unwrap(),
    };
    validate_json_value(&schema(view), &value).unwrap();
    let lifecycle = &value["metadata"]["lifecycle"];
    if !lifecycle.is_null() {
        let limit = if lifecycle["projection"] == "sink" {
            8 * 1024
        } else {
            64 * 1024
        };
        assert!(serde_json::to_vec(&value).unwrap().len() <= limit);
        assert!(serde_json::to_vec(lifecycle).unwrap().len() <= 16 * 1024);
    }
}

#[rstest]
#[case("/metadata/lifecycle/evidence", json!({"membership_id": "hidden"}))]
#[case("/metadata/lifecycle/copies", json!({"object_key": "private"}))]
#[case("/metadata/related_collection_ids", json!([8]))]
#[case("/metadata/lifecycle/scope/collection_uuids", json!(["hidden"]))]
#[case("/metadata/lifecycle/authority", json!({"review_id": "hidden"}))]
#[case("/metadata/lifecycle/delivery", json!({"gap_id": "private"}))]
#[case("/provenance/initiator", json!({"principal_id": 42, "name": "private"}))]
#[case("/actor_user_id", json!(42))]
#[case("/correlation_id", json!("internal-root-operation"))]
#[case("/summary", json!("Collection B contains 17 hidden versions"))]
#[case("/before", json!({"erased_payload": true}))]
fn safe_sink_view_rejects_privileged_or_cross_scope_data(
    #[case] pointer: &str,
    #[case] value: Value,
) {
    let mut sink = example("published_sink");
    let (parent, field) = pointer.rsplit_once('/').unwrap();
    sink.pointer_mut(parent)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(field.to_owned(), value);
    assert_invalid("sink", &sink);
}

#[rstest]
#[case(
    "published_audit",
    "/metadata/lifecycle/evidence/verification_id",
    Value::Null
)]
#[case(
    "published_audit",
    "/metadata/lifecycle/evidence/membership",
    Value::Null
)]
#[case(
    "erasure_completed_audit",
    "/metadata/lifecycle/evidence/receipt_id",
    Value::Null
)]
#[case(
    "erasure_completed_audit",
    "/metadata/lifecycle/evidence/membership",
    Value::Null
)]
#[case("erasure_blocked_audit", "/metadata/lifecycle/state/outcome", json!("committed"))]
#[case("published_audit", "/action", json!("deleted"))]
#[case("system_policy_audit", "/collection_id", json!(7))]
#[case("published_audit", "/collection_id", Value::Null)]
#[case("published_audit", "/metadata/lifecycle/operation_revision", json!(9007199254740993_u64))]
#[case("published_audit", "/metadata/lifecycle/operation_revision", json!("0"))]
#[case("published_audit", "/metadata/lifecycle/contract_version", json!(2))]
#[case("ready_audit", "/metadata/lifecycle/availability", Value::Null)]
#[case("erasure_blocked_audit", "/metadata/lifecycle/state/reasons", json!([]))]
fn malformed_or_insufficient_evidence_is_not_a_valid_success_view(
    #[case] name: &str,
    #[case] pointer: &str,
    #[case] replacement: Value,
) {
    let mut value = example(name);
    *value.pointer_mut(pointer).unwrap() = replacement;
    assert_invalid("audit", &value);
}

#[rstest]
#[case("published_audit", "audit", "/event_id")]
#[case("published_sink", "sink", "/metadata/lifecycle/operation_id")]
#[case("membership_page", "membership", "/members/0/version_uuid")]
#[case("expired_evidence", "unavailable", "/receipt_id")]
#[case("delivery_configured", "delivery", "/subscription_uuid")]
#[case("capture_ready_without_task", "capture", "/capture_id")]
fn evidence_identities_reject_nil_and_reserved_uuids(
    #[case] name: &str,
    #[case] view: &str,
    #[case] pointer: &str,
    #[values(
        "00000000-0000-0000-0000-000000000000",
        "12345678-1234-4234-1234-123456789abc",
        "12345678-1234-0234-8234-123456789abc"
    )]
    identity: &str,
) {
    let mut value = example(name);
    *value.pointer_mut(pointer).unwrap() = json!(identity);
    assert_invalid(view, &value);
}

#[rstest]
#[case("ready_audit", "audit", "/metadata/lifecycle/availability/ready_at")]
#[case("ready_sink", "sink", "/metadata/lifecycle/availability/ready_at")]
#[case("membership_page", "membership", "/expires_at")]
#[case("delivery_configured", "delivery", "/observed_at")]
#[case("capture_ready_without_task", "capture", "/captured_at")]
fn evidence_timestamps_reject_impossible_calendar_dates(
    #[case] name: &str,
    #[case] view: &str,
    #[case] pointer: &str,
) {
    let mut value = example(name);
    *value.pointer_mut(pointer).unwrap() = json!("2026-02-30T00:00:00Z");
    assert_invalid(view, &value);
}

#[rstest]
#[case(
    "erasure_completed_audit",
    "audit",
    "/metadata/lifecycle/evidence/membership"
)]
#[case("membership_page", "membership", "")]
fn protected_membership_has_an_explicit_control_lineage_lifetime(
    #[case] name: &str,
    #[case] view: &str,
    #[case] pointer: &str,
) {
    let mut value = example(name);
    let membership = value.pointer_mut(pointer).unwrap();
    membership["expires_at"] = Value::Null;
    membership["retention"] = json!("control_lineage");
    validate_json_value(&schema(view), &value).unwrap();
}

#[rstest]
#[case(
    "erasure_completed_audit",
    "audit",
    "/metadata/lifecycle/evidence/membership"
)]
#[case("membership_page", "membership", "")]
fn unknown_membership_expiry_is_not_indefinite_protection(
    #[case] name: &str,
    #[case] view: &str,
    #[case] pointer: &str,
) {
    let mut value = example(name);
    value.pointer_mut(pointer).unwrap()["expires_at"] = Value::Null;
    assert_invalid(view, &value);
}

#[rstest]
#[case("lifecycle.archive.verified")]
#[case("lifecycle.delivery_gap.acknowledged")]
fn audit_only_transition_cannot_be_delivered_as_a_sink_view(#[case] name: &str) {
    let mut value = example("published_sink");
    value["metadata"]["lifecycle"]["name"] = json!(name);
    assert_invalid("sink", &value);
}

#[rstest]
#[case("unknown_optional_field")]
#[case("unknown_optional_reason")]
#[case("unknown_name")]
#[case("unknown_major_version")]
fn reader_evolution_vectors_are_distinct_from_closed_v1_producer_payloads(#[case] name: &str) {
    let case = &fixtures()["reader_cases"][name];
    assert_invalid(case["view"].as_str().unwrap(), &case["input"]);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
fn duplicate_out_of_order_delivery_vector_contains_valid_safe_views(#[case] index: usize) {
    let value = &fixtures()["delivery_cases"]["duplicate_out_of_order"]["input"][index];
    validate_json_value(&schema("sink"), value).unwrap();
}

#[rstest]
#[case("delivery_gap_unknown_count", "delivery", "/gap/omitted_events", json!(17))]
#[case("delivery_gap_open", "delivery", "/gap/omitted_events", Value::Null)]
#[case(
    "delivery_gap_acknowledged_expired_source",
    "delivery",
    "/gap/acknowledged_revision",
    Value::Null
)]
#[case("delivery_gap_acknowledged_expired_source", "delivery", "/gap/source_history/state", json!("retained"))]
#[case("delivery_configured", "delivery", "/policy_revision", Value::Null)]
#[case("delivery_configured", "delivery", "/source_generation", Value::Null)]
#[case("delivery_configured", "delivery", "/capacity/row_limit", json!(0))]
#[case("delivery_configured", "delivery", "/capacity/payload_byte_limit", json!(8191))]
#[case("delivery_configured", "delivery", "/capacity/deployment_rows_in_use", json!(999))]
#[case("delivery_configured", "delivery", "/gap_history", json!("unknown"))]
#[case("capture_ready_without_task", "capture", "/coverage/gap_recovered", json!(true))]
#[case("capture_ready_without_task", "capture", "/coverage/basis", json!("wall_clock_interval"))]
#[case("capture_ready_without_task", "capture", "/result/event_count", json!(10001))]
#[case("capture_ready_without_task", "capture", "/result/encoded_bytes", json!(16777217))]
#[case("capture_ready_without_task", "capture", "/result/sha256", json!("provider-etag"))]
#[case("capture_ready_without_task", "capture", "/result/download_url", json!("https://private-provider.example"))]
#[case("capture_ready_without_task", "capture", "/state", json!("expired"))]
#[case(
    "capture_ready_expired_source",
    "capture",
    "/coverage/source_history/reason",
    Value::Null
)]
#[case(
    "delivery_gap_acknowledged_audit",
    "audit",
    "/metadata/lifecycle/delivery",
    Value::Null
)]
fn delivery_evidence_rejects_ambiguous_or_privileged_views(
    #[case] name: &str,
    #[case] view: &str,
    #[case] pointer: &str,
    #[case] replacement: Value,
) {
    let mut value = example(name);
    let (parent, field) = pointer.rsplit_once('/').unwrap();
    value
        .pointer_mut(parent)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(field.to_owned(), replacement);
    assert_invalid(view, &value);
}

#[test]
fn catchup_output_is_bound_to_its_capture_receipt_and_contains_only_safe_views() {
    let bytes = include_bytes!("../docs/contracts/lifecycle-audit/catchup-output.jsonl");
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert!(!bytes.contains(&b'\r'));
    assert!(!bytes.starts_with(&[0xef, 0xbb, 0xbf]));
    let receipt = example("capture_ready_without_task");
    let result = &receipt["result"];
    let digest = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(result["sha256"], digest);
    assert_eq!(result["encoded_bytes"], bytes.len());
    let lines = std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .collect::<Vec<_>>();
    assert_eq!(result["event_count"], lines.len());
    for line in lines {
        let event: Value = serde_json::from_str(line).unwrap();
        validate_json_value(&schema("sink"), &event).unwrap();
        assert_eq!(event["metadata"]["lifecycle"]["scope"], receipt["scope"]);
    }
}

#[test]
fn membership_pages_remain_bounded() {
    let mut page = example("membership_page");
    page["members"] = json!(vec![page["members"][0].clone(); 251]);
    assert_invalid("membership", &page);
}

#[test]
fn unavailable_evidence_cannot_masquerade_as_complete_empty_membership() {
    let mut page = example("empty_membership_page");
    page["coverage"] = json!("expired");
    assert_invalid("membership", &page);
}

#[test]
fn audit_and_sink_views_preserve_the_same_committed_scoped_identity() {
    let audit = example("published_audit");
    let sink = example("published_sink");
    for pointer in [
        "/event_id",
        "/metadata/lifecycle/name",
        "/metadata/lifecycle/operation_id",
        "/metadata/lifecycle/operation_revision",
        "/metadata/lifecycle/scope",
        "/metadata/lifecycle/counts",
    ] {
        assert_eq!(audit.pointer(pointer), sink.pointer(pointer), "{pointer}");
    }
}

#[test]
fn exact_membership_example_is_bound_to_its_published_digest() {
    let page = example("membership_page");
    let mut encoded = format!(
        "hubuum-lifecycle-membership-v1\n{}\n{}\n{}\n",
        page["membership_id"].as_str().unwrap(),
        page["collection_uuid"].as_str().unwrap(),
        page["operation_id"].as_str().unwrap(),
    );
    for member in page["members"].as_array().unwrap() {
        encoded.push_str(member["resource_uuid"].as_str().unwrap());
        encoded.push('\t');
        encoded.push_str(member["version_uuid"].as_str().unwrap());
        encoded.push('\n');
    }
    let audit = example("published_audit");
    let membership = &audit["metadata"]["lifecycle"]["evidence"]["membership"];
    let digest = Sha256::digest(encoded.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(membership["sha256"], digest);
    assert_eq!(membership["encoded_bytes"], encoded.len());
    assert_eq!(
        membership["count"],
        page["members"].as_array().unwrap().len()
    );
}

#[test]
fn proposal_does_not_silently_activate_the_runtime_catalog() {
    let error = serde_json::from_value::<EventEnvelope>(example("published_audit")).unwrap_err();
    assert!(error.to_string().contains("unknown variant `lifecycle`"));
}
