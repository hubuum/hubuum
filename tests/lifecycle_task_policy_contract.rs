//! Proposed wire shapes only; these tests do not certify lifecycle execution.

use hubuum_domain::{JsonSchemaErrorKind, validate_json_value};
use hubuum_storage_core::StorageIdentityHistoryKind;
use rstest::rstest;
use serde_json::{Value, json};

fn examples() -> Value {
    serde_json::from_str(include_str!(
        "../docs/contracts/lifecycle-policy/examples.json"
    ))
    .unwrap()
}

fn apply_schema() -> Value {
    serde_json::from_str(include_str!(
        "../docs/contracts/lifecycle-policy/apply-request.schema.json"
    ))
    .unwrap()
}

#[test]
fn preview_and_apply_examples_preserve_the_exact_proposal() {
    let examples = examples();
    assert_eq!(examples["change"], examples["preview"]["body"]["proposal"]);
    assert_eq!(examples["change"], examples["apply_request"]["proposal"]);
}

#[test]
fn proposed_change_example_satisfies_the_standalone_write_schema() {
    let schema = serde_json::from_str(include_str!(
        "../docs/contracts/lifecycle-policy/policy-change.schema.json"
    ))
    .unwrap();
    validate_json_value(&schema, &examples()["change"]).unwrap();
}

#[derive(Clone, Copy, Debug)]
enum ApplyCase {
    Retrospective,
    Prospective,
    MissingReview,
    UnknownWriteField,
    UnsupportedAction,
    UnsupportedFamily,
    NegativeAge,
    FloatingRevision,
    LargeStringRevision,
    NumericRevision,
    InvalidIntegerReference,
}

#[rstest]
#[case::retrospective(ApplyCase::Retrospective, true)]
#[case::prospective(ApplyCase::Prospective, true)]
#[case::missing_review(ApplyCase::MissingReview, false)]
#[case::unknown_write_field(ApplyCase::UnknownWriteField, false)]
#[case::unsupported_action(ApplyCase::UnsupportedAction, false)]
#[case::unsupported_family(ApplyCase::UnsupportedFamily, false)]
#[case::negative_age(ApplyCase::NegativeAge, false)]
#[case::floating_revision(ApplyCase::FloatingRevision, false)]
#[case::large_string_revision(ApplyCase::LargeStringRevision, true)]
#[case::numeric_revision(ApplyCase::NumericRevision, false)]
#[case::invalid_integer_reference(ApplyCase::InvalidIntegerReference, false)]
fn proposed_apply_contract_rejects_ambiguous_or_unreviewed_writes(
    #[case] case: ApplyCase,
    #[case] valid: bool,
) {
    let mut request = examples()["apply_request"].clone();
    match case {
        ApplyCase::Retrospective => {}
        ApplyCase::Prospective => {
            request["proposal"]["application"] = json!({"kind":"prospective"});
            request.as_object_mut().unwrap().remove("review");
        }
        ApplyCase::MissingReview => {
            request.as_object_mut().unwrap().remove("review");
        }
        ApplyCase::UnknownWriteField => request["confirmed"] = json!(true),
        ApplyCase::UnsupportedAction => request["proposal"]["action"] = json!("erase_now"),
        ApplyCase::UnsupportedFamily => request["proposal"]["family"] = json!("task"),
        ApplyCase::NegativeAge => {
            request["proposal"]["binding"]["trigger"]["age_seconds"] = json!("-1");
        }
        ApplyCase::FloatingRevision => {
            request["proposal"]["expected_policy_revision"] = json!("7.5");
        }
        ApplyCase::LargeStringRevision => {
            request["proposal"]["expected_policy_revision"] = json!("9007199254740993");
        }
        ApplyCase::NumericRevision => {
            request["proposal"]["expected_policy_revision"] = json!(9007199254740993_u64);
        }
        ApplyCase::InvalidIntegerReference => {
            request["proposal"]["scope"]["current_id"] = json!(0);
        }
    }
    let result = validate_json_value(&apply_schema(), &request);
    if valid {
        result.unwrap();
    } else {
        assert_eq!(
            result.unwrap_err().kind(),
            JsonSchemaErrorKind::InvalidValue
        );
    }
}

#[rstest]
#[case::scope_uuid("/proposal/scope/identity", json!("not-a-uuid"))]
#[case::nil_scope("/proposal/scope/identity", json!("00000000-0000-0000-0000-000000000000"))]
#[case::reserved_scope_variant("/proposal/scope/identity", json!("12345678-1234-4234-1234-123456789abc"))]
#[case::reserved_scope_version("/proposal/scope/identity", json!("12345678-1234-0234-8234-123456789abc"))]
#[case::nil_preview("/review/preview_id", json!("00000000-0000-0000-0000-000000000000"))]
#[case::preview_uuid("/review/preview_id", json!("not-a-uuid"))]
#[case::cutoff_text("/proposal/application/closed_through", json!("yesterday"))]
#[case::cutoff_calendar("/proposal/application/closed_through", json!("2026-02-30T00:00:00Z"))]
#[case::cutoff_offset("/proposal/application/closed_through", json!("2026-10-30T01:00:00+01:00"))]
fn malformed_identity_or_cutoff_is_rejected(#[case] pointer: &str, #[case] value: Value) {
    let mut request = examples()["apply_request"].clone();
    *request.pointer_mut(pointer).unwrap() = value;
    assert_eq!(
        validate_json_value(&apply_schema(), &request)
            .unwrap_err()
            .kind(),
        JsonSchemaErrorKind::InvalidValue
    );
}

#[test]
fn standalone_and_embedded_change_schemas_agree() {
    let mut standalone: Value = serde_json::from_str(include_str!(
        "../docs/contracts/lifecycle-policy/policy-change.schema.json"
    ))
    .unwrap();
    standalone.as_object_mut().unwrap().remove("$schema");
    standalone.as_object_mut().unwrap().remove("title");
    assert_eq!(standalone, apply_schema()["properties"]["proposal"]);
}

#[rstest]
#[case::inherit(json!({"kind": "inherit"}))]
#[case::disabled(json!({"kind": "disabled_expiry"}))]
#[case::indefinite(json!({"kind": "indefinite"}))]
#[case::age(json!({"kind": "expire", "trigger": {"kind": "age", "age_seconds": "3600"}, "minimum_age_seconds": "600"}))]
#[case::size(json!({"kind": "expire", "trigger": {"kind": "logical_size", "upper_logical_bytes": "1000", "lower_logical_bytes": "500"}, "minimum_age_seconds": "600"}))]
#[case::or(json!({"kind": "expire", "trigger": {"kind": "age_or_size", "age": {"age_seconds": "3600"}, "size": {"upper_logical_bytes": "1000", "lower_logical_bytes": "500"}}, "minimum_age_seconds": "600"}))]
#[case::and(json!({"kind": "expire", "trigger": {"kind": "age_and_size", "age": {"age_seconds": "3600", "deleted_age_seconds": "7200"}, "size": {"upper_logical_bytes": "1000", "lower_logical_bytes": "500"}}, "minimum_age_seconds": "600"}))]
fn binding_variants_accept_complete_rules(#[case] binding: Value) {
    let mut request = examples()["apply_request"].clone();
    request["proposal"]["binding"] = binding;
    validate_json_value(&apply_schema(), &request).unwrap();
}

#[rstest]
#[case::proposal("/proposal")]
#[case::scope("/proposal/scope")]
#[case::binding("/proposal/binding")]
#[case::trigger("/proposal/binding/trigger")]
#[case::application("/proposal/application")]
#[case::review("/review")]
fn nested_unknown_write_fields_are_rejected(#[case] pointer: &str) {
    let mut request = examples()["apply_request"].clone();
    request.pointer_mut(pointer).unwrap()["confirmed"] = json!(true);
    assert_eq!(
        validate_json_value(&apply_schema(), &request)
            .unwrap_err()
            .kind(),
        JsonSchemaErrorKind::InvalidValue
    );
}

#[rstest]
#[case::collection(StorageIdentityHistoryKind::Collection)]
#[case::class(StorageIdentityHistoryKind::Class)]
#[case::object(StorageIdentityHistoryKind::Object)]
#[case::class_relation(StorageIdentityHistoryKind::ClassRelation)]
#[case::object_relation(StorageIdentityHistoryKind::ObjectRelation)]
#[case::export_template(StorageIdentityHistoryKind::ExportTemplate)]
#[case::remote_target(StorageIdentityHistoryKind::RemoteTarget)]
#[case::computed_definitions(StorageIdentityHistoryKind::ComputedDefinitions)]
#[case::class_schema(StorageIdentityHistoryKind::ClassSchema)]
fn history_families_use_storage_wire_names(#[case] family: StorageIdentityHistoryKind) {
    let mut request = examples()["apply_request"].clone();
    request["proposal"]["family"] = serde_json::to_value(family).unwrap();
    request["proposal"]["scope"] = json!({"kind": "deployment"});
    validate_json_value(&apply_schema(), &request).unwrap();
}

#[test]
fn collection_history_cannot_have_a_class_policy_scope() {
    let mut request = examples()["apply_request"].clone();
    request["proposal"]["family"] = json!("collection");
    assert_eq!(
        validate_json_value(&apply_schema(), &request)
            .unwrap_err()
            .kind(),
        JsonSchemaErrorKind::InvalidValue
    );
}

#[rstest]
#[case::utc_z("2026-10-30T00:00:00Z")]
#[case::utc_offset("2026-10-30T00:00:00+00:00")]
#[case::fractional("2026-10-30T00:00:00.123456Z")]
#[case::leap_day("2028-02-29T00:00:00Z")]
fn utc_cutoffs_are_accepted(#[case] timestamp: &str) {
    let mut request = examples()["apply_request"].clone();
    request["proposal"]["application"]["closed_through"] = json!(timestamp);
    validate_json_value(&apply_schema(), &request).unwrap();
}
