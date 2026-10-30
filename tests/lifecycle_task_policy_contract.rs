//! Proposed wire shapes only; these tests do not certify lifecycle execution.

use hubuum_domain::{JsonSchemaErrorKind, validate_json_value};
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
