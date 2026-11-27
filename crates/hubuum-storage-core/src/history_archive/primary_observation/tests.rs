use chrono::{Duration, TimeZone, Utc};
use rstest::rstest;
use serde_json::json;

use super::*;
use crate::artifacts::{ArtifactIdentity, ArtifactReference, DestinationId};
use crate::history_archive::tests::row;

fn fixture() -> (HistoryArchiveDescriptor, HistoryArchiveRow) {
    let base = row();
    let mut payload = base.payload().clone();
    payload["description"] = json!("a\nβ\"".repeat(LARGE_DESCRIPTION_BYTES / 4));
    payload["additional_evidence"] = json!({"description": "", "version": 1});
    let row = HistoryArchiveRow::new(
        base.identity(),
        base.valid_from(),
        Some(Utc.timestamp_opt(20, 0).unwrap()),
        payload,
    )
    .unwrap();
    let descriptor = HistoryArchiveDescriptor::new(
        &row,
        Vec::new(),
        3,
        ArtifactReference::from_catalog(ArtifactIdentity::new(DestinationId::generate()), [13; 32]),
    )
    .unwrap();
    (descriptor, row)
}

#[rstest]
#[case::original("original")]
#[case::description("description")]
#[case::unrecognized_metadata("metadata")]
#[case::historical_name("name")]
#[case::closed_interval("interval")]
fn native_observation_requires_all_original_fields(#[case] change: &str) {
    let (descriptor, original) = fixture();
    let description = original.collection().description();
    let mut digest = Sha256::digest(description.as_bytes()).into();
    let encoded_bytes = serde_json::to_vec(description).unwrap().len() - 2;
    let mut payload = original.payload().clone();
    payload["description"] = json!("");
    let mut end = original.valid_to().unwrap();
    match change {
        "description" => digest = [0; 32],
        "metadata" => payload["additional_evidence"]["version"] = json!(2),
        "name" => payload["name"] = json!("changed"),
        "interval" => end += Duration::seconds(1),
        "original" => {}
        _ => unreachable!(),
    }
    let projected = HistoryArchiveRow::new(
        original.identity(),
        original.valid_from(),
        Some(end),
        payload,
    )
    .unwrap();
    let source = descriptor
        .observe_primary_description(projected, digest, encoded_bytes)
        .unwrap();
    match change {
        "original" => assert!(matches!(source, HistoryArchiveInput::ObservedPrimary(_))),
        _ => assert!(matches!(source, HistoryArchiveInput::CorruptPrimary(_))),
    }
}

#[test]
fn source_projection_charges_the_escaped_original_row_size() {
    let (descriptor, original) = fixture();
    let mut payload = original.payload().clone();
    payload["description"] = json!("");
    let projected = HistoryArchiveRow::new(
        original.identity(),
        original.valid_from(),
        original.valid_to(),
        payload,
    )
    .unwrap();
    let projected_bytes = serde_json::to_vec(&projected).unwrap().len();
    let original_digest = Sha256::digest(original.collection().description().as_bytes()).into();
    let source = descriptor
        .observe_primary_description(
            projected,
            original_digest,
            HISTORY_ARCHIVE_ROW_BYTES - projected_bytes + 1,
        )
        .unwrap();
    assert!(matches!(source, HistoryArchiveInput::BudgetLimited(_)));
}

#[test]
fn publication_checks_the_optional_primary_commitment() {
    let (descriptor, original) = fixture();
    let mut document = serde_json::to_value(descriptor).unwrap();
    document["primary_commitment_v1"] = json!(vec![0; 32]);
    let changed: HistoryArchiveDescriptor = serde_json::from_value(document).unwrap();
    assert!(!changed.matches_row(&original).unwrap());
}

#[test]
fn legacy_descriptor_still_verifies_a_large_row() {
    let (descriptor, original) = fixture();
    let mut document = serde_json::to_value(descriptor).unwrap();
    document
        .as_object_mut()
        .unwrap()
        .remove("primary_commitment_v1")
        .unwrap();
    let legacy: HistoryArchiveDescriptor = serde_json::from_value(document).unwrap();
    assert!(legacy.matches_row(&original).unwrap());
    assert!(legacy.primary_commitment_v1.is_none());
}
