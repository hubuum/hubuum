use std::collections::BTreeMap;

use chrono::{Duration, NaiveDate};
use hubuum_domain::{IdentityReference, ResourceIdentity, VersionIdentity};
#[cfg(feature = "embedded-migrations")]
use hubuum_storage_core::StorageBackupBudget;
use hubuum_storage_core::{StorageBackupSnapshot, StorageIdentityEvidence, StorageIdentitySubject};
use rstest::rstest;
use serde_json::json;

#[cfg(feature = "embedded-migrations")]
use super::JsonSchemaLimits;
use super::{
    RESTORE_RECONCILIATION_GRACE_SECONDS, RestoreSettings, confirmation_is_stale,
    restore_capability_matches, restore_error_for_storage, sha256,
};
use crate::errors::ApiError;
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupState, CURRENT_BACKUP_VERSION,
};
use crate::storage::{StorageBackupHistorySection, StorageBackupRow, StorageBackupStateSection};

fn verify_backup_document(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<super::BackupVerificationReport, ApiError> {
    super::verify_backup_reader(bytes, max_bytes, hubuum_domain::JsonSchemaLimits::default())
}

#[cfg(feature = "embedded-migrations")]
fn comparison_document(
    mut document: BackupDocument,
) -> Result<super::replay::ReplayDocument, ApiError> {
    document.manifest = BackupManifest::from_sections(&document.state, document.history.as_ref());
    let bytes = serde_json::to_vec(&document)?;
    Ok(super::replay::parse(
        bytes.as_slice(),
        16 * 1024 * 1024,
        JsonSchemaLimits::default(),
    )?
    .0)
}

#[cfg(feature = "embedded-migrations")]
fn compare_backups(source: BackupDocument, restored: BackupDocument) -> Result<(), ApiError> {
    super::replay::compare(
        &comparison_document(source)?,
        &comparison_document(restored)?,
    )
}

fn backup_instant() -> chrono::DateTime<chrono::Utc> {
    NaiveDate::from_ymd_opt(2026, 7, 16)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

fn resource_identity() -> StorageIdentityEvidence {
    StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: ResourceIdentity::generate(),
            },
            version: VersionIdentity::generate(),
        }),
        BTreeMap::new(),
    )
    .unwrap()
}

#[cfg(feature = "embedded-migrations")]
fn audit_row(id: i32, entity_type: &str, action: &str) -> StorageBackupRow {
    let event_id = hubuum_events_core::EventId::from(uuid::Uuid::new_v4());
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Event { identity: event_id }),
        BTreeMap::new(),
    )
    .unwrap();
    StorageBackupRow::try_from_value(json!({
        "id": id, "event_id": event_id, "entity_type": entity_type, "action": action,
        "logical_identity": evidence,
    }))
    .unwrap()
}

fn minimally_valid_document() -> BackupDocument {
    let mut sections = StorageBackupStateSection::ALL
        .iter()
        .copied()
        .map(|section| (section, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    sections
        .get_mut(&StorageBackupStateSection::IdentityScopes)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 1,
                "name": "local",
                "logical_identity": resource_identity(),
                "provider_kind": "local",
                "created_at": "2026-07-16T00:00:00Z",
                "updated_at": "2026-07-16T00:00:00Z",
                "revision": 1
            }))
            .unwrap(),
        );
    sections
        .get_mut(&StorageBackupStateSection::Collections)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 1,
                "name": "root",
                "logical_identity": resource_identity(),
                "parent_collection_id": null,
                "created_at": "2026-07-16T00:00:00Z",
                "updated_at": "2026-07-16T00:00:00Z",
                "revision": 1
            }))
            .unwrap(),
        );
    sections
        .get_mut(&StorageBackupStateSection::CollectionAuthorization)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "collection_id": 1,
                "revision": 1
            }))
            .unwrap(),
        );
    sections
        .get_mut(&StorageBackupStateSection::CollectionHierarchy)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "ancestor_collection_id": 1,
                "descendant_collection_id": 1,
                "depth": 0
            }))
            .unwrap(),
        );
    let state = BackupState { sections };
    let manifest = BackupManifest::from_sections(&state, None);
    BackupDocument {
        backup_version: CURRENT_BACKUP_VERSION,
        created_at: backup_instant(),
        source_version: "0.0.11".to_string(),
        state,
        history: None,
        manifest,
    }
}

#[test]
fn offline_backup_verification_returns_only_sanitized_artifact_evidence() {
    let mut document = minimally_valid_document();
    document
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Collections)
        .unwrap()[0] = StorageBackupRow::try_from_value(json!({
        "id": 1,
        "name": "verification-canary",
        "logical_identity": resource_identity(),
        "parent_collection_id": null,
        "created_at": "2026-07-16T00:00:00Z",
        "updated_at": "2026-07-16T00:00:00Z",
        "revision": 1
    }))
    .unwrap();
    let bytes = serde_json::to_vec(&document).unwrap();

    let report = verify_backup_document(&bytes, bytes.len()).unwrap();
    let report = serde_json::to_string(&report).unwrap();

    assert!(report.contains("\"result\":\"passed\""));
    assert!(report.contains("\"mode\":\"format_only\""));
    assert!(!report.contains("verification-canary"));
}

#[test]
fn offline_backup_verification_rejects_manifest_count_drift() {
    let mut document = minimally_valid_document();
    document
        .manifest
        .item_counts
        .insert("collections".to_string(), 99);
    let bytes = serde_json::to_vec(&document).unwrap();

    let error = verify_backup_document(&bytes, bytes.len()).unwrap_err();

    assert!(error.to_string().contains("manifest count"));
    assert!(error.to_string().contains("collections"));
}

#[rstest::rstest]
#[case::six(6)]
#[case::seven(7)]
#[case::eight(8)]
#[case::nine(9)]
#[case::ten(10)]
#[case::current(CURRENT_BACKUP_VERSION)]
fn offline_backup_verification_accepts_supported_versions(#[case] version: i32) {
    let mut document = minimally_valid_document();
    document.backup_version = version;
    if version < 9 {
        document
            .state
            .sections
            .remove(&StorageBackupStateSection::QueryUsageDeclarations);
    }
    if version < 8 {
        document
            .state
            .sections
            .remove(&StorageBackupStateSection::EventSinkCollectionGrants);
    }
    document.manifest = BackupManifest::from_sections(&document.state, None);
    let bytes = serde_json::to_vec(&document).unwrap();
    assert!(verify_backup_document(&bytes, bytes.len()).is_ok());
}

#[rstest]
#[case::legacy(9, false, true)]
#[case::current_missing(10, false, false)]
#[case::current_present(10, true, true)]
fn backup_version_declares_authoritative_computed_history(
    #[case] version: i32,
    #[case] included: bool,
    #[case] accepted: bool,
) {
    let mut document = minimally_valid_document();
    document.backup_version = version;
    let mut sections = StorageBackupHistorySection::ALL
        .iter()
        .copied()
        .map(|section| (section, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    if !included {
        sections.remove(&StorageBackupHistorySection::ComputedFieldHistory);
    }
    document.history = Some(BackupHistory { sections });
    assert_eq!(document.validate_version().is_ok(), accepted);
}

#[test]
fn offline_backup_verification_rejects_future_versions() {
    let mut document = minimally_valid_document();
    document.backup_version = CURRENT_BACKUP_VERSION + 1;
    let bytes = serde_json::to_vec(&document).unwrap();

    let error = verify_backup_document(&bytes, bytes.len()).unwrap_err();

    assert!(error.to_string().contains("Unsupported backup version"));
}

#[test]
fn offline_backup_verification_rejects_dangling_references() {
    let mut document = minimally_valid_document();
    document
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Groups)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 2,
                "identity_scope_id": 999,
                "revision": 1
            }))
            .unwrap(),
        );
    document.manifest = BackupManifest::from_sections(&document.state, None);
    let bytes = serde_json::to_vec(&document).unwrap();

    let error = verify_backup_document(&bytes, bytes.len()).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("does not reference a retained row")
    );
}

#[test]
fn offline_backup_verification_rejects_invalid_row_timestamps() {
    let mut document = minimally_valid_document();
    document
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Collections)
        .unwrap()[0] = StorageBackupRow::try_from_value(json!({
        "id": 1,
        "name": "root",
        "parent_collection_id": null,
        "created_at": "not-a-timestamp",
        "updated_at": "2026-07-16T00:00:00Z",
        "revision": 1
    }))
    .unwrap();
    let bytes = serde_json::to_vec(&document).unwrap();

    let error = verify_backup_document(&bytes, bytes.len()).unwrap_err();

    assert!(error.to_string().contains("invalid RFC 3339 timestamp"));
}

#[cfg(feature = "embedded-migrations")]
fn retained_comparison_document() -> BackupDocument {
    let mut document = minimally_valid_document();
    let snapshot = StorageBackupSnapshot::try_new(document.state.sections.clone(), None).unwrap();
    let replay = hubuum_storage_core::StorageBackupReplay::from_snapshot(
        snapshot,
        StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
    )
    .unwrap()
    .at_restore_boundary(document.created_at)
    .unwrap();
    let (state, history) = replay.materialize().unwrap().into_parts();
    document.state.sections = state;
    document.history = history.map(|sections| BackupHistory { sections });
    document
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::format_six(6)]
#[case::format_seven(7)]
#[case::format_eight(8)]
fn streaming_comparison_accepts_absent_legacy_declarations(#[case] version: i32) {
    let mut source = minimally_valid_document();
    let restored = source.clone();
    source.backup_version = version;
    source
        .state
        .sections
        .remove(&StorageBackupStateSection::QueryUsageDeclarations);
    assert!(compare_backups(source, restored).is_ok());
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::unchanged(false)]
#[case::changed(true)]
fn streaming_comparison_detects_authoritative_state_drift(#[case] changed: bool) {
    let source = minimally_valid_document();
    let mut restored = source.clone();
    if changed {
        let rows = restored
            .state
            .sections
            .get_mut(&StorageBackupStateSection::Collections)
            .unwrap();
        let mut row = rows[0].clone().into_value();
        row["name"] = json!("changed");
        rows[0] = StorageBackupRow::try_from_value(row).unwrap();
    }
    let result = compare_backups(source, restored);
    if changed {
        assert!(result.unwrap_err().to_string().contains("collections"));
    } else {
        assert!(result.is_ok());
    }
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::missing(0, "restore", "succeeded", false)]
#[case::expected(1, "restore", "succeeded", true)]
#[case::duplicate(2, "restore", "succeeded", false)]
#[case::unrelated(1, "object", "create", false)]
#[case::failed(1, "restore", "failed", false)]
fn streaming_comparison_requires_exactly_one_success_provenance_event(
    #[case] count: i32,
    #[case] entity_type: &str,
    #[case] action: &str,
    #[case] accepted: bool,
) {
    let source = retained_comparison_document();
    let mut restored = source.clone();
    let events = restored
        .history
        .as_mut()
        .unwrap()
        .sections
        .get_mut(&StorageBackupHistorySection::AuditEvents)
        .unwrap();
    for id in 1..=count {
        events.push(audit_row(id, entity_type, action));
    }
    assert_eq!(compare_backups(source, restored).is_ok(), accepted);
}

#[cfg(feature = "embedded-migrations")]
#[test]
fn streaming_comparison_rejects_retained_history_drift() {
    let source = retained_comparison_document();
    let mut restored = source.clone();
    let history = &mut restored.history.as_mut().unwrap().sections;
    history
        .get_mut(&StorageBackupHistorySection::AuditEvents)
        .unwrap()
        .push(audit_row(1, "restore", "succeeded"));
    let rows = history
        .get_mut(&StorageBackupHistorySection::CollectionHistory)
        .unwrap();
    let mut row = rows[0].clone().into_value();
    row["origin"] = json!("changed historical provenance");
    rows[0] = StorageBackupRow::try_from_value(row).unwrap();
    let error = compare_backups(source, restored).unwrap_err();
    assert!(error.to_string().contains("collection_history"), "{error}");
}

#[rstest]
#[case::upload_limit(60, 0)]
#[case::retention(0, 1024)]
fn restore_settings_reject_zero_limits(
    #[case] retention_minutes: i64,
    #[case] upload_bytes: usize,
) {
    assert!(RestoreSettings::new(retention_minutes, upload_bytes).is_err());
}

#[test]
fn restore_settings_reject_unrepresentable_retention() {
    let error = RestoreSettings::new(i64::MAX, 1024).unwrap_err();

    assert_eq!(
        error,
        "restore stage retention is outside the supported duration range"
    );
}

#[rstest]
#[case::exact_match(Some("expected-capability"), "expected-capability", true)]
#[case::mismatch(Some("expected-capability"), "different-capability", false)]
#[case::missing_stage(None, "expected-capability", false)]
fn restore_capability_validation_handles_present_and_missing_stages(
    #[case] stored_capability: Option<&str>,
    #[case] supplied_capability: &str,
    #[case] expected: bool,
) {
    let stored_hash = stored_capability.map(|capability| sha256(capability.as_bytes()));

    assert_eq!(
        restore_capability_matches(stored_hash.as_deref(), supplied_capability),
        expected
    );
}

#[rstest]
#[case::just_confirmed(0, false)]
#[case::inside_grace_period(RESTORE_RECONCILIATION_GRACE_SECONDS - 1, false)]
#[case::at_grace_boundary(RESTORE_RECONCILIATION_GRACE_SECONDS, true)]
#[case::past_grace_period(RESTORE_RECONCILIATION_GRACE_SECONDS + 1, true)]
fn restore_confirmation_staleness_respects_grace_period(
    #[case] age_seconds: i64,
    #[case] expected: bool,
) {
    let now = NaiveDate::from_ymd_opt(2026, 7, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();

    assert_eq!(
        confirmation_is_stale(now - Duration::seconds(age_seconds), now),
        expected
    );
}

#[rstest]
#[case::internal_server(
    ApiError::InternalServerError("secret internal path".to_string()),
    "secret internal path"
)]
#[case::database(
    ApiError::DatabaseError("password=database-secret".to_string()),
    "database-secret"
)]
#[case::connection(
    ApiError::DbConnectionError("postgres://user:secret@db/app".to_string()),
    "user:secret"
)]
#[case::hash(
    ApiError::HashError("hash implementation detail".to_string()),
    "implementation detail"
)]
fn restore_status_internal_errors_do_not_expose_details(
    #[case] error: ApiError,
    #[case] private_detail: &str,
) {
    let stored = restore_error_for_storage(&error);

    assert_eq!(stored, "An internal error occurred");
    assert!(!stored.contains(private_detail));
}

#[rstest]
#[case::validation_failed(
    ApiError::ValidationError("Backup manifest is invalid".to_string()),
    "Backup manifest is invalid"
)]
#[case::conflict(
    ApiError::Conflict("Restore stage changed status concurrently".to_string()),
    "Restore stage changed status concurrently"
)]
fn restore_status_preserves_public_safe_errors(#[case] error: ApiError, #[case] expected: &str) {
    assert_eq!(restore_error_for_storage(&error), expected);
}

#[test]
fn replay_parser_accepts_reordered_members_and_single_byte_reads() {
    use std::io::Read;
    struct OneByte<'a>(&'a [u8]);
    impl Read for OneByte<'_> {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            let limit = bytes.len().min(1);
            self.0.read(&mut bytes[..limit])
        }
    }
    let document = minimally_valid_document();
    // Value's sorted keys put history and manifest before state/version metadata.
    let bytes = serde_json::to_vec(&serde_json::to_value(&document).unwrap()).unwrap();
    let (replay, digest, count) = super::replay::parse(
        OneByte(&bytes),
        bytes.len(),
        hubuum_domain::JsonSchemaLimits::default(),
    )
    .unwrap();
    let expected = StorageBackupSnapshot::try_new_with_limits(
        document.state.sections,
        document.history.map(|h| h.sections),
        hubuum_domain::JsonSchemaLimits::default(),
    )
    .unwrap();
    assert_eq!(replay.snapshot.materialize().unwrap(), expected);
    assert_eq!((digest, count), (sha256(&bytes), bytes.len()));
}

#[rstest]
#[case::duplicate_envelope("duplicate_envelope")]
#[case::duplicate_section("duplicate_section")]
#[case::truncated("truncated")]
#[case::trailing("trailing")]
fn replay_parser_rejects_ambiguous_or_incomplete_input(#[case] change: &str) {
    let mut bytes = serde_json::to_string(&minimally_valid_document()).unwrap();
    match change {
        "duplicate_envelope" => bytes.insert_str(1, "\"backup_version\":10,"),
        "duplicate_section" => {
            bytes = bytes.replacen("\"sections\":{", "\"sections\":{\"objects\":[],", 1)
        }
        "truncated" => {
            bytes.pop();
        }
        "trailing" => bytes.push_str(" {}"),
        _ => unreachable!(),
    }
    assert!(
        super::verify_backup_reader(
            bytes.as_bytes(),
            bytes.len(),
            hubuum_domain::JsonSchemaLimits::default()
        )
        .is_err()
    );
}

#[test]
fn replay_parser_enforces_input_limit_including_trailing_whitespace() {
    let mut bytes = serde_json::to_vec(&minimally_valid_document()).unwrap();
    let limit = bytes.len();
    bytes.push(b' ');
    let error = super::verify_backup_reader(
        bytes.as_slice(),
        limit,
        hubuum_domain::JsonSchemaLimits::default(),
    )
    .unwrap_err();
    assert!(matches!(error, ApiError::PayloadTooLarge(_)));
}

#[test]
fn streaming_restore_accepts_legacy_missing_computed_history() {
    let mut document = minimally_valid_document();
    let bytes = serde_json::to_vec(&document).unwrap();
    let (replay, _, _) = super::replay::parse(
        bytes.as_slice(),
        bytes.len(),
        hubuum_domain::JsonSchemaLimits::default(),
    )
    .unwrap();
    let (state, mut history) = replay
        .snapshot
        .at_restore_boundary(document.created_at)
        .unwrap()
        .materialize()
        .unwrap()
        .into_parts();
    history
        .as_mut()
        .unwrap()
        .remove(&StorageBackupHistorySection::ComputedFieldHistory);
    document.state = BackupState { sections: state };
    document.history = history.map(|sections| BackupHistory { sections });
    document.backup_version = 9;
    document.manifest = BackupManifest::from_sections(&document.state, document.history.as_ref());
    let bytes = serde_json::to_vec(&document).unwrap();
    verify_backup_document(&bytes, bytes.len()).expect("legacy computed history is optional");
}
