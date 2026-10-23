use std::collections::BTreeMap;

use chrono::{Duration, NaiveDate};
use rstest::rstest;
use serde_json::json;

#[cfg(feature = "embedded-migrations")]
use super::{JsonSchemaLimits, verify_restored_backup_matches};
use super::{
    MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS, RESTORE_RECONCILIATION_GRACE_SECONDS,
    RestoreSettings, confirmation_is_stale, normalize_legacy_class_schema_policies,
    normalize_legacy_event_configuration, restore_capability_matches, restore_error_for_storage,
    sha256, validate_computed_field_definitions, verify_backup_document,
};
use crate::errors::ApiError;
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupState, CURRENT_BACKUP_VERSION,
};
use crate::storage::{StorageBackupHistorySection, StorageBackupRow, StorageBackupStateSection};

fn backup_instant() -> chrono::DateTime<chrono::Utc> {
    NaiveDate::from_ymd_opt(2026, 7, 16)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

fn computed_definition(class_id: i32, owner_id: Option<i32>, key: String) -> serde_json::Value {
    json!({
        "class_id": class_id,
        "visibility": if owner_id.is_some() { "personal" } else { "shared" },
        "owner_principal_id": owner_id,
        "key": key,
        "label": "Restored field",
        "description": "",
        "operation": {"type": "first_non_null", "paths": ["/value"]},
        "result_type": "string",
        "enabled": true,
        "revision": 1,
        "semantics_version": 1
    })
}

fn document_with_computed_definitions(definitions: Vec<serde_json::Value>) -> BackupDocument {
    BackupDocument {
        backup_version: CURRENT_BACKUP_VERSION,
        created_at: backup_instant(),
        source_version: "test".to_string(),
        state: BackupState {
            sections: BTreeMap::from([(
                StorageBackupStateSection::ComputedFieldDefinitions,
                definitions
                    .into_iter()
                    .map(StorageBackupRow::try_from_value)
                    .collect::<Result<_, _>>()
                    .unwrap(),
            )]),
        },
        history: None,
        manifest: BackupManifest::default(),
    }
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
fn format_six_notifications_restore_with_legacy_defaults() {
    let mut document = minimally_valid_document();
    document.backup_version = 6;
    document.state.sections.insert(
        StorageBackupStateSection::EventSinks,
        vec![StorageBackupRow::try_from_value(json!({"id": 1, "kind": "webhook"})).unwrap()],
    );
    document.history = Some(BackupHistory {
        sections: BTreeMap::from([(
            StorageBackupHistorySection::TerminalEventDeliveries,
            vec![
                StorageBackupRow::try_from_value(json!({"id": 1, "status": "succeeded"})).unwrap(),
            ],
        )]),
    });
    normalize_legacy_event_configuration(&mut document);
    assert_eq!(
        document.state.sections[&StorageBackupStateSection::EventSinks][0].fields()["delivery_policy"],
        json!({"min_interval_ms": null}),
    );
    let delivery = &document.history.unwrap().sections
        [&StorageBackupHistorySection::TerminalEventDeliveries][0];
    assert_eq!(delivery.fields()["purpose"], "event");
    assert_eq!(delivery.fields()["deferred_reason"], json!(null));
}

#[test]
fn legacy_current_class_without_schema_disables_validation() {
    let mut document = minimally_valid_document();
    document
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Classes)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 2,
                "json_schema": null,
                "validate_schema": true
            }))
            .unwrap(),
        );

    normalize_legacy_class_schema_policies(&mut document);

    let class = &document.state.sections[&StorageBackupStateSection::Classes][0];
    assert_eq!(class.get("validate_schema"), Some(&json!(false)));
}

#[test]
fn legacy_class_history_without_schema_disables_validation() {
    let mut document = minimally_valid_document();
    document.history = Some(BackupHistory {
        sections: StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect(),
    });
    document
        .history
        .as_mut()
        .unwrap()
        .sections
        .get_mut(&StorageBackupHistorySection::ClassHistory)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 2,
                "json_schema": null,
                "validate_schema": true
            }))
            .unwrap(),
        );

    normalize_legacy_class_schema_policies(&mut document);

    let class = &document.history.unwrap().sections[&StorageBackupHistorySection::ClassHistory][0];
    assert_eq!(class.get("validate_schema"), Some(&json!(false)));
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
#[rstest]
#[case::version_six(6, false, true)]
#[case::version_seven(7, false, true)]
#[case::version_eight(8, false, true)]
#[case::unexpected_declaration(8, true, false)]
#[case::missing_current_section(CURRENT_BACKUP_VERSION, false, false)]
fn restored_backup_comparison_normalizes_only_absent_legacy_declarations(
    #[case] version: i32,
    #[case] unexpected_declaration: bool,
    #[case] accepted: bool,
) {
    let mut restored = minimally_valid_document();
    let mut source = restored.clone();
    source.backup_version = version;
    source
        .state
        .sections
        .remove(&StorageBackupStateSection::QueryUsageDeclarations);
    source.manifest = BackupManifest::from_sections(&source.state, None);
    if unexpected_declaration {
        restored
            .state
            .sections
            .get_mut(&StorageBackupStateSection::QueryUsageDeclarations)
            .unwrap()
            .push(StorageBackupRow::try_from_value(json!({"id": 1})).unwrap());
    }

    assert_eq!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok(),
        accepted
    );
}

#[cfg(feature = "embedded-migrations")]
#[test]
fn restored_backup_comparison_accepts_only_the_provenance_event() {
    let mut source = minimally_valid_document();
    source.history = Some(BackupHistory {
        sections: StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect(),
    });
    let mut restored = source.clone();
    restored
        .history
        .as_mut()
        .unwrap()
        .sections
        .get_mut(&StorageBackupHistorySection::AuditEvents)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 1,
                "entity_type": "restore",
                "action": "succeeded"
            }))
            .unwrap(),
        );

    assert!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok()
    );
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::old_to_null(json!({}), json!({"trace_id": null, "trace_span_id": null, "trace_flags": null, "trace_context_version": null}), true)]
#[case::null_to_absent(json!({"trace_id": null, "trace_span_id": null, "trace_flags": null, "trace_context_version": null}), json!({}), true)]
#[case::new_link(json!({}), json!({"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "trace_span_id": "00f067aa0ba902b7", "trace_flags": 1, "trace_context_version": 0}), false)]
#[case::partial_link(json!({}), json!({"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "trace_span_id": null}), false)]
#[case::cleared_link(json!({"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "trace_span_id": "00f067aa0ba902b7", "trace_flags": 1, "trace_context_version": 0}), json!({}), false)]
fn restored_backup_comparison_canonicalizes_only_empty_trace_links(
    #[values(
        StorageBackupHistorySection::TerminalTasks,
        StorageBackupHistorySection::AuditEvents
    )]
    section: StorageBackupHistorySection,
    #[case] source_link: serde_json::Value,
    #[case] restored_link: serde_json::Value,
    #[case] accepted: bool,
) {
    assert_history_comparison(section, source_link, restored_link, accepted);
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::empty_control(StorageBackupHistorySection::TerminalTasks, json!({}), json!({
    "cancel_requested_at": null, "cancel_requested_by": null, "cancel_reason": null,
    "execution_deadline_at": null, "import_effects_committed_at": null,
    "remote_dispatched_at": null, "terminal_reason": null
}), true)]
#[case::lost_cancellation(StorageBackupHistorySection::TerminalTasks, json!({"cancel_requested_at": "2026-09-14T12:00:00Z"}), json!({}), false)]
#[case::lost_deadline(StorageBackupHistorySection::TerminalTasks, json!({"execution_deadline_at": "2026-09-14T12:00:00Z"}), json!({}), false)]
#[case::legacy_remote(StorageBackupHistorySection::RemoteCallResults, json!({}), json!({"side_effect_state": "legacy_unknown"}), true)]
#[case::canonical_legacy_remote(StorageBackupHistorySection::RemoteCallResults, json!({"side_effect_state": "legacy_unknown"}), json!({}), true)]
#[case::lost_dispatch_evidence(StorageBackupHistorySection::RemoteCallResults, json!({"side_effect_state": "possibly_sent"}), json!({}), false)]
#[case::invented_dispatch_evidence(StorageBackupHistorySection::RemoteCallResults, json!({}), json!({"side_effect_state": "not_sent"}), false)]
#[case::malformed_remote(StorageBackupHistorySection::RemoteCallResults, json!({}), json!({"side_effect_state": null}), false)]
#[case::unrelated_drift(StorageBackupHistorySection::TerminalTasks, json!({}), json!({"unknown_field": null}), false)]
fn restored_backup_comparison_preserves_task_execution_evidence(
    #[case] section: StorageBackupHistorySection,
    #[case] source_fields: serde_json::Value,
    #[case] restored_fields: serde_json::Value,
    #[case] accepted: bool,
) {
    assert_history_comparison(section, source_fields, restored_fields, accepted);
}

#[cfg(feature = "embedded-migrations")]
fn assert_history_comparison(
    section: StorageBackupHistorySection,
    source_link: serde_json::Value,
    restored_link: serde_json::Value,
    accepted: bool,
) {
    let row = |link: serde_json::Value| {
        let mut row = json!({"id": 1, "summary": "retained history"});
        row.as_object_mut()
            .unwrap()
            .extend(link.as_object().unwrap().clone());
        StorageBackupRow::try_from_value(row).unwrap()
    };
    let mut source = minimally_valid_document();
    source.history = Some(BackupHistory {
        sections: StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect(),
    });
    source
        .history
        .as_mut()
        .unwrap()
        .sections
        .insert(section, vec![row(source_link)]);
    let mut restored = source.clone();
    let history = restored.history.as_mut().unwrap();
    history.sections.insert(section, vec![row(restored_link)]);
    history
        .sections
        .get_mut(&StorageBackupHistorySection::AuditEvents)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(
                json!({"id": 2, "entity_type": "restore", "action": "succeeded"}),
            )
            .unwrap(),
        );

    assert_eq!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok(),
        accepted
    );
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::legacy(json!({}), json!({"forward_label": null, "reverse_label": null}), true)]
#[case::forward_retained(json!({"forward_label": "contains"}), json!({"forward_label": "contains", "reverse_label": null}), true)]
#[case::reverse_retained(json!({"reverse_label": "is in"}), json!({"forward_label": null, "reverse_label": "is in"}), true)]
#[case::lost_label(json!({"forward_label": "contains"}), json!({"forward_label": null, "reverse_label": null}), false)]
#[case::invented_label(json!({}), json!({"forward_label": "contains", "reverse_label": null}), false)]
#[case::swapped_labels(json!({"forward_label": "contains", "reverse_label": "is in"}), json!({"forward_label": "is in", "reverse_label": "contains"}), false)]
#[case::malformed_label(json!({"forward_label": 42}), json!({"forward_label": null, "reverse_label": null}), false)]
#[case::unrelated_drift(json!({}), json!({"forward_label": null, "reverse_label": null, "unknown_field": null}), false)]
fn restored_backup_comparison_defaults_only_absent_relation_labels(
    #[values(false, true)] history: bool,
    #[case] source_fields: serde_json::Value,
    #[case] restored_fields: serde_json::Value,
    #[case] accepted: bool,
) {
    if history {
        assert_history_comparison(
            StorageBackupHistorySection::ClassRelationHistory,
            source_fields,
            restored_fields,
            accepted,
        );
        return;
    }
    let mut source = minimally_valid_document();
    let mut restored = source.clone();
    for (document, fields) in [
        (&mut source, source_fields),
        (&mut restored, restored_fields),
    ] {
        document.state.sections.insert(
            StorageBackupStateSection::ClassRelations,
            vec![StorageBackupRow::try_from_value(fields).unwrap()],
        );
    }

    assert_eq!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok(),
        accepted
    );
}

#[cfg(feature = "embedded-migrations")]
#[test]
fn restored_backup_comparison_accepts_legacy_class_schema_normalization() {
    let mut source = minimally_valid_document();
    source
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Classes)
        .unwrap()
        .push(
            StorageBackupRow::try_from_value(json!({
                "id": 2,
                "json_schema": null,
                "validate_schema": true
            }))
            .unwrap(),
        );
    let mut restored = source.clone();
    normalize_legacy_class_schema_policies(&mut restored);

    assert!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok()
    );
}

#[cfg(feature = "embedded-migrations")]
#[rstest]
#[case::legacy_spaces(json!("legacy correlation"), json!(null), true)]
#[case::legacy_empty(json!(""), json!(null), true)]
#[case::legacy_overlong(json!("x".repeat(129)), json!(null), true)]
#[case::valid_unchanged(json!("client-correlation"), json!("client-correlation"), true)]
#[case::valid_cleared(json!("client-correlation"), json!(null), false)]
#[case::valid_replaced(json!("client-correlation"), json!("different-correlation"), false)]
#[case::restored_invalid(json!(null), json!("invalid correlation"), false)]
#[case::legacy_replaced_with_invalid(json!("legacy correlation"), json!("invalid correlation"), false)]
#[case::malformed_non_string(json!(42), json!(null), false)]
fn restored_backup_comparison_normalizes_only_legacy_source_correlation_ids(
    #[case] source_correlation: serde_json::Value,
    #[case] restored_correlation: serde_json::Value,
    #[case] accepted: bool,
) {
    let row = |correlation: serde_json::Value| {
        StorageBackupRow::try_from_value(json!({
            "id": 1,
            "correlation_id": correlation
        }))
        .unwrap()
    };
    let mut source = minimally_valid_document();
    source.history = Some(BackupHistory {
        sections: StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect(),
    });
    source.history.as_mut().unwrap().sections.insert(
        StorageBackupHistorySection::AuditEvents,
        vec![row(source_correlation)],
    );
    let mut restored = source.clone();
    restored.history.as_mut().unwrap().sections.insert(
        StorageBackupHistorySection::AuditEvents,
        vec![
            row(restored_correlation),
            StorageBackupRow::try_from_value(json!({
                "id": 2,
                "entity_type": "restore",
                "action": "succeeded"
            }))
            .unwrap(),
        ],
    );

    assert_eq!(
        verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default()).is_ok(),
        accepted
    );
}

#[cfg(feature = "embedded-migrations")]
#[test]
fn restored_backup_comparison_rejects_state_drift() {
    let source = minimally_valid_document();
    let mut restored = source.clone();
    restored
        .state
        .sections
        .get_mut(&StorageBackupStateSection::Collections)
        .unwrap()
        .clear();

    let error = verify_restored_backup_matches(&source, &restored, JsonSchemaLimits::default())
        .unwrap_err();

    assert!(error.to_string().contains("logical state"));
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
#[case::shared(MAX_SHARED_DEFINITIONS, None)]
#[case::personal(MAX_PERSONAL_DEFINITIONS, Some(7))]
fn restore_rejects_computed_definition_scope_over_capacity(
    #[case] maximum: usize,
    #[case] owner_id: Option<i32>,
) {
    let definitions = (0..=maximum)
        .map(|index| computed_definition(42, owner_id, format!("field_{index}")))
        .collect();
    let error =
        validate_computed_field_definitions(&document_with_computed_definitions(definitions))
            .unwrap_err();
    assert!(error.to_string().contains(&maximum.to_string()));
}

#[test]
fn restore_rejects_duplicate_computed_definition_keys() {
    let definitions = vec![
        computed_definition(42, None, "duplicate".to_string()),
        computed_definition(42, None, "duplicate".to_string()),
    ];

    let error =
        validate_computed_field_definitions(&document_with_computed_definitions(definitions))
            .unwrap_err();

    assert!(error.to_string().contains("duplicate"));
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
