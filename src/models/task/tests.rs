#[test]
fn generated_task_inventory_matches_runtime_kinds() {
    let inventory: serde_json::Value = serde_json::from_str(include_str!(
        "../../../docs/generated/project_inventory.json"
    ))
    .unwrap();
    assert_eq!(
        inventory["task_kinds"],
        serde_json::to_value(super::TaskKind::ALL).unwrap()
    );
}

use super::*;

fn test_timestamp() -> NaiveDateTime {
    chrono::DateTime::from_timestamp(1_700_000_000, 0)
        .unwrap()
        .naive_utc()
}

#[test]
fn task_result_counts_derive_processed_from_terminal_outcomes() {
    let counts = TaskResultCounts::from_outcomes(2, 1).unwrap();

    assert_eq!(
        (counts.processed(), counts.success(), counts.failed()),
        (3, 2, 1)
    );
}

#[test]
fn task_result_counts_reject_negative_stored_values() {
    assert!(TaskResultCounts::from_stored(-1, 0, 0).is_err());
}

#[test]
fn task_result_counts_reject_processed_overflow() {
    assert!(TaskResultCounts::from_outcomes(i32::MAX, 1).is_err());
}

#[test]
fn terminal_task_statuses_are_stable() {
    assert_eq!(
        TaskStatus::TERMINAL.map(TaskStatus::as_str),
        ["succeeded", "failed", "partially_succeeded", "cancelled"]
    );
    assert_eq!(
        TaskStatus::ALL.map(TaskStatus::is_terminal),
        [false, false, false, true, true, true, true]
    );
}

#[test]
fn active_task_statuses_are_stable() {
    assert_eq!(
        TaskStatus::ACTIVE.map(TaskStatus::as_str),
        ["validating", "running"]
    );
    assert_eq!(
        TaskStatus::NON_TERMINAL.map(TaskStatus::as_str),
        ["queued", "validating", "running"]
    );
}

#[test]
fn task_details_deserialization_rejects_multiple_kinds() {
    let invalid = serde_json::json!({
        "import": {"results_url": "/imports/1/results"},
        "export": {
            "output_url": "/exports/1/output",
            "output_available": false,
            "output_expired": false,
            "output_expires_at": null,
            "template_name": null,
            "output_content_type": null,
            "warning_count": null,
            "truncated": null,
            "total_duration_ms": null,
            "query_duration_ms": null,
            "hydration_duration_ms": null,
            "render_duration_ms": null
        },
        "backup": null
    });

    let error = serde_json::from_value::<TaskDetails>(invalid).unwrap_err();

    assert!(error.to_string().contains("exactly one detail kind"));
}

#[test]
fn export_details_deserialization_rejects_contradictory_output_state() {
    let invalid = serde_json::json!({
        "output_url": "/exports/1/output",
        "output_available": true,
        "output_expired": true,
        "output_expires_at": test_timestamp(),
        "template_name": null,
        "output_content_type": "application/json",
        "warning_count": 0,
        "truncated": false,
        "total_duration_ms": 1,
        "query_duration_ms": 1,
        "hydration_duration_ms": 0,
        "render_duration_ms": 0
    });

    let error = serde_json::from_value::<ExportTaskDetails>(invalid).unwrap_err();

    assert!(error.to_string().contains("inconsistent"));
}

#[test]
fn task_id_new_accepts_positive() {
    assert_eq!(TaskID::new(1).unwrap().id(), 1);
    assert_eq!(TaskID::new(i32::MAX).unwrap().id(), i32::MAX);
}

#[test]
fn task_id_new_rejects_non_positive() {
    for invalid in [0, -1, i32::MIN] {
        let err: ApiError = TaskID::new(invalid).unwrap_err().into();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }
}

#[test]
fn task_id_deserialize_routes_through_new() {
    // A valid id deserializes; a non-positive id is rejected by the validating constructor,
    // so an invalid path segment never produces a TaskID.
    assert_eq!(serde_json::from_str::<TaskID>("7").unwrap().id(), 7);
    assert!(serde_json::from_str::<TaskID>("0").is_err());
    assert!(serde_json::from_str::<TaskID>("-3").is_err());
}

#[test]
fn task_record_debug_redacts_request_and_scope_material() {
    let timestamp = test_timestamp();
    let task = TaskRecord {
        discovery_metadata: None,
        discovery_state: Default::default(),
        discovery_authorized: true,
        control: hubuum_storage_core::StorageTaskControl::new(
            hubuum_storage_core::StorageTaskKind::Import,
        ),
        id: 7,
        kind: TaskKind::Import.as_str().to_string(),
        status: TaskStatus::Running.as_str().to_string(),
        submitted_by: Some(1),
        idempotency_key: Some("idempotency-secret".to_string()),
        request_hash: Some("request-hash-secret".to_string()),
        request_payload: Some(serde_json::json!({"password": "payload-secret"})),
        summary: Some("safe summary".to_string()),
        total_items: 1,
        processed_items: 0,
        success_items: 0,
        failed_items: 0,
        submitted_token_id: Some(3),
        submitted_token_scoped: true,
        submitted_token_scopes: serde_json::json!({"token": "scope-secret"}),
        request_redacted_at: None,
        started_at: Some(timestamp),
        finished_at: None,
        deleted_at: None,
        deleted_by: None,
        created_at: timestamp,
        updated_at: timestamp,
        lease_expires_at: Some(timestamp),
        attempt_count: 1,
        initiator_user_id: Some(1),
        trace_link: None,
    };

    let debug = format!("{task:?}");

    assert!(debug.contains("safe summary"));
    assert!(debug.contains(REDACTED_DEBUG_VALUE));
    for secret in [
        "idempotency-secret",
        "request-hash-secret",
        "payload-secret",
        "scope-secret",
    ] {
        assert!(!debug.contains(secret), "debug output exposed {secret}");
    }
}

#[test]
fn export_task_details_include_persisted_phase_timings() {
    let timestamp = test_timestamp();
    let task = TaskRecord {
        discovery_metadata: None,
        discovery_state: Default::default(),
        discovery_authorized: true,
        control: hubuum_storage_core::StorageTaskControl::new(
            hubuum_storage_core::StorageTaskKind::Import,
        ),
        id: 7,
        kind: TaskKind::Export.as_str().to_string(),
        status: TaskStatus::Succeeded.as_str().to_string(),
        submitted_by: Some(1),
        idempotency_key: None,
        request_hash: None,
        request_payload: None,
        summary: None,
        total_items: 1,
        processed_items: 1,
        success_items: 1,
        failed_items: 0,
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        request_redacted_at: Some(timestamp),
        started_at: Some(timestamp),
        finished_at: Some(timestamp),
        deleted_at: None,
        deleted_by: None,
        created_at: timestamp,
        updated_at: timestamp,
        lease_expires_at: None,
        attempt_count: 1,
        initiator_user_id: Some(1),
        trace_link: None,
    };
    let output = ExportTaskOutputSummary {
        task_id: task.id,
        template_name: Some("inventory".to_string()),
        content_type: "text/plain".to_string(),
        warning_count: 0,
        truncated: false,
        output_expires_at: timestamp,
        total_duration_ms: 150,
        query_duration_ms: 40,
        hydration_duration_ms: 30,
        render_duration_ms: 70,
    };

    let response = task
        .to_response_with_export_output(ExportOutputLookup::Available(&output))
        .unwrap();
    let TaskDetails::Export(details) = response.details.unwrap() else {
        panic!("expected export task details");
    };
    let ExportTaskOutputStatus::Available {
        total_duration_ms,
        query_duration_ms,
        hydration_duration_ms,
        render_duration_ms,
        ..
    } = details.state()
    else {
        panic!("expected available export output");
    };

    assert_eq!(
        (
            total_duration_ms,
            query_duration_ms,
            hydration_duration_ms,
            render_duration_ms,
        ),
        (150, 40, 30, 70)
    );
}
