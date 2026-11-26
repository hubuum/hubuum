use super::*;

fn export_output_builder() -> StorageExportOutputBuilder {
    let now = chrono::Utc::now();
    StorageExportOutput::builder(
        TaskId::new(91_000).unwrap(),
        "application/json",
        serde_json::json!({}),
        serde_json::json!([]),
        now,
        now,
    )
    .output(Some(serde_json::json!({})), None)
}

#[test]
fn export_output_summary_rejects_a_negative_warning_count() {
    let error = StorageExportOutputSummary::try_new(
        TaskId::new(91_000).unwrap(),
        None,
        "application/json",
        -1,
        false,
        chrono::Utc::now(),
        StorageTaskDurations::default(),
    )
    .err()
    .expect("negative warning counts must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn export_output_rejects_a_negative_warning_count() {
    let error = export_output_builder()
        .warning_state(-1, false)
        .try_build()
        .err()
        .expect("negative warning counts must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn export_output_rejects_missing_content() {
    let error = export_output_builder()
        .output(None, None)
        .try_build()
        .err()
        .expect("missing export output must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn export_output_rejects_multiple_content_representations() {
    let error = export_output_builder()
        .output(
            Some(serde_json::json!({})),
            Some("duplicate output".to_string()),
        )
        .try_build()
        .err()
        .expect("ambiguous export output must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn backup_output_summary_rejects_a_negative_size() {
    let error = StorageBackupOutputSummary::try_new(
        TaskId::new(91_000).unwrap(),
        -1,
        "0000000000000000000000000000000000000000000000000000000000000000",
        chrono::Utc::now(),
    )
    .err()
    .expect("negative backup output size must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn backup_output_summary_rejects_a_malformed_digest() {
    let error = StorageBackupOutputSummary::try_new(
        TaskId::new(91_000).unwrap(),
        0,
        "not-a-digest",
        chrono::Utc::now(),
    )
    .err()
    .expect("malformed backup output digest must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn backup_output_rejects_a_size_that_disagrees_with_the_document() {
    let error = StorageBackupOutput::try_new(
        TaskId::new(91_000).unwrap(),
        b"{}".to_vec(),
        3,
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
        chrono::Utc::now(),
        chrono::Utc::now(),
    )
    .err()
    .expect("mismatched backup output size must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn backup_output_rejects_a_digest_that_disagrees_with_the_document() {
    let error = StorageBackupOutput::try_new(
        TaskId::new(91_000).unwrap(),
        b"{}".to_vec(),
        2,
        "0000000000000000000000000000000000000000000000000000000000000000",
        chrono::Utc::now(),
        chrono::Utc::now(),
    )
    .err()
    .expect("mismatched backup output digest must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn task_durations_reject_negative_phases() {
    for durations in [(-1, 0, 0, 0), (0, -1, 0, 0), (0, 0, -1, 0), (0, 0, 0, -1)] {
        let error =
            StorageTaskDurations::try_new(durations.0, durations.1, durations.2, durations.3)
                .expect_err("negative task durations must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}

#[test]
fn task_create_rejects_negative_total_items() {
    let error = StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        PrincipalId::new(91_001).unwrap(),
        serde_json::json!({}),
        -1,
    )
    .try_build(1)
    .expect_err("negative task totals must be rejected");

    assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
}

#[test]
fn task_create_rejects_zero_capacity() {
    let error = StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        PrincipalId::new(91_001).unwrap(),
        serde_json::json!({}),
        0,
    )
    .try_build(0)
    .expect_err("zero task capacity must be rejected");

    assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
}

#[test]
fn task_create_and_projection_preserve_only_a_typed_trace_link() {
    let link =
        TraceLink::new("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", 1, 0).unwrap();
    let request = StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        PrincipalId::new(91_001).unwrap(),
        serde_json::json!({}),
        0,
    )
    .trace_link(Some(link.clone()))
    .try_build(1)
    .unwrap();
    assert_eq!(request.trace_link(), Some(&link));

    let now = chrono::Utc::now();
    let task = StorageTask::builder(
        TaskId::new(91_002).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Queued,
        now,
        now,
    )
    .trace_link(Some(link.clone()))
    .try_build()
    .unwrap();
    assert_eq!(task.trace_link(), Some(&link));
    assert!(!format!("{task:?}").contains(link.trace_id()));
}

#[test]
fn task_progress_rejects_negative_persisted_counts() {
    for counts in [(-1, 0, 0, 0), (0, -1, 0, 0), (0, 0, -1, 0), (0, 0, 0, -1)] {
        let error = StorageTaskProgress::try_new(counts.0, counts.1, counts.2, counts.3)
            .expect_err("negative persisted progress must be rejected");

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}

#[test]
fn task_projection_rejects_negative_attempt_count() {
    let now = chrono::Utc::now();
    let error = StorageTask::builder(
        TaskId::new(91_010).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Queued,
        now,
        now,
    )
    .attempt_count(-1)
    .try_build()
    .expect_err("a negative persisted attempt count must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn task_projection_rejects_reversed_timestamps() {
    let created_at = chrono::Utc::now();
    let earlier = created_at - chrono::Duration::seconds(1);
    let error = StorageTask::builder(
        TaskId::new(91_011).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Queued,
        created_at,
        earlier,
    )
    .try_build()
    .expect_err("a reversed persisted task interval must be rejected");

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[rstest::rstest]
#[case(StorageTaskKind::Import, 0, false)]
#[case(StorageTaskKind::Import, 1, false)]
#[case(StorageTaskKind::SchemaValidation, 0, false)]
#[case(StorageTaskKind::SchemaValidation, 1, true)]
#[case(StorageTaskKind::Reindex, 0, false)]
#[case(StorageTaskKind::Reindex, 1, true)]
fn queued_task_start_timestamp_requires_a_resumable_attempt(
    #[case] kind: StorageTaskKind,
    #[case] attempt_count: i32,
    #[case] valid: bool,
) {
    let now = chrono::Utc::now();
    let result = StorageTask::builder(
        TaskId::new(91_012).unwrap(),
        kind,
        StorageTaskStatus::Queued,
        now,
        now,
    )
    .started_at(Some(now))
    .attempt_count(attempt_count)
    .try_build();

    assert_eq!(result.is_ok(), valid);
}

#[test]
fn task_projection_rejects_a_stop_reason_on_success() {
    let now = chrono::Utc::now();
    let mut control = crate::StorageTaskControl::new(StorageTaskKind::Import);
    control.request_cancellation(crate::StorageTaskCancellation::new(now, None, None));
    control.acknowledge_stop(now).unwrap();
    let error = StorageTask::builder(
        TaskId::new(91_014).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Succeeded,
        now,
        now,
    )
    .finished_at(Some(now))
    .control(control)
    .try_build()
    .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn active_task_projection_requires_a_start_timestamp_and_lease() {
    let now = chrono::Utc::now();
    let error = StorageTask::builder(
        TaskId::new(91_013).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Running,
        now,
        now,
    )
    .try_build()
    .unwrap_err();

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn terminal_task_projection_requires_a_finish_timestamp() {
    let now = chrono::Utc::now();
    let error = StorageTask::builder(
        TaskId::new(91_014).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Succeeded,
        now,
        now,
    )
    .try_build()
    .unwrap_err();

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn terminal_task_projection_rejects_a_lease() {
    let now = chrono::Utc::now();
    let error = StorageTask::builder(
        TaskId::new(91_015).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Succeeded,
        now,
        now,
    )
    .finished_at(Some(now))
    .lease_expires_at(Some(now))
    .try_build()
    .unwrap_err();

    assert_eq!(
        error.kind(),
        crate::StorageValidationErrorKind::InvalidValue
    );
}

#[test]
fn cancelled_task_projection_allows_cancellation_before_claim() {
    let now = chrono::Utc::now();
    let task = StorageTask::builder(
        TaskId::new(91_016).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Cancelled,
        now,
        now,
    )
    .finished_at(Some(now))
    .try_build();

    assert!(task.is_ok());
}

#[test]
fn task_debug_redacts_identity_payload_and_scope() {
    let now = chrono::Utc::now();
    let task = StorageTask::builder(
        TaskId::new(91_001).unwrap(),
        StorageTaskKind::Import,
        StorageTaskStatus::Running,
        now,
        now,
    )
    .submitted_by(Some(PrincipalId::new(91_002).unwrap()))
    .idempotency_key(Some("secret idempotency".to_string()))
    .request_hash(Some("secret hash".to_string()))
    .request_payload(Some(serde_json::json!({"secret": "payload"})))
    .summary(Some("summary".to_string()))
    .progress(StorageTaskProgress::try_new(2, 1, 1, 0).unwrap())
    .scope_snapshot(StorageTaskScopeSnapshot::new(
        Some(TokenId::new(91_003).unwrap()),
        true,
        serde_json::json!({"permissions": ["secret"]}),
    ))
    .started_at(Some(now))
    .lease_expires_at(Some(now))
    .attempt_count(1)
    .initiator_principal_id(Some(PrincipalId::new(91_004).unwrap()))
    .try_build()
    .unwrap();

    let debug = format!("{task:?}");

    for secret in [
        "91001",
        "91002",
        "91003",
        "91004",
        "secret idempotency",
        "secret hash",
        "secret\": \"payload",
        "permissions\": [\"secret",
    ] {
        assert!(!debug.contains(secret));
    }
    assert!(debug.contains("Import"));
    assert!(debug.contains("Running"));
    assert!(debug.contains("has_lease: true"));
}
