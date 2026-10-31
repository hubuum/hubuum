//! Real backup decoding and cohort synthesis must not bypass retained authority.

use hubuum_domain::{JsonSchemaLimits, MaintenanceState, TaskIdentity};
use hubuum_storage_core::{
    OperationalStateStorage, RestoreStorage, StorageBackupBudget,
    StorageBackupHistorySection as History, StorageBackupStateSection as State,
    StorageExportTaskArtifact, StorageExportTaskArtifactContent, StorageIdentityEvidence,
    StorageRestoreArtifact, StorageRestoreDocumentMetadata, StorageRestoreInitiator,
    StorageRestoreJobStatus, StorageRestoreReplayApply, StorageRestoreStageArtifact,
    StorageTaskOutputLookup,
};
use rstest::rstest;
use serde_json::Value;

use super::*;
use crate::models::HISTORICAL_BACKUP_MANIFEST_EXCLUSIONS;
use crate::restores::replay;
use crate::services::deletion_recovery::{reapply, require_runtime_permit};

const BACKUP_BYTES: usize = 16 * 1024 * 1024;

async fn backup(fixture: &Fixture, legacy: bool, history: bool) -> Vec<u8> {
    let document = replay::capture(
        &fixture.storage,
        history,
        StorageBackupBudget::new(BACKUP_BYTES, 10000).unwrap(),
    )
    .await
    .unwrap();
    let mut document = serde_json::to_value(document).unwrap();
    if legacy {
        document["backup_version"] = json!(10);
        document["manifest"]["exclusions"] = json!(HISTORICAL_BACKUP_MANIFEST_EXCLUSIONS);
        // A genuine pre-identity row has no UUID evidence. Remove only backup
        // envelope fields, never similarly named keys in user payloads.
        for section in ["state", "history"] {
            if let Some(sections) = document
                .get_mut(section)
                .and_then(|value| value.get_mut("sections"))
                .and_then(Value::as_object_mut)
            {
                for rows in sections.values_mut() {
                    for row in rows.as_array_mut().unwrap() {
                        let fields = row.as_object_mut().unwrap();
                        fields.remove("logical_identity");
                        fields.remove("retention_policy_revision");
                        fields.remove("details_expires_at");
                        fields.remove("recovery_unavailable");
                    }
                }
            }
        }
    }
    serde_json::to_vec(&document).unwrap()
}

struct ReplayedIdentities {
    owner: PrincipalIdentity,
    task: Option<TaskIdentity>,
}

async fn restore(fixture: &Fixture, bytes: &[u8], task: TaskId) -> ReplayedIdentities {
    // Exercise the same bounded parser and legacy preview validation used by
    // production staging, then bind the final identities to a persisted job.
    let (document, _, _) = replay::parse(bytes, BACKUP_BYTES, JsonSchemaLimits::default()).unwrap();
    let stage = fixture
        .storage
        .stage_restore_artifact(
            StorageRestoreStageArtifact::try_new(
                StorageRestoreInitiator::try_new(Some(fixture.actor), "local", "retention-owner")
                    .unwrap(),
                StorageRestoreArtifact::from_reader(bytes, BACKUP_BYTES).unwrap(),
                "c".repeat(64),
                serde_json::to_value(document.summary().unwrap()).unwrap(),
                Utc::now() + chrono::Duration::hours(1),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let job = stage.summary().id();
    fixture
        .storage
        .start_restore_draining(job.into())
        .await
        .unwrap();
    let context = fixture
        .storage
        .get_restore_artifact(job)
        .await
        .unwrap()
        .identity_context()
        .unwrap();
    let request = StorageRestoreReplayApply::at_restore_boundary(
        context,
        StorageRestoreDocumentMetadata::new(
            document.metadata.backup_version,
            document.metadata.created_at,
            document.metadata.source_version,
        ),
        document.snapshot,
    )
    .unwrap();
    let (_, _, prepared, _) = request.clone().into_parts();
    let principal = prepared
        .replay()
        .state_rows(State::Principals)
        .map(Result::unwrap)
        .find(|row| row.get("id").and_then(Value::as_i64) == Some(i64::from(fixture.actor.id())))
        .unwrap();
    let evidence: StorageIdentityEvidence =
        serde_json::from_value(principal.get("logical_identity").unwrap().clone()).unwrap();
    let Some(StorageIdentitySubject::Principal { identity }) = evidence.subject() else {
        panic!("restored principal evidence");
    };
    let owner = identity.known_identity().unwrap();
    let task = prepared
        .replay()
        .history_rows(History::TerminalTasks)
        .map(Result::unwrap)
        .find(|row| row.get("id").and_then(Value::as_i64) == Some(i64::from(task.id())))
        .map(|row| {
            let evidence: StorageIdentityEvidence =
                serde_json::from_value(row.get("logical_identity").unwrap().clone()).unwrap();
            let Some(StorageIdentitySubject::Task { identity }) = evidence.subject() else {
                panic!("restored task evidence");
            };
            identity.known_identity().unwrap()
        });
    fixture.storage.apply_restore_replay(request).await.unwrap();
    assert_eq!(
        fixture
            .storage
            .get_restore_status(job)
            .await
            .unwrap()
            .summary()
            .status(),
        StorageRestoreJobStatus::Succeeded
    );
    assert_eq!(
        fixture.storage.get_maintenance_state().await.unwrap(),
        MaintenanceState::RecoveryQuarantine
    );
    ReplayedIdentities { owner, task }
}

async fn unkeyed_export(fixture: &Fixture) -> (TaskId, TaskIdentity) {
    let StorageTaskSubmissionOutcome::CreatedReady(task) = fixture
        .storage
        .create_task(
            StorageTaskCreateRequest::builder(StorageTaskKind::Export, fixture.actor, json!({}), 1)
                .try_build()
                .unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("unkeyed task must be immediately ready");
    };
    let evidence = fixture
        .storage
        .resolve_logical_identity(StorageIdentityTarget::Task(task.id()))
        .await
        .unwrap();
    let Some(StorageIdentitySubject::Task { identity }) = evidence.subject() else {
        panic!("original task evidence");
    };
    let identity = identity.known_identity().unwrap();
    let claim = fixture
        .storage
        .claim_next_task(
            StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(60000).unwrap(),
            )
            .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::Export]).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.task().id(), task.id());
    let output = StorageExportTaskArtifact::builder(
        "application/json",
        StorageExportTaskArtifactContent::Json(json!({"retained_secret":"old output"})),
        json!({}),
        json!([]),
        Utc::now() - chrono::Duration::seconds(1),
    )
    .try_build()
    .unwrap();
    fixture
        .storage
        .complete_task(StorageTaskCompletion::new(
            StorageTaskTerminalUpdate::new(
                claim.lease().clone(),
                StorageTaskTerminalStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            ),
            StorageTaskEventInput::new("succeeded", "Export fixture complete"),
            StorageTaskCompletionPayload::Export(output),
        ))
        .await
        .unwrap();
    (task.id(), identity)
}

async fn reapply_and_check_exposure(fixture: &Fixture, legacy: bool) {
    let result = reapply(
        &fixture.storage,
        &fixture.root.join("repository"),
        &fixture.root.join("key"),
    )
    .await;
    if legacy {
        assert!(matches!(result, Err(ApiError::ServiceUnavailable(_))));
    } else {
        assert!(result.unwrap().application_quarantine_released);
    }
    let options = serde_json::from_value(json!({
        "directory": fixture.root.join("repository"),
        "key": fixture.root.join("key"),
    }))
    .unwrap();
    assert_eq!(
        require_runtime_permit(&fixture.storage, &options)
            .await
            .is_ok(),
        !legacy
    );
    assert_eq!(
        fixture.storage.get_maintenance_state().await.unwrap(),
        if legacy {
            MaintenanceState::RecoveryQuarantine
        } else {
            MaintenanceState::Normal
        }
    );
}

#[rstest]
#[case::legacy_details(true, TaskPurgeScope::TaskDetails)]
#[case::legacy_output(true, TaskPurgeScope::ExpiredOutputs)]
#[case::original_details(false, TaskPurgeScope::TaskDetails)]
#[case::original_output(false, TaskPurgeScope::ExpiredOutputs)]
#[actix_web::test]
async fn replayed_backup_cannot_reidentify_a_retained_purge_target(
    #[case] legacy: bool,
    #[case] scope: TaskPurgeScope,
) {
    let fixture = Fixture::new(
        (scope == TaskPurgeScope::TaskDetails).then_some(Duration::ZERO),
        TaskPurgeBudget::default(),
    )
    .await;
    let (task, identity) = unkeyed_export(&fixture).await;
    let bytes = backup(&fixture, legacy, true).await;
    let source: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        source["history"]["sections"]["export_outputs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    let retained = fixture.storage.read_task_control().await.unwrap();
    assert!(
        retained.receipts().is_empty(),
        "isolate task identity from owner receipt checks"
    );
    assert_eq!(retained.purge_batches().len(), 1);
    let purge = retained.purge_batches().values().next().unwrap();
    assert_eq!(purge.ticket().manifest().scope(), scope);
    assert_eq!(purge.ticket().manifest().tasks(), &[identity]);
    assert!(purge.removed().is_some());

    let restored = restore(&fixture, &bytes, task).await;
    assert_eq!(restored.task == Some(identity), !legacy);
    reapply_and_check_exposure(&fixture, legacy).await;
    if !legacy {
        assert!(matches!(
            fixture.storage.get_export_output(task).await.unwrap(),
            StorageTaskOutputLookup::Missing
        ));
        assert_eq!(
            fixture.storage.get_task_access(task).await.is_ok(),
            scope == TaskPurgeScope::ExpiredOutputs
        );
    }
}

#[rstest]
#[case::legacy_owner(true)]
#[case::original_owner(false)]
#[actix_web::test]
async fn history_free_replay_cannot_open_a_new_namespace_for_a_retained_receipt(
    #[case] legacy: bool,
) {
    let fixture = Fixture::new(None, TaskPurgeBudget::default()).await;
    let (ticket, task) = fixture
        .complete("same-key", StorageTaskCompletionPayload::Import)
        .await;
    fixture
        .service
        .sweep_task_retention(&fixture.storage)
        .await
        .unwrap();
    let bytes = backup(&fixture, legacy, false).await;
    let retained = fixture.storage.read_task_control().await.unwrap();
    assert!(
        retained.purge_batches().is_empty(),
        "isolate receipt-owner ambiguity from purge checks"
    );
    assert!(
        retained
            .receipt(ticket.receipt().submission())
            .unwrap()
            .expires_at()
            .unwrap()
            > Utc::now()
    );

    let restored = restore(&fixture, &bytes, task).await;
    assert!(
        restored.task.is_none(),
        "history-free source cannot trigger a task UUID guard"
    );
    assert_eq!(restored.owner == fixture.owner, !legacy);
    reapply_and_check_exposure(&fixture, legacy).await;
    if !legacy {
        let binding = fixture
            .service
            .prepare_submission(
                restored.owner,
                &IdempotencyKey::new("same-key").unwrap(),
                StorageTaskKind::Import,
                "fixture.submit",
                &json!({}),
                None,
            )
            .unwrap()
            .unwrap();
        let resolved = fixture
            .service
            .resolve_submission(&fixture.storage, binding.lookup().clone())
            .await
            .unwrap();
        let TaskSubmissionResolution::DetailsExpired(expired) = resolved else {
            panic!("retained original owner/key must still recognize its missing task");
        };
        assert_eq!(expired.task(), ticket.receipt().task());
    }
}
