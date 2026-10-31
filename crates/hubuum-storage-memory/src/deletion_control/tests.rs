use hubuum_storage_core::{DeletionCheckpointIdentity, DeletionCopyState, StorageRestoreArtifact};
use rstest::rstest;

use super::*;

async fn fixture(closed: bool) -> (MemoryStorage, HotObjectHistoryDeletion, MemoryHistoryEntry) {
    let storage = MemoryStorage::new();
    let class = storage
        .create_class(
            StorageClassCreate::builder("history", CollectionId::new(1).unwrap(), "").build(),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    let class = storage
        .resolve_class(StorageClassSelector::Id(class.id()))
        .await
        .unwrap();
    let object = storage
        .create_object(
            &class,
            StorageObjectCreate::new(
                "object",
                class.class().collection_id(),
                class.class().id(),
                serde_json::json!({"value": "original"}),
                "",
            ),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    if closed {
        let resolved = storage
            .resolve_object(StorageObjectSelector::Ids {
                class_id: object.class_id(),
                object_id: object.id(),
            })
            .await
            .unwrap();
        storage
            .update_object(
                &resolved,
                StorageObjectUpdate::builder()
                    .description(Some("new version".into()))
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    let entry = storage
        .state
        .read()
        .await
        .history
        .iter()
        .find(|entry| matches!(&entry.value, MemoryHistoryValue::Object(value) if value.id() == object.id()))
        .unwrap()
        .clone();
    let target = entry
        .logical_identity
        .subject()
        .unwrap()
        .record_identity()
        .unwrap();
    let request = HotObjectHistoryDeletion::new(Uuid::new_v4(), Uuid::new_v4(), target).unwrap();
    (storage, request, entry)
}

async fn prepare(
    storage: &MemoryStorage,
    request: &HotObjectHistoryDeletion,
) -> DeletionControlState {
    storage
        .prepare_hot_object_history_deletion(
            storage.read_deletion_control().await.unwrap(),
            request.clone(),
        )
        .await
        .unwrap()
}

fn cleanup(
    expected: DeletionControlState,
    request: HotObjectHistoryDeletion,
) -> HotObjectHistoryCleanup {
    HotObjectHistoryCleanup::new(
        expected,
        request,
        DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn competing_control_changes_cannot_both_commit() {
    let storage = MemoryStorage::new();
    let initial = storage.capture_deletion_control().await.unwrap();
    let subject = DeletionSubject::Version {
        identity: VersionIdentity::generate(),
    };
    let first = initial
        .state()
        .record_intent(Uuid::new_v4(), subject.clone(), vec![Uuid::new_v4()])
        .unwrap();
    let second = initial
        .state()
        .record_intent(Uuid::new_v4(), subject, vec![Uuid::new_v4()])
        .unwrap();
    let (a, b) = tokio::join!(
        storage.commit_deletion_control(first),
        storage.commit_deletion_control(second)
    );
    assert_ne!(a.is_ok(), b.is_ok());
}

#[rstest]
#[case::open("open")]
#[case::mismatched_resource("resource")]
#[case::duplicate_version("duplicate")]
#[tokio::test]
async fn preparation_rejects_invalid_history_copy(#[case] invalid: &str) {
    let (storage, mut request, entry) = fixture(invalid != "open").await;
    match invalid {
        "resource" => {
            request = HotObjectHistoryDeletion::new(
                request.intent(),
                request.copy(),
                StorageRecordIdentity::new(
                    ResourceIdentity::generate(),
                    request.target().version(),
                ),
            )
            .unwrap()
        }
        "duplicate" => storage.state.write().await.history.push(entry),
        "open" => (),
        _ => unreachable!(),
    }
    let expected = storage.read_deletion_control().await.unwrap();
    let error = storage
        .prepare_hot_object_history_deletion(expected.clone(), request)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert_eq!(storage.read_deletion_control().await.unwrap(), expected);
}

#[tokio::test]
async fn cleanup_commits_exact_removal_and_receipt_together() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    let completed = storage
        .cleanup_hot_object_history(cleanup(prepared, request.clone()))
        .await
        .unwrap();
    let state = storage.state.read().await;
    assert!(!state.history.iter().any(|row| row.id == entry.id));
    let intent = &completed.intents()[&request.intent()];
    assert_eq!(intent.copies()[&request.copy()], DeletionCopyState::Removed);
    assert_eq!(
        intent.receipts()[&request.copy()].checkpoint().as_str(),
        "a".repeat(64)
    );
}

#[tokio::test]
async fn cleanup_refuses_reopened_copy_without_recording_removal() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    storage
        .state
        .write()
        .await
        .history
        .iter_mut()
        .find(|row| row.id == entry.id)
        .unwrap()
        .valid_to = None;
    let error = storage
        .cleanup_hot_object_history(cleanup(prepared.clone(), request))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert_eq!(storage.read_deletion_control().await.unwrap(), prepared);
}

#[tokio::test]
async fn preparing_removed_copy_retries_existing_scope() {
    let (storage, request, _) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    let completed = storage
        .cleanup_hot_object_history(cleanup(prepared, request.clone()))
        .await
        .unwrap();
    assert_eq!(prepare(&storage, &request).await, completed);
}

#[tokio::test]
async fn cleanup_retry_rechecks_removed_history_after_recovery() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    let completed = storage
        .cleanup_hot_object_history(cleanup(prepared, request.clone()))
        .await
        .unwrap();
    // Model application data recovered independently of retained control evidence.
    storage.state.write().await.history.push(entry.clone());
    let retried = storage
        .cleanup_hot_object_history(cleanup(completed.clone(), request))
        .await
        .unwrap();
    assert!(
        !storage
            .state
            .read()
            .await
            .history
            .iter()
            .any(|row| row.id == entry.id)
    );
    assert_eq!(retried, completed);
}

#[tokio::test]
async fn cleanup_does_not_confuse_reused_integer_ids() {
    let (storage, request, entry) = fixture(true).await;
    let MemoryHistoryValue::Object(original) = &entry.value else {
        unreachable!()
    };
    let target = storage
        .resolve_object(StorageObjectSelector::Ids {
            class_id: original.class_id(),
            object_id: original.id(),
        })
        .await
        .unwrap();
    storage
        .delete_object(&target, &EventContext::system())
        .await
        .unwrap()
        .into_value();
    storage.state.write().await.next_object_id = original.id().id();
    let class = storage
        .resolve_class(StorageClassSelector::Id(original.class_id()))
        .await
        .unwrap();
    let replacement = storage
        .create_object(
            &class,
            StorageObjectCreate::new(
                "replacement",
                original.collection_id(),
                original.class_id(),
                serde_json::json!({"keep": true}),
                "",
            ),
            &EventContext::system(),
        )
        .await
        .unwrap()
        .into_value();
    assert_eq!(replacement.id(), original.id());
    let prepared = prepare(&storage, &request).await;
    storage
        .cleanup_hot_object_history(cleanup(prepared, request))
        .await
        .unwrap();
    let state = storage.state.read().await;
    assert_eq!(state.objects[&replacement.id().id()], replacement);
    assert!(state.history.iter().any(
        |row| matches!(&row.value, MemoryHistoryValue::Object(value) if value == &replacement)
    ));
}

#[tokio::test]
async fn stale_cleanup_cannot_remove_history() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    let change = prepared
        .record_intent(
            Uuid::new_v4(),
            DeletionSubject::Version {
                identity: VersionIdentity::generate(),
            },
            vec![Uuid::new_v4()],
        )
        .unwrap();
    storage.commit_deletion_control(change).await.unwrap();
    let error = storage
        .cleanup_hot_object_history(cleanup(prepared, request))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert!(
        storage
            .state
            .read()
            .await
            .history
            .iter()
            .any(|row| row.id == entry.id)
    );
}

#[rstest]
#[case::prepare(false)]
#[case::cleanup(true)]
#[tokio::test]
async fn registered_runtime_blocks_history_deletion(#[case] cleaning: bool) {
    let (storage, request, _) = fixture(true).await;
    let expected = if cleaning {
        let expected = prepare(&storage, &request).await;
        // A retained registration recovered alongside authority must still block.
        let registration = RuntimeRegistration::generate();
        storage
            .state
            .write()
            .await
            .runtime_registrations
            .insert(registration.id(), registration.ownership_token());
        expected
    } else {
        storage.register_runtime().await.unwrap();
        storage.read_deletion_control().await.unwrap()
    };
    let result = if cleaning {
        storage
            .cleanup_hot_object_history(cleanup(expected, request))
            .await
    } else {
        storage
            .prepare_hot_object_history_deletion(expected, request)
            .await
    };
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}

#[tokio::test]
async fn runtime_registration_rejects_quarantine() {
    let (storage, request, _) = fixture(true).await;
    prepare(&storage, &request).await;
    assert_eq!(
        storage.register_runtime().await.unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
}

#[tokio::test]
async fn runtime_registration_capacity_never_evicts_an_owner() {
    let storage = MemoryStorage::new();
    for _ in 0..DELETION_CONTROL_MAX_RUNTIMES {
        let registration = RuntimeRegistration::generate();
        storage
            .state
            .write()
            .await
            .runtime_registrations
            .insert(registration.id(), registration.ownership_token());
    }
    assert_eq!(
        storage.register_runtime().await.unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(
        storage.state.read().await.runtime_registrations.len(),
        DELETION_CONTROL_MAX_RUNTIMES
    );
}

#[tokio::test]
async fn runtime_unregister_requires_ownership() {
    let storage = MemoryStorage::new();
    let registration = storage.register_runtime().await.unwrap();
    let id = registration.id();
    storage
        .state
        .write()
        .await
        .runtime_registrations
        .insert(id, Uuid::new_v4());
    assert_eq!(
        storage
            .unregister_runtime(registration)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
    assert!(
        storage
            .state
            .read()
            .await
            .runtime_registrations
            .contains_key(&id)
    );
}

#[tokio::test]
async fn clean_runtime_shutdown_allows_preparation() {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    storage.unregister_runtime(registration).await.unwrap();
    prepare(&storage, &request).await;
}

#[tokio::test]
async fn logical_restore_preserves_runtime_registration() {
    let storage = MemoryStorage::new();
    let registration = storage.register_runtime().await.unwrap();
    let id = registration.id();
    let replay = storage
        .capture_backup_replay(
            true,
            StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
        )
        .await
        .unwrap();
    let stage = storage
        .stage_restore_artifact(
            StorageRestoreStageArtifact::try_new(
                StorageRestoreInitiator::try_new(
                    Some(PrincipalId::new(1).unwrap()),
                    "local",
                    "admin",
                )
                .unwrap(),
                StorageRestoreArtifact::from_reader(b"{}".as_slice(), 2).unwrap(),
                "b".repeat(64),
                serde_json::json!({}),
                Utc::now() + Duration::hours(1),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let job_id = stage.summary().id();
    storage.start_restore_draining(job_id.into()).await.unwrap();
    let context = storage
        .get_restore_artifact(job_id)
        .await
        .unwrap()
        .identity_context()
        .unwrap();
    storage
        .apply_restore_replay(
            StorageRestoreReplayApply::at_restore_boundary(
                context,
                StorageRestoreDocumentMetadata::new(11, Utc::now(), "test"),
                replay,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        storage
            .state
            .read()
            .await
            .runtime_registrations
            .contains_key(&id)
    );
    storage.unregister_runtime(registration).await.unwrap();
}
