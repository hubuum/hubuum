use hubuum_storage_core::{
    DeletionAuditObservation, DeletionAuditProtection, DeletionCheckpointIdentity,
    DeletionCopyState, StorageRestoreArtifact,
};
use rstest::rstest;

use super::*;

pub(super) async fn fixture(
    closed: bool,
) -> (MemoryStorage, HotObjectHistoryDeletion, MemoryHistoryEntry) {
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
        .await.unwrap()
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

pub(super) async fn prepare(
    storage: &MemoryStorage,
    request: &HotObjectHistoryDeletion,
) -> DeletionControlState {
    let prepared = storage
        .prepare_hot_object_history_deletion(
            storage.read_deletion_control().await.unwrap(),
            request.clone(),
        )
        .await
        .unwrap();
    storage
        .observe_deletion_checkpoint(DeletionAuditObservation::new(
            prepared,
            DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            DeletionAuditProtection::Intent,
        ))
        .await
        .unwrap()
}

pub(super) fn cleanup(
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
async fn unavailable_required_audit_rolls_back_intent_and_canonical_events() {
    let (storage, request, entry) = fixture(true).await;
    let before = storage.read_deletion_control().await.unwrap();
    let before_events = storage.state.read_control().await.events.len();
    storage.state.write_control().await.next_event_sequence = i64::MAX;
    let result = storage
        .prepare_hot_object_history_deletion(before.clone(), request)
        .await;
    let state = storage.state.read_control().await;
    assert!(result.is_err());
    assert_eq!(
        (
            &state.deletion_control,
            state.events.len(),
            state.history.iter().any(|row| row.id == entry.id)
        ),
        (&before, before_events, true)
    );
}

#[tokio::test]
async fn ordinary_event_purge_preserves_protected_original_audit_facts() {
    let (storage, request, _) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    let removed = storage
        .cleanup_hot_object_history(cleanup(prepared, request.clone()))
        .await
        .unwrap();
    let completed = storage
        .observe_deletion_checkpoint(DeletionAuditObservation::new(
            removed,
            DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            DeletionAuditProtection::Completion,
        ))
        .await
        .unwrap();
    storage.state.write_control().await.events.clear();
    let recovered = storage.capture_deletion_control().await.unwrap();
    assert_eq!(
        recovered.state().intents()[&request.intent()].audit(),
        completed.intents()[&request.intent()].audit()
    );
}

#[rstest]
#[case::canonical_survives(true)]
#[case::canonical_was_purged(false)]
#[tokio::test]
async fn control_recovery_rehydrates_original_facts_without_duplicate_success(
    #[case] keep_canonical: bool,
) {
    let (storage, request, _) = fixture(true).await;
    let original = storage.read_deletion_control().await.unwrap();
    let prepared = prepare(&storage, &request).await;
    let removed = storage
        .cleanup_hot_object_history(cleanup(prepared, request))
        .await
        .unwrap();
    let completed = storage
        .observe_deletion_checkpoint(DeletionAuditObservation::new(
            removed,
            DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            DeletionAuditProtection::Completion,
        ))
        .await
        .unwrap();
    let original_facts = {
        let state = storage.state.read_control().await;
        state
            .events
            .iter()
            .map(|event| event.as_parts().0)
            .filter(|event| event.entity_type() == EntityType::Lifecycle)
            .map(|event| (event.event_id(), event.occurred_at()))
            .collect::<BTreeSet<_>>()
    };
    {
        let mut state = storage.state.write_control().await;
        state.deletion_control = original.clone();
        if !keep_canonical {
            state
                .events
                .retain(|event| event.as_parts().0.entity_type() != EntityType::Lifecycle);
        }
    }
    storage
        .commit_deletion_control(original.recover_from(&completed).unwrap())
        .await
        .unwrap();
    let state = storage.state.read_control().await;
    let recovered = state
        .events
        .iter()
        .map(|event| event.as_parts().0)
        .filter(|event| event.entity_type() == EntityType::Lifecycle)
        .map(|event| (event.event_id(), event.occurred_at()))
        .collect::<Vec<_>>();
    assert_eq!(recovered.len(), 5);
    assert_eq!(
        recovered.into_iter().collect::<BTreeSet<_>>(),
        original_facts
    );
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
        "duplicate" => storage.state.write().await.unwrap().history.push(entry),
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
    let state = storage.state.read().await.unwrap();
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
        .unwrap()
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
    storage
        .state
        .write()
        .await
        .unwrap()
        .history
        .push(entry.clone());
    let retried = storage
        .cleanup_hot_object_history(cleanup(completed.clone(), request))
        .await
        .unwrap();
    assert!(
        !storage
            .state
            .read()
            .await
            .unwrap()
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
    storage.state.write().await.unwrap().next_object_id = original.id().id();
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
    let state = storage.state.read().await.unwrap();
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
            .unwrap()
            .history
            .iter()
            .any(|row| row.id == entry.id)
    );
}

#[tokio::test]
async fn registered_runtime_blocks_cleanup_until_exact_drain() {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    let expected = prepare(&storage, &request).await;
    let error = storage
        .cleanup_hot_object_history(cleanup(expected.clone(), request.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    let fence = storage.read_application_control().await.unwrap().fence();
    storage
        .acknowledge_runtime_drain(registration.owner(), fence)
        .await
        .unwrap();
    storage
        .cleanup_hot_object_history(cleanup(expected, request))
        .await
        .unwrap();
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
            .unwrap()
            .runtime_registrations
            .insert(
                registration.id(),
                MemoryRuntimeRegistration {
                    ownership_token: registration.ownership_token(),
                    isolation: None,
                    drained: None,
                },
            );
    }
    assert_eq!(
        storage.register_runtime().await.unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(
        storage
            .state
            .read()
            .await
            .unwrap()
            .runtime_registrations
            .len(),
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
        .unwrap()
        .runtime_registrations
        .insert(
            id,
            MemoryRuntimeRegistration {
                ownership_token: Uuid::new_v4(),
                isolation: None,
                drained: None,
            },
        );
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
            .unwrap()
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
            .unwrap()
            .runtime_registrations
            .contains_key(&id)
    );
    storage.unregister_runtime(registration).await.unwrap();
}
