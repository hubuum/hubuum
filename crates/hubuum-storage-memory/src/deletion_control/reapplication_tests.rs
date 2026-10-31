use super::tests::{cleanup, fixture, prepare};
use super::*;
use hubuum_domain::IdentityReference;
use rstest::rstest;

fn checkpoint() -> DeletionCheckpointIdentity {
    DeletionCheckpointIdentity::new("b".repeat(64)).unwrap()
}

async fn reapply(storage: &MemoryStorage) -> ApplicationReapplication {
    storage
        .reapply_deletion_decisions(
            storage.read_deletion_control().await.unwrap(),
            storage.read_application_control().await.unwrap().fence(),
            checkpoint(),
        )
        .await
        .unwrap()
}

async fn release(storage: &MemoryStorage) -> ApplicationReapplication {
    let receipt = reapply(storage).await;
    storage
        .release_application_exposure(receipt.clone(), checkpoint())
        .await
        .unwrap();
    receipt
}

#[rstest]
#[case::pending(false)]
#[case::removed_resurrected(true)]
#[tokio::test]
async fn reapplication_deletes_current_copy_and_requires_published_release(
    #[case] resurrect: bool,
) {
    let (storage, request, entry) = fixture(true).await;
    let expected = prepare(&storage, &request).await;
    if resurrect {
        storage
            .cleanup_hot_object_history(cleanup(expected, request))
            .await
            .unwrap();
        storage
            .state
            .write()
            .await
            .unwrap()
            .history
            .push(entry.clone());
    }
    let receipt = reapply(&storage).await;
    assert!(
        storage
            .state
            .read()
            .await
            .unwrap()
            .history
            .iter()
            .all(|row| row.id != entry.id)
    );
    assert_eq!(
        storage.get_maintenance_state().await.unwrap(),
        MaintenanceState::RecoveryQuarantine
    );
    assert_eq!(
        storage
            .capture_deletion_control()
            .await
            .unwrap()
            .reapplication(),
        Some(&receipt)
    );
    storage
        .release_application_exposure(receipt, checkpoint())
        .await
        .unwrap();
    assert_eq!(
        storage.get_maintenance_state().await.unwrap(),
        MaintenanceState::Normal
    );
}

#[tokio::test]
async fn repeated_reapplication_preserves_exact_receipt() {
    let (storage, request, _) = fixture(true).await;
    prepare(&storage, &request).await;
    let first = reapply(&storage).await;
    assert_eq!(reapply(&storage).await, first);
}

#[tokio::test]
async fn unknown_obligation_rejects_entire_reapplication_before_removal() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    storage
        .commit_deletion_control(
            prepared
                .record_intent(
                    Uuid::new_v4(),
                    DeletionSubject::Version {
                        identity: VersionIdentity::generate(),
                    },
                    vec![Uuid::new_v4()],
                )
                .unwrap(),
        )
        .await
        .unwrap();
    let before = storage.read_deletion_control().await.unwrap();
    let fence = storage.read_application_control().await.unwrap().fence();
    assert!(
        storage
            .reapply_deletion_decisions(before.clone(), fence, checkpoint())
            .await
            .is_err()
    );
    assert_eq!(storage.read_deletion_control().await.unwrap(), before);
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

#[rstest]
#[case::with_history(false)]
#[case::history_free(true)]
#[tokio::test]
async fn reapplication_rejects_restored_live_target(#[case] history_free: bool) {
    let (storage, request, entry) = fixture(true).await;
    prepare(&storage, &request).await;
    let MemoryHistoryValue::Object(object) = &entry.value else {
        unreachable!()
    };
    {
        let mut state = storage.state.write().await.unwrap();
        state.objects.insert(object.id().id(), object.clone());
        state
            .logical_identities
            .restore_record(object, entry.logical_identity.clone());
        if history_free {
            state.history.clear();
        }
    }
    let before = storage.read_deletion_control().await.unwrap();
    let fence = storage.read_application_control().await.unwrap().fence();
    assert_eq!(
        storage
            .reapply_deletion_decisions(before, fence, checkpoint())
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
}

#[rstest]
#[case::live("live")]
#[case::open_history("open")]
#[case::closed_history("closed")]
#[tokio::test]
async fn reapplication_rejects_restore_descendants_of_a_retained_version(
    #[case] placement: &str,
    #[values(false, true)] removed: bool,
) {
    let (storage, request, mut entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    if removed {
        storage
            .cleanup_hot_object_history(cleanup(prepared, request.clone()))
            .await
            .unwrap();
    }
    entry.logical_identity = entry
        .logical_identity
        .at_restore_baseline(VersionIdentity::generate())
        .unwrap()
        .at_restore_baseline(VersionIdentity::generate())
        .unwrap();
    {
        let mut state = storage.state.write().await.unwrap();
        state.history.retain(|row| row.id != entry.id);
        match placement {
            "live" => {
                let MemoryHistoryValue::Object(object) = &entry.value else {
                    unreachable!()
                };
                state.objects.insert(object.id().id(), object.clone());
                state
                    .logical_identities
                    .restore_record(object, entry.logical_identity.clone());
            }
            "open" | "closed" => {
                if placement == "open" {
                    entry.valid_to = None;
                }
                state.history.push(entry.clone());
            }
            _ => unreachable!(),
        }
    }
    let before = storage.read_deletion_control().await.unwrap();
    let application = storage.read_application_control().await.unwrap();
    let error = storage
        .reapply_deletion_decisions(before.clone(), application.fence(), checkpoint())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert_eq!(storage.read_deletion_control().await.unwrap(), before);
    assert_eq!(
        storage.read_application_control().await.unwrap(),
        application
    );
}

#[tokio::test]
async fn exact_cleanup_cannot_acknowledge_absence_of_a_restored_descendant() {
    let (storage, request, entry) = fixture(true).await;
    let prepared = prepare(&storage, &request).await;
    {
        let mut state = storage.state.write().await.unwrap();
        let restored = state
            .history
            .iter_mut()
            .find(|row| row.id == entry.id)
            .unwrap();
        restored.logical_identity = restored
            .logical_identity
            .clone()
            .at_restore_baseline(VersionIdentity::generate())
            .unwrap();
    }
    assert_eq!(
        storage
            .cleanup_hot_object_history(cleanup(prepared.clone(), request))
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
    assert_eq!(storage.read_deletion_control().await.unwrap(), prepared);
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

#[rstest]
#[case::live(true)]
#[case::closed_history(false)]
#[tokio::test]
async fn unknown_restore_ancestry_blocks_only_its_resource(
    #[case] live: bool,
    #[values(false, true)] same_resource: bool,
) {
    let (storage, request, mut entry) = fixture(true).await;
    prepare(&storage, &request).await;
    let mut bytes = Uuid::new_v4().into_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    let legacy_baseline = VersionIdentity::from_bytes(bytes).unwrap();
    entry.logical_identity = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: if same_resource {
                    request.target().resource()
                } else {
                    ResourceIdentity::generate()
                },
            },
            version: legacy_baseline,
        }),
        entry.logical_identity.references().clone(),
    )
    .unwrap();
    {
        let mut state = storage.state.write().await.unwrap();
        state.history.retain(|row| row.id != entry.id);
        if live {
            let MemoryHistoryValue::Object(object) = &entry.value else {
                unreachable!()
            };
            state.objects.insert(object.id().id(), object.clone());
            state
                .logical_identities
                .restore_record(object, entry.logical_identity.clone());
        } else {
            state.history.push(entry);
        }
    }
    let authority = storage.read_deletion_control().await.unwrap();
    let application = storage.read_application_control().await.unwrap();
    let result = storage
        .reapply_deletion_decisions(authority.clone(), application.fence(), checkpoint())
        .await;
    if same_resource {
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        assert_eq!(storage.read_deletion_control().await.unwrap(), authority);
    } else {
        result.unwrap();
    }
}

#[tokio::test]
async fn same_version_history_capture_preserves_restore_ancestry() {
    let (storage, request, entry) = fixture(false).await;
    let MemoryHistoryValue::Object(object) = entry.value else {
        unreachable!()
    };
    let restored = entry
        .logical_identity
        .at_restore_baseline(VersionIdentity::generate())
        .unwrap();
    let mut state = storage.state.write().await.unwrap();
    state
        .logical_identities
        .restore_record(&object, restored.clone());
    let captured = state
        .logical_identities
        .history(&object, StorageHistoryOperation::Update, None);
    assert_eq!(captured.subject(), restored.subject());
    assert!(
        captured
            .restore_source_versions()
            .contains(request.target().version())
    );
}

#[rstest]
#[case::missing(None)]
#[case::foreign(Some(false))]
#[case::unowned(Some(true))]
#[tokio::test]
async fn bound_adapter_requires_its_owner_permit(#[case] supplied: Option<bool>) {
    let storage = MemoryStorage::new();
    let owner = storage.register_runtime().await.unwrap();
    let bound = storage.clone().with_runtime_owner(owner.owner());
    let result = if let Some(unowned) = supplied {
        let other = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution((!unowned).then(|| other.owner()))
            .await
            .unwrap();
        bound
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit),
                bound.get_inventory_counts(),
            )
            .await
    } else {
        bound.get_inventory_counts().await
    };
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}

#[tokio::test]
async fn old_scope_cannot_refresh_itself_after_release() {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    let bound = storage.clone().with_runtime_owner(registration.owner());
    let old = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    prepare(&storage, &request).await;
    let fence = storage.read_application_control().await.unwrap().fence();
    storage
        .acknowledge_runtime_drain(registration.owner(), fence)
        .await
        .unwrap();
    release(&storage).await;
    let fresh = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let result = bound
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(old),
            async {
                bound
                    .run_in_scope(
                        StorageExecutionScope::default().with_application_access(fresh),
                        bound.get_inventory_counts(),
                    )
                    .await
            },
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}

#[tokio::test]
async fn capturing_work_revokes_previous_drain_acknowledgement() {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    prepare(&storage, &request).await;
    let fence = storage.read_application_control().await.unwrap().fence();
    storage
        .acknowledge_runtime_drain(registration.owner(), fence)
        .await
        .unwrap();
    let receipt = reapply(&storage).await;
    storage
        .acknowledge_runtime_drain(registration.owner(), receipt.fence())
        .await
        .unwrap();
    storage
        .release_application_exposure(receipt, checkpoint())
        .await
        .unwrap();
    storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    assert_eq!(
        storage.state.read().await.unwrap().runtime_registrations[&registration.id()].drained,
        None
    );
}

#[tokio::test]
async fn released_application_rejects_runtime_drain_acknowledgement() {
    let storage = MemoryStorage::new();
    let registration = storage.register_runtime().await.unwrap();
    let fence = storage.read_application_control().await.unwrap().fence();
    assert_eq!(
        storage
            .acknowledge_runtime_drain(registration.owner(), fence)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
}

#[rstest]
#[case::unknown(None)]
#[case::different(Some(false))]
#[case::matching(Some(true))]
#[tokio::test]
async fn isolated_recovery_only_retires_matching_known_domain(#[case] kind: Option<bool>) {
    let storage = MemoryStorage::new();
    let isolation = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
    let registered = kind.map(|matches| {
        if matches {
            isolation.clone()
        } else {
            RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 3).unwrap()
        }
    });
    let owner = storage.register_runtime_isolated(registered).await.unwrap();
    let bound = storage.clone().with_runtime_owner(owner.owner());
    let old = storage
        .capture_application_execution(Some(owner.owner()))
        .await
        .unwrap();
    let authority = storage.read_deletion_control().await.unwrap();
    let before = storage.read_application_control().await.unwrap().fence();
    let result = storage
        .recover_application_control(authority.recover_from(&authority).unwrap(), isolation)
        .await;
    if kind == Some(true) {
        assert_ne!(result.unwrap().incarnation(), before.incarnation());
        release(&storage).await;
        assert!(
            bound
                .run_in_scope(
                    StorageExecutionScope::default().with_application_access(old),
                    bound.get_inventory_counts()
                )
                .await
                .is_err()
        );
    } else {
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
        assert!(
            storage
                .state
                .read()
                .await
                .unwrap()
                .runtime_registrations
                .contains_key(&owner.id())
        );
    }
}

#[tokio::test]
async fn a_new_permit_cannot_publish_an_old_task_claim() {
    let (storage, request, _) = fixture(true).await;
    storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                PrincipalId::new(1).unwrap(),
                serde_json::json!({}),
                1,
            )
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap();
    let claim = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
        .await
        .unwrap()
        .unwrap();
    prepare(&storage, &request).await;
    release(&storage).await;
    let permit = storage.capture_application_execution(None).await.unwrap();
    let error = storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit),
            storage.append_task_event(StorageTaskEventAppend::new(
                claim.lease().clone(),
                StorageTaskEventInput::new("running", "stale work"),
            )),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    storage
        .fail_task(StorageTaskFailure::new(
            claim.lease().clone(),
            "old generation",
            StorageTaskEventInput::new("failed", "old generation"),
        ))
        .await
        .unwrap();
}

#[rstest]
#[case::current_object(false)]
#[case::history_object(true)]
#[tokio::test]
async fn unresolved_identity_cannot_be_used_as_absence_evidence(#[case] history: bool) {
    let (storage, request, entry) = fixture(true).await;
    prepare(&storage, &request).await;
    {
        let mut state = storage.state.write().await.unwrap();
        let evidence = StorageIdentityEvidence::try_new(None, BTreeMap::new()).unwrap();
        if history {
            state
                .history
                .iter_mut()
                .find(|row| row.id == entry.id)
                .unwrap()
                .logical_identity = evidence;
        } else {
            let object = state.objects.values().next().unwrap().clone();
            state.logical_identities.restore_record(&object, evidence);
        }
    }
    let before = storage.read_deletion_control().await.unwrap();
    let fence = storage.read_application_control().await.unwrap().fence();
    assert_eq!(
        storage
            .reapply_deletion_decisions(before.clone(), fence, checkpoint())
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
    assert_eq!(storage.read_deletion_control().await.unwrap(), before);
}

#[rstest]
#[case::direct_write(false)]
#[case::transaction(true)]
#[tokio::test]
async fn ordinary_writes_cannot_bypass_stale_application_scope(#[case] transaction: bool) {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    let bound = storage.clone().with_runtime_owner(registration.owner());
    let permit = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    prepare(&storage, &request).await;
    let scope = StorageExecutionScope::default().with_application_access(permit);
    let result = if transaction {
        bound
            .run_in_scope(
                scope,
                bound.with_transaction(EventContext::system(), |_transaction| {
                    Box::pin(async { Ok(()) })
                }),
            )
            .await
    } else {
        bound
            .run_in_scope(
                scope,
                bound.create_class(
                    StorageClassCreate::builder(
                        "must not appear",
                        CollectionId::new(1).unwrap(),
                        "",
                    )
                    .build(),
                    &EventContext::system(),
                ),
            )
            .await
            .map(|_| ())
    };
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}

#[tokio::test]
async fn logical_restore_reapplies_retained_decisions_before_resuming() {
    use hubuum_storage_core::StorageRestoreArtifact;
    let (storage, request, entry) = fixture(true).await;
    let old_fence = storage.read_application_control().await.unwrap().fence();
    let replay = storage
        .capture_backup_replay(
            true,
            StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
        )
        .await
        .unwrap();
    prepare(&storage, &request).await;
    release(&storage).await;
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
                "c".repeat(64),
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
    let restored = storage.get_restore_artifact(job_id).await.unwrap();
    assert_eq!(
        restored.summary().status(),
        StorageRestoreJobStatus::Succeeded
    );
    assert!(restored.into_parts().1.is_none());
    storage.resume_terminal_restore(job_id).await.unwrap();
    let after = storage.read_application_control().await.unwrap();
    assert_ne!(after.fence().incarnation(), old_fence.incarnation());
    assert_eq!(
        storage.get_maintenance_state().await.unwrap(),
        MaintenanceState::RecoveryQuarantine
    );
    assert_eq!(
        storage
            .get_restore_coordinator_snapshot()
            .await
            .unwrap()
            .maintenance_state(),
        MaintenanceState::Draining
    );
    assert!(
        storage
            .state
            .read()
            .await
            .unwrap()
            .history
            .iter()
            .any(|row| row.logical_identity.subject() == entry.logical_identity.subject())
    );
    release(&storage).await;
    assert_eq!(
        storage.get_maintenance_state().await.unwrap(),
        MaintenanceState::Normal
    );
    assert!(
        storage
            .state
            .read()
            .await
            .unwrap()
            .history
            .iter()
            .all(|row| row.logical_identity.subject() != entry.logical_identity.subject())
    );
}

#[rstest]
#[case::fresh(false)]
#[case::inherited(true)]
#[tokio::test]
async fn worker_admission_observes_maintenance_and_preserves_its_permit(#[case] inherited: bool) {
    let storage = MemoryStorage::new();
    let registration = storage.register_runtime().await.unwrap();
    let owner = registration.owner();
    let bound = storage.clone().with_runtime_owner(owner.clone());
    let original = storage
        .capture_application_execution(Some(owner.clone()))
        .await
        .unwrap();
    storage.state.write_control().await.maintenance_state = MaintenanceState::Draining;
    let scope = if inherited {
        StorageExecutionScope::default().with_application_access(original.clone())
    } else {
        StorageExecutionScope::default()
    };
    let (permit, maintenance) = bound
        .run_in_scope(
            scope,
            bound.capture_application_execution_with_maintenance(Some(owner)),
        )
        .await
        .unwrap()
        .into_parts();
    assert_eq!(permit, original);
    assert_eq!(maintenance, MaintenanceState::Draining);
}

#[rstest]
#[case::foreign(false)]
#[case::unowned(true)]
#[tokio::test]
async fn worker_admission_rejects_a_foreign_inherited_permit(#[case] unowned: bool) {
    let storage = MemoryStorage::new();
    let registration = storage.register_runtime().await.unwrap();
    let other = storage.register_runtime().await.unwrap();
    let bound = storage.clone().with_runtime_owner(registration.owner());
    let permit = storage
        .capture_application_execution((!unowned).then(|| other.owner()))
        .await
        .unwrap();
    let result = bound
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit),
            bound.capture_application_execution_with_maintenance(Some(registration.owner())),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}

#[tokio::test]
async fn worker_admission_cannot_refresh_an_old_permit_after_release() {
    let (storage, request, _) = fixture(true).await;
    let registration = storage.register_runtime().await.unwrap();
    let owner = registration.owner();
    let bound = storage.clone().with_runtime_owner(owner.clone());
    let original = storage
        .capture_application_execution(Some(owner.clone()))
        .await
        .unwrap();
    prepare(&storage, &request).await;
    let fence = storage.read_application_control().await.unwrap().fence();
    storage
        .acknowledge_runtime_drain(owner.clone(), fence)
        .await
        .unwrap();
    release(&storage).await;
    let result = bound
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(original),
            bound.capture_application_execution_with_maintenance(Some(owner)),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
}
