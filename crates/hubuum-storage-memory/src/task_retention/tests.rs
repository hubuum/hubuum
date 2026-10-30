use hubuum_task_core::IdempotencyKey;
use rstest::rstest;

use super::*;

fn key_fingerprint() -> ControlKeyFingerprint {
    ControlKeyFingerprint::new("c".repeat(64)).unwrap()
}
fn checkpoint_identity() -> DeletionCheckpointIdentity {
    DeletionCheckpointIdentity::new("d".repeat(64)).unwrap()
}

async fn checkpoint(storage: &MemoryStorage) -> TaskControlCheckpoint {
    let state = storage.read_task_control().await.unwrap();
    TaskControlCheckpoint::new(
        state.lineage(),
        state.epoch(),
        state.revision(),
        checkpoint_identity(),
    )
    .unwrap()
}

async fn configure(storage: &MemoryStorage, window: u64) {
    let state = storage.read_task_control().await.unwrap();
    let next = state
        .with_activation(TaskReceiptActivation::Backfilling {
            key_fingerprint: key_fingerprint(),
        })
        .unwrap();
    storage
        .commit_task_control(TaskControlChange::new(state, next).unwrap())
        .await
        .unwrap();
    let state = storage.read_task_control().await.unwrap();
    let next = state
        .with_activation(TaskReceiptActivation::Protected {
            key_fingerprint: key_fingerprint(),
        })
        .unwrap();
    storage
        .commit_task_control(TaskControlChange::new(state, next).unwrap())
        .await
        .unwrap();
    let state = storage.read_task_control().await.unwrap();
    let policy = TaskRetentionPolicy::new(
        state.policy().revision().next().unwrap(),
        std::time::Duration::from_secs(window),
        Some(std::time::Duration::ZERO),
    )
    .unwrap();
    storage
        .replace_task_retention_policy(TaskRetentionPolicyChange::new(state.revision(), policy))
        .await
        .unwrap();
}

async fn request(
    storage: &MemoryStorage,
    key: char,
    fingerprint: char,
) -> StorageTaskCreateRequest {
    let state = storage.state.read_control().await;
    let Some(StorageIdentitySubject::Principal {
        identity: IdentityReference::Known { identity },
    }) = state
        .logical_identities
        .record(StorageIdentityTarget::Principal(
            PrincipalId::new(1).unwrap(),
        ))
        .unwrap()
        .subject()
    else {
        panic!("known fixture principal")
    };
    let lookup = TaskSubmissionLookup::new(
        TaskSubmissionIdentity::new(
            identity,
            ProtectedTaskKey::new(key.to_string().repeat(64)).unwrap(),
        ),
        StorageTaskKind::Import,
        TaskRequestFingerprint::new(fingerprint.to_string().repeat(64)).unwrap(),
    );
    StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        PrincipalId::new(1).unwrap(),
        serde_json::json!({"changes":[]}),
        1,
    )
    .idempotency_key(Some(IdempotencyKey::new(key.to_string()).unwrap()))
    .receipt_binding(Some(TaskReceiptBinding::new(lookup, key_fingerprint())))
    .try_build()
    .unwrap()
}

async fn submit(storage: &MemoryStorage, request: StorageTaskCreateRequest) -> TaskAdmissionTicket {
    let StorageTaskSubmissionOutcome::PendingProtection(ticket) =
        storage.create_task(request).await.unwrap()
    else {
        panic!("new protected admission")
    };
    ticket
}

async fn activate(storage: &MemoryStorage, ticket: TaskAdmissionTicket) -> TaskActivationOutcome {
    let proof = ProtectedTaskAdmission::new(
        ticket,
        checkpoint(storage).await,
        storage.read_application_control().await.unwrap().fence(),
    )
    .unwrap();
    storage.activate_task_submission(proof).await.unwrap()
}

async fn completed(storage: &MemoryStorage, key: char) -> (TaskAdmissionTicket, TaskId) {
    let ticket = submit(storage, request(storage, key, 'b').await).await;
    let TaskActivationOutcome::Activated(task) = activate(storage, ticket.clone()).await else {
        panic!("activation winner")
    };
    let claim = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.task().id(), task.id());
    storage
        .complete_task(StorageTaskCompletion::new(
            StorageTaskTerminalUpdate::new(
                claim.lease().clone(),
                StorageTaskTerminalStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            ),
            StorageTaskEventInput::new("succeeded", "Fixture task completed"),
            StorageTaskCompletionPayload::Import,
        ))
        .await
        .unwrap();
    (ticket, task.id())
}

async fn quarantine(storage: &MemoryStorage) -> ApplicationFence {
    let authority = storage.read_deletion_control().await.unwrap();
    storage
        .recover_application_control(
            authority.recover_from(&authority).unwrap(),
            RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap(),
        )
        .await
        .unwrap()
}

async fn reapply(storage: &MemoryStorage) -> TaskControlReapplication {
    loop {
        let request = ProtectedTaskReapplication::new(
            storage.read_task_control().await.unwrap(),
            storage.read_application_control().await.unwrap().fence(),
            checkpoint(storage).await,
        )
        .unwrap();
        match storage
            .reapply_task_retention(request, TaskPurgeBudget::default())
            .await
            .unwrap()
        {
            TaskReapplicationProgress::Complete(proof) => return proof,
            TaskReapplicationProgress::More { .. } => {}
            TaskReapplicationProgress::Blocked(block) => {
                panic!("unexpected blocked fixture: {block:?}")
            }
        }
    }
}

async fn release(storage: &MemoryStorage, tasks: TaskControlReapplication) {
    let deletion = storage
        .reapply_deletion_decisions(
            storage.read_deletion_control().await.unwrap(),
            storage.read_application_control().await.unwrap().fence(),
            checkpoint_identity(),
        )
        .await
        .unwrap();
    storage
        .release_application_exposure(
            ApplicationExposureProof::new(deletion, Some(tasks)).unwrap(),
            checkpoint_identity(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn pending_generation_reserves_once_and_is_unclaimable_until_protected_activation() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let create = request(&storage, 'a', 'b').await;
    let ticket = submit(&storage, create.clone()).await;
    assert!(
        storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(storage.state.read_control().await.tasks.len(), 1);
    assert!(
        matches!(storage.create_task(create).await.unwrap(),StorageTaskSubmissionOutcome::DuplicatePendingProtection(value) if value == ticket)
    );
    let before = storage.read_task_control().await.unwrap();
    assert!(matches!(
        activate(&storage, ticket.clone()).await,
        TaskActivationOutcome::Activated(_)
    ));
    assert_eq!(
        storage.read_task_control().await.unwrap(),
        before,
        "activation does not change signed material at the same revision"
    );
    assert!(matches!(
        activate(&storage, ticket).await,
        TaskActivationOutcome::AlreadyActive(_)
    ));
    assert_eq!(
        storage
            .state
            .read_control()
            .await
            .task_events
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        1
    );
}

#[rstest]
#[case::missing_binding(false)]
#[case::wrong_key(true)]
#[tokio::test]
async fn protected_mode_never_falls_back_to_legacy_task_rows(#[case] wrong_key: bool) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let base = request(&storage, 'a', 'b').await;
    let binding = wrong_key.then(|| {
        TaskReceiptBinding::new(
            base.receipt_binding().unwrap().lookup().clone(),
            ControlKeyFingerprint::new("e".repeat(64)).unwrap(),
        )
    });
    let create = StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        PrincipalId::new(1).unwrap(),
        serde_json::json!({}),
        1,
    )
    .idempotency_key(Some(IdempotencyKey::new("a").unwrap()))
    .receipt_binding(binding)
    .try_build()
    .unwrap();
    assert_eq!(
        storage.create_task(create).await.unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(storage.state.read_control().await.tasks.is_empty());
}

#[tokio::test]
async fn task_purge_is_bounded_restricts_reads_and_preserves_receipt_and_global_audit() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (receipt, id) = completed(&storage, 'a').await;
    let audit_count = storage.state.read_control().await.events.len();
    let ticket = storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(storage.get_task_access(id).await.is_err());
    let proof = ProtectedTaskPurge::new(ticket, checkpoint(&storage).await).unwrap();
    let mut more = 0;
    loop {
        match storage
            .purge_task_details_batch(
                proof.clone(),
                TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES).unwrap(),
            )
            .await
            .unwrap()
        {
            TaskPurgeProgress::More { rows, bytes } => {
                assert!(rows <= 1);
                assert!(bytes <= TaskPurgeBudget::MAX_BYTES);
                more += 1;
            }
            TaskPurgeProgress::Removed { rows, .. } => {
                assert!(rows <= 1);
                break;
            }
            TaskPurgeProgress::Blocked(block) => panic!("unexpected fixture block {block:?}"),
        }
    }
    assert!(more > 0);
    let state = storage.state.read_control().await;
    assert!(!state.tasks.contains_key(&id.id()));
    assert_eq!(state.events.len(), audit_count);
    assert_eq!(
        state.task_control.receipts()[0].reference(),
        receipt.receipt()
    );
}

#[tokio::test]
async fn removed_batch_reapplies_to_resurrected_exact_task_identity() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (_, id) = completed(&storage, 'a').await;
    let old = storage.state.read_control().await.tasks[&id.id()].clone();
    let ticket = storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let proof = ProtectedTaskPurge::new(ticket.clone(), checkpoint(&storage).await).unwrap();
    assert!(matches!(
        storage
            .purge_task_details_batch(proof, TaskPurgeBudget::default())
            .await
            .unwrap(),
        TaskPurgeProgress::Removed { .. }
    ));
    storage
        .state
        .write_control()
        .await
        .tasks
        .insert(id.id(), old);
    quarantine(&storage).await;
    reapply(&storage).await;
    assert!(
        !storage
            .state
            .read_control()
            .await
            .tasks
            .contains_key(&id.id())
    );
    assert!(
        storage.read_task_control().await.unwrap().purge_batches()[&ticket.manifest().id()]
            .removed()
            .is_some()
    );
}

#[rstest]
#[case::remapped_legacy_copy(true, false)]
#[case::exact_derived_target(true, true)]
#[case::unrelated_ordinary_task(false, false)]
#[tokio::test]
async fn retained_purge_requires_proven_task_correspondence(
    #[case] derived: bool,
    #[case] exact: bool,
) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (_, id) = completed(&storage, 'a').await;
    let change_identity = |state: &mut MemoryState, id: TaskId| {
        let task = state.tasks[&id.id()].clone();
        let previous = state
            .logical_identities
            .record(StorageIdentityTarget::Task(id))
            .unwrap();
        let mut bytes = *TaskIdentity::generate().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        let evidence = StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Task {
                identity: hubuum_domain::IdentityReference::Known {
                    identity: TaskIdentity::from_bytes(bytes).unwrap(),
                },
            }),
            previous.references().clone(),
        )
        .unwrap();
        state.logical_identities.restore_record(&task, evidence);
    };
    if exact {
        change_identity(&mut *storage.state.write_control().await, id);
    }
    storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    if !exact {
        let (_, other) = completed(&storage, 'b').await;
        if derived {
            change_identity(&mut *storage.state.write_control().await, other);
        }
    }
    let fence = quarantine(&storage).await;
    let request = ProtectedTaskReapplication::new(
        storage.read_task_control().await.unwrap(),
        fence,
        checkpoint(&storage).await,
    )
    .unwrap();
    let result = storage
        .reapply_task_retention(request, TaskPurgeBudget::default())
        .await
        .unwrap();
    if derived && !exact {
        assert!(matches!(
            result,
            TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)
        ));
        assert!(
            storage
                .state
                .read_control()
                .await
                .tasks
                .contains_key(&id.id())
        );
        assert!(storage.capture_application_execution(None).await.is_err());
    } else {
        assert!(matches!(result, TaskReapplicationProgress::Complete(_)));
        assert!(
            !storage
                .state
                .read_control()
                .await
                .tasks
                .contains_key(&id.id())
        );
    }
}

#[rstest]
#[case::remapped_legacy_owner(true, false)]
#[case::exact_derived_owner(true, true)]
#[case::unrelated_ordinary_principal(false, false)]
#[tokio::test]
async fn retained_receipt_requires_proven_principal_correspondence(
    #[case] derived: bool,
    #[case] exact: bool,
) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let change_identity = |state: &mut MemoryState| {
        let principal = state.principals[&1].clone();
        let previous = state
            .logical_identities
            .record(StorageIdentityTarget::Principal(principal.id()))
            .unwrap();
        let mut bytes = *PrincipalIdentity::generate().as_bytes();
        if derived {
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
        }
        let evidence = StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known {
                    identity: PrincipalIdentity::from_bytes(bytes).unwrap(),
                },
            }),
            previous.references().clone(),
        )
        .unwrap();
        state
            .logical_identities
            .restore_record(&principal, evidence);
    };
    if exact {
        change_identity(&mut *storage.state.write_control().await);
    }
    completed(&storage, 'a').await;
    if !exact {
        change_identity(&mut *storage.state.write_control().await);
    }
    let fence = quarantine(&storage).await;
    let request = ProtectedTaskReapplication::new(
        storage.read_task_control().await.unwrap(),
        fence,
        checkpoint(&storage).await,
    )
    .unwrap();
    let result = storage
        .reapply_task_retention(request, TaskPurgeBudget::default())
        .await
        .unwrap();
    if derived && !exact {
        assert!(matches!(
            result,
            TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)
        ));
        assert!(storage.capture_application_execution(None).await.is_err());
    } else {
        assert!(matches!(result, TaskReapplicationProgress::Complete(_)));
    }
}

#[rstest]
#[case::keyed(true)]
#[case::unkeyed(false)]
#[tokio::test]
async fn recovered_nonterminal_work_is_settled_without_automatic_replay(#[case] keyed: bool) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let id = if keyed {
        let ticket = submit(&storage, request(&storage, 'a', 'b').await).await;
        let TaskActivationOutcome::Activated(task) = activate(&storage, ticket).await else {
            panic!("activated")
        };
        task.id()
    } else {
        let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
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
            .unwrap()
        else {
            panic!("fresh unkeyed")
        };
        task.id()
    };
    let old_claim = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
        .await
        .unwrap()
        .unwrap();
    quarantine(&storage).await;
    let proof = reapply(&storage).await;
    release(&storage, proof).await;
    assert_eq!(
        storage
            .get_task_access(id)
            .await
            .unwrap()
            .into_parts()
            .0
            .status(),
        StorageTaskStatus::Failed
    );
    assert!(
        storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !storage
            .renew_task_lease(
                old_claim.lease().clone(),
                StorageTaskLeaseDuration::from_milliseconds(60000).unwrap()
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn hold_release_requires_a_checkpoint_newer_than_the_original_purge_intent() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (receipt, _) = completed(&storage, 'a').await;
    let ticket = storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let old = ProtectedTaskPurge::new(ticket.clone(), checkpoint(&storage).await).unwrap();
    let hold = TaskRetentionHold::new(
        TaskHoldId::generate(),
        receipt.receipt().task(),
        Utc::now(),
        None,
        TaskHoldReason::Operator,
    )
    .unwrap();
    storage
        .change_task_hold(TaskHoldChange::new(
            storage.read_task_control().await.unwrap().revision(),
            hold.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(
        storage
            .purge_task_details_batch(old.clone(), TaskPurgeBudget::default())
            .await
            .unwrap(),
        TaskPurgeProgress::Blocked(TaskPurgeBlock::Held)
    );
    storage
        .change_task_hold(TaskHoldChange::new(
            storage.read_task_control().await.unwrap().revision(),
            hold.release(Utc::now()).unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(
        storage
            .purge_task_details_batch(old, TaskPurgeBudget::default())
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    let current = ProtectedTaskPurge::new(ticket, checkpoint(&storage).await).unwrap();
    assert!(matches!(
        storage
            .purge_task_details_batch(current, TaskPurgeBudget::default())
            .await
            .unwrap(),
        TaskPurgeProgress::Removed { .. }
    ));
}

#[tokio::test]
async fn output_only_retention_works_when_task_details_retention_is_disabled() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let state = storage.read_task_control().await.unwrap();
    storage
        .replace_task_retention_policy(TaskRetentionPolicyChange::new(
            state.revision(),
            TaskRetentionPolicy::new(
                state.policy().revision().next().unwrap(),
                std::time::Duration::from_secs(3600),
                None,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::SchemaProfiling,
                PrincipalId::new(1).unwrap(),
                serde_json::json!({}),
                1,
            )
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("new unkeyed profile")
    };
    let claimed = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
        .await
        .unwrap()
        .unwrap();
    storage
        .complete_task(StorageTaskCompletion::new(
            StorageTaskTerminalUpdate::new(
                claimed.lease().clone(),
                StorageTaskTerminalStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            ),
            StorageTaskEventInput::new("succeeded", "Profile completed"),
            StorageTaskCompletionPayload::SchemaProfiling(
                StorageSchemaProfileArtifact::new(
                    serde_json::json!({"profile":"retained detail"}),
                    Utc::now() - Duration::seconds(1),
                )
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    let selection = TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap();
    assert!(
        storage
            .prepare_task_details_purge(selection)
            .await
            .unwrap()
            .is_none()
    );
    let ticket = storage
        .prepare_expired_task_outputs(selection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ticket.manifest().scope(), TaskPurgeScope::ExpiredOutputs);
    assert!(storage.get_task_access(task.id()).await.is_ok());
    assert!(matches!(
        storage.get_schema_profile_output(task.id()).await.unwrap(),
        StorageTaskOutputLookup::Missing
    ));
    let result = storage
        .purge_task_details_batch(
            ProtectedTaskPurge::new(ticket, checkpoint(&storage).await).unwrap(),
            TaskPurgeBudget::default(),
        )
        .await
        .unwrap();
    assert!(matches!(result, TaskPurgeProgress::Removed { rows: 1, .. }));
    assert!(storage.get_task_access(task.id()).await.is_ok());
    assert!(
        !storage
            .state
            .read_control()
            .await
            .schema_profile_outputs
            .contains_key(&task.id().id())
    );
}

#[tokio::test]
async fn late_hold_does_not_starve_other_members_of_the_same_manifest() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (held, held_id) = completed(&storage, 'a').await;
    let (_, free_id) = completed(&storage, 'b').await;
    let ticket = storage
        .prepare_task_details_purge(
            TaskPurgeSelection::new(64, TaskPurgeBudget::default()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let state = storage.read_task_control().await.unwrap();
    storage
        .change_task_hold(TaskHoldChange::new(
            state.revision(),
            TaskRetentionHold::new(
                TaskHoldId::generate(),
                held.receipt().task(),
                Utc::now(),
                None,
                TaskHoldReason::Operator,
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let result = storage
        .purge_task_details_batch(
            ProtectedTaskPurge::new(ticket, checkpoint(&storage).await).unwrap(),
            TaskPurgeBudget::default(),
        )
        .await
        .unwrap();
    assert!(matches!(result, TaskPurgeProgress::More { rows: 1.., .. }));
    let state = storage.state.read_control().await;
    assert!(state.tasks.contains_key(&held_id.id()));
    assert!(!state.tasks.contains_key(&free_id.id()));
}

#[tokio::test]
async fn logical_task_backup_preserves_fixed_policy_anchor() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let (_, id) = completed(&storage, 'a').await;
    let original = storage.state.read_control().await;
    let snapshot = crate::backup::capture(
        &original,
        true,
        storage.schema_limits,
        StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
    )
    .unwrap();
    let restored =
        crate::backup::restore_without_repairs(snapshot, Default::default(), Default::default())
            .unwrap();
    assert_eq!(
        restored.tasks[&id.id()].retention_policy,
        original.tasks[&id.id()].retention_policy
    );
    assert_eq!(
        restored.tasks[&id.id()].details_expires_at,
        original.tasks[&id.id()].details_expires_at
    );
}

#[tokio::test]
async fn shared_protected_task_admission_contract() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    hubuum_storage_conformance::verify_protected_task_admission(
        &storage,
        request(&storage, 'a', 'b').await,
        checkpoint_identity(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn recovery_settles_protected_admissions_missing_from_logical_restore() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let ticket = submit(&storage, request(&storage, 'a', 'b').await).await;
    storage.state.write_control().await.tasks.clear();
    quarantine(&storage).await;
    reapply(&storage).await;
    let authority = storage.read_task_control().await.unwrap();
    let receipt = authority.receipt(ticket.receipt().submission()).unwrap();
    assert!(matches!(
        receipt.outcome(),
        TaskReceiptOutcome::RecoveryUnknown { .. }
    ));
    assert!(receipt.expires_at().is_some());
    assert!(storage.state.read_control().await.tasks.is_empty());
}

#[tokio::test]
async fn bounded_legacy_backfill_preserves_original_owner_key_and_terminal_anchor() {
    let storage = MemoryStorage::new();
    let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                PrincipalId::new(1).unwrap(),
                serde_json::json!({}),
                1,
            )
            .idempotency_key(Some(IdempotencyKey::new("legacy").unwrap()))
            .request_hash(Some("a".repeat(64)))
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("legacy task")
    };
    let claimed = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
        .await
        .unwrap()
        .unwrap();
    storage
        .fail_task(StorageTaskFailure::new(
            claimed.lease().clone(),
            "legacy terminal",
            StorageTaskEventInput::new("failed", "legacy terminal"),
        ))
        .await
        .unwrap();
    let candidates = storage
        .list_legacy_task_receipts(TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES).unwrap())
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].task().id(), task.id());
    assert_eq!(candidates[0].task().idempotency_key(), Some("legacy"));
    assert_eq!(
        candidates[0].task().request_hash(),
        Some("a".repeat(64).as_str())
    );
    let anchor = candidates[0].task().finished_at().unwrap();
    let before = storage.read_task_control().await.unwrap();
    let next = before
        .with_activation(TaskReceiptActivation::Backfilling {
            key_fingerprint: key_fingerprint(),
        })
        .unwrap();
    storage
        .commit_task_control(TaskControlChange::new(before, next).unwrap())
        .await
        .unwrap();
    let identity = TaskSubmissionIdentity::new(
        candidates[0].principal(),
        ProtectedTaskKey::new("a".repeat(64)).unwrap(),
    );
    storage
        .backfill_task_receipts(
            storage.read_task_control().await.unwrap().revision(),
            vec![TaskLegacyReceiptBinding::new(
                candidates[0].identity(),
                identity.clone(),
                TaskFingerprint::LegacyV1(TaskRequestFingerprint::new("b".repeat(64)).unwrap()),
            )],
        )
        .await
        .unwrap();
    let authority = storage.read_task_control().await.unwrap();
    let receipt = authority.receipt(&identity).unwrap();
    assert_eq!(receipt.reference().task(), candidates[0].identity());
    assert_eq!(receipt.outcome().terminal_at(), Some(anchor));
    assert!(receipt.details_expires_at().is_none());
    assert!(
        storage
            .list_legacy_task_receipts(TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn internal_principal_cancellation_receipt_catchup_preserves_original_completion() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let ticket = submit(&storage, request(&storage, 'a', 'b').await).await;
    let TaskActivationOutcome::Activated(task) = activate(&storage, ticket.clone()).await else {
        panic!("activated")
    };
    storage
        .state
        .write_control()
        .await
        .cancel_queued_tasks_for_principal(PrincipalId::new(1).unwrap())
        .unwrap();
    let finished = storage.state.read_control().await.tasks[&task.id().id()]
        .finished_at
        .unwrap();
    storage.recover_expired_task_leases(1).await.unwrap();
    let control = storage.read_task_control().await.unwrap();
    let receipt = control.receipt(ticket.receipt().submission()).unwrap();
    assert!(
        matches!(receipt.outcome(), TaskReceiptOutcome::Terminal { status: StorageTaskTerminalStatus::Cancelled, finished_at, .. } if *finished_at == finished)
    );
    assert_eq!(receipt.expires_at(), Some(finished + Duration::hours(1)));
    assert!(
        storage
            .recover_expired_task_leases(1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recovery_settles_computed_rebuild_dependency_before_acknowledgement() {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Reindex,
                PrincipalId::new(1).unwrap(),
                serde_json::json!({}),
                1,
            )
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("new rebuild")
    };
    let class = ClassId::new(1).unwrap();
    let now = Utc::now();
    storage
        .state
        .write_control()
        .await
        .computation_states
        .insert(
            class.id(),
            StorageClassComputationState::try_new(
                class,
                StorageComputationRevision::try_new(7).unwrap(),
                StorageComputationRebuildState::Rebuilding {
                    active_task_id: task.id(),
                },
                now,
                now,
            )
            .unwrap(),
        );
    quarantine(&storage).await;
    let budget = TaskPurgeBudget::new(1, TaskPurgeBudget::MAX_BYTES).unwrap();
    let request = ProtectedTaskReapplication::new(
        storage.read_task_control().await.unwrap(),
        storage.read_application_control().await.unwrap().fence(),
        checkpoint(&storage).await,
    )
    .unwrap();
    assert!(matches!(
        storage
            .reapply_task_retention(request, budget)
            .await
            .unwrap(),
        TaskReapplicationProgress::More { rows: 1, .. }
    ));
    {
        let state = storage.state.read_control().await;
        assert_eq!(
            state.tasks[&task.id().id()].status,
            StorageTaskStatus::Failed
        );
        assert_eq!(
            state.computation_states[&class.id()].active_task_id(),
            Some(task.id())
        );
        assert!(state.task_reapplication.is_none());
    }
    let request = ProtectedTaskReapplication::new(
        storage.read_task_control().await.unwrap(),
        storage.read_application_control().await.unwrap().fence(),
        checkpoint(&storage).await,
    )
    .unwrap();
    assert!(matches!(
        storage
            .reapply_task_retention(request, budget)
            .await
            .unwrap(),
        TaskReapplicationProgress::Complete(_)
    ));
    let state = storage.state.read_control().await;
    let computation = &state.computation_states[&class.id()];
    assert_eq!(computation.active_task_id(), None);
    assert_eq!(
        computation.rebuild_status(),
        StorageComputationRebuildStatus::Failed
    );
    assert_eq!(computation.evaluation_revision().get(), 7);
}

#[rstest]
#[case::details(TaskPurgeScope::TaskDetails)]
#[case::outputs(TaskPurgeScope::ExpiredOutputs)]
#[tokio::test]
async fn purge_selection_byte_limit_rejects_without_recording_intent(
    #[case] scope: TaskPurgeScope,
) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let StorageTaskSubmissionOutcome::CreatedReady(task) = storage
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::SchemaProfiling,
                PrincipalId::new(1).unwrap(),
                serde_json::json!({}),
                1,
            )
            .try_build()
            .unwrap(),
        )
        .await
        .unwrap()
    else {
        panic!("new profile")
    };
    let claimed = storage
        .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60000).unwrap())
        .await
        .unwrap()
        .unwrap();
    storage
        .complete_task(StorageTaskCompletion::new(
            StorageTaskTerminalUpdate::new(
                claimed.lease().clone(),
                StorageTaskTerminalStatus::Succeeded,
                StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
            ),
            StorageTaskEventInput::new("succeeded", "Profile completed"),
            StorageTaskCompletionPayload::SchemaProfiling(
                StorageSchemaProfileArtifact::new(
                    serde_json::json!({}),
                    Utc::now() - Duration::seconds(1),
                )
                .unwrap(),
            ),
        ))
        .await
        .unwrap();
    let before = storage.read_task_control().await.unwrap();
    let selection = TaskPurgeSelection::new(1, TaskPurgeBudget::new(1, 1).unwrap()).unwrap();
    assert_eq!(
        storage
            .prepare_task_purge_scope(selection, scope)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(storage.read_task_control().await.unwrap(), before);
    assert!(storage.get_task_access(task.id()).await.is_ok());
    assert!(
        storage
            .prepare_task_purge_scope(
                TaskPurgeSelection::new(1, TaskPurgeBudget::default()).unwrap(),
                scope,
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[rstest]
#[case::zero(0, false)]
#[case::maximum(64, true)]
#[case::over_maximum(65, false)]
#[tokio::test]
async fn pending_purge_listing_limit_matches_postgres(#[case] limit: u32, #[case] accepted: bool) {
    let storage = MemoryStorage::new();
    let result = storage.list_pending_task_purges(limit).await;
    if accepted {
        assert!(result.unwrap().is_empty());
    } else {
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::InvalidInput);
    }
}

async fn add_activation_initiator(storage: &MemoryStorage) -> PrincipalId {
    let id = PrincipalId::new(2).unwrap();
    let now = Utc::now();
    let principal = StoragePrincipal::builder(
        StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).unwrap(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .unwrap(),
        PrincipalKind::Human,
        "activation-initiator",
        IdentityScopeId::new(1).unwrap(),
    )
    .try_build()
    .unwrap();
    storage
        .state
        .write_control()
        .await
        .store_principal(id.id(), principal);
    id
}

async fn remove_or_replace_activation_principal(
    storage: &MemoryStorage,
    id: PrincipalId,
    replace: bool,
) {
    let mut state = storage.state.write_control().await;
    if replace {
        let principal = state.principals[&id.id()].clone();
        let previous = state
            .logical_identities
            .record(StorageIdentityTarget::Principal(id))
            .unwrap();
        let evidence = StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Principal {
                identity: IdentityReference::Known {
                    identity: PrincipalIdentity::generate(),
                },
            }),
            previous.references().clone(),
        )
        .unwrap();
        state
            .logical_identities
            .restore_record(&principal, evidence);
    } else {
        state.principals.remove(&id.id());
    }
}

async fn assert_pending_activation_rejects_changed_principal(
    storage: &MemoryStorage,
    ticket: TaskAdmissionTicket,
) {
    let before = storage.read_task_control().await.unwrap();
    let events = storage.state.read_control().await.events.len();
    let request = ProtectedTaskAdmission::new(
        ticket,
        checkpoint(storage).await,
        storage.read_application_control().await.unwrap().fence(),
    )
    .unwrap();
    assert_eq!(
        storage
            .activate_task_submission(request)
            .await
            .unwrap_err()
            .kind(),
        StorageErrorKind::Conflict
    );
    assert_eq!(storage.read_task_control().await.unwrap(), before);
    let state = storage.state.read_control().await;
    assert_eq!(state.events.len(), events);
    assert!(
        state
            .tasks
            .values()
            .all(|task| { task.receipt_admission.as_ref().unwrap().activated.is_none() })
    );
}

#[rstest]
#[case::removed_submitter(false, false)]
#[case::reused_submitter(false, true)]
#[case::removed_distinct_initiator(true, false)]
#[case::reused_distinct_initiator(true, true)]
#[tokio::test]
async fn pending_admission_preserves_original_principal_provenance(
    #[case] initiator: bool,
    #[case] replace: bool,
) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let principal = if initiator {
        add_activation_initiator(&storage).await
    } else {
        PrincipalId::new(1).unwrap()
    };
    let ticket = submit(&storage, request(&storage, 'a', 'b').await).await;
    if initiator {
        let mut state = storage.state.write_control().await;
        let mut task = state.tasks.values().next().unwrap().clone();
        task.initiator_principal_id = Some(principal);
        state.store_task(task.id.id(), task).unwrap();
    }
    remove_or_replace_activation_principal(&storage, principal, replace).await;
    assert_pending_activation_rejects_changed_principal(&storage, ticket).await;
}

#[rstest]
#[case::removed(false)]
#[case::reused(true)]
#[tokio::test]
async fn pending_invocation_preserves_its_distinct_initiator(#[case] replace: bool) {
    let storage = MemoryStorage::new();
    configure(&storage, 3600).await;
    let initiator = add_activation_initiator(&storage).await;
    let target = storage
        .create_remote_target(StorageRemoteTargetCreate::new(
            CollectionId::new(1).unwrap(),
            "activation-target",
            StorageRemoteTargetDefinition::new(
                "",
                StorageRemoteTargetTransport::try_new(
                    StorageRemoteTargetHttpMethod::Post,
                    "https://example.invalid/",
                    serde_json::json!({}),
                    None,
                    serde_json::json!({"type":"none"}),
                    1000,
                )
                .unwrap(),
                StorageRemoteTargetPolicy::try_new(
                    None,
                    vec![StorageRemoteTargetSubjectType::Collection],
                    true,
                )
                .unwrap(),
            ),
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value();
    let base = request(&storage, 'a', 'b').await;
    let lookup = TaskSubmissionLookup::new(
        base.receipt_binding().unwrap().lookup().identity().clone(),
        StorageTaskKind::RemoteCall,
        TaskRequestFingerprint::new("b".repeat(64)).unwrap(),
    );
    let create = StorageTaskCreateRequest::builder(
        StorageTaskKind::RemoteCall,
        PrincipalId::new(1).unwrap(),
        serde_json::json!({}),
        1,
    )
    .idempotency_key(Some(IdempotencyKey::new("a").unwrap()))
    .receipt_binding(Some(TaskReceiptBinding::new(lookup, key_fingerprint())))
    .remote_invocation(Some(StorageTaskRemoteInvocation::new(
        RemoteTargetId::new(target.metadata().id().id()).unwrap(),
        StorageRemoteTargetSubjectType::Collection,
        ResourceId::new(1).unwrap(),
        EventContext::from_mutation(MutationProvenance::user_for_task(
            PrincipalId::new(1).unwrap(),
            Some(initiator),
            TaskId::new(1).unwrap(),
        )),
    )))
    .try_build()
    .unwrap();
    let ticket = submit(&storage, create).await;
    remove_or_replace_activation_principal(&storage, initiator, replace).await;
    assert_pending_activation_rejects_changed_principal(&storage, ticket).await;
}
