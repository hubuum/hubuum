//! A leased online worker hands a partially removed batch to signed restore.
use super::*;
use hubuum_task_core::TaskExecutionLimit;
use std::future::ready;

async fn with_worker<T>(
    storage: &PostgresStorage,
    work: impl AsyncFnOnce(StorageExecutionScope) -> T,
) -> T {
    let registration = storage.register_history_runtime(None).await.unwrap();
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let result = AssertUnwindSafe(work(scope)).catch_unwind().await;
    storage.unregister_runtime(registration).await.unwrap();
    result.unwrap_or_else(|panic| resume_unwind(panic))
}

async fn claim_expiry_task(
    storage: &PostgresStorage,
    scope: &StorageExecutionScope,
) -> StorageTaskLease {
    let owner = storage
        .run_in_scope(
            scope.clone(),
            storage.admit_history_publication(HistoryPublicationKind::TaskOutput),
        )
        .await
        .unwrap();
    let claimed = storage
        .run_in_scope(scope.clone().with_history_read(owner.context()), async {
            let request = StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(600_000).unwrap(),
            )
            .with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::HistoryExpiry]).unwrap());
            let claim = storage
                .claim_next_task(request)
                .await?
                .ok_or_else(|| StorageError::not_found("queued expiry task"))?;
            let (_, lease) = claim.into_parts();
            storage
                .admit_task_execution(StorageTaskExecutionAdmission::new(
                    lease.clone(),
                    TaskExecutionLimit::from_milliseconds(600_000).unwrap(),
                ))
                .await?;
            Ok::<_, StorageError>(lease)
        })
        .await;
    let settled = owner
        .after_drained(ready(Ok(())), ready(Ok(())))
        .await
        .unwrap();
    storage.settle_history_publication(settled).await.unwrap();
    claimed.unwrap()
}

#[tokio::test]
async fn signed_restore_fences_interrupted_worker_and_finishes_its_original_batch() {
    with_signed_restore(StorageBackendKind::Postgres, verify_worker_handoff).await;
}

struct InterruptedWorker {
    backup: StorageBackupSnapshot,
    execution: StorageExecutionScope,
    removal: HistoryExpiryTaskRequest,
    partial: HistoryExpiryIntent,
}

async fn interrupt_after_first_removal(
    signed: &SignedRestore<'_>,
    worker: &PostgresStorage,
    key: HistoryPolicyKey,
    execution: StorageExecutionScope,
    admitted: &mut Option<AdmittedBackup>,
) -> InterruptedWorker {
    let fixture = signed.fixture;
    let storage = fixture.storage();
    let snapshot = storage.capture_history_lifecycle().await.unwrap();
    let policy = snapshot
        .control()
        .authority()
        .policies()
        .records()
        .iter()
        .find(|record| record.proposal().key() == key)
        .unwrap();
    let submission = HistoryExpiryTaskSubmission::periodic(
        HistoryLifecycleScan::new(key, 1000).unwrap(),
        storage.read_application_control().await.unwrap().fence(),
        policy,
    )
    .unwrap();
    let task = worker
        .run_in_scope(
            execution.clone(),
            worker.submit_history_expiry_task(submission),
        )
        .await
        .unwrap();
    assert!(task.created());
    // A full backup contains the closed copies and omits the queued task.
    let backup = fixture.capture(true).await.unwrap();
    let lease = claim_expiry_task(worker, &execution).await;
    assert_eq!(lease.task_id(), task.task().id());
    let checkpoint = worker
        .run_in_scope(
            execution.clone(),
            worker.process_history_expiry_task(HistoryExpiryTaskRequest::new(
                lease.clone(),
                Some(HistoryExpiryTaskAuthorization::periodic(policy.into())),
            )),
        )
        .await
        .unwrap();
    assert!(checkpoint.admitted());
    *admitted = Some(AdmittedBackup {
        batch: checkpoint.batch(),
        backup: backup.clone(),
    });
    let protection = history_replay_fixture_protection(storage, signed.repository, signed.key_file)
        .await
        .unwrap();
    let removal = HistoryExpiryTaskRequest::new(lease, None).with_protection(protection);
    worker
        .run_in_scope(
            execution.clone(),
            worker.process_history_expiry_task(removal.clone()),
        )
        .await
        .unwrap();
    let snapshot = storage.capture_history_lifecycle().await.unwrap();
    let partial = snapshot
        .control()
        .authority()
        .expiry()
        .intent(checkpoint.batch())
        .unwrap()
        .clone();
    InterruptedWorker {
        backup,
        execution,
        removal,
        partial,
    }
}

async fn verify_worker_handoff(signed: SignedRestore<'_>, admitted: &mut Option<AdmittedBackup>) {
    let SignedRestore {
        fixture,
        pool,
        trace,
        repository,
        key_file,
    } = signed;
    let storage = fixture.storage();
    let worker = PostgresStorage::unobserved(postgres_test_pool_with_timeout(
        &runtime_database_url(),
        2,
        DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    ));
    let collection = fixture
        .create_collection(&TestScope::new().scoped_name("worker_restore"), "original")
        .await
        .unwrap();
    let source = HistorySource::Collection(collection.id());
    let scope = HistoryPolicyScope::Collection {
        identity: current_identity(fixture, source.target()).await.resource(),
    };
    let key = HistoryPolicyKey::new(Family::Collection, scope).unwrap();
    apply_expiry_policy(
        fixture,
        key,
        HistoryExpiryTrigger::Age {
            age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            deleted_age_seconds: None,
        },
        PrincipalIdentity::generate(),
    )
    .await;
    let mut closed = Vec::new();
    for next in ["updated", "updated again", "live"] {
        closed.push(current_identity(fixture, source.target()).await);
        source.update(fixture, next).await;
    }

    let InterruptedWorker {
        backup,
        execution,
        removal,
        partial,
    } = with_worker(&worker, async |execution| {
        interrupt_after_first_removal(&signed, &worker, key, execution, admitted).await
    })
    .await;

    let lease = removal.lease().clone();
    let manifest = partial.manifest();
    assert_eq!(manifest.members().len(), closed.len());
    let [first] = partial.removals() else {
        panic!("one committed first removal before interruption")
    };
    let mut survivors = FamilyImage::read(backup.clone(), Family::Collection);
    survivors.remove_expected(&closed);
    let mut observer = pool.get().await.unwrap();
    for &target in &closed {
        assert!(
            manifest
                .members()
                .iter()
                .any(|member| member.target() == target)
        );
        assert_eq!(
            race::copy_present(&mut observer, Family::Collection, target).await,
            target != first.target()
        );
    }

    trace.operations.lock().unwrap().clear();
    let restore = fixture.restore_with_recovery(backup, repository, key_file);
    tokio::pin!(restore);
    tokio::select! {
        biased;
        () = trace.data_replaced.notified() => {},
        result = &mut restore => panic!("expected to observe the committed replacement: {result:?}"),
    }
    let replacement = storage.read_application_control().await.unwrap();
    assert_ne!(
        replacement.fence().incarnation(),
        lease.execution_fence().unwrap().incarnation()
    );
    assert!(!replacement.is_released_for(&storage.read_deletion_control().await.unwrap()));
    // Retry exactly the operation that previously removed a copy, while all copies exist again.
    let stale = worker
        .run_in_scope(execution, worker.process_history_expiry_task(removal))
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), StorageErrorKind::Unavailable, "{stale}");
    let quarantined = storage.capture_history_lifecycle().await.unwrap();
    assert_eq!(
        quarantined
            .control()
            .authority()
            .expiry()
            .intent(manifest.identity()),
        Some(&partial)
    );
    for &target in &closed {
        assert!(race::copy_present(&mut observer, Family::Collection, target).await);
    }
    tokio::select! {
        biased;
        () = trace.copy_reapplied.notified() => {},
        result = &mut restore => panic!("expected an intermediate replay before release: {result:?}"),
    }
    let mid_replay = storage.read_application_control().await.unwrap();
    assert!(!mid_replay.is_released_for(&storage.read_deletion_control().await.unwrap()));
    for &target in &closed {
        assert_eq!(
            race::copy_present(&mut observer, Family::Collection, target).await,
            target != first.target()
        );
    }
    restore.await.unwrap();
    trace.assert_captures(closed.len(), 1);
    let completed = storage.capture_history_lifecycle().await.unwrap();
    let intent = completed
        .control()
        .authority()
        .expiry()
        .intent(manifest.identity())
        .unwrap();
    assert_eq!(intent.manifest(), manifest);
    assert!(intent.is_complete());
    assert!(intent.removals().contains(first));
    assert_eq!(
        FamilyImage::read(fixture.capture(true).await.unwrap(), Family::Collection),
        survivors
    );
    assert!(
        storage
            .read_application_control()
            .await
            .unwrap()
            .is_released_for(&storage.read_deletion_control().await.unwrap())
    );

    // Current runtime access and signed authority do not revive the old claim.
    let protection = history_replay_fixture_protection(storage, repository, key_file)
        .await
        .unwrap();
    with_worker(&worker, async |execution| {
        let renewed = worker
            .run_in_scope(
                execution.clone(),
                worker.renew_task_lease(
                    lease.clone(),
                    StorageTaskLeaseDuration::from_milliseconds(600_000).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert!(!renewed, "a fresh runtime cannot renew the old lease");
        let stale = worker
            .run_in_scope(
                execution.clone(),
                worker.process_history_expiry_task(
                    HistoryExpiryTaskRequest::new(lease, None).with_protection(protection),
                ),
            )
            .await
            .unwrap_err();
        // Active tasks are omitted from backup. A reused numeric ID must also
        // fail the retired generation check.
        assert!(
            matches!(
                stale.kind(),
                StorageErrorKind::Conflict | StorageErrorKind::Unavailable
            ),
            "{stale}"
        );
        assert!(
            worker
                .run_in_scope(
                    execution,
                    worker.recover_history_expiry_task(manifest.identity())
                )
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
    let after = storage.capture_history_lifecycle().await.unwrap();
    assert_eq!(
        after.control().authority().expiry(),
        completed.control().authority().expiry()
    );
}
