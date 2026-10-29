use hubuum_storage_core::{StorageTaskClaimRequest, TaskWorkerId};
use hubuum::tests::asserts::expect_legacy_task;
use std::panic::{AssertUnwindSafe, resume_unwind};

use futures_util::FutureExt;
use hubuum::events::EventContext;
use hubuum_query::QueryOptions;
use hubuum_storage_core::artifacts::ArtifactStore;
use hubuum_task_core::IdempotencyKey;
use tokio::task::JoinSet;

use super::*;

// Shared policy changes and restore replace deployment-wide state. Use the
// runner's isolated restore database and its existing serialization boundary.
pub(super) async fn with_fixture(
    kind: StorageBackendKind,
    test: impl AsyncFnOnce(&RestoreContractFixture),
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 8, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let fixture = RestoreContractFixture::new(kind, pool).await.unwrap();
    let original = fixture.storage().task_policy().await.unwrap();
    set_policy(fixture.storage(), TaskAdmissionPolicy::default()).await;
    // The shared recovery fixture creates computed-field repair work. Stop
    // those setup tasks before measuring this scenario's admissions and claims.
    let pending = fixture
        .storage()
        .list_tasks(StorageTaskListQuery::new(
            None,
            None,
            Some(StorageTaskStatus::Queued),
            QueryOptions::new(vec![], vec![], Some(100), None, true).unwrap(),
        ))
        .await
        .unwrap()
        .into_parts()
        .0;
    for task in pending {
        fixture
            .storage()
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                &task,
                EventContext::system(),
            ))
            .await
            .unwrap();
    }
    let result = AssertUnwindSafe(test(&fixture)).catch_unwind().await;
    set_policy(fixture.storage(), original.policy().clone()).await;
    fixture.restore_original().await.unwrap();
    // Logical restore preserves physical artifact cleanup obligations. Drain
    // the fixture's retired copies before another test installs a tiny quota.
    while fixture.storage().cleanup().await.unwrap() > 0 {}
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}

pub(super) async fn set_policy(
    storage: &impl TaskCapacityStorage,
    policy: TaskAdmissionPolicy,
) -> StorageTaskPolicy {
    let current = storage.task_policy().await.unwrap();
    storage
        .replace_task_policy(StorageTaskPolicyUpdate::new(current.revision(), policy))
        .await
        .unwrap()
}

fn scope(outstanding: u64, executing: u64) -> TaskScopeCapacity {
    TaskScopeCapacity::new(
        TaskCapacityLimit::new(outstanding).unwrap(),
        TaskCapacityLimit::new(executing).unwrap(),
        TaskByteLimit::new(1024 * 1024 * 1024).unwrap(),
    )
}

#[rstest]
#[case::deployment(TaskCapacityRejection::Deployment)]
#[case::principal(TaskCapacityRejection::Principal)]
#[case::kind(TaskCapacityRejection::Kind)]
#[case::principal_kind(TaskCapacityRejection::PrincipalKind)]
#[case::collection(TaskCapacityRejection::Collection)]
#[tokio::test]
async fn replicas_atomically_enforce_each_admission_boundary(
    #[case] boundary: TaskCapacityRejection,
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let mut policy = TaskAdmissionPolicy::default();
        let limits = scope(2, 16);
        policy = match boundary {
            TaskCapacityRejection::Deployment => policy.with_deployment(limits),
            TaskCapacityRejection::Principal => policy.with_principal(limits),
            TaskCapacityRejection::Kind => policy.with_kind(TaskKindCapacity::new(
                StorageTaskKind::Export,
                limits,
                TaskCapacityLimit::new(100).unwrap(),
            )),
            TaskCapacityRejection::PrincipalKind => {
                let limits = policy.for_kind(StorageTaskKind::Export).capacity();
                policy.with_kind(TaskKindCapacity::new(
                    StorageTaskKind::Export,
                    limits,
                    TaskCapacityLimit::new(2).unwrap(),
                ))
            }
            TaskCapacityRejection::Collection => policy.with_collection(limits),
            _ => unreachable!(),
        };
        let committed = set_policy(storage, policy).await;
        let class = fixture.create_class("capacity_target").await.unwrap();
        let metadata = StorageTaskMetadata::new(TaskMetadataDetails::Export {
            scope_kind: Some(TaskExportScopeKind::ObjectsInClass),
            target: Some(TaskExplicitTarget::Class {
                class_id: class.id(),
            }),
            template_id: None,
            missing_data_policy: None,
            max_items: None,
            max_output_bytes: None,
            warning_count: None,
            truncated: None,
            output: TaskOutputMetadata::NotProduced,
        })
        .unwrap();
        let mut replicas = JoinSet::new();
        for index in 0..16 {
            let storage = storage.clone();
            let request = fixture
                .task_request(StorageTaskKind::Export)
                .metadata(Some(metadata.clone()))
                .try_build()
                .unwrap();
            let expected = committed.clone();
            replicas.spawn(async move {
                // A replica's old or different startup settings cannot change
                // the policy that another replica already committed.
                let proposed = TaskAdmissionPolicy::default().with_deployment(scope(index + 1, 16));
                assert_eq!(
                    storage.initialize_task_policy(proposed).await.unwrap(),
                    expected
                );
                storage.create_task(request).await.map(expect_legacy_task)
            });
        }
        let mut accepted = 0;
        while let Some(result) = replicas.join_next().await {
            match result.unwrap() {
                Ok(_) => accepted += 1,
                Err(error) => assert_eq!(error.task_capacity_rejection(), Some(boundary)),
            }
        }
        assert_eq!(accepted, 2);
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .outstanding(),
            2
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn concurrent_idempotent_duplicates_charge_one_reservation(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(1, 1)),
        )
        .await;
        let mut replicas = JoinSet::new();
        for _ in 0..12 {
            let storage = storage.clone();
            let request = fixture
                .task_request(StorageTaskKind::Import)
                .idempotency_key(Some(IdempotencyKey::new("admission-duplicate").unwrap()))
                .request_hash(Some("same-request".into()))
                .try_build()
                .unwrap();
            replicas.spawn(async move {
                storage
                    .create_task(request)
                    .await
                    .map(expect_legacy_task)
                    .unwrap()
                    .id()
            });
        }
        let mut ids = BTreeSet::new();
        while let Some(result) = replicas.join_next().await {
            ids.insert(result.unwrap());
        }
        assert_eq!(ids.len(), 1);
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .outstanding(),
            1
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn reduced_outstanding_limits_allow_existing_work_to_drain(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        for _ in 0..3 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(1, 1)),
        )
        .await;
        assert!(matches!(
            fixture.create_task(StorageTaskKind::Import).await,
            Err(hubuum::errors::ApiError::TaskCapacity(
                TaskCapacityRejection::Deployment
            ))
        ));
        for _ in 0..3 {
            let claim = storage
                .claim_next_task(StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
                ))
                .await
                .unwrap()
                .unwrap();
            storage
                .complete_task(StorageTaskCompletion::new(
                    StorageTaskTerminalUpdate::new(
                        claim.lease().clone(),
                        StorageTaskTerminalStatus::Succeeded,
                        StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                    ),
                    StorageTaskEventInput::new("succeeded", "capacity drain"),
                    StorageTaskCompletionPayload::Import,
                ))
                .await
                .unwrap();
        }
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .outstanding(),
            1
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn cancellation_releases_admission_for_the_next_request(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(1, 1)),
        )
        .await;
        let task = storage
            .create_task(
                fixture
                    .task_request(StorageTaskKind::Import)
                    .try_build()
                    .unwrap(),
            )
            .await
            .map(expect_legacy_task)
            .unwrap();
        storage
            .request_task_cancellation(StorageTaskCancellationRequest::new(
                &task,
                EventContext::system(),
            ))
            .await
            .unwrap();
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .outstanding(),
            1
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn occupied_execution_slots_are_separate_from_queued_admission(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(20, 2)),
        )
        .await;
        for _ in 0..8 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        let mut replicas = JoinSet::new();
        for _ in 0..8 {
            let storage = storage.clone();
            replicas.spawn(async move {
                storage
                    .claim_next_task(StorageTaskClaimRequest::new(
                        TaskWorkerId::generate(),
                        StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
                    ))
                    .await
                    .unwrap()
            });
        }
        let mut claimed = 0;
        while let Some(result) = replicas.join_next().await {
            claimed += usize::from(result.unwrap().is_some());
        }
        assert_eq!(claimed, 2);
        let usage = storage.task_capacity_snapshot().await.unwrap().ordinary();
        assert_eq!((usage.outstanding(), usage.executing()), (8, 2));
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn full_ordinary_admission_preserves_internal_repair_capacity(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(1, 1)),
        )
        .await;
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        fixture.create_task(StorageTaskKind::Reindex).await.unwrap();
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .internal()
                .outstanding(),
            1
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn stale_policy_revision_cannot_replace_a_committed_change(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let before = storage.task_policy().await.unwrap();
        let after = set_policy(
            storage,
            before.policy().clone().with_deployment(scope(7, 2)),
        )
        .await;
        let result = storage
            .replace_task_policy(StorageTaskPolicyUpdate::new(
                before.revision(),
                before.policy().clone(),
            ))
            .await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        assert_eq!(storage.task_policy().await.unwrap(), after);
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn older_restore_preserves_policy_and_work_for_absent_owners(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let older = fixture.capture(true).await.unwrap();
        let committed = set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(scope(3, 1)),
        )
        .await;
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let claim = storage
            .claim_next_task(StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        let before = storage.task_capacity_snapshot().await.unwrap();
        fixture.restore(older.clone()).await.unwrap();
        let after = storage.task_capacity_snapshot().await.unwrap();
        assert_eq!(after.policy(), &committed);
        assert_eq!(after.ordinary().outstanding(), 0);
        assert_eq!(after.ordinary().executing(), 0);
        assert_eq!(
            after.ordinary().cumulative_input_bytes(),
            before.ordinary().cumulative_input_bytes()
        );
        assert!(
            !storage
                .renew_task_lease(
                    claim.lease().clone(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
                )
                .await
                .unwrap()
        );
        // Restore creates fresh repair work using the discarded integer ID.
        let replacement = storage
            .get_task_access(claim.task().id())
            .await
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(
            replacement.id(),
            claim.task().id(),
            "restore fixture must reuse the compatibility integer"
        );
        assert!(
            !storage
                .renew_task_lease(
                    claim.lease().clone(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
                )
                .await
                .unwrap()
        );
        fixture.restore(older).await.unwrap();
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .cumulative_input_bytes(),
            before.ordinary().cumulative_input_bytes()
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn failed_output_publication_keeps_the_claim_and_accounting_unchanged(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let limited = TaskScopeCapacity::new(
            TaskCapacityLimit::new(20).unwrap(),
            TaskCapacityLimit::new(2).unwrap(),
            TaskByteLimit::new(128).unwrap(),
        );
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(limited),
        )
        .await;
        fixture.create_task(StorageTaskKind::Export).await.unwrap();
        let claim = storage
            .claim_next_task(StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
        let result = storage
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    claim.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "too large"),
                export_payload(StorageExportTaskArtifactContent::Text("x".repeat(256))),
            ))
            .await;
        assert_eq!(
            result.unwrap_err().task_capacity_rejection(),
            Some(TaskCapacityRejection::ResidentBytes)
        );
        assert_eq!(
            storage.task_capacity_snapshot().await.unwrap().ordinary(),
            before
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn request_resident_budget_rejection_leaves_no_reservation(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let limited = TaskScopeCapacity::new(
            TaskCapacityLimit::new(20).unwrap(),
            TaskCapacityLimit::new(2).unwrap(),
            TaskByteLimit::new(1).unwrap(),
        );
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_deployment(limited),
        )
        .await;
        let before = storage.task_capacity_snapshot().await.unwrap().ordinary();
        let result = storage
            .create_task(
                fixture
                    .task_request(StorageTaskKind::Import)
                    .try_build()
                    .unwrap(),
            )
            .await
            .map(expect_legacy_task);
        assert_eq!(
            result.unwrap_err().task_capacity_rejection(),
            Some(TaskCapacityRejection::ResidentBytes)
        );
        assert_eq!(
            storage.task_capacity_snapshot().await.unwrap().ordinary(),
            before
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn recovered_repair_attempts_preserve_admission_and_add_cumulative_work(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let before = storage
            .task_capacity_snapshot()
            .await
            .unwrap()
            .internal()
            .cumulative_input_bytes();
        let class = fixture.create_class("retry_capacity").await.unwrap();
        let revision = storage
            .stage_schema_revision(StorageSchemaStage::new(
                class.collection_id(),
                class.id(),
                StorageValidatedSchemaPolicy::try_new_with_limits(
                    StorageClassSchemaPolicy::try_from_parts(Some(json!(false)), true).unwrap(),
                    Default::default(),
                )
                .unwrap(),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let _ = storage
            .request_schema_work(StorageSchemaWorkRequest::new(
                class.collection_id(),
                revision.reference(),
                StorageSchemaWorkKind::Impact,
                EventContext::system(),
            ))
            .await
            .unwrap();
        let first = storage
            .claim_next_task(StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(10).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        let after_first = storage
            .task_capacity_snapshot()
            .await
            .unwrap()
            .internal()
            .cumulative_input_bytes();
        tokio::time::sleep(Duration::from_millis(30)).await;
        storage.recover_expired_task_leases(100).await.unwrap();
        let second = storage
            .claim_next_task(StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.task().id(), second.task().id());
        assert!(
            !storage
                .renew_task_lease(
                    first.lease().clone(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap()
                )
                .await
                .unwrap()
        );
        let usage = storage.task_capacity_snapshot().await.unwrap().internal();
        assert_eq!((usage.outstanding(), usage.executing()), (1, 1));
        assert_eq!(
            usage.cumulative_input_bytes() - before,
            2 * (after_first - before)
        );
    })
    .await;
}

#[tokio::test]
async fn runtime_writers_cannot_enable_owner_only_reconciliation_bypass() {
    with_fixture(StorageBackendKind::Postgres, async |fixture| {
        let storage = fixture.storage();
        set_policy(storage, TaskAdmissionPolicy::default().with_deployment(scope(1, 1))).await;
        let id = fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let pool = postgres_test_pool_with_timeout(&runtime_database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        let result = with_transaction(&pool, async |connection| {
            diesel::sql_query("SELECT set_config('hubuum.reconcile_task_capacity','on',true)").execute(connection).await?;
            diesel::sql_query("INSERT INTO tasks(kind,status,submitted_by,request_payload,total_items) SELECT kind,'queued',submitted_by,'{}'::jsonb,0 FROM tasks WHERE id=$1")
                .bind::<diesel::sql_types::Integer,_>(id.id()).execute(connection).await
        }).await;
        assert_eq!(StorageError::from(result.unwrap_err()).task_capacity_rejection(), Some(TaskCapacityRejection::Deployment));
        assert_eq!(storage.task_capacity_snapshot().await.unwrap().ordinary().outstanding(), 1);
    }).await;
}

#[tokio::test]
async fn runtime_role_cannot_read_protected_capacity_owners() {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(
        &runtime_database_url(),
        2,
        DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    );
    let result = with_connection(&pool, async |connection| {
        diesel::sql_query(
            "SELECT task_identity,principal_identity FROM hubuum_control.task_accounting",
        )
        .execute(connection)
        .await
    })
    .await;
    assert!(result.is_err());
    PostgresStorage::unobserved(pool)
        .task_capacity_snapshot()
        .await
        .unwrap();
}

#[rstest]
#[case::cancellation(false)]
#[case::output_removal(true)]
#[tokio::test]
async fn releasing_capacity_does_not_hold_the_admission_lock(#[case] remove_output: bool) {
    with_fixture(StorageBackendKind::Postgres, async |fixture| {
        let task_id = if remove_output {
            let task_id = fixture.create_task(StorageTaskKind::Export).await.unwrap();
            let claim = fixture
                .storage()
                .claim_next_task(StorageTaskClaimRequest::new(
                    TaskWorkerId::generate(),
                    StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
                ))
                .await
                .unwrap()
                .unwrap();
            fixture
                .storage()
                .complete_task(StorageTaskCompletion::new(
                    StorageTaskTerminalUpdate::new(
                        claim.lease().clone(),
                        StorageTaskTerminalStatus::Succeeded,
                        StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                    ),
                    StorageTaskEventInput::new("succeeded", "retained output"),
                    export_payload(StorageExportTaskArtifactContent::Text(
                        "retained bytes".into(),
                    )),
                ))
                .await
                .unwrap();
            task_id
        } else {
            fixture.create_task(StorageTaskKind::Import).await.unwrap()
        };
        let pool = postgres_test_pool_with_timeout(
            &runtime_database_url(),
            2,
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        with_transaction(&pool, async |connection| {
            let release = if remove_output {
                "DELETE FROM export_task_outputs WHERE task_id=$1"
            } else {
                "UPDATE tasks SET status='cancelled',request_payload=NULL WHERE id=$1"
            };
            diesel::sql_query(release)
                .bind::<diesel::sql_types::Integer, _>(task_id.id())
                .execute(connection)
                .await?;
            // Cascading class deletion also needs the graph lock. A release
            // must not hold the deployment lock while waiting for that lock.
            with_transaction(&pool, async |other| {
                diesel::sql_query("SET LOCAL lock_timeout='500ms'")
                    .execute(other)
                    .await?;
                diesel::sql_query("SELECT public.hubuum_task_capacity_lock()")
                    .execute(other)
                    .await
            })
            .await
        })
        .await
        .expect("uncommitted release must not block unrelated admission");
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn expired_output_cleanup_releases_bytes_without_erasing_work(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let baseline = storage.task_capacity_snapshot().await.unwrap().ordinary();
        fixture.create_task(StorageTaskKind::Export).await.unwrap();
        let claim = storage
            .claim_next_task(StorageTaskClaimRequest::new(
                TaskWorkerId::generate(),
                StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
            ))
            .await
            .unwrap()
            .unwrap();
        let artifact = StorageExportTaskArtifact::builder(
            "text/plain",
            StorageExportTaskArtifactContent::Text("retained bytes".into()),
            json!({}),
            json!([]),
            Utc::now() + chrono::Duration::milliseconds(10),
        )
        .try_build()
        .unwrap();
        storage
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    claim.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "expiring output"),
                StorageTaskCompletionPayload::Export(artifact),
            ))
            .await
            .unwrap();
        let retained = storage.task_capacity_snapshot().await.unwrap().ordinary();
        assert!(retained.resident_bytes() > baseline.resident_bytes());
        tokio::time::sleep(Duration::from_millis(30)).await;
        storage.purge_expired_export_outputs().await.unwrap();
        let purged = storage.task_capacity_snapshot().await.unwrap().ordinary();
        assert_eq!(purged.resident_bytes(), baseline.resident_bytes());
        assert_eq!(
            purged.cumulative_output_bytes(),
            retained.cumulative_output_bytes()
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn restore_registers_required_repairs_above_reduced_limits(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let snapshot = fixture.capture(true).await.unwrap();
        let minimum = TaskScopeCapacity::new(
            TaskCapacityLimit::new(1).unwrap(),
            TaskCapacityLimit::new(1).unwrap(),
            TaskByteLimit::new(1).unwrap(),
        );
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_internal(minimum)
                .with_cumulative_input_bytes_per_task(TaskByteLimit::new(1).unwrap()),
        )
        .await;
        fixture.restore(snapshot).await.unwrap();
        let reconciled = storage.task_capacity_snapshot().await.unwrap();
        assert!(reconciled.internal().outstanding() > 0);
        assert!(reconciled.internal().resident_bytes() > 1);
        assert_eq!(reconciled.internal().executing(), 0);
        assert_eq!(
            reconciled.work_budget_blocked(),
            reconciled.internal().outstanding()
        );
        assert!(
            storage
                .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            storage
                .create_task(
                    fixture
                        .task_request(StorageTaskKind::Import)
                        .try_build()
                        .unwrap(),
                )
                .await
                .unwrap_err()
                .task_capacity_rejection(),
            Some(TaskCapacityRejection::CumulativeWork)
        );
    })
    .await;
}

#[tokio::test]
async fn missing_capacity_backfill_blocks_initialization_until_reconciled_once() {
    #[derive(diesel::QueryableByName)]
    struct Batch {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        changed: i32,
    }
    with_fixture(StorageBackendKind::Postgres, async |fixture| {
        let storage = fixture.storage();
        let id = fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let pool =
            postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        with_connection(&pool, async |connection| {
            diesel::sql_query("DELETE FROM hubuum_control.task_accounting WHERE task_id=$1")
                .bind::<diesel::sql_types::Integer, _>(id.id())
                .execute(connection)
                .await
        })
        .await
        .unwrap();
        let error = storage
            .initialize_task_policy(TaskAdmissionPolicy::default())
            .await
            .unwrap_err();
        assert_eq!(
            error.task_capacity_rejection(),
            Some(TaskCapacityRejection::ReconciliationRequired)
        );
        for expected in [1, 0] {
            let batch = with_connection(&pool, async |connection| {
                diesel::sql_query("SELECT public.hubuum_backfill_task_capacity(1) AS changed")
                    .get_result::<Batch>(connection)
                    .await
            })
            .await
            .unwrap();
            assert_eq!(batch.changed, expected);
        }
        storage
            .initialize_task_policy(TaskAdmissionPolicy::default())
            .await
            .unwrap();
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .ordinary()
                .outstanding(),
            1
        );
    })
    .await;
}
