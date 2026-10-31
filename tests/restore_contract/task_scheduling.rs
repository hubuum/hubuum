use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Text};
use hubuum::events::EventContext;
use hubuum_storage_core::*;

use super::task_capacity::{set_policy, with_fixture};
use super::*;

fn worker() -> StorageTaskClaimRequest {
    StorageTaskClaimRequest::new(
        TaskWorkerId::generate(),
        StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap(),
    )
}

async fn settle(storage: &impl TaskExecutionStorage, task: StorageTaskClaim) {
    storage
        .fail_task(StorageTaskFailure::new(
            task.lease().clone(),
            "scheduler fixture finished",
            StorageTaskEventInput::new("failed", "scheduler fixture finished"),
        ))
        .await
        .unwrap();
}

#[rstest]
#[tokio::test]
async fn every_task_class_can_be_claimed(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] backend: StorageBackendKind,
    #[values(
        StorageTaskKind::Import,
        StorageTaskKind::Export,
        StorageTaskKind::Backup,
        StorageTaskKind::RemoteCall,
        StorageTaskKind::Reindex,
        StorageTaskKind::SchemaValidation,
        StorageTaskKind::SchemaProfiling
    )]
    kind: StorageTaskKind,
) {
    with_fixture(backend, async |fixture| {
        let storage = fixture.storage();
        let task = fixture.create_task(kind).await.unwrap();
        let claim = storage.claim_next_task(worker()).await.unwrap().unwrap();
        assert_eq!(claim.task().id(), task);
        settle(storage, claim).await;
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn empty_polls_do_not_advance_durable_rotation(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let before = storage.task_capacity_snapshot().await.unwrap();
        let request = worker();
        for _ in 0..9 {
            assert!(storage.claim_next_task(request).await.unwrap().is_none());
        }
        let after = storage.task_capacity_snapshot().await.unwrap();
        assert_eq!(
            (
                after.scheduling().successful_claims(),
                after.scheduling().ordinary_position()
            ),
            (
                before.scheduling().successful_claims(),
                before.scheduling().ordinary_position()
            )
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn a_single_slot_executes_user_and_repair_work(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(TaskSchedulingPolicy::default().with_capacity(1, 1).unwrap()),
        )
        .await;
        let ids = [
            fixture.create_task(StorageTaskKind::Import).await.unwrap(),
            fixture.create_task(StorageTaskKind::Reindex).await.unwrap(),
        ];
        let mut actual = BTreeSet::new();
        let request = worker();
        for _ in 0..2 {
            let claim = storage.claim_next_task(request).await.unwrap().unwrap();
            actual.insert(claim.task().id());
            settle(storage, claim).await;
        }
        assert_eq!(actual, ids.into_iter().collect());
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn an_idle_internal_reservation_cannot_be_borrowed(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(TaskSchedulingPolicy::default().with_capacity(2, 1).unwrap()),
        )
        .await;
        let first = worker();
        let second = worker();
        storage.claim_next_task(first).await.unwrap();
        storage.claim_next_task(second).await.unwrap();
        for _ in 0..2 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        let claimed = storage.claim_next_task(first).await.unwrap().unwrap();
        assert!(storage.claim_next_task(second).await.unwrap().is_none());
        settle(storage, claimed).await;
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn a_worker_cannot_claim_twice_while_occupied(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        for _ in 0..2 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        let request = worker();
        let claim = storage.claim_next_task(request).await.unwrap().unwrap();
        assert!(storage.claim_next_task(request).await.unwrap().is_none());
        settle(storage, claim).await;
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn restricted_workers_report_unsupported_classes(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        fixture.create_task(StorageTaskKind::Export).await.unwrap();
        let request =
            worker().with_kinds(TaskWorkerKinds::new(&[StorageTaskKind::Import]).unwrap());
        assert!(storage.claim_next_task(request).await.unwrap().is_none());
        let snapshot = storage.task_capacity_snapshot().await.unwrap();
        assert!(
            snapshot
                .scheduling()
                .unsupported_kinds()
                .contains(&StorageTaskKind::Export)
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn weighted_rotation_is_shared_between_worker_replicas(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let scheduling = TaskSchedulingPolicy::default()
            .with_capacity(1, 1)
            .unwrap()
            .with_weight(TaskKindWeight::new(StorageTaskKind::Import, 3).unwrap());
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_scheduling(scheduling),
        )
        .await;
        for _ in 0..16 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
            fixture.create_task(StorageTaskKind::Export).await.unwrap();
        }
        let workers = [worker(), worker()];
        let mut imports = 0;
        for index in 0..16 {
            let claim = storage
                .claim_next_task(workers[index % 2])
                .await
                .unwrap()
                .unwrap();
            imports += usize::from(claim.task().kind() == StorageTaskKind::Import);
            settle(storage, claim).await;
        }
        assert_eq!(imports, 12);
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn a_dominant_principal_cannot_take_every_start(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(TaskSchedulingPolicy::default().with_capacity(1, 1).unwrap()),
        )
        .await;
        for _ in 0..20 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        let user = storage
            .create_user(StorageUserCreate::new(
                None,
                format!("scheduler_{}", uuid::Uuid::new_v4()),
                "fixture-password-hash",
                None,
                None,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts();
        let request = StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            user.id().into(),
            json!({}),
            1,
        )
        .try_build()
        .unwrap();
        let minority = storage.create_task(request).await.unwrap().id();
        let request = worker();
        let mut starts = Vec::new();
        for _ in 0..2 {
            let claim = storage.claim_next_task(request).await.unwrap().unwrap();
            starts.push(claim.task().id());
            settle(storage, claim).await;
        }
        assert!(starts.contains(&minority));
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn overdue_saturation_preserves_the_ordinary_lane(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let scheduling = TaskSchedulingPolicy::default()
            .with_capacity(1, 1)
            .unwrap()
            .with_age_promotion(8, 1)
            .unwrap()
            .with_weight(TaskKindWeight::new(StorageTaskKind::Import, 3).unwrap());
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_scheduling(scheduling),
        )
        .await;
        for kind in [StorageTaskKind::Import, StorageTaskKind::Export] {
            for _ in 0..40 {
                fixture.create_task(kind).await.unwrap();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let before = storage.task_capacity_snapshot().await.unwrap();
        let request = worker();
        let mut exports = 0;
        for _ in 0..32 {
            let claim = storage.claim_next_task(request).await.unwrap().unwrap();
            exports += usize::from(claim.task().kind() == StorageTaskKind::Export);
            settle(storage, claim).await;
        }
        let after = storage.task_capacity_snapshot().await.unwrap();
        assert!(
            exports >= 6
                && after.scheduling().promoted_claims() - before.scheduling().promoted_claims()
                    <= 4
        );
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn worker_expiry_enters_shared_mode_and_recovery_restores_reservations(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(TaskSchedulingPolicy::default().with_capacity(2, 1).unwrap()),
        )
        .await;
        let healthy = worker();
        let transient = StorageTaskClaimRequest::new(
            TaskWorkerId::generate(),
            StorageTaskLeaseDuration::from_milliseconds(30).unwrap(),
        );
        storage.claim_next_task(healthy).await.unwrap();
        storage.claim_next_task(transient).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let fallback = storage.task_capacity_snapshot().await.unwrap();
        // Only the initial registration should expire during the test. Keep
        // recovery live through the claim and following snapshot round trips.
        let recovery = StorageTaskClaimRequest::new(transient.worker(), healthy.lease_duration());
        storage.claim_next_task(recovery).await.unwrap();
        let recovered = storage.task_capacity_snapshot().await.unwrap();
        assert_eq!(
            (fallback.scheduling().mode(), recovered.scheduling().mode()),
            (
                TaskSchedulingMode::Shared {
                    reason: TaskSharedCapacityReason::InsufficientWorkers
                },
                TaskSchedulingMode::Reserved
            )
        );
    })
    .await;
}

#[rstest]
#[case::kind(false)]
#[case::principal(true)]
#[tokio::test]
async fn concurrent_workers_cannot_exceed_execution_caps(
    #[case] principal_cap: bool,
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let limits = TaskScopeCapacity::new(
            TaskCapacityLimit::new(100).unwrap(),
            TaskCapacityLimit::new(2).unwrap(),
            TaskByteLimit::new(1024 * 1024).unwrap(),
        );
        let policy = if principal_cap {
            TaskAdmissionPolicy::default().with_principal(limits)
        } else {
            TaskAdmissionPolicy::default().with_kind(TaskKindCapacity::new(
                StorageTaskKind::Import,
                limits,
                TaskCapacityLimit::new(100).unwrap(),
            ))
        };
        set_policy(
            storage,
            policy.with_scheduling(TaskSchedulingPolicy::default().with_capacity(8, 1).unwrap()),
        )
        .await;
        let workers: Vec<_> = (0..8).map(|_| worker()).collect();
        for request in &workers {
            storage.claim_next_task(*request).await.unwrap();
        }
        for _ in 0..12 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        let results = futures_util::future::join_all(
            workers
                .into_iter()
                .map(|request| storage.claim_next_task(request)),
        )
        .await;
        let claims: Vec<_> = results.into_iter().filter_map(Result::unwrap).collect();
        let identities: BTreeSet<_> = claims.iter().map(|claim| claim.task().id()).collect();
        assert_eq!((claims.len(), identities.len()), (2, 2));
        for claim in claims {
            settle(storage, claim).await;
        }
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn capacity_shrink_preserves_an_existing_claim(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(TaskSchedulingPolicy::default().with_capacity(2, 1).unwrap()),
        )
        .await;
        let active = worker();
        let transient = StorageTaskClaimRequest::new(
            TaskWorkerId::generate(),
            StorageTaskLeaseDuration::from_milliseconds(20).unwrap(),
        );
        storage.claim_next_task(active).await.unwrap();
        storage.claim_next_task(transient).await.unwrap();
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        fixture.create_task(StorageTaskKind::Reindex).await.unwrap();
        let claim = storage.claim_next_task(active).await.unwrap().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        // A slot which vanished cannot make an already occupied slot free.
        let blocked = storage.claim_next_task(active).await.unwrap();
        let snapshot = storage.task_capacity_snapshot().await.unwrap();
        assert!(
            blocked.is_none()
                && matches!(
                    snapshot.scheduling().mode(),
                    TaskSchedulingMode::Shared { .. }
                )
        );
        settle(storage, claim).await;
        let next = storage.claim_next_task(active).await.unwrap().unwrap();
        settle(storage, next).await;
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn promotion_interval_changes_do_not_create_adjacent_promotions(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let scheduling = TaskSchedulingPolicy::default()
            .with_capacity(1, 1)
            .unwrap()
            .with_age_promotion(2, 1)
            .unwrap();
        set_policy(
            storage,
            TaskAdmissionPolicy::default().with_scheduling(scheduling.clone()),
        )
        .await;
        for _ in 0..12 {
            fixture.create_task(StorageTaskKind::Import).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let request = worker();
        let mut previous = storage
            .task_capacity_snapshot()
            .await
            .unwrap()
            .scheduling()
            .promoted_claims();
        for _ in 0..2 {
            let claim = storage.claim_next_task(request).await.unwrap().unwrap();
            settle(storage, claim).await;
            let promoted = storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .scheduling()
                .promoted_claims();
            if promoted > previous {
                previous = promoted;
                break;
            }
        }
        set_policy(
            storage,
            TaskAdmissionPolicy::default()
                .with_scheduling(scheduling.with_age_promotion(3, 1).unwrap()),
        )
        .await;
        for _ in 0..2 {
            let claim = storage.claim_next_task(request).await.unwrap().unwrap();
            settle(storage, claim).await;
        }
        assert_eq!(
            storage
                .task_capacity_snapshot()
                .await
                .unwrap()
                .scheduling()
                .promoted_claims(),
            previous
        );
    })
    .await;
}

#[tokio::test]
async fn a_locked_oldest_task_does_not_block_the_next_candidate() {
    with_fixture(StorageBackendKind::Postgres, async |fixture| {
        let storage = fixture.storage();
        let oldest = fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let next = fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let pool = postgres_test_pool_with_timeout(
            &runtime_database_url(),
            2,
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        let claim = with_transaction(&pool, async |connection| {
            diesel::sql_query("SELECT id FROM tasks WHERE id=$1 FOR UPDATE")
                .bind::<diesel::sql_types::Integer, _>(oldest.id())
                .execute(connection)
                .await?;
            let claim = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                storage.claim_next_task(worker()),
            )
            .await
            .expect("scheduler must skip locked candidates")?
            .expect("unlocked candidate");
            Ok::<_, hubuum_storage_postgres::PostgresStorageError>(claim)
        })
        .await
        .unwrap();
        assert_eq!(claim.task().id(), next);
        settle(storage, claim).await;
    })
    .await;
}

#[derive(QueryableByName)]
struct TaskRowsVisited {
    #[diesel(sql_type = BigInt)]
    value: i64,
}

#[rstest]
#[case::new_principals(false, false, false, 1024)]
#[case::ordinary_service_history(true, false, false, 8)]
#[case::promotion_due_without_overdue_work(true, true, false, 8)]
#[case::overdue_promotion(true, true, true, 1024)]
#[tokio::test]
async fn selecting_queue_heads_does_not_scan_the_complete_backlog(
    #[case] served: bool,
    #[case] promotion_due: bool,
    #[case] overdue: bool,
    #[case] maximum_rows: i64,
) {
    with_fixture(StorageBackendKind::Postgres, async |_fixture| {
        let pool = postgres_test_pool_with_timeout(
            &database_url(),
            1,
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        with_transaction(&pool, async |connection| {
            diesel::sql_query("CREATE TEMP TABLE scheduler_plan_principals ON COMMIT DROP AS \
                WITH added AS (INSERT INTO principals(kind,name) \
                    SELECT 'service_account','scheduler-plan-' || gen_random_uuid() FROM generate_series(1,32) RETURNING id) \
                SELECT id,row_number() OVER (ORDER BY id) AS ordinal FROM added")
                .execute(connection).await?;
            diesel::sql_query("SET LOCAL hubuum.reconcile_task_capacity='on'").execute(connection).await?;
            diesel::sql_query("INSERT INTO tasks(kind,status,submitted_by,created_at) \
                SELECT CASE WHEN n%2=0 THEN 'import' ELSE 'export' END,'queued',p.id, \
                    (clock_timestamp() AT TIME ZONE 'UTC')-$1*interval '1 second' \
                FROM generate_series(0,9999) n JOIN scheduler_plan_principals p ON p.ordinal=1+(n/2)%32")
                .bind::<Integer,_>(if overdue { 3600 } else { 0 })
                .execute(connection).await?;
            if served {
                diesel::sql_query("WITH ranked AS (SELECT kind,principal_identity, \
                    row_number() OVER (ORDER BY kind,principal_identity) AS sequence \
                    FROM hubuum_control.task_principal_service) \
                    UPDATE hubuum_control.task_principal_service s SET last_served=r.sequence \
                    FROM ranked r WHERE s.kind=r.kind AND s.principal_identity=r.principal_identity")
                    .execute(connection).await?;
            }
            diesel::sql_query("WITH progress AS (SELECT coalesce(max(last_served),0)+8 AS sequence \
                FROM hubuum_control.task_principal_service) \
                UPDATE hubuum_control.task_scheduler SET successful_claims=progress.sequence, \
                last_promoted_at=progress.sequence-$1,ordinary_position=0 FROM progress")
                .bind::<BigInt,_>(if promotion_due { 8_i64 } else { 0_i64 })
                .execute(connection).await?;
            diesel::sql_query("ANALYZE public.tasks").execute(connection).await?;
            diesel::sql_query("ANALYZE hubuum_control.task_accounting").execute(connection).await?;
            diesel::sql_query("ANALYZE hubuum_control.task_principal_service").execute(connection).await?;
            // Exercise the parameter-independent plans used by warmed workers,
            // including a due promotion while no queued task is overdue.
            diesel::sql_query("SET LOCAL plan_cache_mode='force_generic_plan'").execute(connection).await?;
            let counters = "SELECT seq_tup_read+idx_tup_fetch AS value FROM pg_stat_xact_user_tables \
                WHERE schemaname='public' AND relname='tasks'";
            let before = diesel::sql_query(counters).get_result::<TaskRowsVisited>(connection).await?.value;
            diesel::sql_query("SELECT public.hubuum_schedule_task($1::uuid,ARRAY['import','export'],60000)")
                .bind::<Text,_>(TaskWorkerId::generate().as_string()).execute(connection).await?;
            let after = diesel::sql_query(counters).get_result::<TaskRowsVisited>(connection).await?.value;
            assert!((1..=maximum_rows).contains(&(after-before)), "queue-head selection visited {} task rows (maximum {maximum_rows})", after-before);
            Ok::<_,hubuum_storage_postgres::PostgresStorageError>(())
        }).await.unwrap();
    }).await;
}

#[tokio::test]
async fn a_rolled_back_selection_does_not_consume_a_turn() {
    with_fixture(StorageBackendKind::Postgres, async |fixture| {
        let storage = fixture.storage();
        let task = fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let before = storage.task_capacity_snapshot().await.unwrap();
        let request = worker();
        let pool = postgres_test_pool_with_timeout(
            &runtime_database_url(),
            2,
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        let result = with_transaction(&pool, async |connection| {
            diesel::sql_query("SELECT public.hubuum_schedule_task($1::uuid,ARRAY['import'],60000)")
                .bind::<diesel::sql_types::Text, _>(request.worker().as_string())
                .execute(connection)
                .await?;
            Err::<(), _>(hubuum_storage_postgres::PostgresStorageError::internal(
                "injected after selection",
            ))
        })
        .await;
        assert!(result.is_err());
        let after = storage.task_capacity_snapshot().await.unwrap();
        assert_eq!(after.scheduling(), before.scheduling());
        let claim = storage.claim_next_task(request).await.unwrap().unwrap();
        assert_eq!(claim.task().id(), task);
        settle(storage, claim).await;
    })
    .await;
}

#[rstest]
#[tokio::test]
async fn restore_preserves_service_history_and_fences_worker_registration(
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        fixture.create_task(StorageTaskKind::Import).await.unwrap();
        let backup = fixture.capture(true).await.unwrap();
        let claim = storage.claim_next_task(worker()).await.unwrap().unwrap();
        let before = storage.task_capacity_snapshot().await.unwrap();
        fixture.restore(backup).await.unwrap();
        let after = storage.task_capacity_snapshot().await.unwrap();
        assert_eq!(
            (
                after.scheduling().successful_claims(),
                after.scheduling().ordinary_position(),
                after.scheduling().eligible_slots()
            ),
            (
                before.scheduling().successful_claims(),
                before.scheduling().ordinary_position(),
                0
            )
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
    })
    .await;
}
