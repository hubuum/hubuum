#![cfg(feature = "integration-test-support")]

use diesel::sql_types::{Bool, Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::{
    DeletionControlState, DeletionControlStorage, DeletionCopyState, DeletionSubject,
    StorageErrorKind,
};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresStorage, PostgresStorageError, with_transaction,
};
use rstest::rstest;
use serde_json::Value;
use uuid::Uuid;

static CONTROL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn connection_loss_during_control_recovery_rolls_back_import() {
    use hubuum_storage_postgres::test_support::terminate_backend;
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};
    use std::time::Duration;
    use tokio::time::timeout;

    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(2);
    let storage = PostgresStorage::unobserved(pool.clone());
    let before = storage.read_deletion_control().await.unwrap();
    let required = before
        .record_intent(
            Uuid::new_v4(),
            DeletionSubject::Version {
                identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
            },
            vec![Uuid::new_v4()],
        )
        .unwrap();
    let change = before.recover_from(required.next()).unwrap();
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
    let task_controller = controller.clone();
    let task_storage = storage.clone();
    let task = tokio::spawn(async move {
        task_controller
            .run(task_storage.commit_deletion_control(change))
            .await
    });
    let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
        .await
        .unwrap();
    terminate_backend(&pool, reached.backend_pid().unwrap())
        .await
        .unwrap();
    controller.resume();
    let result = timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert_eq!(storage.read_deletion_control().await.unwrap(), before);
}

#[derive(QueryableByName)]
struct Material {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}

#[derive(QueryableByName)]
struct Gate {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

#[tokio::test]
async fn runtime_can_read_authority_without_control_table_write_access() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_pool(1);
    let storage = PostgresStorage::unobserved(pool.clone());
    storage
        .read_deletion_control()
        .await
        .expect("read-only runtime function");
    if database_role_tests_enabled() {
        let can_write = with_transaction(&pool, async |connection| {
            Ok::<_, PostgresStorageError>(sql_query("SELECT has_table_privilege(current_user, c.oid, 'UPDATE') AS allowed FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'hubuum_control' AND c.relname = 'deletion_authority'")
                .get_result::<Gate>(connection).await?.allowed)
        }).await.unwrap();
        assert!(!can_write);
    }
}

#[rstest]
#[case::pending(DeletionCopyState::Pending)]
#[case::blocked(DeletionCopyState::Blocked)]
#[case::removed(DeletionCopyState::Removed)]
#[tokio::test]
async fn recovered_decisions_restrict_exposure_transactionally(#[case] outcome: DeletionCopyState) {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        connection
            .batch_execute("SAVEPOINT recovery_fixture")
            .await?;
        let before = sql_query(
            "SELECT material FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE",
        )
        .get_result::<Material>(connection)
        .await?;
        let initial: DeletionControlState = serde_json::from_value(before.material).unwrap();
        let id = Uuid::new_v4();
        let copy = Uuid::new_v4();
        let intent = initial
            .record_intent(
                id,
                DeletionSubject::Version {
                    identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
                },
                vec![copy],
            )
            .unwrap();
        let recovered = intent.next().record_copy(id, copy, outcome).unwrap();
        sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(serde_json::to_value(recovered.next()).unwrap())
            .execute(connection)
            .await?;
        let during = sql_query("SELECT public.hubuum_deletion_exposure_allowed() AS allowed")
            .get_result::<Gate>(connection)
            .await?
            .allowed;
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT recovery_fixture")
            .await?;
        assert!(!during);
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
}

async fn assume_owner(connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        sql_query("SELECT set_config('role', $1, true)")
            .bind::<Text, _>(integration_test_database_roles().owner().as_str())
            .execute(connection)
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn deletion_restore_gate_rejects_retained_decisions() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        connection
            .batch_execute("SAVEPOINT deletion_fixture")
            .await?;
        let row = sql_query(
            "SELECT material FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE",
        )
        .get_result::<Material>(connection)
        .await?;
        let state: DeletionControlState = serde_json::from_value(row.material).unwrap();
        let subject = DeletionSubject::Version {
            identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
        };
        let change = state
            .record_intent(Uuid::new_v4(), subject, vec![Uuid::new_v4()])
            .unwrap();
        sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(serde_json::to_value(change.next()).unwrap())
            .execute(connection)
            .await?;
        let result = sql_query("SELECT public.hubuum_check_deletion_restore_gate()")
            .execute(connection)
            .await;
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT deletion_fixture")
            .await?;
        assert!(result.is_err());
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn deletion_authority_has_no_cascading_application_references() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    #[derive(QueryableByName)]
    struct ResultRow {
        #[diesel(sql_type = Bool)]
        independent: bool,
    }
    let pool = integration_test_migration_pool(1);
    let result = with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        Ok::<_, PostgresStorageError>(sql_query("SELECT NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='hubuum_control.deletion_authority'::regclass AND contype='f') AS independent")
            .get_result::<ResultRow>(connection).await?.independent)
    }).await.unwrap();
    assert!(result);
}

#[tokio::test]
async fn deletion_authority_compare_and_swap_rejects_old_snapshot() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    let before = with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        Ok::<_, PostgresStorageError>(
            sql_query("SELECT material FROM hubuum_control.deletion_authority WHERE id=1")
                .get_result::<Material>(connection)
                .await?
                .material,
        )
    })
    .await
    .unwrap();
    let state: DeletionControlState = serde_json::from_value(before.clone()).unwrap();
    let change = state
        .record_intent(
            Uuid::new_v4(),
            DeletionSubject::Version {
                identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
            },
            vec![Uuid::new_v4()],
        )
        .unwrap();
    let storage = PostgresStorage::unobserved(pool.clone());
    let first = storage.commit_deletion_control(change.clone()).await;
    let stale = storage.commit_deletion_control(change).await;
    // Restore this deployment singleton before asserting, including on failure.
    with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(before)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    first.expect("initial adapter transition should commit");
    assert_eq!(stale.unwrap_err().kind(), StorageErrorKind::Conflict);
}

#[tokio::test]
async fn admitted_task_transaction_blocks_a_later_deletion_intent() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let runtime = integration_test_pool(1);
    let owner = integration_test_migration_pool(1);
    with_transaction(&runtime, async |connection| {
        let gate = sql_query("SELECT public.hubuum_deletion_execution_allowed() AS allowed")
            .get_result::<Gate>(connection)
            .await?;
        assert!(gate.allowed, "fixture starts with unrestricted authority");
        let update = with_transaction(&owner, async |connection| {
            assume_owner(connection).await?;
            // Match PostgreSQL's lock_not_available SQLSTATE (55P03), not
            // translated adapter messages or a broad database-error category.
            connection
                .batch_execute(
                    "DO $probe$ BEGIN
                    PERFORM 1 FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE NOWAIT;
                    RAISE EXCEPTION 'Deletion intent acquired authority while task fence was held';
                EXCEPTION WHEN lock_not_available THEN NULL;
                END $probe$",
                )
                .await
                .map_err(|error| {
                    PostgresStorageError::internal(format!(
                        "Deletion authority lock probe failed: {error:?}"
                    ))
                })
        })
        .await;
        // NOWAIT establishes the incompatible lock without relying on sleeps
        // or assuming that a spawned updater was scheduled promptly.
        update.expect("only SQLSTATE 55P03 proves the task fence blocked the intent");
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let update = with_transaction(&owner, async |connection| {
        assume_owner(connection).await?;
        sql_query(
            "SELECT material FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE NOWAIT",
        )
        .get_result::<Material>(connection)
        .await
        .map(|_| ())
        .map_err(PostgresStorageError::from)
    })
    .await;
    update.expect("intent lock becomes available only after task transaction ends");
}

#[tokio::test]
async fn prior_deletion_intent_prevents_task_transaction_body() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use hubuum_storage_core::{ExecutionStorage, StorageExecutionScope};
    use hubuum_task_core::TaskExecutionContext;

    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    let storage = PostgresStorage::unobserved(pool.clone());
    let before = storage.read_deletion_control().await.unwrap();
    let change = before
        .record_intent(
            Uuid::new_v4(),
            DeletionSubject::Version {
                identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
            },
            vec![Uuid::new_v4()],
        )
        .unwrap();
    storage.commit_deletion_control(change).await.unwrap();
    let executed = AtomicBool::new(false);
    let (execution, _) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
    let result = storage
        .run_in_scope(
            StorageExecutionScope::default().with_task_execution(Some(execution)),
            with_transaction(&pool, async |_connection| {
                executed.store(true, Ordering::Relaxed);
                Ok::<_, PostgresStorageError>(())
            }),
        )
        .await;
    // Restore the deployment singleton before asserting the observed result.
    with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
            .bind::<Jsonb, _>(serde_json::to_value(before).unwrap())
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
    assert!(!AtomicBool::load(&executed, Ordering::Relaxed));
}

#[tokio::test]
async fn cancellation_interrupts_waiting_for_deletion_authority() {
    use std::time::Duration;

    use diesel::sql_types::Integer;
    use hubuum_storage_core::{ExecutionStorage, StorageExecutionScope};
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint};
    use hubuum_task_core::{TaskExecutionContext, TaskStopReason};
    use tokio::time::timeout;

    let _guard = CONTROL_TEST_LOCK.lock().await;
    let owner_pool = integration_test_migration_pool(2);
    let owner = PostgresStorage::unobserved(owner_pool.clone());
    let before = owner.read_deletion_control().await.unwrap();
    // An idempotent authority CAS takes the same exclusive row lock as an
    // intent commit, without leaving a persistent restriction behind this test.
    let change = before.recover_from(&before).unwrap();
    let controller = PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
    let writer_controller = controller.clone();
    let writer = tokio::spawn(async move {
        writer_controller
            .run(owner.commit_deletion_control(change))
            .await
    });
    let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
        .await
        .unwrap();
    let writer_pid = reached.backend_pid().unwrap();
    let pool = integration_test_pool(2);
    let storage = PostgresStorage::unobserved(pool.clone());
    let (execution, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
    let task = tokio::spawn(async move {
        storage
            .run_in_scope_send(
                StorageExecutionScope::default().with_task_execution(Some(execution)),
                with_transaction(&pool, async |_connection| {
                    Err::<(), _>(PostgresStorageError::internal(
                        "task body must not run while the authority writer is locked",
                    ))
                }),
            )
            .await
    });
    let blocked = timeout(Duration::from_secs(5), async {
        loop {
            let waiting = with_transaction(&owner_pool, async |connection| {
                Ok::<_, PostgresStorageError>(sql_query("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid))) AS allowed")
                    .bind::<Integer, _>(writer_pid)
                    .get_result::<Gate>(connection).await?.allowed)
            })
            .await?;
            if waiting {
                return Ok::<_, PostgresStorageError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    stop.request_stop(TaskStopReason::Cancelled);
    let cancelled = timeout(Duration::from_secs(5), task).await;
    // The writer remains locked while cancellation must finish. Always release
    // it before assertions, including when a regression makes cancellation time out.
    controller.resume();
    writer.await.unwrap().unwrap();
    blocked.unwrap().unwrap();
    let error = cancelled.unwrap().unwrap().unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::TaskCancelled);
}

mod managed_cleanup {
    use super::*;
    use diesel::sql_types::{BigInt, Uuid as SqlUuid};
    use hubuum_domain::{ResourceIdentity, VersionIdentity};
    use hubuum_storage_core::{
        DeletionCheckpointIdentity, HotObjectHistoryCleanup, HotObjectHistoryDeletion,
        StorageRecordIdentity,
    };
    use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint, PostgresPool};
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(QueryableByName)]
    struct HistoryId {
        #[diesel(sql_type = BigInt)]
        history_id: i64,
    }

    struct Fixture {
        pool: PostgresPool,
        before: DeletionControlState,
        request: HotObjectHistoryDeletion,
        name: String,
    }

    impl Fixture {
        async fn new(closed: bool) -> Self {
            let pool = integration_test_migration_pool(3);
            let before = PostgresStorage::unobserved(pool.clone())
                .read_deletion_control()
                .await
                .unwrap();
            let request = HotObjectHistoryDeletion::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                StorageRecordIdentity::new(
                    ResourceIdentity::generate(),
                    VersionIdentity::generate(),
                ),
            )
            .unwrap();
            let fixture = Self {
                pool,
                before,
                request,
                name: format!("managed_history_{}", Uuid::new_v4().simple()),
            };
            fixture.insert(fixture.request.target(), closed).await;
            fixture
        }

        fn storage(&self) -> PostgresStorage {
            PostgresStorage::unobserved(self.pool.clone())
        }

        async fn insert(&self, target: StorageRecordIdentity, closed: bool) -> i64 {
            let identity = serde_json::json!({
                "subject": {"kind": "resource", "identity": {"state": "known", "identity": target.resource()}, "version": target.version()},
                "references": {},
            });
            with_transaction(&self.pool, async |connection| {
                assume_owner(connection).await?;
                connection.batch_execute("SET LOCAL hubuum.restore_identities = 'on'").await?;
                Ok::<_, PostgresStorageError>(sql_query("INSERT INTO public.hubuumobject_history (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision, logical_identity) VALUES (987654321, $1, 1, 1, '{\"sensitive_fixture\":true}', '', now(), now(), 'U', now() - interval '1 hour', CASE WHEN $2 THEN now() ELSE NULL END, nextval('public.hubuumobject_history_seq'), 1, $3) RETURNING history_id")
                    .bind::<Text, _>(&self.name).bind::<Bool, _>(closed).bind::<Jsonb, _>(identity)
                    .get_result::<HistoryId>(connection).await?.history_id)
            }).await.unwrap()
        }

        async fn exists(&self, target: StorageRecordIdentity) -> bool {
            with_transaction(&self.pool, async |connection| {
                Ok::<_, PostgresStorageError>(sql_query("SELECT EXISTS (SELECT 1 FROM public.hubuumobject_history WHERE (logical_identity #>> '{subject,version}')::uuid = $1) AS allowed")
                    .bind::<SqlUuid, _>(Uuid::from_bytes(*target.version().as_bytes())).get_result::<Gate>(connection).await?.allowed)
            }).await.unwrap()
        }

        async fn prepare(&self) -> DeletionControlState {
            self.storage()
                .prepare_hot_object_history_deletion(self.before.clone(), self.request.clone())
                .await
                .unwrap()
        }

        fn cleanup(&self, state: DeletionControlState) -> HotObjectHistoryCleanup {
            HotObjectHistoryCleanup::new(
                state,
                self.request.clone(),
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            )
            .unwrap()
        }

        async fn finish(self) {
            with_transaction(&self.pool, async |connection| {
                assume_owner(connection).await?;
                sql_query("DELETE FROM public.hubuumobject_history WHERE name=$1")
                    .bind::<Text, _>(&self.name)
                    .execute(connection)
                    .await?;
                sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
                    .bind::<Jsonb, _>(serde_json::to_value(&self.before).unwrap())
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn cleanup_removes_only_bound_uuid_and_keeps_reused_integer_history() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let unrelated =
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
        fixture.insert(unrelated, true).await;
        let state = fixture.prepare().await;
        let result = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(state))
            .await;
        let removed = !fixture.exists(fixture.request.target()).await;
        let retained = fixture.exists(unrelated).await;
        let request = fixture.request.clone();
        fixture.finish().await;
        let completed = result.unwrap();
        assert!(
            removed && retained,
            "only the exact durable identity may be removed"
        );
        assert_eq!(
            completed.intents()[&request.intent()].receipts()[&request.copy()]
                .checkpoint()
                .as_str(),
            "a".repeat(64)
        );
    }

    #[tokio::test]
    async fn cleanup_matches_alternate_uuid_spelling_by_identity() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            connection.batch_execute("SET LOCAL hubuum.restore_identities = 'on'").await?;
            sql_query("UPDATE public.hubuumobject_history SET logical_identity = jsonb_set(jsonb_set(logical_identity, '{subject,version}', to_jsonb(upper(logical_identity #>> '{subject,version}'))), '{subject,identity,identity}', to_jsonb(upper(logical_identity #>> '{subject,identity,identity}'))) WHERE name=$1")
                .bind::<Text, _>(&fixture.name).execute(connection).await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
        let state = fixture.prepare().await;
        let result = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(state))
            .await;
        let absent = !fixture.exists(fixture.request.target()).await;
        fixture.finish().await;
        result.unwrap();
        assert!(absent);
    }

    #[tokio::test]
    async fn open_history_version_cannot_admit_deletion() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(false).await;
        let result = fixture
            .storage()
            .prepare_hot_object_history_deletion(fixture.before.clone(), fixture.request.clone())
            .await;
        let after = fixture.storage().read_deletion_control().await.unwrap();
        let unchanged = after == fixture.before && fixture.exists(fixture.request.target()).await;
        fixture.finish().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        assert!(unchanged);
    }

    #[tokio::test]
    async fn removed_receipt_rechecks_resurrected_matching_history() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let removed = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(prepared))
            .await
            .unwrap();
        fixture.insert(fixture.request.target(), true).await;
        let result = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(removed.clone()))
            .await;
        let absent = !fixture.exists(fixture.request.target()).await;
        fixture.finish().await;
        assert!(
            absent,
            "retained completion cannot skip reapplication to a recovered copy"
        );
        assert_eq!(result.unwrap(), removed);
    }

    #[tokio::test]
    async fn cleanup_connection_loss_rolls_back_copy_and_receipt_together() {
        use hubuum_storage_postgres::test_support::terminate_backend;
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let controller =
            PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
        let control = controller.clone();
        let storage = fixture.storage();
        let request = fixture.cleanup(prepared.clone());
        let task = tokio::spawn(async move {
            control
                .run(storage.cleanup_hot_object_history(request))
                .await
        });
        let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
            .await
            .unwrap();
        terminate_backend(&fixture.pool, reached.backend_pid().unwrap())
            .await
            .unwrap();
        controller.resume();
        let result = timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let after = fixture.storage().read_deletion_control().await.unwrap();
        let retained = fixture.exists(fixture.request.target()).await;
        fixture.finish().await;
        assert!(result.is_err());
        assert!(
            retained && after == prepared,
            "deletion and receipt must roll back together"
        );
    }

    #[tokio::test]
    async fn invalid_authority_rolls_back_runtime_registration() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            sql_query("UPDATE hubuum_control.deletion_authority SET material=jsonb_set(material, '{lineage}', '\"invalid\"') WHERE id=1").execute(connection).await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let registered = runtime.register_runtime().await;
        let empty = with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            Ok::<_, PostgresStorageError>(sql_query("SELECT NOT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations) AS allowed").get_result::<Gate>(connection).await?.allowed)
        }).await.unwrap();
        fixture.finish().await;
        match registered {
            Ok(registration) => {
                runtime.unregister_runtime(registration).await.unwrap();
                panic!("invalid authority permitted runtime registration");
            }
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::Backend),
        }
        assert!(
            empty,
            "failed validation must roll back ownership registration"
        );
    }

    #[tokio::test]
    async fn runtime_ownership_prevents_unrelated_unregister() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let storage = PostgresStorage::unobserved(integration_test_pool(1));
        let registration = storage.register_runtime().await.unwrap();
        let id = registration.id();
        let pool = integration_test_pool(1);
        let allowed = with_transaction(&pool, async |connection| {
            Ok::<_, PostgresStorageError>(
                sql_query("SELECT public.hubuum_unregister_runtime($1, $2) AS allowed")
                    .bind::<SqlUuid, _>(id)
                    .bind::<SqlUuid, _>(Uuid::new_v4())
                    .get_result::<Gate>(connection)
                    .await?
                    .allowed,
            )
        })
        .await
        .unwrap();
        let owner = integration_test_migration_pool(1);
        let retained = with_transaction(&owner, async |connection| {
            assume_owner(connection).await?;
            Ok::<_, PostgresStorageError>(sql_query("SELECT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations WHERE id=$1) AS allowed")
                .bind::<SqlUuid, _>(id).get_result::<Gate>(connection).await?.allowed)
        }).await.unwrap();
        storage.unregister_runtime(registration).await.unwrap();
        assert!(!allowed && retained);
    }

    #[tokio::test]
    async fn runtime_registration_capacity_is_bounded_without_eviction() {
        use diesel::sql_types::Integer;
        use hubuum_storage_core::DELETION_CONTROL_MAX_RUNTIMES;
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let pool = integration_test_migration_pool(1);
        let allowed = with_transaction(&pool, async |connection| {
            assume_owner(connection).await?;
            connection.batch_execute("SAVEPOINT runtime_capacity_fixture").await?;
            sql_query("INSERT INTO hubuum_control.runtime_registrations (id, ownership_token) SELECT gen_random_uuid(), gen_random_uuid() FROM generate_series(1, $1)")
                .bind::<Integer, _>(i32::try_from(DELETION_CONTROL_MAX_RUNTIMES).unwrap()).execute(connection).await?;
            let allowed = sql_query("SELECT public.hubuum_register_runtime($1, $2) AS allowed")
                .bind::<SqlUuid, _>(Uuid::new_v4()).bind::<SqlUuid, _>(Uuid::new_v4()).get_result::<Gate>(connection).await?.allowed;
            connection.batch_execute("ROLLBACK TO SAVEPOINT runtime_capacity_fixture").await?;
            Ok::<_, PostgresStorageError>(allowed)
        }).await.unwrap();
        assert!(!allowed);
    }

    async fn wait_for_blocked_writer(
        pool: &PostgresPool,
        writer_pid: i32,
    ) -> Result<(), tokio::time::error::Elapsed> {
        use diesel::sql_types::Integer;
        timeout(Duration::from_secs(5), async {
            loop {
                let blocked = with_transaction(pool, async |connection| {
                    Ok::<_, PostgresStorageError>(sql_query("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))) AS allowed")
                        .bind::<Integer, _>(writer_pid).get_result::<Gate>(connection).await?.allowed)
                }).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await
    }

    #[tokio::test]
    async fn registered_runtime_wins_race_and_prevents_intent_activation() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let controller =
            PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
        let control = controller.clone();
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let registering =
            tokio::spawn(async move { control.run(runtime.register_runtime()).await });
        let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
            .await
            .unwrap();
        let storage = fixture.storage();
        let before = fixture.before.clone();
        let request = fixture.request.clone();
        let prepare = tokio::spawn(async move {
            storage
                .prepare_hot_object_history_deletion(before, request)
                .await
        });
        let blocked = wait_for_blocked_writer(&fixture.pool, reached.backend_pid().unwrap()).await;
        controller.resume();
        let registration = registering.await.unwrap().unwrap();
        let result = prepare.await.unwrap();
        PostgresStorage::unobserved(integration_test_pool(1))
            .unregister_runtime(registration)
            .await
            .unwrap();
        let unchanged = fixture.storage().read_deletion_control().await.unwrap() == fixture.before;
        fixture.finish().await;
        blocked.unwrap();
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
        assert!(unchanged);
    }

    #[tokio::test]
    async fn deletion_intent_wins_race_and_prevents_runtime_startup() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let controller =
            PostgresFaultController::pausing(PostgresFaultPoint::TransactionBeforeCommit);
        let control = controller.clone();
        let storage = fixture.storage();
        let before = fixture.before.clone();
        let request = fixture.request.clone();
        let preparing = tokio::spawn(async move {
            control
                .run(storage.prepare_hot_object_history_deletion(before, request))
                .await
        });
        let reached = timeout(Duration::from_secs(5), controller.wait_until_reached())
            .await
            .unwrap();
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let registering = tokio::spawn(async move { runtime.register_runtime().await });
        let blocked = wait_for_blocked_writer(&fixture.pool, reached.backend_pid().unwrap()).await;
        controller.resume();
        let prepared = preparing.await.unwrap();
        let registered = registering.await.unwrap();
        if let Ok(registration) = registered.as_ref() {
            // The test must not leave an unexpected registration after a regression.
            with_transaction(&fixture.pool, async |connection| {
                assume_owner(connection).await?;
                sql_query("DELETE FROM hubuum_control.runtime_registrations WHERE id=$1")
                    .bind::<SqlUuid, _>(registration.id())
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .unwrap();
        }
        fixture.finish().await;
        blocked.unwrap();
        prepared.unwrap();
        assert_eq!(
            registered.unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
    }
}
