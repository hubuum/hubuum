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
