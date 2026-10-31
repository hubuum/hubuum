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

async fn capture_application_fixture(pool: &hubuum_storage_postgres::PostgresPool) -> Value {
    with_transaction(pool, async |connection| {
        assume_owner(connection).await?;
        Ok::<_,PostgresStorageError>(sql_query("SELECT to_jsonb(a) AS material FROM hubuum_control.application_control a WHERE id=1").get_result::<Material>(connection).await?.material)
    }).await.unwrap()
}

async fn restore_application_fixture(
    connection: &mut PostgresConnection,
    material: &Value,
) -> Result<(), PostgresStorageError> {
    sql_query("UPDATE hubuum_control.application_control SET (incarnation,reapplication_revision,released_revision,checkpoint,pending_restore)=(SELECT r.incarnation,r.reapplication_revision,r.released_revision,r.checkpoint,r.pending_restore FROM jsonb_populate_record(NULL::hubuum_control.application_control,$1) r) WHERE id=1")
        .bind::<Jsonb,_>(material).execute(connection).await?;
    Ok(())
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
    let application_before = capture_application_fixture(&pool).await;
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
        restore_application_fixture(connection, &application_before).await?;
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
    let application_before = capture_application_fixture(&pool).await;
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
        restore_application_fixture(connection, &application_before).await?;
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
        StorageIdentityEvidence, StorageRecordIdentity,
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
        application_before: Value,
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
            let application_before = capture_application_fixture(&pool).await;
            let fixture = Self {
                application_before,
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

        async fn insert_restored_copy(&self, evidence: &StorageIdentityEvidence, location: &str) {
            let identity = evidence.subject().unwrap().record_identity().unwrap();
            if location != "live" {
                let id = self.insert(identity, location == "closed").await;
                with_transaction(&self.pool, async |connection| {
                    assume_owner(connection).await?;
                    connection.batch_execute("SET LOCAL hubuum.restore_identities='on'").await?;
                    sql_query("UPDATE public.hubuumobject_history SET logical_identity=$1 WHERE history_id=$2")
                        .bind::<Jsonb, _>(serde_json::to_value(evidence).unwrap())
                        .bind::<BigInt, _>(id).execute(connection).await?;
                    Ok::<_, PostgresStorageError>(())
                }).await.unwrap();
                return;
            }
            with_transaction(&self.pool, async |connection| {
                assume_owner(connection).await?;
                sql_query("INSERT INTO public.hubuumclass (name, description, collection_id) SELECT $1, '', id FROM public.collections WHERE parent_collection_id IS NULL")
                    .bind::<Text, _>(&self.name).execute(connection).await?;
                connection.batch_execute("SET LOCAL hubuum.restore_identities='on'").await?;
                sql_query("INSERT INTO public.hubuumobject (name, description, collection_id, hubuum_class_id, logical_identity) SELECT $1, '', collection_id, id, $2 FROM public.hubuumclass WHERE name=$1")
                    .bind::<Text, _>(&self.name).bind::<Jsonb, _>(serde_json::to_value(evidence).unwrap()).execute(connection).await?;
                Ok::<_, PostgresStorageError>(())
            }).await.unwrap();
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
                sql_query("DELETE FROM public.hubuumobject WHERE name=$1")
                    .bind::<Text, _>(&self.name).execute(connection).await?;
                sql_query("DELETE FROM public.hubuumclass WHERE name=$1")
                    .bind::<Text, _>(&self.name).execute(connection).await?;
                sql_query("DELETE FROM public.class_schema_history WHERE class_id IN (SELECT id FROM public.hubuumclass_history WHERE name=$1)")
                    .bind::<Text, _>(&self.name).execute(connection).await?;
                sql_query("DELETE FROM public.hubuumclass_history WHERE name=$1")
                    .bind::<Text, _>(&self.name).execute(connection).await?;
                sql_query("DELETE FROM public.hubuumobject_history WHERE name=$1")
                    .bind::<Text, _>(&self.name)
                    .execute(connection)
                    .await?;
                sql_query("UPDATE hubuum_control.deletion_authority SET material=$1 WHERE id=1")
                    .bind::<Jsonb, _>(serde_json::to_value(&self.before).unwrap())
                    .execute(connection)
                    .await?;
                restore_application_fixture(connection, &self.application_before).await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .unwrap();
        }
    }

    fn identity_evidence(identity: StorageRecordIdentity) -> StorageIdentityEvidence {
        serde_json::from_value(serde_json::json!({
            "subject": {"kind": "resource", "identity": {"state": "known", "identity": identity.resource()}, "version": identity.version()},
            "references": {},
        })).unwrap()
    }

    fn derived_version() -> VersionIdentity {
        let mut bytes = *Uuid::new_v4().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        serde_json::from_value(serde_json::json!(Uuid::from_bytes(bytes))).unwrap()
    }

    #[rstest]
    #[case::live_pending("live", false)]
    #[case::open_pending("open", false)]
    #[case::closed_pending("closed", false)]
    #[case::live_removed("live", true)]
    #[case::open_removed("open", true)]
    #[case::closed_removed("closed", true)]
    #[tokio::test]
    async fn restored_descendant_blocks_exact_copy_reapplication(
        #[case] location: &str,
        #[case] removed: bool,
    ) {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let state = if removed {
            fixture
                .storage()
                .cleanup_hot_object_history(fixture.cleanup(prepared))
                .await
                .unwrap()
        } else {
            prepared
        };
        let descendant = identity_evidence(fixture.request.target())
            .at_restore_baseline(derived_version())
            .unwrap();
        fixture.insert_restored_copy(&descendant, location).await;
        let fence = fixture
            .storage()
            .read_application_control()
            .await
            .unwrap()
            .fence();
        let result = fixture
            .storage()
            .reapply_deletion_decisions(
                state.clone(),
                fence,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            )
            .await;
        let authority = fixture.storage().read_deletion_control().await.unwrap();
        let quarantined = fixture
            .storage()
            .capture_application_execution(None)
            .await
            .is_err();
        let retained = fixture
            .exists(descendant.subject().unwrap().record_identity().unwrap())
            .await;
        fixture.finish().await;
        assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        assert!(
            quarantined && retained && authority == state,
            "a different restored version must remain quarantined without a false removal receipt"
        );
    }

    #[rstest]
    #[case::same_resource(true)]
    #[case::unrelated_resource(false)]
    #[tokio::test]
    async fn legacy_derived_copy_without_ancestry_cannot_prove_same_resource_absence(
        #[case] same_resource: bool,
    ) {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let removed = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(prepared))
            .await
            .unwrap();
        let copy = StorageRecordIdentity::new(
            if same_resource {
                fixture.request.target().resource()
            } else {
                ResourceIdentity::generate()
            },
            derived_version(),
        );
        fixture
            .insert_restored_copy(&identity_evidence(copy), "closed")
            .await;
        let fence = fixture
            .storage()
            .read_application_control()
            .await
            .unwrap()
            .fence();
        let result = fixture
            .storage()
            .reapply_deletion_decisions(
                removed,
                fence,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            )
            .await;
        let retained = fixture.exists(copy).await;
        fixture.finish().await;
        if same_resource {
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        } else {
            result.unwrap();
        }
        assert!(
            retained,
            "unknown provenance must never authorize deleting a different version"
        );
    }

    #[rstest]
    #[case::snapshot("U")]
    #[case::deletion_snapshot("D")]
    #[tokio::test]
    async fn identity_trigger_preserves_restored_sources_in_history_snapshots(#[case] op: &str) {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let descendant = identity_evidence(fixture.request.target())
            .at_restore_baseline(derived_version())
            .unwrap();
        let evidence = with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            let row = sql_query("INSERT INTO public.hubuumobject_history (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, op, valid_from, valid_to, history_id, revision, logical_identity) VALUES (987654321, $1, 1, 1, '{}', '', now(), now(), $2, now(), NULL, nextval('public.hubuumobject_history_seq'), 1, $3) RETURNING logical_identity AS material")
                .bind::<Text, _>(&fixture.name).bind::<Text, _>(op)
                .bind::<Jsonb, _>(serde_json::to_value(descendant).unwrap())
                .get_result::<Material>(connection).await?;
            Ok::<_, PostgresStorageError>(row.material)
        }).await.unwrap();
        let target = fixture.request.target().version();
        fixture.finish().await;
        let captured: StorageIdentityEvidence = serde_json::from_value(evidence).unwrap();
        assert!(captured.restore_source_versions().contains(target));
    }

    #[tokio::test]
    async fn identity_trigger_preserves_sources_when_refresh_keeps_version() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let descendant = identity_evidence(fixture.request.target())
            .at_restore_baseline(derived_version())
            .unwrap();
        fixture.insert_restored_copy(&descendant, "live").await;
        let refreshed = with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            // Exercise the same-version refresh path rather than the revision
            // trigger's ordinary suppression of an entirely unchanged update.
            // Match identity backfill: retaining the revision also suppresses
            // creation of a second history row with that same version UUID.
            connection.batch_execute("SET LOCAL hubuum.restore_revisions='on'; SET LOCAL hubuum.restore_history='on'").await?;
            let row = sql_query("UPDATE public.hubuumobject SET name=name WHERE name=$1 RETURNING logical_identity AS material")
                .bind::<Text, _>(&fixture.name).get_result::<Material>(connection).await?;
            Ok::<_, PostgresStorageError>(row.material)
        }).await;
        fixture.finish().await;
        let refreshed: StorageIdentityEvidence =
            serde_json::from_value(refreshed.unwrap()).unwrap();
        assert_eq!(refreshed.subject(), descendant.subject());
        assert_eq!(
            refreshed.restore_source_versions(),
            descendant.restore_source_versions()
        );
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

    #[derive(Default)]
    struct AcquisitionCounter(std::sync::atomic::AtomicUsize);

    impl hubuum_storage_postgres::PostgresObserver for AcquisitionCounter {
        fn connection_acquired(&self, _: hubuum_storage_core::StorageCallSite, _: Duration) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[rstest]
    #[case::fresh(false)]
    #[case::inherited(true)]
    #[tokio::test]
    async fn worker_admission_and_maintenance_share_one_acquisition(#[case] inherited: bool) {
        use hubuum_storage_core::{ExecutionStorage, StorageExecutionScope};
        use std::sync::{Arc, atomic::Ordering};
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let counter = Arc::new(AcquisitionCounter::default());
        let runtime = PostgresStorage::new(integration_test_pool(1), counter.clone());
        let registration = runtime.register_runtime().await.unwrap();
        let owner = registration.owner();
        let bound = runtime.clone().with_runtime_owner(owner.clone());
        let original = runtime
            .capture_application_execution(Some(owner.clone()))
            .await
            .unwrap();
        let scope = if inherited {
            StorageExecutionScope::default().with_application_access(original.clone())
        } else {
            StorageExecutionScope::default()
        };
        counter.0.store(0, Ordering::SeqCst);
        let admission = bound
            .run_in_scope(
                scope,
                bound.capture_application_execution_with_maintenance(Some(owner)),
            )
            .await
            .unwrap();
        let acquisitions = std::sync::atomic::AtomicUsize::load(&counter.0, Ordering::SeqCst);
        let (permit, maintenance) = admission.into_parts();
        runtime.unregister_runtime(registration).await.unwrap();
        assert_eq!(acquisitions, 1);
        assert_eq!(permit, original);
        assert!(maintenance.is_normal());
    }

    #[rstest]
    #[case::active(false)]
    #[case::settled(true)]
    #[tokio::test]
    async fn coordinator_observes_and_acknowledges_drain_in_one_acquisition(#[case] settled: bool) {
        use hubuum_storage_core::RestoreStorage;
        use std::sync::{Arc, atomic::Ordering};
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let counter = Arc::new(AcquisitionCounter::default());
        let runtime = PostgresStorage::new(integration_test_pool(1), counter.clone());
        let registration = runtime.register_runtime().await.unwrap();
        let owner = registration.owner();
        let bound = runtime.clone().with_runtime_owner(owner.clone());
        fixture.prepare().await;
        counter.0.store(0, Ordering::SeqCst);
        let instance = Uuid::new_v4();
        bound
            .tick_restore_coordinator(instance, &|| settled, false)
            .await
            .unwrap();
        let acquisitions = std::sync::atomic::AtomicUsize::load(&counter.0, Ordering::SeqCst);
        let acknowledged = with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            Ok::<_, PostgresStorageError>(sql_query("SELECT EXISTS (SELECT 1 FROM hubuum_control.runtime_registrations r CROSS JOIN hubuum_control.application_control a CROSS JOIN hubuum_control.deletion_authority d WHERE r.id=$1 AND a.id=1 AND d.id=1 AND r.drained_incarnation=a.incarnation AND r.drained_revision=(d.material->>'revision')::bigint) AS allowed")
                .bind::<SqlUuid, _>(owner.id()).get_result::<Gate>(connection).await?.allowed)
        }).await.unwrap();
        runtime.remove_restore_instance(instance).await.unwrap();
        runtime.unregister_runtime(registration).await.unwrap();
        fixture.finish().await;
        assert_eq!(acquisitions, 1);
        assert_eq!(acknowledged, settled);
    }

    struct HeldOwnerSnapshot {
        release: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<Result<(), diesel::result::Error>>,
        pid: i32,
    }

    impl HeldOwnerSnapshot {
        async fn start(
            pool: PostgresPool,
            permit: hubuum_storage_core::StorageExecutionPermit,
        ) -> Self {
            use diesel::sql_types::{Integer, Nullable};
            use diesel_async::AsyncConnection;
            use hubuum_storage_core::RuntimeOwner;
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, released) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let mut connection = pool.get().await.expect("snapshot connection");
                connection.transaction::<_, diesel::result::Error, _>(async move |connection| {
                    connection.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").await?;
                    let allowed = sql_query("SELECT public.hubuum_application_execution_allowed($1,$2,$3,$4) AS allowed")
                        .bind::<Nullable<SqlUuid>, _>(permit.owner().map(RuntimeOwner::id))
                        .bind::<Nullable<SqlUuid>, _>(permit.owner().map(RuntimeOwner::ownership_token))
                        .bind::<SqlUuid, _>(permit.fence().incarnation())
                        .bind::<BigInt, _>(permit.fence().revision())
                        .get_result::<Gate>(connection).await?.allowed;
                    assert!(allowed, "the captured runtime must enter before the race");
                    let pid = diesel::select(diesel::dsl::sql::<Integer>("pg_backend_pid()"))
                        .get_result::<i32>(connection).await?;
                    started.send(pid).unwrap();
                    released.await.unwrap();
                    Ok(())
                }).await
            });
            let pid = timeout(Duration::from_secs(5), ready)
                .await
                .unwrap()
                .unwrap();
            Self { release, task, pid }
        }

        async fn finish(self) {
            self.release.send(()).unwrap();
            self.task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn other_runtime_can_start_capture_and_retire_during_owned_snapshot() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let runtime = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = runtime.register_runtime().await.unwrap();
        let permit = runtime
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let snapshot = HeldOwnerSnapshot::start(integration_test_pool(1), permit).await;
        let mut other = tokio::spawn(async move {
            let storage = PostgresStorage::unobserved(integration_test_pool(1));
            let other = storage.register_runtime().await?;
            let captured = storage
                .capture_application_execution(Some(other.owner()))
                .await;
            let retired = storage.unregister_runtime(other).await;
            captured?;
            retired
        });
        let completed = timeout(Duration::from_secs(5), &mut other).await;
        let independent = completed.is_ok();
        snapshot.finish().await;
        let result = match completed {
            Ok(result) => result,
            Err(_) => other.await,
        };
        runtime.unregister_runtime(registration).await.unwrap();
        result.unwrap().unwrap();
        assert!(
            independent,
            "unrelated runtime lifecycle must not wait for this owner's snapshot"
        );
    }

    #[tokio::test]
    async fn retirement_waits_for_its_own_inflight_sql_and_revokes_the_permit() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let runtime = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = runtime.register_runtime().await.unwrap();
        let owner = registration.owner();
        let permit = runtime
            .capture_application_execution(Some(owner.clone()))
            .await
            .unwrap();
        let snapshot = HeldOwnerSnapshot::start(integration_test_pool(1), permit).await;
        let retiring_runtime = runtime.clone();
        let retiring =
            tokio::spawn(async move { retiring_runtime.unregister_runtime(registration).await });
        let blocked =
            wait_for_blocked_writer(&integration_test_migration_pool(1), snapshot.pid).await;
        snapshot.finish().await;
        retiring.await.unwrap().unwrap();
        let recaptured = runtime.capture_application_execution(Some(owner)).await;
        blocked.unwrap();
        assert_eq!(
            recaptured.unwrap_err().kind(),
            StorageErrorKind::Unavailable
        );
    }

    #[tokio::test]
    async fn stale_repeatable_read_registration_cannot_exceed_runtime_capacity() {
        use diesel::result::{DatabaseErrorKind, Error as DieselError};
        use diesel::sql_types::Integer;
        use diesel_async::AsyncConnection;
        use hubuum_storage_core::DELETION_CONTROL_MAX_RUNTIMES;
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let pool = integration_test_migration_pool(2);
        let fixture_token = Uuid::new_v4();
        with_transaction(&pool, async |connection| {
            assume_owner(connection).await?;
            sql_query("INSERT INTO hubuum_control.runtime_registrations(id,ownership_token) SELECT gen_random_uuid(),$1 FROM generate_series(1,$2)")
                .bind::<SqlUuid, _>(fixture_token)
                .bind::<Integer, _>(i32::try_from(DELETION_CONTROL_MAX_RUNTIMES - 1).unwrap())
                .execute(connection).await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
        let stale_id = Uuid::new_v4();
        let stale_token = Uuid::new_v4();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (proceed, changed) = tokio::sync::oneshot::channel();
        let stale = tokio::spawn(async move {
            let pool = integration_test_pool(1);
            let mut connection = pool.get().await.expect("registration connection");
            connection
                .transaction::<_, DieselError, _>(async move |connection| {
                    connection
                        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        .await?;
                    sql_query("SELECT public.hubuum_application_control_read() AS material")
                        .get_result::<Material>(connection)
                        .await?;
                    started.send(()).unwrap();
                    changed.await.unwrap();
                    Ok(
                        sql_query("SELECT public.hubuum_register_runtime($1,$2) AS allowed")
                            .bind::<SqlUuid, _>(stale_id)
                            .bind::<SqlUuid, _>(stale_token)
                            .get_result::<Gate>(connection)
                            .await?
                            .allowed,
                    )
                })
                .await
        });
        ready.await.unwrap();
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let last_slot = runtime.register_runtime().await;
        proceed.send(()).unwrap();
        let result = stale.await.unwrap();
        let bounded = with_transaction(&pool, async |connection| {
            assume_owner(connection).await?;
            let bounded = sql_query(
                "SELECT count(*) <= $1 AS allowed FROM hubuum_control.runtime_registrations",
            )
            .bind::<BigInt, _>(i64::try_from(DELETION_CONTROL_MAX_RUNTIMES).unwrap())
            .get_result::<Gate>(connection)
            .await?
            .allowed;
            sql_query(
                "DELETE FROM hubuum_control.runtime_registrations WHERE ownership_token IN ($1,$2)",
            )
            .bind::<SqlUuid, _>(fixture_token)
            .bind::<SqlUuid, _>(stale_token)
            .execute(connection)
            .await?;
            Ok::<_, PostgresStorageError>(bounded)
        })
        .await
        .unwrap();
        runtime
            .unregister_runtime(last_slot.unwrap())
            .await
            .unwrap();
        let error = result.expect_err("an obsolete registry count must not admit a 1025th owner");
        assert!(matches!(
            error,
            DieselError::DatabaseError(DatabaseErrorKind::SerializationFailure, _)
        ));
        assert!(bounded);
    }

    #[tokio::test]
    async fn registered_runtime_wins_race_but_intent_starts_a_drain() {
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
        let prepared = fixture.storage().read_deletion_control().await.unwrap();
        fixture.finish().await;
        blocked.unwrap();
        assert_eq!(result.unwrap(), prepared);
        assert!(!prepared.intents().is_empty());
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
    #[tokio::test]
    async fn runtime_drain_and_exact_checkpoint_release_fence_old_work() {
        use hubuum_storage_core::{ExecutionStorage, StorageExecutionScope};
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let runtime = PostgresStorage::unobserved(integration_test_pool(2));
        let registration = runtime.register_runtime().await.unwrap();
        let owner = registration.owner();
        let bound = runtime.clone().with_runtime_owner(owner.clone());
        let missing = bound.storage_snapshot().await.unwrap_err();
        let old = runtime
            .capture_application_execution(Some(owner.clone()))
            .await
            .unwrap();
        bound
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(old.clone()),
                bound.storage_snapshot(),
            )
            .await
            .unwrap();
        let prepared = fixture.prepare().await;
        let app = fixture.storage().read_application_control().await.unwrap();
        let blocked = fixture
            .storage()
            .reapply_deletion_decisions(
                prepared.clone(),
                app.fence(),
                DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            )
            .await
            .unwrap_err();
        runtime
            .acknowledge_runtime_drain(owner.clone(), app.fence())
            .await
            .unwrap();
        let receipt = fixture
            .storage()
            .reapply_deletion_decisions(
                prepared,
                app.fence(),
                DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        let before_publication = runtime
            .capture_application_execution(Some(owner.clone()))
            .await
            .unwrap_err();
        fixture
            .storage()
            .release_application_exposure(
                receipt,
                DeletionCheckpointIdentity::new("c".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        let stale_admission = bound
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(old.clone()),
                bound.capture_application_execution_with_maintenance(Some(owner.clone())),
            )
            .await
            .unwrap_err();
        assert_eq!(stale_admission.kind(), StorageErrorKind::Unavailable);
        let stale = bound
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(old),
                bound.storage_snapshot(),
            )
            .await
            .unwrap_err();
        let fresh = runtime
            .capture_application_execution(Some(owner))
            .await
            .unwrap();
        bound
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(fresh),
                bound.storage_snapshot(),
            )
            .await
            .unwrap();
        runtime.unregister_runtime(registration).await.unwrap();
        fixture.finish().await;
        assert_eq!(missing.kind(), StorageErrorKind::Unavailable);
        assert_eq!(blocked.kind(), StorageErrorKind::Unavailable);
        assert_eq!(before_publication.kind(), StorageErrorKind::Unavailable);
        assert_eq!(stale.kind(), StorageErrorKind::Unavailable);
    }

    #[tokio::test]
    async fn restored_removed_history_is_reapplied_before_new_incarnation_release() {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let removed = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(prepared))
            .await
            .unwrap();
        let app = fixture.storage().read_application_control().await.unwrap();
        let first = fixture
            .storage()
            .reapply_deletion_decisions(
                removed,
                app.fence(),
                DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        fixture
            .storage()
            .release_application_exposure(
                first.clone(),
                DeletionCheckpointIdentity::new("c".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            sql_query("SELECT public.hubuum_rotate_application_for_restore()")
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        fixture.insert(fixture.request.target(), true).await;
        let current = fixture.storage().read_deletion_control().await.unwrap();
        let image = fixture.storage().read_application_control().await.unwrap();
        let old_release = fixture
            .storage()
            .release_application_exposure(
                first.clone(),
                DeletionCheckpointIdentity::new("c".repeat(64)).unwrap(),
            )
            .await;
        let receipt = fixture
            .storage()
            .reapply_deletion_decisions(
                current,
                image.fence(),
                DeletionCheckpointIdentity::new("d".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        let missing = !fixture.exists(fixture.request.target()).await;
        let before_release = fixture.storage().capture_application_execution(None).await;
        let capture = fixture.storage().capture_deletion_control().await.unwrap();
        fixture
            .storage()
            .release_application_exposure(
                receipt.clone(),
                DeletionCheckpointIdentity::new("e".repeat(64)).unwrap(),
            )
            .await
            .unwrap();
        let admitted = fixture.storage().capture_application_execution(None).await;
        fixture.finish().await;
        assert!(old_release.is_err());
        assert_ne!(receipt.incarnation(), first.incarnation());
        assert!(receipt.revision() > first.revision());
        assert!(missing);
        assert!(before_release.is_err());
        assert_eq!(capture.reapplication(), Some(&receipt));
        assert!(admitted.is_ok());
    }

    #[tokio::test]
    async fn recovery_revokes_only_runtimes_from_the_excluded_isolation_domain() {
        use hubuum_storage_core::RuntimeIsolationDomain;
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let domain = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap();
        let registration = runtime
            .register_runtime_isolated(Some(domain.clone()))
            .await
            .unwrap();
        let owner = registration.owner();
        let unchanged = fixture.before.recover_from(&fixture.before).unwrap();
        let wrong = fixture
            .storage()
            .recover_application_control(
                unchanged.clone(),
                RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 2).unwrap(),
            )
            .await;
        let retained = runtime
            .capture_application_execution(Some(owner.clone()))
            .await;
        fixture
            .storage()
            .recover_application_control(unchanged, domain)
            .await
            .unwrap();
        let retired = runtime.capture_application_execution(Some(owner)).await;
        runtime.unregister_runtime(registration).await.unwrap();
        fixture.finish().await;
        assert_eq!(wrong.unwrap_err().kind(), StorageErrorKind::Unavailable);
        assert!(retained.is_ok());
        assert_eq!(retired.unwrap_err().kind(), StorageErrorKind::Unavailable);
    }
    #[rstest]
    #[case::logical_restore("restore")]
    #[case::same_authority_recovery("recover")]
    #[case::owner_retirement("unregister")]
    #[tokio::test]
    async fn repeatable_read_before_gate_cannot_reuse_revoked_admission(#[case] change: &str) {
        use diesel::result::{DatabaseErrorKind, Error as DieselError};
        use diesel::sql_types::Nullable;
        use diesel_async::AsyncConnection;
        use hubuum_storage_core::{RuntimeIsolationDomain, RuntimeOwner};
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let runtime = PostgresStorage::unobserved(integration_test_pool(1));
        let domain = RuntimeIsolationDomain::new(Uuid::new_v4(), 3, 4).unwrap();
        let mut registration = Some(
            runtime
                .register_runtime_isolated(Some(domain.clone()))
                .await
                .unwrap(),
        );
        let permit = runtime
            .capture_application_execution(Some(registration.as_ref().unwrap().owner()))
            .await
            .unwrap();
        let pool = fixture.pool.clone();
        let (snapshot_started, started) = tokio::sync::oneshot::channel();
        let (changed, continue_gate) = tokio::sync::oneshot::channel();
        let snapshot = tokio::spawn(async move {
            let mut connection = pool.get().await.expect("snapshot connection");
            // Preserve the native error here: the adapter deliberately sanitizes
            // PostgreSQL diagnostics before exposing its public error surface.
            connection
                .transaction::<_, DieselError, _>(async move |connection| {
                    connection
                        .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                        .await?;
                    // Establish MVCC before the control operation, without taking the
                    // authority row lock. This models context setup before the gate.
                    sql_query("SELECT public.hubuum_application_control_read() AS material")
                        .get_result::<Material>(connection)
                        .await?;
                    snapshot_started.send(()).unwrap();
                    continue_gate.await.unwrap();
                    let result = sql_query(
                    "SELECT public.hubuum_application_execution_allowed($1,$2,$3,$4) AS allowed",
                )
                .bind::<Nullable<SqlUuid>, _>(permit.owner().map(RuntimeOwner::id))
                .bind::<Nullable<SqlUuid>, _>(permit.owner().map(RuntimeOwner::ownership_token))
                .bind::<SqlUuid, _>(permit.fence().incarnation())
                .bind::<BigInt, _>(permit.fence().revision())
                .get_result::<Gate>(connection)
                .await?;
                    Ok::<_, DieselError>(result.allowed)
                })
                .await
        });
        started.await.unwrap();
        match change {
            "restore" => {
                with_transaction(&fixture.pool, async |connection| {
                    assume_owner(connection).await?;
                    sql_query("SELECT public.hubuum_rotate_application_for_restore()")
                        .execute(connection)
                        .await?;
                    Ok::<_, PostgresStorageError>(())
                })
                .await
                .unwrap();
            }
            "recover" => {
                fixture
                    .storage()
                    .recover_application_control(
                        fixture.before.recover_from(&fixture.before).unwrap(),
                        domain,
                    )
                    .await
                    .unwrap();
            }
            "unregister" => runtime
                .unregister_runtime(registration.take().unwrap())
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        changed.send(()).unwrap();
        let result = snapshot.await.unwrap();
        if let Some(registration) = registration {
            runtime.unregister_runtime(registration).await.unwrap();
        }
        fixture.finish().await;
        let error = result.expect_err("an old snapshot must not admit revoked application work");
        assert!(
            matches!(
                &error,
                DieselError::DatabaseError(DatabaseErrorKind::SerializationFailure, _)
            ),
            "expected a serialization failure, got {error:?}"
        );
    }
    #[rstest]
    #[case::missing_resource(Value::Null)]
    #[case::nil_resource(serde_json::json!(Uuid::nil()))]
    #[tokio::test]
    async fn unknown_unrelated_history_prevents_removed_copy_reapplication(
        #[case] resource: Value,
    ) {
        let _guard = CONTROL_TEST_LOCK.lock().await;
        let fixture = Fixture::new(true).await;
        let prepared = fixture.prepare().await;
        let removed = fixture
            .storage()
            .cleanup_hot_object_history(fixture.cleanup(prepared))
            .await
            .unwrap();
        let unknown =
            StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
        let id = fixture.insert(unknown, true).await;
        with_transaction(&fixture.pool,async |connection| {
            assume_owner(connection).await?;
            connection.batch_execute("SET LOCAL hubuum.restore_identities='on'").await?;
            sql_query("UPDATE public.hubuumobject_history SET logical_identity=jsonb_set(logical_identity,'{subject,identity,identity}',$1) WHERE history_id=$2")
                .bind::<Jsonb,_>(&resource).bind::<BigInt,_>(id).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
        let fence = fixture
            .storage()
            .read_application_control()
            .await
            .unwrap()
            .fence();
        let result = fixture
            .storage()
            .reapply_deletion_decisions(
                removed,
                fence,
                DeletionCheckpointIdentity::new("a".repeat(64)).unwrap(),
            )
            .await;
        let quarantined = fixture
            .storage()
            .capture_application_execution(None)
            .await
            .is_err();
        fixture.finish().await;
        assert!(
            result.is_err(),
            "unresolved legacy correspondence must not prove absence"
        );
        assert!(quarantined);
    }
}
