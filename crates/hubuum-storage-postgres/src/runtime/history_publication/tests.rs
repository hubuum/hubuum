use super::*;
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use crate::{PostgresStorage, with_transaction};
use chrono::Utc;
use diesel::sql_types::Integer;
use diesel::{QueryableByName, sql_query};
use diesel_async::SimpleAsyncConnection;
use hubuum_domain::ResourceIdentity;
use hubuum_storage_core::*;
use tokio::time::{Duration, sleep, timeout};

#[derive(QueryableByName)]
struct Gate {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}
#[derive(QueryableByName)]
struct Backend {
    #[diesel(sql_type = Integer)]
    pid: i32,
}
async fn owner_role(connection: &mut PostgresConnection) {
    if database_role_tests_enabled() {
        sql_query("SELECT set_config('role',$1,true)")
            .bind::<diesel::sql_types::Text, _>(integration_test_database_roles().owner().as_str())
            .execute(connection)
            .await
            .unwrap();
    }
}
async fn settle(connection: &mut PostgresConnection, producer: &HistoryProducerContext) -> bool {
    sql_query("SELECT public.hubuum_history_producer_settle($1,$2,$3) AS allowed")
        .bind::<SqlUuid, _>(producer.parent().id())
        .bind::<SqlUuid, _>(producer.id())
        .bind::<SqlUuid, _>(producer.ownership_token())
        .get_result::<Gate>(connection)
        .await
        .unwrap()
        .allowed
}
async fn observed_block(connection: &mut PostgresConnection, pid: i32) {
    timeout(Duration::from_secs(5), async {
        loop {
            if sql_query("SELECT cardinality(pg_blocking_pids($1))>0 AS allowed")
                .bind::<Integer, _>(pid)
                .get_result::<Gate>(connection)
                .await
                .unwrap()
                .allowed
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("settlement must actually wait on the productive transaction");
}

#[rstest::rstest]
#[case::rc_read_first(false, false)]
#[case::rr_read_first(true, false)]
#[case::rc_settler_first(false, true)]
#[case::rr_settler_first(true, true)]
#[tokio::test]
async fn archive_gate_orders_child_before_parent_under_settlement(
    #[case] repeatable_read: bool,
    #[case] settler_first: bool,
) {
    let pool = integration_test_pool(3);
    let backend = PostgresStorage::unobserved(pool.clone());
    let runtime = PostgresRuntime::unobserved(pool.clone());
    let registration = backend.register_runtime().await.unwrap();
    let application = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let owner = backend
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            backend.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let permit = owner.permit().clone();
    let child = permit.producer();
    let admin = integration_test_migration_pool(2);
    let mut reader = pool.get().await.unwrap();
    let mut writer = admin.get().await.unwrap();
    let mut observer = admin.get().await.unwrap();
    reader
        .batch_execute(if repeatable_read {
            "BEGIN ISOLATION LEVEL REPEATABLE READ"
        } else {
            "BEGIN ISOLATION LEVEL READ COMMITTED"
        })
        .await
        .unwrap();
    // Establish the retained snapshot before the competing lock/settlement.
    sql_query("SELECT public.hubuum_history_publication_generation()>=0 AS allowed")
        .get_result::<Gate>(&mut reader)
        .await
        .unwrap();
    writer.batch_execute("BEGIN").await.unwrap();
    owner_role(&mut writer).await;
    let pid = sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Backend>(&mut writer)
        .await
        .unwrap()
        .pid;
    if settler_first {
        sql_query("SELECT true AS allowed FROM hubuum_control.history_publication_producers WHERE publication_id=$1 AND id=$2 AND ownership_token=$3 FOR UPDATE")
            .bind::<SqlUuid,_>(child.parent().id()).bind::<SqlUuid,_>(child.id()).bind::<SqlUuid,_>(child.ownership_token())
            .get_result::<Gate>(&mut writer).await.unwrap();
        // A third party owns parent UPDATE. Taking parent before child would
        // block, forming the three-party cycle this test must exclude.
        observer.batch_execute("BEGIN").await.unwrap();
        owner_role(&mut observer).await;
        sql_query("SELECT true AS allowed FROM hubuum_control.history_publication_owners WHERE id=$1 FOR UPDATE NOWAIT")
            .bind::<SqlUuid,_>(child.parent().id()).get_result::<Gate>(&mut observer).await.unwrap();
    }
    let validation = timeout(
        Duration::from_secs(5),
        backend.run_in_scope(permit.execution_scope().clone(), async {
            let mut context = TransactionLocalContext::ambient();
            context.history_mode = HistoryEntryMode::Archive;
            context.archive_producer = Some(child.clone());
            runtime
                .require_application_execution(&mut reader, &context)
                .await
        }),
    )
    .await
    .expect("child contention must reject without waiting on the parent");
    if settler_first {
        let error: StorageError = validation.unwrap_err().into();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(error.to_string(), "Archive producer is unavailable or busy");
        observer.batch_execute("ROLLBACK").await.unwrap();
        assert!(settle(&mut writer, child).await);
        writer.batch_execute("COMMIT").await.unwrap();
    } else {
        validation.unwrap();
        {
            let pending = settle(&mut writer, child);
            tokio::pin!(pending);
            tokio::select! {
                result = &mut pending => panic!("settlement escaped productive SHARE locks: {result}"),
                () = observed_block(&mut observer, pid) => {}
            }
            reader.batch_execute("COMMIT").await.unwrap();
            assert!(timeout(Duration::from_secs(5), &mut pending).await.unwrap());
        }
        writer.batch_execute("COMMIT").await.unwrap();
    }
    reader.batch_execute("ROLLBACK").await.unwrap();
    // The public operation also refuses the original permit after retirement;
    // no fresh parent/API/application authority is captured to finish old work.
    let stale = backend
        .run_in_scope(
            permit.execution_scope().clone(),
            backend.history_archive_catalog(ResourceIdentity::generate(), &permit),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), StorageErrorKind::Unavailable);
    assert_eq!(stale.to_string(), "Archive producer is unavailable or busy");
    let completed = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    backend
        .settle_history_producer(completed.producer())
        .await
        .unwrap();
    backend
        .settle_history_producer(completed.producer())
        .await
        .unwrap();
    let parent = child.parent().id();
    backend
        .settle_history_publication(completed.into_publication())
        .await
        .unwrap();
    // All probes are idle; return the exact two owner connections BEFORE census.
    drop(writer);
    drop(observer);
    drop(reader);
    let clean = with_transaction(&admin, async |connection| {
        owner_role(connection).await;
        sql_query("SELECT NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners WHERE id=$1) AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_producers WHERE publication_id=$1) AS allowed")
            .bind::<SqlUuid,_>(parent).get_result::<Gate>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().allowed;
    backend.unregister_runtime(registration).await.unwrap();
    assert!(clean);
}

#[derive(QueryableByName)]
struct Reserved {
    #[diesel(sql_type=BigInt)]
    bytes: i64,
}
#[rstest::rstest]
#[case::owned(0, true)]
#[case::missing_scope(1, false)]
#[case::callback(2, false)]
#[case::different_parent(3, false)]
#[case::retired(4, false)]
#[tokio::test]
async fn archive_gate_five_entries_retain_exact_owner(
    #[case] mode: u8,
    #[case] allowed: bool,
    #[values(0u8, 1, 2, 3, 4)] operation: u8,
) {
    let storage = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = storage.register_runtime().await.unwrap();
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let original = StorageExecutionScope::default().with_application_access(application);
    let owner = storage
        .run_in_scope(original.clone(), storage.admit_history_archive_execution())
        .await
        .unwrap();
    let permit = owner.permit().clone();
    let principal = hubuum_domain::PrincipalIdentity::generate();
    let resource = ResourceIdentity::generate();
    let request = HistoryArchiveCaptureRequest::new(
        principal,
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            resource,
            HistoryArchivePoint::AsOf { at: Utc::now() },
        ),
        HistoryArchiveCatalogGeneration::default(),
        Utc::now() + chrono::Duration::hours(1),
    );
    let capture = storage
        .run_in_scope(
            permit.execution_scope().clone(),
            storage.capture_history_archive(request.clone(), &permit),
        )
        .await
        .unwrap();
    assert_eq!(
        capture.coverage(Utc::now()),
        HistoryArchiveCoverageState::Unavailable
    );
    let other = if mode == 3 {
        Some(
            storage
                .run_in_scope(original, storage.admit_history_archive_execution())
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let scope = match mode {
        1 => StorageExecutionScope::default(),
        2 => permit.execution_scope().clone().with_storage_callback(),
        3 => other.as_ref().unwrap().permit().execution_scope().clone(),
        _ => permit.execution_scope().clone(),
    };
    let completed = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    if mode == 4 {
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
    }
    let outcome = storage
        .run_in_scope(scope, async {
            match operation {
                0 => storage
                    .history_archive_catalog(resource, &permit)
                    .await
                    .map(|_| ()),
                // Invalid shape is projected ONLY after productive entry validation.
                1 => storage
                    .publish_history_archive(
                        HistoryArchiveCatalogGeneration::default(),
                        vec![],
                        &permit,
                    )
                    .await
                    .map(|_| ()),
                2 => storage
                    .capture_history_archive(request, &permit)
                    .await
                    .map(|_| ()),
                3 => storage
                    .history_archive_capture(capture.id(), principal, &permit)
                    .await
                    .map(|_| ()),
                4 => storage
                    .reserve_history_archive_read(capture.id(), principal, 1, &permit)
                    .await
                    .map(|_| ()),
                _ => unreachable!(),
            }
        })
        .await;
    let expected = if allowed && operation == 1 {
        Some(StorageErrorKind::InvalidInput)
    } else if allowed {
        None
    } else {
        Some(StorageErrorKind::Unavailable)
    };
    assert_eq!(outcome.err().map(|error| error.kind()), expected);
    let admin = integration_test_migration_pool(1);
    let bytes = with_transaction(&admin, async |connection| {
            owner_role(connection).await;
            sql_query("SELECT (material->>'reserved_read_bytes')::BIGINT AS bytes FROM hubuum_control.history_archive_captures WHERE id=$1")
                .bind::<SqlUuid,_>(capture.id()).get_result::<Reserved>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap().bytes;
    assert_eq!(bytes, i64::from(allowed && operation == 4));
    let original_parent = permit.producer().parent().id();
    let other_parent = other.as_ref().map_or(original_parent, |owner| {
        owner.permit().producer().parent().id()
    });
    storage
        .settle_history_producer(completed.producer())
        .await
        .unwrap();
    storage
        .settle_history_producer(completed.producer())
        .await
        .unwrap();
    storage
        .settle_history_publication(completed.into_publication())
        .await
        .unwrap();
    if let Some(other) = other {
        let completed = other
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_publication(completed.into_publication())
            .await
            .unwrap();
    }
    let clean = with_transaction(&admin, async |connection| {
            owner_role(connection).await;
            sql_query("SELECT NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners WHERE id=$1 OR id=$2) AND NOT EXISTS(SELECT 1 FROM hubuum_control.history_publication_producers WHERE publication_id=$1 OR publication_id=$2) AS allowed")
                .bind::<SqlUuid,_>(original_parent).bind::<SqlUuid,_>(other_parent).get_result::<Gate>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap().allowed;
    storage.unregister_runtime(registration).await.unwrap();
    assert!(clean);
}

#[rstest::rstest]
#[case::other_actual_parent(false)]
#[case::legacy_unbound_capture(true)]
#[tokio::test]
async fn archive_gate_capture_requires_original_actual_producer(
    #[case] unbound: bool,
    #[values(false, true)] reserve: bool,
) {
    let pool = integration_test_pool(2);
    let storage = PostgresStorage::unobserved(pool.clone());
    let registration = storage.register_runtime().await.unwrap();
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let owner = storage
        .run_in_scope(scope.clone(), storage.admit_history_archive_execution())
        .await
        .unwrap();
    let other = storage
        .run_in_scope(scope, storage.admit_history_archive_execution())
        .await
        .unwrap();
    assert_eq!(owner.permit().application(), other.permit().application());
    let principal = hubuum_domain::PrincipalIdentity::generate();
    let resource = ResourceIdentity::generate();
    let now = Utc::now();
    let request = HistoryArchiveCaptureRequest::new(
        principal,
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            resource,
            HistoryArchivePoint::AsOf { at: now },
        ),
        HistoryArchiveCatalogGeneration::default(),
        now + chrono::Duration::hours(1),
    );
    let capture = if unbound {
        // Genuine Legacy wire/storage entry, retained only for the pre-expiry
        // compatibility boundary. It cannot be upgraded by minting a new pair.
        // This is negative coverage, never permission to enable OnlineV1.
        let capture = HistoryArchiveCapture::capture(
            request,
            &HistoryArchiveCatalog::empty(resource),
            owner.permit().application(),
            HistoryArchiveInput::Unavailable,
            now,
        )
        .unwrap();
        let app = owner.permit().application();
        let runtime = app.owner().unwrap();
        let saved = with_transaction(&pool, async |connection| {
            sql_query(
                "SELECT public.hubuum_history_archive_capture_save($1,$2,$3,$4,$5,$6) AS allowed",
            )
            .bind::<diesel::sql_types::Jsonb, _>(serde_json::to_value(&capture).unwrap())
            .bind::<Nullable<BigInt>, _>(None::<i64>)
            .bind::<SqlUuid, _>(runtime.id())
            .bind::<SqlUuid, _>(runtime.ownership_token())
            .bind::<SqlUuid, _>(app.fence().incarnation())
            .bind::<BigInt, _>(app.fence().revision())
            .get_result::<Gate>(connection)
            .await
            .map_err(PostgresStorageError::from)
        })
        .await
        .unwrap()
        .allowed;
        assert!(saved);
        capture
    } else {
        storage
            .run_in_scope(
                owner.permit().execution_scope().clone(),
                storage.capture_history_archive(request, owner.permit()),
            )
            .await
            .unwrap()
    };
    let permit = if unbound {
        owner.permit()
    } else {
        other.permit()
    };
    let result = storage
        .run_in_scope(permit.execution_scope().clone(), async {
            if reserve {
                storage
                    .reserve_history_archive_read(capture.id(), principal, 1, permit)
                    .await
                    .map(|_| ())
            } else {
                storage
                    .history_archive_capture(capture.id(), principal, permit)
                    .await
                    .map(|_| ())
            }
        })
        .await;
    let error = result.unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(
        error.to_string(),
        "Archive capture belongs to a different original producer"
    );
    let admin = integration_test_migration_pool(1);
    let bytes = with_transaction(&admin, async |connection| {
        owner_role(connection).await;
        sql_query("SELECT (material->>'reserved_read_bytes')::BIGINT AS bytes FROM hubuum_control.history_archive_captures WHERE id=$1")
            .bind::<SqlUuid,_>(capture.id()).get_result::<Reserved>(connection).await.map_err(PostgresStorageError::from)
    }).await.unwrap().bytes;
    assert_eq!(bytes, 0);
    for owner in [owner, other] {
        let completed = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_publication(completed.into_publication())
            .await
            .unwrap();
    }
    storage.unregister_runtime(registration).await.unwrap();
}

/// API occupancy can end before publication. Retain its ORIGINAL identity in
/// the parent without a foreign key or a fresh owner captured during replay.
#[rstest::rstest]
#[case::bound(true)]
#[case::unrelated(false)]
#[tokio::test]
async fn publication_retains_original_api_binding_after_exact_api_settlement(#[case] bound: bool) {
    use futures_util::FutureExt;
    use hubuum_domain::PrincipalId;
    use hubuum_events_core::EventContext;
    use std::panic::{AssertUnwindSafe, resume_unwind};
    use uuid::Uuid;

    #[derive(QueryableByName)]
    struct Retained {
        #[diesel(sql_type=Nullable<SqlUuid>)]
        api_request_owner: Option<Uuid>,
    }
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let user = backend
        .create_user(StorageUserCreate::new(
            None,
            format!("history_api_binding_{}", Uuid::new_v4().simple()),
            "test-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    let registration = backend.register_runtime().await.unwrap();
    let application = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let api = backend
        .admit_api_request(
            PrincipalId::new(user.id().id()).unwrap(),
            ApiRequestClass::History,
            0,
        )
        .await
        .unwrap();
    let mut scope = StorageExecutionScope::default().with_application_access(application);
    if bound {
        scope = scope.with_api_request(api.clone());
    }
    let owner = backend
        .run_in_scope(scope, backend.admit_history_archive_execution())
        .await
        .unwrap();
    let permit = owner.permit().clone();
    let parent = permit.producer().parent().id();
    let admin = integration_test_migration_pool(1);
    let checked = AssertUnwindSafe(async {
        let before = with_transaction(&admin, async |connection| {
            owner_role(connection).await;
            sql_query("SELECT api_request_owner FROM hubuum_control.history_publication_owners WHERE id=$1")
                .bind::<SqlUuid,_>(parent).get_result::<Retained>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap();
        assert_eq!(before.api_request_owner, bound.then_some(api.owner()));
        backend.settle_api_request(api.clone()).await.unwrap();
        // Real productive access uses the runtime login and SAME original scope.
        let result = backend.run_in_scope(permit.execution_scope().clone(),
            backend.history_archive_catalog(ResourceIdentity::generate(), &permit)).await;
        if bound {
            assert_eq!(result.unwrap_err().api_request_rejection(), Some(ApiRequestRejection::ReconciliationRequired));
        } else {
            result.unwrap();
        }
        let after = with_transaction(&admin, async |connection| {
            owner_role(connection).await;
            sql_query("SELECT api_request_owner FROM hubuum_control.history_publication_owners WHERE id=$1")
                .bind::<SqlUuid,_>(parent).get_result::<Retained>(connection).await.map_err(PostgresStorageError::from)
        }).await.unwrap();
        assert_eq!(after.api_request_owner, before.api_request_owner);
    }).catch_unwind().await;
    // No work remains issued. Exact cleanup is outside productive/API/history
    // scope and runs even when the assertions above fail; it cannot refresh it.
    let cleanup = async {
        backend.settle_api_request(api).await?;
        let completion = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await?;
        backend
            .settle_history_producer(completion.producer())
            .await?;
        backend
            .settle_history_publication(completion.into_publication())
            .await?;
        backend.unregister_runtime(registration).await?;
        backend
            .delete_user(StorageUserDelete::new(user.id(), EventContext::system()))
            .await?
            .into_value();
        Ok::<_, StorageError>(())
    }
    .await;
    if let Err(panic) = checked {
        if let Err(error) = cleanup {
            eprintln!(
                "API binding fixture cleanup also failed: {:?}",
                error.kind()
            );
        }
        resume_unwind(panic);
    }
    cleanup.unwrap();
}
