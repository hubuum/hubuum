use super::*;
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use diesel::sql_types::Integer;
use diesel_async::SimpleAsyncConnection;
use rstest::rstest;

#[rstest]
#[case::legacy(0, true)]
#[case::online_v1(1, false)]
#[case::unknown(7, false)]
#[tokio::test]
async fn native_archive_entry_refuses_nonlegacy_owners(
    #[case] protocol: i32,
    #[case] expected: bool,
) {
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let admin = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let outcome=crate::with_connection(&admin,async |connection| {
        connection.batch_execute("BEGIN").await?;
        let result=async {
            if database_role_tests_enabled() {
                connection.batch_execute(&format!("SET LOCAL ROLE {}",integration_test_database_roles().owner().as_str())).await?;
            }
            // Authority first, before the temporary registration-table lock.
            sql_query("SELECT public.hubuum_deletion_execution_allowed() AS allowed").get_result::<Allowed>(connection).await?;
            connection.batch_execute("ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN IF NOT EXISTS history_protocol integer NOT NULL DEFAULT 0").await?;
            sql_query("UPDATE hubuum_control.runtime_registrations SET history_protocol=$1 WHERE id=$2")
                .bind::<Integer,_>(protocol).bind::<SqlUuid,_>(permit.owner().unwrap().id()).execute(connection).await?;
            let allowed=fenced!(sql_query("SELECT public.hubuum_archive_history_entry($1,$2,$3,$4) AS allowed"),&permit)
                .get_result::<Allowed>(connection).await?.allowed;
            Ok::<_,PostgresStorageError>(allowed)
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        result
    }).await;
    backend.unregister_runtime(registration).await.unwrap();
    assert_eq!(outcome.unwrap(), expected);
}

#[tokio::test]
async fn rejected_capture_preserves_expired_cleanup_accounting() {
    #[derive(Debug, Eq, PartialEq, QueryableByName)]
    struct Counter {
        #[diesel(sql_type = Integer)]
        captures: i32,
        #[diesel(sql_type = BigInt)]
        bytes: i64,
        #[diesel(sql_type = BigInt)]
        actual_captures: i64,
        #[diesel(sql_type = BigInt)]
        actual_bytes: i64,
        #[diesel(sql_type = BigInt)]
        live_bytes: i64,
    }
    const COUNTER: &str = "SELECT captures,bytes,(SELECT count(*) FROM hubuum_control.history_archive_captures) AS actual_captures,(SELECT COALESCE(sum(bytes),0) FROM hubuum_control.history_archive_captures) AS actual_bytes,(SELECT COALESCE(sum(bytes),0) FROM hubuum_control.history_archive_captures WHERE id=$1) AS live_bytes FROM hubuum_control.history_archive_capture_capacity WHERE id=1";
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let resource = ResourceIdentity::generate();
    let catalog = HistoryArchiveCatalog::empty(resource);
    let make_capture = |now| {
        HistoryArchiveCapture::capture(
            HistoryArchiveCaptureRequest::new(
                PrincipalIdentity::generate(),
                HistoryArchiveSelection::new(
                    StorageBackupHistorySection::CollectionHistory,
                    resource,
                    HistoryArchivePoint::AsOf { at: now },
                ),
                catalog.generation(),
                now + chrono::Duration::hours(1),
            ),
            &catalog,
            &permit,
            HistoryArchiveInput::Unavailable,
            now,
        )
        .unwrap()
    };
    let live = make_capture(Utc::now());
    let expired = make_capture(Utc::now() - chrono::Duration::hours(2));
    let mut oversized = serde_json::to_value(make_capture(Utc::now())).unwrap();
    oversized["padding"] = serde_json::json!("x".repeat(HISTORY_ARCHIVE_CAPTURE_BYTES));
    let admin = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let outcome = crate::with_connection(&admin, async |connection| {
        connection.batch_execute("BEGIN").await?;
        let result = async {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role', $1, true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str()).execute(connection).await?;
            }
            let authority = fenced!(sql_query("SELECT public.hubuum_archive_history_entry($1,$2,$3,$4) AS allowed"), &permit)
                .get_result::<Allowed>(connection).await?.allowed;
            if !authority { return Err(PostgresStorageError::unavailable("Archive fixture owner unavailable")); }
            let before = sql_query(format!("{COUNTER} FOR UPDATE"))
                .bind::<SqlUuid,_>(live.id()).get_result::<Counter>(connection).await?;
            save_on(connection, &live, None, &permit).await?;
            // Simulate an elapsed stored metadata pin, not read coverage or
            // runtime/publication-owner expiry. The registration remains live.
            save_on(connection, &expired, None, &permit).await?;
            let mut observations = Vec::new();
            for _ in 0..2 {
                let allowed = fenced!(sql_query("SELECT public.hubuum_history_archive_capture_save($1,$2,$3,$4,$5,$6) AS allowed")
                    .bind::<Jsonb,_>(oversized.clone()).bind::<Nullable<BigInt>,_>(None::<i64>), &permit)
                    .get_result::<Allowed>(connection).await?.allowed;
                let counter = sql_query(COUNTER).bind::<SqlUuid,_>(live.id())
                    .get_result::<Counter>(connection).await?;
                observations.push((allowed,counter));
            }
            Ok::<_,PostgresStorageError>((before,observations))
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        connection.batch_execute("BEGIN").await?;
        let restored = async {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role', $1, true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str()).execute(connection).await?;
            }
            sql_query(COUNTER).bind::<SqlUuid,_>(live.id())
                .get_result::<Counter>(connection).await.map_err(PostgresStorageError::from)
        }.await;
        connection.batch_execute("ROLLBACK").await?;
        let restored = restored?;
        result.map(|(before,observations)|(before,observations,restored))
    }).await;
    let owner_still_live = backend.history_archive_catalog(resource, &permit).await;
    backend.unregister_runtime(registration).await.unwrap();
    let (before, observations, restored) = outcome.unwrap();
    assert!(owner_still_live.is_ok());
    for (allowed, counter) in &observations {
        assert!(!allowed);
        assert_eq!(i64::from(counter.captures), counter.actual_captures);
        assert_eq!(counter.bytes, counter.actual_bytes);
        assert!(counter.live_bytes > 0);
    }
    assert_eq!(observations[0].1, observations[1].1);
    assert_eq!(restored, before);
}
