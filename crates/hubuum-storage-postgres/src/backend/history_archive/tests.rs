use super::*;
use crate::test_support::{
    IsolatedRestoreTarget, database_role_tests_enabled, integration_test_database_roles,
    integration_test_database_url, integration_test_migration_pool, integration_test_pool,
};
use crate::{PostgresPoolSettings, build_postgres_pool};
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
            assert!(save_on(connection, &live, None, &permit).await?);
            // Simulate an elapsed stored metadata pin, not read coverage or
            // runtime/publication-owner expiry. The registration remains live.
            assert!(save_on(connection, &expired, None, &permit).await?);
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

#[tokio::test]
async fn rejected_admission_commits_cleanup_before_the_next_attempt() {
    // Only this adapter unit test uses the runner's separate restore database.
    // Other test binaries run sequentially and receive it back without our pins.
    IsolatedRestoreTarget::new(
        &std::env::var("HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL").unwrap(),
        &integration_test_database_url(),
    )
    .unwrap();
    let pool = |variable| {
        let settings = PostgresPoolSettings::builder(std::env::var(variable).unwrap())
            .max_size(2)
            .statement_timeout_ms(10_000)
            .acquire_timeout_ms(5_000)
            .build()
            .unwrap();
        build_postgres_pool(&settings).unwrap()
    };
    let admin = pool("HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL");
    let backend = PostgresStorage::unobserved(pool("HUBUUM_RESTORE_TEST_DATABASE_URL"));
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let name = format!("archive_cleanup_{}", Uuid::new_v4());
    let identity = crate::with_transaction(&admin, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        let material = sql_query("INSERT INTO public.collections(name,description,parent_collection_id) SELECT $1,$2,id FROM public.collections WHERE parent_collection_id IS NULL RETURNING logical_identity::text AS material")
            .bind::<Text, _>(&name).bind::<Text, _>("x".repeat(24 * 1024))
            .get_result::<Material>(connection).await?;
        let evidence: StorageIdentityEvidence = decode(material.material.as_deref().unwrap())?;
        let identity = evidence.subject().and_then(StorageIdentitySubject::record_identity).unwrap();
        // Exercise the real 1 GiB limit using charged metadata fixtures, without
        // allocating a GiB of irrelevant payloads. Table/counter charges agree.
        let empty = sql_query("SELECT captures=0 AND bytes=0 AS allowed FROM hubuum_control.history_archive_capture_capacity WHERE id=1 FOR UPDATE")
            .get_result::<Allowed>(connection).await?;
        assert!(empty.allowed, "The isolated capacity fixture must start empty");
        sql_query("INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes) SELECT gen_random_uuid(),$1,clock_timestamp()+interval '24 hours',jsonb_build_object('runtime',$2::text),CASE WHEN n=256 THEN 4194304-32768 ELSE 4194304 END FROM generate_series(1,256) n")
            .bind::<SqlUuid, _>(Uuid::new_v4()).bind::<Text, _>(registration.owner().id().to_string())
            .execute(connection).await?;
        sql_query("INSERT INTO hubuum_control.history_archive_captures(id,principal,expires_at,material,bytes) SELECT gen_random_uuid(),$1,clock_timestamp()-interval '1 hour',jsonb_build_object('runtime',$2::text),1024 FROM generate_series(1,32)")
            .bind::<SqlUuid, _>(Uuid::new_v4()).bind::<Text, _>(registration.owner().id().to_string())
            .execute(connection).await?;
        sql_query("UPDATE hubuum_control.history_archive_capture_capacity SET captures=(SELECT count(*) FROM hubuum_control.history_archive_captures),bytes=(SELECT sum(bytes) FROM hubuum_control.history_archive_captures) WHERE id=1")
            .execute(connection).await?;
        Ok::<_, PostgresStorageError>(identity)
    }).await.unwrap();
    let request = HistoryArchiveCaptureRequest::new(
        PrincipalIdentity::generate(),
        HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            identity.resource(),
            HistoryArchivePoint::Exact {
                version: identity.version(),
            },
        ),
        HistoryArchiveCatalogGeneration::default(),
        Utc::now() + chrono::Duration::hours(1),
    );
    let rejected = backend
        .capture_history_archive(request.clone(), &permit)
        .await;
    let admitted = backend
        .capture_history_archive(request.clone(), &permit)
        .await;
    // Cleanup before asserting the outcomes, including when the old rollback
    // behavior rejects both attempts.
    crate::with_transaction(&admin, async |connection| {
        if database_role_tests_enabled() {
            sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                .execute(connection).await?;
        }
        sql_query("WITH removed AS (DELETE FROM hubuum_control.history_archive_captures WHERE material->>'runtime'=$1 RETURNING bytes) UPDATE hubuum_control.history_archive_capture_capacity SET captures=captures-(SELECT count(*)::integer FROM removed),bytes=bytes-(SELECT COALESCE(sum(bytes),0) FROM removed) WHERE id=1")
            .bind::<Text, _>(registration.owner().id().to_string()).execute(connection).await?;
        sql_query("DELETE FROM public.collections WHERE name=$1").bind::<Text, _>(&name).execute(connection).await?;
        sql_query("DELETE FROM public.collections_history WHERE name=$1").bind::<Text, _>(&name).execute(connection).await?;
        Ok::<_, PostgresStorageError>(())
    }).await.unwrap();
    backend.unregister_runtime(registration).await.unwrap();
    rejected
        .unwrap_err()
        .history_archive_budget_evidence()
        .expect("Bounded rejection evidence")
        .validate_request(&request, &permit, Utc::now())
        .unwrap();
    assert_eq!(
        admitted.unwrap().coverage(Utc::now()),
        HistoryArchiveCoverageState::Complete
    );
}
