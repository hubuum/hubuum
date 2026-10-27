#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::StorageIdentityEvidence;
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use serde_json::Value;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = Integer)]
    count: i32,
}

#[derive(QueryableByName)]
struct RetainedVersion {
    #[diesel(sql_type = BigInt)]
    history_id: i64,
    #[diesel(sql_type = BigInt)]
    revision: i64,
    #[diesel(sql_type = Jsonb)]
    evidence: Value,
}

#[rstest]
#[case::positive_offset("2026-10-27T05:30:00+05:30")]
#[case::negative_offset("2026-10-26T20:00:00-04:00")]
#[tokio::test]
async fn backfill_mapping_uses_creation_instant_independently_of_timezone(#[case] timestamp: &str) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            assume_owner(connection).await?;
            connection
                .batch_execute("SAVEPOINT identity_timezone_fixture")
                .await?;
            let same = diesel::sql_query(
                "SELECT CASE WHEN
            public.hubuum_legacy_resource_identity('query_usage_declaration',
                jsonb_build_object('id', 1, 'created_at', '2026-10-27T00:00:00+00:00')) =
            public.hubuum_legacy_resource_identity('query_usage_declaration',
                jsonb_build_object('id', 1, 'created_at', $1::TEXT))
            THEN 1 ELSE 0 END AS count",
            )
            .bind::<Text, _>(timestamp)
            .get_result::<Count>(connection)
            .await?
            .count;
            connection
                .batch_execute("ROLLBACK TO SAVEPOINT identity_timezone_fixture")
                .await?;
            assert_eq!(
                same, 1,
                "one creation instant must have one authoritative mapping"
            );
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[rstest]
#[case::live(false)]
#[case::history_only(true)]
#[tokio::test]
async fn bounded_backfill_preserves_versions_and_converges_after_retry(#[case] deleted: bool) {
    let _serial = BACKFILL_FIXTURES.lock().await;
    let split = database_role_tests_enabled();
    let pool = if split {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        if split {
            let roles = integration_test_database_roles();
            connection.batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str())).await?;
        }
        // Transactional trigger changes model pre-upgrade rows without exposing
        // an incomplete table to parallel writers. The savepoint removes fixtures
        // and restores the production completion state on success as well.
        connection.batch_execute("SAVEPOINT identity_backfill_fixture;
            UPDATE hubuum_control.identity_backfill SET completed = FALSE;
            ALTER TABLE public.collections DISABLE TRIGGER zz_logical_identity;
            ALTER TABLE public.collections_history DISABLE TRIGGER zz_logical_identity;").await?;
        let name = format!("identity_backfill_{}", Uuid::new_v4().simple());
        let id = diesel::sql_query("INSERT INTO public.collections(name, description, parent_collection_id)
            SELECT $1, 'before', id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id AS count")
            .bind::<Text, _>(name).get_result::<Count>(connection).await?.count;
        diesel::sql_query("UPDATE public.collections SET description = 'after' WHERE id = $1")
            .bind::<Integer, _>(id).execute(connection).await?;
        if deleted {
            diesel::sql_query("DELETE FROM public.collections WHERE id = $1")
                .bind::<Integer, _>(id).execute(connection).await?;
        }
        connection.batch_execute("ALTER TABLE public.collections ENABLE TRIGGER zz_logical_identity;
            ALTER TABLE public.collections_history ENABLE TRIGGER zz_logical_identity;").await?;
        let mut changed = 0;
        loop {
            let count = diesel::sql_query("SELECT public.hubuum_backfill_logical_identities(1) AS count")
                .get_result::<Count>(connection).await?.count;
            assert!((0..=1).contains(&count), "one-row batches must remain bounded");
            changed += count;
            if count == 0 { break; }
        }
        assert_eq!(changed, 3, "two live versions plus live row, or three history-only versions");
        let before = diesel::sql_query("SELECT history_id, revision, logical_identity AS evidence
            FROM public.collections_history WHERE id = $1 ORDER BY history_id")
            .bind::<Integer, _>(id).load::<RetainedVersion>(connection).await?;
        connection.batch_execute("SELECT public.hubuum_validate_logical_identities();
            SELECT public.hubuum_backfill_logical_identities(1);").await?;
        let after = diesel::sql_query("SELECT history_id, revision, logical_identity AS evidence
            FROM public.collections_history WHERE id = $1 ORDER BY history_id")
            .bind::<Integer, _>(id).load::<RetainedVersion>(connection).await?;
        assert_eq!(before.len(), after.len());
        let mut identities = Vec::new();
        for (before, after) in before.iter().zip(&after) {
            assert_eq!((before.history_id, before.revision, &before.evidence), (after.history_id, after.revision, &after.evidence));
            let evidence: StorageIdentityEvidence = serde_json::from_value(after.evidence.clone()).unwrap();
            identities.push(evidence.subject().unwrap().record_identity().unwrap());
        }
        assert!(identities.iter().all(|identity| identity.resource() == identities[0].resource()));
        let versions = identities.iter().map(|identity| identity.version()).collect::<std::collections::BTreeSet<_>>();
        assert_eq!(versions.len(), identities.len());
        connection.batch_execute("ROLLBACK TO SAVEPOINT identity_backfill_fixture").await?;
        Ok(())
    }).await.unwrap();
}

/// Race the first update of an unassigned incarnation against bounded backfill.
/// Both paths must use the same uniqueness-enforced legacy mapping.
#[tokio::test]
async fn concurrent_writes_and_backfill_share_one_incarnation() {
    let _serial = BACKFILL_FIXTURES.lock().await;
    let split = database_role_tests_enabled();
    let owner = if split {
        integration_test_migration_pool(2)
    } else {
        integration_test_pool(2)
    };
    let runtime = integration_test_pool(1);
    let id = with_transaction(&owner, async |connection| -> Result<i32, PostgresStorageError> {
        assume_owner(connection).await?;
        connection.batch_execute("ALTER TABLE public.collections DISABLE TRIGGER zz_logical_identity;
            ALTER TABLE public.collections_history DISABLE TRIGGER zz_logical_identity;").await?;
        let id = diesel::sql_query("INSERT INTO public.collections(name, description, parent_collection_id)
            SELECT $1, 'legacy', id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id AS count")
            .bind::<Text, _>(format!("identity_race_{}", Uuid::new_v4().simple()))
            .get_result::<Count>(connection).await?.count;
        connection.batch_execute("ALTER TABLE public.collections ENABLE TRIGGER zz_logical_identity;
            ALTER TABLE public.collections_history ENABLE TRIGGER zz_logical_identity;
            UPDATE hubuum_control.identity_backfill SET completed = FALSE;").await?;
        Ok(id)
    }).await.unwrap();
    let result = async {
        let backfill = async {
            for _ in 0..8 {
                with_transaction(
                    &owner,
                    async |connection| -> Result<(), PostgresStorageError> {
                        assume_owner(connection).await?;
                        connection
                            .batch_execute("SELECT public.hubuum_backfill_logical_identities(1)")
                            .await?;
                        Ok(())
                    },
                )
                .await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, PostgresStorageError>(())
        };
        let writes = async {
            for revision in 0..8 {
                with_transaction(
                    &runtime,
                    async |connection| -> Result<(), PostgresStorageError> {
                        diesel::sql_query(
                            "UPDATE public.collections SET description = $2 WHERE id = $1",
                        )
                        .bind::<Integer, _>(id)
                        .bind::<Text, _>(format!("write {revision}"))
                        .execute(connection)
                        .await?;
                        Ok(())
                    },
                )
                .await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, PostgresStorageError>(())
        };
        let (backfilled, written) = tokio::join!(backfill, writes);
        backfilled?;
        written?;
        with_transaction(
            &owner,
            async |connection| -> Result<Vec<RetainedVersion>, PostgresStorageError> {
                assume_owner(connection).await?;
                loop {
                    let count = diesel::sql_query(
                        "SELECT public.hubuum_backfill_logical_identities(1) AS count",
                    )
                    .get_result::<Count>(connection)
                    .await?
                    .count;
                    if count == 0 {
                        break;
                    }
                }
                connection
                    .batch_execute("SELECT public.hubuum_validate_logical_identities()")
                    .await?;
                Ok(diesel::sql_query(
                    "SELECT history_id, revision, logical_identity AS evidence
                FROM public.collections_history WHERE id = $1 ORDER BY history_id",
                )
                .bind::<Integer, _>(id)
                .load::<RetainedVersion>(connection)
                .await?)
            },
        )
        .await
    }
    .await;
    // Cleanup runs before checking the race result, including on database errors.
    with_transaction(
        &owner,
        async |connection| -> Result<(), PostgresStorageError> {
            assume_owner(connection).await?;
            diesel::sql_query("DELETE FROM public.collections WHERE id = $1")
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            diesel::sql_query("DELETE FROM public.collections_history WHERE id = $1")
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            connection
                .batch_execute(
                    "SELECT public.hubuum_backfill_logical_identities(1000);
            SELECT public.hubuum_validate_logical_identities()",
                )
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
    let versions = result
        .unwrap()
        .iter()
        .map(|row| {
            serde_json::from_value::<StorageIdentityEvidence>(row.evidence.clone())
                .unwrap()
                .subject()
                .unwrap()
                .record_identity()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let resources = versions
        .iter()
        .map(|version| version.resource())
        .collect::<std::collections::BTreeSet<_>>();
    let distinct_versions = versions
        .iter()
        .map(|version| version.version())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        (resources.len(), versions.len(), distinct_versions.len()),
        (1, 9, 9)
    );
}

static BACKFILL_FIXTURES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn assume_owner(
    connection: &mut hubuum_storage_postgres::PostgresConnection,
) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        let roles = integration_test_database_roles();
        connection
            .batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
            .await?;
    }
    Ok(())
}
