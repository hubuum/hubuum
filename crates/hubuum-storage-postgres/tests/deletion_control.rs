#![cfg(feature = "integration-test-support")]

use diesel::sql_types::{Bool, Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::{
    DeletionControlState, DeletionControlStorage, DeletionSubject, StorageErrorKind,
};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresStorage, PostgresStorageError, with_transaction,
};
use serde_json::Value;
use uuid::Uuid;

#[derive(QueryableByName)]
struct Material {
    #[diesel(sql_type = Jsonb)]
    material: Value,
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
