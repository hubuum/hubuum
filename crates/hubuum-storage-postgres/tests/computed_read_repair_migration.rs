#![cfg(feature = "integration-test-support")]

use diesel::sql_types::Jsonb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

#[rstest]
#[case::missing(json!({"type":"computed_read_repairs"}))]
#[case::null(json!({"type":"computed_read_repairs","objects":null}))]
#[case::wrong_shape(json!({"type":"computed_read_repairs","objects":{}}))]
#[case::overflow(json!({"type":"computed_read_repairs","objects":vec![json!({"class_id":1,"object_id":1});4097]}))]
#[tokio::test]
async fn database_rejects_invalid_or_oversized_repair_queues(#[case] payload: Value) {
    let split = database_role_tests_enabled();
    let pool = if split {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("repair_migration_{}", Uuid::new_v4().simple());
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            if split {
                let roles = integration_test_database_roles();
                connection
                    .batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
                    .await?;
            }
            connection
                .batch_execute(&format!(
                    "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};
            CREATE TABLE tasks (id integer PRIMARY KEY, kind text NOT NULL, status text NOT NULL,
                deleted_at timestamp, request_payload jsonb);"
                ))
                .await?;
            connection
                .batch_execute(include_str!(
                    "../migrations/2026-10-25-000001_computed_read_repairs/up.sql"
                ))
                .await?;
            connection.batch_execute("SAVEPOINT invalid_queue").await?;
            let rejected =
                diesel::sql_query("INSERT INTO tasks VALUES (1, 'reindex', 'queued', NULL, $1)")
                    .bind::<Jsonb, _>(payload)
                    .execute(connection)
                    .await;
            assert!(rejected.is_err());
            connection
                .batch_execute("ROLLBACK TO SAVEPOINT invalid_queue")
                .await?;
            connection
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}
