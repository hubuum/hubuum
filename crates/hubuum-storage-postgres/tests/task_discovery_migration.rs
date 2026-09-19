#![cfg(feature = "integration-test-support")]

use diesel::QueryableByName;
use diesel::sql_types::{Bool, Jsonb, Nullable};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(QueryableByName)]
struct Discovery {
    #[diesel(sql_type = Nullable<Jsonb>)]
    dry_run: Option<Value>,
}

#[rstest]
#[case::omitted(Some(json!({})), Some(false))]
#[case::null(Some(json!({"dry_run": null})), Some(false))]
#[case::false_value(Some(json!({"dry_run": false})), Some(false))]
#[case::true_value(Some(json!({"dry_run": true})), Some(true))]
#[case::redacted(None, None)]
#[tokio::test]
async fn backfill_recovers_import_dry_run(
    #[case] payload: Option<Value>,
    #[case] expected: Option<bool>,
) {
    let split_roles = database_role_tests_enabled();
    let pool = if split_roles {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("discovery_migration_{}", Uuid::new_v4().simple());
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            if split_roles {
                let roles = integration_test_database_roles();
                connection
                    .batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
                    .await?;
            }
            // An isolated pre-migration schema keeps the full migration independent
            // of concurrent application tests. The transaction cleans up on failure.
            connection
                .batch_execute(&format!(
                    "CREATE SCHEMA {schema};
             SET LOCAL search_path TO {schema};
             CREATE TABLE tasks (id integer PRIMARY KEY, kind text, status text,
                 failed_items integer, request_payload jsonb);
             CREATE TABLE schema_validation_work (task_id integer, class_id integer,
                 schema_revision bigint, kind text);
             CREATE TABLE export_task_outputs (task_id integer, warning_count integer,
                 truncated boolean, output_expires_at timestamp);
             CREATE TABLE backup_task_outputs (task_id integer, output_expires_at timestamp);
             CREATE TABLE remote_call_results (task_id integer, target_id integer,
                 subject_type text, subject_id integer);"
                ))
                .await?;
            diesel::sql_query("INSERT INTO tasks VALUES (1, 'import', 'queued', 0, $1)")
                .bind::<Nullable<Jsonb>, _>(payload)
                .execute(connection)
                .await?;
            connection
                .batch_execute(include_str!(
                    "../migrations/2026-09-18-000001_task_discovery/up.sql"
                ))
                .await?;
            let discovery = diesel::sql_query(
                "SELECT discovery_metadata->'data'->'dry_run' AS dry_run FROM tasks WHERE id = 1",
            )
            .get_result::<Discovery>(connection)
            .await?;
            assert_eq!(discovery.dry_run, expected.map(Value::Bool));
            // Use the discovery search predicate to prove recovered defaults match.
            let matches = diesel::sql_query(
                "SELECT discovery_metadata->'data'->'dry_run' AS dry_run FROM tasks
             WHERE (discovery_metadata->'data'->>'dry_run') = $1::text",
            )
            .bind::<Bool, _>(expected.unwrap_or(false))
            .load::<Discovery>(connection)
            .await?;
            assert_eq!(matches.len(), usize::from(expected.is_some()));
            connection
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}
