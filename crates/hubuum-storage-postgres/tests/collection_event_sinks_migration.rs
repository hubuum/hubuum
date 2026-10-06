#![cfg(feature = "integration-test-support")]

use diesel::{QueryableByName, sql_types::Integer};
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use uuid::Uuid;

async fn fixture(
    connection: &mut AsyncPgConnection,
    schema: &str,
) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        let roles = integration_test_database_roles();
        connection
            .batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
            .await?;
    }
    connection.batch_execute(&format!(
        "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};
         CREATE TABLE collections (id integer PRIMARY KEY);
         CREATE TABLE event_sinks (id integer PRIMARY KEY, kind text NOT NULL, config jsonb NOT NULL, secret_ref text);
         CREATE TABLE event_subscriptions (id integer PRIMARY KEY, sink_id integer, collection_id integer);
         INSERT INTO collections VALUES (1),(2);
         INSERT INTO event_sinks VALUES (1,'webhook','{{}}',NULL),(2,'webhook','{{}}',NULL);
         INSERT INTO event_subscriptions VALUES (1,1,1),(2,1,1),(3,1,NULL);"
    )).await?;
    connection
        .batch_execute(include_str!(
            "../migrations/2026-10-05-000001_collection_event_sinks/up.sql"
        ))
        .await?;
    Ok(())
}

#[tokio::test]
async fn migration_grants_only_existing_collection_sink_pairs() {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("collection_sinks_{}", Uuid::new_v4().simple());
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            fixture(connection, &schema).await?;
            #[derive(QueryableByName)]
            struct Grant {
                #[diesel(sql_type = Integer)]
                sink_id: i32,
                #[diesel(sql_type = Integer)]
                collection_id: i32,
            }
            let grants = diesel::sql_query(
                "SELECT sink_id, collection_id FROM event_sink_collection_grants",
            )
            .load::<Grant>(connection)
            .await?;
            assert_eq!(
                grants
                    .into_iter()
                    .map(|grant| (grant.sink_id, grant.collection_id))
                    .collect::<Vec<_>>(),
                [(1, 1)]
            );
            connection
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[rstest]
#[case::no_owned_sinks(false)]
#[case::owned_sink(true)]
#[tokio::test]
async fn rollback_preserves_collection_ownership_boundary(#[case] owned: bool) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("collection_sink_rollback_{}", Uuid::new_v4().simple());
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        fixture(connection, &schema).await?;
        if owned {
            connection.batch_execute("UPDATE event_sinks SET collection_id=1, config='{\"destination_url\":\"https://example.test/hook\"}' WHERE id=2").await?;
        }
        connection.batch_execute("SAVEPOINT rollback_probe").await?;
        let result = connection.batch_execute(include_str!("../migrations/2026-10-05-000001_collection_event_sinks/down.sql")).await;
        if owned {
            assert!(result.unwrap_err().to_string().contains("Remove collection-owned event sinks"));
            connection.batch_execute("ROLLBACK TO SAVEPOINT rollback_probe").await?;
        } else {
            result?;
        }
        connection.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await?;
        Ok(())
    }).await.unwrap();
}
