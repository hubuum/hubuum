#![cfg(feature = "integration-test-support")]

use diesel::{
    QueryableByName,
    sql_types::{Bool, Jsonb},
};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use uuid::Uuid;

#[rstest]
#[case::anonymous("none", true, true)]
#[case::bearer("bearer_secret", true, false)]
#[case::basic("basic_secret", true, false)]
#[case::api_key("api_key_secret", true, false)]
#[case::already_disabled("bearer_secret", false, false)]
#[tokio::test]
async fn migration_requires_owner_review_for_existing_credential_bindings(
    #[case] auth_type: &str,
    #[case] enabled: bool,
    #[case] expected: bool,
) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("remote_bindings_{}", Uuid::new_v4().simple());
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            if database_role_tests_enabled() {
                let roles = integration_test_database_roles();
                connection
                    .batch_execute(&format!("SET LOCAL ROLE \"{}\"", roles.owner().as_str()))
                    .await?;
            }
            connection
                .batch_execute(&format!(
                    "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};
             CREATE TABLE remote_targets (enabled boolean NOT NULL, auth_config jsonb NOT NULL);"
                ))
                .await?;
            diesel::sql_query("INSERT INTO remote_targets VALUES ($1, $2)")
                .bind::<Bool, _>(enabled)
                .bind::<Jsonb, _>(serde_json::json!({"type":auth_type}))
                .execute(connection)
                .await?;
            connection
                .batch_execute(include_str!(
                    "../migrations/2026-10-18-000001_remote_credential_bindings/up.sql"
                ))
                .await?;
            connection
                .batch_execute(include_str!(
                    "../migrations/2026-10-18-000001_remote_credential_bindings/down.sql"
                ))
                .await?;
            #[derive(QueryableByName)]
            struct State {
                #[diesel(sql_type = Bool)]
                enabled: bool,
            }
            let state: State = diesel::sql_query("SELECT enabled FROM remote_targets")
                .get_result(connection)
                .await?;
            assert_eq!(state.enabled, expected);
            connection
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}
