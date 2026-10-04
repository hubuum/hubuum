#![cfg(feature = "integration-test-support")]

use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresStorageError, with_transaction};
use rstest::rstest;
use uuid::Uuid;

async fn create_fixture(
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
         CREATE TABLE events (id bigint PRIMARY KEY, entity_type text, action text, dispatched_at timestamp);
         CREATE TABLE event_sinks (id integer PRIMARY KEY, kind text, config jsonb NOT NULL DEFAULT '{{}}',
             CONSTRAINT event_sinks_kind_check CHECK (kind IN ('webhook','amqp','valkey_stream','email')));
         CREATE TABLE event_subscriptions (id integer PRIMARY KEY, name text, collection_id integer NOT NULL,
             sink_id integer REFERENCES event_sinks(id) ON DELETE CASCADE);
         CREATE TABLE event_deliveries (id bigint PRIMARY KEY,
             event_id bigint REFERENCES events(id) ON DELETE CASCADE,
             subscription_id integer REFERENCES event_subscriptions(id) ON DELETE CASCADE,
             UNIQUE(event_id,subscription_id));"
    )).await?;
    connection
        .batch_execute(include_str!(
            "../migrations/2026-10-01-000001_webhook_notifications/up.sql"
        ))
        .await?;
    Ok(())
}

#[rstest]
#[case::undispatched(false)]
#[case::dispatched(true)]
#[tokio::test]
async fn rollback_rejects_test_audits_after_sink_deletion(#[case] dispatched: bool) {
    let split_roles = database_role_tests_enabled();
    let pool = if split_roles {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("webhook_migration_{}", Uuid::new_v4().simple());
    with_transaction(&pool, async |connection| -> Result<(), PostgresStorageError> {
        create_fixture(connection, &schema).await?;
        diesel::sql_query("INSERT INTO events VALUES (1,'event_sink','invoked',CASE WHEN $1 THEN now() END)")
            .bind::<diesel::sql_types::Bool,_>(dispatched).execute(connection).await?;
        connection.batch_execute(
            "INSERT INTO event_sinks (id,kind) VALUES (1,'webhook');
             INSERT INTO event_subscriptions VALUES (1,'test',NULL,1);
             INSERT INTO event_deliveries (id,event_id,subscription_id,purpose) VALUES (1,1,1,'test');
             DELETE FROM event_sinks WHERE id=1;
             SAVEPOINT rollback_probe;"
        ).await?;
        let error = connection.batch_execute(include_str!("../migrations/2026-10-01-000001_webhook_notifications/down.sql"))
            .await.expect_err("retained audit events must block rollback");
        assert!(error.to_string().contains("event_sink.invoked audit events remain"), "{error}");
        assert!(error.to_string().contains("archive"), "{error}");
        connection.batch_execute("ROLLBACK TO SAVEPOINT rollback_probe; DELETE FROM events WHERE id=1;").await?;
        connection.batch_execute(include_str!("../migrations/2026-10-01-000001_webhook_notifications/down.sql")).await?;
        connection.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await?;
        Ok(())
    }).await.unwrap();
}

#[rstest]
#[case::legacy_webhook("webhook", serde_json::json!({}), false)]
#[case::webhook_template("webhook", serde_json::json!({"body_template":"{}"}), true)]
#[case::webhook_url_secret("webhook", serde_json::json!({"url_secret_ref":"destination"}), true)]
#[case::webhook_response("webhook", serde_json::json!({"response":{"rate_limit":true}}), true)]
#[case::legacy_email("email", serde_json::json!({"body_template":"{{ summary }}"}), false)]
#[tokio::test]
async fn rollback_only_rejects_incompatible_transport_settings(
    #[case] kind: &str,
    #[case] config: serde_json::Value,
    #[case] incompatible: bool,
) {
    let pool = if database_role_tests_enabled() {
        integration_test_migration_pool(1)
    } else {
        integration_test_pool(1)
    };
    let schema = format!("webhook_config_migration_{}", Uuid::new_v4().simple());
    with_transaction(
        &pool,
        async |connection| -> Result<(), PostgresStorageError> {
            create_fixture(connection, &schema).await?;
            diesel::sql_query("INSERT INTO event_sinks (id,kind,config) VALUES (1,$1,$2)")
                .bind::<diesel::sql_types::Text, _>(kind)
                .bind::<diesel::sql_types::Jsonb, _>(&config)
                .execute(connection)
                .await?;
            connection.batch_execute("SAVEPOINT rollback_probe").await?;
            let result = connection
                .batch_execute(include_str!(
                    "../migrations/2026-10-01-000001_webhook_notifications/down.sql"
                ))
                .await;
            if incompatible {
                let error = result.expect_err("enhanced webhook settings must block rollback");
                assert!(
                    error
                        .to_string()
                        .contains("Remove enhanced webhook configuration"),
                    "{error}"
                );
                connection
                    .batch_execute("ROLLBACK TO SAVEPOINT rollback_probe")
                    .await?;
            } else {
                result.expect("legacy transport settings must allow rollback");
            }
            connection
                .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await?;
            Ok(())
        },
    )
    .await
    .unwrap();
}
