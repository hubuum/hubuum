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
