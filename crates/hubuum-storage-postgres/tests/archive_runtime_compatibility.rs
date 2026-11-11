#![cfg(feature = "integration-test-support")]

use diesel::sql_query;
use diesel::sql_types::{BigInt, Bool, Text, Uuid as SqlUuid};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::{DeletionControlStorage, StorageExecutionPermit};
use hubuum_storage_postgres::test_support::{
    IsolatedRestoreTarget, database_role_tests_enabled, integration_test_database_roles,
    integration_test_database_url, integration_test_restore_database_url,
    integration_test_restore_migration_database_url,
};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresPool, PostgresPoolSettings, PostgresStorage, PostgresStorageError,
    build_postgres_pool, with_transaction,
};
use rstest::rstest;
use tokio::sync::Mutex;

const UP: &str =
    include_str!("../migrations/2026-11-11-000001_archive_runtime_compatibility/up.sql");
const DOWN: &str =
    include_str!("../migrations/2026-11-11-000001_archive_runtime_compatibility/down.sql");
const ROLLBACK: &str = "Rollback-only archive compatibility fixture";
// These cases alter one shared function in the runner's separate restore
// database. Serialize only these migration fixtures; production concurrency
// contracts retain their normal parallel coverage in the other test binaries.
static MIGRATION_FIXTURE: Mutex<()> = Mutex::const_new(());

#[derive(diesel::QueryableByName)]
struct Allowed {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

#[derive(diesel::QueryableByName)]
struct Definition {
    #[diesel(sql_type = Text)]
    definition: String,
}

fn pool(database_url: String) -> PostgresPool {
    build_postgres_pool(
        &PostgresPoolSettings::builder(database_url)
            .max_size(2)
            .statement_timeout_ms(10_000)
            .acquire_timeout_ms(5_000)
            .build()
            .unwrap(),
    )
    .unwrap()
}

struct Fixture {
    admin: PostgresPool,
    backend: PostgresStorage,
}

impl Fixture {
    fn new() -> Self {
        let migration_url = integration_test_restore_migration_database_url();
        IsolatedRestoreTarget::new(&migration_url, &integration_test_database_url()).unwrap();
        Self {
            admin: pool(migration_url),
            backend: PostgresStorage::unobserved(pool(integration_test_restore_database_url())),
        }
    }
}

async fn become_owner(connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        sql_query("SELECT set_config('role', $1, true)")
            .bind::<Text, _>(integration_test_database_roles().owner().as_str())
            .execute(connection)
            .await?;
    }
    // Match the production authority-before-registry lock order before DDL.
    sql_query("SELECT public.hubuum_deletion_execution_allowed() AS allowed")
        .get_result::<Allowed>(connection)
        .await?;
    Ok(())
}

async fn entry(
    connection: &mut PostgresConnection,
    permit: &StorageExecutionPermit,
) -> Result<bool, diesel::result::Error> {
    let owner = permit.owner().expect("registered fixture owner");
    sql_query("SELECT public.hubuum_archive_history_entry($1,$2,$3,$4) AS allowed")
        .bind::<SqlUuid, _>(owner.id())
        .bind::<SqlUuid, _>(owner.ownership_token())
        .bind::<SqlUuid, _>(permit.fence().incarnation())
        .bind::<BigInt, _>(permit.fence().revision())
        .get_result::<Allowed>(connection)
        .await
        .map(|row| row.allowed)
}

async fn definition(connection: &mut PostgresConnection) -> Result<String, PostgresStorageError> {
    Ok(sql_query("SELECT pg_get_functiondef('public.hubuum_archive_history_entry(uuid,uuid,uuid,bigint)'::regprocedure) AS definition")
        .get_result::<Definition>(connection).await?.definition)
}

#[rstest]
#[case::upgrade_legacy(true, false)]
#[case::downgrade_legacy(false, false)]
#[case::upgrade_versioned(true, true)]
#[case::downgrade_versioned(false, true)]
#[tokio::test]
async fn compatibility_migration_only_changes_unversioned_legacy_guards(
    #[case] upgrade: bool,
    #[case] versioned: bool,
) {
    let _serial = MIGRATION_FIXTURE.lock().await;
    let fixture = Fixture::new();
    let registration = fixture.backend.register_runtime().await.unwrap();
    let permit = fixture
        .backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let mut observed = None;
    let result = with_transaction(&fixture.admin, async |connection| {
        become_owner(connection).await?;
        if versioned {
            connection.batch_execute("ALTER TABLE hubuum_control.runtime_registrations ADD COLUMN history_protocol integer NOT NULL DEFAULT 0").await?;
        } else if upgrade {
            connection.batch_execute(DOWN).await?;
        }
        let before = definition(connection).await?;
        connection.batch_execute(if upgrade { UP } else { DOWN }).await?;
        let after = definition(connection).await?;
        connection.batch_execute("SAVEPOINT entry_check").await?;
        let allowed = entry(connection, &permit).await.map_err(|error| error.to_string());
        connection.batch_execute("ROLLBACK TO SAVEPOINT entry_check").await?;
        observed = Some((before, after, allowed));
        Err::<(), _>(PostgresStorageError::invalid_input(ROLLBACK))
    }).await;
    fixture
        .backend
        .unregister_runtime(registration)
        .await
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), ROLLBACK);
    let (before, after, allowed) = observed.unwrap();
    if versioned {
        assert_eq!(
            before, after,
            "An installed versioned protocol must remain unchanged"
        );
    } else if upgrade {
        assert!(allowed.unwrap());
    } else {
        assert!(
            allowed
                .unwrap_err()
                .contains("Unversioned archive runtime schema is not supported")
        );
    }
}

#[tokio::test]
async fn unversioned_archive_guard_rejects_unknown_future_checkpoints() {
    let _serial = MIGRATION_FIXTURE.lock().await;
    let fixture = Fixture::new();
    let registration = fixture.backend.register_runtime().await.unwrap();
    let permit = fixture
        .backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let mut rejection = None;
    let result = with_transaction(&fixture.admin, async |connection| {
        become_owner(connection).await?;
        sql_query("INSERT INTO public.__diesel_schema_migrations(version) VALUES($1)")
            .bind::<Text, _>("20990101000001")
            .execute(connection)
            .await?;
        connection.batch_execute("SAVEPOINT entry_check").await?;
        rejection = Some(
            entry(connection, &permit)
                .await
                .map_err(|error| error.to_string()),
        );
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT entry_check")
            .await?;
        Err::<(), _>(PostgresStorageError::invalid_input(ROLLBACK))
    })
    .await;
    fixture
        .backend
        .unregister_runtime(registration)
        .await
        .unwrap();
    assert_eq!(result.unwrap_err().to_string(), ROLLBACK);
    assert!(
        rejection
            .unwrap()
            .unwrap_err()
            .contains("Unversioned archive runtime schema is not supported")
    );
}
