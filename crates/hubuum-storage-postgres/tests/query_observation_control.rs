#![cfg(feature = "integration-test-support")]

use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use futures_util::StreamExt;
use hubuum_storage_core::{QueryUsageStorage, StorageNotification, StorageQueryObservationControl};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_database_url,
    integration_test_migration_pool, integration_test_pool,
};
use hubuum_storage_postgres::worker_notifications::channel_name;
use hubuum_storage_postgres::{
    PostgresPool, PostgresPoolSettings, PostgresStorage, PostgresStorageError, build_postgres_pool,
    with_transaction,
};
use rstest::rstest;
use std::time::Duration;
use uuid::Uuid;

const UP: &str = include_str!("../migrations/2026-10-19-000001_query_observation_control/up.sql");
const DOWN: &str =
    include_str!("../migrations/2026-10-19-000001_query_observation_control/down.sql");

/// Isolate the deployment singleton from the parallel suite while exercising
/// the production adapter through independent runtime-role connection pools.
struct Fixture {
    schema: String,
    migration_pool: PostgresPool,
}

impl Fixture {
    async fn new() -> Self {
        let split = database_role_tests_enabled();
        let fixture = Self {
            schema: format!("query_control_{}", Uuid::new_v4().simple()),
            migration_pool: if split {
                integration_test_migration_pool(1)
            } else {
                integration_test_pool(1)
            },
        };
        with_transaction(&fixture.migration_pool, async |connection| -> Result<(), PostgresStorageError> {
            if split {
                connection.batch_execute(&format!("SET LOCAL ROLE \"{}\"", integration_test_database_roles().owner().as_str())).await?;
            }
            connection.batch_execute(&format!("CREATE SCHEMA {}; SET LOCAL search_path TO {}", fixture.schema, fixture.schema)).await?;
            connection.batch_execute(UP).await?;
            // Exercise both migration directions before testing runtime access.
            connection.batch_execute(DOWN).await?;
            connection.batch_execute(UP).await?;
            if split {
                connection.batch_execute(&format!(
                    "GRANT USAGE ON SCHEMA {} TO \"{}\"; GRANT SELECT, UPDATE ON query_observation_control TO \"{}\"",
                    fixture.schema,
                    integration_test_database_roles().runtime().as_str(),
                    integration_test_database_roles().runtime().as_str(),
                )).await?;
            }
            Ok(())
        }).await.unwrap();
        fixture
    }

    fn pool(&self) -> PostgresPool {
        let url = integration_test_database_url();
        let separator = if url.contains('?') { '&' } else { '?' };
        let settings = PostgresPoolSettings::builder(format!(
            "{url}{separator}options=-csearch_path%3D{}",
            self.schema,
        ))
        .max_size(1)
        .acquire_timeout_ms(5000)
        .build()
        .unwrap();
        build_postgres_pool(&settings).unwrap()
    }

    fn replica(&self) -> PostgresStorage {
        PostgresStorage::unobserved(self.pool())
    }

    async fn cleanup(self) {
        with_transaction(
            &self.migration_pool,
            async |connection| -> Result<(), PostgresStorageError> {
                if database_role_tests_enabled() {
                    connection
                        .batch_execute(&format!(
                            "SET LOCAL ROLE \"{}\"",
                            integration_test_database_roles().owner().as_str()
                        ))
                        .await?;
                }
                connection
                    .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
                    .await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    }
}

#[rstest]
#[case::enable(StorageQueryObservationControl::Enabled)]
#[case::disable(StorageQueryObservationControl::Disabled)]
#[tokio::test]
async fn override_is_shared_and_survives_reconnecting(
    #[case] control: StorageQueryObservationControl,
) {
    let fixture = Fixture::new().await;
    let writer = fixture.replica();
    let reader = fixture.replica();
    assert_eq!(
        reader.query_observation_control().await.unwrap(),
        StorageQueryObservationControl::Startup
    );
    writer.set_query_observation_control(control).await.unwrap();
    drop(writer);
    assert_eq!(reader.query_observation_control().await.unwrap(), control);
    drop(reader);
    assert_eq!(
        fixture.replica().query_observation_control().await.unwrap(),
        control
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn later_committed_toggle_replaces_previous_value() {
    let fixture = Fixture::new().await;
    let first = fixture.replica();
    let second = fixture.replica();
    first
        .set_query_observation_control(StorageQueryObservationControl::Enabled)
        .await
        .unwrap();
    second
        .set_query_observation_control(StorageQueryObservationControl::Disabled)
        .await
        .unwrap();
    assert_eq!(
        first.query_observation_control().await.unwrap(),
        StorageQueryObservationControl::Disabled
    );
    fixture.cleanup().await;
}

#[rstest]
#[case::enable(StorageQueryObservationControl::Enabled)]
#[case::disable(StorageQueryObservationControl::Disabled)]
#[tokio::test]
async fn committed_toggle_notifies_every_connected_replica(
    #[case] control: StorageQueryObservationControl,
) {
    let fixture = Fixture::new().await;
    let first_pool = fixture.pool();
    let second_pool = fixture.pool();
    let mut first = first_pool.get().await.unwrap();
    let mut second = second_pool.get().await.unwrap();
    let channel = channel_name(StorageNotification::QueryObservationControl);
    for connection in [&mut first, &mut second] {
        diesel::sql_query(format!("LISTEN {channel}"))
            .execute(&mut **connection)
            .await
            .unwrap();
    }
    let writer = fixture.replica();
    writer.set_query_observation_control(control).await.unwrap();
    for connection in [&mut first, &mut second] {
        let notifications = connection.notifications_stream();
        futures_util::pin_mut!(notifications);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notification = notifications.next().await.unwrap().unwrap();
                if notification.channel == channel {
                    break;
                }
            }
        })
        .await
        .expect("each replica must receive the committed control change");
    }
    assert_eq!(
        fixture.replica().query_observation_control().await.unwrap(),
        control
    );
    drop((first, second));
    fixture.cleanup().await;
}
