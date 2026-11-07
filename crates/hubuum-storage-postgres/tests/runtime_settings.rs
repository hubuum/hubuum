#![cfg(feature = "integration-test-support")]

use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use futures_util::StreamExt;
use hubuum_domain::ResourceRevision;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    RuntimeSettingsStorage, StorageErrorKind, StorageNotification, StoragePaginationPolicy,
    StorageQueryObservationPolicy, StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
    StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsUpdate,
    StorageRuntimeSettingsUpdateOutcome, StorageRuntimeSettingsValue,
};
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

const LEGACY_UP: &str =
    include_str!("../migrations/2026-10-19-000001_query_observation_control/up.sql");
const UP: &str = include_str!("../migrations/2026-11-06-000001_runtime_settings/up.sql");
const DOWN: &str = include_str!("../migrations/2026-11-06-000001_runtime_settings/down.sql");

/// Isolate the deployment-wide rows from the parallel suite while exercising
/// the production adapter through independent runtime-role connection pools.
struct Fixture {
    schema: String,
    migration_pool: PostgresPool,
}

impl Fixture {
    /// Apply the migration with an optional pre-existing legacy override.
    async fn new(legacy_override: Option<bool>) -> Self {
        let split = database_role_tests_enabled();
        let fixture = Self {
            schema: format!("runtime_settings_{}", Uuid::new_v4().simple()),
            migration_pool: if split {
                integration_test_migration_pool(1)
            } else {
                integration_test_pool(1)
            },
        };
        let legacy = match legacy_override {
            Some(enabled) => format!("UPDATE query_observation_control SET enabled = {enabled};"),
            None => String::new(),
        };
        fixture
            .owner(&format!(
                "CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema}; {LEGACY_UP} {legacy} {UP} {grants}",
                schema = fixture.schema,
                grants = fixture.runtime_grants(),
            ))
            .await
            .unwrap();
        fixture
    }

    fn runtime_grants(&self) -> String {
        if !database_role_tests_enabled() {
            return String::new();
        }
        let runtime = integration_test_database_roles()
            .runtime()
            .as_str()
            .to_owned();
        format!(
            "GRANT USAGE ON SCHEMA {schema} TO \"{runtime}\";
             GRANT SELECT, UPDATE ON runtime_settings TO \"{runtime}\";
             GRANT SELECT, INSERT, UPDATE, DELETE ON runtime_setting_changes TO \"{runtime}\";",
            schema = self.schema,
        )
    }

    /// Run SQL as the schema owner inside the fixture schema.
    async fn owner(&self, sql: &str) -> Result<(), PostgresStorageError> {
        let sql = sql.to_owned();
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
                    .batch_execute(&format!("SET LOCAL search_path TO {}; {sql}", self.schema))
                    .await?;
                Ok(())
            },
        )
        .await
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
        self.owner(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

fn pagination(limit: u64) -> StorageRuntimeSettingsAssignment {
    StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::Pagination(
        StoragePaginationPolicy::try_new(limit).unwrap(),
    ))
}

fn observations(enabled: bool) -> StorageRuntimeSettingsAssignment {
    StorageRuntimeSettingsAssignment::Managed(StorageRuntimeSettingsValue::QueryObservations(
        StorageQueryObservationPolicy::new(enabled),
    ))
}

fn update(
    assignment: StorageRuntimeSettingsAssignment,
    expected: i64,
) -> StorageRuntimeSettingsUpdate {
    StorageRuntimeSettingsUpdate::new(
        assignment,
        ResourceRevision::new(expected).unwrap(),
        EventContext::system(),
    )
}

async fn history(
    storage: &PostgresStorage,
    group: StorageRuntimeSettingsGroup,
) -> Vec<(
    i64,
    StorageRuntimeSettingsAssignment,
    StorageRuntimeSettingsAssignment,
)> {
    storage
        .list_runtime_settings_changes(
            StorageRuntimeSettingsHistoryQuery::try_new(group, None, 100).unwrap(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|change| (change.revision().get(), change.previous(), change.current()))
        .collect()
}

#[rstest]
#[case::enabled(true)]
#[case::disabled(false)]
#[tokio::test]
async fn migration_adopts_an_existing_override_as_an_audited_revision(#[case] enabled: bool) {
    let fixture = Fixture::new(Some(enabled)).await;
    let storage = fixture.replica();
    let snapshot = storage.load_runtime_settings().await.unwrap();
    assert_eq!(
        (
            snapshot.query_observations(),
            snapshot
                .record(StorageRuntimeSettingsGroup::QueryObservations)
                .revision()
                .get(),
            history(&storage, StorageRuntimeSettingsGroup::QueryObservations).await,
        ),
        (
            Some(StorageQueryObservationPolicy::new(enabled)),
            2,
            vec![(
                2,
                StorageRuntimeSettingsAssignment::Deployment(
                    StorageRuntimeSettingsGroup::QueryObservations
                ),
                observations(enabled)
            )]
        )
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn migration_without_an_override_leaves_every_group_to_deployment() {
    let fixture = Fixture::new(None).await;
    let snapshot = fixture.replica().load_runtime_settings().await.unwrap();
    assert_eq!(
        snapshot
            .records()
            .iter()
            .map(|record| (record.assignment().value(), record.revision()))
            .collect::<Vec<_>>(),
        vec![(None, ResourceRevision::INITIAL); 2]
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn down_migration_restores_the_legacy_override() {
    let fixture = Fixture::new(None).await;
    fixture
        .replica()
        .update_runtime_settings(update(observations(true), 1))
        .await
        .unwrap();
    fixture.owner(&format!("{DOWN} {UP}")).await.unwrap();
    assert_eq!(
        fixture
            .replica()
            .load_runtime_settings()
            .await
            .unwrap()
            .query_observations(),
        Some(StorageQueryObservationPolicy::new(true))
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn committed_values_are_shared_and_survive_reconnecting() {
    let fixture = Fixture::new(None).await;
    let writer = fixture.replica();
    let reader = fixture.replica();
    writer
        .update_runtime_settings(update(pagination(12), 1))
        .await
        .unwrap();
    drop(writer);
    let shared = reader.load_runtime_settings().await.unwrap().pagination();
    drop(reader);
    let restarted = fixture
        .replica()
        .load_runtime_settings()
        .await
        .unwrap()
        .pagination();
    assert_eq!(
        (shared, restarted),
        (
            Some(StoragePaginationPolicy::try_new(12).unwrap()),
            Some(StoragePaginationPolicy::try_new(12).unwrap())
        )
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn concurrent_edits_of_one_revision_commit_exactly_once() {
    let fixture = Fixture::new(None).await;
    let first = fixture.replica();
    let second = fixture.replica();
    let (left, right) = tokio::join!(
        first.update_runtime_settings(update(pagination(5), 1)),
        second.update_runtime_settings(update(pagination(6), 1)),
    );
    let outcomes = [left, right];
    assert_eq!(
        (
            outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    Ok(StorageRuntimeSettingsUpdateOutcome::Committed(_))
                ))
                .count(),
            outcomes
                .iter()
                .filter_map(|outcome| outcome.as_ref().err())
                .map(|error| (error.kind(), error.current_revision()))
                .collect::<Vec<_>>(),
            history(&first, StorageRuntimeSettingsGroup::Pagination)
                .await
                .len(),
        ),
        (
            1,
            vec![(
                StorageErrorKind::RevisionConflict,
                Some(ResourceRevision::new(2).unwrap())
            )],
            1
        )
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn a_failed_audit_insert_rolls_back_the_update() {
    let fixture = Fixture::new(None).await;
    // Occupy the next audit key so the adapter's audit insert must fail.
    fixture
        .owner(
            "INSERT INTO runtime_setting_changes (setting_group, revision, changed_at, actor_kind)
             VALUES ('pagination', 2, now(), 'system')",
        )
        .await
        .unwrap();
    let storage = fixture.replica();
    let result = storage
        .update_runtime_settings(update(pagination(8), 1))
        .await;
    let record = storage
        .load_runtime_settings()
        .await
        .unwrap()
        .record(StorageRuntimeSettingsGroup::Pagination)
        .clone();
    assert_eq!(
        (
            result.is_err(),
            record.revision(),
            record.assignment().value()
        ),
        (true, ResourceRevision::INITIAL, None)
    );
    fixture.cleanup().await;
}

#[rstest]
#[case::update("UPDATE runtime_setting_changes SET actor_kind = 'user'")]
#[case::delete("DELETE FROM runtime_setting_changes")]
#[case::truncate("TRUNCATE runtime_setting_changes")]
#[tokio::test]
async fn audit_records_are_append_only(#[case] statement: &str) {
    let fixture = Fixture::new(Some(true)).await;
    assert!(fixture.owner(statement).await.is_err());
    fixture.cleanup().await;
}

#[rstest]
#[case::bad_group("INSERT INTO runtime_settings (setting_group, revision) VALUES ('unknown', 1)")]
#[case::zero_revision("UPDATE runtime_settings SET revision = 0")]
#[case::scalar_value("UPDATE runtime_settings SET value = '5'::jsonb")]
#[case::initial_audit(
    "INSERT INTO runtime_setting_changes (setting_group, revision, changed_at, actor_kind) VALUES ('pagination', 1, now(), 'system')"
)]
#[case::bad_actor(
    "INSERT INTO runtime_setting_changes (setting_group, revision, changed_at, actor_kind) VALUES ('pagination', 9, now(), 'robot')"
)]
#[tokio::test]
async fn constraints_reject_invalid_persisted_state(#[case] statement: &str) {
    let fixture = Fixture::new(None).await;
    assert!(fixture.owner(statement).await.is_err());
    fixture.cleanup().await;
}

#[tokio::test]
async fn committed_changes_notify_every_connected_replica() {
    let fixture = Fixture::new(None).await;
    let channel = channel_name(StorageNotification::RuntimeSettings);
    let first_pool = fixture.pool();
    let second_pool = fixture.pool();
    let mut first = first_pool.get().await.unwrap();
    let mut second = second_pool.get().await.unwrap();
    for connection in [&mut first, &mut second] {
        diesel::sql_query(format!("LISTEN {channel}"))
            .execute(&mut **connection)
            .await
            .unwrap();
    }
    fixture
        .replica()
        .update_runtime_settings(update(observations(true), 1))
        .await
        .unwrap();
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
        .expect("each replica must receive the committed change");
    }
    drop((first, second));
    fixture.cleanup().await;
}
