use diesel::RunQueryDsl;
use diesel::connection::SimpleConnection;
use diesel::deserialize::QueryableByName;
use diesel::migration::MigrationSource;
use diesel::pg::Pg;
use diesel::sql_types::{BigInt, Text};
use diesel::{Connection, PgConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use hubuum_storage_core::StorageError;

use crate::{DatabaseRoleNames, PostgresStorageError, database_role_reconciliation_sql};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

// Keep this set aligned with the hash-pinned offline migration reviews.
const OFFLINE_MIGRATIONS: &[&str] = &["20261001000001", "20261005000001"];

/// Whether pending migrations permit the previous API to remain online.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationMode {
    Rolling,
    Offline,
}

#[derive(QueryableByName)]
struct AppliedMigration {
    #[diesel(sql_type = Text)]
    version: String,
}

/// Inspect migration history without modifying the database. Unknown applied
/// migrations reject a downgrade rather than treating it as a rolling update.
pub fn inspect_migration_mode(
    connection_url: &str,
    roles: Option<&DatabaseRoleNames>,
) -> Result<MigrationMode, StorageError> {
    let mut connection = PgConnection::establish(connection_url)
        .map_err(|error| PostgresStorageError::database(error.to_string()))?;
    if let Some(roles) = roles {
        connection
            .batch_execute(&format!("SET ROLE {};", roles.owner().quoted()))
            .map_err(PostgresStorageError::from)?;
    }
    let markers = diesel::sql_query(
        "SELECT to_regclass('public.__diesel_schema_migrations') IS NOT NULL AS has_migrations, \
         to_regclass('public.collections') IS NOT NULL AS has_collections",
    )
    .get_result::<DisposableRestoreMarkers>(&mut connection)
    .map_err(PostgresStorageError::from)?;
    let applied = if markers.has_migrations {
        diesel::sql_query("SELECT version::text FROM public.__diesel_schema_migrations")
            .load::<AppliedMigration>(&mut connection)
            .map_err(PostgresStorageError::from)?
            .into_iter()
            .map(|row| row.version)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let known = <EmbeddedMigrations as MigrationSource<Pg>>::migrations(&MIGRATIONS)
        .map_err(|error| PostgresStorageError::database(error.to_string()))?
        .into_iter()
        .map(|migration| migration.name().version().to_string())
        .collect::<Vec<_>>();
    migration_mode_for_versions(&applied, &known).map_err(StorageError::from)
}

fn migration_mode_for_versions(
    applied: &[String],
    known: &[String],
) -> Result<MigrationMode, PostgresStorageError> {
    if applied.iter().any(|version| !known.contains(version)) {
        return Err(PostgresStorageError::database(
            "Database contains migrations unknown to this binary; restore a pre-upgrade database snapshot before downgrading",
        ));
    }
    Ok(
        if OFFLINE_MIGRATIONS
            .iter()
            .any(|version| !applied.iter().any(|applied| applied == version))
        {
            MigrationMode::Offline
        } else {
            MigrationMode::Rolling
        },
    )
}

#[derive(QueryableByName)]
struct DisposableDatabaseState {
    #[diesel(sql_type = Text)]
    database_name: String,
    #[diesel(sql_type = BigInt)]
    user_object_count: i64,
}

#[derive(QueryableByName)]
struct DisposableRestoreMarkers {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    has_migrations: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    has_collections: bool,
}

/// Apply every embedded PostgreSQL migration not yet recorded by Diesel.
pub fn run_embedded_migrations(connection_url: &str) -> Result<usize, StorageError> {
    run_embedded_migrations_with_roles(connection_url, None)
}

/// Apply migrations as the schema-owner role and reconcile runtime grants.
pub fn run_embedded_migrations_as(
    connection_url: &str,
    roles: &DatabaseRoleNames,
) -> Result<usize, StorageError> {
    run_embedded_migrations_with_roles(connection_url, Some(roles))
}

/// Require a genuinely empty, non-maintenance database and migrate it for an
/// isolated restore verification. The emptiness check and migrations share
/// one connection so a caller cannot accidentally point this path at an
/// already initialized Hubuum database.
pub fn prepare_disposable_restore_database(connection_url: &str) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect to the disposable restore-test database: {error}"
        )))
    })?;
    let state = diesel::sql_query(
        "SELECT current_database()::text AS database_name, COUNT(*)::bigint AS user_object_count \
         FROM ( \
           SELECT relation.oid \
           FROM pg_catalog.pg_class AS relation \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
             AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'c') \
           UNION ALL \
           SELECT routine.oid \
           FROM pg_catalog.pg_proc AS routine \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = routine.pronamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
           UNION ALL \
           SELECT type.oid \
           FROM pg_catalog.pg_type AS type \
           JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = type.typnamespace \
           WHERE namespace.nspname <> 'information_schema' \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
             AND type.typtype IN ('c', 'd', 'e', 'm', 'r') \
             AND type.typrelid = 0 \
           UNION ALL \
           SELECT namespace.oid \
           FROM pg_catalog.pg_namespace AS namespace \
           WHERE namespace.nspname NOT IN ('information_schema', 'public') \
             AND namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\' \
         ) AS user_objects",
    )
    .get_result::<DisposableDatabaseState>(&mut connection)
    .map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to inspect the disposable restore-test database: {error}"
        )))
    })?;
    if matches!(
        state.database_name.as_str(),
        "postgres" | "template0" | "template1"
    ) {
        return Err(StorageError::invalid_input(
            "Refused to use a PostgreSQL maintenance database for restore verification",
        ));
    }
    if state.user_object_count != 0 {
        return Err(StorageError::invalid_input(format!(
            "Refused restore verification because the target database contains {} user object(s); provide a newly created empty disposable database",
            state.user_object_count
        )));
    }
    connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to migrate the disposable restore-test database: {error}"
            )))
        })
        .map(|migrations| migrations.len())
}

/// Remove the Hubuum schema created by [`prepare_disposable_restore_database`]
/// after an isolated restore test. This deliberately refuses databases that
/// do not contain both expected Hubuum marker tables.
pub fn reset_disposable_restore_database(connection_url: &str) -> Result<(), StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect for disposable restore-test cleanup: {error}"
        )))
    })?;
    let markers = diesel::sql_query(
        "SELECT to_regclass('public.__diesel_schema_migrations') IS NOT NULL AS has_migrations, \
                to_regclass('public.collections') IS NOT NULL AS has_collections",
    )
    .get_result::<DisposableRestoreMarkers>(&mut connection)
    .map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to inspect disposable restore-test cleanup markers: {error}"
        )))
    })?;
    if !markers.has_migrations || !markers.has_collections {
        return Err(StorageError::invalid_input(
            "Refused disposable restore-test cleanup because Hubuum schema markers are missing",
        ));
    }
    connection
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to reset the disposable restore-test database: {error}"
            )))
        })
}

fn run_embedded_migrations_with_roles(
    connection_url: &str,
    roles: Option<&DatabaseRoleNames>,
) -> Result<usize, StorageError> {
    let mut connection = PgConnection::establish(connection_url).map_err(|error| {
        StorageError::from(PostgresStorageError::database(format!(
            "failed to connect for storage migrations: {error}"
        )))
    })?;
    if let Some(roles) = roles {
        connection
            .batch_execute(&format!("SET ROLE {};", roles.owner().quoted()))
            .map_err(|error| {
                StorageError::from(PostgresStorageError::database(format!(
                    "failed to assume database schema-owner role '{}': {error}",
                    roles.owner().as_str()
                )))
            })?;
    }
    let applied_count = connection
        .run_pending_migrations(MIGRATIONS)
        .map_err(|error| {
            StorageError::from(PostgresStorageError::database(format!(
                "failed to run storage migrations: {error}"
            )))
        })?
        .len();
    if let Some(roles) = roles {
        connection
            .batch_execute(&database_role_reconciliation_sql(roles))
            .map_err(|error| {
                StorageError::from(PostgresStorageError::database(format!(
                    "failed to reconcile database ownership and runtime grants: {error}"
                )))
            })?;
    }
    Ok(applied_count)
}

#[cfg(test)]
mod tests {
    use super::{MigrationMode, OFFLINE_MIGRATIONS, migration_mode_for_versions};
    use rstest::rstest;

    #[rstest]
    #[case::fresh(vec![], MigrationMode::Offline)]
    #[case::previous(vec!["20260919000001"], MigrationMode::Offline)]
    #[case::before_collection_sinks(vec!["20260919000001", "20261001000001"], MigrationMode::Offline)]
    #[case::current(vec!["20260919000001", "20261001000001", "20261005000001"], MigrationMode::Rolling)]
    fn pending_offline_migrations_determine_mode(
        #[case] applied: Vec<&str>,
        #[case] expected: MigrationMode,
    ) {
        let applied = applied.into_iter().map(str::to_owned).collect::<Vec<_>>();
        let known = ["20260919000001", "20261001000001", "20261005000001"].map(str::to_owned);
        assert_eq!(
            migration_mode_for_versions(&applied, &known).unwrap(),
            expected
        );
    }

    #[test]
    fn unknown_applied_migrations_reject_downgrades() {
        let error = migration_mode_for_versions(&["20990101000001".to_string()], &[])
            .expect_err("unknown database history must fail closed");
        assert!(error.to_string().contains("pre-upgrade database snapshot"));
    }

    #[test]
    fn offline_migrations_match_reviewed_policy() {
        let reviews: serde_json::Value = serde_json::from_str(include_str!(
            "../../../.github/migration-offline-reviews.json"
        ))
        .unwrap();
        let versions = reviews["reviews"]
            .as_array()
            .unwrap()
            .iter()
            .map(|review| {
                let path = review["migration"].as_str().unwrap();
                let directory = path.split('/').nth_back(1).unwrap();
                directory.split('_').next().unwrap().replace('-', "")
            })
            .collect::<Vec<_>>();
        assert_eq!(versions, OFFLINE_MIGRATIONS);
    }
}
