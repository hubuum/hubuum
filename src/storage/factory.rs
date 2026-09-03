//! Application composition for complete storage backends.
//!
//! Process entry points supply validated, backend-neutral settings and receive
//! an opaque [`StorageHandle`]. Adapter selection, endpoint diagnostics, and
//! backend-specific initialization errors remain inside this module.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hubuum_storage_core::{StorageCallSite, StorageNotification};
use hubuum_storage_postgres::{
    DatabasePrivilegeFinding as PostgresDatabasePrivilegeFinding,
    DatabasePrivilegeReport as PostgresDatabasePrivilegeReport,
    DatabaseRoleNames as PostgresDatabaseRoleNames, PostgresObserver, PostgresPool,
    PostgresPoolBuildError, PostgresPoolSettings, PostgresStorage, build_postgres_pool,
    inspect_database_privileges,
};
use serde::Serialize;
use tracing::{error, info};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use opentelemetry::trace::{Span as _, Status, Tracer as _};
use opentelemetry::{KeyValue, global};

use super::{
    DatabaseDiagnosticsProvider, DatabasePoolAcquisitions, DatabasePoolCapacity,
    DatabasePoolConnections, DatabasePoolState, DatabaseStorageSnapshot, StorageBackendKind,
    StorageError, StorageErrorKind, StorageHandle,
};

/// Validated names for the database roles used by application composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageDatabaseRoleNames {
    owner: String,
    migrator: String,
    runtime: String,
}

impl StorageDatabaseRoleNames {
    pub(crate) fn new(
        owner: impl Into<String>,
        migrator: impl Into<String>,
        runtime: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let names = Self {
            owner: owner.into(),
            migrator: migrator.into(),
            runtime: runtime.into(),
        };
        names.postgres_names()?;
        Ok(names)
    }

    pub(crate) fn runtime(&self) -> &str {
        &self.runtime
    }

    fn postgres_names(&self) -> Result<PostgresDatabaseRoleNames, StorageError> {
        PostgresDatabaseRoleNames::new(
            self.owner.clone(),
            self.migrator.clone(),
            self.runtime.clone(),
        )
        .map_err(StorageError::from)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StorageDatabaseRole {
    Owner,
    Migrator,
    Runtime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct StorageDatabasePrivilegeFinding {
    code: String,
    object: String,
    detail: String,
}

impl StorageDatabasePrivilegeFinding {
    pub(crate) fn code(&self) -> &str {
        &self.code
    }

    pub(crate) fn object(&self) -> &str {
        &self.object
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct StorageDatabasePrivilegeReport {
    role: String,
    connected_role: String,
    safe: bool,
    dangerous: Vec<StorageDatabasePrivilegeFinding>,
    missing: Vec<StorageDatabasePrivilegeFinding>,
}

impl StorageDatabasePrivilegeReport {
    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub(crate) fn connected_role(&self) -> &str {
        &self.connected_role
    }

    pub(crate) const fn is_safe(&self) -> bool {
        self.safe
    }

    pub(crate) fn dangerous(&self) -> &[StorageDatabasePrivilegeFinding] {
        &self.dangerous
    }

    pub(crate) fn missing(&self) -> &[StorageDatabasePrivilegeFinding] {
        &self.missing
    }
}

impl From<PostgresDatabasePrivilegeReport> for StorageDatabasePrivilegeReport {
    fn from(report: PostgresDatabasePrivilegeReport) -> Self {
        let findings = |source: &[PostgresDatabasePrivilegeFinding]| {
            source
                .iter()
                .map(|finding| StorageDatabasePrivilegeFinding {
                    code: finding.code().to_string(),
                    object: finding.object().to_string(),
                    detail: finding.detail().to_string(),
                })
                .collect::<Vec<_>>()
        };
        Self {
            role: report.role().to_string(),
            connected_role: report.connected_role().to_string(),
            safe: report.is_safe(),
            dangerous: findings(report.dangerous()),
            missing: findings(report.missing()),
        }
    }
}

#[derive(Debug)]
struct ApplicationPostgresObserver;

impl PostgresObserver for ApplicationPostgresObserver {
    fn connection_acquired(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquired(call_site.as_str(), duration);
        record_completed_database_span("db.connection", call_site, "pool_acquire", duration, "ok");
    }

    fn connection_acquisition_failed(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquire_failed(call_site.as_str(), duration);
        record_completed_database_span(
            "db.connection",
            call_site,
            "pool_acquire",
            duration,
            "error",
        );
    }

    fn operation_finished(
        &self,
        call_site: StorageCallSite,
        operation: &'static str,
        duration: Duration,
        error: Option<StorageErrorKind>,
    ) {
        let result = error.map_or(crate::observability::metrics::ResultKind::Ok, |kind| {
            crate::observability::metrics::ResultKind::Error(kind.as_str())
        });
        crate::observability::metrics::db_operation_finished(
            call_site.as_str(),
            operation,
            duration,
            &result,
        );
        record_completed_database_span(
            "db.operation",
            call_site,
            operation,
            duration,
            error.map_or("ok", StorageErrorKind::as_str),
        );
    }

    fn computed_evaluation(&self, scope: &'static str, error_codes: &[&'static str]) {
        crate::observability::metrics::computed_evaluation_summary(scope, error_codes);
    }

    fn computed_live_fallback(&self) {
        crate::observability::metrics::computed_live_fallback();
    }

    fn computed_read_repair(&self, outcome: &'static str) {
        crate::observability::metrics::computed_read_repair(outcome);
    }

    fn revision_condition(&self, outcome: &'static str) {
        crate::observability::metrics::revision_condition(outcome);
    }

    fn task_completed(&self, kind: &'static str, status: &'static str, duration: Option<Duration>) {
        crate::observability::metrics::task_completed(kind, status, duration);
    }

    fn computed_rebuild_finished(&self, outcome: &'static str, duration: Duration) {
        crate::observability::metrics::computed_rebuild_finished(outcome, duration);
    }

    fn computed_rebuild_batch(&self, object_count: usize) {
        crate::observability::metrics::computed_rebuild_batch(object_count);
    }
}

fn record_completed_database_span(
    name: &'static str,
    call_site: StorageCallSite,
    operation: &'static str,
    duration: Duration,
    result: &'static str,
) {
    let tracer = global::tracer("hubuum");
    let ended_at = SystemTime::now();
    let started_at = ended_at.checked_sub(duration).unwrap_or(ended_at);
    let parent = tracing::Span::current().context();
    let mut span = tracer
        .span_builder(name)
        .with_start_time(started_at)
        .with_attributes([
            KeyValue::new("db.caller", call_site.as_str()),
            KeyValue::new("db.operation.category", operation),
            KeyValue::new("db.connection.mode", "pooled"),
            KeyValue::new(
                "db.duration_ms",
                i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
            ),
            KeyValue::new("db.result", result),
        ])
        .start_with_context(&tracer, &parent);
    if result != "ok" {
        span.set_status(Status::error(result));
    }
    span.end_with_timestamp(ended_at);
}

pub(super) fn compose_postgres(pool: PostgresPool) -> PostgresStorage {
    let computed_reindex_batch_size = crate::config::get_config()
        .ok()
        .and_then(|config| NonZeroUsize::new(config.computed_reindex_batch_size))
        .unwrap_or(hubuum_storage_postgres::DEFAULT_COMPUTED_REINDEX_BATCH_SIZE);
    PostgresStorage::new(pool, Arc::new(ApplicationPostgresObserver))
        .with_computed_reindex_batch_size(computed_reindex_batch_size)
}

struct PostgresDatabaseDiagnostics {
    backend: PostgresStorage,
}

#[async_trait::async_trait]
impl DatabaseDiagnosticsProvider for PostgresDatabaseDiagnostics {
    fn pool_state(&self) -> DatabasePoolState {
        let state = self.backend.pool_state();

        DatabasePoolState {
            capacity: DatabasePoolCapacity {
                max_connections: state.max_connections(),
                total_connections: state.total_connections(),
                available_connections: state.available_connections(),
                idle_connections: state.idle_connections(),
                in_use_connections: state.in_use_connections(),
            },
            acquisitions: DatabasePoolAcquisitions {
                pending: state.pending_acquisitions(),
                started: state.acquisitions_started(),
                direct: state.acquisitions_direct(),
                waited: state.acquisitions_waited(),
                timed_out: state.acquisitions_timed_out(),
                wait_time_ms: state.acquisition_wait_time_ms(),
            },
            connections: DatabasePoolConnections {
                created: state.connections_created(),
                closed_broken: state.connections_closed_broken(),
                closed_invalid: state.connections_closed_invalid(),
                closed_max_lifetime: state.connections_closed_max_lifetime(),
                closed_idle_timeout: state.connections_closed_idle_timeout(),
            },
        }
    }

    async fn storage_snapshot(&self) -> Result<DatabaseStorageSnapshot, StorageError> {
        let snapshot = self.backend.storage_snapshot().await?;
        Ok(DatabaseStorageSnapshot {
            active_sessions: snapshot.active_sessions(),
            storage_bytes: snapshot.storage_bytes(),
            last_maintenance_at: snapshot.last_maintenance_at(),
        })
    }
}

pub(in crate::storage) fn postgres_database_diagnostics(
    backend: PostgresStorage,
) -> Arc<dyn DatabaseDiagnosticsProvider> {
    Arc::new(PostgresDatabaseDiagnostics { backend })
}

pub(crate) struct StorageSettings {
    adapter: StorageAdapterSettings,
}

enum StorageAdapterSettings {
    Postgres(PostgresPoolSettings),
    Memory,
}

impl StorageSettings {
    pub(crate) const fn memory() -> Self {
        Self {
            adapter: StorageAdapterSettings::Memory,
        }
    }

    pub(crate) fn postgres(connection_url: impl Into<String>) -> PostgresStorageSettingsBuilder {
        PostgresStorageSettingsBuilder {
            connection_url: connection_url.into(),
            max_connections: None,
            statement_timeout_ms: 0,
            acquire_timeout_ms: None,
        }
    }

    pub(crate) fn same_database_endpoint(&self, other: &Self) -> bool {
        match (&self.adapter, &other.adapter) {
            (StorageAdapterSettings::Postgres(left), StorageAdapterSettings::Postgres(right)) => {
                let left = left.endpoint();
                let right = right.endpoint();
                left.host() == right.host()
                    && left.port() == right.port()
                    && left.database() == right.database()
            }
            (StorageAdapterSettings::Memory, StorageAdapterSettings::Memory) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for StorageSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.adapter {
            StorageAdapterSettings::Postgres(settings) => formatter
                .debug_struct("StorageSettings")
                .field("backend", &StorageBackendKind::Postgres.as_str())
                .field("connection_url", &"<redacted>")
                .field("max_connections", &settings.max_size())
                .field("statement_timeout_ms", &settings.statement_timeout_ms())
                .field("acquire_timeout_ms", &settings.acquire_timeout_ms())
                .finish(),
            StorageAdapterSettings::Memory => formatter
                .debug_struct("StorageSettings")
                .field("backend", &StorageBackendKind::Memory.as_str())
                .finish(),
        }
    }
}

pub(crate) struct PostgresStorageSettingsBuilder {
    connection_url: String,
    max_connections: Option<u32>,
    statement_timeout_ms: u64,
    acquire_timeout_ms: Option<u64>,
}

impl PostgresStorageSettingsBuilder {
    #[must_use]
    pub(crate) fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    #[must_use]
    pub(crate) fn statement_timeout_ms(mut self, statement_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self
    }

    #[must_use]
    pub(crate) fn acquire_timeout_ms(mut self, acquire_timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = Some(acquire_timeout_ms);
        self
    }

    pub(crate) fn build(self) -> Result<StorageSettings, StorageError> {
        PostgresAdapterFactory::build_settings(self)
    }
}

/// Application-local composition for the PostgreSQL adapter.
///
/// Backend crates expose implementation primitives. This module centralizes
/// the translation from Hubuum process settings into those primitives. Adding
/// a selectable backend still requires explicit updates to the backend kind,
/// context dispatch, settings, initialization and migration dispatch,
/// administrator projections, certification, and test environment described
/// in the storage backend author guide.
struct PostgresAdapterFactory;

impl PostgresAdapterFactory {
    fn build_settings(
        builder: PostgresStorageSettingsBuilder,
    ) -> Result<StorageSettings, StorageError> {
        let settings = PostgresPoolSettings::builder(builder.connection_url)
            .max_size(builder.max_connections.ok_or_else(|| {
                StorageError::invalid_input("storage maximum connection count is required")
            })?)
            .statement_timeout_ms(builder.statement_timeout_ms)
            .acquire_timeout_ms(builder.acquire_timeout_ms.ok_or_else(|| {
                StorageError::invalid_input("storage acquire timeout is required")
            })?)
            .build()
            .map_err(Self::initialization_error)?;
        Ok(StorageSettings {
            adapter: StorageAdapterSettings::Postgres(settings),
        })
    }

    fn initialize(settings: &PostgresPoolSettings) -> Result<StorageHandle, StorageError> {
        let endpoint = settings.endpoint();
        info!(
            message = "storage backend configured",
            backend = StorageBackendKind::Postgres.as_str(),
            username = endpoint.username(),
            host = endpoint.host(),
            port = endpoint.port(),
            database = endpoint.database(),
            max_connections = settings.max_size(),
            acquire_timeout_ms = settings.acquire_timeout_ms(),
            statement_timeout_ms = settings.statement_timeout_ms(),
        );
        let pool = build_postgres_pool(settings).map_err(Self::initialization_error)?;
        let task_lease_pool_settings =
            operational_pool_settings(settings, 1).map_err(Self::initialization_error)?;
        let task_lease_pool =
            build_postgres_pool(&task_lease_pool_settings).map_err(Self::initialization_error)?;
        let notification_listener_pool_settings =
            notification_listener_pool_settings(settings).map_err(Self::initialization_error)?;
        let notification_listener_pool = build_postgres_pool(&notification_listener_pool_settings)
            .map_err(Self::initialization_error)?;
        let backend = compose_postgres(pool)
            .with_task_lease_pool(task_lease_pool)
            .with_notification_listener_pool(notification_listener_pool);
        Ok(StorageHandle::from_postgres_backend(backend))
    }

    fn initialization_error(error: PostgresPoolBuildError) -> StorageError {
        match error {
            PostgresPoolBuildError::InvalidSettings(_)
            | PostgresPoolBuildError::InvalidUrl(_)
            | PostgresPoolBuildError::UnsupportedDatabaseType
            | PostgresPoolBuildError::UnsupportedTlsMode(_) => {
                StorageError::invalid_input(error.to_string())
            }
            PostgresPoolBuildError::Tls(native_error) => {
                error!(
                    message = "PostgreSQL TLS initialization failed",
                    backend = "postgresql",
                    native_error = %native_error,
                );
                StorageError::unavailable("PostgreSQL TLS configuration failed")
            }
        }
    }

    #[cfg(feature = "embedded-migrations")]
    fn run_migrations(
        settings: &PostgresPoolSettings,
        roles: Option<&PostgresDatabaseRoleNames>,
    ) -> Result<usize, StorageError> {
        match roles {
            Some(roles) => hubuum_storage_postgres::run_embedded_migrations_as(
                settings.connection_url(),
                roles,
            ),
            None => hubuum_storage_postgres::run_embedded_migrations(settings.connection_url()),
        }
    }

    async fn inspect_privileges(
        settings: &PostgresPoolSettings,
        role: StorageDatabaseRole,
        roles: &PostgresDatabaseRoleNames,
    ) -> Result<StorageDatabasePrivilegeReport, StorageError> {
        let pool_settings =
            operational_pool_settings(settings, 1).map_err(Self::initialization_error)?;
        let pool = build_postgres_pool(&pool_settings).map_err(Self::initialization_error)?;
        let role = match role {
            StorageDatabaseRole::Owner => roles.owner(),
            StorageDatabaseRole::Migrator => roles.migrator(),
            StorageDatabaseRole::Runtime => roles.runtime(),
        };
        inspect_database_privileges(&pool, role, roles)
            .await
            .map_err(StorageError::from)
            .map(StorageDatabasePrivilegeReport::from)
    }
}

fn operational_pool_settings(
    settings: &PostgresPoolSettings,
    max_size: u32,
) -> Result<PostgresPoolSettings, PostgresPoolBuildError> {
    PostgresPoolSettings::builder(settings.connection_url().to_string())
        .max_size(max_size)
        .statement_timeout_ms(settings.statement_timeout_ms())
        .acquire_timeout_ms(settings.acquire_timeout_ms())
        .build()
}

pub(in crate::storage) fn notification_listener_pool_settings(
    settings: &PostgresPoolSettings,
) -> Result<PostgresPoolSettings, PostgresPoolBuildError> {
    let listener_count = u32::try_from(StorageNotification::ALL.len())
        .expect("the bounded storage notification topic count must fit in u32");
    operational_pool_settings(settings, listener_count)
}

pub(crate) fn initialize_storage(
    settings: &StorageSettings,
) -> Result<StorageHandle, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgres(settings) => PostgresAdapterFactory::initialize(settings),
        StorageAdapterSettings::Memory => Ok(StorageHandle::memory()),
    }
}

#[cfg(feature = "embedded-migrations")]
pub(crate) fn run_storage_migrations(
    settings: &StorageSettings,
    roles: Option<&StorageDatabaseRoleNames>,
) -> Result<usize, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgres(settings) => match roles {
            Some(roles) => {
                let roles = roles.postgres_names()?;
                PostgresAdapterFactory::run_migrations(settings, Some(&roles))
            }
            None => PostgresAdapterFactory::run_migrations(settings, None),
        },
        StorageAdapterSettings::Memory => Ok(0),
    }
}

#[cfg(feature = "embedded-migrations")]
pub(crate) fn prepare_disposable_restore_database(
    settings: &StorageSettings,
) -> Result<usize, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgres(settings) => {
            hubuum_storage_postgres::prepare_disposable_restore_database(settings.connection_url())
        }
        StorageAdapterSettings::Memory => Err(StorageError::invalid_input(
            "Isolated restore verification requires the PostgreSQL storage backend",
        )),
    }
}

#[cfg(feature = "embedded-migrations")]
pub(crate) fn reset_disposable_restore_database(
    settings: &StorageSettings,
) -> Result<(), StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgres(settings) => {
            hubuum_storage_postgres::reset_disposable_restore_database(settings.connection_url())
        }
        StorageAdapterSettings::Memory => Err(StorageError::invalid_input(
            "Isolated restore verification requires the PostgreSQL storage backend",
        )),
    }
}

pub(crate) fn storage_database_role_setup_sql(
    roles: &StorageDatabaseRoleNames,
) -> Result<String, StorageError> {
    Ok(hubuum_storage_postgres::database_role_setup_sql(
        &roles.postgres_names()?,
    ))
}

pub(crate) fn storage_database_role_grants_sql(
    roles: &StorageDatabaseRoleNames,
) -> Result<String, StorageError> {
    Ok(hubuum_storage_postgres::database_role_reconciliation_sql(
        &roles.postgres_names()?,
    ))
}

pub(crate) async fn inspect_storage_database_privileges(
    settings: &StorageSettings,
    role: StorageDatabaseRole,
    roles: &StorageDatabaseRoleNames,
) -> Result<Option<StorageDatabasePrivilegeReport>, StorageError> {
    match &settings.adapter {
        StorageAdapterSettings::Postgres(settings) => {
            let roles = roles.postgres_names()?;
            PostgresAdapterFactory::inspect_privileges(settings, role, &roles)
                .await
                .map(Some)
        }
        StorageAdapterSettings::Memory => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn postgres_settings(url: &str) -> StorageSettings {
        StorageSettings::postgres(url)
            .max_connections(1)
            .statement_timeout_ms(500)
            .acquire_timeout_ms(1_000)
            .build()
            .expect("settings should be valid")
    }

    #[rstest]
    #[case::different_credentials(
        "postgres://runtime:one@database.example:5432/hubuum?sslmode=require",
        "postgres://restore:two@database.example:5432/hubuum?sslmode=disable",
        true
    )]
    #[case::different_database(
        "postgres://runtime:one@database.example:5432/hubuum",
        "postgres://runtime:one@database.example:5432/hubuum_restore",
        false
    )]
    #[case::different_port(
        "postgres://runtime:one@database.example:5432/hubuum",
        "postgres://runtime:one@database.example:5433/hubuum",
        false
    )]
    fn database_endpoint_comparison_ignores_credentials_only(
        #[case] left: &str,
        #[case] right: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(
            postgres_settings(left).same_database_endpoint(&postgres_settings(right)),
            expected
        );
    }

    #[test]
    fn settings_debug_redacts_the_connection_url() {
        let settings =
            StorageSettings::postgres("postgres://secret-user:secret-password@example.test/hubuum")
                .max_connections(4)
                .statement_timeout_ms(500)
                .acquire_timeout_ms(1_000)
                .build()
                .expect("settings should be valid");

        let debug = format!("{settings:?}");
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-password"));
        assert!(debug.contains("max_connections: 4"));
    }

    #[test]
    fn settings_require_backend_neutral_pool_limits() {
        let error = StorageSettings::postgres("postgres://localhost/hubuum")
            .build()
            .expect_err("missing connection limits should fail");

        assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    }

    #[test]
    fn postgres_tls_initialization_error_hides_native_details() {
        let native_detail = "could not read /private/adapter/root-ca.pem";
        let error = PostgresAdapterFactory::initialization_error(PostgresPoolBuildError::Tls(
            native_detail.to_string(),
        ));

        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(error.message(), "PostgreSQL TLS configuration failed");
        assert!(!error.message().contains(native_detail));
    }

    #[test]
    fn postgres_listener_pool_reserves_one_connection_per_topic() {
        let settings = PostgresPoolSettings::builder("postgres://localhost/hubuum")
            .max_size(1)
            .statement_timeout_ms(500)
            .acquire_timeout_ms(1_000)
            .build()
            .expect("settings should be valid");

        let listener_settings = notification_listener_pool_settings(&settings)
            .expect("listener settings should be valid");

        assert_eq!(
            usize::try_from(listener_settings.max_size())
                .expect("the listener pool size should fit in usize"),
            StorageNotification::ALL.len()
        );
    }
}
