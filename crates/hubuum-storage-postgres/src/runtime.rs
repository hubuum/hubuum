//! PostgreSQL-owned connection and transaction execution.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::{debug, warn};

use crate::{PostgresConnection, PostgresPool, PostgresPooledConnection, PostgresStorageError};
use hubuum_storage_core::{StorageCallSite, StorageErrorKind};

/// Latest migration required by this adapter.
pub const REQUIRED_DATABASE_MIGRATION_VERSION: &str = "20260804000025";

/// Adapter-level telemetry supplied by the application composition root.
///
/// The PostgreSQL adapter owns the timing points because it knows when pool
/// acquisition and database operations actually begin and end. Applications
/// decide how to export those measurements without coupling this crate to a
/// metrics implementation or global registry.
pub trait PostgresTelemetry: Send + Sync {
    fn connection_acquired(&self, _call_site: StorageCallSite, _duration: Duration) {}

    fn connection_acquisition_failed(&self, _call_site: StorageCallSite, _duration: Duration) {}

    fn operation_finished(
        &self,
        _call_site: StorageCallSite,
        _operation: &'static str,
        _duration: Duration,
        _error: Option<StorageErrorKind>,
    ) {
    }
}

#[derive(Debug, Default)]
struct NoopPostgresTelemetry;

impl PostgresTelemetry for NoopPostgresTelemetry {}

/// Runtime dependencies shared by PostgreSQL operations.
#[derive(Clone)]
pub struct PostgresRuntime {
    pool: PostgresPool,
    telemetry: Arc<dyn PostgresTelemetry>,
}

impl fmt::Debug for PostgresRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresRuntime")
            .field("pool", &"<postgresql pool>")
            .field("telemetry", &"<postgresql telemetry>")
            .finish()
    }
}

impl PostgresRuntime {
    #[must_use]
    pub fn new(pool: PostgresPool) -> Self {
        Self {
            pool,
            telemetry: Arc::new(NoopPostgresTelemetry),
        }
    }

    #[must_use]
    pub fn with_telemetry(mut self, telemetry: Arc<dyn PostgresTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[must_use]
    pub fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    async fn acquire_connection(
        &self,
    ) -> Result<PostgresPooledConnection<'_>, PostgresStorageError> {
        let started_at = Instant::now();
        let call_site = ambient_storage_call_site();
        match self.pool.get().await {
            Ok(connection) => {
                #[cfg(feature = "query-capture")]
                let mut connection = connection;
                #[cfg(feature = "query-capture")]
                crate::configure_connection(&mut connection);
                let duration = started_at.elapsed();
                self.telemetry.connection_acquired(call_site, duration);
                debug!(
                    message = "storage backend connection acquired",
                    backend = "postgresql",
                    caller = call_site.as_str(),
                    elapsed_ms = duration.as_millis(),
                );
                Ok(connection)
            }
            Err(error) => {
                let duration = started_at.elapsed();
                self.telemetry
                    .connection_acquisition_failed(call_site, duration);
                warn!(
                    message = "storage backend connection acquisition failed",
                    backend = "postgresql",
                    caller = call_site.as_str(),
                    elapsed_ms = duration.as_millis(),
                    error = %error,
                );
                Err(error.into())
            }
        }
    }

    pub async fn with_connection<F, R, E>(&self, operation: F) -> Result<R, PostgresStorageError>
    where
        F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
            + for<'connection> SendAsyncFn<
                &'connection mut PostgresConnection,
                Result<R, E>,
                Fut: Send,
            > + Send,
        R: Send,
        E: Send,
        PostgresStorageError: From<E>,
    {
        let mut connection = self.acquire_connection().await?;
        let started_at = Instant::now();
        let result = operation(&mut connection)
            .await
            .map_err(PostgresStorageError::from);
        self.record_completion("connection", started_at, &result);
        result
    }

    pub async fn with_transaction<F, R, E>(&self, operation: F) -> Result<R, PostgresStorageError>
    where
        F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
            + for<'connection> SendAsyncFn<
                &'connection mut PostgresConnection,
                Result<R, E>,
                Fut: Send,
            > + Send,
        R: Send,
        E: Send,
        PostgresStorageError: From<E>,
    {
        let mut connection = self.acquire_connection().await?;
        let started_at = Instant::now();
        let result = connection
            .transaction::<R, PostgresStorageError, _>(async move |connection| {
                operation(connection)
                    .await
                    .map_err(PostgresStorageError::from)
            })
            .await;
        self.record_completion("transaction", started_at, &result);
        result
    }

    fn record_completion<R>(
        &self,
        operation: &'static str,
        started_at: Instant,
        result: &Result<R, PostgresStorageError>,
    ) {
        let duration = started_at.elapsed();
        let call_site = ambient_storage_call_site();
        self.telemetry.operation_finished(
            call_site,
            operation,
            duration,
            result.as_ref().err().map(|error| error.kind()),
        );
        log_completion(call_site, operation, duration, result);
    }
}

tokio::task_local! {
    static AMBIENT_STORAGE_CALL_SITE: StorageCallSite;
}

fn ambient_storage_call_site() -> StorageCallSite {
    AMBIENT_STORAGE_CALL_SITE
        .try_with(|call_site| *call_site)
        .unwrap_or_default()
}

/// Attribute adapter work performed while `future` is being polled.
pub async fn with_storage_call_site<F>(call_site: StorageCallSite, future: F) -> F::Output
where
    F: Future,
{
    AMBIENT_STORAGE_CALL_SITE.scope(call_site, future).await
}

#[derive(QueryableByName)]
struct DatabaseSchemaReadiness {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    ready: bool,
}

pub(crate) async fn postgres_schema_is_ready(
    connection: &mut PostgresConnection,
) -> Result<bool, diesel::result::Error> {
    Ok(diesel::sql_query(
        "SELECT EXISTS (\
            SELECT 1 FROM __diesel_schema_migrations WHERE version = $1\
        ) AS ready",
    )
    .bind::<diesel::sql_types::Text, _>(REQUIRED_DATABASE_MIGRATION_VERSION)
    .get_result::<DatabaseSchemaReadiness>(connection)
    .await?
    .ready)
}

/// Verify that the latest schema required by this adapter is installed.
pub async fn schema_is_ready(pool: &PostgresPool) -> Result<bool, PostgresStorageError> {
    with_connection(pool, postgres_schema_is_ready).await
}

/// Helper bound requiring futures returned by higher-ranked async closures to
/// remain sendable while borrowing a pooled connection.
#[doc(hidden)]
pub trait SendAsyncFn<T, R>:
    AsyncFnOnce(T) -> R + FnOnce(T) -> <Self as SendAsyncFn<T, R>>::Fut
{
    type Fut: Future<Output = R>;
}

impl<F, T, Fut, R> SendAsyncFn<T, R> for F
where
    F: AsyncFnOnce(T) -> R + FnOnce(T) -> Fut,
    Fut: Future<Output = R>,
{
    type Fut = Fut;
}

/// Execute one read, one write, or other non-atomic operation on a pooled
/// PostgreSQL connection.
pub async fn with_connection<F, R, E>(
    pool: &PostgresPool,
    operation: F,
) -> Result<R, PostgresStorageError>
where
    F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
        + for<'connection> SendAsyncFn<&'connection mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    PostgresStorageError: From<E>,
{
    PostgresRuntime::new(pool.clone())
        .with_connection(operation)
        .await
}

/// Execute an atomic multi-step operation in a PostgreSQL transaction.
pub async fn with_transaction<F, R, E>(
    pool: &PostgresPool,
    operation: F,
) -> Result<R, PostgresStorageError>
where
    F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
        + for<'connection> SendAsyncFn<&'connection mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    PostgresStorageError: From<E>,
{
    PostgresRuntime::new(pool.clone())
        .with_transaction(operation)
        .await
}

fn log_completion<R>(
    call_site: StorageCallSite,
    operation: &'static str,
    duration: Duration,
    result: &Result<R, PostgresStorageError>,
) {
    let elapsed_ms = duration.as_millis();
    match result {
        Ok(_) => debug!(
            message = "storage backend operation complete",
            backend = "postgresql",
            caller = call_site.as_str(),
            operation,
            elapsed_ms,
        ),
        Err(error) if error.kind().is_backend_failure() => warn!(
            message = "storage backend operation failed",
            backend = "postgresql",
            caller = call_site.as_str(),
            operation,
            error_class = error.kind().as_str(),
            elapsed_ms,
            error = %error,
        ),
        Err(error) => debug!(
            message = "storage backend operation rejected",
            backend = "postgresql",
            caller = call_site.as_str(),
            operation,
            error_class = error.kind().as_str(),
            elapsed_ms,
            error = %error,
        ),
    }
}
