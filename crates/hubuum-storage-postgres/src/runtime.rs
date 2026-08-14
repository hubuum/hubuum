//! PostgreSQL-owned connection and transaction execution.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, RunQueryDsl};
use hubuum_events_core::MutationProvenance;
use hubuum_storage_core::{
    StorageCallSite, StorageErrorKind, StorageQueryBudget, StorageRevisionPrecondition,
};
use tracing::{debug, warn};

use crate::{PostgresConnection, PostgresPool, PostgresPooledConnection, PostgresStorageError};

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
        let context = TransactionLocalContext::ambient();
        let mut connection = self.acquire_connection().await?;
        let started_at = Instant::now();
        let result = if context.is_empty() {
            operation(&mut connection)
                .await
                .map_err(PostgresStorageError::from)
        } else {
            connection
                .transaction::<R, PostgresStorageError, _>(async move |connection| {
                    context.apply(connection).await?;
                    operation(connection)
                        .await
                        .map_err(PostgresStorageError::from)
                })
                .await
        };
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
        let context = TransactionLocalContext::ambient();
        let mut connection = self.acquire_connection().await?;
        let started_at = Instant::now();
        let result = connection
            .transaction::<R, PostgresStorageError, _>(async move |connection| {
                context.apply(connection).await?;
                operation(connection)
                    .await
                    .map_err(PostgresStorageError::from)
            })
            .await;
        self.record_completion("transaction", started_at, &result);
        result
    }

    /// Execute a repeatable-read, read-only snapshot transaction.
    ///
    /// PostgreSQL requires the isolation declaration to be the transaction's
    /// first statement, so ambient mutation and revision settings are
    /// intentionally not applied to this read-only operation.
    pub async fn with_read_only_snapshot<F, R, E>(
        &self,
        operation: F,
    ) -> Result<R, PostgresStorageError>
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
                diesel::sql_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .execute(connection)
                    .await?;
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

tokio::task_local! {
    static AMBIENT_QUERY_BUDGET: Option<StorageQueryBudget>;
}

tokio::task_local! {
    static AMBIENT_MUTATION_PROVENANCE: Option<MutationProvenance>;
}

tokio::task_local! {
    static AMBIENT_REVISION_PRECONDITION: Option<StorageRevisionPrecondition>;
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

/// Apply a backend-neutral query budget to adapter work in `future`.
pub async fn with_query_budget<F>(budget: Option<StorageQueryBudget>, future: F) -> F::Output
where
    F: Future,
{
    AMBIENT_QUERY_BUDGET.scope(budget, future).await
}

/// Attribute durable writes performed by adapter work in `future`.
pub async fn with_mutation_provenance<F>(
    provenance: Option<MutationProvenance>,
    future: F,
) -> F::Output
where
    F: Future,
{
    AMBIENT_MUTATION_PROVENANCE.scope(provenance, future).await
}

/// Apply an optimistic-concurrency assertion to adapter work in `future`.
pub async fn with_revision_precondition<F>(
    precondition: Option<StorageRevisionPrecondition>,
    future: F,
) -> F::Output
where
    F: Future,
{
    AMBIENT_REVISION_PRECONDITION
        .scope(precondition, future)
        .await
}

pub(crate) async fn assert_locked_revision_precondition(
    connection: &mut PostgresConnection,
    owner_key: &str,
    revision: crate::PostgresRevision,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query("SELECT hubuum_assert_revision_precondition($1, $2)")
        .bind::<Text, _>(owner_key)
        .bind::<BigInt, _>(revision.get())
        .execute(connection)
        .await?;
    Ok(())
}

pub(crate) fn require_existing_revision_target<T>(
    target: Option<T>,
    owner_key: &str,
) -> Result<T, PostgresStorageError> {
    match target {
        Some(target) => Ok(target),
        None => {
            let has_matching_precondition = AMBIENT_REVISION_PRECONDITION
                .try_with(|precondition| {
                    precondition
                        .as_ref()
                        .is_some_and(|precondition| precondition.owner_key() == owner_key)
                })
                .unwrap_or(false);
            if has_matching_precondition {
                Err(PostgresStorageError::precondition_failed(
                    "The resource changed since the supplied validator was issued",
                    None,
                ))
            } else {
                Err(PostgresStorageError::not_found("Entity not found"))
            }
        }
    }
}

struct TransactionLocalContext {
    query_budget: Option<StorageQueryBudget>,
    provenance: Option<MutationProvenance>,
    revision_precondition: Option<StorageRevisionPrecondition>,
}

impl TransactionLocalContext {
    fn ambient() -> Self {
        Self {
            query_budget: AMBIENT_QUERY_BUDGET
                .try_with(|budget| *budget)
                .unwrap_or(None),
            provenance: AMBIENT_MUTATION_PROVENANCE
                .try_with(Clone::clone)
                .unwrap_or(None),
            revision_precondition: AMBIENT_REVISION_PRECONDITION
                .try_with(Clone::clone)
                .unwrap_or(None),
        }
    }

    fn is_empty(&self) -> bool {
        self.query_budget.is_none()
            && self.provenance.is_none()
            && self.revision_precondition.is_none()
    }

    async fn apply(&self, connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
        if let Some(query_budget) = self.query_budget {
            diesel::sql_query("SELECT set_config('statement_timeout', $1, true)")
                .bind::<diesel::sql_types::Text, _>(query_budget.as_millis().to_string())
                .execute(connection)
                .await?;
        }
        if let Some(provenance) = &self.provenance {
            diesel::sql_query(
                "SELECT \
                 set_config('hubuum.actor_kind', $1, true), \
                 set_config('hubuum.actor_id', $2, true), \
                 set_config('hubuum.initiator_user_id', $3, true), \
                 set_config('hubuum.task_id', $4, true)",
            )
            .bind::<diesel::sql_types::Text, _>(provenance.actor_kind().as_str())
            .bind::<diesel::sql_types::Text, _>(
                provenance
                    .actor_user_id()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            )
            .bind::<diesel::sql_types::Text, _>(
                provenance
                    .initiator_user_id()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            )
            .bind::<diesel::sql_types::Text, _>(
                provenance
                    .task_id()
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
            )
            .execute(connection)
            .await?;
        }
        if let Some(precondition) = &self.revision_precondition {
            diesel::sql_query(
                "SELECT \
                 set_config('hubuum.if_match_owner', $1, true), \
                 set_config('hubuum.if_match_revisions', $2, true), \
                 set_config('hubuum.if_match_checked', '', true)",
            )
            .bind::<diesel::sql_types::Text, _>(precondition.owner_key())
            .bind::<diesel::sql_types::Text, _>(
                precondition
                    .revisions()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .execute(connection)
            .await?;
        }
        Ok(())
    }
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
