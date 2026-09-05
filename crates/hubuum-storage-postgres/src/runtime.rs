//! PostgreSQL-owned connection and transaction execution.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel::sql_types::{BigInt, Text};
use diesel_async::{AsyncConnection, RunQueryDsl};
use hubuum_events_core::{MutationProvenance, TraceLink};
use hubuum_storage_core::{
    StorageCallSite, StorageErrorKind, StorageQueryBudget, StorageRevisionPrecondition,
};
use tracing::{debug, warn};

use crate::revision::revision_owner_key;
use crate::{PostgresConnection, PostgresPool, PostgresPooledConnection, PostgresStorageError};

/// Latest migration required by this adapter.
pub const REQUIRED_DATABASE_MIGRATION_VERSION: &str = "20260904000001";
// These migrations were added on a parallel branch and precede the latest
// checkpoint. Its presence alone does not prove that tracing is installed.
const REQUIRED_DATABASE_MIGRATION_VERSIONS: &[&str] = &[
    "20260903000002",
    "20260903000003",
    REQUIRED_DATABASE_MIGRATION_VERSION,
];
pub const DEFAULT_COMPUTED_REINDEX_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();

/// Adapter-level observation hook supplied by the application composition root.
///
/// The PostgreSQL adapter owns the timing points because it knows when pool
/// acquisition and database operations actually begin and end. Applications
/// decide how to export those measurements without coupling this crate to a
/// metrics implementation or global registry.
pub trait PostgresObserver: Send + Sync {
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

    fn computed_evaluation(&self, _scope: &'static str, _error_codes: &[&'static str]) {}

    fn computed_live_fallback(&self) {}

    fn computed_read_repair(&self, _outcome: &'static str) {}

    fn revision_condition(&self, _outcome: &'static str) {}

    fn task_completed(
        &self,
        _kind: &'static str,
        _status: &'static str,
        _duration: Option<Duration>,
    ) {
    }

    fn computed_rebuild_finished(&self, _outcome: &'static str, _duration: Duration) {}

    fn computed_rebuild_batch(&self, _object_count: usize) {}
}

/// Explicit telemetry opt-out for tests, benchmarks, and one-shot maintenance
/// tools that intentionally do not export adapter observations.
///
/// Normal application composition should pass its own [`PostgresObserver`]
/// implementation to [`PostgresRuntime::new`].
#[derive(Debug, Default)]
pub struct NoopPostgresObserver;

impl PostgresObserver for NoopPostgresObserver {}

/// Runtime dependencies shared by PostgreSQL operations.
#[derive(Clone)]
pub struct PostgresRuntime {
    pool: PostgresPool,
    task_lease_pool: PostgresPool,
    computed_reindex_batch_size: NonZeroUsize,
    observer: Arc<dyn PostgresObserver>,
}

impl fmt::Debug for PostgresRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresRuntime")
            .field("pool", &"<postgresql pool>")
            .field("task_lease_pool", &"<postgresql pool>")
            .field(
                "computed_reindex_batch_size",
                &self.computed_reindex_batch_size,
            )
            .field("observer", &"<postgresql observer>")
            .finish()
    }
}

impl PostgresRuntime {
    #[must_use]
    pub fn new(pool: PostgresPool, observer: Arc<dyn PostgresObserver>) -> Self {
        Self {
            task_lease_pool: pool.clone(),
            computed_reindex_batch_size: DEFAULT_COMPUTED_REINDEX_BATCH_SIZE,
            pool,
            observer,
        }
    }

    /// Construct a runtime with an explicit telemetry opt-out.
    ///
    /// This is intended for tests, benchmarks, and one-shot maintenance tools.
    #[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
    #[must_use]
    pub fn unobserved(pool: PostgresPool) -> Self {
        Self::new(pool, Arc::new(NoopPostgresObserver))
    }

    /// Use a small isolated pool for lease heartbeats.
    ///
    /// A worker may hold a connection from the execution pool while it renews
    /// its lease. Keeping renewal on a separate pool prevents that safety path
    /// from deadlocking behind the work it is protecting.
    #[must_use]
    pub fn with_task_lease_pool(mut self, task_lease_pool: PostgresPool) -> Self {
        self.task_lease_pool = task_lease_pool;
        self
    }

    /// Configure the bounded number of objects rebuilt in one transaction.
    #[must_use]
    pub fn with_computed_reindex_batch_size(mut self, batch_size: NonZeroUsize) -> Self {
        self.computed_reindex_batch_size = batch_size;
        self
    }

    #[must_use]
    pub fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    pub(crate) fn computed_reindex_batch_size(&self) -> usize {
        self.computed_reindex_batch_size.get()
    }

    async fn acquire_connection_from<'pool>(
        &self,
        pool: &'pool PostgresPool,
    ) -> Result<PostgresPooledConnection<'pool>, PostgresStorageError> {
        let started_at = Instant::now();
        let call_site = ambient_storage_call_site();
        match pool.get().await {
            Ok(connection) => {
                #[cfg(feature = "query-capture")]
                let mut connection = connection;
                #[cfg(feature = "query-capture")]
                crate::configure_connection(&mut connection);
                let duration = started_at.elapsed();
                self.observer.connection_acquired(call_site, duration);
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
                self.observer
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

    async fn acquire_connection(
        &self,
    ) -> Result<PostgresPooledConnection<'_>, PostgresStorageError> {
        self.acquire_connection_from(&self.pool).await
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

    /// Run a lease heartbeat through the isolated task-lease pool.
    ///
    /// This keeps lease safety independent from connections held by task work
    /// while retaining the runtime's call-site attribution, logging, and
    /// telemetry implementation.
    pub async fn with_task_lease_connection<F, R, E>(
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
        let context = TransactionLocalContext::ambient();
        let mut connection = self.acquire_connection_from(&self.task_lease_pool).await?;
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
        self.record_completion("task_lease_connection", started_at, &result);
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
                let value = operation(&mut *connection)
                    .await
                    .map_err(PostgresStorageError::from)?;
                crate::reach_fault_point(
                    crate::PostgresFaultPoint::TransactionBeforeCommit,
                    Some(connection),
                )
                .await?;
                Ok(value)
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
        self.observer.operation_finished(
            call_site,
            operation,
            duration,
            result.as_ref().err().map(|error| error.kind()),
        );
        log_completion(call_site, operation, duration, result);
    }

    pub(crate) fn record_computed_evaluation(
        &self,
        scope: &'static str,
        error_codes: &[&'static str],
    ) {
        self.observer.computed_evaluation(scope, error_codes);
    }

    pub(crate) fn record_computed_live_fallback(&self) {
        self.observer.computed_live_fallback();
    }

    pub(crate) fn record_computed_read_repair(&self, outcome: &'static str) {
        self.observer.computed_read_repair(outcome);
    }

    pub(crate) fn record_revision_condition(&self, outcome: &'static str) {
        self.observer.revision_condition(outcome);
    }

    pub(crate) fn record_task_completed(
        &self,
        kind: &'static str,
        status: &'static str,
        duration: Option<Duration>,
    ) {
        self.observer.task_completed(kind, status, duration);
    }

    pub(crate) fn record_computed_rebuild_finished(
        &self,
        outcome: &'static str,
        duration: Duration,
    ) {
        self.observer.computed_rebuild_finished(outcome, duration);
    }

    pub(crate) fn record_computed_rebuild_batch(&self, object_count: usize) {
        self.observer.computed_rebuild_batch(object_count);
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

pub(crate) fn ambient_mutation_trace_link() -> Option<TraceLink> {
    AMBIENT_MUTATION_PROVENANCE
        .try_with(|provenance| {
            provenance
                .as_ref()
                .and_then(MutationProvenance::trace_link)
                .cloned()
        })
        .unwrap_or(None)
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
    let result = diesel::sql_query("SELECT hubuum_assert_revision_precondition($1, $2)")
        .bind::<Text, _>(owner_key)
        .bind::<BigInt, _>(revision.get())
        .execute(connection)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info))
            if info.message() == "hubuum_stale_resource" =>
        {
            Err(PostgresStorageError::revision_conflict(
                "The resource changed since the supplied validator was issued",
                revision.into_domain(),
            ))
        }
        Err(error) => Err(PostgresStorageError::from(error)),
    }
}

/// Reject a conditional mutation when its authoritative row disappeared
/// before the adapter could lock it. Unconditional callers may continue with
/// their operation-specific missing-target behavior.
pub(crate) fn assert_revision_precondition_allows_missing_target(
    owner_key: &str,
) -> Result<(), PostgresStorageError> {
    let has_matching_precondition = AMBIENT_REVISION_PRECONDITION
        .try_with(|precondition| {
            precondition
                .as_ref()
                .is_some_and(|precondition| revision_owner_key(precondition.target()) == owner_key)
        })
        .unwrap_or(false);
    if has_matching_precondition {
        Err(PostgresStorageError::precondition_failed(
            "The resource changed since the supplied validator was issued",
            None,
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn require_existing_revision_target<T>(
    target: Option<T>,
    owner_key: &str,
) -> Result<T, PostgresStorageError> {
    match target {
        Some(target) => Ok(target),
        None => {
            assert_revision_precondition_allows_missing_target(owner_key)?;
            Err(PostgresStorageError::not_found("Entity not found"))
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
            let owner_key = revision_owner_key(precondition.target());
            diesel::sql_query(
                "SELECT \
                 set_config('hubuum.if_match_owner', $1, true), \
                 set_config('hubuum.if_match_revisions', $2, true), \
                 set_config('hubuum.if_match_checked', '', true)",
            )
            .bind::<diesel::sql_types::Text, _>(owner_key)
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
        "SELECT NOT EXISTS (\
            SELECT unnest($1::text[]) \
            EXCEPT SELECT version::text FROM __diesel_schema_migrations\
        ) AS ready",
    )
    .bind::<diesel::sql_types::Array<Text>, _>(REQUIRED_DATABASE_MIGRATION_VERSIONS)
    .get_result::<DatabaseSchemaReadiness>(connection)
    .await?
    .ready)
}

/// Verify that every required schema checkpoint is installed, including
/// migrations added out of order on parallel branches.
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
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
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
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
    PostgresRuntime::unobserved(pool.clone())
        .with_connection(operation)
        .await
}

/// Execute an atomic multi-step operation in a PostgreSQL transaction.
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
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
    PostgresRuntime::unobserved(pool.clone())
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::REQUIRED_DATABASE_MIGRATION_VERSION;

    #[cfg(feature = "integration-test-support")]
    #[rstest::rstest]
    #[case::complete(None)]
    #[case::trace_schema(Some("20260903000002"))]
    #[case::trace_validation(Some("20260903000003"))]
    #[case::latest(Some(REQUIRED_DATABASE_MIGRATION_VERSION))]
    #[tokio::test]
    async fn readiness_requires_every_schema_checkpoint(#[case] missing: Option<&str>) {
        use diesel::sql_types::Text;
        use diesel_async::RunQueryDsl;

        use crate::test_support::{database_role_tests_enabled, integration_test_migration_pool};
        use crate::with_connection;

        if !database_role_tests_enabled() {
            return;
        }
        let pool = integration_test_migration_pool(1);
        let ready = with_connection(&pool, async |connection| {
            diesel::sql_query("BEGIN").execute(&mut *connection).await?;
            if let Some(version) = missing {
                diesel::sql_query("DELETE FROM __diesel_schema_migrations WHERE version = $1")
                    .bind::<Text, _>(version)
                    .execute(&mut *connection)
                    .await?;
            }
            let result = super::postgres_schema_is_ready(connection).await;
            diesel::sql_query("ROLLBACK")
                .execute(&mut *connection)
                .await?;
            result
        })
        .await
        .unwrap();

        assert_eq!(ready, missing.is_none());
    }

    #[test]
    fn required_database_migration_version_matches_latest_migration() {
        let migrations = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let latest = fs::read_dir(migrations)
            .expect("migration directory must be readable")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .max()
            .expect("at least one migration must exist");
        let version = latest
            .split_once('_')
            .map_or(latest.as_str(), |(version, _)| version)
            .replace('-', "");

        assert_eq!(REQUIRED_DATABASE_MIGRATION_VERSION, version);
    }
}
