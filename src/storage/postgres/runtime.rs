pub use hubuum_storage_postgres::{PostgresConnection, PostgresPool, PostgresPoolSettings};
#[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
pub use hubuum_storage_postgres::{QueryCaptureSnapshot, capture_queries};

/// Diesel query-building traits paired with diesel-async's I/O traits.
///
/// Importing this prelude avoids bringing the synchronous `Connection` and
/// `RunQueryDsl` traits into scope, which can otherwise make query execution
/// ambiguous when an [`AsyncPgConnection`] is used.
pub mod prelude {
    pub use diesel::associations::{Associations, GroupedBy, Identifiable};
    pub use diesel::deserialize::{Queryable, QueryableByName};
    pub use diesel::expression::IntoSql as _;
    pub use diesel::expression::functions::{declare_sql_function, define_sql_function};
    pub use diesel::expression::{
        AppearsOnTable, BoxableExpression, Expression, IntoSql, Selectable, SelectableExpression,
        SelectableHelper,
    };
    pub use diesel::expression_methods::*;
    pub use diesel::insertable::Insertable;
    pub use diesel::query_builder::{AsChangeset, DecoratableTarget};
    pub use diesel::query_dsl::{BelongingToDsl, CombineDsl, JoinOnDsl, QueryDsl};
    pub use diesel::query_source::SizeRestrictedColumn as _;
    pub use diesel::query_source::{Column, JoinTo, QuerySource, Table};
    pub use diesel::result::{
        ConnectionError, ConnectionResult, OptionalEmptyChangesetExtension, OptionalExtension,
        QueryResult,
    };
    pub use diesel_async::{AsyncConnection, RunQueryDsl, SaveChangesDsl};
}

use diesel_async::{AsyncConnection, RunQueryDsl};
use hubuum_storage_postgres::{PostgresPooledConnection, build_postgres_pool};
use std::future::Future;
use std::time::Duration;

use tracing::{debug, warn};

use crate::api::etag::RevisionPrecondition;
use crate::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use crate::events::MutationProvenance;
use crate::observability::metrics::{self, ResultKind};
use crate::storage::context::postgres_pool;
use crate::storage::{StorageContext, StorageQueryBudget};

/// Bounded attribution for database pool checkouts and helper operations.
///
/// The value is carried through an async task-local scope so subsystem
/// boundaries can add useful low-cardinality metrics without threading a
/// label through every storage capability method.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum StorageCallSite {
    EventDelivery,
    EventFanout,
    EventRetention,
    HttpRequest,
    MetricsRefresh,
    Readiness,
    RequestMaintenance,
    RestoreCoordinator,
    TaskLease,
    TaskWorker,
    TokenRetention,
    #[default]
    Unattributed,
}

impl StorageCallSite {
    const fn as_str(self) -> &'static str {
        match self {
            Self::EventDelivery => "event_delivery",
            Self::EventFanout => "event_fanout",
            Self::EventRetention => "event_retention",
            Self::HttpRequest => "http_request",
            Self::MetricsRefresh => "metrics_refresh",
            Self::Readiness => "readiness",
            Self::RequestMaintenance => "request_maintenance",
            Self::RestoreCoordinator => "restore_coordinator",
            Self::TaskLease => "task_lease",
            Self::TaskWorker => "task_worker",
            Self::TokenRetention => "token_retention",
            Self::Unattributed => "unattributed",
        }
    }
}

/// Latest migration required by this binary. The test below keeps this value
/// synchronized with the migration directory so readiness cannot silently lag
/// behind a newly added schema change.
pub const REQUIRED_DATABASE_MIGRATION_VERSION: &str = "20260804000025";

#[derive(diesel::QueryableByName)]
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

/// Verify both database connectivity and the schema version required by this
/// binary. Distributed API and worker replicas use this without taking
/// migration ownership from the one-shot migration job.
pub async fn ensure_postgres_schema_ready(pool: &PostgresPool) -> Result<(), ApiError> {
    let ready = with_connection(pool, async |connection| {
        postgres_schema_is_ready(connection).await
    })
    .await?;
    if ready {
        Ok(())
    } else {
        Err(ApiError::ServiceUnavailable(format!(
            "Database migration {REQUIRED_DATABASE_MIGRATION_VERSION} has not been applied"
        )))
    }
}

/// Helper bound used to require that futures returned by higher-ranked async
/// closures are `Send`. This mirrors diesel-async's transaction bound while
/// keeping the helper private to the database adapter.
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

async fn acquire_connection(pool: &PostgresPool) -> Result<PostgresPooledConnection<'_>, ApiError> {
    let start = std::time::Instant::now();
    let call_site = ambient_db_call_site();
    match pool.get().await {
        Ok(conn) => {
            #[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
            let mut conn = conn;
            #[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
            hubuum_storage_postgres::configure_connection(&mut conn);
            let duration = start.elapsed();
            metrics::db_connection_acquired(call_site.as_str(), duration);
            debug!(
                message = "storage backend connection acquired",
                backend = "postgresql",
                caller = call_site.as_str(),
                elapsed_ms = duration.as_millis(),
            );
            Ok(conn)
        }
        Err(error) => {
            let duration = start.elapsed();
            metrics::db_connection_acquire_failed(call_site.as_str(), duration);
            warn!(
                message = "storage backend connection acquisition failed",
                backend = "postgresql",
                caller = call_site.as_str(),
                elapsed_ms = duration.as_millis(),
                error = %error,
            );
            Err(ApiError::from(error))
        }
    }
}

fn record_database_operation<R>(
    call_site: StorageCallSite,
    operation: &'static str,
    duration: Duration,
    result: &Result<R, ApiError>,
) {
    let result_kind = match result {
        Ok(_) => ResultKind::Ok,
        Err(error) => ResultKind::Error(error.class()),
    };
    metrics::db_operation_finished(call_site.as_str(), operation, duration, &result_kind);

    match result {
        Ok(_) => debug!(
            message = "storage backend operation complete",
            backend = "postgresql",
            caller = call_site.as_str(),
            operation,
            elapsed_ms = duration.as_millis(),
        ),
        Err(error)
            if matches!(
                error,
                ApiError::DatabaseError(_)
                    | ApiError::DbConnectionError(_)
                    | ApiError::InternalServerError(_)
                    | ApiError::ServiceUnavailable(_)
            ) =>
        {
            warn!(
                message = "storage backend operation failed",
                backend = "postgresql",
                caller = call_site.as_str(),
                operation,
                error_class = error.class(),
                elapsed_ms = duration.as_millis(),
                error = %error,
            )
        }
        Err(error) => debug!(
            message = "storage backend operation rejected",
            backend = "postgresql",
            caller = call_site.as_str(),
            operation,
            error_class = error.class(),
            elapsed_ms = duration.as_millis(),
            error = %error,
        ),
    }
}

tokio::task_local! {
    static AMBIENT_DB_CALL_SITE: StorageCallSite;
}

tokio::task_local! {
    /// The per-query Postgres `statement_timeout` in effect for the current
    /// async task, if any. Set for the duration of a scope via
    /// [`with_export_query_budget_scope`] and consulted by [`with_connection`] /
    /// [`with_transaction`] so that all DB work inside the scope is bounded
    /// without threading a timeout through every caller. Outside any scope the
    /// lookup yields `None`, so behavior is unchanged.
    static AMBIENT_STATEMENT_TIMEOUT: Option<StorageQueryBudget>;
}

tokio::task_local! {
    /// Typed mutation attribution for the current async task. Applied as
    /// transaction-local Postgres settings by [`with_connection_timeout`] /
    /// [`with_transaction`] so temporal triggers can preserve the immediate
    /// actor, root task initiator, and task id without threading them through
    /// every persistence call.
    static AMBIENT_MUTATION_PROVENANCE: Option<MutationProvenance>;
}

tokio::task_local! {
    /// An HTTP or queued-work revision assertion. Database revision triggers
    /// evaluate it at the first authoritative row lock in the transaction.
    static AMBIENT_REVISION_PRECONDITION: Option<RevisionPrecondition>;
}

fn ambient_db_call_site() -> StorageCallSite {
    AMBIENT_DB_CALL_SITE
        .try_with(|call_site| *call_site)
        .unwrap_or_default()
}

/// Run `future` with bounded database metrics attribution.
pub(crate) async fn with_storage_call_site<F>(call_site: StorageCallSite, future: F) -> F::Output
where
    F: Future,
{
    AMBIENT_DB_CALL_SITE.scope(call_site, future).await
}

/// Run `future` with an ambient per-query `statement_timeout` in effect.
///
/// While the future is being polled, every [`with_connection`] /
/// [`with_transaction`] call made on the same task applies the given
/// `statement_timeout` as a transaction-local `SET LOCAL statement_timeout`.
/// This is how the export execution path bounds its queries independently of
/// the pool-global `db_statement_timeout_ms`, without threading the timeout
/// through the search layer. A `statement_timeout` of `None` is a no-op scope.
pub(super) async fn with_export_query_budget_scope<F, R>(
    statement_timeout: Option<StorageQueryBudget>,
    future: F,
) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_STATEMENT_TIMEOUT
        .scope(statement_timeout, future)
        .await
}

/// The ambient per-query `statement_timeout` for the current task, or `None`
/// when not running inside a [`with_export_query_budget_scope`] (including from
/// synchronous, non-task contexts).
fn ambient_statement_timeout() -> Option<StorageQueryBudget> {
    AMBIENT_STATEMENT_TIMEOUT
        .try_with(|timeout| *timeout)
        .unwrap_or(None)
}

/// Run `future` with typed mutation provenance in effect.
pub async fn with_mutation_provenance_scope<F, R>(
    provenance: Option<MutationProvenance>,
    future: F,
) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_MUTATION_PROVENANCE.scope(provenance, future).await
}

/// Compatibility helper for callers that only have a direct user actor.
pub async fn with_actor_scope<F, R>(actor: Option<i32>, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    with_mutation_provenance_scope(actor.map(MutationProvenance::user), future).await
}

/// The ambient mutation provenance, or `None` outside any scope.
fn ambient_mutation_provenance() -> Option<MutationProvenance> {
    AMBIENT_MUTATION_PROVENANCE
        .try_with(Clone::clone)
        .unwrap_or(None)
}

/// Run `future` with a conditional-mutation assertion in effect. The
/// condition is applied transaction-locally by every database helper used
/// inside the scope and therefore cannot leak through the connection pool.
pub async fn with_revision_precondition_scope<F, R>(
    precondition: Option<RevisionPrecondition>,
    future: F,
) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_REVISION_PRECONDITION
        .scope(precondition, future)
        .await
}

fn ambient_revision_precondition() -> Option<RevisionPrecondition> {
    AMBIENT_REVISION_PRECONDITION
        .try_with(Clone::clone)
        .unwrap_or(None)
}

async fn set_local_revision_precondition(
    conn: &mut PostgresConnection,
    precondition: &RevisionPrecondition,
) -> Result<(), diesel::result::Error> {
    diesel::sql_query(
        "SELECT \
         set_config('hubuum.if_match_owner', $1, true), \
         set_config('hubuum.if_match_revisions', $2, true), \
         set_config('hubuum.if_match_checked', '', true)",
    )
    .bind::<diesel::sql_types::Text, _>(precondition.owner_key())
    .bind::<diesel::sql_types::Text, _>(precondition.revisions_csv())
    .execute(conn)
    .await?;
    Ok(())
}

/// Evaluate the ambient condition immediately after a caller has locked an
/// authoritative row. Mutation triggers repeat this defensively, but callers
/// such as JSON Patch need stale detection before interpreting the payload.
pub(crate) async fn assert_locked_revision_precondition(
    conn: &mut PostgresConnection,
    owner_key: &str,
    revision: crate::models::ResourceRevision,
) -> Result<(), diesel::result::Error> {
    diesel::sql_query("SELECT hubuum_assert_revision_precondition($1, $2)")
        .bind::<diesel::sql_types::Text, _>(owner_key)
        .bind::<diesel::sql_types::BigInt, _>(revision.get())
        .execute(conn)
        .await?;
    Ok(())
}

/// Reject a conditional mutation when the authoritative row disappeared
/// before it could be locked. `If-Match` (including `*`) requires the selected
/// resource to still exist; unconditional callers retain their ordinary
/// missing-target behavior.
pub(crate) fn assert_revision_precondition_allows_missing_target(
    owner_key: &str,
) -> Result<(), crate::errors::ApiError> {
    if ambient_revision_precondition()
        .as_ref()
        .is_some_and(|precondition| precondition.owner_key() == owner_key)
    {
        return Err(crate::errors::ApiError::PreconditionFailed(
            "The resource changed since the supplied validator was issued".to_string(),
            None,
        ));
    }
    Ok(())
}

/// Require an authoritative row that was resolved before entering the
/// mutation transaction. If a matching conditional request lost the row
/// before it could be locked, report a stale resource instead of an ordinary
/// not-found response.
pub(crate) fn require_existing_revision_target<T>(
    target: Option<T>,
    owner_key: &str,
) -> Result<T, crate::errors::ApiError> {
    match target {
        Some(target) => Ok(target),
        None => {
            assert_revision_precondition_allows_missing_target(owner_key)?;
            Err(diesel::result::Error::NotFound.into())
        }
    }
}

/// Apply transaction-local provenance settings consumed by history triggers.
/// Empty optional values are converted back to NULL by the trigger.
async fn set_local_mutation_provenance(
    conn: &mut PostgresConnection,
    provenance: &MutationProvenance,
) -> Result<(), diesel::result::Error> {
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
    .execute(conn)
    .await?;
    Ok(())
}

/// Apply a transaction-local `SET LOCAL statement_timeout` to the current
/// transaction. The value is bound rather than formatted into the SQL,
/// mirroring [`StatementTimeoutCustomizer`]. `set_config(name, value,
/// is_local=true)` scopes the value to the current transaction, so it reverts
/// automatically at COMMIT/ROLLBACK and never leaks back to the shared pool.
async fn set_local_statement_timeout(
    conn: &mut PostgresConnection,
    statement_timeout: StorageQueryBudget,
) -> Result<(), diesel::result::Error> {
    diesel::sql_query("SELECT set_config('statement_timeout', $1, true)")
        .bind::<diesel::sql_types::Text, _>(statement_timeout.as_millis().to_string())
        .execute(conn)
        .await?;
    Ok(())
}

struct TransactionLocalContext {
    statement_timeout: Option<StorageQueryBudget>,
    provenance: Option<MutationProvenance>,
    revision_precondition: Option<RevisionPrecondition>,
}

impl TransactionLocalContext {
    fn ambient() -> Self {
        Self::with_statement_timeout(ambient_statement_timeout())
    }

    fn with_statement_timeout(statement_timeout: Option<StorageQueryBudget>) -> Self {
        Self {
            statement_timeout,
            provenance: ambient_mutation_provenance(),
            revision_precondition: ambient_revision_precondition(),
        }
    }

    fn is_empty(&self) -> bool {
        self.statement_timeout.is_none()
            && self.provenance.is_none()
            && self.revision_precondition.is_none()
    }

    async fn apply(&self, conn: &mut PostgresConnection) -> Result<(), diesel::result::Error> {
        if let Some(statement_timeout) = self.statement_timeout {
            set_local_statement_timeout(conn, statement_timeout).await?;
        }
        if let Some(provenance) = &self.provenance {
            set_local_mutation_provenance(conn, provenance).await?;
        }
        if let Some(precondition) = &self.revision_precondition {
            set_local_revision_precondition(conn, precondition).await?;
        }
        Ok(())
    }
}

/// Run database work on a single pooled connection without starting an explicit transaction.
///
/// Use this for:
/// - single read queries
/// - single-statement writes
/// - other DB work that does not require all-or-nothing rollback across multiple statements
///
/// The closure may return any error type `E` as long as it can be converted into [`ApiError`].
/// In practice this means the closure can return either Diesel errors directly or higher-level
/// domain errors that already map into `ApiError`.
///
/// If a [`with_export_query_budget_scope`] is in effect on the current task, the
/// work is automatically bounded by that per-query `statement_timeout` (see
/// [`with_connection_timeout`]). Otherwise no timeout is applied.
///
/// Note: block closures that use `?` and end with `Ok(...)` may require an explicit closure
/// return type, for example:
/// `with_connection(pool, async |conn| -> Result<_, diesel::result::Error> { ... }).await`
pub async fn with_connection<C, F, R, E>(backend: &C, f: F) -> Result<R, ApiError>
where
    C: StorageContext + ?Sized,
    F: for<'conn> AsyncFnOnce(&'conn mut PostgresConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    with_connection_timeout(backend, ambient_statement_timeout(), f).await
}

/// Compatibility alias retained while callers migrate from the former
/// `spawn_blocking` bridge. Both helpers now execute non-blocking database I/O.
pub async fn with_connection_async<C, F, R, E>(backend: C, f: F) -> Result<R, ApiError>
where
    C: StorageContext,
    F: for<'conn> AsyncFnOnce(&'conn mut PostgresConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    with_connection_context(&backend, TransactionLocalContext::ambient(), f).await
}

async fn with_connection_context<C, F, R, E>(
    backend: &C,
    context: TransactionLocalContext,
    f: F,
) -> Result<R, ApiError>
where
    C: StorageContext + ?Sized,
    F: for<'conn> AsyncFnOnce(&'conn mut PostgresConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    let call_site = ambient_db_call_site();
    let mut conn = acquire_connection(postgres_pool(backend)).await?;
    let start = std::time::Instant::now();
    let result = if context.is_empty() {
        f(&mut conn).await.map_err(ApiError::from)
    } else {
        conn.transaction::<R, ApiError, _>(async move |conn| {
            context.apply(conn).await?;
            f(conn).await.map_err(ApiError::from)
        })
        .await
    };
    record_database_operation(call_site, "connection", start.elapsed(), &result);
    result
}

/// Return an updated row, or fetch the current row when a temporal no-op trigger
/// suppressed an unchanged `UPDATE`.
///
/// PostgreSQL `BEFORE UPDATE` triggers skip a row by returning `NULL`; an
/// `UPDATE ... RETURNING` therefore returns no row even though the target row
/// still exists. Centralizing that fallback keeps update call sites from
/// encoding trigger behavior themselves.
pub async fn updated_or_current<T, E>(
    updated: Result<Option<T>, E>,
    select_current: impl AsyncFnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match updated? {
        Some(row) => Ok(row),
        None => select_current().await,
    }
}

/// Run database work on a single pooled connection, optionally bounding it with
/// an explicit per-query Postgres `statement_timeout`.
///
/// When `statement_timeout` is `None`, this behaves exactly like a plain pooled
/// connection (no transaction, no override).
///
/// When it is `Some`, the closure runs inside a transaction that first issues a
/// transaction-local `SET LOCAL statement_timeout`. Postgres cancels any
/// statement exceeding the budget server-side, and the override reverts
/// automatically at COMMIT/ROLLBACK, so it never leaks back to the shared pool.
/// This is the mechanism that makes export queries bounded independently of the
/// pool-global `db_statement_timeout_ms`.
///
/// Most callers should use [`with_connection`] and set the timeout ambiently via
/// [`with_export_query_budget_scope`]; this explicit variant exists for callers
/// (and tests) that want to pass the timeout directly.
///
/// Note: this intentionally wraps a (possibly read-only) closure in a
/// transaction. That is contrary to the usual "single reads use
/// [`with_connection`]" guidance, but the transaction here exists solely to
/// scope `SET LOCAL`, not for multi-statement atomicity, and is encapsulated in
/// this one helper rather than imposed on callers.
pub async fn with_connection_timeout<C, F, R, E>(
    backend: &C,
    statement_timeout: Option<StorageQueryBudget>,
    f: F,
) -> Result<R, ApiError>
where
    C: StorageContext + ?Sized,
    F: for<'conn> AsyncFnOnce(&'conn mut PostgresConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    with_connection_context(
        backend,
        TransactionLocalContext::with_statement_timeout(statement_timeout),
        f,
    )
    .await
}

/// Run database work inside a SQL transaction on a single pooled connection.
///
/// Use this when correctness depends on all enclosed operations succeeding or failing together.
/// If the closure returns `Ok`, the transaction is committed. If it returns `Err`, the
/// transaction is rolled back and the error is mapped into [`ApiError`].
///
/// This is the right helper for multi-step writes such as:
/// - create + related insert
/// - read/modify/write sequences that must be atomic
/// - permission mutations that must not leave partial state behind
///
/// If a [`with_export_query_budget_scope`] is in effect on the current task, a
/// transaction-local `SET LOCAL statement_timeout` is applied at the start of
/// the transaction so this work is bounded too.
///
/// As with [`with_connection`], the closure may return any error type `E` that converts into
/// [`ApiError`]. Block closures that end with `Ok(...)` may need an explicit closure return type,
/// for example:
/// `with_transaction(pool, async |conn| -> Result<_, ApiError> { ... }).await`
pub async fn with_transaction<C, F, R, E>(backend: &C, f: F) -> Result<R, ApiError>
where
    C: StorageContext,
    F: for<'conn> AsyncFnOnce(&'conn mut PostgresConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut PostgresConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    let context = TransactionLocalContext::ambient();
    let call_site = ambient_db_call_site();
    let mut conn = acquire_connection(postgres_pool(backend)).await?;
    let start = std::time::Instant::now();
    let result = crate::logger::defer_operation_mutation_logs_until_commit(
        conn.transaction::<R, ApiError, _>(async move |conn| {
            context.apply(conn).await?;
            f(conn).await.map_err(ApiError::from)
        }),
    )
    .await;
    record_database_operation(call_site, "transaction", start.elapsed(), &result);
    result
}

pub fn init_postgres_pool(database_url: &str, max_size: u32) -> PostgresPool {
    // Read the optional pool-global statement timeout from config. This is
    // intentionally pool-global: every connection handed out by this pool
    // inherits it, so it bounds all DB work (exports, imports, admin commands,
    // health/auth queries), not just export stages. 0 = disabled.
    let statement_timeout_ms = crate::config::get_config()
        .map(|config| config.db_statement_timeout_ms)
        .unwrap_or(crate::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let acquire_timeout_ms = crate::config::get_config()
        .map(|config| config.db_pool_acquire_timeout_ms)
        .unwrap_or(crate::config::DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS);
    let settings = postgres_pool_settings(
        database_url,
        max_size,
        statement_timeout_ms,
        acquire_timeout_ms,
    );
    init_postgres_pool_with_settings(&settings)
}

pub fn init_postgres_pool_with_settings(settings: &PostgresPoolSettings) -> PostgresPool {
    let endpoint = settings.endpoint();
    debug!(
        message = "PostgreSQL storage endpoint configured.",
        backend = "postgresql",
        username = endpoint.username(),
        host = endpoint.host(),
        port = endpoint.port(),
        database = endpoint.database(),
    );

    build_postgres_pool(settings).unwrap_or_else(|error| {
        fatal_error(
            &format!("Failed to initialize PostgreSQL storage: {error}"),
            EXIT_CODE_CONFIG_ERROR,
        )
    })
}

/// Build a pool with an explicit Postgres `statement_timeout` (in milliseconds)
/// applied to every connection on acquisition. A value of 0 disables the
/// timeout. Exposed so tests can exercise the customizer without mutating the
/// global config.
pub fn init_postgres_pool_with_statement_timeout(
    database_url: &str,
    max_size: u32,
    statement_timeout_ms: u64,
) -> PostgresPool {
    let settings = postgres_pool_settings(
        database_url,
        max_size,
        statement_timeout_ms,
        crate::config::DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS,
    );
    init_postgres_pool_with_settings(&settings)
}

fn postgres_pool_settings(
    database_url: &str,
    max_size: u32,
    statement_timeout_ms: u64,
    acquire_timeout_ms: u64,
) -> PostgresPoolSettings {
    PostgresPoolSettings::builder(database_url)
        .max_size(max_size)
        .statement_timeout_ms(statement_timeout_ms)
        .acquire_timeout_ms(acquire_timeout_ms)
        .build()
        .unwrap_or_else(|error| {
            fatal_error(
                &format!("Invalid PostgreSQL storage settings: {error}"),
                EXIT_CODE_CONFIG_ERROR,
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::get_config;
    use crate::storage::postgres::prelude::*;
    use diesel::dsl::count_star;
    use diesel::insert_into;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_group_name(prefix: &str) -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before Unix epoch")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{now}_{counter}")
    }

    #[test]
    fn database_call_sites_have_stable_bounded_labels() {
        let call_sites = [
            StorageCallSite::EventDelivery,
            StorageCallSite::EventFanout,
            StorageCallSite::EventRetention,
            StorageCallSite::HttpRequest,
            StorageCallSite::MetricsRefresh,
            StorageCallSite::Readiness,
            StorageCallSite::RequestMaintenance,
            StorageCallSite::RestoreCoordinator,
            StorageCallSite::TaskLease,
            StorageCallSite::TaskWorker,
            StorageCallSite::TokenRetention,
            StorageCallSite::Unattributed,
        ];

        assert_eq!(
            call_sites.map(StorageCallSite::as_str),
            [
                "event_delivery",
                "event_fanout",
                "event_retention",
                "http_request",
                "metrics_refresh",
                "readiness",
                "request_maintenance",
                "restore_coordinator",
                "task_lease",
                "task_worker",
                "token_retention",
                "unattributed",
            ]
        );
    }

    #[test]
    fn required_database_migration_matches_latest_migration_directory() {
        let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let latest = std::fs::read_dir(migrations)
            .expect("migration directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().join("up.sql").is_file())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter_map(|name| name.split('_').next().map(str::to_string))
            .map(|version| version.replace('-', ""))
            .max()
            .expect("at least one migration");

        assert_eq!(REQUIRED_DATABASE_MIGRATION_VERSION, latest);
    }

    #[test]
    fn resource_scope_rollback_revokes_tokens_before_dropping_scope_tables() {
        let rollback =
            include_str!("../../../migrations/2026-07-22-000001_token_resource_scopes/down.sql");
        let revoke = rollback
            .find("WHERE resource_scoped")
            .expect("rollback revokes resource-scoped tokens");
        let first_drop = rollback
            .find("DROP TABLE token_object_scopes")
            .expect("rollback drops resource-scope tables");

        assert!(revoke < first_drop);
    }

    #[test]
    fn resource_revision_rollback_drops_the_persistent_computed_field_index() {
        let rollback =
            include_str!("../../../migrations/2026-08-03-000001_resource_revisions/down.sql");

        assert!(rollback.contains("DROP INDEX IF EXISTS computed_field_class_revision_id_idx;"));
    }

    #[test]
    fn resource_revision_migration_has_explicit_phase_transactions() {
        let migration =
            include_str!("../../../migrations/2026-08-03-000001_resource_revisions/up.sql");
        let metadata =
            include_str!("../../../migrations/2026-08-03-000001_resource_revisions/metadata.toml");
        let transaction_count = migration
            .lines()
            .filter(|line| line.trim() == "BEGIN;")
            .count();
        let commit_count = migration
            .lines()
            .filter(|line| line.trim() == "COMMIT;")
            .count();

        assert_eq!(metadata.trim(), "run_in_transaction = false");
        assert_eq!(transaction_count, 16);
        assert_eq!(commit_count, transaction_count);
    }

    #[tokio::test]
    async fn database_schema_readiness_accepts_the_migrated_test_database() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_postgres_pool(&config.database_url, 1);

        ensure_postgres_schema_ready(&pool)
            .await
            .expect("test database should have the latest migration");
    }

    #[tokio::test]
    async fn test_init_pool() {
        let config = get_config().expect("Failed to load config for test");
        let database_url = config.database_url.clone();
        let pool_size = config.db_pool_size;
        let pool = init_postgres_pool(&database_url, pool_size);
        assert_eq!(pool.config().max_size, pool_size);
    }

    #[tokio::test]
    async fn statement_timeout_cancels_slow_queries() {
        let config = get_config().expect("Failed to load config for test");
        let database_url = config.database_url.clone();

        // A tiny timeout must cancel a query that sleeps past the budget...
        let bounded = init_postgres_pool_with_statement_timeout(&database_url, 1, 50);
        let mut conn = bounded
            .get()
            .await
            .expect("failed to acquire bounded connection");
        let slow = diesel::sql_query("SELECT pg_sleep(1)")
            .execute(&mut conn)
            .await;
        assert!(
            slow.is_err(),
            "pg_sleep(1) should be cancelled by a 50ms statement_timeout"
        );
        drop(conn);

        // ...while a fast query on a fresh checkout still succeeds, proving the
        // connection was returned in a usable state.
        let mut conn = bounded
            .get()
            .await
            .expect("failed to re-acquire bounded connection");
        diesel::sql_query("SELECT 1")
            .execute(&mut conn)
            .await
            .expect("fast query should succeed under the timeout");

        // With the timeout disabled (0), the same sleep completes.
        let unbounded = init_postgres_pool_with_statement_timeout(&database_url, 1, 0);
        let mut conn = unbounded
            .get()
            .await
            .expect("failed to acquire unbounded connection");
        diesel::sql_query("SELECT pg_sleep(0.1)")
            .execute(&mut conn)
            .await
            .expect("pg_sleep should complete when statement_timeout is disabled");
    }

    #[tokio::test]
    async fn statement_timeout_ms_new_treats_zero_as_disabled() {
        assert_eq!(StorageQueryBudget::from_millis(0), None);
        assert_eq!(
            StorageQueryBudget::from_millis(50).map(StorageQueryBudget::as_millis),
            Some(50)
        );
    }

    #[tokio::test]
    async fn with_connection_timeout_bounds_and_reverts() {
        let config = get_config().expect("Failed to load config for test");
        // Pool-global timeout disabled, so any cancellation must come from the
        // per-query `SET LOCAL` applied by `with_connection_timeout` itself.
        let pool = init_postgres_pool_with_statement_timeout(&config.database_url, 1, 0);

        // A tiny explicit timeout cancels a query that sleeps past the budget.
        let slow =
            with_connection_timeout(&pool, StorageQueryBudget::from_millis(50), async |conn| {
                diesel::sql_query("SELECT pg_sleep(1)").execute(conn).await
            })
            .await;
        assert!(
            slow.is_err(),
            "pg_sleep(1) should be cancelled by a 50ms per-query statement_timeout"
        );

        // The `SET LOCAL` reverts with the transaction, so a later checkout that
        // passes `None` is unbounded again (proving no leak back to the pool).
        with_connection_timeout(&pool, None, async |conn| {
            diesel::sql_query("SELECT pg_sleep(0.1)")
                .execute(conn)
                .await
        })
        .await
        .expect("pg_sleep should complete when no per-query timeout is applied");
    }

    #[tokio::test]
    async fn ambient_statement_timeout_scope_bounds_with_connection() {
        let config = get_config().expect("Failed to load config for test");
        // Pool-global timeout disabled; the only possible cancel is the ambient
        // scope applied via `with_export_query_budget_scope`.
        let pool = init_postgres_pool_with_statement_timeout(&config.database_url, 1, 0);

        // Inside the scope, a plain `with_connection` call is bounded.
        let bounded = with_export_query_budget_scope(StorageQueryBudget::from_millis(50), async {
            with_connection(&pool, async |conn| {
                diesel::sql_query("SELECT pg_sleep(1)").execute(conn).await
            })
            .await
        })
        .await;
        assert!(
            bounded.is_err(),
            "with_connection inside a 50ms scope should cancel pg_sleep(1)"
        );

        // Outside any scope, the ambient timeout is gone and slow work runs.
        with_connection(&pool, async |conn| {
            diesel::sql_query("SELECT pg_sleep(0.1)")
                .execute(conn)
                .await
        })
        .await
        .expect("with_connection outside a scope must not be bounded");
    }

    #[tokio::test]
    async fn test_with_connection_returns_error_on_invalid_pool() {
        let settings = postgres_pool_settings("postgres://invalid:5432/nonexistent", 1, 0, 100);
        let pool = init_postgres_pool_with_settings(&settings);

        // This should return an error, not panic
        let result = with_connection(&pool, async |_conn| Ok::<_, diesel::result::Error>(())).await;

        assert!(result.is_err());

        // Verify it's the right kind of error
        match result {
            Err(ApiError::DbConnectionError(_)) => {
                // Expected error type
            }
            other => panic!("Expected DbConnectionError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_with_connection_success_path() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_postgres_pool(&config.database_url, 1);

        // This should succeed
        let result =
            with_connection(&pool, async |_conn| Ok::<i32, diesel::result::Error>(42)).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_transaction_rolls_back_on_error() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_postgres_pool(&config.database_url, 1);
        let rollback_name = unique_group_name("with_tx_rollback");

        let result: Result<(), ApiError> =
            with_transaction(&pool, async |conn| -> Result<(), diesel::result::Error> {
                use crate::schema::groups::dsl::{description, groupname, groups};

                insert_into(groups)
                    .values((
                        groupname.eq(&rollback_name),
                        description.eq("rollback-test"),
                    ))
                    .execute(conn)
                    .await?;

                insert_into(groups)
                    .values((
                        groupname.eq(&rollback_name),
                        description.eq("rollback-test-duplicate"),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await;

        assert!(
            matches!(result, Err(ApiError::Conflict(_))),
            "expected unique violation mapped to ApiError::Conflict, got {result:?}",
        );

        let committed_rows = with_connection(&pool, async |conn| {
            use crate::schema::groups::dsl::{groupname, groups};

            groups
                .filter(groupname.eq(&rollback_name))
                .select(count_star())
                .first::<i64>(conn)
                .await
        })
        .await
        .expect("Failed to count rows after rollback test");

        assert_eq!(
            committed_rows, 0,
            "failed transaction should rollback all rows for {rollback_name}",
        );
    }

    #[tokio::test]
    async fn cancelled_transaction_is_discarded_and_rolled_back() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_postgres_pool(&config.database_url, 1);
        let cancelled_name = unique_group_name("with_tx_cancelled");
        let closed_broken_before = pool.state().statistics.connections_closed_broken;
        let (inserted_tx, inserted_rx) = tokio::sync::oneshot::channel();
        let transaction_pool = pool.clone();
        let transaction_name = cancelled_name.clone();

        let transaction_task = tokio::spawn(async move {
            with_transaction(
                &transaction_pool,
                async move |conn| -> Result<(), diesel::result::Error> {
                    use crate::schema::groups::dsl::{description, groupname, groups};

                    insert_into(groups)
                        .values((
                            groupname.eq(&transaction_name),
                            description.eq("cancelled-transaction-test"),
                        ))
                        .execute(conn)
                        .await?;
                    let _ = inserted_tx.send(());
                    std::future::pending::<()>().await;
                    Ok(())
                },
            )
            .await
        });

        inserted_rx
            .await
            .expect("transaction task ended before inserting its marker");
        transaction_task.abort();
        let join_error = transaction_task
            .await
            .expect_err("transaction task should have been cancelled");
        assert!(join_error.is_cancelled());

        let state_after_cancel = pool.state();
        assert!(
            state_after_cancel.statistics.connections_closed_broken > closed_broken_before,
            "the cancelled transaction connection must be discarded instead of pooled",
        );

        let persisted_rows = with_connection(&pool, async |conn| {
            use crate::schema::groups::dsl::{groupname, groups};

            groups
                .filter(groupname.eq(&cancelled_name))
                .select(count_star())
                .first::<i64>(conn)
                .await
        })
        .await
        .expect("replacement connection should remain usable");

        assert_eq!(
            persisted_rows, 0,
            "Postgres should roll back work from the discarded connection",
        );
    }

    #[tokio::test]
    async fn test_with_transaction_commits_on_success() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_postgres_pool(&config.database_url, 1);
        let first_name = unique_group_name("with_tx_commit_one");
        let second_name = unique_group_name("with_tx_commit_two");

        let result: Result<(), ApiError> =
            with_transaction(&pool, async |conn| -> Result<(), diesel::result::Error> {
                use crate::schema::groups::dsl::{description, groupname, groups};

                insert_into(groups)
                    .values((groupname.eq(&first_name), description.eq("commit-test-one")))
                    .execute(conn)
                    .await?;

                insert_into(groups)
                    .values((
                        groupname.eq(&second_name),
                        description.eq("commit-test-two"),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await;

        assert!(
            result.is_ok(),
            "expected transaction commit, got {result:?}"
        );

        let committed_rows = with_connection(&pool, async |conn| {
            use crate::schema::groups::dsl::{groupname, groups};

            groups
                .filter(groupname.eq_any(vec![first_name.clone(), second_name.clone()]))
                .select(count_star())
                .first::<i64>(conn)
                .await
        })
        .await
        .expect("Failed to count rows after commit test");

        assert_eq!(
            committed_rows, 2,
            "successful transaction should commit both rows",
        );

        let _ = with_connection(&pool, async |conn| {
            use crate::schema::groups::dsl::{groupname, groups};

            diesel::delete(groups.filter(groupname.eq_any(vec![first_name, second_name])))
                .execute(conn)
                .await
        })
        .await;
    }
}
