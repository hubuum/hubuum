pub(crate) mod json;
pub mod traits;

#[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
mod query_capture;
#[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
pub use query_capture::{QueryCaptureSnapshot, capture_queries};

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

use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use futures_util::FutureExt;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use rustls::{ClientConfig, RootCertStore};
use rustls_platform_verifier::BuilderVerifierExt;
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use tracing::debug;

use crate::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use crate::models::search::StatementTimeoutMs;
use crate::observability::metrics::{self, ResultKind};
use crate::utilities::db::DatabaseUrlComponents;

pub type DbConnection = AsyncPgConnection;
pub type DbPool = Pool<DbConnection>;

/// Latest migration required by this binary. The test below keeps this value
/// synchronized with the migration directory so readiness cannot silently lag
/// behind a newly added schema change.
pub const REQUIRED_DATABASE_MIGRATION_VERSION: &str = "20260722000001";

#[derive(diesel::QueryableByName)]
struct DatabaseSchemaReadiness {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    ready: bool,
}

async fn database_schema_is_ready(
    connection: &mut DbConnection,
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
pub async fn ensure_database_schema_ready(pool: &DbPool) -> Result<(), ApiError> {
    let ready = with_connection(pool, async |connection| {
        database_schema_is_ready(connection).await
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

/// Consumer-owned settings needed to construct the process database pool.
///
/// Keeping this type in the database adapter means startup can translate from
/// env/CLI configuration once, without giving the adapter the whole AppConfig.
pub struct DatabasePoolSettings {
    database_url: String,
    max_size: u32,
    statement_timeout_ms: u64,
    acquire_timeout_ms: u64,
}

impl DatabasePoolSettings {
    pub fn builder(database_url: impl Into<String>) -> DatabasePoolSettingsBuilder {
        DatabasePoolSettingsBuilder {
            database_url: database_url.into(),
            max_size: None,
            statement_timeout_ms: 0,
            acquire_timeout_ms: None,
        }
    }
}

pub struct DatabasePoolSettingsBuilder {
    database_url: String,
    max_size: Option<u32>,
    statement_timeout_ms: u64,
    acquire_timeout_ms: Option<u64>,
}

impl DatabasePoolSettingsBuilder {
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = Some(max_size);
        self
    }

    pub fn statement_timeout_ms(mut self, statement_timeout_ms: u64) -> Self {
        self.statement_timeout_ms = statement_timeout_ms;
        self
    }

    pub fn acquire_timeout_ms(mut self, acquire_timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = Some(acquire_timeout_ms);
        self
    }

    pub fn build(self) -> Result<DatabasePoolSettings, String> {
        let max_size = self
            .max_size
            .ok_or_else(|| "database pool size is required".to_string())?;
        let acquire_timeout_ms = self
            .acquire_timeout_ms
            .ok_or_else(|| "database pool acquire timeout is required".to_string())?;
        let database_url = self.database_url;
        if database_url.trim().is_empty() {
            return Err("database URL must not be empty".to_string());
        }
        if max_size == 0 {
            return Err("database pool size must be greater than zero".to_string());
        }
        if acquire_timeout_ms == 0 {
            return Err("database pool acquire timeout must be greater than zero".to_string());
        }

        Ok(DatabasePoolSettings {
            database_url,
            max_size,
            statement_timeout_ms: self.statement_timeout_ms,
            acquire_timeout_ms,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseTlsMode {
    Disable,
    Verify,
}

fn database_tls_mode(database_url: &str, host: &str) -> Result<DatabaseTlsMode, String> {
    let mut explicit_mode = None;
    if let Some((_, query)) = database_url.split_once('?') {
        for (key, value) in query
            .split('&')
            .filter_map(|parameter| parameter.split_once('='))
        {
            if key.eq_ignore_ascii_case("sslmode") && explicit_mode.replace(value).is_some() {
                return Err("PostgreSQL sslmode must not be repeated".to_string());
            }
        }
    }

    match explicit_mode {
        Some("disable") => Ok(DatabaseTlsMode::Disable),
        Some("prefer" | "require") => Ok(DatabaseTlsMode::Verify),
        Some(mode) => Err(format!(
            "Unsupported PostgreSQL sslmode '{mode}'; expected disable, prefer, or require"
        )),
        None if is_loopback_database_host(host) => Ok(DatabaseTlsMode::Disable),
        None => Ok(DatabaseTlsMode::Verify),
    }
}

fn is_loopback_database_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn database_tls_config() -> Result<ClientConfig, diesel::result::ConnectionError> {
    let builder = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;

    let Some(root_cert_path) = std::env::var_os("PGSSLROOTCERT") else {
        return builder
            .with_platform_verifier()
            .map(|builder| builder.with_no_client_auth())
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()));
    };

    // PostgreSQL 17 accepts `PGSSLROOTCERT=system`; match that behavior by
    // delegating to the platform verifier rather than treating it as a path.
    if root_cert_path == "system" {
        return builder
            .with_platform_verifier()
            .map(|builder| builder.with_no_client_auth())
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()));
    }

    let certificates = CertificateDer::pem_file_iter(&root_cert_path)
        .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
    let mut roots = RootCertStore::empty();
    let mut root_count = 0usize;
    for certificate in certificates {
        roots
            .add(certificate.map_err(|error| {
                diesel::result::ConnectionError::BadConnection(error.to_string())
            })?)
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
        root_count += 1;
    }
    if root_count == 0 {
        return Err(diesel::result::ConnectionError::BadConnection(format!(
            "PGSSLROOTCERT contains no certificates: {}",
            root_cert_path.to_string_lossy()
        )));
    }

    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

async fn establish_database_connection(
    database_url: &str,
    tls_config: Option<ClientConfig>,
) -> Result<DbConnection, diesel::result::ConnectionError> {
    let connection = if let Some(tls_config) = tls_config {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
        let (client, connection) = tokio_postgres::connect(database_url, tls)
            .await
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
        DbConnection::try_from_client_and_connection(client, connection).await?
    } else {
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(|error| diesel::result::ConnectionError::BadConnection(error.to_string()))?;
        DbConnection::try_from_client_and_connection(client, connection).await?
    };

    Ok(connection)
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

async fn acquire_connection(pool: &DbPool) -> Result<PooledConnection<'_, DbConnection>, ApiError> {
    let start = std::time::Instant::now();
    match pool.get().await {
        Ok(conn) => {
            #[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
            let mut conn = conn;
            #[cfg(any(test, feature = "query-capture", feature = "integration-test-support"))]
            query_capture::configure_connection(&mut conn);
            metrics::db_connection_acquired(start.elapsed());
            Ok(conn)
        }
        Err(error) => {
            metrics::db_connection_acquire_failed(start.elapsed());
            Err(ApiError::from(error))
        }
    }
}

tokio::task_local! {
    /// The per-query Postgres `statement_timeout` in effect for the current
    /// async task, if any. Set for the duration of a scope via
    /// [`with_statement_timeout_scope`] and consulted by [`with_connection`] /
    /// [`with_transaction`] so that all DB work inside the scope is bounded
    /// without threading a timeout through every caller. Outside any scope the
    /// lookup yields `None`, so behavior is unchanged.
    static AMBIENT_STATEMENT_TIMEOUT: Option<StatementTimeoutMs>;
}

tokio::task_local! {
    /// The acting user id for the current async task, if any. Set via
    /// [`with_actor_scope`] and applied as a transaction-local
    /// `SET LOCAL hubuum.actor_id` by [`with_connection_timeout`] /
    /// [`with_transaction`], so the history trigger can attribute writes to a
    /// user without threading the actor through every caller. Outside any scope
    /// the lookup yields `None`, recorded as a NULL actor.
    static AMBIENT_ACTOR: Option<i32>;
}

/// Run `future` with an ambient per-query `statement_timeout` in effect.
///
/// While the future is being polled, every [`with_connection`] /
/// [`with_transaction`] call made on the same task applies the given
/// `statement_timeout` as a transaction-local `SET LOCAL statement_timeout`.
/// This is how the export execution path bounds its queries independently of
/// the pool-global `db_statement_timeout_ms`, without threading the timeout
/// through the search layer. A `statement_timeout` of `None` is a no-op scope.
pub async fn with_statement_timeout_scope<F, R>(
    statement_timeout: Option<StatementTimeoutMs>,
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
/// when not running inside a [`with_statement_timeout_scope`] (including from
/// synchronous, non-task contexts).
fn ambient_statement_timeout() -> Option<StatementTimeoutMs> {
    AMBIENT_STATEMENT_TIMEOUT
        .try_with(|timeout| *timeout)
        .unwrap_or(None)
}

/// Run `future` with an ambient actor id in effect (see [`AMBIENT_ACTOR`]).
pub async fn with_actor_scope<F, R>(actor: Option<i32>, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_ACTOR.scope(actor, future).await
}

/// The ambient actor id for the current task, or `None` outside any scope.
fn ambient_actor() -> Option<i32> {
    AMBIENT_ACTOR.try_with(|actor| *actor).unwrap_or(None)
}

/// Apply a transaction-local `SET LOCAL hubuum.actor_id`. Bound, not formatted,
/// mirroring [`set_local_statement_timeout`]. Reverts at COMMIT/ROLLBACK.
async fn set_local_actor(conn: &mut DbConnection, actor: i32) -> Result<(), diesel::result::Error> {
    diesel::sql_query("SELECT set_config('hubuum.actor_id', $1, true)")
        .bind::<diesel::sql_types::Text, _>(actor.to_string())
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
    conn: &mut DbConnection,
    statement_timeout: StatementTimeoutMs,
) -> Result<(), diesel::result::Error> {
    diesel::sql_query("SELECT set_config('statement_timeout', $1, true)")
        .bind::<diesel::sql_types::Text, _>(statement_timeout.as_millis().to_string())
        .execute(conn)
        .await?;
    Ok(())
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
/// If a [`with_statement_timeout_scope`] is in effect on the current task, the
/// work is automatically bounded by that per-query `statement_timeout` (see
/// [`with_connection_timeout`]). Otherwise no timeout is applied.
///
/// Note: block closures that use `?` and end with `Ok(...)` may require an explicit closure
/// return type, for example:
/// `with_connection(pool, async |conn| -> Result<_, diesel::result::Error> { ... }).await`
pub async fn with_connection<F, R, E>(pool: &DbPool, f: F) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(&'conn mut DbConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut DbConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    with_connection_timeout(pool, ambient_statement_timeout(), f).await
}

/// Compatibility alias retained while callers migrate from the former
/// `spawn_blocking` bridge. Both helpers now execute non-blocking database I/O.
pub async fn with_connection_async<F, R, E>(pool: DbPool, f: F) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(&'conn mut DbConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut DbConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    let statement_timeout = ambient_statement_timeout();
    let actor = ambient_actor();
    with_connection_context(&pool, statement_timeout, actor, f).await
}

async fn with_connection_context<F, R, E>(
    pool: &DbPool,
    statement_timeout: Option<StatementTimeoutMs>,
    actor: Option<i32>,
    f: F,
) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(&'conn mut DbConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut DbConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    let mut conn = acquire_connection(pool).await?;
    let start = std::time::Instant::now();
    let result = if statement_timeout.is_none() && actor.is_none() {
        f(&mut conn).await.map_err(ApiError::from)
    } else {
        conn.transaction::<R, ApiError, _>(async move |conn| {
            if let Some(statement_timeout) = statement_timeout {
                set_local_statement_timeout(conn, statement_timeout).await?;
            }
            if let Some(actor) = actor {
                set_local_actor(conn, actor).await?;
            }
            f(conn).await.map_err(ApiError::from)
        })
        .await
    };
    let result_kind = match &result {
        Ok(_) => ResultKind::Ok,
        Err(error) => ResultKind::Error(error.class()),
    };
    metrics::db_operation_finished("connection", start.elapsed(), &result_kind);
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
/// [`with_statement_timeout_scope`]; this explicit variant exists for callers
/// (and tests) that want to pass the timeout directly.
///
/// Note: this intentionally wraps a (possibly read-only) closure in a
/// transaction. That is contrary to the usual "single reads use
/// [`with_connection`]" guidance, but the transaction here exists solely to
/// scope `SET LOCAL`, not for multi-statement atomicity, and is encapsulated in
/// this one helper rather than imposed on callers.
pub async fn with_connection_timeout<F, R, E>(
    pool: &DbPool,
    statement_timeout: Option<StatementTimeoutMs>,
    f: F,
) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(&'conn mut DbConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut DbConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    with_connection_context(pool, statement_timeout, ambient_actor(), f).await
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
/// If a [`with_statement_timeout_scope`] is in effect on the current task, a
/// transaction-local `SET LOCAL statement_timeout` is applied at the start of
/// the transaction so this work is bounded too.
///
/// As with [`with_connection`], the closure may return any error type `E` that converts into
/// [`ApiError`]. Block closures that end with `Ok(...)` may need an explicit closure return type,
/// for example:
/// `with_transaction(pool, async |conn| -> Result<_, ApiError> { ... }).await`
pub async fn with_transaction<F, R, E>(pool: &DbPool, f: F) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(&'conn mut DbConnection) -> Result<R, E>
        + for<'conn> SendAsyncFn<&'conn mut DbConnection, Result<R, E>, Fut: Send>
        + Send,
    R: Send,
    E: Send,
    ApiError: From<E>,
{
    let statement_timeout = ambient_statement_timeout();
    let actor = ambient_actor();
    let mut conn = acquire_connection(pool).await?;
    let start = std::time::Instant::now();
    let result = crate::logger::defer_operation_mutation_logs_until_commit(
        conn.transaction::<R, ApiError, _>(async move |conn| {
            if let Some(statement_timeout) = statement_timeout {
                set_local_statement_timeout(conn, statement_timeout).await?;
            }
            if let Some(actor) = actor {
                set_local_actor(conn, actor).await?;
            }
            f(conn).await.map_err(ApiError::from)
        }),
    )
    .await;
    let result_kind = match &result {
        Ok(_) => ResultKind::Ok,
        Err(error) => ResultKind::Error(error.class()),
    };
    metrics::db_operation_finished("transaction", start.elapsed(), &result_kind);
    result
}

pub fn init_pool(database_url: &str, max_size: u32) -> DbPool {
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
    init_pool_with_timeouts(
        database_url,
        max_size,
        statement_timeout_ms,
        acquire_timeout_ms,
    )
}

pub fn init_pool_with_settings(settings: &DatabasePoolSettings) -> DbPool {
    if settings.acquire_timeout_ms == crate::config::DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS {
        return init_pool_with_statement_timeout(
            &settings.database_url,
            settings.max_size,
            settings.statement_timeout_ms,
        );
    }
    init_pool_with_timeouts(
        &settings.database_url,
        settings.max_size,
        settings.statement_timeout_ms,
        settings.acquire_timeout_ms,
    )
}

/// Build a pool with an explicit Postgres `statement_timeout` (in milliseconds)
/// applied to every connection on acquisition. A value of 0 disables the
/// timeout. Exposed so tests can exercise the customizer without mutating the
/// global config.
pub fn init_pool_with_statement_timeout(
    database_url: &str,
    max_size: u32,
    statement_timeout_ms: u64,
) -> DbPool {
    init_pool_with_timeouts(
        database_url,
        max_size,
        statement_timeout_ms,
        crate::config::DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS,
    )
}

fn init_pool_with_timeouts(
    database_url: &str,
    max_size: u32,
    statement_timeout_ms: u64,
    acquire_timeout_ms: u64,
) -> DbPool {
    let database_host = match database_url.parse::<DatabaseUrlComponents>() {
        Ok(components) => {
            debug!(
                message = "Database URL parsed.",
                vendor = %components.vendor(),
                username = components.username(),
                host = components.host(),
                port = components.port(),
                database = components.database(),
            );
            components.host().to_string()
        }
        Err(err) => fatal_error(
            &format!("Failed to parse database URL: {}", err),
            EXIT_CODE_CONFIG_ERROR,
        ),
    };

    let tls_mode = database_tls_mode(database_url, &database_host).unwrap_or_else(|error| {
        fatal_error(
            &format!("Failed to configure database TLS: {error}"),
            EXIT_CODE_CONFIG_ERROR,
        )
    });

    let mut manager_config = ManagerConfig::<DbConnection>::default();
    let tls_config = match tls_mode {
        DatabaseTlsMode::Disable => None,
        DatabaseTlsMode::Verify => Some(database_tls_config().unwrap_or_else(|error| {
            fatal_error(
                &format!("Failed to configure database TLS: {error}"),
                EXIT_CODE_CONFIG_ERROR,
            )
        })),
    };
    manager_config.custom_setup = Box::new(move |url| {
        let tls_config = tls_config.clone();
        async move {
            let mut conn = establish_database_connection(url, tls_config).await?;

            if statement_timeout_ms > 0 {
                diesel::sql_query("SELECT set_config('statement_timeout', $1, false)")
                    .bind::<diesel::sql_types::Text, _>(statement_timeout_ms.to_string())
                    .execute(&mut conn)
                    .await
                    .map_err(|error| {
                        diesel::result::ConnectionError::BadConnection(error.to_string())
                    })?;
            }
            Ok(conn)
        }
        .boxed()
    });
    let manager =
        AsyncDieselConnectionManager::<DbConnection>::new_with_config(database_url, manager_config);

    let builder = Pool::builder()
        .max_size(max_size)
        .connection_timeout(Duration::from_millis(acquire_timeout_ms));

    // Pool construction remains synchronous for compatibility with the test
    // fixture singleton. The first checkout establishes connections lazily;
    // startup initialization immediately verifies connectivity.
    builder.build_unchecked(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::get_config;
    use crate::db::prelude::*;
    use diesel::dsl::count_star;
    use diesel::insert_into;
    use rstest::rstest;
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

    #[tokio::test]
    async fn database_schema_readiness_accepts_the_migrated_test_database() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_pool(&config.database_url, 1);

        ensure_database_schema_ready(&pool)
            .await
            .expect("test database should have the latest migration");
    }

    #[rstest]
    #[case::empty_url("", 1, 100, "database URL must not be empty")]
    #[case::zero_pool_size(
        "postgres://localhost/db",
        0,
        100,
        "database pool size must be greater than zero"
    )]
    #[case::zero_acquire_timeout(
        "postgres://localhost/db",
        1,
        0,
        "database pool acquire timeout must be greater than zero"
    )]
    fn database_pool_settings_reject_invalid_values(
        #[case] database_url: &str,
        #[case] max_size: u32,
        #[case] acquire_timeout_ms: u64,
        #[case] expected: &str,
    ) {
        let result = DatabasePoolSettings::builder(database_url)
            .max_size(max_size)
            .acquire_timeout_ms(acquire_timeout_ms)
            .build();

        assert_eq!(result.err().as_deref(), Some(expected));
    }

    #[rstest]
    #[case::localhost("localhost")]
    #[case::ipv4("127.0.0.1")]
    #[case::ipv6("::1")]
    fn implicit_loopback_database_urls_disable_tls(#[case] host: &str) {
        assert_eq!(
            database_tls_mode("postgres://postgres@localhost/hubuum", host),
            Ok(DatabaseTlsMode::Disable)
        );
    }

    #[rstest]
    #[case::local_require(
        "postgres://postgres@localhost/hubuum?sslmode=require",
        "localhost",
        DatabaseTlsMode::Verify
    )]
    #[case::local_prefer(
        "postgres://postgres@localhost/hubuum?sslmode=prefer",
        "localhost",
        DatabaseTlsMode::Verify
    )]
    #[case::remote_disable(
        "postgres://postgres@db.example.com/hubuum?sslmode=disable",
        "db.example.com",
        DatabaseTlsMode::Disable
    )]
    #[case::remote_implicit(
        "postgres://postgres@db.example.com/hubuum",
        "db.example.com",
        DatabaseTlsMode::Verify
    )]
    fn database_tls_mode_follows_url_and_host(
        #[case] database_url: &str,
        #[case] host: &str,
        #[case] expected: DatabaseTlsMode,
    ) {
        assert_eq!(database_tls_mode(database_url, host), Ok(expected));
    }

    #[test]
    fn unsupported_database_sslmode_is_rejected() {
        let error = database_tls_mode(
            "postgres://postgres@db.example.com/hubuum?sslmode=verify-full",
            "db.example.com",
        )
        .unwrap_err();

        assert!(error.contains("Unsupported PostgreSQL sslmode 'verify-full'"));
    }

    #[test]
    fn repeated_database_sslmode_is_rejected() {
        assert!(matches!(
            database_tls_mode(
                "postgres://postgres@db.example.com/hubuum?sslmode=require&sslmode=disable",
                "db.example.com"
            ),
            Err(message) if message == "PostgreSQL sslmode must not be repeated"
        ));
    }

    #[tokio::test]
    async fn test_init_pool() {
        let config = get_config().expect("Failed to load config for test");
        let database_url = config.database_url.clone();
        let pool_size = config.db_pool_size;
        let pool = init_pool(&database_url, pool_size);
        assert_eq!(pool.config().max_size, pool_size);
    }

    #[tokio::test]
    async fn statement_timeout_cancels_slow_queries() {
        let config = get_config().expect("Failed to load config for test");
        let database_url = config.database_url.clone();

        // A tiny timeout must cancel a query that sleeps past the budget...
        let bounded = init_pool_with_statement_timeout(&database_url, 1, 50);
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
        let unbounded = init_pool_with_statement_timeout(&database_url, 1, 0);
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
        assert_eq!(StatementTimeoutMs::new(0), None);
        assert_eq!(
            StatementTimeoutMs::new(50).map(StatementTimeoutMs::as_millis),
            Some(50)
        );
    }

    #[tokio::test]
    async fn with_connection_timeout_bounds_and_reverts() {
        let config = get_config().expect("Failed to load config for test");
        // Pool-global timeout disabled, so any cancellation must come from the
        // per-query `SET LOCAL` applied by `with_connection_timeout` itself.
        let pool = init_pool_with_statement_timeout(&config.database_url, 1, 0);

        // A tiny explicit timeout cancels a query that sleeps past the budget.
        let slow = with_connection_timeout(&pool, StatementTimeoutMs::new(50), async |conn| {
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
        // scope applied via `with_statement_timeout_scope`.
        let pool = init_pool_with_statement_timeout(&config.database_url, 1, 0);

        // Inside the scope, a plain `with_connection` call is bounded.
        let bounded = with_statement_timeout_scope(StatementTimeoutMs::new(50), async {
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
        let pool = init_pool_with_timeouts("postgres://invalid:5432/nonexistent", 1, 0, 100);

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
        let pool = init_pool(&config.database_url, 1);

        // This should succeed
        let result =
            with_connection(&pool, async |_conn| Ok::<i32, diesel::result::Error>(42)).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_transaction_rolls_back_on_error() {
        let config = get_config().expect("Failed to load config for test");
        let pool = init_pool(&config.database_url, 1);
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
        let pool = init_pool(&config.database_url, 1);
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
        let pool = init_pool(&config.database_url, 1);
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
