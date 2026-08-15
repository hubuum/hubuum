//! PostgreSQL-owned storage runtime primitives.
//!
//! Diesel and connection-pool types are intentionally exposed only as the
//! integration surface for Hubuum's PostgreSQL adapter. They must not cross
//! the backend-neutral traits in `hubuum-storage-core`.

mod backend;
pub mod cursor;
mod error;
mod failpoints;
#[doc(hidden)]
pub mod filters;
#[cfg(feature = "embedded-migrations")]
mod migrations;
#[doc(hidden)]
pub mod operations;
mod pool;
mod query_capture;
mod revision;
mod runtime;
#[doc(hidden)]
pub mod schema;
pub mod worker_notifications;

pub use backend::PostgresStorage;
pub use error::PostgresStorageError;
#[doc(hidden)]
pub use failpoints::{PostgresFailpoint, check_failpoint, with_failpoint};
#[cfg(feature = "embedded-migrations")]
pub use migrations::run_embedded_migrations;
pub use pool::{
    PostgresConnection, PostgresEndpoint, PostgresPool, PostgresPoolBuildError,
    PostgresPoolSettings, PostgresPoolSettingsBuilder, PostgresPooledConnection,
    build_postgres_pool,
};
pub use query_capture::{QueryCaptureSnapshot, capture_queries, configure_connection};
#[doc(hidden)]
pub use revision::PostgresRevision;
pub use runtime::{
    DEFAULT_COMPUTED_REINDEX_BATCH_SIZE, PostgresRuntime, PostgresTelemetry,
    REQUIRED_DATABASE_MIGRATION_VERSION, SendAsyncFn, schema_is_ready, with_connection,
    with_mutation_provenance, with_query_budget, with_revision_precondition,
    with_storage_call_site, with_transaction,
};
