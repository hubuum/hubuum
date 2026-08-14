//! PostgreSQL-owned storage runtime primitives.
//!
//! Diesel and connection-pool types are intentionally exposed only as the
//! integration surface for Hubuum's PostgreSQL adapter. They must not cross
//! the backend-neutral traits in `hubuum-storage-core`.

pub mod cursor;
mod error;
#[doc(hidden)]
pub mod filters;
pub mod jsonb;
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

pub use error::PostgresStorageError;
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
    PostgresRuntime, PostgresTelemetry, REQUIRED_DATABASE_MIGRATION_VERSION, SendAsyncFn,
    schema_is_ready, with_connection, with_mutation_provenance, with_query_budget,
    with_revision_precondition, with_storage_call_site, with_transaction,
};
