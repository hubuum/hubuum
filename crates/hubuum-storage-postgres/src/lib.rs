//! PostgreSQL-owned storage runtime primitives.
//!
//! Diesel and connection-pool types are intentionally exposed only as the
//! integration surface for Hubuum's PostgreSQL adapter. They must not cross
//! the backend-neutral traits in `hubuum-storage-core`.

mod error;
pub mod jsonb;
#[cfg(feature = "embedded-migrations")]
mod migrations;
mod pool;
mod query_capture;
mod revision;
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
