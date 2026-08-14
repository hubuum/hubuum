//! PostgreSQL-owned storage runtime primitives.
//!
//! Diesel and connection-pool types are intentionally exposed only as the
//! integration surface for Hubuum's PostgreSQL adapter. They must not cross
//! the backend-neutral traits in `hubuum-storage-core`.

mod error;
pub mod jsonb;
mod pool;
mod query_capture;

pub use error::PostgresStorageError;
pub use pool::{
    PostgresConnection, PostgresEndpoint, PostgresPool, PostgresPoolBuildError,
    PostgresPoolSettings, PostgresPoolSettingsBuilder, PostgresPooledConnection,
    build_postgres_pool,
};
pub use query_capture::{QueryCaptureSnapshot, capture_queries, configure_connection};
