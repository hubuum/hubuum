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
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod test_support;
pub mod worker_notifications;

/// Diesel query-building traits paired with diesel-async's I/O traits.
///
/// This is an intentional PostgreSQL integration surface for adapter-owned
/// tests and diagnostics. Backend-neutral callers must use
/// `hubuum-storage-core` instead.
#[doc(hidden)]
pub mod diesel_async_prelude {
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

    pub use crate::PostgresRevision;
}

pub use backend::PostgresStorage;
pub use error::PostgresStorageError;
#[doc(hidden)]
pub use failpoints::{
    PostgresFaultController, PostgresFaultPoint, PostgresFaultReached, reach_fault_point,
};
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
    DEFAULT_COMPUTED_REINDEX_BATCH_SIZE, NoopPostgresTelemetry, PostgresRuntime, PostgresTelemetry,
    REQUIRED_DATABASE_MIGRATION_VERSION, SendAsyncFn, schema_is_ready, with_connection,
    with_mutation_provenance, with_query_budget, with_revision_precondition,
    with_storage_call_site, with_transaction,
};
