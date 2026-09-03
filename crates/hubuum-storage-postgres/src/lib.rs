//! PostgreSQL-owned storage runtime primitives.
//!
//! Diesel and connection-pool types are intentionally exposed only as the
//! integration surface for Hubuum's PostgreSQL adapter. They must not cross
//! the backend-neutral traits in `hubuum-storage-core`.

mod backend;
mod cursor;
mod database_privileges;
mod diagnostics;
mod error;
mod failpoints;
mod filters;
#[cfg(feature = "embedded-migrations")]
mod migrations;
mod operations;
mod pool;
#[cfg(feature = "query-capture")]
mod query_capture;
mod revision;
mod runtime;
#[cfg(feature = "scale-benchmark-support")]
#[doc(hidden)]
pub mod scale_benchmark;
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod schema;
#[cfg(not(feature = "integration-test-support"))]
mod schema;
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod test_support;
#[cfg(feature = "integration-test-support")]
pub mod worker_notifications;
#[cfg(not(feature = "integration-test-support"))]
mod worker_notifications;

/// Diesel query-building traits paired with diesel-async's I/O traits.
///
/// This is an intentional PostgreSQL integration surface for adapter-owned
/// tests and diagnostics. Backend-neutral callers must use
/// `hubuum-storage-core` instead.
#[cfg(feature = "integration-test-support")]
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
pub(crate) use cursor::{apply_cursor_ordering_fields, apply_query_options_with_fields};
pub use database_privileges::{
    DatabasePrivilegeFinding, DatabasePrivilegeReport, DatabaseRoleName, DatabaseRoleNames,
    database_privilege_capabilities, database_privilege_manifest_json,
    database_role_reconciliation_sql, database_role_setup_sql, inspect_database_privileges,
};
pub(crate) use diagnostics::{
    PostgresPoolAcquisitionState, PostgresPoolCapacity, PostgresPoolConnectionState,
};
pub use diagnostics::{PostgresPoolState, PostgresStorageSnapshot};
pub use error::PostgresStorageError;
pub(crate) use error::{persisted_candidate_page, persisted_page, validate_persisted};
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub use failpoints::{
    PostgresFaultController, PostgresFaultPoint, PostgresFaultReached, reach_fault_point,
};
#[cfg(not(feature = "integration-test-support"))]
pub(crate) use failpoints::{PostgresFaultPoint, reach_fault_point};
pub(crate) use filters::{
    postgres_boolean_filter, postgres_datetime_filter, postgres_integer_filter,
    postgres_is_null_filter, postgres_revision_filter, postgres_string_filter,
};
#[cfg(feature = "embedded-migrations")]
pub use migrations::{
    prepare_disposable_restore_database, reset_disposable_restore_database,
    run_embedded_migrations, run_embedded_migrations_as,
};
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
#[doc(hidden)]
pub use operations::computed_materialization::source_data_sha256;
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
#[doc(hidden)]
pub use operations::json_filter::compile_json_filter_for_benchmark;
pub use pool::{
    PostgresConnection, PostgresEndpoint, PostgresPool, PostgresPoolBuildError,
    PostgresPoolSettings, PostgresPoolSettingsBuilder, PostgresPooledConnection,
    build_postgres_pool,
};
#[cfg(feature = "query-capture")]
pub use query_capture::{QueryCaptureSnapshot, capture_queries, configure_connection};
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub use revision::PostgresRevision;
#[cfg(not(feature = "integration-test-support"))]
pub(crate) use revision::PostgresRevision;
#[cfg(not(any(feature = "integration-test-support", feature = "benchmark-support")))]
pub(crate) use runtime::SendAsyncFn;
pub use runtime::{DEFAULT_COMPUTED_REINDEX_BATCH_SIZE, PostgresObserver};
pub(crate) use runtime::{
    NoopPostgresObserver, PostgresRuntime, with_mutation_provenance, with_query_budget,
    with_revision_precondition, with_storage_call_site,
};
#[cfg(any(feature = "integration-test-support", feature = "benchmark-support"))]
#[doc(hidden)]
pub use runtime::{SendAsyncFn, schema_is_ready, with_connection, with_transaction};
