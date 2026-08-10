//! Explicit PostgreSQL adapter hooks for opt-in database benchmarks.
//!
//! This module is excluded from production builds and exists only for the
//! `postgres-bench` benchmark target.

use crate::services::Services;
use crate::storage::postgres::PostgresPool;
use crate::storage::{DynLifecycleStorage, PostgresStorage};

/// Build lifecycle services around the PostgreSQL adapter for benchmarks.
#[must_use]
pub fn services_for_postgres(pool: PostgresPool) -> Services {
    Services::from_lifecycle_storage(DynLifecycleStorage::from_backend(PostgresStorage::new(
        pool,
    )))
}
