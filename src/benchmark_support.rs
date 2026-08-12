//! Explicit PostgreSQL adapter hooks for opt-in database benchmarks.
//!
//! This module is excluded from production builds and exists only for the
//! `postgres-bench` benchmark target.

use crate::services::Services;
use crate::storage::postgres::PostgresPool;
use crate::storage::{BenchmarkStorageContext, StorageHandle};

/// Compose a PostgreSQL pool into the same opaque context used by the
/// application boundary.
#[must_use]
pub fn storage_for_postgres(pool: PostgresPool) -> BenchmarkStorageContext {
    StorageHandle::postgres(pool)
}

/// Build lifecycle services from an already-composed benchmark context.
#[must_use]
pub fn services_for_storage(storage: &BenchmarkStorageContext) -> Services {
    Services::from_lifecycle_storage(storage.lifecycle_storage())
}
