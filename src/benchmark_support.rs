//! Explicit application adapter hooks for benchmark targets.
//!
//! The root library is an internal application composition crate. These
//! helpers keep benchmark-only construction out of normal service APIs while
//! allowing benchmarks to exercise the same observed storage boundary.

use std::sync::Arc;

use crate::services::CollectionService;
use crate::storage::{
    ApplicationStorageTelemetry, CollectionStore, ObservedStorage, StorageIdentity,
};

#[cfg(feature = "postgres-bench")]
use crate::services::Services;
#[cfg(feature = "postgres-bench")]
use crate::storage::postgres::PostgresPool;
#[cfg(feature = "postgres-bench")]
use crate::storage::{BenchmarkStorageContext, StorageHandle};

/// Build the collection service around the production observability wrapper.
///
/// Deterministic benchmarks provide a fixed storage capability so the
/// measurement covers only application-side service, diagnostics, and DTO
/// conversion work.
#[must_use]
pub fn observed_collection_service<S>(storage: S) -> CollectionService
where
    S: CollectionStore + StorageIdentity + 'static,
{
    CollectionService::new(Arc::new(ObservedStorage::new(
        storage,
        Arc::new(ApplicationStorageTelemetry),
    )))
}

/// Compose a PostgreSQL pool into the same opaque context used by the
/// application boundary.
#[cfg(feature = "postgres-bench")]
#[must_use]
pub fn storage_for_postgres(pool: PostgresPool) -> BenchmarkStorageContext {
    StorageHandle::postgres(pool)
}

/// Build resource-family services from an already-composed benchmark context.
#[cfg(feature = "postgres-bench")]
#[must_use]
pub fn services_for_storage(storage: &BenchmarkStorageContext) -> Services {
    Services::from_storage(storage.clone())
}
