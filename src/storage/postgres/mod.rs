#[cfg(test)]
mod backend_tests;
mod error;
#[cfg(any(test, feature = "integration-test-support"))]
mod failpoints;
#[cfg(any(test, feature = "integration-test-support"))]
mod notifications;
// Legacy row fixtures and SQL assertions used by the root crate's unit and
// integration tests. Production storage operations live in the adapter crate.
#[cfg(any(test, feature = "integration-test-support"))]
#[doc(hidden)]
pub mod operations;
mod runtime;

#[cfg(test)]
pub(crate) use failpoints::{PostgresFailpoint, with_failpoint};
pub use runtime::*;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use hubuum_storage_core::{StorageCallSite, StorageErrorKind};
pub(crate) use hubuum_storage_postgres::PostgresStorage;
use hubuum_storage_postgres::PostgresTelemetry;

#[derive(Debug)]
struct ApplicationPostgresTelemetry;

impl PostgresTelemetry for ApplicationPostgresTelemetry {
    fn connection_acquired(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquired(call_site.as_str(), duration);
    }

    fn connection_acquisition_failed(&self, call_site: StorageCallSite, duration: Duration) {
        crate::observability::metrics::db_connection_acquire_failed(call_site.as_str(), duration);
    }

    fn operation_finished(
        &self,
        call_site: StorageCallSite,
        operation: &'static str,
        duration: Duration,
        error: Option<StorageErrorKind>,
    ) {
        let result = error.map_or(crate::observability::metrics::ResultKind::Ok, |kind| {
            crate::observability::metrics::ResultKind::Error(kind.as_str())
        });
        crate::observability::metrics::db_operation_finished(
            call_site.as_str(),
            operation,
            duration,
            &result,
        );
    }

    fn computed_evaluation(&self, scope: &'static str, error_codes: &[&'static str]) {
        crate::observability::metrics::computed_evaluation_summary(scope, error_codes);
    }

    fn computed_live_fallback(&self) {
        crate::observability::metrics::computed_live_fallback();
    }

    fn computed_read_repair(&self, outcome: &'static str) {
        crate::observability::metrics::computed_read_repair(outcome);
    }

    fn revision_condition(&self, outcome: &'static str) {
        crate::observability::metrics::revision_condition(outcome);
    }

    fn task_completed(&self, kind: &'static str, status: &'static str, duration: Option<Duration>) {
        crate::observability::metrics::task_completed(kind, status, duration);
    }

    fn computed_rebuild_finished(&self, outcome: &'static str, duration: Duration) {
        crate::observability::metrics::computed_rebuild_finished(outcome, duration);
    }

    fn computed_rebuild_batch(&self, object_count: usize) {
        crate::observability::metrics::computed_rebuild_batch(object_count);
    }
}

#[cfg(feature = "embedded-migrations")]
pub(in crate::storage) use hubuum_storage_postgres::run_embedded_migrations;

pub(crate) fn configured_postgres_storage(pool: PostgresPool) -> PostgresStorage {
    let computed_reindex_batch_size = crate::config::get_config()
        .ok()
        .and_then(|config| NonZeroUsize::new(config.computed_reindex_batch_size))
        .unwrap_or(hubuum_storage_postgres::DEFAULT_COMPUTED_REINDEX_BATCH_SIZE);
    PostgresStorage::new(pool, Arc::new(ApplicationPostgresTelemetry))
        .with_computed_reindex_batch_size(computed_reindex_batch_size)
}

pub(crate) fn configured_postgres_storage_with_operational_pools(
    pool: PostgresPool,
    operational_pool_settings: &PostgresPoolSettings,
) -> PostgresStorage {
    let task_lease_pool = runtime::init_postgres_pool_with_settings(operational_pool_settings);
    let notification_listener_pool =
        runtime::init_postgres_pool_with_settings(operational_pool_settings);
    configured_postgres_storage(pool)
        .with_task_lease_pool(task_lease_pool)
        .with_notification_listener_pool(notification_listener_pool)
}
