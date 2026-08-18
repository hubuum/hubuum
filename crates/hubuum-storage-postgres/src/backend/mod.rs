//! Complete PostgreSQL implementation of the backend-neutral storage contract.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use hubuum_domain::{
    ClassId, CollectionId, EventDeliverySettings, EventFanoutSettings, EventRetentionSettings,
    MaintenanceState, ObjectId, TokenRetentionSettings,
};
use hubuum_events_core::{EventContext, MutationProvenance};
use hubuum_query::QueryOptions;
use hubuum_storage_core::*;

use crate::{PostgresPool, PostgresRuntime, PostgresTelemetry};

mod backup_snapshot;
mod capabilities;
mod computed_fields;
mod export_templates;
mod imports;
mod notifications;
mod remote_targets;
mod restores;
mod task_execution;
mod task_queue;
mod transaction;

/// Complete statically linked PostgreSQL storage backend.
///
/// Native pools and runtime state remain private. Application composition may
/// attach telemetry and dedicated operational pools before placing this value
/// behind its opaque backend handle.
#[derive(Clone)]
pub struct PostgresStorage {
    runtime: PostgresRuntime,
    notification_listener_pool: PostgresPool,
}

impl PostgresStorage {
    #[must_use]
    pub fn new(pool: PostgresPool, telemetry: Arc<dyn PostgresTelemetry>) -> Self {
        Self {
            runtime: PostgresRuntime::new(pool.clone(), telemetry),
            notification_listener_pool: pool,
        }
    }

    /// Construct a backend with an explicit telemetry opt-out.
    ///
    /// This is intended for tests, benchmarks, and one-shot maintenance tools.
    #[must_use]
    pub fn unobserved(pool: PostgresPool) -> Self {
        Self::new(pool, Arc::new(crate::NoopPostgresTelemetry))
    }

    #[must_use]
    pub fn with_task_lease_pool(mut self, pool: PostgresPool) -> Self {
        self.runtime = self.runtime.with_task_lease_pool(pool);
        self
    }

    #[must_use]
    pub fn with_notification_listener_pool(mut self, pool: PostgresPool) -> Self {
        self.notification_listener_pool = pool;
        self
    }

    #[must_use]
    pub fn with_computed_reindex_batch_size(mut self, batch_size: NonZeroUsize) -> Self {
        self.runtime = self.runtime.with_computed_reindex_batch_size(batch_size);
        self
    }

    fn runtime(&self) -> &PostgresRuntime {
        &self.runtime
    }

    fn notification_listener_pool(&self) -> PostgresPool {
        self.notification_listener_pool.clone()
    }
}

impl StorageBackend for PostgresStorage {}
