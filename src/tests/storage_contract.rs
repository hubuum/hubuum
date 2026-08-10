use std::sync::{Arc, LazyLock};

use actix_web::web::Data;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::events::{EventFanoutSettings, EventRetentionSettings};
use crate::models::CollectionID;
use crate::models::TokenRetentionSettings;
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    EventArchive, EventDeliveryStorage, EventFanoutStorage, EventHealthStorage,
    EventRetentionStorage, MetricsStorage, OperationalStateStorage, RetainedEvent,
    STORAGE_CONTRACT_VERSION, StorageBackendKind, StorageError, TokenRetentionStorage,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());

                let pool_state = backend.metrics_pool_state();
                assert!(pool_state.max_connections > 0);
                backend
                    .metrics_inventory_snapshot()
                    .await
                    .expect("certified backend should supply inventory metrics");
                backend
                    .metrics_task_snapshot()
                    .await
                    .expect("certified backend should supply task metrics");
                backend
                    .metrics_event_snapshot()
                    .await
                    .expect("certified backend should supply event metrics");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let state = backend
                    .maintenance_state()
                    .await
                    .expect("certified backend should expose maintenance state");
                let readiness = backend
                    .readiness_snapshot()
                    .await
                    .expect("certified backend should expose readiness state");

                assert_eq!(readiness.maintenance_state(), state);
                assert!(readiness.schema_is_ready());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .event_delivery_health()
                    .await
                    .expect("certified backend should expose event delivery health");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                backend
                    .process_event_fanout_batch(settings)
                    .await
                    .expect("certified backend should process event fan-out");
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_retention() {
    struct DiscardArchive;

    impl EventArchive for DiscardArchive {
        fn archive(&self, _events: &[RetainedEvent]) -> Result<(), StorageError> {
            Ok(())
        }
    }

    let _permit = postgres_permit().await;
    let settings = EventRetentionSettings::new(10_000, 10_000, 10)
        .expect("compatibility event-retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let summary = backend
                    .process_event_retention_batch(settings, &DiscardArchive)
                    .await
                    .expect("certified backend should process event retention");

                assert!(!summary.did_work());
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_token_retention() {
    let _permit = postgres_permit().await;
    let settings = TokenRetentionSettings::builder()
        .retention_days(1_000_000)
        .token_lifetime_hours(24)
        .batch_size(10)
        .build()
        .expect("compatibility retention settings should be valid");

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                let purged = backend
                    .purge_expired_tokens(settings)
                    .await
                    .expect("certified backend should execute token retention");

                assert_eq!(purged, 0);
            }
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_the_complete_contract() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
                fn accepts_event_delivery_contract(_backend: &impl EventDeliveryStorage) {}
                accepts_event_delivery_contract(&backend);
                let descriptor = backend.descriptor();
                assert_eq!(descriptor.kind(), kind);
                assert_eq!(descriptor.contract_version(), STORAGE_CONTRACT_VERSION);

                let services = Services::from_lifecycle_storage(backend.lifecycle_storage());
                let root = services
                    .collections()
                    .get(CollectionID::new(1).expect("valid root collection id"))
                    .await
                    .expect("certified backend should serve lifecycle operations");
                assert_eq!(root.id, 1);
            }
        }
    }
}

pub(crate) async fn postgres_permit() -> OwnedSemaphorePermit {
    static LIMITER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
    LIMITER
        .clone()
        .acquire_owned()
        .await
        .expect("storage contract semaphore should remain open")
}

pub(crate) fn pool() -> Data<PostgresPool> {
    let config = crate::tests::integration_test_config()
        .expect("integration test config should be initialized");
    Data::new(crate::storage::postgres::init_postgres_pool(
        &config.database_url,
        2,
    ))
}

pub(crate) fn prefix(label: &str) -> String {
    let suffix = crate::utilities::auth::generate_random_password(12).to_ascii_lowercase();
    format!("storage_contract_{label}_{suffix}")
}
