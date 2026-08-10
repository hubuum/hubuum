use std::sync::{Arc, LazyLock};

use actix_web::web::Data;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::models::CollectionID;
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::postgres::PostgresPool;
use crate::storage::{STORAGE_CONTRACT_VERSION, StorageBackendKind};

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_the_complete_contract() {
    let _permit = postgres_permit().await;

    for kind in StorageBackendKind::ALL {
        match kind {
            StorageBackendKind::Postgresql => {
                let backend = StorageHandle::postgres(pool().get_ref().clone());
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
