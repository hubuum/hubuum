mod classes;
mod collections;
mod objects;

pub use classes::ClassService;
pub use collections::CollectionService;
pub use objects::ObjectService;

use crate::db::DbPool;
use crate::storage::{DynStorage, PostgresStorage};

#[cfg(test)]
pub(crate) async fn storage_contract_postgres_permit() -> tokio::sync::OwnedSemaphorePermit {
    use std::sync::{Arc, LazyLock};
    use tokio::sync::Semaphore;

    static LIMITER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
    LIMITER
        .clone()
        .acquire_owned()
        .await
        .expect("storage contract semaphore should remain open")
}

#[cfg(test)]
pub(crate) fn storage_contract_pool() -> actix_web::web::Data<DbPool> {
    let config = crate::tests::integration_test_config()
        .expect("integration test config should be initialized");
    actix_web::web::Data::new(crate::db::init_pool(&config.database_url, 2))
}

#[cfg(test)]
pub(crate) fn storage_contract_prefix(label: &str) -> String {
    let suffix = crate::utilities::auth::generate_random_password(12).to_ascii_lowercase();
    format!("storage_contract_{label}_{suffix}")
}

/// Application use-case facade.
#[derive(Clone)]
pub struct Services {
    classes: ClassService,
    collections: CollectionService,
    objects: ObjectService,
}

impl Services {
    pub fn postgres(pool: DbPool) -> Self {
        Self::from_storage(DynStorage::new(PostgresStorage::new(pool)))
    }

    pub(crate) fn from_storage(storage: DynStorage) -> Self {
        Self {
            classes: ClassService::new(storage.clone()),
            collections: CollectionService::new(storage.clone()),
            objects: ObjectService::new(storage),
        }
    }

    pub fn classes(&self) -> &ClassService {
        &self.classes
    }

    pub fn collections(&self) -> &CollectionService {
        &self.collections
    }

    pub fn objects(&self) -> &ObjectService {
        &self.objects
    }
}
