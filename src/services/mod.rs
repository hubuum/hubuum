mod collections;

pub use collections::CollectionService;

use crate::db::DbPool;
use crate::storage::{DynStorage, PostgresStorage};

/// Application use-case facade.
#[derive(Clone)]
pub struct Services {
    collections: CollectionService,
}

impl Services {
    pub fn postgres(pool: DbPool) -> Self {
        Self::from_storage(DynStorage::new(PostgresStorage::new(pool)))
    }

    pub(crate) fn from_storage(storage: DynStorage) -> Self {
        Self {
            collections: CollectionService::new(storage),
        }
    }

    pub fn collections(&self) -> &CollectionService {
        &self.collections
    }
}
