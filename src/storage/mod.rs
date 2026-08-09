mod class_relations;
mod classes;
mod collections;
mod contract;
mod error;
#[cfg(test)]
mod memory;
mod object_relations;
mod objects;
mod observed;
mod postgres;

pub(crate) use class_relations::ClassRelationStore;
pub(crate) use classes::ClassStore;
pub(crate) use collections::CollectionStore;
#[cfg(test)]
pub(crate) use contract::STORAGE_CONTRACT_VERSION;
pub(crate) use contract::{
    DynLifecycleStorage, LifecycleStorage, StorageBackend, StorageBackendDescriptor,
    StorageBackendKind, StorageIdentity,
};
pub(crate) use error::{StorageError, StorageErrorKind};
#[cfg(test)]
pub(crate) use memory::MemoryStorageModel;
pub(crate) use object_relations::ObjectRelationStore;
pub(crate) use objects::ObjectStore;
pub(crate) use postgres::PostgresStorage;
