pub mod authentication;
pub(crate) mod authorization;
pub(crate) mod authorization_resources;
pub(crate) mod backups;
pub(crate) mod catalog;
mod class_relations;
mod classes;
mod collections;
pub(crate) mod computed_fields;
pub(crate) mod computed_objects;
pub(crate) mod event_administration;
pub(crate) mod groups;
pub(crate) mod history;
pub mod identity;
pub(crate) mod import_boundary;
pub(crate) mod inventory;
pub(crate) mod object_aggregates;
mod object_relations;
mod objects;
pub(crate) mod operational_administration;
pub(crate) mod related_filter_authorization;
pub(crate) mod relation_queries;
pub(crate) mod remote_targets;
pub(crate) mod storage_boundary;
pub(crate) mod tasks;
pub(crate) mod unified_search;

pub use class_relations::ClassRelationService;
pub use classes::ClassService;
pub use collections::CollectionService;
pub use object_relations::ObjectRelationService;
pub use objects::ObjectService;

#[cfg(test)]
use std::sync::Arc;

use crate::storage::StorageHandle;
#[cfg(test)]
use crate::storage::{
    ClassRelationStore, ClassStore, CollectionStore, ObjectRelationStore, ObjectStore,
    ObservedStorage, StorageIdentity,
};

/// Application use-case facade.
#[derive(Clone)]
pub struct Services {
    classes: ClassService,
    class_relations: ClassRelationService,
    collections: CollectionService,
    objects: ObjectService,
    object_relations: ObjectRelationService,
}

impl Services {
    pub(crate) fn from_storage(storage: StorageHandle) -> Self {
        Self {
            classes: ClassService::new(storage.class_store()),
            class_relations: ClassRelationService::new(storage.class_relation_store()),
            collections: CollectionService::new(storage.collection_store()),
            objects: ObjectService::new(storage.object_store()),
            object_relations: ObjectRelationService::new(storage.object_relation_store()),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_resource_storage<S>(storage: S) -> Self
    where
        S: StorageIdentity
            + CollectionStore
            + ClassStore
            + ObjectStore
            + ClassRelationStore
            + ObjectRelationStore
            + 'static,
    {
        let storage = Arc::new(ObservedStorage::new(storage));
        Self {
            classes: ClassService::new(storage.clone()),
            class_relations: ClassRelationService::new(storage.clone()),
            collections: CollectionService::new(storage.clone()),
            objects: ObjectService::new(storage.clone()),
            object_relations: ObjectRelationService::new(storage),
        }
    }

    pub fn classes(&self) -> &ClassService {
        &self.classes
    }

    pub fn class_relations(&self) -> &ClassRelationService {
        &self.class_relations
    }

    pub fn collections(&self) -> &CollectionService {
        &self.collections
    }

    pub fn objects(&self) -> &ObjectService {
        &self.objects
    }

    pub fn object_relations(&self) -> &ObjectRelationService {
        &self.object_relations
    }
}
