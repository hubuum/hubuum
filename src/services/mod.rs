pub(crate) mod catalog;
mod class_relations;
mod classes;
mod collections;
pub(crate) mod computed_objects;
pub(crate) mod history;
mod object_relations;
mod objects;
pub(crate) mod related_filter_authorization;
pub(crate) mod relation_queries;
mod storage_boundary;
pub(crate) mod unified_search;

pub use class_relations::ClassRelationService;
pub use classes::ClassService;
pub use collections::CollectionService;
pub use object_relations::ObjectRelationService;
pub use objects::ObjectService;

use crate::storage::DynLifecycleStorage;

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
    pub(crate) fn from_lifecycle_storage(storage: DynLifecycleStorage) -> Self {
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
