use super::super::*;

#[async_trait]
impl CollectionStorage for PostgresStorage {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        crate::operations::collection::get_collection(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::create_collection(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::update_collection(self.runtime(), id.id(), changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::collection::delete_collection(self.runtime(), id.id(), context)
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::list_collection_children(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::list_collection_ancestors(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::move_collection(
            self.runtime(),
            id.id(),
            new_parent_id.id(),
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ClassStorage for PostgresStorage {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        crate::operations::class::resolve_class(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        crate::operations::class::create_class(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        crate::operations::class::update_class(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::class::delete_class(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let class_ids = class_ids.into_iter().map(ClassId::id).collect();
        crate::operations::class::resolve_class_names(self.runtime(), class_ids)
            .await
            .map_err(StorageError::from)
            .and_then(|rows| {
                rows.into_iter()
                    .map(|(id, name)| {
                        ClassId::new(id)
                            .map(|id| (id, name))
                            .map_err(crate::PostgresStorageError::from)
                            .map_err(StorageError::from)
                    })
                    .collect()
            })
    }
}

#[async_trait]
impl ClassRelationStorage for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        crate::operations::relation::prepare_class_relation(self.runtime(), command)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        crate::operations::relation::resolve_class_relation(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError> {
        crate::operations::relation::create_class_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::relation::delete_class_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectRelationStorage for PostgresStorage {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        crate::operations::relation::prepare_object_relation(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        crate::operations::relation::resolve_object_relation(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        crate::operations::relation::create_object_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::relation::delete_object_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectStorage for PostgresStorage {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        crate::operations::object::get_object(self.runtime(), object_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        crate::operations::object::resolve_object(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::create_object(self.runtime(), class, command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::update_object(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::patch_object_data(self.runtime(), target, patch, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        crate::operations::object::delete_object(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        crate::operations::object::validate_object(self.runtime(), object)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        crate::operations::object::validate_object_create_command(self.runtime(), command)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        crate::operations::object::validate_object_update_command(
            self.runtime(),
            object_id.id(),
            changes,
        )
        .await
        .map_err(StorageError::from)
    }
}
