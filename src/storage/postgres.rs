use async_trait::async_trait;

use crate::db::DbPool;
use crate::db::traits::GetCollection;
use crate::db::traits::class::{
    CreateClassRecord, DeleteResolvedClassRecord, ResolveClassSelectorRecord,
    UpdateResolvedClassRecord,
};
use crate::db::traits::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    move_collection_record_from_backend,
};
use crate::events::EventContext;
use crate::models::{
    ClassSelector, Collection, CollectionID, HubuumClass, NewCollectionWithAssignee,
    NewHubuumClass, ResolvedClassTarget, UpdateCollection, UpdateHubuumClass,
};

use super::{ClassStore, CollectionStore, StorageError};

/// Canonical production storage adapter.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: DbPool,
}

impl PostgresStorage {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CollectionStore for PostgresStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        id.collection_from_backend(&self.pool)
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, Some(context))
            .await
            .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        changes
            .update_collection_record(&self.pool, id.id(), Some(context))
            .await
            .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        id.delete_collection_record(&self.pool, Some(context))
            .await
            .map_err(StorageError::from)
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        collection_children_from_backend(&self.pool, id)
            .await
            .map_err(StorageError::from)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        collection_ancestors_from_backend(&self.pool, id)
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(&self.pool, id.id(), new_parent_id.id(), Some(context))
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ClassStore for PostgresStorage {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError> {
        let class = selector
            .resolve_class_selector_record(&self.pool)
            .await
            .map_err(StorageError::from)?;
        Ok(ResolvedClassTarget::new(selector, class))
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        command.validate_schema().map_err(StorageError::from)?;
        command
            .create_class_record(&self.pool, Some(context))
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        changes
            .update_resolved_class_record(&self.pool, target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_record(&self.pool, context)
            .await
            .map_err(StorageError::from)
    }
}
