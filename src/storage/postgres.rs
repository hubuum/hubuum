use async_trait::async_trait;

use crate::db::DbPool;
use crate::db::traits::GetCollection;
use crate::db::traits::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    move_collection_record_from_backend,
};
use crate::events::EventContext;
use crate::models::{Collection, CollectionID, NewCollectionWithAssignee, UpdateCollection};

use super::{CollectionStore, StorageError};

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
