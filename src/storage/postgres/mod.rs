mod error;
#[doc(hidden)]
pub mod operations;
mod runtime;

pub use runtime::*;

use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    ClassSelector, Collection, CollectionID, HubuumClass, HubuumClassRelationID, HubuumObject,
    NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationSelector, ObjectSelector,
    PreparedClassRelation, PreparedObjectRelation, ResolvedClassRelationTarget,
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget, UpdateCollection,
    UpdateHubuumClass, UpdateHubuumObject,
};
use crate::storage::postgres::operations::GetCollection;
use crate::storage::postgres::operations::class::{
    CreateClassRecord, DeleteResolvedClassRecord, ResolveClassSelectorRecord,
    UpdateResolvedClassRecord,
};
use crate::storage::postgres::operations::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    move_collection_record_from_backend,
};
use crate::storage::postgres::operations::object::{
    CreateObjectInResolvedClassRecord, DeleteResolvedObjectRecord, PatchObjectDataRecord,
    ResolveObjectSelectorRecord, UpdateResolvedObjectRecord,
};
use crate::storage::postgres::operations::relations::{
    CreatePreparedClassRelationRecord, CreatePreparedObjectRelationRecord,
    DeleteResolvedClassRelationRecord, DeleteResolvedObjectRelationRecord,
    PrepareClassRelationRecord, PrepareObjectRelationRecord, ResolveClassRelationTargetRecord,
    ResolveObjectRelationTargetRecord,
};

use super::{
    ClassRelationStore, ClassStore, CollectionStore, ObjectRelationStore, ObjectStore,
    StorageError, StorageIdentity,
};
use error::map_postgres_error;

/// Canonical production storage adapter.
#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PostgresPool,
}

impl PostgresStorage {
    pub(crate) fn new(pool: PostgresPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &PostgresPool {
        &self.pool
    }
}

impl StorageIdentity for PostgresStorage {
    fn storage_name(&self) -> &'static str {
        "postgresql"
    }
}

#[async_trait]
impl CollectionStore for PostgresStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        id.collection_from_backend(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
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
            .map_err(map_postgres_error)
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        id.delete_collection_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        collection_children_from_backend(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        collection_ancestors_from_backend(&self.pool, id)
            .await
            .map_err(map_postgres_error)
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(&self.pool, id.id(), new_parent_id.id(), Some(context))
            .await
            .map_err(map_postgres_error)
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
            .map_err(map_postgres_error)?;
        Ok(ResolvedClassTarget::new(selector, class))
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        command.validate_schema().map_err(map_postgres_error)?;
        command
            .create_class_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
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
            .map_err(map_postgres_error)
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassRelationStore for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, StorageError> {
        command
            .prepare_class_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        id.resolve_class_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_class_relation(
        &self,
        prepared: &PreparedClassRelation,
        context: &EventContext,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedClassRelationTarget::new(
            relation,
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectRelationStore for PostgresStorage {
    async fn prepare_object_relation(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, StorageError> {
        selector
            .prepare_object_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_object_relation(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        selector
            .resolve_object_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
        context: &EventContext,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedObjectRelationTarget::new(
            relation,
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectStore for PostgresStorage {
    async fn resolve_object(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, StorageError> {
        let (class, object) = selector
            .resolve_object_selector_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        Ok(ResolvedObjectTarget::new(selector, class, object))
    }

    async fn create_object(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        command
            .create_object_in_resolved_class_record(&self.pool, class, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_object(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        changes
            .update_resolved_object_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn patch_object_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        patch
            .patch_object_data_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}
