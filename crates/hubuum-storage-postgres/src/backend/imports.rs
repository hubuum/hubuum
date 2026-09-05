use crate::operations::import_workflow;
use async_trait::async_trait;
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use hubuum_storage_core::{FencedImportPlan, FencedImportResults};

use hubuum_storage_core::{
    ImportStorage, StorageClass, StorageCollection, StorageError, StorageImportApply,
    StorageImportCollectionKey, StorageImportMode, StorageImportPlan, StorageImportPreflight,
    StorageImportResult, StorageObject,
};

use super::PostgresStorage;

#[async_trait]
impl ImportStorage for PostgresStorage {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        import_workflow::root_collection(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_by_id(self.runtime(), collection_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_by_key(self.runtime(), key)
            .await
            .map_err(StorageError::from)
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        import_workflow::collections_by_name(self.runtime(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_child_by_name(self.runtime(), parent_collection_id.id(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClass>, StorageError> {
        import_workflow::class_by_name(self.runtime(), collection_id.id(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClass>, StorageError> {
        import_workflow::classes_by_names(self.runtime(), collection_id.id(), names)
            .await
            .map_err(StorageError::from)
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        import_workflow::object_by_name(self.runtime(), class_id.id(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        import_workflow::objects_by_names(self.runtime(), class_id.id(), names)
            .await
            .map_err(StorageError::from)
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        import_workflow::class_relation_exists(
            self.runtime(),
            left_class_id.id(),
            right_class_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        import_workflow::object_relation_exists(
            self.runtime(),
            left_object_id.id(),
            right_object_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        import_workflow::group_exists(self.runtime(), identity_scope, group_name)
            .await
            .map_err(StorageError::from)
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        import_workflow::preflight_import(self.runtime(), plan, mode)
            .await
            .map_err(StorageError::from)
    }

    async fn apply_claimed_import_strict(
        &self,
        plan: FencedImportPlan,
    ) -> Result<(), StorageError> {
        crate::operations::import_execution::apply_claimed_import_strict(self.runtime(), plan)
            .await
            .map_err(StorageError::from)
    }
    async fn apply_claimed_import_best_effort(
        &self,
        plan: FencedImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        crate::operations::import_execution::apply_claimed_import_best_effort(
            self.runtime(),
            plan,
            mode,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        import_workflow::apply_import_strict(self.runtime(), plan)
            .await
            .map_err(StorageError::from)
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        import_workflow::apply_import_best_effort(self.runtime(), plan, mode)
            .await
            .map_err(StorageError::from)
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        import_workflow::record_results(self.runtime(), results)
            .await
            .map_err(StorageError::from)
    }

    async fn record_claimed_import_results(
        &self,
        results: FencedImportResults,
    ) -> Result<(), StorageError> {
        crate::operations::import_execution::record_claimed_import_results(self.runtime(), results)
            .await
            .map_err(StorageError::from)
    }
}
