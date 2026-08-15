use async_trait::async_trait;
use hubuum_storage_postgres::operations::import_workflow;

use crate::storage::{
    ImportStorage, StorageClassRecord, StorageCollection, StorageError, StorageImportApply,
    StorageImportCollectionKey, StorageImportMode, StorageImportPlanItem, StorageImportPreflight,
    StorageImportResult, StorageObject,
};

use super::PostgresStorage;

#[async_trait]
impl ImportStorage for PostgresStorage {
    async fn import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        import_workflow::root_collection(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_by_id(self.runtime(), collection_id)
            .await
            .map_err(StorageError::from)
    }

    async fn import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_by_key(self.runtime(), key)
            .await
            .map_err(StorageError::from)
    }

    async fn import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        import_workflow::collections_by_name(self.runtime(), name)
            .await
            .map_err(StorageError::from)
    }

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        import_workflow::collection_child_by_name(self.runtime(), parent_collection_id, name)
            .await
            .map_err(StorageError::from)
    }

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError> {
        import_workflow::class_by_name(self.runtime(), collection_id, name)
            .await
            .map_err(StorageError::from)
    }

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError> {
        import_workflow::classes_by_names(self.runtime(), collection_id, names)
            .await
            .map_err(StorageError::from)
    }

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        import_workflow::object_by_name(self.runtime(), class_id, name)
            .await
            .map_err(StorageError::from)
    }

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        import_workflow::objects_by_names(self.runtime(), class_id, names)
            .await
            .map_err(StorageError::from)
    }

    async fn import_class_relation_exists(
        &self,
        left_class_id: i32,
        right_class_id: i32,
    ) -> Result<bool, StorageError> {
        import_workflow::class_relation_exists(self.runtime(), left_class_id, right_class_id)
            .await
            .map_err(StorageError::from)
    }

    async fn import_object_relation_exists(
        &self,
        left_object_id: i32,
        right_object_id: i32,
    ) -> Result<bool, StorageError> {
        import_workflow::object_relation_exists(self.runtime(), left_object_id, right_object_id)
            .await
            .map_err(StorageError::from)
    }

    async fn import_group_exists(
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
        items: Vec<StorageImportPlanItem>,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        import_workflow::preflight_import(self.runtime(), items, mode)
            .await
            .map_err(StorageError::from)
    }

    async fn apply_import_strict(
        &self,
        items: Vec<StorageImportPlanItem>,
    ) -> Result<(), StorageError> {
        import_workflow::apply_import_strict(self.runtime(), items)
            .await
            .map_err(StorageError::from)
    }

    async fn apply_import_best_effort(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        import_workflow::apply_import_best_effort(self.runtime(), items, mode)
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
}
