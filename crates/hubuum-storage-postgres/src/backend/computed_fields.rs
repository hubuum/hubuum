use crate::operations::computed_lifecycle as postgres_computed_lifecycle;
use async_trait::async_trait;
use hubuum_domain::{ClassId, ComputedFieldDefinitionId};

use hubuum_storage_core::{
    ComputedFieldLifecycleStorage, MutationOutcome, StorageClassComputationState,
    StorageComputedFieldDefinition, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldRebuildRequest, StorageError, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageTask,
    StorageTaskLease,
};

use super::PostgresStorage;

#[async_trait]
impl ComputedFieldLifecycleStorage for PostgresStorage {
    async fn computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        postgres_computed_lifecycle::computed_field_state(self.runtime(), class_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        postgres_computed_lifecycle::list_shared_computed_fields(self.runtime(), class_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StorageComputedFieldPage, StorageError> {
        postgres_computed_lifecycle::list_personal_computed_fields(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        postgres_computed_lifecycle::get_computed_field(self.runtime(), definition_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<MutationOutcome<StorageComputedFieldMutation>, StorageError> {
        postgres_computed_lifecycle::create_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<MutationOutcome<StorageComputedFieldMutation>, StorageError> {
        postgres_computed_lifecycle::update_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<MutationOutcome<StorageClassComputationState>, StorageError> {
        postgres_computed_lifecycle::delete_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        postgres_computed_lifecycle::create_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        postgres_computed_lifecycle::update_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<(), StorageError> {
        postgres_computed_lifecycle::delete_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        postgres_computed_lifecycle::request_computed_field_rebuild(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        postgres_computed_lifecycle::execute_computed_field_rebuild(self.runtime(), lease)
            .await
            .map_err(StorageError::from)
    }
}
