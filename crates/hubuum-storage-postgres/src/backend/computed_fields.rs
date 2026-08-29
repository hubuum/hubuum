use crate::operations::computed_fields as postgres_computed_fields;
use async_trait::async_trait;
use hubuum_domain::{ClassId, ComputedFieldDefinitionId};

use hubuum_storage_core::{
    ComputedFieldStorage, StorageClassComputationState, StorageComputedFieldDefinition,
    StorageComputedFieldMutation, StorageComputedFieldRebuildRequest, StorageError,
    StorageMutationOutcome, StoragePage, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageTask,
    StorageTaskLease,
};

use super::PostgresStorage;

#[async_trait]
impl ComputedFieldStorage for PostgresStorage {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        postgres_computed_fields::get_computed_field_state(self.runtime(), class_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        postgres_computed_fields::list_shared_computed_fields(self.runtime(), class_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError> {
        postgres_computed_fields::list_personal_computed_fields(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        postgres_computed_fields::get_computed_field(self.runtime(), definition_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        postgres_computed_fields::create_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        postgres_computed_fields::update_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<StorageClassComputationState>, StorageError> {
        postgres_computed_fields::delete_shared_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        postgres_computed_fields::create_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        postgres_computed_fields::update_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        postgres_computed_fields::delete_personal_computed_field(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        postgres_computed_fields::request_computed_field_rebuild(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        postgres_computed_fields::execute_computed_field_rebuild(self.runtime(), lease)
            .await
            .map_err(StorageError::from)
    }
}
