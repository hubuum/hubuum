use super::*;

#[async_trait]
impl ComputedFieldStorage for StorageHandle {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "get_computed_field_state",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_computed_field_state(class_id).await
                })
            },
        )
        .await
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "list_shared_computed_fields",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_shared_computed_fields(class_id).await
                })
            },
        )
        .await
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "list_personal_computed_fields",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_personal_computed_fields(query).await
                })
            },
        )
        .await
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "get_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_computed_field(definition_id).await
                })
            },
        )
        .await
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "create_shared_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_shared_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "update_shared_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_shared_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<StorageClassComputationState>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "delete_shared_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_shared_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "create_personal_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_personal_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "update_personal_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_personal_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "delete_personal_computed_field",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_personal_computed_field(request).await
                })
            },
        )
        .await
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "request_computed_field_rebuild",
            async {
                dispatch_backend!(self, |backend| {
                    backend.request_computed_field_rebuild(request).await
                })
            },
        )
        .await
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ComputedField,
            "execute_computed_field_rebuild",
            async {
                dispatch_backend!(self, |backend| {
                    backend.execute_computed_field_rebuild(lease).await
                })
            },
        )
        .await
    }
}
