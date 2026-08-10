use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ClassComputationState,
    ComputedFieldDefinition, ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest,
    ComputedFieldMutationResponse, ComputedResultType,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    ComputedFieldLifecycleStorage, StorageClassComputationState, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionContent, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldProvenance, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StorageError, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageRecordMetadata, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate,
};

use super::error::map_postgres_error;
use super::{PostgresStorage, operations};

#[async_trait]
impl ComputedFieldLifecycleStorage for PostgresStorage {
    async fn computed_field_state(
        &self,
        class_id: i32,
    ) -> Result<StorageClassComputationState, StorageError> {
        operations::computed_field::class_computation_state_for(self.pool(), class_id)
            .await
            .map(state_to_storage)
            .map_err(map_postgres_error)
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: i32,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        operations::computed_field::list_shared_definitions(self.pool(), class_id)
            .await
            .and_then(definitions_to_storage)
            .map_err(map_postgres_error)
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StorageComputedFieldPage, StorageError> {
        let (owner_id, class_id, options) = query.into_parts();
        operations::computed_field::list_personal_definitions_page(
            self.pool(),
            owner_id,
            class_id,
            &options,
        )
        .await
        .and_then(|(definitions, total)| {
            Ok(StorageComputedFieldPage::new(
                definitions_to_storage(definitions)?,
                (total != SKIPPED_TOTAL_COUNT).then_some(total),
            ))
        })
        .map_err(map_postgres_error)
    }

    async fn get_computed_field(
        &self,
        definition_id: i32,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        operations::computed_field::get_computed_definition(self.pool(), definition_id)
            .await
            .and_then(definition_to_storage)
            .map_err(map_postgres_error)
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageComputedFieldMutation, StorageError> {
        let (class_id, collection_id, actor_id, definition, context) = request.into_parts();
        operations::computed_field::create_shared_definition(
            self.pool(),
            class_id,
            collection_id,
            actor_id,
            input_from_storage(definition).map_err(map_postgres_error)?,
            &context,
        )
        .await
        .and_then(mutation_to_storage)
        .map_err(map_postgres_error)
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageComputedFieldMutation, StorageError> {
        let (class_id, collection_id, definition_id, actor_id, patch, context) =
            request.into_parts();
        operations::computed_field::update_shared_definition(
            self.pool(),
            class_id,
            collection_id,
            definition_id,
            actor_id,
            patch_from_storage(patch).map_err(map_postgres_error)?,
            &context,
        )
        .await
        .and_then(mutation_to_storage)
        .map_err(map_postgres_error)
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageClassComputationState, StorageError> {
        let (class_id, collection_id, definition_id, actor_id, context) = request.into_parts();
        operations::computed_field::delete_shared_definition(
            self.pool(),
            class_id,
            collection_id,
            definition_id,
            actor_id,
            &context,
        )
        .await
        .map(state_to_storage)
        .map_err(map_postgres_error)
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        let (class_id, owner_id, definition) = request.into_parts();
        operations::computed_field::create_personal_definition(
            self.pool(),
            class_id,
            owner_id,
            input_from_storage(definition).map_err(map_postgres_error)?,
        )
        .await
        .and_then(definition_to_storage)
        .map_err(map_postgres_error)
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        let (owner_id, definition_id, patch) = request.into_parts();
        operations::computed_field::update_personal_definition(
            self.pool(),
            owner_id,
            definition_id,
            patch_from_storage(patch).map_err(map_postgres_error)?,
        )
        .await
        .and_then(definition_to_storage)
        .map_err(map_postgres_error)
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<(), StorageError> {
        let (owner_id, definition_id) = request.into_parts();
        operations::computed_field::delete_personal_definition(self.pool(), owner_id, definition_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        let (class_id, collection_id, actor_id) = request.into_parts();
        operations::computed_field::request_class_rebuild(
            self.pool(),
            class_id,
            collection_id,
            actor_id,
        )
        .await
        .map(state_to_storage)
        .map_err(map_postgres_error)
    }
}

fn input_from_storage(
    input: StorageComputedFieldDefinitionInput,
) -> Result<ComputedFieldDefinitionRequest, ApiError> {
    let request = ComputedFieldDefinitionRequest {
        key: input.key().to_string(),
        label: input.label().to_string(),
        description: input.description().to_string(),
        operation: input.operation().clone(),
        result_type: ComputedResultType::from_db(input.result_type())?,
        enabled: input.enabled(),
    };
    request.validate()?;
    Ok(request)
}

fn patch_from_storage(
    patch: StorageComputedFieldDefinitionPatch,
) -> Result<ComputedFieldDefinitionPatch, ApiError> {
    Ok(ComputedFieldDefinitionPatch {
        key: patch.key().map(str::to_string),
        label: patch.label().map(str::to_string),
        description: patch.description().map(str::to_string),
        operation: patch.operation().cloned(),
        result_type: patch
            .result_type()
            .map(ComputedResultType::from_db)
            .transpose()?,
        enabled: patch.enabled(),
    })
}

fn definitions_to_storage(
    definitions: Vec<ComputedFieldDefinition>,
) -> Result<Vec<StorageComputedFieldDefinition>, ApiError> {
    definitions.into_iter().map(definition_to_storage).collect()
}

fn definition_to_storage(
    definition: ComputedFieldDefinition,
) -> Result<StorageComputedFieldDefinition, ApiError> {
    let visibility = match (definition.visibility.as_str(), definition.owner_user_id) {
        (COMPUTED_FIELD_VISIBILITY_SHARED, None) => StorageComputedFieldVisibility::Shared,
        (COMPUTED_FIELD_VISIBILITY_PERSONAL, Some(owner_id)) => {
            StorageComputedFieldVisibility::Personal { owner_id }
        }
        (visibility, owner_id) => {
            return Err(ApiError::InternalServerError(format!(
                "Computed-field definition {} has invalid visibility '{visibility}' and owner {owner_id:?}",
                definition.id
            )));
        }
    };
    Ok(StorageComputedFieldDefinition::new(
        StorageRecordMetadata::new(
            definition.id,
            definition.created_at,
            definition.updated_at,
            definition.revision.get(),
        ),
        definition.class_id,
        visibility,
        StorageComputedFieldDefinitionContent::new(
            StorageComputedFieldDefinitionInput::new(
                definition.key,
                definition.label,
                definition.operation,
                definition.result_type,
            )
            .with_description(definition.description)
            .with_enabled(definition.enabled),
            definition.semantics_version,
        ),
        StorageComputedFieldProvenance::new(definition.created_by, definition.updated_by),
    ))
}

fn state_to_storage(state: ClassComputationState) -> StorageClassComputationState {
    StorageClassComputationState::new(
        state.class_id,
        state.evaluation_revision,
        state.rebuild_status,
        state.created_at,
        state.updated_at,
    )
    .active_task(state.active_task_id)
    .last_error(state.last_error)
}

fn mutation_to_storage(
    mutation: ComputedFieldMutationResponse,
) -> Result<StorageComputedFieldMutation, ApiError> {
    Ok(StorageComputedFieldMutation::new(
        definition_to_storage(mutation.definition)?,
        state_to_storage(mutation.state),
    ))
}
