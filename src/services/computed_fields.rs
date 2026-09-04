use hubuum_computed_fields::{EvaluationLimits, evaluate};
use hubuum_domain::{ClassId, CollectionId, ComputedFieldDefinitionId, PrincipalId};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::QueryOptions;
use crate::models::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ClassComputationState,
    ComputedFieldDefinition, ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest,
    ComputedFieldErrorResponse, ComputedFieldMutationResponse, ComputedFieldPreviewResponse,
    ComputedResultType,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    ComputedFieldStorage, StorageClassComputationState, StorageComputedFieldDefinition,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldMutation, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StorageContext, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, storage_handle,
};

#[doc(hidden)]
pub async fn class_computation_state_for(
    backend: &impl StorageContext,
    class_id: ClassId,
) -> Result<ClassComputationState, ApiError> {
    let state = storage_handle(backend)
        .get_computed_field_state(class_id)
        .await?;
    Ok(state_from_storage(state))
}

pub(crate) async fn list_shared_definitions(
    backend: &impl StorageContext,
    class_id: ClassId,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    storage_handle(backend)
        .list_shared_computed_fields(class_id)
        .await?
        .into_iter()
        .map(definition_from_storage)
        .collect()
}

pub(crate) async fn list_personal_definitions_page(
    backend: &impl StorageContext,
    owner_id: PrincipalId,
    class_id: Option<ClassId>,
    options: QueryOptions,
) -> Result<(Vec<ComputedFieldDefinition>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_personal_computed_fields(StoragePersonalComputedFieldListQuery::new(
            owner_id, class_id, options,
        ))
        .await?;
    let (definitions, total) = page.into_parts();
    Ok((
        definitions
            .into_iter()
            .map(definition_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_computed_definition(
    backend: &impl StorageContext,
    definition_id: ComputedFieldDefinitionId,
) -> Result<ComputedFieldDefinition, ApiError> {
    let definition = storage_handle(backend)
        .get_computed_field(definition_id)
        .await?;
    definition_from_storage(definition)
}

#[doc(hidden)]
pub async fn create_shared_definition(
    backend: &impl StorageContext,
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    actor_id: PrincipalId,
    definition: ComputedFieldDefinitionRequest,
    event_context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let request = StorageSharedComputedFieldCreate::new(
        class_id,
        authorized_collection_id,
        actor_id,
        input_to_storage(definition)?,
        event_context.clone(),
    );
    let mutation = storage_handle(backend)
        .create_shared_computed_field(request)
        .await?;
    mutation_from_storage(mutation.into_value())
}

#[doc(hidden)]
pub async fn update_shared_definition(
    backend: &impl StorageContext,
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    definition_id: ComputedFieldDefinitionId,
    actor_id: PrincipalId,
    patch: ComputedFieldDefinitionPatch,
    event_context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let request = StorageSharedComputedFieldUpdate::new(
        class_id,
        authorized_collection_id,
        definition_id,
        actor_id,
        patch_to_storage(patch),
        event_context.clone(),
    );
    let mutation = storage_handle(backend)
        .update_shared_computed_field(request)
        .await?;
    mutation_from_storage(mutation.into_value())
}

pub(crate) async fn delete_shared_definition(
    backend: &impl StorageContext,
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    definition_id: ComputedFieldDefinitionId,
    actor_id: PrincipalId,
    event_context: &EventContext,
) -> Result<ClassComputationState, ApiError> {
    let request = StorageSharedComputedFieldDelete::new(
        class_id,
        authorized_collection_id,
        definition_id,
        actor_id,
        event_context.clone(),
    );
    let state = storage_handle(backend)
        .delete_shared_computed_field(request)
        .await?;
    Ok(state_from_storage(state.into_value()))
}

#[doc(hidden)]
pub async fn create_personal_definition(
    backend: &impl StorageContext,
    class_id: ClassId,
    owner_id: PrincipalId,
    definition: ComputedFieldDefinitionRequest,
    event_context: &EventContext,
) -> Result<ComputedFieldDefinition, ApiError> {
    let request = StoragePersonalComputedFieldCreate::new(
        class_id,
        owner_id,
        input_to_storage(definition)?,
        event_context.clone(),
    );
    let definition = storage_handle(backend)
        .create_personal_computed_field(request)
        .await?;
    definition_from_storage(definition.into_value())
}

pub(crate) async fn update_personal_definition(
    backend: &impl StorageContext,
    owner_id: PrincipalId,
    definition_id: ComputedFieldDefinitionId,
    patch: ComputedFieldDefinitionPatch,
    event_context: &EventContext,
) -> Result<ComputedFieldDefinition, ApiError> {
    let request = StoragePersonalComputedFieldUpdate::new(
        owner_id,
        definition_id,
        patch_to_storage(patch),
        event_context.clone(),
    );
    let definition = storage_handle(backend)
        .update_personal_computed_field(request)
        .await?;
    definition_from_storage(definition.into_value())
}

pub(crate) async fn delete_personal_definition(
    backend: &impl StorageContext,
    owner_id: PrincipalId,
    definition_id: ComputedFieldDefinitionId,
    event_context: &EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_personal_computed_field(StoragePersonalComputedFieldDelete::new(
            owner_id,
            definition_id,
            event_context.clone(),
        ))
        .await?
        .into_value();
    Ok(())
}

#[doc(hidden)]
pub async fn request_class_rebuild(
    backend: &impl StorageContext,
    class_id: ClassId,
    authorized_collection_id: CollectionId,
    actor_id: Option<PrincipalId>,
) -> Result<ClassComputationState, ApiError> {
    let state = storage_handle(backend)
        .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
            class_id,
            authorized_collection_id,
            actor_id,
        ))
        .await?;
    Ok(state_from_storage(state))
}

pub(crate) fn preview_computed_definition(
    data: &serde_json::Value,
    request: &ComputedFieldDefinitionRequest,
) -> Result<ComputedFieldPreviewResponse, ApiError> {
    let definition = request.validate()?;
    let result =
        evaluate(data, &[definition], 1, EvaluationLimits::standard()).map_err(|error| {
            ApiError::InternalServerError(format!("Computed-field preview failed: {error}"))
        })?;
    crate::observability::metrics::computed_evaluation("preview", &result);
    Ok(ComputedFieldPreviewResponse {
        value: result
            .values
            .get(&request.key)
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        error: result
            .errors
            .get(&request.key)
            .cloned()
            .map(ComputedFieldErrorResponse::from),
    })
}

fn input_to_storage(
    input: ComputedFieldDefinitionRequest,
) -> Result<StorageComputedFieldDefinitionInput, ApiError> {
    Ok(StorageComputedFieldDefinitionInput::new(input.validate()?))
}

fn patch_to_storage(patch: ComputedFieldDefinitionPatch) -> StorageComputedFieldDefinitionPatch {
    StorageComputedFieldDefinitionPatch::new()
        .with_key(patch.key)
        .with_label(patch.label)
        .with_description(patch.description)
        .with_operation(patch.operation)
        .with_result_type(
            patch
                .result_type
                .map(|result_type| result_type.as_str().to_string()),
        )
        .with_enabled(patch.enabled)
}

fn definition_from_storage(
    definition: StorageComputedFieldDefinition,
) -> Result<ComputedFieldDefinition, ApiError> {
    let metadata = definition.metadata();
    let (visibility, owner_user_id) = match definition.visibility() {
        StorageComputedFieldVisibility::Shared => {
            (COMPUTED_FIELD_VISIBILITY_SHARED.to_string(), None)
        }
        StorageComputedFieldVisibility::Personal { owner_id } => (
            COMPUTED_FIELD_VISIBILITY_PERSONAL.to_string(),
            Some(owner_id.id()),
        ),
    };
    Ok(ComputedFieldDefinition {
        id: metadata.id().id(),
        class_id: definition.class_id().id(),
        visibility,
        owner_user_id,
        key: definition.key().to_string(),
        label: definition.label().to_string(),
        description: definition.description().to_string(),
        operation: serde_json::to_value(definition.operation()).map_err(|error| {
            ApiError::InternalServerError(format!(
                "Failed to serialize validated computed-field operation: {error}"
            ))
        })?,
        result_type: ComputedResultType::from(definition.result_type())
            .as_str()
            .to_string(),
        enabled: definition.enabled(),
        revision: metadata.revision(),
        semantics_version: definition.semantics_version(),
        created_by: definition.created_by().map(|id| id.id()),
        updated_by: definition.updated_by().map(|id| id.id()),
        created_at: metadata.created_at().naive_utc(),
        updated_at: metadata.updated_at().naive_utc(),
    })
}

fn state_from_storage(state: StorageClassComputationState) -> ClassComputationState {
    ClassComputationState {
        class_id: state.class_id().id(),
        evaluation_revision: state.evaluation_revision().get(),
        rebuild_status: state.rebuild_status().as_str().to_string(),
        active_task_id: state.active_task_id().map(|id| id.id()),
        last_error: state.last_error_message().map(str::to_string),
        created_at: state.created_at().naive_utc(),
        updated_at: state.updated_at().naive_utc(),
    }
}

fn mutation_from_storage(
    mutation: StorageComputedFieldMutation,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let (definition, state) = mutation.into_parts();
    Ok(ComputedFieldMutationResponse {
        definition: definition_from_storage(definition)?,
        state: state_from_storage(state),
    })
}
