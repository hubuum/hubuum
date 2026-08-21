use hubuum_computed_fields::{EvaluationLimits, evaluate};

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::QueryOptions;
use crate::models::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ClassComputationState,
    ComputedFieldDefinition, ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest,
    ComputedFieldErrorResponse, ComputedFieldMutationResponse, ComputedFieldPreviewResponse,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::services::storage_boundary::{
    class_id_to_storage, collection_id_to_storage, principal_id_to_storage,
};
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
    class_id: i32,
) -> Result<ClassComputationState, ApiError> {
    let state = storage_handle(backend)
        .get_computed_field_state(class_id_to_storage(class_id))
        .await?;
    Ok(state_from_storage(state))
}

pub(crate) async fn list_shared_definitions(
    backend: &impl StorageContext,
    class_id: i32,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    storage_handle(backend)
        .list_shared_computed_fields(class_id_to_storage(class_id))
        .await?
        .into_iter()
        .map(definition_from_storage)
        .collect()
}

pub(crate) async fn list_personal_definitions_page(
    backend: &impl StorageContext,
    owner_id: i32,
    class_id: Option<i32>,
    options: QueryOptions,
) -> Result<(Vec<ComputedFieldDefinition>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_personal_computed_fields(StoragePersonalComputedFieldListQuery::new(
            principal_id_to_storage(owner_id),
            class_id.map(class_id_to_storage),
            options,
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
    definition_id: i32,
) -> Result<ComputedFieldDefinition, ApiError> {
    let definition = storage_handle(backend)
        .get_computed_field(
            hubuum_domain::ComputedFieldDefinitionId::new(definition_id)
                .expect("validated computed field definition id must be positive"),
        )
        .await?;
    definition_from_storage(definition)
}

#[doc(hidden)]
pub async fn create_shared_definition(
    backend: &impl StorageContext,
    class_id: i32,
    authorized_collection_id: i32,
    actor_id: i32,
    definition: ComputedFieldDefinitionRequest,
    event_context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let request = StorageSharedComputedFieldCreate::new(
        class_id_to_storage(class_id),
        collection_id_to_storage(authorized_collection_id),
        principal_id_to_storage(actor_id),
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
    class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    patch: ComputedFieldDefinitionPatch,
    event_context: &EventContext,
) -> Result<ComputedFieldMutationResponse, ApiError> {
    let request = StorageSharedComputedFieldUpdate::new(
        class_id_to_storage(class_id),
        collection_id_to_storage(authorized_collection_id),
        hubuum_domain::ComputedFieldDefinitionId::new(definition_id)
            .expect("validated computed field definition id must be positive"),
        principal_id_to_storage(actor_id),
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
    class_id: i32,
    authorized_collection_id: i32,
    definition_id: i32,
    actor_id: i32,
    event_context: &EventContext,
) -> Result<ClassComputationState, ApiError> {
    let request = StorageSharedComputedFieldDelete::new(
        class_id_to_storage(class_id),
        collection_id_to_storage(authorized_collection_id),
        hubuum_domain::ComputedFieldDefinitionId::new(definition_id)
            .expect("validated computed field definition id must be positive"),
        principal_id_to_storage(actor_id),
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
    class_id: i32,
    owner_id: i32,
    definition: ComputedFieldDefinitionRequest,
) -> Result<ComputedFieldDefinition, ApiError> {
    let request = StoragePersonalComputedFieldCreate::new(
        class_id_to_storage(class_id),
        principal_id_to_storage(owner_id),
        input_to_storage(definition)?,
    );
    let definition = storage_handle(backend)
        .create_personal_computed_field(request)
        .await?;
    definition_from_storage(definition)
}

pub(crate) async fn update_personal_definition(
    backend: &impl StorageContext,
    owner_id: i32,
    definition_id: i32,
    patch: ComputedFieldDefinitionPatch,
) -> Result<ComputedFieldDefinition, ApiError> {
    let request = StoragePersonalComputedFieldUpdate::new(
        principal_id_to_storage(owner_id),
        hubuum_domain::ComputedFieldDefinitionId::new(definition_id)
            .expect("validated computed field definition id must be positive"),
        patch_to_storage(patch),
    );
    let definition = storage_handle(backend)
        .update_personal_computed_field(request)
        .await?;
    definition_from_storage(definition)
}

pub(crate) async fn delete_personal_definition(
    backend: &impl StorageContext,
    owner_id: i32,
    definition_id: i32,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_personal_computed_field(StoragePersonalComputedFieldDelete::new(
            principal_id_to_storage(owner_id),
            hubuum_domain::ComputedFieldDefinitionId::new(definition_id)
                .expect("validated computed field definition id must be positive"),
        ))
        .await?;
    Ok(())
}

#[doc(hidden)]
pub async fn request_class_rebuild(
    backend: &impl StorageContext,
    class_id: i32,
    authorized_collection_id: i32,
    actor_id: Option<i32>,
) -> Result<ClassComputationState, ApiError> {
    let state = storage_handle(backend)
        .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
            class_id_to_storage(class_id),
            collection_id_to_storage(authorized_collection_id),
            actor_id.map(principal_id_to_storage),
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
    input.validate()?;
    Ok(StorageComputedFieldDefinitionInput::new(
        input.key,
        input.label,
        input.operation,
        input.result_type.as_str().to_string(),
    )
    .with_description(input.description)
    .with_enabled(input.enabled))
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
        operation: definition.operation().clone(),
        result_type: definition.result_type().to_string(),
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
