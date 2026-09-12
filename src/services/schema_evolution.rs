use hubuum_domain::{ClassId, CollectionId, SchemaReference, TaskId};
use hubuum_storage_core::schema_evolution::*;
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    errors::ApiError,
    events::EventContext,
    models::schema_evolution::*,
    storage::{StorageContext, storage_handle},
};

fn response<T: DeserializeOwned>(value: impl Serialize) -> Result<T, ApiError> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(|_| ApiError::InternalServerError("Schema response projection failed".into()))
}

pub async fn list_revisions(
    context: &impl StorageContext,
    class_id: ClassId,
    query: &SchemaPageRequest,
) -> Result<Vec<SchemaRevisionResponse>, ApiError> {
    let page = StorageSchemaPage::try_new(class_id, query.after, query.limit)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    response(storage_handle(context).list_schema_revisions(page).await?)
}
pub async fn class_state(
    context: &impl StorageContext,
    class_id: ClassId,
) -> Result<ClassSchemaResponse, ApiError> {
    response(storage_handle(context).get_schema_state(class_id).await?)
}
pub async fn stage(
    context: &impl StorageContext,
    authorized_collection: CollectionId,
    class_id: ClassId,
    request: SchemaStageRequest,
    event: &EventContext,
) -> Result<SchemaRevisionResponse, ApiError> {
    let policy = StorageValidatedSchemaPolicy::try_new_with_limits(
        hubuum_storage_core::StorageClassSchemaPolicy::try_from_parts(
            request.json_schema,
            request.validate_schema,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        storage_handle(context).schema_limits(),
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    response(
        storage_handle(context)
            .stage_schema_revision(StorageSchemaStage::new(
                authorized_collection,
                class_id,
                policy,
                event.clone(),
            ))
            .await?
            .into_value(),
    )
}
pub async fn abandon(
    context: &impl StorageContext,
    authorized_collection: CollectionId,
    target: SchemaReference,
    event: &EventContext,
) -> Result<SchemaRevisionResponse, ApiError> {
    response(
        storage_handle(context)
            .abandon_schema_revision(target, authorized_collection, event)
            .await?
            .into_value(),
    )
}
pub async fn activate(
    context: &impl StorageContext,
    authorized_collection: CollectionId,
    target: SchemaReference,
    request: SchemaActivationRequest,
    event: &EventContext,
) -> Result<SchemaActivationResponse, ApiError> {
    let request = StorageSchemaActivation::new(
        authorized_collection,
        target,
        request.expected_active_revision,
        request.policy.into(),
        event.clone(),
    )
    .with_proof_task(request.impact_task_id);
    response(
        storage_handle(context)
            .activate_schema_revision(request)
            .await?
            .into_value(),
    )
}
pub async fn request_work(
    context: &impl StorageContext,
    authorized_collection: CollectionId,
    target: SchemaReference,
    kind: StorageSchemaWorkKind,
    event: &EventContext,
) -> Result<SchemaWorkResponse, ApiError> {
    work_response(
        context,
        storage_handle(context)
            .request_schema_work(StorageSchemaWorkRequest::new(
                authorized_collection,
                target,
                kind,
                event.clone(),
            ))
            .await?
            .into_value(),
    )
    .await
}
pub async fn get_work(
    context: &impl StorageContext,
    task_id: TaskId,
) -> Result<SchemaWorkResponse, ApiError> {
    work_response(
        context,
        storage_handle(context).get_schema_work(task_id).await?,
    )
    .await
}
pub async fn cancel_work(
    context: &impl StorageContext,
    authorized_collection: CollectionId,
    task_id: TaskId,
    event: &EventContext,
) -> Result<SchemaWorkResponse, ApiError> {
    work_response(
        context,
        storage_handle(context)
            .cancel_schema_work(task_id, authorized_collection, event)
            .await?
            .into_value(),
    )
    .await
}
pub async fn compliance(
    context: &impl StorageContext,
    class_id: ClassId,
    query: &SchemaPageRequest,
) -> Result<Vec<ObjectComplianceResponse>, ApiError> {
    let page = StorageSchemaPage::try_new(class_id, query.after, query.limit)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    response(
        storage_handle(context)
            .list_schema_compliance(page, query.status.map(Into::into))
            .await?,
    )
}
pub async fn revision(
    context: &impl StorageContext,
    target: SchemaReference,
) -> Result<SchemaRevisionResponse, ApiError> {
    let query = SchemaPageRequest {
        after: target.revision().get() - 1,
        limit: 1,
        status: None,
    };
    list_revisions(context, target.class_id(), &query)
        .await?
        .into_iter()
        .find(|revision| revision.revision == target.revision())
        .ok_or_else(|| ApiError::NotFound("Schema revision was not found".into()))
}

async fn work_response(
    context: &impl StorageContext,
    work: StorageSchemaWork,
) -> Result<SchemaWorkResponse, ApiError> {
    let mut result: SchemaWorkResponse = response(&work)?;
    if work.kind() == StorageSchemaWorkKind::Impact {
        let boundary = storage_handle(context)
            .get_schema_impact_boundary(work.target())
            .await?;
        result.readiness = Some(response(work.impact_readiness(&boundary))?);
        result.current_epoch = Some(boundary.epoch());
        result.current_active_schema = Some(boundary.active());
    }
    Ok(result)
}
