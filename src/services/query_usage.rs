use hubuum_domain::{ResourceId, ResourceRevision};
use hubuum_storage_core::{
    QueryUsageStorage, StorageQueryUsageCreate, StorageQueryUsageDeclaration,
    StorageQueryUsageDelete, StorageQueryUsagePattern, StorageQueryUsageReplace,
    StorageQueryUsageScope,
};
use serde_json::Value;

use crate::{
    errors::ApiError,
    events::EventContext,
    models::query_usage::QueryUsageResponse,
    storage::{StorageContext, storage_handle},
};

fn response(
    value: StorageQueryUsageDeclaration,
    schema: Option<&Value>,
) -> Result<QueryUsageResponse, ApiError> {
    QueryUsageResponse::from_record(value, schema)
        .map_err(|_| ApiError::InternalServerError("Query usage response projection failed".into()))
}

pub async fn list(
    context: &impl StorageContext,
    scope: StorageQueryUsageScope,
    schema: Option<&Value>,
) -> Result<Vec<QueryUsageResponse>, ApiError> {
    storage_handle(context)
        .list_query_usage(scope)
        .await?
        .into_iter()
        .map(|value| response(value, schema))
        .collect()
}

pub async fn create(
    context: &impl StorageContext,
    scope: StorageQueryUsageScope,
    pattern: StorageQueryUsagePattern,
    schema: Option<&Value>,
    event: EventContext,
) -> Result<QueryUsageResponse, ApiError> {
    response(
        storage_handle(context)
            .create_query_usage(StorageQueryUsageCreate::new(scope, pattern, event))
            .await?
            .into_value(),
        schema,
    )
}

pub async fn replace(
    context: &impl StorageContext,
    request: StorageQueryUsageReplace,
    schema: Option<&Value>,
) -> Result<QueryUsageResponse, ApiError> {
    response(
        storage_handle(context)
            .replace_query_usage(request)
            .await?
            .into_value(),
        schema,
    )
}

pub async fn delete(
    context: &impl StorageContext,
    scope: StorageQueryUsageScope,
    id: ResourceId,
    revision: ResourceRevision,
    event: EventContext,
) -> Result<(), ApiError> {
    let _ = storage_handle(context)
        .delete_query_usage(StorageQueryUsageDelete::new(scope, id, revision, event))
        .await?;
    Ok(())
}
