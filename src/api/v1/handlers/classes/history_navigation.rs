use crate::api::v1::handlers::history::{HistoryResponse, resolve_history_principal_names};
use crate::api::{openapi::ApiErrorResponse, response::ApiResponse};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{
    HistoryMarker, HistoryTimelineParameters, HubuumClassHistory, HubuumClassID,
    HubuumObjectHistory, HubuumObjectID,
};
use crate::pagination::{effective_page_limit, finalize_page};
use crate::permissions::AppContext;
use crate::services::history::navigation::{HistoryEntry, HistoryReader};
use crate::storage::StorageHistoryResource;
use actix_web::{Responder, get, http::StatusCode, web};
use hubuum_domain::HistoryRecordId;

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/history/timeline", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "class_id"), HistoryTimelineParameters),
    responses((status = 200, description = "Lightweight authorized timeline; ordered by valid_from and history_id", body = [HistoryResponse<HistoryMarker>]),
        (status = 400, description = "Invalid navigation query", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/history/timeline")]
pub async fn get_class_history_timeline(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<HubuumClassID>,
    params: web::Query<HistoryTimelineParameters>,
) -> Result<impl Responder, ApiError> {
    let class_id = path.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let (rows, total, options) = reader.timeline(params.into_inner()).await?;
    let page = finalize_page(rows, &options)?;
    let names = resolve_history_principal_names(&context, &page.items).await?;
    let items = page
        .items
        .into_iter()
        .map(|row| HistoryResponse::new(row, &names))
        .collect::<Vec<_>>();
    Ok(ApiResponse::paginated_items(
        items,
        &page.next_cursor,
        total,
        effective_page_limit(&options)?,
        true,
    ))
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/history/entries/{history_id}", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "class_id"), ("history_id" = i64, Path, description = "history_id")),
    responses((status = 200, description = "Exact retained history entry, including deletion markers", body = HistoryResponse<HubuumClassHistory>),
        (status = 400, description = "Invalid navigation query", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/history/entries/{history_id}")]
pub async fn get_class_history_entry(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HistoryRecordId)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, history_id) = path.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let HistoryEntry::Class(row) = reader.entry(history_id).await? else {
        return Err(ApiError::InternalServerError(
            "History storage returned the wrong resource kind".into(),
        ));
    };
    let names = resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new_private_no_store(
        HistoryResponse::new(row, &names),
        StatusCode::OK,
    ))
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/{object_id}/history/timeline", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "class_id"), ("object_id" = i32, Path, description = "object_id"), HistoryTimelineParameters),
    responses((status = 200, description = "Lightweight authorized timeline; ordered by valid_from and history_id", body = [HistoryResponse<HistoryMarker>]),
        (status = 400, description = "Invalid navigation query", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/{object_id}/history/timeline")]
pub async fn get_object_history_timeline(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HubuumObjectID)>,
    params: web::Query<HistoryTimelineParameters>,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = path.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Object {
            class_id,
            object_id,
        },
    )
    .await?;
    let (rows, total, options) = reader.timeline(params.into_inner()).await?;
    let page = finalize_page(rows, &options)?;
    let names = resolve_history_principal_names(&context, &page.items).await?;
    let items = page
        .items
        .into_iter()
        .map(|row| HistoryResponse::new(row, &names))
        .collect::<Vec<_>>();
    Ok(ApiResponse::paginated_items(
        items,
        &page.next_cursor,
        total,
        effective_page_limit(&options)?,
        true,
    ))
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/{object_id}/history/entries/{history_id}", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "class_id"), ("object_id" = i32, Path, description = "object_id"), ("history_id" = i64, Path, description = "history_id")),
    responses((status = 200, description = "Exact retained history entry, including deletion markers", body = HistoryResponse<HubuumObjectHistory>),
        (status = 400, description = "Invalid navigation query", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/{object_id}/history/entries/{history_id}")]
pub async fn get_object_history_entry(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HubuumObjectID, HistoryRecordId)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id, history_id) = path.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Object {
            class_id,
            object_id,
        },
    )
    .await?;
    let HistoryEntry::Object(row) = reader.entry(history_id).await? else {
        return Err(ApiError::InternalServerError(
            "History storage returned the wrong resource kind".into(),
        ));
    };
    let names = resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new_private_no_store(
        HistoryResponse::new(row, &names),
        StatusCode::OK,
    ))
}
