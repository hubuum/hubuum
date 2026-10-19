use super::*;
use crate::services::history::navigation::{HistoryEntry, HistoryReader, HistoryRows};
use crate::storage::StorageHistoryResource;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    responses(
        (status = 200, description = "Class history", body = [HistoryResponse<HubuumClassHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history")]
async fn get_class_history(
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{HistoryResponse, resolve_history_principal_names};
    let class_id = class_id.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let params = parse_query_parameter(req.query_string())?;
    let (rows, total_count) = reader.list(&params).await?;
    let HistoryRows::Class(rows) = rows else {
        return Err(ApiError::InternalServerError(
            "History returned the wrong resource kind".into(),
        ));
    };

    let principal_names = resolve_history_principal_names(&context, &rows).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| HistoryResponse::new(row, &principal_names))
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Class version at timestamp", body = HistoryResponse<HubuumClassHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history/as-of")]
async fn get_class_as_of(
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, parse_as_of, resolve_history_principal_names,
    };
    let class_id = class_id.into_inner();
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let at = parse_as_of(req.query_string())?;
    let HistoryEntry::Class(row) = reader.as_of(at).await? else {
        return Err(ApiError::InternalServerError(
            "History returned the wrong resource kind".into(),
        ));
    };

    let principal_names =
        resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new(
        HistoryResponse::new(row, &principal_names),
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Object history", body = [HistoryResponse<HubuumObjectHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history")]
async fn get_object_history(
    context: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{HistoryResponse, resolve_history_principal_names};
    let (class_id, object_id) = paths.into_inner();
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
    let params = parse_query_parameter(req.query_string())?;
    let (rows, total_count) = reader.list(&params).await?;
    let HistoryRows::Object(rows) = rows else {
        return Err(ApiError::InternalServerError(
            "History returned the wrong resource kind".into(),
        ));
    };

    let principal_names = resolve_history_principal_names(&context, &rows).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| HistoryResponse::new(row, &principal_names))
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Object version at timestamp", body = HistoryResponse<HubuumObjectHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class, object, or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history/as-of")]
async fn get_object_as_of(
    context: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, parse_as_of, resolve_history_principal_names,
    };
    let (class_id, object_id) = paths.into_inner();
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
    let at = parse_as_of(req.query_string())?;
    let HistoryEntry::Object(row) = reader.as_of(at).await? else {
        return Err(ApiError::InternalServerError(
            "History returned the wrong resource kind".into(),
        ));
    };

    let principal_names =
        resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new(
        HistoryResponse::new(row, &principal_names),
        StatusCode::OK,
    ))
}
