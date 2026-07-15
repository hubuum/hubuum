use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};
use bytes::BytesMut;
use futures_util::StreamExt;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::principal::load_principal_by_id;
use crate::models::{
    BackupDocument, RestoreConfirmRequest, RestoreInitiator, RestoreStageRequest,
    RestoreStageResponse,
};
use crate::restores::{
    RestoreSettings, confirm_restore, identity_scope_name, restore_status, stage_restore,
};

const RESTORE_CAPABILITY_HEADER: &str = "X-Hubuum-Restore-Capability";

#[utoipa::path(
    post,
    path = "/api/v1/restores",
    tag = "restores",
    security(("bearer_auth" = [])),
    request_body(content = BackupDocument, content_type = "application/json", description = "Full BackupDocument JSON bytes"),
    responses(
        (status = 201, description = "Restore document staged and validated", body = RestoreStageResponse,
            headers(("Cache-Control" = String, description = "Always no-store for restore capabilities and metadata"))
        ),
        (status = 400, description = "Invalid backup document", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Administrator access required", body = ApiErrorResponse),
        (status = 413, description = "Restore upload too large", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn create_restore_stage(
    pool: web::Data<DbPool>,
    settings: web::Data<RestoreSettings>,
    admin: AdminAccess,
    mut payload: web::Payload,
) -> Result<impl Responder, ApiError> {
    let mut document = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|error| {
            ApiError::BadRequest(format!("Failed to read restore upload: {error}"))
        })?;
        if document.len().saturating_add(chunk.len()) > settings.max_upload_bytes() {
            return Err(ApiError::PayloadTooLarge(format!(
                "Restore upload exceeds the configured {} byte limit",
                settings.max_upload_bytes()
            )));
        }
        document.extend_from_slice(&chunk);
    }
    if document.is_empty() {
        return Err(ApiError::BadRequest(
            "Restore upload must contain a backup document".to_string(),
        ));
    }
    let principal = load_principal_by_id(&pool, admin.user.id).await?;
    let scope_name = identity_scope_name(&pool, principal.identity_scope_id).await?;
    let initiator = RestoreInitiator::principal(&principal, scope_name)?;
    let request = RestoreStageRequest::new(initiator, document.to_vec())?;
    let staged = stage_restore(&pool, &settings, request).await?;
    Ok(ApiResponse::new_no_store(staged, StatusCode::CREATED))
}

#[utoipa::path(
    post,
    path = "/api/v1/restores/{restore_id}/confirm",
    tag = "restores",
    security(("bearer_auth" = [])),
    params(("restore_id" = i64, Path, description = "Restore stage ID")),
    request_body = RestoreConfirmRequest,
    responses(
        (status = 200, description = "Restore completed", body = RestoreStageResponse,
            headers(("Cache-Control" = String, description = "Always no-store for restore metadata"))
        ),
        (status = 400, description = "Confirmation phrase is invalid", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Administrator or capability rejected", body = ApiErrorResponse),
        (status = 409, description = "Stage state or SHA-256 mismatch", body = ApiErrorResponse),
        (status = 410, description = "Restore stage expired", body = ApiErrorResponse)
    )
)]
#[post("/{restore_id}/confirm")]
pub async fn confirm_restore_stage(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    restore_id: web::Path<i64>,
    confirmation: web::Json<RestoreConfirmRequest>,
) -> Result<impl Responder, ApiError> {
    let response = confirm_restore(&pool, restore_id.into_inner(), &confirmation).await?;
    Ok(ApiResponse::new_no_store(response, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/restores/{restore_id}/status",
    tag = "restores",
    security(("restore_capability" = [])),
    params(("restore_id" = i64, Path, description = "Restore stage ID")),
    responses(
        (status = 200, description = "Restore status", body = RestoreStageResponse,
            headers(("Cache-Control" = String, description = "Always no-store for restore metadata"))
        ),
        (status = 400, description = "Restore capability header is missing", body = ApiErrorResponse),
        (status = 403, description = "Capability rejected", body = ApiErrorResponse),
        (status = 404, description = "Restore stage not found", body = ApiErrorResponse)
    )
)]
#[get("/{restore_id}/status")]
pub async fn get_restore_status(
    pool: web::Data<DbPool>,
    restore_id: web::Path<i64>,
    request: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let capability = request
        .headers()
        .get(RESTORE_CAPABILITY_HEADER)
        .ok_or_else(|| ApiError::BadRequest(format!("Missing {RESTORE_CAPABILITY_HEADER} header")))?
        .to_str()
        .map_err(|_| {
            ApiError::BadRequest(format!(
                "{RESTORE_CAPABILITY_HEADER} header is not valid text"
            ))
        })?;
    let response = restore_status(&pool, restore_id.into_inner(), capability).await?;
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(response))
}
