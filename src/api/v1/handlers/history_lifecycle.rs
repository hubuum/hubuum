use actix_web::{Responder, error::JsonPayloadError, get, http::StatusCode, post, put, web};
use hubuum_storage_core::{
    HistoryLifecycleOperationEvidence, HistoryPolicyPreview, HistoryPolicyRecord,
    HistoryRetentionHold,
};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::history_lifecycle::{
    HistoryLifecycleInspection, HistoryLifecycleInspectionRequest, HistoryPolicyApplicationRequest,
    HistoryPolicyListRequest, HistoryPolicyPage, HistoryPolicyPreviewRequest,
    HistoryRetentionHoldReleaseRequest, HistoryRetentionHoldRequest,
};
use crate::permissions::AppContext;
use crate::services::deletion_recovery::history_policy;

#[utoipa::path(get, path = "/api/v1/admin/history-lifecycle/policies", tag = "admin", security(("bearer_auth" = [])), responses((status = 200, description = "Bounded immutable policy revisions", body = HistoryPolicyPage), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)))]
#[get("/policies")]
pub async fn policies(
    context: AppContext,
    admin: AdminAccess,
    query: web::Query<HistoryPolicyListRequest>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new_no_store(
        history_policy::list(
            &context,
            &admin,
            query.into_inner(),
            context.history_retention(),
        )
        .await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/admin/history-lifecycle/inspect", tag = "admin", security(("bearer_auth" = [])), request_body = HistoryLifecycleInspectionRequest, responses((status = 200, description = "Bounded effective sources, original anchors, deadlines and blockers; samples contain no retained payloads", body = HistoryLifecycleInspection), (status = 400, description = "Invalid scope or limits", body = ApiErrorResponse), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)))]
#[post("/inspect")]
pub async fn inspect(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<HistoryLifecycleInspectionRequest>,
) -> Result<impl Responder, ApiError> {
    let scan = request.into_inner().resolve(&context).await?;
    Ok(ApiResponse::new_no_store(
        history_policy::inspect(&context, &admin, scan).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/admin/history-lifecycle/policy-previews", tag = "admin", security(("bearer_auth" = [])), request_body = HistoryPolicyPreviewRequest, responses((status = 200, description = "Issued principal/revision-bound review with fixed expiry and bounded impact coverage; reserves no execution capacity", body = HistoryPolicyPreview), (status = 400, description = "Invalid proposed policy", body = ApiErrorResponse), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse), (status = 409, description = "Policy or protection authority changed", body = ApiErrorResponse)))]
#[post("/policy-previews")]
pub async fn preview(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<HistoryPolicyPreviewRequest>,
) -> Result<impl Responder, ApiError> {
    let proposal = request.into_inner().change.resolve(&context).await?;
    Ok(ApiResponse::new_no_store(
        history_policy::preview(&context, &admin, proposal).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(put, path = "/api/v1/admin/history-lifecycle/policies", tag = "admin", security(("bearer_auth" = [])), request_body = HistoryPolicyApplicationRequest, responses((status = 200, description = "Exact immutable applied policy, independently protected; physical expiry is a separate operation", body = HistoryPolicyRecord), (status = 400, description = "Malformed change or missing retrospective review", body = ApiErrorResponse), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse), (status = 404, description = "Unknown or inaccessible review", body = ApiErrorResponse), (status = 409, description = "stale_preview, preview_expired or preview_mismatch; inspect current authority before reviewing again", body = ApiErrorResponse), (status = 503, description = "Independent recovery protection unavailable; application may be committed and pending protection", body = ApiErrorResponse)))]
#[put("/policies")]
pub async fn apply(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<HistoryPolicyApplicationRequest>,
) -> Result<impl Responder, ApiError> {
    let request = request.into_inner().resolve(&context).await?;
    Ok(ApiResponse::new_no_store(
        history_policy::apply(&context, &admin, context.history_retention(), request).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/admin/history-lifecycle/holds", tag = "admin", security(("bearer_auth" = [])), request_body = HistoryRetentionHoldRequest, responses((status = 200, description = "Independently protected hold, preserving original creation and expiry clocks on retry", body = HistoryRetentionHold), (status = 400, description = "Invalid hold", body = ApiErrorResponse), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse), (status = 409, description = "Changed authority or reused hold identity", body = ApiErrorResponse), (status = 503, description = "Independent recovery protection unavailable", body = ApiErrorResponse)))]
#[post("/holds")]
pub async fn create_hold(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<HistoryRetentionHoldRequest>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new_no_store(
        history_policy::create_hold(
            &context,
            &admin,
            context.history_retention(),
            request.into_inner(),
        )
        .await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(post, path = "/api/v1/admin/history-lifecycle/holds/{hold_id}/release", tag = "admin", security(("bearer_auth" = [])), params(("hold_id" = String, Path, description = "Stable hold UUID")), request_body = HistoryRetentionHoldReleaseRequest, responses((status = 200, description = "Independently protected hold release", body = HistoryRetentionHold), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse), (status = 404, description = "Hold not found", body = ApiErrorResponse), (status = 409, description = "Changed authority", body = ApiErrorResponse), (status = 503, description = "Independent recovery protection unavailable", body = ApiErrorResponse)))]
#[post("/holds/{hold_id}/release")]
pub async fn release_hold(
    context: AppContext,
    admin: AdminAccess,
    identity: web::Path<uuid::Uuid>,
    request: web::Json<HistoryRetentionHoldReleaseRequest>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new_no_store(
        history_policy::release_hold(
            &context,
            &admin,
            context.history_retention(),
            identity.into_inner(),
            request.expected_authority_revision,
        )
        .await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(get, path = "/api/v1/admin/history-lifecycle/operations/{operation_id}", tag = "admin", security(("bearer_auth" = [])), params(("operation_id" = String, Path, description = "Exact lifecycle audit operation/receipt UUID")), responses((status = 200, description = "Retained policy, hold or review evidence with original scope and clocks", body = HistoryLifecycleOperationEvidence), (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse), (status = 404, description = "Operation evidence unavailable", body = ApiErrorResponse)))]
#[get("/operations/{operation_id}")]
pub async fn operation(
    context: AppContext,
    admin: AdminAccess,
    identity: web::Path<uuid::Uuid>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new_no_store(
        history_policy::operation(&context, &admin, identity.into_inner()).await?,
        StatusCode::OK,
    ))
}
pub fn config(config: &mut web::ServiceConfig) {
    config.app_data(
        web::JsonConfig::default()
            .limit(crate::models::history_lifecycle::HISTORY_LIFECYCLE_MAX_REQUEST_BYTES)
            .error_handler(|error, request| match error {
                JsonPayloadError::OverflowKnownLength { .. }
                | JsonPayloadError::Overflow { .. } => {
                    crate::observability::metrics::extraction_failure("json");
                    ApiError::PayloadTooLarge("History lifecycle request exceeds 16 KiB".into())
                        .into()
                }
                other => crate::errors::json_error_handler(other, request),
            }),
    );
    config
        .service(policies)
        .service(inspect)
        .service(preview)
        .service(apply)
        .service(create_hold)
        .service(release_hold)
        .service(operation);
}
