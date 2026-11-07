use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, put, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::RunningConfig;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess};
use crate::models::runtime_settings::{
    PaginationSettingsRequest, QueryObservationSettingsRequest, RuntimeSettingsGroup,
    RuntimeSettingsHistoryParams, RuntimeSettingsHistoryResponse, RuntimeSettingsReleaseParams,
    RuntimeSettingsReport, RuntimeSettingsUpdateResponse,
};
use crate::pagination::deployment_page_limits;
use crate::permissions::AppContext;
use crate::services::runtime_settings;

#[utoipa::path(
    get,
    path = "/api/v1/admin/config",
    tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Effective redacted process configuration, including runtime settings applied by the responding replica", body = RunningConfig),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/config")]
pub async fn get_running_config(
    config: web::Data<RunningConfig>,
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let mut config = config.get_ref().clone();
    let settings = runtime_settings::report(&context).await?;
    let storage = context.backend();
    config.query_observations =
        runtime_settings::query_observation_config(storage, &storage.applied_runtime_settings());
    config.pagination.default_page_limit = settings.pagination.effective.default_page_limit;
    config.runtime_settings = Some(settings);
    Ok(ApiResponse::new(config, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/runtime-settings",
    tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Runtime settings applied by the responding replica after reconciling with shared storage", body = RuntimeSettingsReport),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/runtime-settings")]
pub async fn get_runtime_settings(
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(runtime_settings::report(&context).await?))
}

fn log_update(admin: &AdminAccess, response: &RuntimeSettingsUpdateResponse) {
    tracing::info!(
        actor_id = admin.user.id,
        group = ?response.group,
        revision = response.committed_revision.get(),
        changed = response.changed,
        "Runtime settings updated"
    );
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/runtime-settings/query_observations",
    tag = "admin",
    security(("bearer_auth" = [])),
    request_body = QueryObservationSettingsRequest,
    responses(
        (status = 200, description = "Shared value committed with an audit record; applied by the responding replica, other replicas reconcile asynchronously", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; the response carries the current revision", body = ApiErrorResponse)
    )
)]
#[put("/runtime-settings/query_observations")]
pub async fn set_query_observation_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    request: web::Json<QueryObservationSettingsRequest>,
) -> Result<impl Responder, ApiError> {
    let update = request
        .into_inner()
        .into_update(admin.event_context(&http_request));
    let response = runtime_settings::update(&context, update).await?;
    log_update(&admin, &response);
    Ok(ApiResponse::ok(response))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/runtime-settings/pagination",
    tag = "admin",
    security(("bearer_auth" = [])),
    request_body = PaginationSettingsRequest,
    responses(
        (status = 200, description = "Shared value committed with an audit record; applied by the responding replica, other replicas reconcile asynchronously", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid value or a default above the deployment max_page_limit", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; the response carries the current revision", body = ApiErrorResponse)
    )
)]
#[put("/runtime-settings/pagination")]
pub async fn set_pagination_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    request: web::Json<PaginationSettingsRequest>,
) -> Result<impl Responder, ApiError> {
    let update = request.into_inner().into_update(
        deployment_page_limits()?,
        admin.event_context(&http_request),
    )?;
    let response = runtime_settings::update(&context, update).await?;
    log_update(&admin, &response);
    Ok(ApiResponse::ok(response))
}

async fn release(
    context: &AppContext,
    admin: &AdminAccess,
    http_request: &HttpRequest,
    group: RuntimeSettingsGroup,
    params: RuntimeSettingsReleaseParams,
) -> Result<ApiResponse<RuntimeSettingsUpdateResponse>, ApiError> {
    let update = params.into_update(group, admin.event_context(http_request));
    let response = runtime_settings::update(context, update).await?;
    log_update(admin, &response);
    Ok(ApiResponse::ok(response))
}

#[utoipa::path(
    delete,
    path = "/api/v1/admin/runtime-settings/query_observations",
    tag = "admin",
    security(("bearer_auth" = [])),
    params(RuntimeSettingsReleaseParams),
    responses(
        (status = 200, description = "Deployment ownership committed as a new audited revision", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; the response carries the current revision", body = ApiErrorResponse)
    )
)]
#[delete("/runtime-settings/query_observations")]
pub async fn release_query_observation_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    params: web::Query<RuntimeSettingsReleaseParams>,
) -> Result<impl Responder, ApiError> {
    release(
        &context,
        &admin,
        &http_request,
        RuntimeSettingsGroup::QueryObservations,
        params.into_inner(),
    )
    .await
}

#[utoipa::path(
    delete,
    path = "/api/v1/admin/runtime-settings/pagination",
    tag = "admin",
    security(("bearer_auth" = [])),
    params(RuntimeSettingsReleaseParams),
    responses(
        (status = 200, description = "Deployment ownership committed as a new audited revision", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; the response carries the current revision", body = ApiErrorResponse)
    )
)]
#[delete("/runtime-settings/pagination")]
pub async fn release_pagination_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    params: web::Query<RuntimeSettingsReleaseParams>,
) -> Result<impl Responder, ApiError> {
    release(
        &context,
        &admin,
        &http_request,
        RuntimeSettingsGroup::Pagination,
        params.into_inner(),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/runtime-settings/history/{group}",
    tag = "admin",
    security(("bearer_auth" = [])),
    params(
        ("group" = RuntimeSettingsGroup, Path, description = "Runtime settings group"),
        RuntimeSettingsHistoryParams
    ),
    responses(
        (status = 200, description = "Durable audit records for the group, newest first", body = RuntimeSettingsHistoryResponse),
        (status = 400, description = "Invalid group or paging parameters", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/runtime-settings/history/{group}")]
pub async fn get_runtime_settings_history(
    context: AppContext,
    _admin: AdminAccess,
    group: web::Path<RuntimeSettingsGroup>,
    params: web::Query<RuntimeSettingsHistoryParams>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(
        runtime_settings::history(&context, group.into_inner(), &params).await?,
    ))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_running_config)
        .service(get_runtime_settings)
        .service(set_query_observation_settings)
        .service(set_pagination_settings)
        .service(release_query_observation_settings)
        .service(release_pagination_settings)
        .service(get_runtime_settings_history);
}
