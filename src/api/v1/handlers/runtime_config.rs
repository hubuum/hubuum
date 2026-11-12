use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, put, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::RunningConfig;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess};
use crate::models::runtime_settings::discovery::{
    RuntimeSettingsCatalogue, RuntimeSettingsGroupState, RuntimeSettingsPatch,
};
use crate::models::runtime_settings::{
    AuthenticationSettingsRequest, AuthorizationSettingsRequest, PaginationSettingsRequest,
    QueryObservationSettingsRequest, RuntimeSettingsGroup, RuntimeSettingsHistoryParams,
    RuntimeSettingsHistoryResponse, RuntimeSettingsReleaseParams, RuntimeSettingsReport,
    RuntimeSettingsUpdateResponse,
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
    let settings = runtime_settings::report(&context).await?;
    let effective = crate::config::runtime::for_storage(context.backend())?;
    let mut effective_config = RunningConfig::from(effective.as_ref());
    effective_config.database.backend = config.database.backend.clone();
    let mut config = effective_config;
    let storage = context.backend();
    config.query_observations =
        runtime_settings::query_observation_config(storage, &storage.applied_runtime_settings());
    config.pagination.default_page_limit = settings.pagination.effective.default_page_limit;
    config.pagination.max_page_limit = settings.pagination.effective.max_page_limit;
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
    get, path = "/api/v1/admin/runtime-settings/schema", tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Versioned settings catalogue with this replica's deployment bounds; capability metadata is not a cluster-wide acknowledgement", body = RuntimeSettingsCatalogue),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/runtime-settings/schema")]
pub async fn get_runtime_settings_schema(
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(runtime_settings::catalogue(&context)?))
}

#[utoipa::path(
    get, path = "/api/v1/admin/runtime-settings/{group}", tag = "admin",
    security(("bearer_auth" = [])), params(("group" = RuntimeSettingsGroup, Path, description = "Discovered settings group")),
    responses(
        (status = 200, description = "Uniform redacted state of one group on the responding replica", body = RuntimeSettingsGroupState),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/runtime-settings/{group}")]
pub async fn get_runtime_settings_group(
    context: AppContext,
    _admin: AdminAccess,
    group: web::Path<RuntimeSettingsGroup>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(RuntimeSettingsGroupState::from_report(
        group.into_inner(),
        runtime_settings::report(&context).await?,
    )?))
}

#[utoipa::path(
    patch, path = "/api/v1/admin/runtime-settings/{group}", tag = "admin",
    security(("bearer_auth" = [])), params(("group" = RuntimeSettingsGroup, Path, description = "Discovered settings group with patch_supported=true")),
    request_body = RuntimeSettingsPatch,
    responses(
        (status = 200, description = "Supplied fields committed atomically; omitted fields preserved, replicas reconcile asynchronously", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid fields, values, combination, deployment ceiling or unsupported workflow", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Administrator required; security settings require a local password-capable human administrator", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; refetch before editing", body = ApiErrorResponse)
    )
)]
#[patch("/runtime-settings/{group}")]
pub async fn patch_runtime_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    group: web::Path<RuntimeSettingsGroup>,
    request: web::Json<RuntimeSettingsPatch>,
) -> Result<impl Responder, ApiError> {
    let response = runtime_settings::patch(
        &context,
        group.into_inner(),
        request.into_inner(),
        admin.event_context(&http_request),
        crate::middlewares::client_allowlist::NetworkChangeOrigin::from_request(&http_request),
    )
    .await?;
    log_update(&admin, &response);
    Ok(ApiResponse::ok(response))
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
    let update = request.into_inner().into_update(
        context
            .clone_backend()
            .deployment_query_observation_settings(),
        admin.event_context(&http_request),
    )?;
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
        (status = 400, description = "Invalid page limits, default above maximum, or deployment ceiling exceeded", body = ApiErrorResponse),
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

#[utoipa::path(post, path = "/api/v1/admin/runtime-settings/authentication/preflight", tag = "admin",
    security(("bearer_auth" = [])), request_body = AuthenticationSettingsRequest,
    responses((status = 409, description = "Use the verified security candidate workflow", body = ApiErrorResponse),
    (status = 401, description = "Unauthorized", body = ApiErrorResponse), (status = 403, description = "Administrator required", body = ApiErrorResponse)))]
#[post("/runtime-settings/authentication/preflight")]
pub async fn preflight_authentication_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    request: web::Json<AuthenticationSettingsRequest>,
) -> Result<impl Responder, ApiError> {
    let _ = (context, admin, http_request, request);
    Err::<ApiResponse<RuntimeSettingsUpdateResponse>, _>(ApiError::Conflict(
        "Authentication changes require POST /admin/runtime-settings/security-candidates and fresh activation approval".into(),
    ))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/runtime-settings/authorization",
    tag = "admin",
    security(("bearer_auth" = [])),
    request_body = AuthorizationSettingsRequest,
    responses(
        (status = 409, description = "Use the verified security candidate workflow, including release_ownership for releases", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped local recovery administrator required", body = ApiErrorResponse)
    )
)]
#[put("/runtime-settings/authorization")]
pub async fn set_authorization_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    request: web::Json<AuthorizationSettingsRequest>,
) -> Result<impl Responder, ApiError> {
    let update = request
        .into_inner()
        .into_update(admin.event_context(&http_request));
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
    let response = runtime_settings::update_with_origin(
        context,
        update,
        Some(crate::middlewares::client_allowlist::NetworkChangeOrigin::from_request(http_request)),
    )
    .await?;
    log_update(admin, &response);
    Ok(ApiResponse::ok(response))
}

#[utoipa::path(
    delete, path = "/api/v1/admin/runtime-settings/{group}", tag = "admin",
    security(("bearer_auth" = [])),
    params(("group" = RuntimeSettingsGroup, Path, description = "Discovered settings group"), RuntimeSettingsReleaseParams),
    responses(
        (status = 200, description = "Deployment ownership committed as a new audited revision", body = RuntimeSettingsUpdateResponse),
        (status = 400, description = "Invalid request or unavailable deployment configuration", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Administrator required; security settings require a local password-capable human administrator", body = ApiErrorResponse),
        (status = 412, description = "Stale expected_revision; refetch before editing", body = ApiErrorResponse)
    )
)]
#[delete("/runtime-settings/{group}")]
pub async fn release_runtime_settings_group(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    group: web::Path<RuntimeSettingsGroup>,
    params: web::Query<RuntimeSettingsReleaseParams>,
) -> Result<impl Responder, ApiError> {
    release(
        &context,
        &admin,
        &http_request,
        group.into_inner(),
        params.into_inner(),
    )
    .await
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
    delete,
    path = "/api/v1/admin/runtime-settings/authentication",
    tag = "admin",
    security(("bearer_auth" = [])),
    params(RuntimeSettingsReleaseParams),
    responses(
        (status = 409, description = "Use a verified release_ownership security candidate", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped local recovery administrator required", body = ApiErrorResponse)
    )
)]
#[delete("/runtime-settings/authentication")]
pub async fn release_authentication_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    params: web::Query<RuntimeSettingsReleaseParams>,
) -> Result<impl Responder, ApiError> {
    release(
        &context,
        &admin,
        &http_request,
        RuntimeSettingsGroup::Authentication,
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

#[utoipa::path(
    delete,
    path = "/api/v1/admin/runtime-settings/authorization",
    tag = "admin",
    security(("bearer_auth" = [])),
    params(RuntimeSettingsReleaseParams),
    responses(
        (status = 409, description = "Use the verified security candidate workflow, including release_ownership for releases", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped local recovery administrator required", body = ApiErrorResponse)
    )
)]
#[delete("/runtime-settings/authorization")]
pub async fn release_authorization_settings(
    context: AppContext,
    admin: AdminAccess,
    http_request: HttpRequest,
    params: web::Query<RuntimeSettingsReleaseParams>,
) -> Result<impl Responder, ApiError> {
    release(
        &context,
        &admin,
        &http_request,
        RuntimeSettingsGroup::Authorization,
        params.into_inner(),
    )
    .await
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.configure(super::security_candidates::config)
        .service(get_running_config)
        .service(get_runtime_settings)
        .service(get_runtime_settings_schema)
        .service(preflight_authentication_settings)
        .service(set_authorization_settings)
        .service(set_query_observation_settings)
        .service(set_pagination_settings)
        .service(release_authentication_settings)
        .service(release_authorization_settings)
        .service(release_query_observation_settings)
        .service(release_pagination_settings)
        .service(get_runtime_settings_history)
        .service(get_runtime_settings_group)
        .service(release_runtime_settings_group)
        .service(patch_runtime_settings);
}
