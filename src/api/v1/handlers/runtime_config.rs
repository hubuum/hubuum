use crate::storage::storage_handle;
use actix_web::{Responder, get, http::StatusCode, put, web};
use hubuum_storage_core::{
    StorageTaskCapacitySnapshot, StorageTaskPolicy, StorageTaskPolicyUpdate, TaskCapacityStorage,
};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::{QueryObservationConfig, RunningConfig};
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::query_usage::QueryObservationUpdateRequest;
use crate::permissions::AppContext;
use crate::services::query_usage;

#[utoipa::path(
    get,
    path = "/api/v1/admin/config",
    tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Effective redacted process configuration", body = RunningConfig),
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
    config.query_observations = query_usage::observation_configuration(&context).await?;
    config.apply_task_capacity(storage_handle(&context).task_capacity_snapshot().await?);
    Ok(ApiResponse::new(config, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/query-observations",
    tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Shared override applied to the responding replica and its effective observation settings", body = QueryObservationConfig),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/query-observations")]
pub async fn get_query_observations(
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(
        query_usage::observation_configuration(&context).await?,
    ))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/query-observations",
    tag = "admin",
    security(("bearer_auth" = [])),
    request_body = QueryObservationUpdateRequest,
    responses(
        (status = 200, description = "Shared override committed and broadcast; applied on this replica, other replicas reconcile asynchronously", body = QueryObservationConfig),
        (status = 400, description = "Invalid request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[put("/query-observations")]
pub async fn set_query_observations(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<QueryObservationUpdateRequest>,
) -> Result<impl Responder, ApiError> {
    let config = query_usage::set_observation_configuration(&context, request.into_inner()).await?;
    tracing::info!(
        actor_id = admin.user.id,
        enabled = config.enabled,
        "Query observation collection updated"
    );
    Ok(ApiResponse::ok(config))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/task-capacity",
    tag = "admin",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Authoritative shared task policy and capacity accounting", body = StorageTaskCapacitySnapshot),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse)
    )
)]
#[get("/task-capacity")]
pub async fn get_task_capacity(
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(
        storage_handle(&context).task_capacity_snapshot().await?,
    ))
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/task-capacity",
    tag = "admin",
    security(("bearer_auth" = [])),
    request_body = StorageTaskPolicyUpdate,
    responses(
        (status = 200, description = "Policy committed; reductions drain existing work", body = StorageTaskPolicy),
        (status = 400, description = "Invalid policy", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped human administrator required", body = ApiErrorResponse),
        (status = 409, description = "Policy revision changed; refetch before updating", body = ApiErrorResponse)
    )
)]
#[put("/task-capacity")]
pub async fn set_task_capacity(
    context: AppContext,
    admin: AdminAccess,
    request: web::Json<StorageTaskPolicyUpdate>,
) -> Result<impl Responder, ApiError> {
    let policy = storage_handle(&context)
        .replace_task_policy(request.into_inner())
        .await?;
    tracing::info!(
        actor_id = admin.user.id,
        task_policy_revision = policy.revision().get(),
        "Shared task admission policy updated"
    );
    Ok(ApiResponse::ok(policy))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_running_config)
        .service(get_query_observations)
        .service(set_query_observations)
        .service(get_task_capacity)
        .service(set_task_capacity);
}
