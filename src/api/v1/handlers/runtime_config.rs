use actix_web::{Responder, get, http::StatusCode, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::RunningConfig;
use crate::errors::ApiError;
use crate::extractors::AdminAccess;

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
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new(config.get_ref().clone(), StatusCode::OK))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_running_config);
}
