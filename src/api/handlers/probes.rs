use actix_web::{Responder, get, http::StatusCode, web};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::{DbPool, ensure_database_schema_ready};
use crate::errors::ApiError;

#[derive(Serialize, ToSchema)]
pub struct ProbeResponse {
    status: String,
}

impl ProbeResponse {
    fn ok(status: &str) -> Self {
        Self {
            status: status.to_string(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "probes",
    responses(
        (status = 200, description = "Process is alive", body = ProbeResponse)
    )
)]
#[get("/healthz")]
pub async fn healthz() -> impl Responder {
    ApiResponse::new(ProbeResponse::ok("ok"), StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "probes",
    responses(
        (status = 200, description = "Service is ready to receive traffic", body = ProbeResponse),
        (status = 503, description = "Service is not ready", body = ApiErrorResponse)
    )
)]
#[get("/readyz")]
pub async fn readyz(pool: web::Data<DbPool>) -> Result<impl Responder, ApiError> {
    ensure_database_schema_ready(pool.get_ref())
        .await
        .map_err(|_| ApiError::ServiceUnavailable("Database schema is not ready".to_string()))?;

    Ok(ApiResponse::new(ProbeResponse::ok("ready"), StatusCode::OK))
}
