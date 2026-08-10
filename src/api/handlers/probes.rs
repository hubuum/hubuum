use actix_web::{Responder, get, http::StatusCode};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::storage::capabilities::probe::ProbeBackend;
use crate::storage::capabilities::{StorageCallSite, with_storage_call_site};

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
pub async fn readyz(context: AppContext) -> Result<impl Responder, ApiError> {
    let snapshot = with_storage_call_site(StorageCallSite::Readiness, context.readiness_snapshot())
        .await
        .map_err(|_| ApiError::ServiceUnavailable("Database is not ready".to_string()))?;
    if !snapshot.schema_is_ready() {
        return Err(ApiError::ServiceUnavailable(
            "Database schema is not ready".to_string(),
        ));
    }
    if !snapshot.maintenance_state().is_normal() {
        return Err(ApiError::ServiceUnavailable(format!(
            "Service is in '{}' maintenance",
            snapshot.maintenance_state()
        )));
    }

    Ok(ApiResponse::new(ProbeResponse::ok("ready"), StatusCode::OK))
}
