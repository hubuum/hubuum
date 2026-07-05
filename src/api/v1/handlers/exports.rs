use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::db::traits::task::TaskBackend;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{ExportJsonResponse, ExportOutputLookup, ExportRequest, TaskID, TaskResponse};
use crate::exports::{render_export_task_output, submit_export_task};
use crate::tasks::{ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker};

#[utoipa::path(
    post,
    path = "/api/v1/exports",
    tag = "exports",
    security(("bearer_auth" = [])),
    request_body = ExportRequest,
    responses(
        (status = 202, description = "Export task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active export tasks", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_export(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    export: web::Json<ExportRequest>,
) -> Result<impl Responder, ApiError> {
    let export = export.into_inner();
    let task = submit_export_task(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        Some(requestor.token_meta.id),
        idempotency_key_from_headers(req.headers())?,
        export,
        None,
    )
    .await?;

    let response = task.to_response()?;
    kick_task_worker(pool.get_ref().clone());

    Ok(ApiResponse::accepted_at(
        response,
        api_locations::task(task.id)?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/exports/{task_id}",
    tag = "exports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Export task ID")
    ),
    responses(
        (status = 200, description = "Export task projection", body = TaskResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Export task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}")]
pub async fn get_export(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = task_id
        .into_inner()
        .load_authorized_export(&pool, &requestor.principal)
        .await?;
    let output = task.find_export_output_summary(&pool).await?;
    Ok(ApiResponse::new(
        task.to_response_with_export_output(output.as_ref())?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/exports/{task_id}/output",
    tag = "exports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Export task ID")
    ),
    responses(
        (
            status = 200,
            description = "Stored export output",
            content(
                (ExportJsonResponse = "application/json"),
                (String = "text/plain"),
                (String = "text/html"),
                (String = "text/csv")
            )
        ),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Export output not found", body = ApiErrorResponse),
        (status = 410, description = "Export output expired", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/output")]
pub async fn get_export_output(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    task_id
        .load_authorized_export(&pool, &requestor.principal)
        .await?;
    match task_id.find_export_output(&pool).await? {
        ExportOutputLookup::Available(output) => render_export_task_output(output),
        ExportOutputLookup::Expired { expires_at } => Err(ApiError::Gone(format!(
            "Export output expired at {expires_at} UTC"
        ))),
        ExportOutputLookup::Missing => {
            Err(ApiError::NotFound("Export output not found".to_string()))
        }
    }
}
