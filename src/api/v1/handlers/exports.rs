use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::traits::task::TaskBackend;
use crate::errors::ApiError;
use crate::exports::submit_export_task;
use crate::extractors::Authenticated;
use crate::models::{
    ExportContentType, ExportJsonResponse, ExportMeta, ExportOutputLookup, ExportRequest,
    ExportTaskOutputRecord, ExportWarning, TaskID, TaskResponse,
};
use crate::permissions::{AppContext, require_unscoped_runtime_admin};
use crate::tasks::{ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker};

const EXPORT_WARNINGS_HEADER: &str = "X-Hubuum-Export-Warnings";
const EXPORT_TRUNCATED_HEADER: &str = "X-Hubuum-Export-Truncated";

fn render_export_task_output(output: ExportTaskOutputRecord) -> Result<HttpResponse, ApiError> {
    let content_type = ExportContentType::from_mime(&output.content_type)?;
    let _meta: ExportMeta = serde_json::from_value(output.meta_json)?;
    let warnings: Vec<ExportWarning> = serde_json::from_value(output.warnings_json)?;
    let mut response = HttpResponse::build(StatusCode::OK);
    response.insert_header((EXPORT_WARNINGS_HEADER, warnings.len().to_string()));
    response.insert_header((EXPORT_TRUNCATED_HEADER, output.truncated.to_string()));

    match content_type {
        ExportContentType::ApplicationJson => {
            let body: ExportJsonResponse =
                serde_json::from_value(output.json_output.ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Stored export JSON output is missing".to_string(),
                    )
                })?)?;
            Ok(response.json(body))
        }
        ExportContentType::TextPlain | ExportContentType::TextHtml | ExportContentType::TextCsv => {
            response.content_type(content_type.as_mime());
            Ok(response.body(output.text_output.ok_or_else(|| {
                ApiError::InternalServerError("Stored export text output is missing".to_string())
            })?))
        }
    }
}

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
        (status = 403, description = "Runtime administrator required", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active export tasks", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_export(
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    export: web::Json<ExportRequest>,
) -> Result<impl Responder, ApiError> {
    require_unscoped_runtime_admin(
        &pool,
        &requestor.principal,
        requestor.token_meta.is_scoped(),
    )
    .await?;
    let export = export.into_inner();
    let task = submit_export_task(
        &pool,
        &requestor.principal,
        Some(requestor.token_meta.id),
        idempotency_key_from_headers(req.headers())?,
        export,
        None,
    )
    .await?;

    let response = task.to_response()?;
    kick_task_worker(pool.clone());

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
    pool: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.clone());
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
    pool: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExportScope, ExportScopeKind};

    fn test_timestamp() -> chrono::NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    fn text_export_output(meta_truncated: bool, output_truncated: bool) -> ExportTaskOutputRecord {
        ExportTaskOutputRecord {
            id: 1,
            task_id: 1,
            template_name: Some("summary".to_string()),
            content_type: ExportContentType::TextPlain.as_mime().to_string(),
            json_output: None,
            text_output: Some("ok".to_string()),
            meta_json: serde_json::to_value(ExportMeta {
                count: 1,
                truncated: meta_truncated,
                scope: ExportScope {
                    kind: ExportScopeKind::ObjectsInClass,
                    class_id: Some(1),
                    object_id: None,
                },
                content_type: ExportContentType::TextPlain,
            })
            .unwrap(),
            warnings_json: serde_json::json!([]),
            warning_count: 0,
            truncated: output_truncated,
            output_expires_at: test_timestamp(),
            total_duration_ms: 0,
            query_duration_ms: 0,
            hydration_duration_ms: 0,
            render_duration_ms: 0,
            created_at: test_timestamp(),
        }
    }

    #[test]
    fn text_export_output_headers_use_persisted_truncated_column() {
        let response = render_export_task_output(text_export_output(false, true)).unwrap();

        assert_eq!(
            response
                .headers()
                .get(EXPORT_TRUNCATED_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
    }
}
