use crate::models::TaskDetailsExpiredResponse;
use crate::services::tasks::TaskSubmissionAttempt;
use crate::services::tasks::TaskSubmissionOperation;
use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{
    ExportContentType, ExportJsonResponse, ExportMeta, ExportOutputLookup, ExportRequest,
    ExportTaskOutput, ExportWarning, TaskID, TaskResponse, TokenID,
};
use crate::permissions::AppContext;
use crate::services::exports::{ExportTaskSubmission, submit_export_task};
use crate::services::tasks::{export_output, export_output_summary, load_authorized_export};
use crate::tasks::{ensure_task_worker_running, idempotency_key_from_headers};

const EXPORT_WARNINGS_HEADER: &str = "X-Hubuum-Export-Warnings";
const EXPORT_TRUNCATED_HEADER: &str = "X-Hubuum-Export-Truncated";

fn render_export_task_output(output: ExportTaskOutput) -> Result<HttpResponse, ApiError> {
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
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 410, description = "Submission recognized; task details expired", body = TaskDetailsExpiredResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Shared task admission or byte capacity exhausted", body = ApiErrorResponse),
        (status = 503, description = "Task identity or control reconciliation unavailable", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_export(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    export: web::Json<ExportRequest>,
) -> Result<impl Responder, ApiError> {
    let export = export.into_inner();
    let hash =
        crate::tasks::request_hash(&serde_json::json!({ "export": &export, "template_id": null }))?;
    let attempt = TaskSubmissionAttempt::prepare(
        &context,
        &requestor.principal,
        idempotency_key_from_headers(req.headers())?,
        TaskSubmissionOperation::Export,
        &export,
        Some(&hash),
    )
    .await?;
    let mut task = if let Some(task) = attempt.lookup(&context, &requestor.principal).await? {
        task
    } else {
        let submission = ExportTaskSubmission::for_token(
            export,
            TokenID::new(requestor.token_meta.id().id())?,
            requestor.scopes(),
            attempt,
        );
        submit_export_task(&context, &requestor.principal, submission).await?
    };
    super::tasks::discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;

    let output = crate::services::tasks::export_output_summary(
        &context,
        crate::models::TaskID::new(task.id)?,
    )
    .await?;
    let response = task.to_response_with_export_output(output.as_ref())?;

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
    context: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let task_id = task_id.into_inner();
    let mut task = load_authorized_export(&context, &requestor.principal, task_id).await?;
    super::tasks::discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    let output = export_output_summary(&context, task_id).await?;
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
        (status = 403, description = "Token scope does not cover the stored export", body = ApiErrorResponse),
        (status = 404, description = "Export output not found", body = ApiErrorResponse),
        (status = 410, description = "Export output expired", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/output")]
pub async fn get_export_output(
    context: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let task_id = task_id.into_inner();
    let mut task = load_authorized_export(&context, &requestor.principal, task_id).await?;
    super::tasks::discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    if !task.discovery_authorized {
        return Err(ApiError::NotFound("Export output not found".into()));
    }
    match export_output(&context, &task, requestor.scopes()).await? {
        ExportOutputLookup::Available(output) => {
            if let Some(reference) = output.artifact_reference {
                let stream = crate::services::artifacts::download(&context, reference);
                Ok(HttpResponse::Ok()
                    .content_type(output.content_type)
                    .insert_header((
                        EXPORT_WARNINGS_HEADER,
                        serde_json::from_value::<Vec<ExportWarning>>(output.warnings_json)?
                            .len()
                            .to_string(),
                    ))
                    .insert_header((EXPORT_TRUNCATED_HEADER, output.truncated.to_string()))
                    .insert_header(("Cache-Control", "no-store"))
                    .streaming(stream))
            } else {
                render_export_task_output(output)
            }
        }
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

    fn text_export_output(meta_truncated: bool, output_truncated: bool) -> ExportTaskOutput {
        ExportTaskOutput {
            artifact_reference: None,
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
            truncated: output_truncated,
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
