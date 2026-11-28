use crate::api::response::{ApiErrorResponse, ApiResponse};
use crate::models::TaskDetailsExpiredResponse;
use crate::services::artifacts::{OutputDownload, download};
use crate::services::tasks::TaskSubmissionOperation;
use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};
use base64::{Engine, engine::general_purpose::STANDARD};

use crate::api::locations as api_locations;
use crate::errors::ApiError;
use crate::extractors::{Authenticated, UnscopedRuntimeAdmin};
use crate::models::{
    BackupDocument, BackupOutputLookup, BackupRequest, TaskID, TaskKind, TaskResponse,
};
use crate::permissions::AppContext;
use crate::services::tasks::{
    TaskSubmission, TaskSubmissionAttempt, backup_output, backup_output_summary,
    load_authorized_backup, task_scope_snapshot,
};
use crate::tasks::{idempotency_key_from_headers, request_hash};

fn render_backup_download(
    body: OutputDownload,
    task_id: TaskID,
    byte_size: i64,
    sha256: &str,
) -> Result<HttpResponse, ApiError> {
    let hex: String = body
        .sha256()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if u64::try_from(byte_size).ok() != Some(body.byte_size()) || !hex.eq_ignore_ascii_case(sha256)
    {
        return Err(ApiError::InternalServerError(
            "Stored backup metadata differs from the output".into(),
        ));
    }
    let digest = STANDARD.encode(body.sha256());
    let mut response = HttpResponse::Ok();
    response
        .content_type("application/json")
        .insert_header((
            "Content-Disposition",
            format!(
                "attachment; filename=\"hubuum-backup-{}.json\"",
                task_id.id()
            ),
        ))
        .insert_header(("Digest", format!("sha-256={digest}")))
        .insert_header(("X-Hubuum-Backup-SHA256", hex));
    Ok(body.respond(&mut response))
}

#[utoipa::path(
    post,
    path = "/api/v1/backups",
    tag = "backups",
    security(("bearer_auth" = [])),
    request_body = BackupRequest,
    responses(
        (status = 202, description = "Backup task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped administrator access required", body = ApiErrorResponse),
        (status = 410, description = "Submission recognized; no new work created and task details expired. Do not resubmit with a new key.", body = TaskDetailsExpiredResponse, headers(("Cache-Control" = String, description = "no-store; no automatic retry is implied"))),
        (status = 409, description = "Conflict; reason idempotency_key_conflict identifies a changed protected request", body = ApiErrorResponse, headers(("Cache-Control" = String, description = "no-store for protected request conflicts; no Retry-After"))),
        (status = 429, description = "Shared task admission or byte capacity exhausted", body = ApiErrorResponse),
        (status = 503, description = "Task identity or control reconciliation unavailable", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn create_backup(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    request: web::Json<BackupRequest>,
) -> Result<impl Responder, ApiError> {
    let request = request.into_inner();
    let payload = serde_json::to_value(&request)?;
    let hash = request_hash(&payload)?;
    let attempt = TaskSubmissionAttempt::prepare(
        &context,
        &requestor.principal,
        idempotency_key_from_headers(req.headers())?,
        TaskSubmissionOperation::Backup,
        &request,
        Some(&hash),
    )
    .await?;
    let task = if let Some(task) = attempt.lookup(&context, &requestor.principal).await? {
        task
    } else {
        let admin = requestor.into_runtime_admin(&context).await?;
        let scope_snapshot = task_scope_snapshot(Some(admin.token_id()), None);
        attempt
            .submit(
                &context,
                admin.principal(),
                TaskSubmission::new(TaskKind::Backup, admin.principal().id(), payload, 1)
                    .request_hash(Some(hash))
                    .scope_snapshot(scope_snapshot),
            )
            .await?
    };
    let output = backup_output_summary(&context, TaskID::new(task.id)?).await?;
    let response = task.to_response_with_backup_output(output.as_ref())?;
    Ok(ApiResponse::accepted_at(
        response,
        api_locations::task(task.id)?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/backups/{task_id}",
    tag = "backups",
    security(("bearer_auth" = [])),
    params(("task_id" = i32, Path, description = "Backup task ID")),
    responses(
        (status = 200, description = "Backup task projection", body = TaskResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped administrator access required", body = ApiErrorResponse),
        (status = 404, description = "Backup task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}")]
pub async fn get_backup(
    context: AppContext,
    admin: UnscopedRuntimeAdmin,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    let task_id = task_id.into_inner();
    let task = load_authorized_backup(&context, admin.principal(), task_id).await?;
    let output = backup_output_summary(&context, task_id).await?;
    Ok(ApiResponse::new(
        task.to_response_with_backup_output(output.as_ref())?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/backups/{task_id}/output",
    tag = "backups",
    security(("bearer_auth" = [])),
    params(("task_id" = i32, Path, description = "Backup task ID")),
    responses(
        (status = 200, description = "Stored backup document", body = BackupDocument, content_type = "application/json",
            headers(
                ("Cache-Control" = String, description = "Always no-store for credential-bearing artifacts"),
                ("Content-Disposition" = String, description = "Attachment filename for the backup document"),
                ("Content-Length" = u64, description = "Exact byte count of the complete backup document"),
                ("Content-Digest" = String, description = "RFC 9530 SHA-256 digest of the complete response content"),
                ("Digest" = String, description = "Base64-encoded SHA-256 content digest"),
                ("X-Hubuum-Backup-SHA256" = String, description = "Lowercase hexadecimal SHA-256 digest")
            )
        ),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Unscoped administrator access required", body = ApiErrorResponse),
        (status = 404, description = "Backup output not found", body = ApiErrorResponse),
        (status = 410, description = "Backup output expired", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/output")]
pub async fn get_backup_output(
    context: AppContext,
    admin: UnscopedRuntimeAdmin,
    task_id: web::Path<TaskID>,
) -> Result<HttpResponse, ApiError> {
    let task_id = task_id.into_inner();
    load_authorized_backup(&context, admin.principal(), task_id).await?;
    match backup_output(&context, task_id).await? {
        BackupOutputLookup::Available(output) => {
            let body = match output.artifact_reference {
                Some(reference) => download(&context, reference).await?,
                None => OutputDownload::inline(
                    output
                        .document
                        .ok_or_else(|| {
                            ApiError::InternalServerError("Missing backup document".into())
                        })?
                        .into(),
                ),
            };
            render_backup_download(body, task_id, output.byte_size, &output.sha256)
        }
        BackupOutputLookup::Expired { expires_at } => Err(ApiError::Gone(format!(
            "Backup output expired at {expires_at} UTC"
        ))),
        BackupOutputLookup::Missing => {
            Err(ApiError::NotFound("Backup output not found".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn empty_backup_digest_headers_identify_the_same_bytes() {
        let body = OutputDownload::inline(web::Bytes::new());
        let response =
            render_backup_download(body, TaskID::new(1).unwrap(), 0, EMPTY_SHA256).unwrap();
        let digest = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        assert_eq!(
            response.headers().get("Digest").unwrap(),
            &format!("sha-256={digest}")
        );
        assert_eq!(
            response.headers().get("Content-Digest").unwrap(),
            &format!("sha-256=:{digest}:")
        );
        assert_eq!(response.headers().get("Content-Length").unwrap(), "0");
    }

    #[rstest]
    #[case::wrong_size(1, EMPTY_SHA256)]
    #[case::wrong_digest(0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    #[case::malformed_digest(0, "invalid")]
    fn inconsistent_backup_metadata_fails_before_headers(
        #[case] length: i64,
        #[case] digest: &str,
    ) {
        let body = OutputDownload::inline(web::Bytes::new());
        assert!(matches!(
            render_backup_download(body, TaskID::new(1).unwrap(), length, digest),
            Err(ApiError::InternalServerError(_))
        ));
    }
}
