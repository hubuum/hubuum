use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};
use base64::Engine;

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::backups::{BackupSettings, authorize_backup_request};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{
    BackupDocument, BackupOutputLookup, BackupRequest, PrincipalID, TaskID, TaskKind, TaskResponse,
    TokenID,
};
use crate::permissions::AppContext;
use crate::services::tasks::{
    TaskSubmission, backup_output, backup_output_summary, load_authorized_backup, submit_task,
    task_scope_snapshot,
};
use crate::tasks::{idempotency_key_from_headers, kick_task_worker, request_hash};

fn digest_header_from_sha256(sha256: &str) -> Result<String, ApiError> {
    let encoded = sha256.as_bytes();
    if encoded.len() != 64 {
        return Err(invalid_stored_sha256());
    }

    let mut decoded = [0_u8; 32];
    for (byte, pair) in decoded.iter_mut().zip(encoded.as_chunks::<2>().0) {
        let high = decode_hex_nibble(pair[0]).ok_or_else(invalid_stored_sha256)?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(invalid_stored_sha256)?;
        *byte = (high << 4) | low;
    }

    let digest = base64::engine::general_purpose::STANDARD.encode(decoded);
    Ok(format!("sha-256={digest}"))
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn invalid_stored_sha256() -> ApiError {
    ApiError::InternalServerError("Stored backup SHA-256 is invalid".to_string())
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
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active backup tasks", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn create_backup(
    context: AppContext,
    settings: web::Data<BackupSettings>,
    requestor: Authenticated,
    req: HttpRequest,
    request: web::Json<BackupRequest>,
) -> Result<impl Responder, ApiError> {
    let request = request.into_inner();
    authorize_backup_request(&context, &requestor.principal, requestor.scopes()).await?;
    let payload = serde_json::to_value(&request)?;
    let hash = request_hash(&payload)?;
    let scope_snapshot = task_scope_snapshot(
        Some(TokenID::new(requestor.token_meta.id().id())?),
        requestor.scopes(),
    );
    let task = submit_task(
        &context,
        TaskSubmission::new(
            TaskKind::Backup,
            PrincipalID::new(requestor.principal.id().id())?,
            payload,
            1,
            settings.max_active_tasks_per_user(),
        )
        .idempotency_key(idempotency_key_from_headers(req.headers())?)
        .request_hash(Some(hash))
        .scope_snapshot(scope_snapshot),
    )
    .await?;
    let response = task.to_response()?;
    kick_task_worker(context.clone());
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
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    authorize_backup_request(&context, &requestor.principal, requestor.scopes()).await?;
    let task_id = task_id.into_inner();
    let task = load_authorized_backup(&context, &requestor.principal, task_id).await?;
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
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<HttpResponse, ApiError> {
    let task_id = task_id.into_inner();
    authorize_backup_request(&context, &requestor.principal, requestor.scopes()).await?;
    load_authorized_backup(&context, &requestor.principal, task_id).await?;
    match backup_output(&context, task_id).await? {
        BackupOutputLookup::Available(output) => {
            let digest = digest_header_from_sha256(&output.sha256)?;
            Ok(HttpResponse::Ok()
                .content_type("application/json")
                .insert_header(("Cache-Control", "no-store"))
                .insert_header((
                    "Content-Disposition",
                    format!(
                        "attachment; filename=\"hubuum-backup-{}.json\"",
                        task_id.id()
                    ),
                ))
                .insert_header(("Digest", digest))
                .insert_header(("X-Hubuum-Backup-SHA256", output.sha256))
                .body(output.document))
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

    #[test]
    fn digest_header_decodes_stored_sha256_hex() {
        let digest = digest_header_from_sha256(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();

        assert_eq!(
            digest,
            "sha-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }

    #[test]
    fn digest_header_rejects_invalid_stored_sha256() {
        let error = digest_header_from_sha256(
            "z3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap_err();

        assert!(matches!(error, ApiError::InternalServerError(_)));
    }
}
