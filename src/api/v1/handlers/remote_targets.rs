use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use tracing::{debug, info};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::remote_target::{
    DeleteRemoteTargetRecord, SaveRemoteTargetRecord, UpdateRemoteTargetRecord,
};
use crate::db::traits::task::TaskCreateRequest;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{
    NamespaceID, NewRemoteTarget, Permissions, RemoteTarget, RemoteTargetID,
    RemoteTargetInvokeRequest, StoredRemoteCallTaskPayload, TaskKind, UpdateRemoteTarget,
    authorize_remote_invocation,
};
use crate::pagination::prepare_db_pagination;
use crate::tasks::{
    ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker, request_hash,
};
use crate::traits::NamespaceAccessors;
use crate::utilities::response::{
    json_response, json_response_created, json_response_with_header, paginated_json_response,
};

#[utoipa::path(
    post,
    path = "/api/v1/remote-targets",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    request_body = NewRemoteTarget,
    responses(
        (status = 201, description = "Remote target created", body = RemoteTarget),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_remote_target(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    target: web::Json<NewRemoteTarget>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let target = target.into_inner();
    can!(
        &pool,
        user,
        [Permissions::CreateRemoteTarget],
        target.namespace_id
    );

    let created: RemoteTarget = target
        .into_row()?
        .save_remote_target_record(&pool)
        .await?
        .try_into()?;
    Ok(json_response_created(
        &created,
        &format!("/api/v1/remote-targets/{}", created.id),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/remote-targets",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Remote targets visible to caller", body = [RemoteTarget]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_remote_targets(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<RemoteTarget>(&params)?;
    let allowed_namespace_ids =
        crate::models::namespace::user_can_on_any(&pool, user, Permissions::ReadRemoteTarget)
            .await?
            .into_iter()
            .map(|namespace| namespace.id)
            .collect::<Vec<_>>();
    let (targets, total_count) =
        RemoteTarget::list_with_total_count(&pool, &allowed_namespace_ids, &query_options).await?;

    paginated_json_response(targets, total_count, StatusCode::OK, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/remote-targets/{target_id}",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(("target_id" = i32, Path, description = "Remote target ID")),
    responses(
        (status = 200, description = "Remote target", body = RemoteTarget),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target not found", body = ApiErrorResponse)
    )
)]
#[get("/{target_id}")]
pub async fn get_remote_target(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    target_id: web::Path<RemoteTargetID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let target = target_id.into_inner().instance(&pool).await?;
    can!(
        &pool,
        user,
        [Permissions::ReadRemoteTarget],
        NamespaceID::new(target.namespace_id)?
    );
    Ok(json_response(target, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/remote-targets/{target_id}",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(("target_id" = i32, Path, description = "Remote target ID")),
    request_body = UpdateRemoteTarget,
    responses(
        (status = 200, description = "Remote target updated", body = RemoteTarget),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{target_id}")]
pub async fn patch_remote_target(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    target_id: web::Path<RemoteTargetID>,
    update: web::Json<UpdateRemoteTarget>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let target_id = target_id.into_inner();
    let update = update.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Remote target update must include at least one field".to_string(),
        ));
    }

    let existing = target_id.instance(&pool).await?;
    can!(
        &pool,
        user.clone(),
        [Permissions::UpdateRemoteTarget],
        NamespaceID::new(existing.namespace_id)?
    );
    if let Some(namespace_id) = update.namespace_id {
        can!(&pool, user, [Permissions::CreateRemoteTarget], namespace_id);
    }

    let row = update.into_row(&existing)?;
    let updated: RemoteTarget = row
        .update_remote_target_record(&pool, existing.id)
        .await?
        .try_into()?;
    Ok(json_response(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/remote-targets/{target_id}",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(("target_id" = i32, Path, description = "Remote target ID")),
    responses(
        (status = 204, description = "Remote target deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target not found", body = ApiErrorResponse)
    )
)]
#[delete("/{target_id}")]
pub async fn delete_remote_target(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    target_id: web::Path<RemoteTargetID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let target_id = target_id.into_inner();
    let existing = target_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        [Permissions::DeleteRemoteTarget],
        NamespaceID::new(existing.namespace_id)?
    );
    target_id.delete_remote_target_record(&pool).await?;
    Ok(actix_web::HttpResponse::NoContent().finish())
}

#[utoipa::path(
    post,
    path = "/api/v1/remote-targets/{target_id}/invoke",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(
        ("target_id" = i32, Path, description = "Remote target ID")
    ),
    request_body = RemoteTargetInvokeRequest,
    responses(
        (status = 202, description = "Remote call task accepted", body = crate::models::TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Subject or remote target not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{target_id}/invoke")]
pub async fn invoke_remote_target(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    target_id: web::Path<RemoteTargetID>,
    body: web::Json<RemoteTargetInvokeRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let user = requestor.user;
    let target_id = target_id.into_inner();
    let invoke = body.into_inner();
    let target = target_id.instance(&pool).await?;
    let resolved = authorize_remote_invocation(&pool, &user, &target, &invoke.subject).await?;

    let payload = serde_json::to_value(StoredRemoteCallTaskPayload {
        target_id,
        subject: invoke.subject,
        parameters: invoke.parameters,
        body_override: invoke.body_override,
    })?;
    let task = find_or_create_remote_call_task(
        &pool,
        user.id,
        idempotency_key_from_headers(req.headers())?,
        payload,
    )
    .await?;
    kick_task_worker(pool.get_ref().clone());

    debug!(
        message = "Remote target invocation queued",
        task_id = task.id,
        target_id = target.id,
        subject_type = resolved.subject_type.as_str(),
        subject_id = resolved.subject_id
    );

    let mut headers = std::collections::HashMap::new();
    headers.insert("Location".to_string(), format!("/api/v1/tasks/{}", task.id));
    Ok(json_response_with_header(
        task.to_response()?,
        StatusCode::ACCEPTED,
        Some(headers),
    ))
}

async fn find_or_create_remote_call_task(
    pool: &DbPool,
    submitted_by: i32,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
) -> Result<crate::models::TaskRecord, ApiError> {
    let hash = request_hash(&payload)?;
    let matches_request = |task: &crate::models::TaskRecord| {
        task.kind == TaskKind::RemoteCall.as_str()
            && task.request_hash.as_deref() == Some(hash.as_str())
    };

    if let Some(key) = idempotency_key.as_deref()
        && let Some(existing) =
            crate::models::TaskRecord::find_by_idempotency(pool, submitted_by, key).await?
    {
        if matches_request(&existing) {
            return Ok(existing);
        }
        return Err(ApiError::Conflict(format!(
            "Idempotency-Key '{key}' is already in use for a different task submission"
        )));
    }

    info!(
        message = "Creating remote call task",
        submitted_by = submitted_by
    );
    match (TaskCreateRequest {
        kind: TaskKind::RemoteCall,
        submitted_by,
        idempotency_key,
        request_hash: Some(hash),
        request_payload: payload,
        total_items: 1,
    })
    .create_generic(pool)
    .await
    {
        Ok(task) => Ok(task),
        Err(ApiError::Conflict(_)) => Err(ApiError::Conflict(
            "Idempotency-Key is already in use for a different task submission".to_string(),
        )),
        Err(error) => Err(error),
    }
}
