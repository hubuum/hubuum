use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use tracing::{debug, info};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::config::{DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER, get_config};
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::remote_target::{
    DeleteRemoteTargetRecord, SaveRemoteTargetRecord, UpdateRemoteTargetRecord,
};
use crate::db::traits::task::{TaskCreateRequest, TaskScopeSnapshot};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::namespace::user_can_on_any;
use crate::models::search::parse_query_parameter;
use crate::models::{
    HubuumClassID, NamespaceID, NewRemoteTarget, Permissions, RemoteTarget, RemoteTargetID,
    RemoteTargetInvokeRequest, StoredRemoteCallTaskPayload, TaskKind, UpdateRemoteTarget,
    authorize_remote_invocation,
};
use crate::pagination::prepare_db_pagination;
use crate::tasks::{
    ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker, request_hash,
};
use crate::traits::{ClassAccessors, NamespaceAccessors};
use crate::utilities::response::{
    json_response, json_response_created, json_response_with_header, paginated_json_response,
};

crate::history_db_fns!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    crate::schema::remote_targets_history,
    crate::models::RemoteTargetHistory
);

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
    requestor: Authenticated,
    target: web::Json<NewRemoteTarget>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target = target.into_inner();
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::CreateRemoteTarget],
        target.namespace_id
    );
    validate_remote_target_class_scope(
        &pool,
        target.namespace_id.id(),
        target.class_id.map(HubuumClassID::id),
    )
    .await?;

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
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<RemoteTarget>(&params)?;
    let allowed_namespace_ids = user_can_on_any(
        &pool,
        user,
        Permissions::ReadRemoteTarget,
        requestor.scopes(),
    )
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
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target = target_id.into_inner().instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
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
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
    update: web::Json<UpdateRemoteTarget>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
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
        requestor.scopes(),
        [Permissions::UpdateRemoteTarget],
        NamespaceID::new(existing.namespace_id)?
    );
    if let Some(namespace_id) = update.namespace_id {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateRemoteTarget],
            namespace_id
        );
    }
    let effective_namespace_id = update
        .namespace_id
        .map(NamespaceID::id)
        .unwrap_or(existing.namespace_id);
    let effective_class_id = match update.class_id {
        Some(Some(class_id)) => Some(class_id.id()),
        Some(None) => None,
        None => existing.class_id,
    };
    validate_remote_target_class_scope(&pool, effective_namespace_id, effective_class_id).await?;

    let row = update.into_row(&existing)?;
    let updated: RemoteTarget = row
        .update_remote_target_record(&pool, existing.id)
        .await?
        .try_into()?;
    Ok(json_response(updated, StatusCode::OK))
}

async fn validate_remote_target_class_scope(
    pool: &DbPool,
    namespace_id: i32,
    class_id: Option<i32>,
) -> Result<(), ApiError> {
    let Some(class_id) = class_id else {
        return Ok(());
    };
    let class = HubuumClassID::new(class_id)?.class(pool).await?;
    if class.namespace_id != namespace_id {
        return Err(ApiError::BadRequest(
            "class_id must belong to the remote target namespace".to_string(),
        ));
    }
    Ok(())
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
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target_id = target_id.into_inner();
    let existing = target_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
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
    requestor: Authenticated,
    req: HttpRequest,
    target_id: web::Path<RemoteTargetID>,
    body: web::Json<RemoteTargetInvokeRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let user = &requestor.principal;
    let target_id = target_id.into_inner();
    let invoke = body.into_inner();
    let target = target_id.instance(&pool).await?;
    let resolved =
        authorize_remote_invocation(&pool, user, requestor.scopes(), &target, &invoke.subject)
            .await?;

    let payload = serde_json::to_value(StoredRemoteCallTaskPayload {
        target_id,
        subject: invoke.subject,
        parameters: invoke.parameters,
        body_override: invoke.body_override,
    })?;
    let snapshot =
        TaskScopeSnapshot::from_request(Some(requestor.token_meta.id), requestor.scopes());
    let task = find_or_create_remote_call_task(
        &pool,
        user.id,
        snapshot,
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
    snapshot: TaskScopeSnapshot,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
) -> Result<crate::models::TaskRecord, ApiError> {
    let hash = request_hash(&payload)?;
    let request_hash_for_match = hash.clone();
    let matches_request = |task: &crate::models::TaskRecord| {
        task.kind == TaskKind::RemoteCall.as_str()
            && task.request_hash.as_deref() == Some(request_hash_for_match.as_str())
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
    let create_idempotency_key = idempotency_key.clone();

    match (TaskCreateRequest {
        kind: TaskKind::RemoteCall,
        submitted_by,
        idempotency_key: create_idempotency_key,
        request_hash: Some(hash),
        request_payload: payload,
        total_items: 1,
        submitted_token_id: snapshot.token_id,
        submitted_token_scoped: snapshot.scoped,
        submitted_token_scopes: snapshot.scopes,
    })
    .create_with_active_remote_call_limit(pool, max_active_remote_call_tasks_per_user())
    .await
    {
        Ok(task) => Ok(task),
        Err(ApiError::Conflict(_)) => {
            if let Some(key) = idempotency_key.as_deref()
                && let Some(existing) =
                    crate::models::TaskRecord::find_by_idempotency(pool, submitted_by, key).await?
                && matches_request(&existing)
            {
                return Ok(existing);
            }

            Err(ApiError::Conflict(
                "Idempotency-Key is already in use for a different task submission".to_string(),
            ))
        }
        Err(error) => Err(error),
    }
}

fn max_active_remote_call_tasks_per_user() -> usize {
    get_config()
        .map(|config| config.remote_call_max_active_tasks_per_user)
        .unwrap_or(DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER)
}

#[utoipa::path(
    get,
    path = "/api/v1/remote-targets/{remote_target_id}/history",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(("remote_target_id" = i32, Path, description = "Remote target ID")),
    responses(
        (status = 200, description = "Remote target history", body = [crate::api::v1::handlers::history::HistoryResponse<crate::models::RemoteTargetHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target not found", body = ApiErrorResponse)
    )
)]
#[get("/{remote_target_id}/history")]
pub async fn get_remote_target_history(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    remote_target_id: web::Path<RemoteTargetID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;
    use crate::utilities::response::paginated_json_mapped_response;

    let user = &requestor.principal;
    let remote_target_id = remote_target_id.into_inner();
    let (entity_id, require_history) = match remote_target_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadRemoteTarget],
                NamespaceID::new(instance.namespace_id)?
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            (remote_target_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::RemoteTargetHistory>(&params)?;
    let (rows, total_count) =
        remote_target_history_paginated_with_total_count(entity_id, &pool, &search_params).await?;
    if require_history && total_count == 0 {
        return Err(ApiError::NotFound(format!(
            "remote target {entity_id} not found"
        )));
    }

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    paginated_json_mapped_response(rows, total_count, StatusCode::OK, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/remote-targets/{remote_target_id}/history/as-of",
    tag = "remote-targets",
    security(("bearer_auth" = [])),
    params(
        ("remote_target_id" = i32, Path, description = "Remote target ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Remote target version at timestamp", body = crate::api::v1::handlers::history::HistoryResponse<crate::models::RemoteTargetHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{remote_target_id}/history/as-of")]
pub async fn get_remote_target_as_of(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    remote_target_id: web::Path<RemoteTargetID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let remote_target_id = remote_target_id.into_inner();
    let entity_id = match remote_target_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadRemoteTarget],
                NamespaceID::new(instance.namespace_id)?
            );
            instance.id
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            remote_target_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = remote_target_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!("no version of remote target {entity_id} at {at}"))
        })?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(json_response(
        HistoryResponse {
            entry: row,
            actor_username,
        },
        StatusCode::OK,
    ))
}
