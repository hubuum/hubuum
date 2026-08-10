use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use hubuum_task_core::IdempotencyKey;
use tracing::{debug, info};

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::HistoryResponse;
use crate::can;
use crate::config::{DEFAULT_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER, get_config};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::collection::user_can_on_any;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, HistoryAuthorizationSnapshot, HubuumClassID, NewRemoteTarget, Permissions,
    PrincipalID, RemoteTarget, RemoteTargetHistory, RemoteTargetID, RemoteTargetInvokeRequest,
    StoredRemoteCallTaskPayload, TaskKind, TaskRecord, TaskResponse, TokenID, UpdateRemoteTarget,
    authorize_remote_invocation,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::{AppContext, PrincipalRef};
use crate::services::history::{
    HistoryCollectionFilter, remote_target_as_of, remote_target_history_paginated_with_total_count,
};
use crate::storage::capabilities::UserPermissions;
use crate::storage::capabilities::authz::scope_allows;
use crate::storage::capabilities::remote_target::{
    DeleteRemoteTargetRecord, SaveRemoteTargetRecord, UpdateRemoteTargetRecord,
    emit_remote_target_invoked_event,
};
use crate::storage::capabilities::task::{TaskCreateRequest, TaskScopeSnapshot};
use crate::storage::capabilities::with_revision_precondition_scope;
use crate::tasks::{
    ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker, request_hash,
};
use crate::traits::ClassAccessors;

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
    context: AppContext,
    requestor: Authenticated,
    target: web::Json<NewRemoteTarget>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target = target.into_inner();
    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::CreateRemoteTarget],
        target.collection_id
    );
    validate_remote_target_class_scope(
        &context,
        target.collection_id.id(),
        target.class_id.map(HubuumClassID::id),
    )
    .await?;

    let event_context = requestor.event_context(&req);
    let created: RemoteTarget = target
        .into_row()?
        .save_remote_target_record(&context, Some(&event_context))
        .await?
        .try_into()?;
    let location = api_locations::remote_target(created.id)?;
    ApiResponse::created_revisioned(created, location)
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
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<RemoteTarget>(&params)?;
    let visible_collections = if context
        .permission_backend()
        .supports_storage_visibility_filtering()
    {
        user_can_on_any(
            &context,
            user,
            Permissions::ReadRemoteTarget,
            requestor.scopes(),
        )
        .await?
    } else if scope_allows(requestor.scopes(), &[Permissions::ReadRemoteTarget]) {
        let principal = PrincipalRef::load(&context, user).await?;
        context
            .permission_backend()
            .collections_user_can(&principal, &[Permissions::ReadRemoteTarget])
            .await?
    } else {
        Vec::new()
    };
    let mut allowed_collection_ids = visible_collections
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
    if let Some(scope) = requestor.scopes() {
        scope.retain_allowed_collection_ids(&mut allowed_collection_ids);
    }
    let (targets, total_count) =
        RemoteTarget::list_with_total_count(&context, &allowed_collection_ids, &query_options)
            .await?;

    ApiResponse::paginated(targets, total_count, &params)
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
    context: AppContext,
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target = target_id.into_inner().instance(&context).await?;
    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::ReadRemoteTarget],
        CollectionID::new(target.collection_id)?
    );
    ApiResponse::ok_revisioned(target)
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
    context: AppContext,
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
    update: web::Json<UpdateRemoteTarget>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target_id = target_id.into_inner();
    let update = update.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Remote target update must include at least one field".to_string(),
        ));
    }

    let existing = target_id.instance(&context).await?;
    can!(
        &context,
        user.clone(),
        requestor.scopes(),
        [Permissions::UpdateRemoteTarget],
        CollectionID::new(existing.collection_id)?
    );
    if let Some(collection_id) = update.collection_id {
        can!(
            &context,
            user,
            requestor.scopes(),
            [Permissions::CreateRemoteTarget],
            collection_id
        );
    }
    let effective_collection_id = update
        .collection_id
        .map(CollectionID::id)
        .unwrap_or(existing.collection_id);
    let effective_class_id = match update.class_id {
        Some(Some(class_id)) => Some(class_id.id()),
        Some(None) => None,
        None => existing.class_id,
    };
    validate_remote_target_class_scope(&context, effective_collection_id, effective_class_id)
        .await?;

    let precondition = revision_precondition(&req, &existing)?;
    let row = update.into_row(&existing)?;
    let event_context = requestor.event_context(&req);
    let updated: RemoteTarget = with_revision_precondition_scope(
        precondition,
        row.update_remote_target_record(&context, existing.id, Some(&event_context)),
    )
    .await?
    .try_into()?;
    ApiResponse::ok_revisioned(updated)
}

async fn validate_remote_target_class_scope(
    context: &impl crate::storage::StorageContext,
    collection_id: i32,
    class_id: Option<i32>,
) -> Result<(), ApiError> {
    let Some(class_id) = class_id else {
        return Ok(());
    };
    let class = HubuumClassID::new(class_id)?.class(context).await?;
    class.ensure_in_collection(collection_id, "Remote target")
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
    context: AppContext,
    requestor: Authenticated,
    target_id: web::Path<RemoteTargetID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let target_id = target_id.into_inner();
    let existing = target_id.instance(&context).await?;
    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::DeleteRemoteTarget],
        CollectionID::new(existing.collection_id)?
    );
    let etag = existing.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition_scope(
        precondition,
        target_id.delete_remote_target_record(&context, Some(&event_context)),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
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
        (status = 202, description = "Remote call task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Subject or remote target not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{target_id}/invoke")]
pub async fn invoke_remote_target(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    target_id: web::Path<RemoteTargetID>,
    body: web::Json<RemoteTargetInvokeRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let user = &requestor.principal;
    let target_id = target_id.into_inner();
    let invoke = body.into_inner();
    let target = target_id.instance(&context).await?;
    let resolved =
        authorize_remote_invocation(&context, user, requestor.scopes(), &target, &invoke.subject)
            .await?;

    let payload = serde_json::to_value(StoredRemoteCallTaskPayload {
        target_id,
        subject: invoke.subject,
        parameters: invoke.parameters,
        body_override: invoke.body_override,
    })?;
    let snapshot = TaskScopeSnapshot::from_request(
        Some(TokenID::new(requestor.token_meta.id)?),
        requestor.scopes(),
    );
    let task = find_or_create_remote_call_task(
        &context,
        PrincipalID::new(user.id())?,
        snapshot,
        idempotency_key_from_headers(req.headers())?,
        payload,
    )
    .await?;
    let event_context = requestor.event_context(&req);
    emit_remote_target_invoked_event(
        &context,
        &target,
        &event_context,
        task.id,
        resolved.subject_type.as_str(),
        resolved.subject_id,
    )
    .await?;
    kick_task_worker(context.clone());

    debug!(
        message = "Remote target invocation queued",
        task_id = task.id,
        target_id = target.id,
        subject_type = resolved.subject_type.as_str(),
        subject_id = resolved.subject_id
    );

    Ok(ApiResponse::accepted_at(
        task.to_response()?,
        api_locations::task(task.id)?,
    ))
}

async fn find_or_create_remote_call_task(
    context: &impl crate::storage::StorageContext,
    submitted_by: PrincipalID,
    snapshot: TaskScopeSnapshot,
    idempotency_key: Option<IdempotencyKey>,
    payload: serde_json::Value,
) -> Result<TaskRecord, ApiError> {
    let hash = request_hash(&payload)?;

    info!(
        message = "Creating remote call task",
        submitted_by = submitted_by.id()
    );
    TaskCreateRequest::builder(TaskKind::RemoteCall, submitted_by, payload, 1)
        .idempotency_key(idempotency_key)
        .request_hash(Some(hash))
        .scope_snapshot(snapshot)
        .build()
        .create_idempotently_with_active_limit(context, max_active_remote_call_tasks_per_user())
        .await
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
        (status = 200, description = "Remote target history", body = [HistoryResponse<RemoteTargetHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target not found", body = ApiErrorResponse)
    )
)]
#[get("/{remote_target_id}/history")]
pub async fn get_remote_target_history(
    context: AppContext,
    requestor: Authenticated,
    remote_target_id: web::Path<RemoteTargetID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_page, can_read_deleted_history,
        history_candidate_query_options, readable_history_collection_ids,
        resolve_history_principal_names,
    };
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;

    let user = &requestor.principal;
    let remote_target_id = remote_target_id.into_inner();
    let (entity_id, require_history) = match remote_target_id.instance(&context).await {
        Ok(instance) => {
            can!(
                &context,
                user,
                requestor.scopes(),
                [Permissions::ReadRemoteTarget],
                CollectionID::new(instance.collection_id)?
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &context,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (remote_target_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<RemoteTargetHistory>(&params)?;
    let (rows, total_count) = if require_history {
        remote_target_history_paginated_with_total_count(
            entity_id,
            &context,
            &search_params,
            HistoryCollectionFilter::All,
        )
        .await?
    } else if context
        .permission_backend()
        .supports_storage_visibility_filtering()
    {
        let collection_ids = readable_history_collection_ids(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadRemoteTarget,
        )
        .await?;
        remote_target_history_paginated_with_total_count(
            entity_id,
            &context,
            &search_params,
            HistoryCollectionFilter::Visible(&collection_ids),
        )
        .await?
    } else {
        let candidate_params = history_candidate_query_options(&params);
        let (candidates, _) = remote_target_history_paginated_with_total_count(
            entity_id,
            &context,
            &candidate_params,
            HistoryCollectionFilter::All,
        )
        .await?;
        authorize_history_page(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadRemoteTarget,
            candidates,
            &search_params,
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await?
    };
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!(
            "remote target {entity_id} not found"
        )));
    }

    let principal_names = resolve_history_principal_names(&context, &rows).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| HistoryResponse::new(row, &principal_names))
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
        (status = 200, description = "Remote target version at timestamp", body = HistoryResponse<RemoteTargetHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Remote target or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{remote_target_id}/history/as-of")]
pub async fn get_remote_target_as_of(
    context: AppContext,
    requestor: Authenticated,
    remote_target_id: web::Path<RemoteTargetID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_snapshot, can_read_deleted_history, parse_as_of,
        resolve_history_principal_names,
    };

    let user = &requestor.principal;
    let remote_target_id = remote_target_id.into_inner();
    let (entity_id, deleted) = match remote_target_id.instance(&context).await {
        Ok(instance) => {
            can!(
                &context,
                user,
                requestor.scopes(),
                [Permissions::ReadRemoteTarget],
                CollectionID::new(instance.collection_id)?
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &context,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (remote_target_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = remote_target_as_of(entity_id, at, &context)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!("no version of remote target {entity_id} at {at}"))
        })?;

    if !deleted {
        authorize_history_snapshot(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadRemoteTarget,
            HistoryAuthorizationSnapshot::from(&row),
        )
        .await?;
    }

    let principal_names =
        resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new(
        HistoryResponse::new(row, &principal_names),
        StatusCode::OK,
    ))
}
