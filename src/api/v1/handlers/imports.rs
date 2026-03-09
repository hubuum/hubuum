use std::collections::HashMap;

use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskCreateRequest, create_generic_task, find_task_by_idempotency, find_task_record,
    list_import_results,
};
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CURRENT_IMPORT_VERSION, ImportRequest, ImportTaskResultResponse, TaskKind, TaskRecord,
    TaskResponse, User,
};
use crate::pagination::prepare_db_pagination;
use crate::tasks::{ensure_task_worker_running, kick_task_worker, request_hash};
use crate::traits::GroupMemberships;
use crate::utilities::response::{
    json_response, json_response_with_header, paginated_json_response,
};

async fn load_authorized_import_task(
    pool: &DbPool,
    requestor: &User,
    task_id: i32,
) -> Result<TaskRecord, ApiError> {
    let task = find_task_record(pool, task_id).await?;
    if task.kind != TaskKind::Import.as_str() {
        return Err(ApiError::NotFound(format!(
            "Import task {} not found",
            task_id
        )));
    }
    if task.submitted_by == Some(requestor.id) || requestor.is_admin(pool).await? {
        Ok(task)
    } else {
        // Return 404 instead of 403 to hide existence of task from unauthorized users
        Err(ApiError::NotFound("Import task not found".to_string()))
    }
}

async fn find_or_create_import_task(
    pool: &DbPool,
    submitted_by: i32,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
    request_hash: String,
    total_items: i32,
) -> Result<TaskRecord, ApiError> {
    let request_hash_for_match = request_hash.clone();
    let matches_request = |task: &TaskRecord| {
        task.kind == TaskKind::Import.as_str()
            && task.request_hash.as_deref() == Some(request_hash_for_match.as_str())
    };

    if let Some(key) = idempotency_key.as_deref()
        && let Some(existing) = find_task_by_idempotency(pool, submitted_by, key).await?
    {
        if matches_request(&existing) {
            return Ok(existing);
        }

        return Err(ApiError::Conflict(format!(
            "Idempotency-Key '{key}' is already in use for a different task submission"
        )));
    }

    let create_idempotency_key = idempotency_key.clone();

    match create_generic_task(
        pool,
        TaskCreateRequest {
            kind: TaskKind::Import,
            submitted_by,
            idempotency_key: create_idempotency_key,
            request_hash: Some(request_hash),
            request_payload: payload,
            total_items,
        },
    )
    .await
    {
        Ok(task) => Ok(task),
        Err(ApiError::Conflict(_)) => {
            if let Some(key) = idempotency_key.as_deref()
                && let Some(existing) = find_task_by_idempotency(pool, submitted_by, key).await?
                && matches_request(&existing)
            {
                return Ok(existing);
            }

            Err(ApiError::Conflict(
                "Idempotency-Key is already in use for a different task submission".to_string(),
            ))
        }
        Err(err) => Err(err),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/imports",
    tag = "imports",
    security(("bearer_auth" = [])),
    request_body = ImportRequest,
    responses(
        (status = 202, description = "Import task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn create_import(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    import_request: web::Json<ImportRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());

    let import_request = import_request.into_inner();
    if import_request.version != CURRENT_IMPORT_VERSION {
        return Err(ApiError::BadRequest(format!(
            "Unsupported import version '{}'; expected {}",
            import_request.version, CURRENT_IMPORT_VERSION
        )));
    }
    let payload = serde_json::to_value(&import_request)?;
    let hash = request_hash(&payload)?;
    let idempotency_key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let task = find_or_create_import_task(
        &pool,
        requestor.user.id,
        idempotency_key,
        payload,
        hash,
        import_request.total_items(),
    )
    .await?;

    let response = task.to_response()?;
    let mut headers = HashMap::new();
    headers.insert("Location".to_string(), format!("/api/v1/tasks/{}", task.id));
    kick_task_worker(pool.get_ref().clone());

    Ok(json_response_with_header(
        response,
        StatusCode::ACCEPTED,
        Some(headers),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/imports/{task_id}",
    tag = "imports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Import task ID")
    ),
    responses(
        (status = 200, description = "Import task projection", body = TaskResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Import task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}")]
pub async fn get_import(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = load_authorized_import_task(&pool, &requestor.user, task_id.into_inner()).await?;
    Ok(json_response(task.to_response()?, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/imports/{task_id}/results",
    tag = "imports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Import task ID")
    ),
    responses(
        (status = 200, description = "Import item results", body = [ImportTaskResultResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Import task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/results")]
pub async fn get_import_results(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    load_authorized_import_task(&pool, &requestor.user, task_id).await?;
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<ImportTaskResultResponse>(&params)?;
    let results = list_import_results(&pool, task_id, &search_params)
        .await?
        .into_iter()
        .map(ImportTaskResultResponse::from)
        .collect::<Vec<_>>();
    paginated_json_response(results, StatusCode::OK, &params)
}
