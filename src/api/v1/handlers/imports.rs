use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::{DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER, get_config};
use crate::db::DbPool;
use crate::db::traits::task::{TaskBackend, TaskCreateRequest, TaskScopeSnapshot};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CURRENT_IMPORT_VERSION, ImportRequest, ImportTaskResultResponse, TaskID, TaskKind, TaskRecord,
    TaskResponse,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::tasks::{
    ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker, request_hash,
};

async fn find_or_create_import_task(
    pool: &DbPool,
    submitted_by: i32,
    snapshot: TaskScopeSnapshot,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
    request_hash: String,
    total_items: i32,
) -> Result<TaskRecord, ApiError> {
    (TaskCreateRequest {
        kind: TaskKind::Import,
        submitted_by,
        idempotency_key,
        request_hash: Some(request_hash),
        request_payload: payload,
        total_items,
        submitted_token_id: snapshot.token_id,
        submitted_token_scoped: snapshot.scoped,
        submitted_token_scopes: snapshot.scopes,
    })
    .create_idempotently_with_active_limit(pool, max_active_import_tasks_per_user())
    .await
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
        (status = 429, description = "Too many active import tasks", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn create_import(
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    import_request: web::Json<ImportRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.clone());

    let import_request = import_request.into_inner();
    if import_request.version != CURRENT_IMPORT_VERSION {
        return Err(ApiError::BadRequest(format!(
            "Unsupported import version '{}'; expected {}",
            import_request.version, CURRENT_IMPORT_VERSION
        )));
    }
    let payload = serde_json::to_value(&import_request)?;
    let hash = request_hash(&payload)?;
    let idempotency_key = idempotency_key_from_headers(req.headers())?;
    let snapshot =
        TaskScopeSnapshot::from_request(Some(requestor.token_meta.id), requestor.scopes());

    let task = find_or_create_import_task(
        &pool,
        requestor.principal.id,
        snapshot,
        idempotency_key,
        payload,
        hash,
        import_request.total_items(),
    )
    .await?;

    let response = task.to_response()?;
    kick_task_worker(pool.clone());

    Ok(ApiResponse::accepted_at(
        response,
        api_locations::task(task.id)?,
    ))
}

fn max_active_import_tasks_per_user() -> usize {
    get_config()
        .map(|config| config.import_max_active_tasks_per_user)
        .unwrap_or(DEFAULT_IMPORT_MAX_ACTIVE_TASKS_PER_USER)
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
    pool: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.clone());
    let task = task_id
        .into_inner()
        .load_authorized_import(&pool, &requestor.principal)
        .await?;
    Ok(ApiResponse::new(task.to_response()?, StatusCode::OK))
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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.clone());
    let task_id = task_id.into_inner();
    task_id
        .load_authorized_import(&pool, &requestor.principal)
        .await?;
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<ImportTaskResultResponse>(&params)?;
    let (results, total_count) = task_id
        .list_import_results_with_total_count(&pool, &search_params)
        .await?;
    let results = results
        .into_iter()
        .map(ImportTaskResultResponse::from)
        .collect::<Vec<_>>();
    ApiResponse::paginated(results, total_count, &params)
}
