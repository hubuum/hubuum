use actix_web::{HttpRequest, Responder, get, http::StatusCode, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::task::{find_task_record, list_task_events, list_tasks};
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{TaskEventResponse, TaskKind, TaskRecord, TaskResponse, TaskStatus, User};
use crate::pagination::prepare_db_pagination;
use crate::tasks::ensure_task_worker_running;
use crate::traits::GroupMemberships;
use crate::utilities::response::{json_response, paginated_json_response};

#[derive(Debug, Default)]
struct TaskListFilters {
    kind: Option<TaskKind>,
    status: Option<TaskStatus>,
    submitted_by: Option<i32>,
}

fn parse_task_list_query(
    query_string: &str,
) -> Result<(crate::models::search::QueryOptions, TaskListFilters), ApiError> {
    let (query_options, mut passthrough) =
        parse_query_parameter_with_passthrough(query_string, &["kind", "status", "submitted_by"])?;

    let kind = match passthrough.remove("kind") {
        Some(values) if values.len() > 1 => {
            return Err(ApiError::BadRequest("duplicate kind".into()));
        }
        Some(mut values) => Some(TaskKind::from_db(values.remove(0).as_str()).map_err(|_| {
            ApiError::BadRequest(
                "invalid kind filter; expected one of import, report, export, reindex".to_string(),
            )
        })?),
        None => None,
    };

    let status = match passthrough.remove("status") {
        Some(values) if values.len() > 1 => return Err(ApiError::BadRequest("duplicate status".into())),
        Some(mut values) => Some(TaskStatus::from_db(values.remove(0).as_str()).map_err(|_| {
            ApiError::BadRequest(
                "invalid status filter; expected one of queued, validating, running, succeeded, failed, partially_succeeded, cancelled".to_string(),
            )
        })?),
        None => None,
    };

    let submitted_by = match passthrough.remove("submitted_by") {
        Some(values) if values.len() > 1 => {
            return Err(ApiError::BadRequest("duplicate submitted_by".into()));
        }
        Some(mut values) => Some(
            values
                .remove(0)
                .parse::<i32>()
                .map_err(|e| ApiError::BadRequest(format!("bad submitted_by: {e}")))?,
        ),
        None => None,
    };

    Ok((
        query_options,
        TaskListFilters {
            kind,
            status,
            submitted_by,
        },
    ))
}

async fn load_authorized_task(
    pool: &DbPool,
    requestor: &User,
    task_id: i32,
) -> Result<TaskRecord, ApiError> {
    let task = find_task_record(pool, task_id).await?;
    if task.submitted_by == Some(requestor.id) || requestor.is_admin(pool).await? {
        Ok(task)
    } else {
        // Return 404 instead of 403 to hide existence of task from unauthorized users
        Err(ApiError::NotFound("Task not found".to_string()))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks",
    tag = "tasks",
    security(("bearer_auth" = [])),
    params(
        ("kind" = String, Query, description = "Optional task kind filter (import|report|export|reindex)"),
        ("status" = String, Query, description = "Optional task status filter"),
        ("submitted_by" = i32, Query, description = "Optional submitter user id filter (effective only for admins)"),
        ("limit" = usize, Query, description = "Cursor page size"),
        ("sort" = String, Query, description = "Comma-separated sort fields. Supported fields: id, kind, status, submitted_by, created_at, started_at, finished_at. Example: kind.asc,id.desc"),
        ("cursor" = String, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Visible tasks", body = [TaskResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_tasks(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let (params, filters) = parse_task_list_query(req.query_string())?;
    let search_params = prepare_db_pagination::<TaskResponse>(&params)?;
    let is_admin = requestor.user.is_admin(&pool).await?;
    let submitted_by_filter = if is_admin {
        filters.submitted_by
    } else {
        Some(requestor.user.id)
    };

    let tasks = list_tasks(
        &pool,
        submitted_by_filter,
        filters.kind.map(TaskKind::as_str),
        filters.status.map(TaskStatus::as_str),
        &search_params,
    )
    .await?
    .into_iter()
    .map(|task| task.to_response())
    .collect::<Result<Vec<_>, _>>()?;

    paginated_json_response(tasks, StatusCode::OK, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks/{task_id}",
    tag = "tasks",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Task ID")
    ),
    responses(
        (status = 200, description = "Task state", body = TaskResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}")]
pub async fn get_task(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = load_authorized_task(&pool, &requestor.user, task_id.into_inner()).await?;
    Ok(json_response(task.to_response()?, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks/{task_id}/events",
    tag = "tasks",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Task ID")
    ),
    responses(
        (status = 200, description = "Task event history", body = [TaskEventResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/events")]
pub async fn get_task_events(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    load_authorized_task(&pool, &requestor.user, task_id).await?;
    let (params, _) = parse_query_parameter_with_passthrough(req.query_string(), &[])?;
    let search_params = prepare_db_pagination::<TaskEventResponse>(&params)?;
    let events = list_task_events(&pool, task_id, &search_params)
        .await?
        .into_iter()
        .map(TaskEventResponse::from)
        .collect::<Vec<_>>();
    paginated_json_response(events, StatusCode::OK, &params)
}
