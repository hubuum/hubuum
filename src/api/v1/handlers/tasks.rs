use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::task::{find_task_record, list_task_events};
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{TaskEventResponse, TaskRecord, TaskResponse, User};
use crate::pagination::prepare_db_pagination;
use crate::traits::GroupMemberships;
use crate::utilities::response::{json_response, paginated_json_response};
use crate::utilities::tasks::ensure_task_worker_running;

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
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<TaskEventResponse>(&params)?;
    let events = list_task_events(&pool, task_id, &search_params)
        .await?
        .into_iter()
        .map(TaskEventResponse::from)
        .collect::<Vec<_>>();
    paginated_json_response(events, StatusCode::OK, &params)
}
