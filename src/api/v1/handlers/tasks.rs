use actix_web::{HttpRequest, Responder, get, http::StatusCode, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskBackend, list_export_task_output_summaries, list_tasks_with_total_count,
};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{
    ExportOutputLookup, TaskEventResponse, TaskID, TaskKind, TaskResponse, TaskStatus,
};
use crate::pagination::prepare_db_pagination;
use crate::tasks::ensure_task_worker_running;
use crate::traits::AuthzSubject;

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
                "invalid kind filter; expected one of import, export, reindex, remote_call"
                    .to_string(),
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

#[utoipa::path(
    get,
    path = "/api/v1/tasks",
    tag = "tasks",
    security(("bearer_auth" = [])),
    params(
        ("kind" = String, Query, description = "Optional task kind filter (import|export|reindex|remote_call)"),
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
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let (params, filters) = parse_task_list_query(req.query_string())?;
    let search_params = prepare_db_pagination::<TaskResponse>(&params)?;
    let is_admin = requestor.principal.is_admin(&pool).await?;
    let submitted_by_filter = if is_admin {
        filters.submitted_by
    } else {
        Some(requestor.principal.id)
    };
    let (tasks, total_count) = list_tasks_with_total_count(
        &pool,
        submitted_by_filter,
        filters.kind.map(TaskKind::as_str),
        filters.status.map(TaskStatus::as_str),
        &search_params,
    )
    .await?;
    let tasks = tasks.into_iter().collect::<Vec<_>>();
    let export_task_ids = tasks
        .iter()
        .filter(|task| task.kind == TaskKind::Export.as_str())
        .map(|task| task.id)
        .collect::<Vec<_>>();
    let export_outputs = list_export_task_output_summaries(&pool, &export_task_ids)
        .await?
        .into_iter()
        .map(|output| (output.task_id, output))
        .collect::<std::collections::HashMap<_, _>>();
    let now = chrono::Utc::now().naive_utc();
    let tasks = tasks
        .into_iter()
        .map(|task| {
            // Classify each summary the same way the single-task lookups do, so `output_expired`
            // is reported consistently here as on GET /tasks/{id} and GET /exports/{id}.
            let export_output = match export_outputs.get(&task.id) {
                Some(summary) if summary.output_expires_at > now => {
                    ExportOutputLookup::Available(summary)
                }
                Some(summary) => ExportOutputLookup::Expired {
                    expires_at: summary.output_expires_at,
                },
                None => ExportOutputLookup::Missing,
            };
            task.to_response_with_export_output(export_output)
        })
        .collect::<Result<Vec<_>, _>>()?;

    ApiResponse::paginated(tasks, total_count, &params)
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
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = task_id
        .into_inner()
        .load_authorized(&pool, &requestor.principal)
        .await?;
    let export_output = if task.kind == TaskKind::Export.as_str() {
        task.find_export_output_summary(&pool).await?
    } else {
        ExportOutputLookup::Missing
    };
    Ok(ApiResponse::new(
        task.to_response_with_export_output(export_output.as_ref())?,
        StatusCode::OK,
    ))
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
    requestor: Authenticated,
    req: HttpRequest,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    task_id.load_authorized(&pool, &requestor.principal).await?;
    let (params, _) = parse_query_parameter_with_passthrough(req.query_string(), &[])?;
    let search_params = prepare_db_pagination::<TaskEventResponse>(&params)?;
    let (events, total_count) = task_id
        .list_events_with_total_count(&pool, &search_params)
        .await?;
    let events = events
        .into_iter()
        .map(TaskEventResponse::from)
        .collect::<Vec<_>>();
    ApiResponse::paginated(events, total_count, &params)
}
