use actix_web::{HttpRequest, Responder, get, http::StatusCode, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::{QueryOptions, parse_query_parameter_with_passthrough};
use crate::models::{
    BackupOutputLookup, ExportOutputLookup, TaskEventResponse, TaskID, TaskKind, TaskResponse,
    TaskStatus,
};
use crate::pagination::{
    count_query_options, known_count_or_skipped, paginate_in_memory, prepare_db_pagination,
};
use crate::permissions::AppContext;
use crate::permissions::{PermissionDecision, PrincipalRef};
use crate::services::tasks::{
    backup_output_summary, export_output_summary, list_backup_output_summaries,
    list_export_output_summaries, list_task_events, list_tasks, load_authorized_task,
    task_resource,
};
use crate::tasks::ensure_task_worker_running;

#[derive(Debug, Default)]
struct TaskListFilters {
    kind: Option<TaskKind>,
    status: Option<TaskStatus>,
    submitted_by: Option<i32>,
}

fn parse_task_list_query(query_string: &str) -> Result<(QueryOptions, TaskListFilters), ApiError> {
    let (query_options, mut passthrough) =
        parse_query_parameter_with_passthrough(query_string, &["kind", "status", "submitted_by"])?;

    let kind = match passthrough.remove("kind") {
        Some(values) if values.len() > 1 => {
            return Err(ApiError::BadRequest("duplicate kind".into()));
        }
        Some(mut values) => Some(TaskKind::from_db(values.remove(0).as_str()).map_err(|_| {
            ApiError::BadRequest(
                "invalid kind filter; expected one of import, export, backup, reindex, remote_call"
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
        ("kind" = String, Query, description = "Optional task kind filter (import|export|backup|reindex|remote_call)"),
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
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let (params, filters) = parse_task_list_query(req.query_string())?;
    let search_params = prepare_db_pagination::<TaskResponse>(&params)?;
    let backend = context.permission_backend();
    let principal = PrincipalRef::load(&context, &requestor.principal).await?;
    let is_admin = backend.is_admin(&principal).await?;
    let submitted_by_filter = if is_admin {
        filters.submitted_by
    } else if backend.supports_storage_visibility_filtering() {
        Some(requestor.principal.id().id())
    } else {
        None
    };
    let (tasks, total_count) = if backend.supports_storage_visibility_filtering() {
        list_tasks(
            &context,
            submitted_by_filter,
            filters.kind,
            filters.status,
            search_params.clone(),
        )
        .await?
    } else {
        let mut candidate_options = count_query_options(&params);
        candidate_options.set_include_total(false);
        let (candidates, _) = list_tasks(
            &context,
            submitted_by_filter,
            filters.kind,
            filters.status,
            candidate_options,
        )
        .await?;
        let resources = candidates.iter().map(task_resource).collect::<Vec<_>>();
        let decisions = backend.authorize_tasks(&principal, &resources).await?;
        let authorized = candidates
            .into_iter()
            .zip(decisions)
            .filter_map(|(task, decision)| (decision == PermissionDecision::Allow).then_some(task))
            .collect::<Vec<_>>();
        let total_count = known_count_or_skipped(&params, authorized.len() as i64);
        (paginate_in_memory(authorized, &search_params)?, total_count)
    };
    let export_task_ids = tasks
        .iter()
        .filter(|task| task.kind == TaskKind::Export.as_str())
        .map(|task| task.id)
        .collect::<Vec<_>>();
    let export_outputs = list_export_output_summaries(&context, export_task_ids)
        .await?
        .into_iter()
        .map(|output| (output.task_id, output))
        .collect::<std::collections::HashMap<_, _>>();
    let backup_task_ids = tasks
        .iter()
        .filter(|task| task.kind == TaskKind::Backup.as_str())
        .map(|task| task.id)
        .collect::<Vec<_>>();
    let backup_outputs = list_backup_output_summaries(&context, backup_task_ids)
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
            let backup_output = match backup_outputs.get(&task.id) {
                Some(summary) if summary.output_expires_at > now => {
                    BackupOutputLookup::Available(summary)
                }
                Some(summary) => BackupOutputLookup::Expired {
                    expires_at: summary.output_expires_at,
                },
                None => BackupOutputLookup::Missing,
            };
            task.to_response_with_outputs(export_output, backup_output)
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
    context: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let task_id = task_id.into_inner();
    let task = load_authorized_task(&context, &requestor.principal, task_id).await?;
    let export_output = if task.kind == TaskKind::Export.as_str() {
        export_output_summary(&context, task_id).await?
    } else {
        ExportOutputLookup::Missing
    };
    let backup_output = if task.kind == TaskKind::Backup.as_str() {
        backup_output_summary(&context, task_id).await?
    } else {
        BackupOutputLookup::Missing
    };
    Ok(ApiResponse::new(
        task.to_response_with_outputs(export_output.as_ref(), backup_output.as_ref())?,
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
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(context.clone());
    let task_id = task_id.into_inner();
    load_authorized_task(&context, &requestor.principal, task_id).await?;
    let (params, _) = parse_query_parameter_with_passthrough(req.query_string(), &[])?;
    let search_params = prepare_db_pagination::<TaskEventResponse>(&params)?;
    let (events, total_count) = list_task_events(&context, task_id, search_params).await?;
    ApiResponse::paginated(events, total_count, &params)
}
