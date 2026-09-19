pub(crate) mod discovery;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use hubuum_query::decode_query_parameter_pairs;
use hubuum_storage_core::{StorageTaskKind, StorageTaskSearch, StorageTaskStatus, TaskTimeRange};
use hubuum_task_core::TaskStopReason;

use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::{QueryOptions, parse_query_parameter_with_passthrough};
use crate::models::{
    BackupOutputLookup, ExportOutputLookup, TaskEventResponse, TaskID, TaskKind, TaskRecord,
    TaskResponse, TaskStatus,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::permissions::visibility::filter_authorized_cursor_page_from_storage;
use crate::permissions::{PermissionDecision, PrincipalRef};
use crate::services::tasks::{
    backup_output_summary, export_output_summary, list_backup_output_summaries,
    list_export_output_summaries, list_task_events, list_tasks, load_authorized_task,
    task_resource,
};
use crate::tasks::ensure_task_worker_running;

#[derive(Debug, Default)]
struct TaskListFilters {
    search: StorageTaskSearch,
    submitted_by: Option<i32>,
}

fn parse_task_list_query(query_string: &str) -> Result<(QueryOptions, TaskListFilters), ApiError> {
    const FILTERS: &[&str] = &[
        "kind",
        "status",
        "terminal",
        "submitted_by",
        "created_after",
        "created_before",
        "started_after",
        "started_before",
        "finished_after",
        "finished_before",
        "cancel_requested",
        "terminal_reason",
        "trace_id",
    ];
    let (query_options, passthrough) = parse_query_parameter_with_passthrough(
        query_string,
        &[FILTERS, discovery::FILTERS].concat(),
    )?;
    let mut seen = HashSet::new();
    for (key, _) in decode_query_parameter_pairs(query_string)? {
        if !seen.insert(key.clone()) {
            return Err(ApiError::BadRequest(format!("duplicate {key}")));
        }
    }
    if !query_options.filters().is_empty() {
        return Err(ApiError::BadRequest("Unsupported task filter".into()));
    }
    let mut values = HashMap::new();
    for (key, mut entries) in passthrough {
        if entries.len() != 1 {
            return Err(ApiError::BadRequest(format!("duplicate {key}")));
        }
        values.insert(key, entries.remove(0));
    }
    let invalid = |name: &str| ApiError::BadRequest(format!("invalid {name} filter"));
    let kinds = values
        .get("kind")
        .map(|s| {
            s.split(',')
                .map(|v| StorageTaskKind::from_persisted(v).ok_or_else(|| invalid("kind")))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let statuses = values
        .get("status")
        .map(|s| {
            s.split(',')
                .map(|v| StorageTaskStatus::from_persisted(v).ok_or_else(|| invalid("status")))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let boolean = |name: &str| {
        values
            .get(name)
            .map(|v| v.parse::<bool>().map_err(|_| invalid(name)))
            .transpose()
    };
    let range = |prefix: &str| -> Result<TaskTimeRange, ApiError> {
        let timestamp = |suffix: &str| {
            let key = format!("{prefix}_{suffix}");
            values
                .get(&key)
                .map(|v| {
                    DateTime::parse_from_rfc3339(v)
                        .map(|v| v.with_timezone(&Utc))
                        .map_err(|_| invalid(&key))
                })
                .transpose()
        };
        TaskTimeRange::try_new(timestamp("after")?, timestamp("before")?)
            .map_err(|e| ApiError::BadRequest(e.to_string()))
    };
    let reason = values
        .get("terminal_reason")
        .map(|v| match v.as_str() {
            "cancel_requested" => Ok(TaskStopReason::Cancelled),
            "deadline_exceeded" => Ok(TaskStopReason::DeadlineExceeded),
            _ => Err(invalid("terminal_reason")),
        })
        .transpose()?;
    let search = StorageTaskSearch::default()
        .lifecycle(kinds, statuses, boolean("terminal")?)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?
        .time_ranges(range("created")?, range("started")?, range("finished")?)
        .operations(boolean("cancel_requested")?, reason)
        .with_trace_id(values.get("trace_id").cloned())
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let search = search
        .discovering(discovery::parse(&values)?)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let submitted_by = values
        .get("submitted_by")
        .map(|v| v.parse::<i32>().map_err(|_| invalid("submitted_by")))
        .transpose()?;
    if submitted_by.is_some_and(|id| id <= 0) {
        return Err(invalid("submitted_by"));
    }
    Ok((
        query_options,
        TaskListFilters {
            search,
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
        ("class_id" = Option<i32>, Query, description = "Explicit target class"),
        ("object_id" = Option<i32>, Query, description = "Explicit target object"),
        ("collection_id" = Option<i32>, Query, description = "Explicit target collection"),
        ("relation_type" = Option<String>, Query, description = "class_relation or object_relation; requires relation_id"),
        ("relation_id" = Option<i32>, Query, description = "Explicit relation identity; requires relation_type"),
        ("schema_revision" = Option<i64>, Query, description = "Target schema revision; requires class_id"),
        ("schema_work_kind" = Option<String>, Query, description = "Retained work kind: impact or revalidation"),
        ("schema_work_status" = Option<String>, Query, description = "Retained work status: running, failed, complete, cancelled, superseded"),
        ("computation_revision" = Option<i64>, Query, description = "Target computation revision; requires class_id"),
        ("remote_target_id" = Option<i32>, Query, description = "Remote target configuration identity"),
        ("remote_side_effect_state" = Option<String>, Query, description = "not_sent, possibly_sent or legacy_unknown"),
        ("export_scope_kind" = Option<String>, Query, description = "Captured export scope kind"),
        ("export_template_id" = Option<i32>, Query, description = "Resolved export template identity"),
        ("export_has_warnings" = Option<bool>, Query, description = "Known export warning outcome"),
        ("export_truncated" = Option<bool>, Query, description = "Known export truncation outcome"),
        ("import_dry_run" = Option<bool>, Query, description = "Captured import dry run option"),
        ("import_atomicity" = Option<String>, Query, description = "strict or best_effort"),
        ("import_collision_policy" = Option<String>, Query, description = "abort or overwrite"),
        ("import_permission_policy" = Option<String>, Query, description = "abort or continue"),
        ("import_has_failed_items" = Option<bool>, Query, description = "Known terminal import failure count is nonzero"),
        ("backup_include_history" = Option<bool>, Query, description = "Captured backup history option"),
        ("output_state" = Option<String>, Query, description = "available, expired, not_produced or unknown; exports and backups"),
        ("kind" = Option<String>, Query, description = "Comma-separated task kinds (import|export|backup|reindex|remote_call|schema_validation; schema tasks require administrator access)"),
        ("status" = Option<String>, Query, description = "Comma-separated task statuses"),
        ("terminal" = Option<bool>, Query, description = "Restrict to terminal or nonterminal states; must agree with status"),
        ("cancel_requested" = Option<bool>, Query, description = "Match durable cancellation intent"),
        ("terminal_reason" = Option<String>, Query, description = "cancel_requested or deadline_exceeded"),
        ("trace_id" = Option<String>, Query, description = "32 hexadecimal digits identifying the originating trace"),
        ("created_after" = Option<String>, Query, description = "RFC 3339 created timestamp; inclusive lower bound"),
        ("created_before" = Option<String>, Query, description = "RFC 3339 created timestamp; exclusive upper bound"),
        ("started_after" = Option<String>, Query, description = "RFC 3339 started timestamp; inclusive lower bound"),
        ("started_before" = Option<String>, Query, description = "RFC 3339 started timestamp; exclusive upper bound"),
        ("finished_after" = Option<String>, Query, description = "RFC 3339 finished timestamp; inclusive lower bound"),
        ("finished_before" = Option<String>, Query, description = "RFC 3339 finished timestamp; exclusive upper bound"),
        ("submitted_by" = Option<i32>, Query, description = "Optional submitter user id filter (effective only for admins)"),
        ("limit" = Option<usize>, Query, description = "Cursor page size"),
        ("sort" = Option<String>, Query, description = "Comma-separated sort fields. Supported fields: id, kind, status, submitted_by, created_at, started_at, finished_at. Example: kind.asc,id.desc"),
        ("cursor" = Option<String>, Query, description = "Cursor token from X-Next-Cursor")
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
    discovery::authorize_filters(&context, &requestor, &filters.search).await?;
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
    let (mut tasks, total_count) = if backend.supports_storage_visibility_filtering() {
        list_tasks(
            &context,
            submitted_by_filter,
            filters.search.clone(),
            (!is_admin || requestor.scopes().is_some()).then_some(TaskKind::SchemaValidation),
            search_params.clone(),
        )
        .await?
    } else {
        let page = filter_authorized_cursor_page_from_storage(
            backend,
            &params,
            |options| async {
                list_tasks(
                    &context,
                    submitted_by_filter,
                    filters.search.clone(),
                    (!is_admin || requestor.scopes().is_some())
                        .then_some(TaskKind::SchemaValidation),
                    options,
                )
                .await
                .map(|page| page.0)
            },
            |candidates: Vec<TaskRecord>| {
                let principal = &principal;
                async move {
                    let resources = candidates.iter().map(task_resource).collect::<Vec<_>>();
                    let decisions = backend.authorize_tasks(principal, &resources).await?;
                    if decisions.len() != candidates.len() {
                        return Err(ApiError::InternalServerError(
                            "Permission backend returned an unexpected number of task decisions"
                                .to_string(),
                        ));
                    }
                    Ok(candidates
                        .into_iter()
                        .zip(decisions)
                        .filter_map(|(task, decision)| {
                            (decision == PermissionDecision::Allow).then_some(task)
                        })
                        .collect())
                }
            },
        )
        .await?;
        (page.rows, page.total_count)
    };
    discovery::redact(&context, &requestor, &mut tasks).await?;
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
    let evaluated_at = filters
        .search
        .discovery()
        .map_or_else(chrono::Utc::now, |d| d.evaluated_at());
    let now = evaluated_at.naive_utc();
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
            task.to_response_with_outputs_at(export_output, backup_output, evaluated_at)
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
    let mut task = load_authorized_task(&context, &requestor.principal, task_id).await?;
    discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    if task.kind == TaskKind::SchemaValidation.as_str() && requestor.scopes().is_some() {
        return Err(ApiError::NotFound("Task not found".into()));
    }
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
    let task = load_authorized_task(&context, &requestor.principal, task_id).await?;
    if task.kind == TaskKind::SchemaValidation.as_str() && requestor.scopes().is_some() {
        return Err(ApiError::NotFound("Task not found".into()));
    }
    let (params, _) = parse_query_parameter_with_passthrough(req.query_string(), &[])?;
    let search_params = prepare_db_pagination::<TaskEventResponse>(&params)?;
    let (events, total_count) = list_task_events(&context, task_id, search_params).await?;
    ApiResponse::paginated(events, total_count, &params)
}

#[utoipa::path(
    post, path = "/api/v1/tasks/{task_id}/cancel", tag = "tasks",
    security(("bearer_auth" = [])),
    params(("task_id" = TaskID, Path, description = "Task ID")),
    request_body = crate::models::TaskCancelRequest,
    responses(
        (status = 200, description = "Terminal task; repeated requests return the same state", body = TaskResponse),
        (status = 202, description = "Durable cancellation requested; executor cleanup is pending", body = TaskResponse),
        (status = 400, description = "Invalid cancellation reason or status", body = ApiErrorResponse),
        (status = 401, description = "Unauthenticated", body = ApiErrorResponse),
        (status = 403, description = "Cancellation is not authorized", body = ApiErrorResponse),
        (status = 404, description = "Task not found", body = ApiErrorResponse),
        (status = 409, description = "Expected status no longer matches", body = ApiErrorResponse)
    )
)]
#[post("/{task_id}/cancel")]
pub async fn cancel_task(
    context: AppContext,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
    body: web::Json<crate::models::TaskCancelRequest>,
    request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let task_id = task_id.into_inner();
    let mut task = crate::services::tasks::cancel_task(
        &context,
        &requestor,
        task_id,
        body.into_inner(),
        requestor.event_context(&request),
    )
    .await?;
    discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    let status = if TaskStatus::from_db(&task.status)?.is_terminal() {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
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
        status,
    ))
}

#[cfg(test)]
mod search_tests {
    use super::parse_task_list_query;
    use rstest::rstest;

    #[rstest]
    #[case("kind=import,export&status=succeeded,failed&terminal=true")]
    #[case("terminal=false")]
    #[case("created_after=2026-01-01T00:00:00Z&created_before=2026-01-02T01:00:00%2B01:00")]
    #[case("cancel_requested=false&terminal_reason=deadline_exceeded")]
    #[case("trace_id=ABCDEF1234567890abcdef1234567890")]
    fn accepts_search(#[case] query: &str) {
        assert!(parse_task_list_query(query).is_ok());
    }

    #[rstest]
    #[case("kind=")]
    #[case("kind=import,")]
    #[case("kind=import&kind=export")]
    #[case("sort=id.asc&sort=kind.asc")]
    #[case("sort=id.asc&%73ort=kind.asc")]
    #[case("status=queued&terminal=true")]
    #[case("status=cancelled&terminal=false")]
    #[case("status=succeeded,queued&terminal=true")]
    #[case("terminal=1")]
    #[case("terminal=true&terminal=false")]
    #[case("created_after=2026-01-01T00:00:00")]
    #[case("started_after=2026-01-02T00:00:00Z&started_before=2026-01-01T00:00:00Z")]
    #[case("finished_after=2026-01-01T00:00:00Z&finished_before=2026-01-01T00:00:00Z")]
    #[case("cancel_requested=unknown")]
    #[case("terminal_reason=succeeded")]
    #[case("trace_id=00000000000000000000000000000000")]
    #[case("trace_id=oops")]
    #[case("submitted_by=0")]
    #[case("name=anything")]
    #[case("unsupported=true")]
    fn rejects_invalid_search(#[case] query: &str) {
        assert!(parse_task_list_query(query).is_err());
    }
}
