use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::JsonResponse;
use crate::can;
use crate::config::{
    DEFAULT_REPORT_DB_STATEMENT_TIMEOUT_MS, DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER,
    DEFAULT_REPORT_MAX_OUTPUT_BYTES, DEFAULT_REPORT_OUTPUT_RETENTION_HOURS,
    DEFAULT_REPORT_STAGE_TIMEOUT_MS, DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS, get_config,
};
use crate::db::traits::UserPermissions;
use crate::db::traits::task::{TaskBackend, TaskCreateRequest, TaskScopeSnapshot, TaskStateUpdate};
use crate::db::{DbPool, with_statement_timeout_scope};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, SearchOperator, StatementTimeoutMs,
    parse_query_parameter,
};
use crate::models::{
    ClassIdSet, HubuumClassID, HubuumClassRelation, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectWithPath, NamespaceID, NamespaceReportTemplates,
    NewReportTaskOutputRecord, NewTaskEventRecord, Permissions, RELATED_INCLUDE_DEFAULT_LIMIT,
    RELATED_INCLUDE_DEFAULT_MAX_DEPTH, ReportContentType, ReportIncludeRelatedDirection,
    ReportIncludeRelatedQuery, ReportIncludeRelatedSort, ReportJsonResponse, ReportMeta,
    ReportMissingDataPolicy, ReportOutputLookup, ReportRequest, ReportScope, ReportScopeKind,
    ReportTaskOutputRecord, ReportTemplate, ReportTemplateID, ReportWarning, TaskID, TaskKind,
    TaskRecord, TaskResponse,
};
use crate::pagination::page_limits_or_defaults;
use crate::tasks::{
    ensure_task_worker_running, idempotency_key_from_headers, kick_task_worker, request_hash,
};
use crate::traits::{AuthzSubject, NamespaceAccessors, SelfAccessors};
use crate::utilities::reporting::{SizeLimitedWriter, render_template};

use super::check_if_object_in_class;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredReportTaskPayload {
    report: ReportRequest,
    template_id: Option<i32>,
}

struct ReportArtifact {
    content_type: ReportContentType,
    json_output: Option<ReportJsonResponse>,
    text_output: Option<String>,
    meta: ReportMeta,
    warnings: Vec<ReportWarning>,
    template_name: Option<String>,
    timings: ReportExecutionTimings,
}

const REPORT_WARNINGS_HEADER: &str = "X-Hubuum-Report-Warnings";
const REPORT_TRUNCATED_HEADER: &str = "X-Hubuum-Report-Truncated";

struct ReportRuntime {
    report: ReportRequest,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    template: Option<ReportTemplate>,
    namespace_templates: Vec<ReportTemplate>,
}

struct ReportExecution {
    items: Vec<serde_json::Value>,
    warnings: Vec<ReportWarning>,
    meta: ReportMeta,
    template_items: Vec<serde_json::Value>,
    source: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ReportExecutionTimings {
    total_duration_ms: i32,
    query_duration_ms: i32,
    hydration_duration_ms: i32,
    render_duration_ms: i32,
}

#[derive(Debug, Clone, Serialize)]
struct HydratedTemplateObject {
    id: i32,
    name: String,
    namespace_id: i32,
    hubuum_class_id: i32,
    data: serde_json::Value,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    path: Vec<i32>,
    path_objects: Vec<HydratedTemplatePathObject>,
    related: BTreeMap<String, Vec<HydratedTemplateObject>>,
    reachable: BTreeMap<String, Vec<HydratedTemplateObject>>,
    paths: BTreeMap<String, Vec<HydratedTemplateObject>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HydratedTemplatePathObject {
    id: i32,
    name: String,
    namespace_id: i32,
    hubuum_class_id: i32,
}

struct RelationHydrationPlan {
    depth_limit: i32,
    enabled_for_scope: bool,
}

struct ObjectNeighborhood {
    objects_by_id: BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    class_relations_by_pair: BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_names_by_id: BTreeMap<i32, String>,
}

struct HydrationBudget {
    max_objects: usize,
    hydrated_objects: usize,
}

impl HydrationBudget {
    fn new(max_objects: usize) -> Self {
        Self {
            max_objects,
            hydrated_objects: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.max_objects.saturating_sub(self.hydrated_objects)
    }

    fn remaining_related_capacity(&self) -> Result<usize, ApiError> {
        if self.remaining() == 0 {
            return Err(ApiError::BadRequest(format!(
                "Hydrated template object limit exceeded ({} >= {})",
                self.hydrated_objects, self.max_objects
            )));
        }

        Ok(self.remaining().saturating_sub(1))
    }

    fn count_object(&mut self) -> Result<(), ApiError> {
        self.hydrated_objects = self.hydrated_objects.saturating_add(1);
        if self.hydrated_objects > self.max_objects {
            return Err(ApiError::BadRequest(format!(
                "Hydrated template object limit exceeded ({} > {})",
                self.hydrated_objects, self.max_objects
            )));
        }

        Ok(())
    }
}

// Reproduces the per-root capacity check the old per-root query path applied:
// `remaining_related_capacity()` reserves one slot for the root, the query fetched
// `cap + 1` rows, and a root over `cap` errored with the fetched count (`cap + 1`).
// Roots are processed in `items` order so the shared budget shrinks exactly as before.
fn take_related_within_budget(
    budget: &HydrationBudget,
    mut related: Vec<HubuumObjectWithPath>,
) -> Result<Vec<HubuumObjectWithPath>, ApiError> {
    let max_related_objects = budget.remaining_related_capacity()?;
    related.truncate(max_related_objects.saturating_add(1));
    if related.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related.len(),
            max_related_objects
        )));
    }
    Ok(related)
}

#[derive(Debug, Clone)]
struct ReachableTemplateTarget {
    target_id: i32,
    path: Vec<i32>,
    remaining_depth: i32,
}

#[utoipa::path(
    post,
    path = "/api/v1/reports",
    tag = "reports",
    security(("bearer_auth" = [])),
    request_body = ReportRequest,
    responses(
        (status = 202, description = "Report task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active report tasks", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_report(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    report: web::Json<ReportRequest>,
) -> Result<impl Responder, ApiError> {
    let report = report.into_inner();
    let task = submit_report_task(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        Some(requestor.token_meta.id),
        req,
        report,
        None,
    )
    .await?;

    let response = task.to_response()?;
    let mut headers = HashMap::new();
    headers.insert(
        "Location".to_string(),
        api_locations::task(task.id)?.as_str().to_string(),
    );
    kick_task_worker(pool.get_ref().clone());

    Ok(JsonResponse::with_headers(
        response,
        StatusCode::ACCEPTED,
        Some(headers),
    ))
}

pub(crate) async fn submit_report_task<S: AuthzSubject>(
    pool: &DbPool,
    subject: &S,
    // Scope boundary of the submitting token, persisted as the task scope
    // snapshot so async execution cannot exceed it.
    scopes: Option<&[Permissions]>,
    submitted_token_id: Option<i32>,
    req: HttpRequest,
    report: ReportRequest,
    template: Option<ReportTemplate>,
) -> Result<TaskRecord, ApiError> {
    ensure_task_worker_running(pool.clone());

    let task_payload = StoredReportTaskPayload {
        report,
        template_id: template.as_ref().map(|template| template.id),
    };
    let payload = serde_json::to_value(&task_payload)?;
    let hash = request_hash(&payload)?;
    let idempotency_key = idempotency_key_from_headers(req.headers())?;

    let runtime = prepare_report_runtime(pool, task_payload.report.clone(), template).await?;
    validate_report_submission(&runtime)?;
    let task_payload = runtime_to_task_payload(&runtime)?;

    let snapshot = TaskScopeSnapshot::from_request(submitted_token_id, scopes);

    find_or_create_report_task(
        pool,
        subject.principal_id(),
        snapshot,
        idempotency_key,
        serde_json::to_value(task_payload)?,
        hash,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/reports/{task_id}",
    tag = "reports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Report task ID")
    ),
    responses(
        (status = 200, description = "Report task projection", body = TaskResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Report task not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}")]
pub async fn get_report(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = task_id
        .into_inner()
        .load_authorized_report(&pool, &requestor.principal)
        .await?;
    let output = task.find_report_output_summary(&pool).await?;
    Ok(JsonResponse::new(
        task.to_response_with_report_output(output.as_ref())?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/reports/{task_id}/output",
    tag = "reports",
    security(("bearer_auth" = [])),
    params(
        ("task_id" = i32, Path, description = "Report task ID")
    ),
    responses(
        (
            status = 200,
            description = "Stored report output",
            content(
                (ReportJsonResponse = "application/json"),
                (String = "text/plain"),
                (String = "text/html"),
                (String = "text/csv")
            )
        ),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Report output not found", body = ApiErrorResponse),
        (status = 410, description = "Report output expired", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/output")]
pub async fn get_report_output(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    task_id: web::Path<TaskID>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    task_id
        .load_authorized_report(&pool, &requestor.principal)
        .await?;
    match task_id.find_report_output(&pool).await? {
        ReportOutputLookup::Available(output) => render_report_task_output(output),
        ReportOutputLookup::Expired { expires_at } => Err(ApiError::Gone(format!(
            "Report output expired at {expires_at} UTC"
        ))),
        ReportOutputLookup::Missing => {
            Err(ApiError::NotFound("Report output not found".to_string()))
        }
    }
}

async fn prepare_report_runtime(
    pool: &DbPool,
    report: ReportRequest,
    template: Option<ReportTemplate>,
) -> Result<ReportRuntime, ApiError> {
    report.scope.validate()?;
    validate_report_include(&report)?;

    let namespace_templates = match &template {
        Some(template) => {
            NamespaceID::new(template.namespace_id)?
                .report_templates(pool, None)
                .await?
        }
        None => Vec::new(),
    };
    let content_type = template
        .as_ref()
        .map(|template| template.content_type)
        .unwrap_or(ReportContentType::ApplicationJson);

    Ok(ReportRuntime {
        content_type,
        missing_data_policy: report
            .missing_data_policy
            .unwrap_or(ReportMissingDataPolicy::Strict),
        template,
        namespace_templates,
        report,
    })
}

async fn resolve_template(
    pool: &DbPool,
    subject: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
    template_id: Option<i32>,
) -> Result<Option<ReportTemplate>, ApiError> {
    let Some(template_id) = template_id else {
        return Ok(None);
    };

    let template = ReportTemplateID::new(template_id)?.instance(pool).await?;
    can!(
        pool,
        subject,
        scopes,
        [Permissions::ReadTemplate],
        NamespaceID::new(template.namespace_id)?
    );

    Ok(Some(template))
}

fn validate_report_submission(runtime: &ReportRuntime) -> Result<(), ApiError> {
    if runtime.report.relation_context.is_some()
        && runtime
            .report
            .include
            .as_ref()
            .and_then(|include| include.related_objects.as_ref())
            .is_some_and(|related_objects| !related_objects.is_empty())
    {
        return Err(ApiError::BadRequest(
            "include.related_objects cannot be combined with relation_context".to_string(),
        ));
    }

    let mut query_options = prepare_query_options(&runtime.report)?;
    let _ = resolve_relation_hydration_plan(runtime, &mut query_options)?;
    Ok(())
}

fn runtime_to_task_payload(runtime: &ReportRuntime) -> Result<StoredReportTaskPayload, ApiError> {
    validate_report_submission(runtime)?;
    Ok(StoredReportTaskPayload {
        report: runtime.report.clone(),
        template_id: runtime.template.as_ref().map(|template| template.id),
    })
}

async fn find_or_create_report_task(
    pool: &DbPool,
    submitted_by: i32,
    snapshot: TaskScopeSnapshot,
    idempotency_key: Option<String>,
    payload: serde_json::Value,
    request_hash_value: String,
) -> Result<TaskRecord, ApiError> {
    let request_hash_for_match = request_hash_value.clone();
    let matches_request = |task: &TaskRecord| {
        task.kind == TaskKind::Report.as_str()
            && task.request_hash.as_deref() == Some(request_hash_for_match.as_str())
    };

    if let Some(key) = idempotency_key.as_deref()
        && let Some(existing) = TaskRecord::find_by_idempotency(pool, submitted_by, key).await?
    {
        if matches_request(&existing) {
            return Ok(existing);
        }

        return Err(ApiError::Conflict(format!(
            "Idempotency-Key '{key}' is already in use for a different task submission"
        )));
    }

    match (TaskCreateRequest {
        kind: TaskKind::Report,
        submitted_by,
        idempotency_key: idempotency_key.clone(),
        request_hash: Some(request_hash_value),
        request_payload: payload,
        total_items: 1,
        submitted_token_id: snapshot.token_id,
        submitted_token_scoped: snapshot.scoped,
        submitted_token_scopes: snapshot.scopes,
    })
    .create_with_active_report_limit(pool, max_active_report_tasks_per_user())
    .await
    {
        Ok(task) => Ok(task),
        Err(ApiError::Conflict(_)) => {
            if let Some(key) = idempotency_key.as_deref()
                && let Some(existing) =
                    TaskRecord::find_by_idempotency(pool, submitted_by, key).await?
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

pub(crate) async fn execute_report_task(
    pool: &DbPool,
    task: &TaskRecord,
    subject: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
) -> Result<(), ApiError> {
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Report task payload is missing".to_string()))?;
    let payload: StoredReportTaskPayload = serde_json::from_value(payload)?;
    let template = resolve_template(pool, subject, scopes, payload.template_id).await?;
    let runtime = prepare_report_runtime(pool, payload.report, template).await?;
    validate_report_submission(&runtime)?;
    let total_start = Instant::now();
    let mut timings = ReportExecutionTimings::default();

    NewTaskEventRecord {
        task_id: task.id,
        event_type: "running".to_string(),
        message: "Report execution started".to_string(),
        data: None,
    }
    .append(pool)
    .await?;
    task.update_state(
        pool,
        TaskStateUpdate {
            status: crate::models::TaskStatus::Running,
            summary: None,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
    )
    .await?;

    NewTaskEventRecord {
        task_id: task.id,
        event_type: "running".to_string(),
        message: "Query execution started".to_string(),
        data: Some(serde_json::json!({
            "scope": runtime.report.scope.kind.as_str(),
            "content_type": runtime.content_type.as_mime(),
        })),
    }
    .append(pool)
    .await?;

    // Report-scoped, in-flight query budget. While these query stages run, every
    // DB query they issue is bounded by this `statement_timeout` (applied as a
    // transaction-local `SET LOCAL`), independently of the pool-global timeout
    // and without affecting bookkeeping writes outside these scopes.
    let statement_timeout = report_statement_timeout();
    let mut query_options = prepare_query_options(&runtime.report)?;
    let relation_hydration = resolve_relation_hydration_plan(&runtime, &mut query_options)?;
    let query_start = Instant::now();
    let (items, mut warnings, truncated) = with_statement_timeout_scope(
        statement_timeout,
        execute_scope(pool, subject, scopes, &runtime.report.scope, query_options),
    )
    .await?;
    let mut items = items;
    with_statement_timeout_scope(
        statement_timeout,
        apply_report_includes(pool, subject, scopes, &runtime.report, &mut items),
    )
    .await?;
    timings.query_duration_ms = duration_to_millis_i32(query_start.elapsed());
    enforce_report_stage_timeout(query_start, "query execution")?;

    if relation_hydration
        .as_ref()
        .is_some_and(|plan| plan.enabled_for_scope)
    {
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "running".to_string(),
            message: "Hydrating relation-aware template context".to_string(),
            data: relation_hydration.as_ref().map(|plan| {
                serde_json::json!({
                    "depth_limit": plan.depth_limit,
                })
            }),
        }
        .append(pool)
        .await?;
    }

    add_truncation_warning(&mut warnings, truncated);
    let hydration_start = Instant::now();
    let (template_items, source) = with_statement_timeout_scope(
        statement_timeout,
        build_template_items(pool, subject, scopes, &runtime, &items, relation_hydration),
    )
    .await?;
    timings.hydration_duration_ms = duration_to_millis_i32(hydration_start.elapsed());
    enforce_report_stage_timeout(hydration_start, "relation hydration")?;
    let template_report = runtime.template.is_some();
    let item_count = if template_report {
        template_items.len()
    } else {
        items.len()
    };
    let execution_items = if template_report {
        drop(items);
        Vec::new()
    } else {
        items
    };
    let execution = ReportExecution {
        meta: ReportMeta {
            count: item_count,
            truncated,
            scope: runtime.report.scope.clone(),
            content_type: runtime.content_type,
        },
        items: execution_items,
        template_items,
        warnings,
        source,
    };

    NewTaskEventRecord {
        task_id: task.id,
        event_type: "running".to_string(),
        message: "Rendering report output".to_string(),
        data: None,
    }
    .append(pool)
    .await?;

    let render_start = Instant::now();
    let artifact = build_report_artifact(&runtime, execution, timings)?;
    let mut timings = artifact.timings;
    timings.render_duration_ms = duration_to_millis_i32(render_start.elapsed());
    timings.total_duration_ms = duration_to_millis_i32(total_start.elapsed());
    enforce_report_stage_timeout(render_start, "template rendering")?;
    log_report_stage_metrics(task.id, &runtime, timings);
    let artifact = ReportArtifact {
        timings,
        ..artifact
    };

    NewTaskEventRecord {
        task_id: task.id,
        event_type: "running".to_string(),
        message: "Persisting report output".to_string(),
        data: None,
    }
    .append(pool)
    .await?;

    task.finalize_report_with_output(
        pool,
        TaskStateUpdate {
            status: crate::models::TaskStatus::Succeeded,
            summary: Some("Report completed successfully".to_string()),
            processed_items: 1,
            success_items: 1,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: crate::models::TaskStatus::Succeeded.as_str().to_string(),
            message: format!(
                "Report completed successfully in {:?}",
                total_start.elapsed()
            ),
            data: Some(serde_json::json!({
                "content_type": artifact.content_type.as_mime(),
                "template_name": artifact.template_name.clone(),
                "warning_count": artifact.warnings.len(),
                "truncated": artifact.meta.truncated,
                "total_duration_ms": artifact.timings.total_duration_ms,
                "query_duration_ms": artifact.timings.query_duration_ms,
                "hydration_duration_ms": artifact.timings.hydration_duration_ms,
                "render_duration_ms": artifact.timings.render_duration_ms,
            })),
        },
        artifact_to_output_record(task.id, artifact)?,
    )
    .await?;

    Ok(())
}

fn artifact_to_output_record(
    task_id: i32,
    artifact: ReportArtifact,
) -> Result<NewReportTaskOutputRecord, ApiError> {
    let retention_hours = get_config()
        .map(|config| config.report_output_retention_hours)
        .unwrap_or(DEFAULT_REPORT_OUTPUT_RETENTION_HOURS);
    let output_expires_at =
        chrono::Utc::now().naive_utc() + chrono::Duration::hours(retention_hours);
    Ok(NewReportTaskOutputRecord {
        task_id,
        template_name: artifact.template_name,
        content_type: artifact.content_type.as_mime().to_string(),
        json_output: artifact.json_output.map(serde_json::to_value).transpose()?,
        text_output: artifact.text_output,
        meta_json: serde_json::to_value(&artifact.meta)?,
        warnings_json: serde_json::to_value(&artifact.warnings)?,
        warning_count: i32::try_from(artifact.warnings.len()).unwrap_or(i32::MAX),
        truncated: artifact.meta.truncated,
        output_expires_at,
        total_duration_ms: artifact.timings.total_duration_ms,
        query_duration_ms: artifact.timings.query_duration_ms,
        hydration_duration_ms: artifact.timings.hydration_duration_ms,
        render_duration_ms: artifact.timings.render_duration_ms,
    })
}

fn validate_report_include(report: &ReportRequest) -> Result<(), ApiError> {
    let Some(include) = &report.include else {
        return Ok(());
    };

    let Some(related_objects) = &include.related_objects else {
        return Ok(());
    };

    if !related_objects.is_empty() && report.scope.kind != ReportScopeKind::ObjectsInClass {
        return Err(ApiError::BadRequest(
            "include.related_objects is only supported for scope 'objects_in_class'".to_string(),
        ));
    }

    include.validate_related_objects()
}

fn add_truncation_warning(warnings: &mut Vec<ReportWarning>, truncated: bool) {
    if truncated {
        warnings.push(ReportWarning {
            code: "truncated".to_string(),
            message: "The report was truncated to the configured max_items limit".to_string(),
            path: None,
        });
    }
}

fn build_report_artifact(
    runtime: &ReportRuntime,
    execution: ReportExecution,
    timings: ReportExecutionTimings,
) -> Result<ReportArtifact, ApiError> {
    match runtime.content_type {
        ReportContentType::ApplicationJson => {
            build_json_report_artifact(runtime, execution, timings)
        }
        ReportContentType::TextPlain | ReportContentType::TextHtml | ReportContentType::TextCsv => {
            build_text_report_artifact(runtime, execution, timings)
        }
    }
}

fn build_json_report_artifact(
    runtime: &ReportRuntime,
    execution: ReportExecution,
    timings: ReportExecutionTimings,
) -> Result<ReportArtifact, ApiError> {
    let response = ReportJsonResponse {
        items: execution.items,
        meta: execution.meta.clone(),
        warnings: execution.warnings.clone(),
    };

    enforce_json_output_limit(&response, &runtime.report)?;

    Ok(ReportArtifact {
        content_type: ReportContentType::ApplicationJson,
        json_output: Some(response),
        text_output: None,
        meta: execution.meta,
        warnings: execution.warnings,
        template_name: None,
        timings,
    })
}

fn build_text_report_artifact(
    runtime: &ReportRuntime,
    execution: ReportExecution,
    timings: ReportExecutionTimings,
) -> Result<ReportArtifact, ApiError> {
    let template = required_template(runtime, runtime.content_type)?;
    let context = report_template_context(&runtime.report, &execution);
    let max_output_bytes = runtime
        .report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_report_max_output_bytes);
    let (rendered, template_warnings) = render_template(
        template,
        &runtime.namespace_templates,
        &context,
        runtime.content_type,
        runtime.missing_data_policy,
        max_output_bytes,
    )?;
    let mut warnings = execution.warnings;
    warnings.extend(template_warnings);

    Ok(ReportArtifact {
        content_type: runtime.content_type,
        json_output: None,
        text_output: Some(rendered),
        meta: execution.meta,
        warnings,
        template_name: Some(template.name.clone()),
        timings,
    })
}

fn required_template(
    runtime: &ReportRuntime,
    content_type: ReportContentType,
) -> Result<&ReportTemplate, ApiError> {
    runtime.template.as_ref().ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Output type '{}' requires running an executable report template",
            content_type.as_mime()
        ))
    })
}

fn render_report_task_output(output: ReportTaskOutputRecord) -> Result<HttpResponse, ApiError> {
    let content_type = ReportContentType::from_mime(&output.content_type)?;
    let _meta: ReportMeta = serde_json::from_value(output.meta_json)?;
    let warnings: Vec<ReportWarning> = serde_json::from_value(output.warnings_json)?;
    let warning_count = warnings.len();
    let truncated = output.truncated;

    match content_type {
        ReportContentType::ApplicationJson => {
            let response: ReportJsonResponse =
                serde_json::from_value(output.json_output.ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Stored report JSON output is missing".to_string(),
                    )
                })?)?;
            let mut http_response = HttpResponse::build(StatusCode::OK);
            for (key, value) in report_headers(warning_count, truncated) {
                http_response.insert_header((key, value));
            }
            Ok(http_response.json(response))
        }
        ReportContentType::TextPlain | ReportContentType::TextHtml | ReportContentType::TextCsv => {
            let mut response = HttpResponse::build(StatusCode::OK);
            response.content_type(content_type.as_mime());
            for (key, value) in report_headers(warning_count, truncated) {
                response.insert_header((key, value));
            }
            Ok(response.body(output.text_output.ok_or_else(|| {
                ApiError::InternalServerError("Stored report text output is missing".to_string())
            })?))
        }
    }
}

fn duration_to_millis_i32(duration: std::time::Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

/// The report-scoped Postgres `statement_timeout` to apply to report queries,
/// or `None` when disabled (`report_db_statement_timeout_ms == 0`).
///
/// This is the in-flight, server-side query cancel that complements the
/// post-completion wall-clock budget enforced by [`enforce_report_stage_timeout`].
fn report_statement_timeout() -> Option<StatementTimeoutMs> {
    let milliseconds = get_config()
        .map(|config| config.report_db_statement_timeout_ms)
        .unwrap_or(DEFAULT_REPORT_DB_STATEMENT_TIMEOUT_MS);
    StatementTimeoutMs::new(milliseconds)
}

/// Post-completion rejection guard for a report stage.
///
/// This is **not** an in-flight interrupt: it is called after a stage has already
/// finished and rejects the report if the stage took longer than the configured
/// budget. It bounds how long a stage is *accepted* to have taken, not how long
/// it is *allowed to run*. In-flight protection comes from the MiniJinja fuel
/// budget, `report_template_max_objects`, the output byte caps, the pool-global
/// `db_statement_timeout_ms`, and the report-scoped `report_db_statement_timeout_ms`
/// (both of which cancel slow queries server-side).
fn enforce_report_stage_timeout(stage_start: Instant, stage_name: &str) -> Result<(), ApiError> {
    let stage_timeout_ms = get_config()
        .map(|config| config.report_stage_timeout_ms)
        .unwrap_or(DEFAULT_REPORT_STAGE_TIMEOUT_MS);
    let elapsed = stage_start.elapsed();
    if elapsed.as_millis() > u128::from(stage_timeout_ms) {
        return Err(ApiError::BadRequest(format!(
            "Report {stage_name} exceeded the configured time budget ({}ms > {}ms)",
            elapsed.as_millis(),
            stage_timeout_ms
        )));
    }
    Ok(())
}

fn log_report_stage_metrics(
    task_id: i32,
    runtime: &ReportRuntime,
    timings: ReportExecutionTimings,
) {
    tracing::info!(
        message = "Report execution timings recorded",
        task_id = task_id,
        scope = runtime.report.scope.kind.as_str(),
        content_type = runtime.content_type.as_mime(),
        template_name = runtime
            .template
            .as_ref()
            .map(|template| template.name.as_str()),
        total_duration_ms = timings.total_duration_ms,
        query_duration_ms = timings.query_duration_ms,
        hydration_duration_ms = timings.hydration_duration_ms,
        render_duration_ms = timings.render_duration_ms
    );
}

fn report_template_context(
    report: &ReportRequest,
    execution: &ReportExecution,
) -> serde_json::Value {
    json!({
        "items": &execution.template_items,
        "meta": &execution.meta,
        "warnings": &execution.warnings,
        "request": report,
        "source": &execution.source,
    })
}

fn prepare_query_options(report: &ReportRequest) -> Result<QueryOptions, ApiError> {
    let mut query_options = parse_query_parameter(report.query.as_deref().unwrap_or_default())?;
    if query_options.cursor.is_some() {
        return Err(ApiError::BadRequest(
            "Reports do not support cursor pagination".to_string(),
        ));
    }

    validate_report_limits(report)?;

    let (default_page_limit, max_page_limit) = page_limits_or_defaults();
    let configured_limit = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_items)
        .unwrap_or(default_page_limit);
    let requested_limit = query_options.limit.unwrap_or(configured_limit);
    let effective_limit = requested_limit.min(configured_limit).min(max_page_limit);

    query_options.limit = Some(effective_limit.saturating_add(1));
    Ok(query_options)
}

fn validate_report_limits(report: &ReportRequest) -> Result<(), ApiError> {
    let Some(limits) = &report.limits else {
        return Ok(());
    };

    if let Some(0) = limits.max_items {
        return Err(ApiError::BadRequest(
            "max_items must be greater than 0".to_string(),
        ));
    }

    if let Some(0) = limits.max_output_bytes {
        return Err(ApiError::BadRequest(
            "max_output_bytes must be greater than 0".to_string(),
        ));
    }

    if let Some(max_output_bytes) = limits.max_output_bytes {
        let server_max_output_bytes = configured_report_max_output_bytes();
        if max_output_bytes > server_max_output_bytes {
            return Err(ApiError::BadRequest(format!(
                "max_output_bytes ({max_output_bytes}) exceeds server maximum ({server_max_output_bytes})"
            )));
        }
    }

    Ok(())
}

fn resolve_relation_hydration_plan(
    runtime: &ReportRuntime,
    query_options: &mut QueryOptions,
) -> Result<Option<RelationHydrationPlan>, ApiError> {
    let has_template = runtime.template.is_some();
    let scope = &runtime.report.scope;
    let relation_context = runtime.report.relation_context.as_ref();

    if relation_context.is_some() && !has_template {
        return Err(ApiError::BadRequest(
            "relation_context is only supported for templated text reports".to_string(),
        ));
    }

    if relation_context.is_some()
        && !matches!(
            scope.kind,
            ReportScopeKind::ObjectsInClass | ReportScopeKind::RelatedObjects
        )
    {
        return Err(ApiError::BadRequest(
            "relation_context is only supported for objects_in_class and related_objects reports"
                .to_string(),
        ));
    }

    if !has_template {
        return Ok(None);
    }

    match scope.kind {
        ReportScopeKind::ObjectsInClass => {
            let Some(context) = relation_context else {
                return Ok(None);
            };
            Ok(Some(RelationHydrationPlan {
                depth_limit: validate_relation_depth(context.depth.unwrap_or(2))?,
                enabled_for_scope: true,
            }))
        }
        ReportScopeKind::RelatedObjects => {
            let depth_limit = validate_relation_depth(
                relation_context
                    .and_then(|context| context.depth)
                    .unwrap_or(2),
            )?;
            query_options
                .filters
                .retain(|filter| filter.field != FilterField::Depth);
            query_options.filters.push(ParsedQueryParam::new(
                "depth",
                Some(SearchOperator::Lte { is_negated: false }),
                &depth_limit.to_string(),
            )?);
            Ok(Some(RelationHydrationPlan {
                depth_limit,
                enabled_for_scope: true,
            }))
        }
        _ => Ok(None),
    }
}

fn validate_relation_depth(depth: i32) -> Result<i32, ApiError> {
    if !(1..=2).contains(&depth) {
        return Err(ApiError::BadRequest(
            "Templated relation hydration only supports depth 1 or 2".to_string(),
        ));
    }
    Ok(depth)
}

async fn build_template_items(
    pool: &DbPool,
    user: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
    runtime: &ReportRuntime,
    items: &[serde_json::Value],
    relation_hydration: Option<RelationHydrationPlan>,
) -> Result<(Vec<serde_json::Value>, Option<serde_json::Value>), ApiError> {
    if runtime.template.is_none() {
        return Ok((items.to_vec(), None));
    }

    let Some(relation_hydration) = relation_hydration else {
        return Ok((items.to_vec(), None));
    };

    if !relation_hydration.enabled_for_scope {
        return Ok((items.to_vec(), None));
    }

    let mut hydration_budget = HydrationBudget::new(max_hydrated_template_objects());

    match runtime.report.scope.kind {
        ReportScopeKind::ObjectsInClass => {
            let roots = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObject>)
                .collect::<Result<Vec<_>, _>>()?;
            if roots.is_empty() {
                return Ok((Vec::new(), None));
            }

            let root_ids = roots.iter().map(|root| root.id).collect::<Vec<_>>();
            let per_root_cap = i32::try_from(max_hydrated_template_objects()).unwrap_or(i32::MAX);
            let related_rows = user
                .bidirectionally_related_objects_for_roots(
                    pool,
                    &root_ids,
                    relation_hydration.depth_limit,
                    per_root_cap,
                    scopes,
                )
                .await?;

            // Descendants grouped per root, preserving the query's per-root ordering.
            let mut related_by_root: BTreeMap<i32, Vec<HubuumObjectWithPath>> =
                root_ids.iter().map(|id| (*id, Vec::new())).collect();
            for row in &related_rows {
                if let Some(list) = related_by_root.get_mut(&row.root_object_id) {
                    list.push(row.to_descendant_object_with_path());
                }
            }

            // One relations fetch over the union of all roots + descendants.
            let mut all_object_ids = root_ids.clone();
            for row in &related_rows {
                all_object_ids.push(row.descendant_object_id);
            }
            all_object_ids.sort_unstable();
            all_object_ids.dedup();
            let all_relations = user
                .search_object_relations_between_ids(pool, &all_object_ids, scopes)
                .await?;

            // One class-metadata fetch over every object in the report.
            let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                all_objects.insert(root_with_path.id, root_with_path);
            }
            for row in &related_rows {
                let object = row.to_descendant_object_with_path();
                all_objects.entry(object.id).or_insert(object);
            }
            let class_metadata = load_hydration_class_metadata(pool, &all_objects).await?;

            let mut hydrated_items = Vec::with_capacity(roots.len());
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                let related = related_by_root.remove(&root.id).unwrap_or_default();
                let related = take_related_within_budget(&hydration_budget, related)?;

                let mut neighborhood_ids = related
                    .iter()
                    .map(|object| object.id)
                    .collect::<std::collections::HashSet<_>>();
                neighborhood_ids.insert(root.id);
                let relations = all_relations
                    .iter()
                    .filter(|relation| {
                        neighborhood_ids.contains(&relation.from_hubuum_object_id)
                            && neighborhood_ids.contains(&relation.to_hubuum_object_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                let neighborhood = build_object_neighborhood(
                    root_with_path.clone(),
                    related,
                    relations,
                    &class_metadata,
                )?;
                let hydrated = hydrate_object(
                    &neighborhood,
                    root_with_path.id,
                    vec![root_with_path.id],
                    relation_hydration.depth_limit,
                    &mut hydration_budget,
                )?;
                hydrated_items.push(serde_json::to_value(hydrated)?);
            }

            Ok((hydrated_items, None))
        }
        ReportScopeKind::RelatedObjects => {
            let source_object = HubuumObjectID::new(runtime.report.scope.object_id_required()?)?
                .instance(pool)
                .await?;
            let source = object_with_root_path(&source_object);
            let related_objects = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObjectWithPath>)
                .collect::<Result<Vec<_>, _>>()?;
            let hydrated = hydrate_related_root(
                pool,
                user,
                scopes,
                source,
                related_objects,
                relation_hydration.depth_limit,
                &mut hydration_budget,
            )
            .await?;
            let source = serde_json::to_value(&hydrated)?;
            Ok((vec![source.clone()], Some(source)))
        }
        _ => Ok((items.to_vec(), None)),
    }
}

async fn hydrate_related_root(
    pool: &DbPool,
    user: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
    source: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    depth_limit: i32,
    hydration_budget: &mut HydrationBudget,
) -> Result<HydratedTemplateObject, ApiError> {
    let max_related_objects = hydration_budget.remaining_related_capacity()?;
    if related_objects.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related_objects.len(),
            max_related_objects
        )));
    }

    let object_ids = std::iter::once(source.id)
        .chain(related_objects.iter().map(|object| object.id))
        .collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(pool, &object_ids, scopes)
        .await?;

    let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
    all_objects.insert(source.id, source.clone());
    for object in &related_objects {
        all_objects
            .entry(object.id)
            .or_insert_with(|| object.clone());
    }
    let class_metadata = load_hydration_class_metadata(pool, &all_objects).await?;

    let neighborhood =
        build_object_neighborhood(source.clone(), related_objects, relations, &class_metadata)?;
    hydrate_object(
        &neighborhood,
        source.id,
        vec![source.id],
        depth_limit,
        hydration_budget,
    )
}

struct HydrationClassMetadata {
    class_names: BTreeMap<i32, String>,
    class_relations_by_object_class: BTreeMap<i32, Vec<HubuumClassRelation>>,
}

// One-shot replacement for the per-root ensure_class_names + seed_alias DB work.
// Loads every class relation touching any object class once (sorted by id so the
// normalized-pair last-write-wins is deterministic), and primes class names for both
// object classes AND every relation endpoint class (the adjacent class name is needed
// by relation_alias_for_viewer even when no object of that class is in a neighborhood).
async fn load_hydration_class_metadata(
    pool: &DbPool,
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
) -> Result<HydrationClassMetadata, ApiError> {
    let object_class_ids =
        ClassIdSet::new(objects_by_id.values().map(|object| object.hubuum_class_id))?;

    let mut class_relations = object_class_ids.load_relations_touching(pool).await?;
    class_relations.sort_by_key(|relation| relation.id);

    let mut class_relations_by_object_class = BTreeMap::<i32, Vec<HubuumClassRelation>>::new();
    let mut name_ids = object_class_ids.as_slice().to_vec();
    for relation in &class_relations {
        name_ids.push(relation.from_hubuum_class_id);
        name_ids.push(relation.to_hubuum_class_id);
        if object_class_ids
            .as_slice()
            .binary_search(&relation.from_hubuum_class_id)
            .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.from_hubuum_class_id)
                .or_default()
                .push(relation.clone());
        }
        if relation.to_hubuum_class_id != relation.from_hubuum_class_id
            && object_class_ids
                .as_slice()
                .binary_search(&relation.to_hubuum_class_id)
                .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.to_hubuum_class_id)
                .or_default()
                .push(relation.clone());
        }
    }

    let mut class_names = BTreeMap::new();
    ensure_class_name_ids(pool, &name_ids, &mut class_names).await?;

    Ok(HydrationClassMetadata {
        class_names,
        class_relations_by_object_class,
    })
}

fn build_object_neighborhood(
    root: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    relations: Vec<HubuumObjectRelation>,
    class_metadata: &HydrationClassMetadata,
) -> Result<ObjectNeighborhood, ApiError> {
    let mut objects_by_id = BTreeMap::new();
    objects_by_id.insert(root.id, root);
    for object in related_objects {
        objects_by_id.insert(object.id, object);
    }

    let class_names = &class_metadata.class_names;

    let mut aliases_by_object_id = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, Vec<i32>>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut alias_owners = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, i32>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut class_relations_by_pair = BTreeMap::new();

    seed_alias_buckets(
        &objects_by_id,
        &mut aliases_by_object_id,
        &mut alias_owners,
        &mut class_relations_by_pair,
        &class_metadata.class_relations_by_object_class,
        class_names,
    )?;

    for relation in relations {
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            class_names,
            relation.from_hubuum_object_id,
            relation.to_hubuum_object_id,
        )?;
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            class_names,
            relation.to_hubuum_object_id,
            relation.from_hubuum_object_id,
        )?;
    }

    for alias_map in aliases_by_object_id.values_mut() {
        for ids in alias_map.values_mut() {
            ids.sort_unstable_by(|left, right| {
                let left_object = &objects_by_id[left];
                let right_object = &objects_by_id[right];
                left_object
                    .name
                    .cmp(&right_object.name)
                    .then_with(|| left.cmp(right))
            });
            ids.dedup();
        }
    }

    Ok(ObjectNeighborhood {
        objects_by_id,
        aliases_by_object_id,
        class_relations_by_pair,
        class_names_by_id: class_names.clone(),
    })
}

fn seed_alias_buckets(
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &mut BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_relations_by_object_class: &BTreeMap<i32, Vec<HubuumClassRelation>>,
    class_names: &BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    for object in objects_by_id.values() {
        let Some(class_relations) = class_relations_by_object_class.get(&object.hubuum_class_id)
        else {
            continue;
        };
        for relation in class_relations {
            class_relations_by_pair.insert(
                relation_pair_key(relation.from_hubuum_class_id, relation.to_hubuum_class_id),
                relation.clone(),
            );
            let adjacent_class_id = if relation.from_hubuum_class_id == object.hubuum_class_id {
                relation.to_hubuum_class_id
            } else {
                relation.from_hubuum_class_id
            };

            let alias = relation_alias_for_viewer(
                relation,
                object.hubuum_class_id,
                adjacent_class_id,
                class_names,
            )?;
            let alias_owner_map = alias_owners.get_mut(&object.id).ok_or_else(|| {
                ApiError::InternalServerError("Missing alias ownership state".to_string())
            })?;
            if let Some(existing_class_id) = alias_owner_map.get(&alias)
                && *existing_class_id != adjacent_class_id
            {
                return Err(ApiError::BadRequest(format!(
                    "Relation alias collision for object '{}' on alias '{}'",
                    object.name, alias
                )));
            }
            alias_owner_map.insert(alias.clone(), adjacent_class_id);
            aliases_by_object_id
                .get_mut(&object.id)
                .ok_or_else(|| {
                    ApiError::InternalServerError("Missing alias grouping state".to_string())
                })?
                .entry(alias)
                .or_default();
        }
    }

    Ok(())
}

async fn ensure_class_name_ids(
    pool: &DbPool,
    class_ids: &[i32],
    class_names: &mut BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    let missing = ClassIdSet::new(
        class_ids
            .iter()
            .copied()
            .filter(|class_id| !class_names.contains_key(class_id)),
    )?;

    if missing.is_empty() {
        return Ok(());
    }

    for (class_id, class_name) in missing.load_names(pool).await? {
        class_names.insert(class_id, class_name);
    }

    for class_id in missing.as_slice() {
        if !class_names.contains_key(class_id) {
            return Err(ApiError::NotFound(format!("Class {class_id} not found")));
        }
    }

    Ok(())
}

fn add_bidirectional_alias_edge(
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_names: &BTreeMap<i32, String>,
    from_object_id: i32,
    to_object_id: i32,
) -> Result<(), ApiError> {
    let Some(from_object) = objects_by_id.get(&from_object_id) else {
        return Ok(());
    };
    let Some(to_object) = objects_by_id.get(&to_object_id) else {
        return Ok(());
    };
    let alias = reachable_alias_for_classes(
        class_relations_by_pair,
        class_names,
        from_object.hubuum_class_id,
        to_object.hubuum_class_id,
    )?;

    let alias_owner_map = alias_owners.get_mut(&from_object.id).ok_or_else(|| {
        ApiError::InternalServerError("Missing alias ownership state".to_string())
    })?;
    if let Some(existing_class_id) = alias_owner_map.get(&alias)
        && *existing_class_id != to_object.hubuum_class_id
    {
        return Err(ApiError::BadRequest(format!(
            "Relation alias collision for object '{}' on alias '{}'",
            from_object.name, alias
        )));
    }
    alias_owner_map.insert(alias.clone(), to_object.hubuum_class_id);

    aliases_by_object_id
        .get_mut(&from_object.id)
        .ok_or_else(|| ApiError::InternalServerError("Missing alias grouping state".to_string()))?
        .entry(alias)
        .or_default()
        .push(to_object.id);
    Ok(())
}

fn inferred_relation_alias(class_name: &str) -> String {
    pluralize_alias(&normalize_alias_segment(class_name))
}

fn relation_pair_key(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn relation_alias_for_viewer(
    relation: &crate::models::HubuumClassRelation,
    viewer_class_id: i32,
    adjacent_class_id: i32,
    class_names: &BTreeMap<i32, String>,
) -> Result<String, ApiError> {
    if viewer_class_id == relation.from_hubuum_class_id
        && adjacent_class_id == relation.to_hubuum_class_id
        && let Some(alias) = relation.forward_template_alias.as_deref()
    {
        return Ok(alias.to_string());
    }
    if viewer_class_id == relation.to_hubuum_class_id
        && adjacent_class_id == relation.from_hubuum_class_id
        && let Some(alias) = relation.reverse_template_alias.as_deref()
    {
        return Ok(alias.to_string());
    }

    Ok(inferred_relation_alias(
        class_names.get(&adjacent_class_id).ok_or_else(|| {
            ApiError::InternalServerError(
                "Missing adjacent class name while hydrating relations".to_string(),
            )
        })?,
    ))
}

fn reachable_alias_for_classes(
    class_relations_by_pair: &BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_names: &BTreeMap<i32, String>,
    source_class_id: i32,
    target_class_id: i32,
) -> Result<String, ApiError> {
    if let Some(relation) =
        class_relations_by_pair.get(&relation_pair_key(source_class_id, target_class_id))
    {
        return relation_alias_for_viewer(relation, source_class_id, target_class_id, class_names);
    }

    Ok(inferred_relation_alias(
        class_names.get(&target_class_id).ok_or_else(|| {
            ApiError::InternalServerError(
                "Missing class name while hydrating relations".to_string(),
            )
        })?,
    ))
}

fn normalize_alias_segment(class_name: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = true;

    for character in class_name.chars() {
        if character.is_ascii_alphanumeric() {
            if character.is_ascii_uppercase()
                && !previous_was_separator
                && !normalized.ends_with('_')
            {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !normalized.ends_with('_') && !normalized.is_empty() {
            normalized.push('_');
            previous_was_separator = true;
        }
    }

    normalized.trim_matches('_').to_string()
}

fn pluralize_alias(alias: &str) -> String {
    if alias.ends_with('y')
        && alias.len() > 1
        && !matches!(
            alias.chars().nth(alias.len() - 2),
            Some('a' | 'e' | 'i' | 'o' | 'u')
        )
    {
        format!("{}ies", &alias[..alias.len() - 1])
    } else if alias.ends_with("ch")
        || alias.ends_with("sh")
        || alias.ends_with('s')
        || alias.ends_with('x')
        || alias.ends_with('z')
    {
        format!("{alias}es")
    } else {
        format!("{alias}s")
    }
}

fn hydrate_object(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: Vec<i32>,
    remaining_depth: i32,
    hydration_budget: &mut HydrationBudget,
) -> Result<HydratedTemplateObject, ApiError> {
    hydration_budget.count_object()?;

    let object = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
        ApiError::InternalServerError("Missing object while hydrating template".to_string())
    })?;
    let mut related = BTreeMap::new();
    let mut reachable = BTreeMap::new();
    let mut paths = BTreeMap::new();

    if let Some(alias_groups) = neighborhood.aliases_by_object_id.get(&object_id) {
        for (alias, neighbor_ids) in alias_groups {
            let mut hydrated_neighbors = Vec::new();
            if remaining_depth > 0 {
                for neighbor_id in neighbor_ids {
                    if path.contains(neighbor_id) {
                        continue;
                    }
                    let mut next_path = path.clone();
                    next_path.push(*neighbor_id);
                    hydrated_neighbors.push(hydrate_object(
                        neighborhood,
                        *neighbor_id,
                        next_path,
                        remaining_depth - 1,
                        hydration_budget,
                    )?);
                }
            }
            related.insert(alias.clone(), hydrated_neighbors);
        }
    }

    for (alias, targets) in collect_reachable_targets(
        neighborhood,
        object_id,
        &path,
        remaining_depth,
        hydration_budget.remaining(),
    )? {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydration_budget,
            )?);
        }
        reachable.insert(alias, hydrated_targets);
    }

    for (alias, targets) in collect_path_targets(
        neighborhood,
        object_id,
        &path,
        remaining_depth,
        hydration_budget.remaining(),
    )? {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydration_budget,
            )?);
        }
        paths.insert(alias, hydrated_targets);
    }

    Ok(HydratedTemplateObject {
        id: object.id,
        name: object.name.clone(),
        namespace_id: object.namespace_id,
        hubuum_class_id: object.hubuum_class_id,
        data: object.data.clone(),
        description: object.description.clone(),
        created_at: object.created_at,
        updated_at: object.updated_at,
        path: path.clone(),
        path_objects: build_path_objects(neighborhood, &path)?,
        related,
        reachable,
        paths,
    })
}

fn collect_reachable_targets(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: &[i32],
    remaining_depth: i32,
    max_targets: usize,
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut reachable_by_alias = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(reachable_by_alias);
    }

    let mut collected_targets = 0_usize;
    let mut queue = VecDeque::from([(object_id, path.to_vec(), 0_i32)]);
    let mut visited_distances = BTreeMap::from([(object_id, 0_i32)]);
    let mut alias_owners = BTreeMap::<String, i32>::new();

    while let Some((current_id, current_path, current_distance)) = queue.pop_front() {
        if current_distance >= remaining_depth {
            continue;
        }

        for neighbor_id in direct_neighbor_ids(neighborhood, current_id)? {
            if current_path.contains(&neighbor_id) {
                continue;
            }

            let next_distance = current_distance + 1;
            if visited_distances.contains_key(&neighbor_id) {
                continue;
            }

            let neighbor = neighborhood
                .objects_by_id
                .get(&neighbor_id)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing reachable object while hydrating template".to_string(),
                    )
                })?;
            let current = neighborhood.objects_by_id.get(&current_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing current object while hydrating reachable relations".to_string(),
                )
            })?;
            let alias = reachable_alias_for_classes(
                &neighborhood.class_relations_by_pair,
                &neighborhood.class_names_by_id,
                current.hubuum_class_id,
                neighbor.hubuum_class_id,
            )?;
            if let Some(existing_class_id) = alias_owners.get(&alias)
                && *existing_class_id != neighbor.hubuum_class_id
            {
                let source = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing source object while hydrating reachable relations".to_string(),
                    )
                })?;
                return Err(ApiError::BadRequest(format!(
                    "Reachable relation alias collision for object '{}' on alias '{}'",
                    source.name, alias
                )));
            }
            alias_owners.insert(alias.clone(), neighbor.hubuum_class_id);

            count_collected_template_target(&mut collected_targets, max_targets, "reachable")?;
            let mut next_path = current_path.clone();
            next_path.push(neighbor_id);
            visited_distances.insert(neighbor_id, next_distance);
            queue.push_back((neighbor_id, next_path.clone(), next_distance));
            reachable_by_alias
                .entry(alias)
                .or_default()
                .push(ReachableTemplateTarget {
                    target_id: neighbor_id,
                    path: next_path,
                    remaining_depth: remaining_depth - next_distance,
                });
        }
    }

    for targets in reachable_by_alias.values_mut() {
        targets.sort_unstable_by(|left, right| {
            let left_object = &neighborhood.objects_by_id[&left.target_id];
            let right_object = &neighborhood.objects_by_id[&right.target_id];
            left_object
                .name
                .cmp(&right_object.name)
                .then_with(|| left.target_id.cmp(&right.target_id))
        });
    }

    Ok(reachable_by_alias)
}

fn count_collected_template_target(
    collected_targets: &mut usize,
    max_targets: usize,
    relation_kind: &str,
) -> Result<(), ApiError> {
    if *collected_targets >= max_targets {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded while collecting {relation_kind} relation targets ({} >= {} remaining capacity)",
            *collected_targets, max_targets
        )));
    }

    *collected_targets = collected_targets.saturating_add(1);
    Ok(())
}

fn collect_path_targets(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: &[i32],
    remaining_depth: i32,
    max_targets: usize,
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut path_targets = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(path_targets);
    }

    let mut collected_targets = 0_usize;
    let mut queue = VecDeque::from([(object_id, path.to_vec(), 0_i32)]);
    let mut alias_owners = BTreeMap::<String, i32>::new();

    while let Some((current_id, current_path, current_distance)) = queue.pop_front() {
        if current_distance >= remaining_depth {
            continue;
        }

        for neighbor_id in direct_neighbor_ids(neighborhood, current_id)? {
            if current_path.contains(&neighbor_id) {
                continue;
            }

            let next_distance = current_distance + 1;
            let neighbor = neighborhood
                .objects_by_id
                .get(&neighbor_id)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing path object while hydrating template".to_string(),
                    )
                })?;
            let current = neighborhood.objects_by_id.get(&current_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing current object while hydrating path relations".to_string(),
                )
            })?;
            let alias = reachable_alias_for_classes(
                &neighborhood.class_relations_by_pair,
                &neighborhood.class_names_by_id,
                current.hubuum_class_id,
                neighbor.hubuum_class_id,
            )?;
            if let Some(existing_class_id) = alias_owners.get(&alias)
                && *existing_class_id != neighbor.hubuum_class_id
            {
                let source = neighborhood.objects_by_id.get(&object_id).ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Missing source object while hydrating path relations".to_string(),
                    )
                })?;
                return Err(ApiError::BadRequest(format!(
                    "Path relation alias collision for object '{}' on alias '{}'",
                    source.name, alias
                )));
            }
            alias_owners.insert(alias.clone(), neighbor.hubuum_class_id);

            count_collected_template_target(&mut collected_targets, max_targets, "path")?;
            let mut next_path = current_path.clone();
            next_path.push(neighbor_id);
            queue.push_back((neighbor_id, next_path.clone(), next_distance));
            path_targets
                .entry(alias)
                .or_default()
                .push(ReachableTemplateTarget {
                    target_id: neighbor_id,
                    path: next_path,
                    remaining_depth: remaining_depth - next_distance,
                });
        }
    }

    for targets in path_targets.values_mut() {
        targets.sort_unstable_by(|left, right| {
            left.path.len().cmp(&right.path.len()).then_with(|| {
                let left_object = &neighborhood.objects_by_id[&left.target_id];
                let right_object = &neighborhood.objects_by_id[&right.target_id];
                left_object
                    .name
                    .cmp(&right_object.name)
                    .then_with(|| left.target_id.cmp(&right.target_id))
            })
        });
    }

    Ok(path_targets)
}

fn max_hydrated_template_objects() -> usize {
    get_config()
        .map(|config| config.report_template_max_objects)
        .unwrap_or(DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS)
}

fn max_active_report_tasks_per_user() -> usize {
    get_config()
        .map(|config| config.report_max_active_tasks_per_user)
        .unwrap_or(DEFAULT_REPORT_MAX_ACTIVE_TASKS_PER_USER)
}

fn configured_report_max_output_bytes() -> usize {
    get_config()
        .map(|config| config.report_max_output_bytes)
        .unwrap_or(DEFAULT_REPORT_MAX_OUTPUT_BYTES)
}

fn build_path_objects(
    neighborhood: &ObjectNeighborhood,
    path: &[i32],
) -> Result<Vec<HydratedTemplatePathObject>, ApiError> {
    path.iter()
        .map(|object_id| {
            let object = neighborhood.objects_by_id.get(object_id).ok_or_else(|| {
                ApiError::InternalServerError(
                    "Missing object while building hydrated template path".to_string(),
                )
            })?;
            Ok(HydratedTemplatePathObject {
                id: object.id,
                name: object.name.clone(),
                namespace_id: object.namespace_id,
                hubuum_class_id: object.hubuum_class_id,
            })
        })
        .collect()
}

fn direct_neighbor_ids(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
) -> Result<Vec<i32>, ApiError> {
    let mut neighbor_ids = neighborhood
        .aliases_by_object_id
        .get(&object_id)
        .ok_or_else(|| ApiError::InternalServerError("Missing alias grouping state".to_string()))?
        .values()
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    neighbor_ids.sort_unstable_by(|left, right| {
        let left_object = &neighborhood.objects_by_id[left];
        let right_object = &neighborhood.objects_by_id[right];
        left_object
            .name
            .cmp(&right_object.name)
            .then_with(|| left.cmp(right))
    });
    neighbor_ids.dedup();

    Ok(neighbor_ids)
}

fn object_with_root_path(object: &HubuumObject) -> HubuumObjectWithPath {
    HubuumObjectWithPath {
        id: object.id,
        name: object.name.clone(),
        namespace_id: object.namespace_id,
        hubuum_class_id: object.hubuum_class_id,
        data: object.data.clone(),
        description: object.description.clone(),
        created_at: object.created_at,
        updated_at: object.updated_at,
        path: vec![object.id],
    }
}

async fn execute_scope(
    pool: &DbPool,
    subject: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
    scope: &ReportScope,
    mut query_options: QueryOptions,
) -> Result<(Vec<serde_json::Value>, Vec<ReportWarning>, bool), ApiError> {
    let item_limit = query_options.limit.unwrap_or(1).saturating_sub(1).max(1);

    let data = match scope.kind {
        ReportScopeKind::Namespaces => to_json_items(
            subject
                .search_namespaces(pool, query_options, scopes)
                .await?,
        )?,
        ReportScopeKind::Classes => {
            to_json_items(subject.search_classes(pool, query_options, scopes).await?)?
        }
        ReportScopeKind::ObjectsInClass => {
            push_exact_filter(
                &mut query_options,
                FilterField::ClassId,
                scope.class_id_required()?,
            )?;
            to_json_items(subject.search_objects(pool, query_options, scopes).await?)?
        }
        ReportScopeKind::ClassRelations => to_json_items(
            subject
                .search_class_relations(pool, query_options, scopes)
                .await?,
        )?,
        ReportScopeKind::ObjectRelations => to_json_items(
            subject
                .search_object_relations(pool, query_options, scopes)
                .await?,
        )?,
        ReportScopeKind::RelatedObjects => {
            let class_id = HubuumClassID::new(scope.class_id_required()?)?;
            let object_id = HubuumObjectID::new(scope.object_id_required()?)?;
            check_if_object_in_class(pool, &class_id, &object_id).await?;
            let source_object = object_id.instance(pool).await?;
            can!(
                pool,
                subject,
                scopes,
                [Permissions::ReadObject],
                source_object
            );
            let related = subject
                .search_objects_related_to(pool, object_id, query_options, scopes)
                .await?;
            to_json_items(
                related
                    .into_iter()
                    .map(|relation| relation.to_descendant_object_with_path())
                    .collect::<Vec<_>>(),
            )?
        }
    };

    let (items, truncated) = truncate_items(data, item_limit);
    Ok((items, Vec::new(), truncated))
}

async fn apply_report_includes(
    pool: &DbPool,
    user: &impl crate::traits::Search,
    scopes: Option<&[Permissions]>,
    report: &ReportRequest,
    items: &mut [serde_json::Value],
) -> Result<(), ApiError> {
    let Some(related_objects) = report
        .include
        .as_ref()
        .and_then(|include| include.related_objects.as_ref())
    else {
        return Ok(());
    };

    if related_objects.is_empty() || items.is_empty() {
        return Ok(());
    }

    let root_object_ids = report_item_ids(items)?;
    for alias in related_objects.keys() {
        initialize_related_alias(items, alias)?;
    }

    let item_indexes = root_object_ids
        .iter()
        .enumerate()
        .map(|(index, object_id)| (*object_id, index))
        .collect::<HashMap<i32, usize>>();

    for (alias, include) in related_objects {
        let max_depth = include
            .max_depth
            .unwrap_or(RELATED_INCLUDE_DEFAULT_MAX_DEPTH);
        let limit = include.limit.unwrap_or(RELATED_INCLUDE_DEFAULT_LIMIT);
        let direction = include
            .direction
            .unwrap_or(ReportIncludeRelatedDirection::Any);
        let sort = include.sort.unwrap_or(ReportIncludeRelatedSort::Path);
        let include_query = ReportIncludeRelatedQuery {
            class_id: include.class_id,
            class_relation_id: include.class_relation_id,
            direction,
            sort,
            max_depth,
            limit,
        };
        let related = user
            .related_objects_for_roots(pool, &root_object_ids, include_query, scopes)
            .await?;

        for row in related {
            let Some(item_index) = item_indexes.get(&row.root_object_id) else {
                continue;
            };
            let related_object = row.to_descendant_object_with_path();
            let related_value = serde_json::to_value(related_object).map_err(ApiError::from)?;
            push_related_alias_value(&mut items[*item_index], alias, related_value)?;
        }
    }

    Ok(())
}

fn report_item_ids(items: &[serde_json::Value]) -> Result<Vec<i32>, ApiError> {
    items
        .iter()
        .map(|item| {
            let id = item
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Report object item did not include integer id".to_string(),
                    )
                })?;
            i32::try_from(id).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "Report object item id '{id}' is outside i32 range"
                ))
            })
        })
        .collect()
}

fn initialize_related_alias(items: &mut [serde_json::Value], alias: &str) -> Result<(), ApiError> {
    for item in items {
        let related = related_object_mut(item)?;
        related.insert(alias.to_string(), serde_json::Value::Array(Vec::new()));
    }
    Ok(())
}

fn push_related_alias_value(
    item: &mut serde_json::Value,
    alias: &str,
    value: serde_json::Value,
) -> Result<(), ApiError> {
    let related = related_object_mut(item)?;
    let Some(alias_value) = related.get_mut(alias) else {
        return Err(ApiError::InternalServerError(format!(
            "Related include alias '{alias}' was not initialized"
        )));
    };
    let Some(alias_values) = alias_value.as_array_mut() else {
        return Err(ApiError::InternalServerError(format!(
            "Related include alias '{alias}' is not an array"
        )));
    };
    alias_values.push(value);
    Ok(())
}

fn related_object_mut(
    item: &mut serde_json::Value,
) -> Result<&mut serde_json::Map<String, serde_json::Value>, ApiError> {
    let Some(item_object) = item.as_object_mut() else {
        return Err(ApiError::InternalServerError(
            "Report object item was not a JSON object".to_string(),
        ));
    };

    let related = item_object
        .entry("related")
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    related.as_object_mut().ok_or_else(|| {
        ApiError::InternalServerError(
            "Report object item related field was not an object".to_string(),
        )
    })
}

fn push_exact_filter(
    query_options: &mut QueryOptions,
    field: FilterField,
    value: i32,
) -> Result<(), ApiError> {
    if query_options.filters.iter().any(|param| {
        param.field == field
            && matches!(
                param.operator,
                crate::models::search::SearchOperator::Equals { is_negated: false }
            )
            && param.value == value.to_string()
    }) {
        return Ok(());
    }

    query_options.filters.push(ParsedQueryParam::new(
        &field.to_string(),
        None,
        &value.to_string(),
    )?);
    Ok(())
}

fn to_json_items<T: Serialize>(items: Vec<T>) -> Result<Vec<serde_json::Value>, ApiError> {
    items
        .into_iter()
        .map(|item| serde_json::to_value(item).map_err(ApiError::from))
        .collect()
}

fn truncate_items(
    mut items: Vec<serde_json::Value>,
    limit: usize,
) -> (Vec<serde_json::Value>, bool) {
    if items.len() > limit {
        items.truncate(limit);
        (items, true)
    } else {
        (items, false)
    }
}

fn report_headers(warning_count: usize, truncated: bool) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        REPORT_WARNINGS_HEADER.to_string(),
        warning_count.to_string(),
    );
    headers.insert(REPORT_TRUNCATED_HEADER.to_string(), truncated.to_string());
    headers
}

fn enforce_json_output_limit(
    response: &ReportJsonResponse,
    report: &ReportRequest,
) -> Result<(), ApiError> {
    let max_output_bytes = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_report_max_output_bytes);

    let mut writer = SizeLimitedWriter::new(max_output_bytes);
    if let Err(error) = serde_json::to_writer(&mut writer, response) {
        if writer.exceeded() {
            return Err(ApiError::PayloadTooLarge(format!(
                "Rendered report exceeded max_output_bytes (> {max_output_bytes})"
            )));
        }

        return Err(ApiError::InternalServerError(format!(
            "Failed to serialize report: {error}"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::models::{
        ReportContentType, ReportInclude, ReportIncludeRelatedObject, ReportLimits, ReportMeta,
        ReportMissingDataPolicy, ReportRelationContext, ReportRequest, ReportScope,
        ReportScopeKind, ReportTaskOutputRecord, ReportTemplate, ReportTemplateKind,
    };

    use super::{
        HydrationBudget, REPORT_TRUNCATED_HEADER, ReportRuntime, inferred_relation_alias,
        normalize_alias_segment, pluralize_alias, render_report_task_output,
        take_related_within_budget, validate_report_limits, validate_report_submission,
    };
    use crate::errors::ApiError;

    fn test_timestamp() -> chrono::NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    fn test_object_with_path(id: i32) -> crate::models::HubuumObjectWithPath {
        crate::models::HubuumObjectWithPath {
            id,
            name: format!("object-{id}"),
            namespace_id: 1,
            hubuum_class_id: 1,
            data: serde_json::json!({}),
            description: String::new(),
            created_at: test_timestamp(),
            updated_at: test_timestamp(),
            path: vec![id],
        }
    }

    #[test]
    fn take_related_within_budget_allows_within_capacity() {
        let mut budget = HydrationBudget::new(5);
        budget.count_object().unwrap(); // hydrated=1, remaining=4, cap=3
        let kept = take_related_within_budget(
            &budget,
            vec![test_object_with_path(10), test_object_with_path(11)],
        )
        .unwrap();
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn take_related_within_budget_errors_when_second_root_exceeds_remaining() {
        let mut budget = HydrationBudget::new(5);
        // Simulate the first root consuming three objects.
        budget.count_object().unwrap();
        budget.count_object().unwrap();
        budget.count_object().unwrap(); // hydrated=3, remaining=2, cap=1
        let err = take_related_within_budget(
            &budget,
            vec![
                test_object_with_path(10),
                test_object_with_path(11),
                test_object_with_path(12),
            ],
        )
        .unwrap_err();
        match err {
            ApiError::BadRequest(message) => assert_eq!(
                message,
                "Hydrated template object limit exceeded (2 related objects > 1 remaining related capacity)"
            ),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn take_related_within_budget_errors_when_exhausted() {
        let mut budget = HydrationBudget::new(2);
        budget.count_object().unwrap();
        budget.count_object().unwrap(); // hydrated=2, remaining=0
        let err = take_related_within_budget(&budget, vec![test_object_with_path(10)]).unwrap_err();
        match err {
            ApiError::BadRequest(message) => {
                assert_eq!(message, "Hydrated template object limit exceeded (2 >= 2)")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn report_with_limits(limits: ReportLimits) -> ReportRequest {
        ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(1),
                object_id: None,
            },
            query: None,
            missing_data_policy: None,
            limits: Some(limits),
            include: None,
            relation_context: None,
        }
    }

    fn templated_report_with_include(
        related_objects: HashMap<String, ReportIncludeRelatedObject>,
    ) -> ReportRequest {
        ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(1),
                object_id: None,
            },
            query: None,
            missing_data_policy: None,
            limits: None,
            include: Some(ReportInclude {
                related_objects: Some(related_objects),
            }),
            relation_context: Some(ReportRelationContext { depth: Some(1) }),
        }
    }

    fn report_runtime(report: ReportRequest) -> ReportRuntime {
        ReportRuntime {
            report,
            content_type: ReportContentType::TextPlain,
            missing_data_policy: ReportMissingDataPolicy::Strict,
            template: Some(ReportTemplate {
                id: 1,
                namespace_id: 1,
                name: "summary".to_string(),
                description: String::new(),
                content_type: ReportContentType::TextPlain,
                template: "{{ items|length }}".to_string(),
                kind: ReportTemplateKind::Report,
                scope_kind: Some(ReportScopeKind::ObjectsInClass),
                class_id: Some(1),
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                created_at: test_timestamp(),
                updated_at: test_timestamp(),
            }),
            namespace_templates: Vec::new(),
        }
    }

    fn report_output_record(
        meta_truncated: bool,
        output_truncated: bool,
    ) -> ReportTaskOutputRecord {
        ReportTaskOutputRecord {
            id: 1,
            task_id: 1,
            template_name: Some("summary".to_string()),
            content_type: ReportContentType::TextPlain.as_mime().to_string(),
            json_output: None,
            text_output: Some("ok".to_string()),
            meta_json: serde_json::to_value(ReportMeta {
                count: 1,
                truncated: meta_truncated,
                scope: ReportScope {
                    kind: ReportScopeKind::ObjectsInClass,
                    class_id: Some(1),
                    object_id: None,
                },
                content_type: ReportContentType::TextPlain,
            })
            .unwrap(),
            warnings_json: serde_json::json!([]),
            warning_count: 0,
            truncated: output_truncated,
            output_expires_at: test_timestamp(),
            total_duration_ms: 0,
            query_duration_ms: 0,
            hydration_duration_ms: 0,
            render_duration_ms: 0,
            created_at: test_timestamp(),
        }
    }

    #[test]
    fn normalizes_relation_alias_segments_predictably() {
        assert_eq!(normalize_alias_segment("Access Policy"), "access_policy");
        assert_eq!(normalize_alias_segment("Person async"), "person_async");
        assert_eq!(normalize_alias_segment("Host"), "host");
    }

    #[test]
    fn pluralizes_relation_aliases_predictably() {
        assert_eq!(pluralize_alias("room"), "rooms");
        assert_eq!(pluralize_alias("person"), "persons");
        assert_eq!(pluralize_alias("policy"), "policies");
        assert_eq!(pluralize_alias("class"), "classes");
    }

    #[test]
    fn infers_relation_aliases_from_class_names() {
        assert_eq!(inferred_relation_alias("Room"), "rooms");
        assert_eq!(inferred_relation_alias("Person"), "persons");
        assert_eq!(inferred_relation_alias("Access Policy"), "access_policies");
        assert_eq!(inferred_relation_alias("Person async"), "person_asyncs");
    }

    #[test]
    fn rejects_relation_context_with_related_object_includes() {
        let mut related_objects = HashMap::new();
        related_objects.insert(
            "owners".to_string(),
            ReportIncludeRelatedObject {
                class_id: 2,
                class_relation_id: None,
                direction: None,
                sort: None,
                max_depth: None,
                limit: None,
            },
        );
        let runtime = report_runtime(templated_report_with_include(related_objects));

        let error = validate_report_submission(&runtime).unwrap_err();

        assert_eq!(
            error.to_string(),
            "include.related_objects cannot be combined with relation_context"
        );
    }

    #[test]
    fn allows_relation_context_with_empty_related_object_includes() {
        let runtime = report_runtime(templated_report_with_include(HashMap::new()));

        validate_report_submission(&runtime).unwrap();
    }

    #[test]
    fn text_report_output_headers_use_persisted_truncated_column() {
        let response = render_report_task_output(report_output_record(false, true)).unwrap();

        assert_eq!(
            response
                .headers()
                .get(REPORT_TRUNCATED_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
    }

    #[test]
    fn rejects_zero_report_limits() {
        let max_items_error = validate_report_limits(&report_with_limits(ReportLimits {
            max_items: Some(0),
            max_output_bytes: None,
        }))
        .unwrap_err();

        assert_eq!(
            max_items_error.to_string(),
            "max_items must be greater than 0"
        );

        let max_output_error = validate_report_limits(&report_with_limits(ReportLimits {
            max_items: None,
            max_output_bytes: Some(0),
        }))
        .unwrap_err();

        assert_eq!(
            max_output_error.to_string(),
            "max_output_bytes must be greater than 0"
        );
    }

    #[test]
    fn rejects_report_output_limit_above_server_cap() {
        let error = validate_report_limits(&report_with_limits(ReportLimits {
            max_items: None,
            max_output_bytes: Some(usize::MAX),
        }))
        .unwrap_err();

        assert!(error.to_string().contains("exceeds server maximum"));
    }

    #[test]
    fn hydration_budget_reserves_capacity_for_root_object() {
        let budget = HydrationBudget::new(2);

        assert_eq!(budget.remaining(), 2);
        assert_eq!(budget.remaining_related_capacity().unwrap(), 1);
    }

    #[test]
    fn hydration_budget_rejects_when_report_budget_is_exhausted() {
        let mut budget = HydrationBudget::new(1);

        assert_eq!(budget.remaining_related_capacity().unwrap(), 0);
        budget.count_object().unwrap();

        let error = budget.remaining_related_capacity().unwrap_err();

        assert_eq!(
            error.to_string(),
            "Hydrated template object limit exceeded (1 >= 1)"
        );
    }
}
