use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post, web};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::config::{
    DEFAULT_REPORT_OUTPUT_RETENTION_HOURS, DEFAULT_REPORT_STAGE_TIMEOUT_MS,
    DEFAULT_REPORT_TEMPLATE_MAX_OBJECTS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::task::{
    TaskCreateRequest, TaskStateUpdate, append_task_event, create_generic_task,
    finalize_report_task_with_output, find_report_task_output, find_report_task_output_summary,
    find_task_by_idempotency, find_task_record, update_task_state,
};
use crate::db::traits::{SelfRelations, UserPermissions};
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, SearchOperator, parse_query_parameter,
};
use crate::models::{
    HubuumClassID, HubuumObject, HubuumObjectID, HubuumObjectRelation, HubuumObjectWithPath,
    NamespaceID, NewReportTaskOutputRecord, NewTaskEventRecord, Permissions, ReportContentType,
    ReportJsonResponse, ReportMeta, ReportMissingDataPolicy, ReportRequest, ReportScope,
    ReportScopeKind, ReportTaskOutputRecord, ReportTemplate, ReportTemplateID, ReportWarning,
    TaskKind, TaskRecord, TaskResponse, User,
};
use crate::pagination::page_limits_or_defaults;
use crate::tasks::{ensure_task_worker_running, kick_task_worker, request_hash};
use crate::traits::{GroupMemberships, NamespaceAccessors, Search, SelfAccessors};
use crate::utilities::reporting::render_template;
use crate::utilities::response::{json_response, json_response_with_header};

use super::check_if_object_in_class;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredReportTaskPayload {
    report: ReportRequest,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    template: Option<ReportTemplate>,
    namespace_templates: Vec<ReportTemplate>,
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

const DEFAULT_MAX_OUTPUT_BYTES: usize = 262_144;
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
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_report(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    report: web::Json<ReportRequest>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());

    let report = report.into_inner();
    let payload = serde_json::to_value(&report)?;
    let hash = request_hash(&payload)?;
    let idempotency_key = req
        .headers()
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    let runtime = prepare_report_runtime(&pool, &requestor.user, report).await?;
    validate_report_submission(&runtime)?;
    let task_payload = runtime_to_task_payload(&runtime)?;

    let task = find_or_create_report_task(
        &pool,
        requestor.user.id,
        idempotency_key,
        serde_json::to_value(task_payload)?,
        hash,
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
    requestor: UserAccess,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task = load_authorized_report_task(&pool, &requestor.user, task_id.into_inner()).await?;
    let output = find_report_task_output_summary(&pool, task.id).await?;
    Ok(json_response(
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
        (status = 404, description = "Report output not found", body = ApiErrorResponse)
    )
)]
#[get("/{task_id}/output")]
pub async fn get_report_output(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    task_id: web::Path<i32>,
) -> Result<impl Responder, ApiError> {
    ensure_task_worker_running(pool.get_ref().clone());
    let task_id = task_id.into_inner();
    load_authorized_report_task(&pool, &requestor.user, task_id).await?;
    let output = find_report_task_output(&pool, task_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("Report output not found".to_string()))?;
    render_report_task_output(output)
}

async fn prepare_report_runtime(
    pool: &DbPool,
    user: &User,
    report: ReportRequest,
) -> Result<ReportRuntime, ApiError> {
    report.scope.validate()?;

    let template = resolve_template(pool, user, &report).await?;
    let namespace_templates = match &template {
        Some(template) => {
            crate::models::report_template::report_templates_in_namespace(
                pool,
                template.namespace_id,
                None,
            )
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
    user: &crate::models::User,
    report: &ReportRequest,
) -> Result<Option<ReportTemplate>, ApiError> {
    let Some(template_id) = report.output.as_ref().and_then(|output| output.template_id) else {
        return Ok(None);
    };

    let template = ReportTemplateID(template_id).instance(pool).await?;
    can!(
        pool,
        user.clone(),
        [Permissions::ReadTemplate],
        NamespaceID(template.namespace_id)
    );

    Ok(Some(template))
}

fn validate_report_submission(runtime: &ReportRuntime) -> Result<(), ApiError> {
    let mut query_options = prepare_query_options(&runtime.report)?;
    let _ = resolve_relation_hydration_plan(runtime, &mut query_options)?;
    Ok(())
}

fn runtime_to_task_payload(runtime: &ReportRuntime) -> Result<StoredReportTaskPayload, ApiError> {
    validate_report_submission(runtime)?;
    Ok(StoredReportTaskPayload {
        report: runtime.report.clone(),
        content_type: runtime.content_type,
        missing_data_policy: runtime.missing_data_policy,
        template: runtime.template.clone(),
        namespace_templates: runtime.namespace_templates.clone(),
    })
}

fn runtime_from_task_payload(payload: StoredReportTaskPayload) -> ReportRuntime {
    ReportRuntime {
        report: payload.report,
        content_type: payload.content_type,
        missing_data_policy: payload.missing_data_policy,
        template: payload.template,
        namespace_templates: payload.namespace_templates,
    }
}

async fn find_or_create_report_task(
    pool: &DbPool,
    submitted_by: i32,
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
        && let Some(existing) = find_task_by_idempotency(pool, submitted_by, key).await?
    {
        if matches_request(&existing) {
            return Ok(existing);
        }

        return Err(ApiError::Conflict(format!(
            "Idempotency-Key '{key}' is already in use for a different task submission"
        )));
    }

    match create_generic_task(
        pool,
        TaskCreateRequest {
            kind: TaskKind::Report,
            submitted_by,
            idempotency_key: idempotency_key.clone(),
            request_hash: Some(request_hash_value),
            request_payload: payload,
            total_items: 1,
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
        Err(error) => Err(error),
    }
}

async fn load_authorized_report_task(
    pool: &DbPool,
    requestor: &User,
    task_id: i32,
) -> Result<TaskRecord, ApiError> {
    let task = find_task_record(pool, task_id).await?;
    if task.kind != TaskKind::Report.as_str() {
        return Err(ApiError::NotFound(format!(
            "Report task {} not found",
            task_id
        )));
    }
    if task.submitted_by == Some(requestor.id) || requestor.is_admin(pool).await? {
        Ok(task)
    } else {
        Err(ApiError::NotFound("Report task not found".to_string()))
    }
}

pub(crate) async fn execute_report_task(
    pool: &DbPool,
    task: &TaskRecord,
    user: &User,
) -> Result<(), ApiError> {
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Report task payload is missing".to_string()))?;
    let payload: StoredReportTaskPayload = serde_json::from_value(payload)?;
    let runtime = runtime_from_task_payload(payload);
    let total_start = Instant::now();
    let mut timings = ReportExecutionTimings::default();

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "running".to_string(),
            message: "Report execution started".to_string(),
            data: None,
        },
    )
    .await?;
    update_task_state(
        pool,
        task.id,
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

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "running".to_string(),
            message: "Query execution started".to_string(),
            data: Some(serde_json::json!({
                "scope": runtime.report.scope.kind.as_str(),
                "content_type": runtime.content_type.as_mime(),
            })),
        },
    )
    .await?;

    let mut query_options = prepare_query_options(&runtime.report)?;
    let relation_hydration = resolve_relation_hydration_plan(&runtime, &mut query_options)?;
    let query_start = Instant::now();
    let (items, mut warnings, truncated) =
        execute_scope(pool, user, &runtime.report.scope, query_options).await?;
    timings.query_duration_ms = duration_to_millis_i32(query_start.elapsed());
    enforce_report_stage_timeout(query_start, "query execution")?;

    if relation_hydration
        .as_ref()
        .is_some_and(|plan| plan.enabled_for_scope)
    {
        append_task_event(
            pool,
            NewTaskEventRecord {
                task_id: task.id,
                event_type: "running".to_string(),
                message: "Hydrating relation-aware template context".to_string(),
                data: relation_hydration.as_ref().map(|plan| {
                    serde_json::json!({
                        "depth_limit": plan.depth_limit,
                    })
                }),
            },
        )
        .await?;
    }

    add_truncation_warning(&mut warnings, truncated);
    let hydration_start = Instant::now();
    let (template_items, source) =
        build_template_items(pool, user, &runtime, &items, relation_hydration).await?;
    timings.hydration_duration_ms = duration_to_millis_i32(hydration_start.elapsed());
    enforce_report_stage_timeout(hydration_start, "relation hydration")?;
    let execution = ReportExecution {
        meta: ReportMeta {
            count: if runtime.template.is_some() {
                template_items.len()
            } else {
                items.len()
            },
            truncated,
            scope: runtime.report.scope.clone(),
            content_type: runtime.content_type,
        },
        items,
        template_items,
        warnings,
        source,
    };

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "running".to_string(),
            message: "Rendering report output".to_string(),
            data: None,
        },
    )
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

    append_task_event(
        pool,
        NewTaskEventRecord {
            task_id: task.id,
            event_type: "running".to_string(),
            message: "Persisting report output".to_string(),
            data: None,
        },
    )
    .await?;

    finalize_report_task_with_output(
        pool,
        task.id,
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
    ensure_json_output_has_no_template_id(&runtime.report)?;
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
    let (rendered, template_warnings) = render_template(
        template,
        &runtime.namespace_templates,
        &context,
        runtime.content_type,
        runtime.missing_data_policy,
    )?;
    let mut warnings = execution.warnings;
    warnings.extend(template_warnings);

    enforce_text_output_limit(&rendered, &runtime.report)?;

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

fn ensure_json_output_has_no_template_id(report: &ReportRequest) -> Result<(), ApiError> {
    if report
        .output
        .as_ref()
        .and_then(|output| output.template_id)
        .is_some()
    {
        return Err(ApiError::BadRequest(
            "Template references are only supported for text/plain, text/html, and text/csv output"
                .to_string(),
        ));
    }

    Ok(())
}

fn required_template(
    runtime: &ReportRuntime,
    content_type: ReportContentType,
) -> Result<&ReportTemplate, ApiError> {
    runtime.template.as_ref().ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Output type '{}' requires output.template_id",
            content_type.as_mime()
        ))
    })
}

fn render_report_task_output(output: ReportTaskOutputRecord) -> Result<HttpResponse, ApiError> {
    let content_type = ReportContentType::from_mime(&output.content_type)?;
    let meta: ReportMeta = serde_json::from_value(output.meta_json)?;
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
            Ok(json_response_with_header(
                response,
                StatusCode::OK,
                Some(report_headers(warning_count, truncated)),
            ))
        }
        ReportContentType::TextPlain | ReportContentType::TextHtml | ReportContentType::TextCsv => {
            let mut response = HttpResponse::build(StatusCode::OK);
            response.content_type(content_type.as_mime());
            for (key, value) in report_headers(warning_count, meta.truncated) {
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

    if let Some(limits) = &report.limits
        && let Some(0) = limits.max_items
    {
        return Err(ApiError::BadRequest(
            "max_items must be greater than 0".to_string(),
        ));
    }

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
    user: &crate::models::User,
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

    let mut class_names = BTreeMap::new();

    match runtime.report.scope.kind {
        ReportScopeKind::ObjectsInClass => {
            let roots = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObject>)
                .collect::<Result<Vec<_>, _>>()?;
            let mut hydrated_items = Vec::with_capacity(roots.len());

            for root in roots {
                let hydrated = hydrate_objects_in_class_root(
                    pool,
                    user,
                    &root,
                    relation_hydration.depth_limit,
                    &mut class_names,
                )
                .await?;
                hydrated_items.push(serde_json::to_value(hydrated)?);
            }

            Ok((hydrated_items, None))
        }
        ReportScopeKind::RelatedObjects => {
            let source_object = HubuumObjectID(runtime.report.scope.object_id_required()?)
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
                source,
                related_objects,
                relation_hydration.depth_limit,
                &mut class_names,
            )
            .await?;
            let source = serde_json::to_value(&hydrated)?;
            Ok((vec![source.clone()], Some(source)))
        }
        _ => Ok((items.to_vec(), None)),
    }
}

async fn hydrate_objects_in_class_root(
    pool: &DbPool,
    user: &crate::models::User,
    root: &HubuumObject,
    depth_limit: i32,
    class_names: &mut BTreeMap<i32, String>,
) -> Result<HydratedTemplateObject, ApiError> {
    let root_with_path = object_with_root_path(root);
    let related_objects = load_related_objects_for_root(pool, user, root.id, depth_limit).await?;
    let object_ids = std::iter::once(root.id)
        .chain(related_objects.iter().map(|object| object.id))
        .collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(pool, &object_ids)
        .await?;
    let neighborhood = build_object_neighborhood(
        pool,
        root_with_path.clone(),
        related_objects,
        relations,
        class_names,
    )
    .await?;
    let mut hydrated_object_count = 0usize;
    hydrate_object(
        &neighborhood,
        root_with_path.id,
        vec![root_with_path.id],
        depth_limit,
        &mut hydrated_object_count,
        max_hydrated_template_objects(),
    )
}

async fn hydrate_related_root(
    pool: &DbPool,
    user: &crate::models::User,
    source: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    depth_limit: i32,
    class_names: &mut BTreeMap<i32, String>,
) -> Result<HydratedTemplateObject, ApiError> {
    let object_ids = std::iter::once(source.id)
        .chain(related_objects.iter().map(|object| object.id))
        .collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(pool, &object_ids)
        .await?;
    let neighborhood = build_object_neighborhood(
        pool,
        source.clone(),
        related_objects,
        relations,
        class_names,
    )
    .await?;
    let mut hydrated_object_count = 0usize;
    hydrate_object(
        &neighborhood,
        source.id,
        vec![source.id],
        depth_limit,
        &mut hydrated_object_count,
        max_hydrated_template_objects(),
    )
}

async fn load_related_objects_for_root(
    pool: &DbPool,
    user: &crate::models::User,
    root_object_id: i32,
    depth_limit: i32,
) -> Result<Vec<HubuumObjectWithPath>, ApiError> {
    let query_options = QueryOptions {
        filters: vec![ParsedQueryParam::new(
            "depth",
            Some(SearchOperator::Lte { is_negated: false }),
            &depth_limit.to_string(),
        )?],
        sort: Vec::new(),
        limit: None,
        cursor: None,
    };
    Ok(user
        .search_objects_related_to(pool, HubuumObjectID(root_object_id), query_options)
        .await?
        .into_iter()
        .map(|relation| relation.to_descendant_object_with_path())
        .collect())
}

async fn build_object_neighborhood(
    pool: &DbPool,
    root: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    relations: Vec<HubuumObjectRelation>,
    class_names: &mut BTreeMap<i32, String>,
) -> Result<ObjectNeighborhood, ApiError> {
    let mut objects_by_id = BTreeMap::new();
    objects_by_id.insert(root.id, root);
    for object in related_objects {
        objects_by_id.insert(object.id, object);
    }

    ensure_class_names(pool, &objects_by_id, class_names).await?;

    let mut aliases_by_object_id = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, Vec<i32>>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut alias_owners = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, i32>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut class_relations_by_pair = BTreeMap::new();

    seed_alias_buckets_from_class_relations(
        pool,
        &objects_by_id,
        &mut aliases_by_object_id,
        &mut alias_owners,
        &mut class_relations_by_pair,
        class_names,
    )
    .await?;

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

async fn ensure_class_names(
    pool: &DbPool,
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    class_names: &mut BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    let mut missing_ids = objects_by_id
        .values()
        .map(|object| object.hubuum_class_id)
        .filter(|class_id| !class_names.contains_key(class_id))
        .collect::<Vec<_>>();
    missing_ids.sort_unstable();
    missing_ids.dedup();

    for class_id in missing_ids {
        let class = HubuumClassID(class_id).instance(pool).await?;
        class_names.insert(class_id, class.name);
    }

    Ok(())
}

async fn seed_alias_buckets_from_class_relations(
    pool: &DbPool,
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &mut BTreeMap<(i32, i32), crate::models::HubuumClassRelation>,
    class_names: &mut BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    for object in objects_by_id.values() {
        let class_relations = HubuumClassID(object.hubuum_class_id)
            .relations(pool)
            .await?;
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

            if let std::collections::btree_map::Entry::Vacant(entry) =
                class_names.entry(adjacent_class_id)
            {
                let class = HubuumClassID(adjacent_class_id).instance(pool).await?;
                entry.insert(class.name);
            }

            let alias = relation_alias_for_viewer(
                &relation,
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
    hydrated_object_count: &mut usize,
    max_hydrated_objects: usize,
) -> Result<HydratedTemplateObject, ApiError> {
    *hydrated_object_count = hydrated_object_count.saturating_add(1);
    if *hydrated_object_count > max_hydrated_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} > {})",
            *hydrated_object_count, max_hydrated_objects
        )));
    }

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
                        hydrated_object_count,
                        max_hydrated_objects,
                    )?);
                }
            }
            related.insert(alias.clone(), hydrated_neighbors);
        }
    }

    for (alias, targets) in
        collect_reachable_targets(neighborhood, object_id, &path, remaining_depth)?
    {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydrated_object_count,
                max_hydrated_objects,
            )?);
        }
        reachable.insert(alias, hydrated_targets);
    }

    for (alias, targets) in collect_path_targets(neighborhood, object_id, &path, remaining_depth)? {
        let mut hydrated_targets = Vec::with_capacity(targets.len());
        for target in targets {
            hydrated_targets.push(hydrate_object(
                neighborhood,
                target.target_id,
                target.path,
                target.remaining_depth,
                hydrated_object_count,
                max_hydrated_objects,
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
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut reachable_by_alias = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(reachable_by_alias);
    }

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

fn collect_path_targets(
    neighborhood: &ObjectNeighborhood,
    object_id: i32,
    path: &[i32],
    remaining_depth: i32,
) -> Result<BTreeMap<String, Vec<ReachableTemplateTarget>>, ApiError> {
    let mut path_targets = BTreeMap::<String, Vec<ReachableTemplateTarget>>::new();
    if remaining_depth <= 0 {
        return Ok(path_targets);
    }

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
    user: &crate::models::User,
    scope: &ReportScope,
    mut query_options: QueryOptions,
) -> Result<(Vec<serde_json::Value>, Vec<ReportWarning>, bool), ApiError> {
    let item_limit = query_options.limit.unwrap_or(1).saturating_sub(1).max(1);

    let data = match scope.kind {
        ReportScopeKind::Namespaces => {
            to_json_items(user.search_namespaces(pool, query_options).await?)?
        }
        ReportScopeKind::Classes => to_json_items(user.search_classes(pool, query_options).await?)?,
        ReportScopeKind::ObjectsInClass => {
            push_exact_filter(
                &mut query_options,
                FilterField::ClassId,
                scope.class_id_required()?,
            )?;
            to_json_items(user.search_objects(pool, query_options).await?)?
        }
        ReportScopeKind::ClassRelations => {
            to_json_items(user.search_class_relations(pool, query_options).await?)?
        }
        ReportScopeKind::ObjectRelations => {
            to_json_items(user.search_object_relations(pool, query_options).await?)?
        }
        ReportScopeKind::RelatedObjects => {
            let class_id = HubuumClassID(scope.class_id_required()?);
            let object_id = HubuumObjectID(scope.object_id_required()?);
            check_if_object_in_class(pool, &class_id, &object_id).await?;
            let source_object = object_id.instance(pool).await?;
            can!(pool, user.clone(), [Permissions::ReadObject], source_object);
            let related = user
                .search_objects_related_to(pool, object_id, query_options)
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
    let bytes = serde_json::to_vec(response).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to serialize report: {error}"))
    })?;
    let max_output_bytes = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);

    if bytes.len() > max_output_bytes {
        return Err(ApiError::PayloadTooLarge(format!(
            "Rendered report exceeded max_output_bytes ({} > {})",
            bytes.len(),
            max_output_bytes
        )));
    }

    Ok(())
}

fn enforce_text_output_limit(rendered: &str, report: &ReportRequest) -> Result<(), ApiError> {
    let max_output_bytes = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);

    if rendered.len() > max_output_bytes {
        return Err(ApiError::PayloadTooLarge(format!(
            "Rendered report exceeded max_output_bytes ({} > {})",
            rendered.len(),
            max_output_bytes
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{inferred_relation_alias, normalize_alias_segment, pluralize_alias};

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
}
