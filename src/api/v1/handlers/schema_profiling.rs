use crate::models::TaskDetailsExpiredResponse;
use crate::services::tasks::TaskSubmissionAttempt;
use crate::services::tasks::TaskSubmissionOperation;
use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};
use hubuum_domain::{ClassId, TaskId};
use hubuum_storage_core::{
    StorageTaskOutputLookup, TaskExecutionStorage, TaskMetadataDetails, TaskOutputMetadata,
};

use crate::api::{locations, openapi::ApiErrorResponse, response::ApiResponse};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::schema_profiling::{
    ProfileRequestWire, SchemaProfileReport, SchemaProfileRequest,
};
use crate::models::{Permissions, TaskKind, TaskResponse};
use crate::permissions::AppContext;
use crate::services::{schema_profiling, tasks::load_authorized_task};
use crate::storage::storage_handle;
use crate::tasks::idempotency_key_from_headers;
use crate::traits::{SelfAccessors, UserPermissions};

#[utoipa::path(post, path = "/api/v1/classes/{class_id}/schema/profiles", tag = "classes",
    summary = "Queue schema inference from authorized raw object data",
    security(("bearer_auth" = [])), request_body = ProfileRequestWire,
    params(("class_id" = i32, Path, description = "Class ID")),
    responses((status = 202, description = "Schema profiling task accepted", body = TaskResponse),
        (status = 400, description = "Invalid profiling options", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Class access denied", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse),
        (status = 410, description = "Submission recognized; no new work created and task details expired. Do not resubmit with a new key.", body = TaskDetailsExpiredResponse, headers(("Cache-Control" = String, description = "no-store; no automatic retry is implied"))),
        (status = 409, description = "Changed protected request; reason idempotency_key_conflict", body = ApiErrorResponse, headers(("Cache-Control" = String, description = "no-store for protected request conflicts; no Retry-After"))),
        (status = 429, description = "Shared task admission or byte capacity exhausted", body = ApiErrorResponse),
        (status = 503, description = "Task identity or control reconciliation unavailable", body = ApiErrorResponse))) ]
#[post("/{class_id}/schema/profiles")]
pub(crate) async fn create_schema_profile(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    class_id: web::Path<ClassId>,
    options: web::Json<SchemaProfileRequest>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let options = options.into_inner();
    let hash = crate::tasks::request_hash(
        &serde_json::json!({ "class_id": class_id, "options": &options }),
    )?;
    let attempt = TaskSubmissionAttempt::prepare(
        &context,
        &requestor.principal,
        idempotency_key_from_headers(req.headers())?,
        TaskSubmissionOperation::SchemaProfile,
        &(class_id, &options),
        Some(&hash),
    )
    .await?;
    let mut task = if let Some(task) = attempt.lookup(&context, &requestor.principal).await? {
        task
    } else {
        let class = class_id.instance(&context).await?;
        crate::can!(
            &context,
            &requestor.principal,
            requestor.scopes(),
            [Permissions::ReadClass],
            class
        );
        schema_profiling::submit(&context, &requestor, &class, options, attempt).await?
    };
    super::tasks::discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    let response = task.to_response()?;
    Ok(ApiResponse::accepted_at(
        response,
        locations::task(task.id)?,
    ))
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/schema/profiles/{task_id}", tag = "classes",
    summary = "Read an authorized schema profiling report",
    description = "Returns sampled observations, a candidate or inference findings, and replay results. Never modifies the class schema. Reports expire after 24 hours; access to every sampled object is rechecked.",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID"), ("task_id" = i32, Path, description = "Schema profiling task ID")),
    responses((status = 200, description = "Schema profiling report", body = SchemaProfileReport),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Current permissions or token scope do not cover the report", body = ApiErrorResponse),
        (status = 404, description = "Task, class, sampled object, or output not found", body = ApiErrorResponse),
        (status = 410, description = "Report expired", body = ApiErrorResponse))) ]
#[get("/{class_id}/schema/profiles/{task_id}")]
pub(crate) async fn get_schema_profile(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, TaskId)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, task_id) = path.into_inner();
    let task = load_authorized_task(&context, &requestor.principal, task_id).await?;
    let metadata = task.discovery_metadata.as_ref().map(|m| m.details());
    if task.kind != TaskKind::SchemaProfiling.as_str()
        || !matches!(metadata, Some(TaskMetadataDetails::SchemaProfiling { class_id: Some(id), .. }) if *id == class_id)
    {
        return Err(ApiError::NotFound("Schema profiling task not found".into()));
    }
    let class = class_id.instance(&context).await?;
    crate::can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    match storage_handle(&context)
        .get_schema_profile_output(task_id)
        .await?
    {
        StorageTaskOutputLookup::Available(output) => {
            schema_profiling::authorize_output(&context, &requestor, &task, output.report())
                .await?;
            Ok(ApiResponse::new_private_no_store(
                output.report()["report"].clone(),
                StatusCode::OK,
            ))
        }
        StorageTaskOutputLookup::Expired { .. } => {
            Err(ApiError::Gone("Schema profiling report expired".into()))
        }
        StorageTaskOutputLookup::Missing => {
            if matches!(
                metadata.and_then(TaskMetadataDetails::output),
                Some(TaskOutputMetadata::Produced { .. })
            ) {
                return Err(ApiError::Gone("Schema profiling report expired".into()));
            }
            Err(ApiError::NotFound(
                "Schema profiling report is not available; poll the task for status".into(),
            ))
        }
    }
}
