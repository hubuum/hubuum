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
use crate::tasks::{idempotency_key_from_headers, kick_task_worker};
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
        (status = 409, description = "Idempotency key conflict", body = ApiErrorResponse),
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
    let class = class_id.into_inner().instance(&context).await?;
    crate::can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let mut task = schema_profiling::submit(
        &context,
        &requestor,
        &class,
        options.into_inner(),
        idempotency_key_from_headers(req.headers())?,
    )
    .await?;
    super::tasks::discovery::redact(&context, &requestor, std::slice::from_mut(&mut task)).await?;
    let response = task.to_response()?;
    kick_task_worker(context.clone());
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
