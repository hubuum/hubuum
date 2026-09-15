use crate::models::HubuumClass;
use crate::services::schema_evolution as service;
use crate::storage::storage_handle;
use crate::{
    api::{openapi::ApiErrorResponse, response::ApiResponse},
    can,
    errors::ApiError,
    extractors::{AccessEventContext, Authenticated},
    models::{Permissions, schema_evolution::*},
    permissions::AppContext,
    traits::{SelfAccessors, UserPermissions},
};
use actix_web::{HttpRequest, HttpResponse, Responder, delete, get, http::StatusCode, post, web};
use hubuum_domain::{ClassId, CollectionId, SchemaReference, SchemaRevision, TaskId};
use hubuum_storage_core::{
    SchemaEvolutionStorage, StorageSchemaRepairReport, StorageSchemaWork, StorageSchemaWorkKind,
};

async fn authorized_report_source(
    context: &AppContext,
    requestor: &Authenticated,
    class_id: ClassId,
    task_id: TaskId,
) -> Result<(HubuumClass, StorageSchemaWork), ApiError> {
    let class = class_id.instance(context).await?;
    can!(
        context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    require_admin(context, requestor).await?;
    let work = storage_handle(context).get_schema_work(task_id).await?;
    if work.target().class_id() != class_id || work.kind() != StorageSchemaWorkKind::Impact {
        return Err(ApiError::NotFound(
            "Impact analysis was not found in this class".into(),
        ));
    }
    Ok((class, work))
}

fn repair_report_response(report: StorageSchemaRepairReport, download: bool) -> HttpResponse {
    let disposition = if download { "attachment" } else { "inline" };
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .insert_header(("Content-Disposition", format!("{disposition}; filename=\"schema-impact-{}.html\"", report.task_id())))
        .insert_header(("Cache-Control", "private, no-store"))
        .insert_header(("X-Content-Type-Options", "nosniff"))
        .insert_header(("Content-Security-Policy", "sandbox allow-popups allow-popups-to-escape-sandbox; default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; form-action 'none'"))
        .insert_header(("X-Hubuum-Report-Generated-At", report.generated_at().to_rfc3339()))
        .body(report.into_html())
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/schema/tasks/{task_id}/report",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("task_id"=TaskId,Path,description="Source impact analysis")),request_body=SchemaRepairReportRequest,responses((status=200,description="Generated and retained complete HTML rendering of saved findings",body=String,content_type="text/html"),(status=400,description="Invalid URL, layout or rendering budget",body=ApiErrorResponse),(status=403,description="Source analysis or template access denied",body=ApiErrorResponse),(status=404,description="Impact analysis not found",body=ApiErrorResponse),(status=413,description="Complete report exceeds assembly or output limit; previous report is retained",body=ApiErrorResponse)))]
#[post("/{class_id}/schema/tasks/{task_id}/report")]
pub async fn generate_schema_repair_report(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, TaskId)>,
    request: web::Json<SchemaRepairReportRequest>,
) -> Result<impl Responder, ApiError> {
    let (class_id, task_id) = path.into_inner();
    let (class, work) = authorized_report_source(&context, &requestor, class_id, task_id).await?;
    let request = request.into_inner();
    let layout = if let Some(id) = request.template_id {
        let template = id.instance(&context).await?;
        can!(
            &context,
            &requestor.principal,
            requestor.scopes(),
            [Permissions::ReadTemplate],
            template
        );
        let siblings = template.collection_siblings(&context).await?;
        for sibling in &siblings {
            can!(
                &context,
                &requestor.principal,
                requestor.scopes(),
                [Permissions::ReadTemplate],
                sibling
            );
        }
        Some(service::RepairReportLayout::try_new(template, siblings)?)
    } else {
        None
    };
    let generation = service::RepairReportGeneration::new(
        work,
        class.name,
        CollectionId::new(class.collection_id)?,
        request,
    )
    .with_layout(layout);
    let report = service::generate_repair_report(&context, generation).await?;
    Ok(repair_report_response(report, false))
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema/tasks/{task_id}/report",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("task_id"=TaskId,Path,description="Source impact analysis"),("download"=Option<bool>,Query,description="Download the retained HTML")),responses((status=200,description="Same retained HTML without reanalysis or rerendering",body=String,content_type="text/html"),(status=403,description="Source analysis access denied",body=ApiErrorResponse),(status=404,description="Analysis or retained report not found",body=ApiErrorResponse)))]
#[get("/{class_id}/schema/tasks/{task_id}/report")]
pub async fn get_schema_repair_report(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, TaskId)>,
    query: web::Query<SchemaRepairReportQuery>,
) -> Result<impl Responder, ApiError> {
    let (class_id, task_id) = path.into_inner();
    let (_, work) = authorized_report_source(&context, &requestor, class_id, task_id).await?;
    let report = service::retained_repair_report(&context, work.target(), task_id).await?;
    Ok(repair_report_response(report, query.download))
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema/objects",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("after"=Option<i64>,Query,description="Last inspected object ID"),("limit"=Option<usize>,Query,description="1 to 100 candidates"),("status"=Option<ComplianceStatus>,Query,description="Effective active-revision compliance")),responses((status=200,description="Authorized compliance page; totals and hidden objects are omitted",body=SchemaCompliancePage),(status=403,description="Forbidden",body=ApiErrorResponse)))]
#[get("/{class_id}/schema/objects")]
pub async fn list_object_compliance(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
    query: web::Query<SchemaPageRequest>,
) -> Result<impl Responder, ApiError> {
    let class_id = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let candidates = service::compliance(&context, class_id, &query).await?;
    let next_after = (candidates.len() == query.limit)
        .then(|| candidates.last().map(|object| i64::from(object.object_id)))
        .flatten();
    let resources =
        crate::services::authorization_resources::schema_compliance_authorization_resources(
            &context,
            candidates.iter().map(|object| object.object_id),
        )
        .await?;
    let mut items = Vec::new();
    for candidate in candidates {
        let Some(resource) = resources.get(&candidate.object_id) else {
            continue;
        };
        match crate::permissions::authorize_resources(
            context.permission_backend(),
            &context,
            &requestor.principal,
            requestor.scopes(),
            vec![Permissions::ReadObject],
            vec![resource.clone()],
        )
        .await
        {
            Ok(()) => items.push(candidate),
            Err(ApiError::Forbidden(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(ApiResponse::ok(SchemaCompliancePage { items, next_after }))
}

async fn require_admin(context: &AppContext, requestor: &Authenticated) -> Result<(), ApiError> {
    if requestor.scopes().is_some() || !context.is_admin(&requestor.principal).await? {
        return Err(ApiError::Forbidden(
            "Detailed class compliance reports require administrator access".into(),
        ));
    }
    Ok(())
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema/revisions",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("after"=Option<i64>,Query,description="Last schema revision; defaults to zero"),("limit"=Option<usize>,Query,description="1 to 100 revisions; defaults to 50")),responses((status=200,description="Bounded immutable schema revision list",body=Vec<SchemaRevisionResponse>),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[get("/{class_id}/schema/revisions")]
pub async fn list_schema_revisions(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
    query: web::Query<SchemaPageRequest>,
) -> Result<impl Responder, ApiError> {
    let class_id = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let result = service::list_revisions(&context, class_id, &query).await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID")),responses((status=200,description="Administrator class compliance report",body=ClassSchemaResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[get("/{class_id}/schema")]
pub async fn get_schema_state(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
) -> Result<impl Responder, ApiError> {
    let class_id = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    require_admin(&context, &requestor).await?;
    let result = service::class_state(&context, class_id).await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/schema/revisions",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID")),request_body=SchemaStageRequest,responses((status=201,description="Stage an immutable schema revision without activation",body=SchemaRevisionResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[post("/{class_id}/schema/revisions")]
pub async fn stage_schema_revision(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
    request: web::Json<SchemaStageRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let class_id = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );

    let result = service::stage(
        &context,
        CollectionId::new(class.collection_id)?,
        class_id,
        request.into_inner(),
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::CREATED))
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema/revisions/{revision}",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("revision"=SchemaRevision,Path,description="Positive immutable identity")),responses((status=200,description="Read an exact schema revision",body=SchemaRevisionResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[get("/{class_id}/schema/revisions/{revision}")]
pub async fn get_schema_revision(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, SchemaRevision)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, revision) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let result = service::revision(&context, SchemaReference::new(class_id, revision)).await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(delete,path="/api/v1/classes/{class_id}/schema/revisions/{revision}",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("revision"=SchemaRevision,Path,description="Positive immutable identity")),responses((status=200,description="Abandon a staged revision; active and retired documents remain immutable",body=SchemaRevisionResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[delete("/{class_id}/schema/revisions/{revision}")]
pub async fn abandon_schema_revision(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, SchemaRevision)>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, revision) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );

    let result = service::abandon(
        &context,
        CollectionId::new(class.collection_id)?,
        SchemaReference::new(class_id, revision),
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/schema/revisions/{revision}/activate",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("revision"=SchemaRevision,Path,description="Positive immutable identity")),request_body=SchemaActivationRequest,responses((status=200,description="Explicit activation; reject_incompatible requires an exact current impact proof for nonempty classes; allow_pending additionally requires administrator access",body=SchemaActivationResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[post("/{class_id}/schema/revisions/{revision}/activate")]
pub async fn activate_schema_revision(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, SchemaRevision)>,
    request: web::Json<SchemaActivationRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, revision) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    if matches!(request.policy, SchemaActivationPolicy::AllowPending) {
        require_admin(&context, &requestor).await?;
    }
    let result = service::activate(
        &context,
        CollectionId::new(class.collection_id)?,
        SchemaReference::new(class_id, revision),
        request.into_inner(),
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/schema/revisions/{revision}/impact",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("revision"=SchemaRevision,Path,description="Positive immutable identity")),responses((status=202,description="Queue bounded impact analysis; advisory results cannot authorize strict activation",body=SchemaWorkResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse),(status=413,description="Report exceeds the 16 MiB assembly budget; saved work is retained",body=ApiErrorResponse)))]
#[post("/{class_id}/schema/revisions/{revision}/impact")]
pub async fn analyze_schema_impact(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, SchemaRevision)>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, revision) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );

    require_admin(&context, &requestor).await?;
    let result = service::request_work(
        &context,
        CollectionId::new(class.collection_id)?,
        SchemaReference::new(class_id, revision),
        StorageSchemaWorkKind::Impact,
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::ACCEPTED))
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/schema/revisions/{revision}/revalidate",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("revision"=SchemaRevision,Path,description="Positive immutable identity")),responses((status=202,description="Queue deduplicated revalidation of the active revision",body=SchemaWorkResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse)))]
#[post("/{class_id}/schema/revisions/{revision}/revalidate")]
pub async fn revalidate_schema(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, SchemaRevision)>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, revision) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );

    require_admin(&context, &requestor).await?;
    let result = service::request_work(
        &context,
        CollectionId::new(class.collection_id)?,
        SchemaReference::new(class_id, revision),
        StorageSchemaWorkKind::Revalidation,
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::ACCEPTED))
}

#[utoipa::path(get,path="/api/v1/classes/{class_id}/schema/tasks/{task_id}",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("task_id"=TaskId,Path,description="Positive immutable identity")),responses((status=200,description="Administrator report with complete grouped impact mismatches and exact/advisory population boundary",body=SchemaWorkResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse),(status=413,description="Report exceeds the 16 MiB assembly budget; saved work is retained",body=ApiErrorResponse)))]
#[get("/{class_id}/schema/tasks/{task_id}")]
pub async fn get_schema_work(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, TaskId)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, task_id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    require_admin(&context, &requestor).await?;
    let result = service::get_work(&context, task_id).await?;
    if result.target.class_id() != class_id {
        return Err(ApiError::NotFound(
            "Schema task was not found in this class".into(),
        ));
    }
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(delete,path="/api/v1/classes/{class_id}/schema/tasks/{task_id}",tag="schema evolution",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"), ("task_id"=TaskId,Path,description="Positive immutable identity")),responses((status=200,description="Cancel schema work atomically; completed batches remain and later batch commits are fenced",body=SchemaWorkResponse),(status=400,description="Invalid schema, policy or bounded request",body=ApiErrorResponse),(status=403,description="Forbidden",body=ApiErrorResponse),(status=404,description="Class, revision or task not found",body=ApiErrorResponse),(status=409,description="Stale revision, incompatible activation or invalid lifecycle transition",body=ApiErrorResponse),(status=413,description="Report exceeds the 16 MiB assembly budget; saved work is retained",body=ApiErrorResponse)))]
#[delete("/{class_id}/schema/tasks/{task_id}")]
pub async fn cancel_schema_work(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, TaskId)>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, task_id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    require_admin(&context, &requestor).await?;
    let existing = storage_handle(&context).get_schema_work(task_id).await?;
    if existing.target().class_id() != class_id {
        return Err(ApiError::NotFound(
            "Schema task was not found in this class".into(),
        ));
    }
    let result = service::cancel_work(
        &context,
        CollectionId::new(class.collection_id)?,
        task_id,
        &requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}
