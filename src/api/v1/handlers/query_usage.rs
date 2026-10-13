use crate::{
    api::{openapi::ApiErrorResponse, response::ApiResponse},
    can,
    errors::ApiError,
    extractors::{AccessEventContext, Authenticated},
    models::{Permissions, query_usage::*},
    permissions::AppContext,
    services::query_usage as service,
    traits::{SelfAccessors, UserPermissions},
};
use actix_web::{
    HttpRequest, HttpResponse, Responder, delete, get, http::StatusCode, post, put, web,
};
use hubuum_domain::{ClassId, CollectionId, ResourceId, ResourceRevision};
use hubuum_storage_core::{
    StorageQueryUsageCreate, StorageQueryUsageReplace, StorageQueryUsageScope,
};

#[utoipa::path(get,path="/api/v1/classes/{class_id}/query-usage",tag="query usage",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID")),responses((status=200,description="Class-scoped advisory query usage",body=Vec<QueryUsageResponse>),(status=400,description="Invalid declaration",body=ApiErrorResponse),(status=403,description="Class permission required",body=ApiErrorResponse),(status=404,description="Class or declaration not found",body=ApiErrorResponse),(status=409,description="Stale revision, duplicate declaration or class limit",body=ApiErrorResponse)))]
#[get("/{class_id}/query-usage")]
pub async fn list_query_usage(
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
    let scope = StorageQueryUsageScope::new(class_id, CollectionId::new(class.collection_id)?);
    let result = service::list(&context, scope, class.json_schema.as_ref()).await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/query-usage",tag="query usage",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID")),request_body=QueryUsageCreateRequest,responses((status=201,description="Class-scoped advisory query usage",body=QueryUsageResponse),(status=400,description="Invalid declaration",body=ApiErrorResponse),(status=403,description="Class permission required",body=ApiErrorResponse),(status=404,description="Class or declaration not found",body=ApiErrorResponse),(status=409,description="Stale revision, duplicate declaration or class limit",body=ApiErrorResponse)))]
#[post("/{class_id}/query-usage")]
pub async fn create_query_usage(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
    request: web::Json<QueryUsageCreateRequest>,
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
    let scope = StorageQueryUsageScope::new(class_id, CollectionId::new(class.collection_id)?);
    let result = service::create(
        &context,
        scope,
        request.into_inner().pattern,
        class.json_schema.as_ref(),
        requestor.event_context(&http_request),
    )
    .await?;
    Ok(ApiResponse::new(result, StatusCode::CREATED))
}

#[utoipa::path(put,path="/api/v1/classes/{class_id}/query-usage/{declaration_id}",tag="query usage",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("declaration_id"=ResourceId,Path,description="Declaration ID")),request_body=QueryUsageReplaceRequest,responses((status=200,description="Class-scoped advisory query usage",body=QueryUsageResponse),(status=400,description="Invalid declaration",body=ApiErrorResponse),(status=403,description="Class permission required",body=ApiErrorResponse),(status=404,description="Class or declaration not found",body=ApiErrorResponse),(status=409,description="Stale revision, duplicate declaration or class limit",body=ApiErrorResponse)))]
#[put("/{class_id}/query-usage/{declaration_id}")]
pub async fn replace_query_usage(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, ResourceId)>,
    request: web::Json<QueryUsageReplaceRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let scope = StorageQueryUsageScope::new(class_id, CollectionId::new(class.collection_id)?);
    let request = request.into_inner();
    let replacement = StorageQueryUsageReplace::new(
        StorageQueryUsageCreate::new(
            scope,
            request.pattern,
            requestor.event_context(&http_request),
        ),
        id,
        request.expected_revision,
    );
    let result = service::replace(&context, replacement, class.json_schema.as_ref()).await?;
    Ok(ApiResponse::new(result, StatusCode::OK))
}

#[utoipa::path(delete,path="/api/v1/classes/{class_id}/query-usage/{declaration_id}",tag="query usage",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID"),("declaration_id"=ResourceId,Path,description="Declaration ID"),("expected_revision"=ResourceRevision,Query,description="Current declaration revision")),responses((status=204,description="Class-scoped advisory query usage"),(status=400,description="Invalid declaration",body=ApiErrorResponse),(status=403,description="Class permission required",body=ApiErrorResponse),(status=404,description="Class or declaration not found",body=ApiErrorResponse),(status=409,description="Stale revision, duplicate declaration or class limit",body=ApiErrorResponse)))]
#[delete("/{class_id}/query-usage/{declaration_id}")]
pub async fn delete_query_usage(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(ClassId, ResourceId)>,
    request: web::Query<QueryUsageDeleteRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let scope = StorageQueryUsageScope::new(class_id, CollectionId::new(class.collection_id)?);
    service::delete(
        &context,
        scope,
        id,
        request.expected_revision,
        requestor.event_context(&http_request),
    )
    .await?;
    Ok(HttpResponse::NoContent().finish())
}

#[utoipa::path(post,path="/api/v1/classes/{class_id}/query-usage/analysis",tag="query usage",security(("bearer_auth"=[])),params(("class_id"=ClassId,Path,description="Class ID")),request_body=QueryUsageAnalysisRequest,responses((status=200,description="Read-only administrative review; availability and evidence are explicit",body=QueryUsageAnalysisResponse),(status=400,description="Invalid or excessive proposed patterns",body=ApiErrorResponse),(status=403,description="Unscoped administrator and class read access required",body=ApiErrorResponse),(status=404,description="Class not found",body=ApiErrorResponse)))]
#[post("/{class_id}/query-usage/analysis")]
pub async fn analyze_query_usage(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<ClassId>,
    request: web::Json<QueryUsageAnalysisRequest>,
) -> Result<impl Responder, ApiError> {
    if requestor.scopes().is_some() || !context.is_admin(&requestor.principal).await? {
        return Err(ApiError::Forbidden(
            "Query usage analysis requires unscoped administrator access".into(),
        ));
    }
    let class_id = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let scope = StorageQueryUsageScope::new(class_id, CollectionId::new(class.collection_id)?);
    Ok(ApiResponse::ok(
        service::analyze(&context, scope, request.into_inner().proposed).await?,
    ))
}
