use std::collections::HashMap;

use crate::api::etag::{RevisionedResource, revision_precondition_for_tag};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::{QueryParamsExt, parse_query_parameter};
use crate::models::{
    HubuumClassRelation, HubuumClassRelationID, HubuumObjectRelation, HubuumObjectRelationID,
    NewHubuumClassRelation, NewHubuumObjectRelation, ObjectRelationCreateSelector,
    ObjectRelationSelector, Permissions,
};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::permissions::visibility::authorize_cursor_page;
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::services::authorization_resources::{
    class_relation_authorization_resources, object_relation_authorization_resources,
};
use crate::services::relation_queries;
use crate::storage::with_revision_precondition;
use crate::traits::scope_allows;
use actix_web::delete;
use tracing::debug;

use crate::traits::Search;

use actix_web::{HttpRequest, Responder, get, http::StatusCode, routes, web};

#[utoipa::path(
    get,
    path = "/api/v1/relations/classes",
    tag = "relations",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Class relations matching optional query filters", body = [HubuumClassRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("classes")]
#[get("classes/")]
async fn get_class_relations(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "Listing class relations", user_id = user.id());

    let (classes, total_count) = if context
        .permission_backend()
        .supports_storage_visibility_filtering()
    {
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
        user.class_relations_page(&context, search_params, requestor.scopes())
            .await?
    } else {
        let mut required = params.filters.permissions()?;
        required.ensure_contains(&[Permissions::ReadClassRelation]);
        let required = required.iter().copied().collect::<Vec<_>>();
        if !scope_allows(requestor.scopes(), &required) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }

        let mut candidate_options = count_query_options(&params);
        candidate_options.include_total = false;
        let (candidates, _) = relation_queries::list_class_relations(
            &context,
            relation_queries::RelationAccess::new(user.id(), true, None),
            candidate_options,
        )
        .await?;
        let resources = class_relation_authorization_resources(&context, &candidates).await?;
        let resources = resources
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&context, user).await?;
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
        let page = authorize_cursor_page(
            context.permission_backend(),
            &principal,
            candidates,
            requestor.scopes(),
            required,
            &search_params,
            |relation| {
                resources
                    .get(&relation.id)
                    .expect("every relation candidate has an authorization resource")
                    .clone()
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    ApiResponse::paginated(classes, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/relations/classes/{relation_id}",
    tag = "relations",
    security(("bearer_auth" = [])),
    params(
        ("relation_id" = i32, Path, description = "Class relation ID")
    ),
    responses(
        (status = 200, description = "Class relation", body = HubuumClassRelation),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[get("classes/{relation_id}")]
async fn get_class_relation(
    context: AppContext,
    requestor: Authenticated,
    relation_id: web::Path<HubuumClassRelationID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Getting class relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let target = context
        .class_relation_service()
        .resolve(relation_id)
        .await?;
    let resource = target.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::ReadClassRelation],
        vec![resource],
    )
    .await?;

    ApiResponse::ok_revisioned(target.relation().clone())
}

#[utoipa::path(
    post,
    path = "/api/v1/relations/classes",
    tag = "relations",
    security(("bearer_auth" = [])),
    request_body = NewHubuumClassRelation,
    responses(
        (status = 201, description = "Class relation created", body = HubuumClassRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("classes")]
#[post("classes/")]
async fn create_class_relation(
    context: AppContext,
    requestor: Authenticated,
    relation: web::Json<NewHubuumClassRelation>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let relation = relation.into_inner();
    let user = &requestor.principal;

    debug!(
        message = "Creating class relation",
        user_id = user.id(),
        from_class = relation.from_hubuum_class_id,
        to_class = relation.to_hubuum_class_id,
    );

    let prepared = context
        .class_relation_service()
        .prepare_create(relation)
        .await?;
    let resource = prepared.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::CreateClassRelation],
        vec![resource],
    )
    .await?;

    let event_context = requestor.event_context(&req);
    let target = context
        .class_relation_service()
        .create(&prepared, &event_context)
        .await?;

    ApiResponse::revisioned(target.relation().clone(), StatusCode::CREATED)
}

#[utoipa::path(
    delete,
    path = "/api/v1/relations/classes/{relation_id}",
    tag = "relations",
    security(("bearer_auth" = [])),
    params(
        ("relation_id" = i32, Path, description = "Class relation ID")
    ),
    responses(
        (status = 204, description = "Class relation deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[delete("classes/{relation_id}")]
async fn delete_class_relation(
    context: AppContext,
    requestor: Authenticated,
    relation_id: web::Path<HubuumClassRelationID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Deleting class relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let target = context
        .class_relation_service()
        .resolve(relation_id)
        .await?;
    let resource = target.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::DeleteClassRelation],
        vec![resource],
    )
    .await?;

    let etag = target.relation().entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        context
            .class_relation_service()
            .delete(&target, &event_context),
    )
    .await?;

    Ok(ApiResponse::no_content_with_etag(etag))
}

#[utoipa::path(
    get,
    path = "/api/v1/relations/objects",
    tag = "relations",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Object relations matching optional query filters", body = [HubuumObjectRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("objects")]
#[get("objects/")]
async fn get_object_relations(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "Listing object relations", user_id = user.id());

    let (object_relations, total_count) = if context
        .permission_backend()
        .supports_storage_visibility_filtering()
    {
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        user.object_relations_page(&context, search_params, requestor.scopes())
            .await?
    } else {
        let mut required = params.filters.permissions()?;
        required.ensure_contains(&[Permissions::ReadObjectRelation]);
        let required = required.iter().copied().collect::<Vec<_>>();
        if !scope_allows(requestor.scopes(), &required) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }

        let mut candidate_options = count_query_options(&params);
        candidate_options.include_total = false;
        let (candidates, _) = relation_queries::list_object_relations(
            &context,
            relation_queries::RelationAccess::new(user.id(), true, None),
            candidate_options,
        )
        .await?;
        let resources = object_relation_authorization_resources(&context, &candidates).await?;
        let resources = resources
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&context, user).await?;
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        let page = authorize_cursor_page(
            context.permission_backend(),
            &principal,
            candidates,
            requestor.scopes(),
            required,
            &search_params,
            |relation| {
                resources
                    .get(&relation.id)
                    .expect("every relation candidate has an authorization resource")
                    .clone()
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    ApiResponse::paginated(object_relations, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/relations/objects/{relation_id}",
    tag = "relations",
    security(("bearer_auth" = [])),
    params(
        ("relation_id" = i32, Path, description = "Object relation ID")
    ),
    responses(
        (status = 200, description = "Object relation", body = HubuumObjectRelation),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[get("objects/{relation_id}")]
async fn get_object_relation(
    context: AppContext,
    requestor: Authenticated,
    relation_id: web::Path<HubuumObjectRelationID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Getting object relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let target = context
        .object_relation_service()
        .resolve(ObjectRelationSelector::by_id(relation_id))
        .await?;
    let resource = target.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::ReadObjectRelation],
        vec![resource],
    )
    .await?;

    ApiResponse::ok_revisioned(*target.relation())
}

#[utoipa::path(
    post,
    path = "/api/v1/relations/objects",
    tag = "relations",
    security(("bearer_auth" = [])),
    request_body = NewHubuumObjectRelation,
    responses(
        (status = 201, description = "Object relation created", body = HubuumObjectRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("objects")]
#[post("objects/")]
async fn create_object_relation(
    context: AppContext,
    requestor: Authenticated,
    relation: web::Json<NewHubuumObjectRelation>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let relation = relation.into_inner();
    let user = &requestor.principal;

    debug!(
        message = "Creating object relation",
        user_id = user.id(),
        from_object = relation.from_hubuum_object_id,
        to_object = relation.to_hubuum_object_id,
    );

    let prepared = context
        .object_relation_service()
        .prepare_create(ObjectRelationCreateSelector::explicit(relation))
        .await?;
    let resource = prepared.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::CreateObjectRelation],
        vec![resource],
    )
    .await?;

    let event_context = requestor.event_context(&req);
    let target = context
        .object_relation_service()
        .create(&prepared, &event_context)
        .await?;

    ApiResponse::revisioned(*target.relation(), StatusCode::CREATED)
}

#[utoipa::path(
    delete,
    path = "/api/v1/relations/objects/{relation_id}",
    tag = "relations",
    security(("bearer_auth" = [])),
    params(
        ("relation_id" = i32, Path, description = "Object relation ID")
    ),
    responses(
        (status = 204, description = "Object relation deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[delete("objects/{relation_id}")]
async fn delete_object_relation(
    context: AppContext,
    requestor: Authenticated,
    relation_id: web::Path<HubuumObjectRelationID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let relation_id = relation_id.into_inner();

    debug!(
        message = "Deleting object relation",
        user_id = user.id(),
        relation_id = ?relation_id,
    );

    let target = context
        .object_relation_service()
        .resolve(ObjectRelationSelector::by_id(relation_id))
        .await?;
    let resource = target.authorization_resource();
    authorize_resources(
        context.permission_backend(),
        &context,
        user,
        requestor.scopes(),
        vec![Permissions::DeleteObjectRelation],
        vec![resource],
    )
    .await?;

    let etag = target.relation().entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        context
            .object_relation_service()
            .delete(&target, &event_context),
    )
    .await?;

    Ok(ApiResponse::no_content_with_etag(etag))
}
