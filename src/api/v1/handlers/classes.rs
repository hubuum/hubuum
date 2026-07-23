use std::collections::HashMap;

use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};

use tracing::{debug, info};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::HistoryResponse;
use crate::can;
use crate::db::traits::authz::scope_allows;
use crate::db::traits::computed_field::enrich_objects_with_computed;
use crate::db::traits::history::{
    class_as_of, class_history_authorization_snapshots, class_history_paginated_with_total_count,
    object_as_of, object_history_authorization_snapshots,
    object_history_paginated_with_total_count,
};
use crate::db::traits::relations::{
    class_relation_authorization_resources, object_relation_authorization_resources,
};
use crate::db::traits::user::UserSearchBackend;
use crate::db::traits::{ClassRelation, ObjectRelationMemberships, UserPermissions};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated, ObjectDataPatchPayload};
use crate::models::collection as collection_model;
use crate::models::traits::{ExpandCollection, ToHubuumObjects, check_if_object_in_class};
use crate::pagination::{
    SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, page_limits,
    prepare_db_pagination,
};
use crate::permissions::visibility::authorize_cursor_page;
use crate::permissions::{
    AppContext, AuthzTarget, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
    authorize_resources,
};

use crate::models::traits::{
    CreateObjectInResolvedClass, DeleteResolvedClass, DeleteResolvedObject, PatchObjectData,
    ResolveClassTarget, ResolveObjectTarget, UpdateResolvedClass, UpdateResolvedObject,
};
use crate::models::{
    ClassGraphRow, ClassSelector, CollectionID, GroupPermission, HistoryAuthorizationSnapshot,
    HubuumClass, HubuumClassExpanded, HubuumClassHistory, HubuumClassID, HubuumClassRelation,
    HubuumClassRelationID, HubuumClassWithPath, HubuumObject, HubuumObjectHistory, HubuumObjectID,
    HubuumObjectReadResponse, HubuumObjectRelation, HubuumObjectWithPath, NewHubuumClass,
    NewHubuumClassRelationFromClass, NewHubuumObjectRelation, NewHubuumObjectRequest,
    ObjectDataPatchDocument, ObjectSelector, Permissions, RelatedClassGraph, RelatedObjectGraph,
    RelatedObjectGraphRow, ResolvedClassTarget, ResolvedObjectTarget, UpdateHubuumClass,
    UpdateHubuumObject, UpdateHubuumObjectRequest,
};
use crate::traits::{BackendContext, CanDelete, CanSave, Search, SelfAccessors};
use crate::utilities::extensions::CustomStringExtensions;

use crate::models::search::{
    FilterField, QueryOptions, QueryParamsExt, SearchOperator, parse_query_parameter,
    parse_query_parameter_with_computed_filters_and_passthrough,
    parse_query_parameter_with_passthrough,
};
use crate::models::traits::class_relation::ToHubuumClasses;
use crate::pagination::{Page, finalize_page};

fn parse_computed_include(query_string: &str) -> Result<(QueryOptions, bool), ApiError> {
    let (params, mut passthrough) =
        parse_query_parameter_with_passthrough(query_string, &["include"])?;
    let include_computed = match passthrough.remove("include") {
        None => false,
        Some(values) if values.as_slice() == ["computed"] => true,
        Some(_) => {
            return Err(ApiError::BadRequest(
                "include accepts exactly one value: computed".to_string(),
            ));
        }
    };
    Ok((params, include_computed))
}

fn parse_computed_object_list_query(query_string: &str) -> Result<(QueryOptions, bool), ApiError> {
    let (params, mut passthrough) =
        parse_query_parameter_with_computed_filters_and_passthrough(query_string, &["include"])?;
    let include_computed = match passthrough.remove("include") {
        None => false,
        Some(values) if values.as_slice() == ["computed"] => true,
        Some(_) => {
            return Err(ApiError::BadRequest(
                "include accepts exactly one value: computed".to_string(),
            ));
        }
    };
    Ok((params, include_computed))
}

fn scope_object_query_to_class(params: &mut QueryOptions, class: &HubuumClassID) {
    params
        .filters
        .retain(|param| !matches!(param.field, FilterField::ClassId | FilterField::Classes));
    params.filters.add_filter(
        FilterField::ClassId,
        SearchOperator::Equals { is_negated: false },
        &class.id().to_string(),
    );
}

async fn computed_personal_owner(
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
) -> Result<Option<i32>, ApiError> {
    if !requestor.principal.is_human() {
        return Ok(None);
    }
    let resource = class.to_resource_ref(pool.db_pool()).await?;
    match authorize_resources(
        pool.permission_backend(),
        pool,
        &requestor.principal,
        requestor.scopes(),
        vec![Permissions::ReadClass],
        vec![resource],
    )
    .await
    {
        Ok(()) => Ok(Some(requestor.principal.id)),
        Err(ApiError::Forbidden(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn object_read_page<T>(
    page: Page<T>,
    total_count: i64,
    effective_limit: usize,
    no_store: bool,
) -> Result<ApiResponse<Vec<HubuumObjectReadResponse>>, ApiError>
where
    T: Into<HubuumObjectReadResponse>,
{
    let items = page.items.into_iter().map(Into::into).collect();
    Ok(ApiResponse::paginated_items(
        items,
        &page.next_cursor,
        total_count,
        effective_limit,
        no_store,
    ))
}

fn object_with_root_path(object: &HubuumObject) -> HubuumObjectWithPath {
    HubuumObjectWithPath {
        id: object.id,
        name: object.name.clone(),
        collection_id: object.collection_id,
        hubuum_class_id: object.hubuum_class_id,
        data: object.data.clone(),
        description: object.description.clone(),
        created_at: object.created_at,
        updated_at: object.updated_at,
        path: vec![object.id],
    }
}

fn class_with_root_path(class: &HubuumClass) -> HubuumClassWithPath {
    HubuumClassWithPath {
        id: class.id,
        name: class.name.clone(),
        collection_id: class.collection_id,
        json_schema: class.json_schema.clone(),
        validate_schema: class.validate_schema,
        description: class.description.clone(),
        created_at: class.created_at,
        updated_at: class.updated_at,
        path: vec![class.id],
    }
}

#[derive(Debug, Default)]
struct RelatedObjectsOptions {
    ignore_classes: Vec<i32>,
    ignore_self_class: bool,
}

fn parse_related_objects_query(
    query_string: &str,
) -> Result<(QueryOptions, RelatedObjectsOptions), ApiError> {
    let (query_options, mut passthrough) = parse_query_parameter_with_passthrough(
        query_string,
        &["ignore_classes", "ignore_self_class"],
    )?;

    let ignore_classes = match passthrough.remove("ignore_classes") {
        Some(values) if values.len() > 1 => {
            return Err(ApiError::BadRequest("duplicate ignore_classes".into()));
        }
        Some(mut values) => Some(values.remove(0).as_integer()?),
        None => None,
    };
    let ignore_self_class = match passthrough.remove("ignore_self_class") {
        Some(values) if values.len() > 1 => {
            return Err(ApiError::BadRequest("duplicate ignore_self_class".into()));
        }
        Some(mut values) => Some(values.remove(0).as_boolean()?),
        None => None,
    };

    Ok((
        query_options,
        RelatedObjectsOptions {
            ignore_classes: ignore_classes.unwrap_or_default(),
            ignore_self_class: ignore_self_class.unwrap_or(true),
        },
    ))
}

fn ensure_object_update_stays_in_path_class(
    update: &UpdateHubuumObject,
    object: &HubuumObject,
) -> Result<(), ApiError> {
    if let Some(class_id) = update.hubuum_class_id
        && class_id != object.hubuum_class_id
    {
        return Err(ApiError::BadRequest(
            "Object class cannot be changed through a class-scoped object endpoint".to_string(),
        ));
    }

    if let Some(collection_id) = update.collection_id
        && collection_id != object.collection_id
    {
        return Err(ApiError::BadRequest(
            "Object collection cannot be changed through a class-scoped object endpoint"
                .to_string(),
        ));
    }

    Ok(())
}

fn prepare_graph_query_options(
    mut params: QueryOptions,
) -> Result<(QueryOptions, usize), ApiError> {
    if params.cursor.is_some() {
        return Err(ApiError::BadRequest(
            "Graph endpoint does not support cursor".to_string(),
        ));
    }

    let limit = page_limits()?.resolve(params.limit)?;
    params.limit = Some(limit + 1);

    Ok((params, limit))
}

fn ensure_graph_result_within_limit<T>(
    items: &[T],
    limit: usize,
    resource_name: &str,
) -> Result<(), ApiError> {
    if items.len() > limit {
        return Err(ApiError::BadRequest(format!(
            "Graph contains more than {limit} related {resource_name}; narrow the query or increase limit"
        )));
    }

    Ok(())
}

// GET /api/v1/classes, list all classes the user may see.
#[utoipa::path(
    get,
    path = "/api/v1/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Classes matching optional query filters", body = [HubuumClassExpanded]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
async fn get_classes(
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "Listing classes", user_id = user.id());

    let (classes, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let total_count = if params.include_total {
            user.count_classes(&pool, count_query_options(&params), requestor.scopes())
                .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let search_params = prepare_db_pagination::<HubuumClassExpanded>(&params)?;
        let classes = user
            .search_classes(&pool, search_params, requestor.scopes())
            .await?;
        (classes, total_count)
    } else {
        if !scope_allows(requestor.scopes(), &[Permissions::ReadClass]) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }
        let candidates = user
            .search_classes_from_backend_with_admin_status(
                &pool,
                count_query_options(&params),
                true,
                None,
            )
            .await?;
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumClassExpanded>(&params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
            requestor.scopes(),
            vec![Permissions::ReadClass],
            &search_params,
            |class| ResourceRef {
                kind: ResourceKind::Class,
                id: class.id,
                attrs: ResourceAttrs {
                    collection_id: Some(class.collection.id),
                    name: Some(class.name.clone()),
                    ..Default::default()
                },
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    ApiResponse::paginated(classes, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    request_body = NewHubuumClass,
    responses(
        (status = 201, description = "Class created", body = HubuumClassExpanded),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
async fn create_class(
    pool: AppContext,
    requestor: Authenticated,
    class_data: web::Json<NewHubuumClass>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_data = class_data.into_inner();

    debug!(
        message = "Creating class",
        user_id = user.id(),
        class_name = class_data.name
    );

    let collection = CollectionID::new(class_data.collection_id)?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::CreateClass],
        collection
    );

    let event_context = requestor.event_context(&req);
    let class = class_data
        .save(&pool, &event_context)
        .await?
        .expand_collection(&pool)
        .await?;

    let location = api_locations::class(class.id)?;
    Ok(ApiResponse::created(class, location))
}

async fn read_resolved_class(
    pool: &AppContext,
    requestor: &Authenticated,
    target: ResolvedClassTarget,
) -> Result<HubuumClassExpanded, ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        target.class()
    );
    target.class().expand_collection(pool).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Class", body = HubuumClassExpanded),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}")]
async fn get_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();

    debug!(
        message = "Getting class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let target = ClassSelector::by_id(class_id)
        .resolve_class_target(&pool)
        .await?;
    let class = read_resolved_class(&pool, &requestor, target).await?;

    Ok(ApiResponse::new(class, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}",
    tag = "classes",
    summary = "Get a class by name",
    description = "Name-addressed alias for the class endpoint. The explicit by-name segment always treats numeric-looking values as names.",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique class name")),
    responses(
        (status = 200, description = "Class", body = HubuumClassExpanded),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}")]
async fn get_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    let class = read_resolved_class(&pool, &requestor, target).await?;
    Ok(ApiResponse::new(class, StatusCode::OK))
}

async fn apply_resolved_class_update(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedClassTarget,
    update: UpdateHubuumClass,
) -> Result<HubuumClassExpanded, ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        target.class()
    );

    if let Some(target_collection_id) = update.collection_id
        && target_collection_id != target.class().collection_id
    {
        can!(
            pool,
            &requestor.principal,
            requestor.scopes(),
            [Permissions::CreateClass],
            CollectionID::new(target_collection_id)?
        );
    }

    let event_context = requestor.event_context(req);
    update
        .update_resolved_class(pool, &target, &event_context)
        .await?
        .expand_collection(pool)
        .await
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = UpdateHubuumClass,
    responses(
        (status = 200, description = "Updated class", body = HubuumClassExpanded),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[patch("/{class_id}")]
async fn update_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    class_data: web::Json<UpdateHubuumClass>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let class_data = class_data.into_inner();

    debug!(
        message = "Updating class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let target = ClassSelector::by_id(class_id)
        .resolve_class_target(&pool)
        .await?;
    let class = apply_resolved_class_update(&pool, &requestor, &req, target, class_data).await?;
    Ok(ApiResponse::new(class, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/by-name/{class_name}",
    tag = "classes",
    summary = "Update a class by name",
    description = "Name-addressed alias for class update. Authorization is bound to the resolved class, and the original name plus resolved ID are rechecked under the transactional row lock.",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique current class name")),
    request_body = UpdateHubuumClass,
    responses(
        (status = 200, description = "Updated class", body = HubuumClassExpanded),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found or concurrently renamed", body = ApiErrorResponse)
    )
)]
#[patch("/by-name/{class_name}")]
async fn update_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    class_data: web::Json<UpdateHubuumClass>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    let class =
        apply_resolved_class_update(&pool, &requestor, &req, target, class_data.into_inner())
            .await?;
    Ok(ApiResponse::new(class, StatusCode::OK))
}

async fn delete_resolved_class(
    pool: &AppContext,
    requestor: &Authenticated,
    req: &HttpRequest,
    target: ResolvedClassTarget,
) -> Result<(), ApiError> {
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DeleteClass],
        target.class()
    );

    let event_context = requestor.event_context(req);
    target.delete_resolved_class(pool, &event_context).await
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 204, description = "Class deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}")]
async fn delete_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();

    debug!(
        message = "Deleting class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let target = ClassSelector::by_id(class_id)
        .resolve_class_target(&pool)
        .await?;
    delete_resolved_class(&pool, &requestor, &req, target).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/by-name/{class_name}",
    tag = "classes",
    summary = "Delete a class by name",
    description = "Name-addressed alias for class deletion. The original name and resolved ID are rechecked under the transactional row lock.",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique current class name")),
    responses(
        (status = 204, description = "Class deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found or concurrently renamed", body = ApiErrorResponse)
    )
)]
#[delete("/by-name/{class_name}")]
async fn delete_class_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    delete_resolved_class(&pool, &requestor, &req, target).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/permissions",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Collection-group permission mappings for class collection", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/permissions")]
async fn get_class_permissions(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_resolved_class_permissions(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/permissions",
    tag = "classes",
    summary = "Get class permissions by class name",
    security(("bearer_auth" = [])),
    params(("class_name" = String, Path, description = "Globally unique class name")),
    responses(
        (status = 200, description = "Collection-group permission mappings for class collection", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/permissions")]
async fn get_class_permissions_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_resolved_class_permissions(pool, requestor, target, req).await
}

async fn read_resolved_class_permissions(
    pool: AppContext,
    requestor: Authenticated,
    target: ResolvedClassTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<GroupPermission>>, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;
    let class = target.class();

    debug!(
        message = "Getting class permissions",
        user_id = user.id(),
        class_id = class.id
    );

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let target_collection_id = CollectionID::new(class.collection_id)?;
    let class_permissions = [
        Permissions::CreateClass,
        Permissions::UpdateClass,
        Permissions::ReadClass,
        Permissions::DeleteClass,
    ];
    let search_params = prepare_db_pagination::<GroupPermission>(&params)?;
    let (permissions, total_count) = if pool.permission_backend().uses_sql_permission_store() {
        let total_count = if params.include_total {
            collection_model::count_groups_on_paginated(
                &pool,
                target_collection_id,
                class_permissions.to_vec(),
                &count_query_options(&params),
            )
            .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let permissions = collection_model::groups_on_paginated(
            &pool,
            target_collection_id,
            class_permissions.to_vec(),
            &search_params,
        )
        .await?;
        (permissions, total_count)
    } else {
        pool.permission_backend()
            .groups_with_permissions_on(
                target_collection_id.id(),
                &class_permissions,
                &search_params,
            )
            .await?
    };

    ApiResponse::paginated(permissions, total_count, &params)
}

mod class_objects;
mod class_related;
mod computed_objects;
mod history_endpoints;
pub(crate) mod object_aggregates;
mod object_related;

pub use class_objects::*;
pub use class_related::*;
pub use history_endpoints::*;
pub(crate) use object_aggregates::{get_object_aggregates, get_object_aggregates_by_name};
pub use object_related::*;
