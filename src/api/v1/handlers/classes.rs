use std::collections::HashMap;

use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};

use tracing::{debug, info};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::HistoryResponse;
use crate::can;
use crate::db::traits::authz::scope_allows;
use crate::db::traits::computed_field::{
    enrich_objects_with_computed, enrich_objects_with_computed_sort_snapshot,
    resolve_computed_query_fields,
};
use crate::db::traits::history::{
    class_as_of, class_history_paginated_with_total_count, object_as_of,
    object_history_paginated_with_total_count,
};
use crate::db::traits::relations::{
    class_relation_authorization_resources, object_relation_authorization_resources,
};
use crate::db::traits::user::UserSearchBackend;
use crate::db::traits::{ClassRelation, ObjectRelationMemberships, UserPermissions};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::collection as collection_model;
use crate::models::traits::{
    ExpandCollection, ToHubuumObjects, check_if_object_in_class, object_cursor_sql_fields,
};
use crate::pagination::{
    SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, known_count_or_skipped,
    page_limits, paginate_in_memory_with_fields, prepare_db_pagination, validate_page_limit,
};
use crate::permissions::visibility::{authorize_all_candidates, authorize_cursor_page};
use crate::permissions::{
    AppContext, AuthzTarget, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
    authorize_resources,
};

use crate::models::{
    ClassGraphRow, CollectionID, GroupPermission, HubuumClass, HubuumClassExpanded,
    HubuumClassHistory, HubuumClassID, HubuumClassRelation, HubuumClassRelationID,
    HubuumClassWithPath, HubuumObject, HubuumObjectComputedResponse, HubuumObjectHistory,
    HubuumObjectID, HubuumObjectRelation, HubuumObjectWithPath, NewHubuumClass,
    NewHubuumClassRelationFromClass, NewHubuumObject, NewHubuumObjectRelation,
    NewHubuumObjectRequest, Permissions, RelatedClassGraph, RelatedObjectGraph,
    RelatedObjectGraphRow, UpdateHubuumClass, UpdateHubuumObject, UpdateHubuumObjectRequest,
};
use crate::traits::{BackendContext, CanDelete, CanSave, CanUpdate, Search, SelfAccessors};
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

async fn can_list_objects_in_class(
    pool: &AppContext,
    requestor: &Authenticated,
    class: &HubuumClass,
) -> Result<bool, ApiError> {
    let resource = class.to_resource_ref(pool.db_pool()).await?;
    let permissions = if pool.permission_backend().supports_sql_visibility_pushdown() {
        vec![Permissions::ReadObject, Permissions::ReadCollection]
    } else {
        vec![Permissions::ReadObject]
    };
    match authorize_resources(
        pool.permission_backend(),
        pool,
        &requestor.principal,
        requestor.scopes(),
        permissions,
        vec![resource],
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(ApiError::Forbidden(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

fn serialized_object_page<T: serde::Serialize>(
    page: Page<T>,
    total_count: i64,
    effective_limit: usize,
    no_store: bool,
) -> Result<ApiResponse<serde_json::Value>, ApiError> {
    Ok(ApiResponse::paginated_items(
        serde_json::to_value(page.items)?,
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

fn ensure_new_object_matches_path_class(
    object: &NewHubuumObject,
    class: &HubuumClass,
) -> Result<(), ApiError> {
    if object.hubuum_class_id != class.id {
        return Err(ApiError::BadRequest(format!(
            "Object hubuum_class_id {} does not match path class_id {}",
            object.hubuum_class_id, class.id
        )));
    }

    if object.collection_id != class.collection_id {
        return Err(ApiError::BadRequest(format!(
            "Object collection_id {} does not match class collection_id {}",
            object.collection_id, class.collection_id
        )));
    }

    Ok(())
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

    let (default_limit, _) = page_limits()?;
    let limit = validate_page_limit(params.limit.unwrap_or(default_limit))?;
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
    let class = class_id.into_inner();

    debug!(
        message = "Getting class",
        user_id = user.id(),
        class_id = class.id()
    );

    let class = class.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let class = class.expand_collection(&pool).await?;

    Ok(ApiResponse::new(class, StatusCode::OK))
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

    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );

    if let Some(target_collection_id) = class_data.collection_id
        && target_collection_id != class.collection_id
    {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateClass],
            CollectionID::new(target_collection_id)?
        );
    }

    let event_context = requestor.event_context(&req);
    let class = class_data
        .update(&pool, class.id, &event_context)
        .await?
        .expand_collection(&pool)
        .await?;
    Ok(ApiResponse::new(class, StatusCode::OK))
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

    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::DeleteClass],
        class
    );

    let event_context = requestor.event_context(&req);
    class.delete(&pool, &event_context).await?;
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
    use crate::traits::CollectionAccessors;

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let params = parse_query_parameter(req.query_string())?;

    debug!(
        message = "Getting class permissions",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let target_collection_id = class.collection_id(&pool).await?;
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

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/related/classes",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Classes connected to the class", body = [HubuumClassWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/classes")]
#[get("/{class_id}/related/classes/")]
async fn get_related_classes(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let params = parse_query_parameter(req.query_string())?;
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let search_params = prepare_db_pagination::<ClassGraphRow>(&params)?;
    let (classes, total_count) = user
        .classes_related_to_page(&pool, class, search_params, requestor.scopes())
        .await?;

    ApiResponse::mapped_paginated(classes, total_count, &params, |page| {
        page.to_descendant_classes_with_path()
    })
}

// Contextual post for class relations
#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = NewHubuumClassRelationFromClass,
    responses(
        (status = 201, description = "Class relation created", body = HubuumClassRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("/{class_id}/relations")]
#[post("/{class_id}/relations/")]
async fn create_class_relation(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    relation_data: web::Json<NewHubuumClassRelationFromClass>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::models::NewHubuumClassRelation;
    use crate::traits::CollectionAccessors;

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let partial_relation = relation_data.into_inner();

    debug!(
        message = "Creating class relation",
        user_id = user.id(),
        from_class = class_id.id(),
        to_class = partial_relation.to_hubuum_class_id,
    );

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_id.id(),
        to_hubuum_class_id: partial_relation.to_hubuum_class_id,
        forward_template_alias: partial_relation.forward_template_alias.clone(),
        reverse_template_alias: partial_relation.reverse_template_alias.clone(),
    };

    if pool.permission_backend().uses_sql_permission_store() {
        let ids = relation.collection_id(&pool).await?;
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateClassRelation],
            ids.0,
            ids.1
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::CreateClassRelation],
            vec![resource],
        )
        .await?;
    }

    let event_context = requestor.event_context(&req);
    let relation = relation.save(&pool, &event_context).await?;

    let location = api_locations::class_relation(class_id.id(), relation.id())?;
    Ok(ApiResponse::created(relation, location))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/relations/{relation_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("relation_id" = i32, Path, description = "Class relation ID")
    ),
    responses(
        (status = 204, description = "Class relation deleted"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or relation not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/relations/{relation_id}")]
async fn delete_class_relation(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumClassRelationID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::traits::CollectionAccessors;

    let user = &requestor.principal;
    let (class_id, relation_id) = paths.into_inner();

    debug!(
        message = "Deleting class relation",
        user_id = user.id(),
        class_id = class_id.id(),
        relation_id = relation_id.id()
    );

    let relation = relation_id.instance(&pool).await?;

    if pool.permission_backend().uses_sql_permission_store() {
        let ids = relation_id.collection_id(&pool).await?;
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::DeleteClassRelation],
            ids.0,
            ids.1
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::DeleteClassRelation],
            vec![resource],
        )
        .await?;
    }

    if relation.from_hubuum_class_id == class_id.id()
        || relation.to_hubuum_class_id == class_id.id()
    {
        let event_context = requestor.event_context(&req);
        relation.delete(&pool, &event_context).await?;
        Ok(ApiResponse::no_content())
    } else {
        info!(
            message = "Relation membership mismatch when deleting relation: class does not match either endpoint",
            user_id = user.id(),
            class_id = class_id.id(),
            relation_id = relation_id.id(),
            relation_from_class = relation.from_hubuum_class_id,
            relation_to_class = relation.to_hubuum_class_id
        );
        Err(ApiError::BadRequest(format!(
            "Class {} is not part of relation {}.",
            class_id.id(),
            relation.id,
        )))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/related/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Direct relations touching the class", body = [HubuumClassRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/relations")]
#[get("/{class_id}/related/relations/")]
async fn get_related_class_relations(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let params = parse_query_parameter(req.query_string())?;
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    debug!(
        message = "Getting direct relations touching class",
        user_id = user.id(),
        class_id = class_id.id()
    );

    let (relations, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
        user.class_relations_touching_page(&pool, class, search_params, requestor.scopes())
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
        let (candidates, _) = user
            .class_relations_touching_page_from_backend_with_admin_status(
                &pool,
                class,
                candidate_options,
                true,
                None,
            )
            .await?;
        let resources = class_relation_authorization_resources(&pool, &candidates)
            .await?
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumClassRelation>(&params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
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
    ApiResponse::paginated(relations, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/related/graph",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    responses(
        (status = 200, description = "Neighborhood graph for the class", body = RelatedClassGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/related/graph")]
#[get("/{class_id}/related/graph/")]
async fn get_related_class_graph(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let (params, graph_limit) =
        prepare_graph_query_options(parse_query_parameter(req.query_string())?)?;
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );

    let root_class = class_with_root_path(&class);
    let connected_classes = user
        .search_classes_related_to(&pool, class, params, requestor.scopes())
        .await?;
    ensure_graph_result_within_limit(&connected_classes, graph_limit, "classes")?;
    let mut classes = Vec::with_capacity(connected_classes.len() + 1);
    classes.push(root_class);
    classes.extend(connected_classes.to_descendant_classes_with_path());

    let class_ids = classes.iter().map(|item| item.id).collect::<Vec<_>>();
    let relations = user
        .search_class_relations_between_ids(&pool, &class_ids, requestor.scopes())
        .await?;

    Ok(ApiResponse::new(
        RelatedClassGraph { classes, relations },
        StatusCode::OK,
    ))
}

//
// Object API
//

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    description = "Lists objects in the path class. Enabled computed fields can be filtered with computed.shared.<key> or computed.personal.<key> using the normal __operator suffix, and sorted with the same names.",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich each object"),
        ("sort" = Option<String>, Query, description = "Sort by object fields or computed.shared.<key>/computed.personal.<key>; computed sorting supports at most two explicit sort fields")
    ),
    responses(
        (status = 200, description = "Objects in class, optionally enriched with computed fields", body = [crate::models::HubuumObjectReadResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/")]
async fn get_objects_in_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class = class_id.into_inner();
    let query_string = req.query_string();

    let (mut params, include_computed) = parse_computed_object_list_query(query_string)?;

    // The path is authoritative even if the caller supplied a conflicting
    // class_id/classes filter.
    scope_object_query_to_class(&mut params, &class);

    debug!(
        message = "Getting objects in class",
        user_id = user.id(),
        class_id = class.id(),
        query = query_string
    );

    let computed_querying = params
        .filters
        .iter()
        .any(|filter| filter.field.computed_sort().is_some())
        || params
            .sort
            .iter()
            .any(|sort| sort.field.computed_sort().is_some());
    if computed_querying {
        if !scope_allows(requestor.scopes(), &[Permissions::ReadObject]) {
            return serialized_object_page(
                Page::<HubuumObjectComputedResponse> {
                    items: Vec::new(),
                    next_cursor: None,
                },
                known_count_or_skipped(&params, 0),
                effective_page_limit(&params)?,
                true,
            );
        }
        let class_instance = class.instance(&pool).await?;
        if !can_list_objects_in_class(&pool, &requestor, &class_instance).await? {
            return serialized_object_page(
                Page::<HubuumObjectComputedResponse> {
                    items: Vec::new(),
                    next_cursor: None,
                },
                known_count_or_skipped(&params, 0),
                effective_page_limit(&params)?,
                true,
            );
        }
        let personal_owner = computed_personal_owner(&pool, &requestor, &class_instance).await?;
        let computed_sort_snapshot = resolve_computed_query_fields(
            pool.db_pool(),
            class_instance.id,
            personal_owner,
            &mut params.filters,
            &mut params.sort,
        )
        .await?;

        let total_count;
        let enriched = if pool.permission_backend().supports_sql_visibility_pushdown() {
            total_count = if params.include_total {
                user.count_objects(&pool, count_query_options(&params), requestor.scopes())
                    .await?
            } else {
                SKIPPED_TOTAL_COUNT
            };
            let search_params = prepare_db_pagination::<HubuumObjectComputedResponse>(&params)?;
            let objects = user
                .search_objects(&pool, search_params, requestor.scopes())
                .await?;
            enrich_objects_with_computed_sort_snapshot(
                pool.db_pool(),
                objects,
                personal_owner,
                &computed_sort_snapshot,
            )
            .await?
        } else {
            let candidates = user
                .search_objects_from_backend_with_admin_status(
                    &pool,
                    count_query_options(&params),
                    true,
                    None,
                )
                .await?;
            let principal = PrincipalRef::load(&pool, user).await?;
            let authorized = authorize_all_candidates(
                pool.permission_backend(),
                &principal,
                candidates,
                vec![Permissions::ReadObject],
                |object| ResourceRef {
                    kind: ResourceKind::Object,
                    id: object.id,
                    attrs: ResourceAttrs {
                        collection_id: Some(object.collection_id),
                        class_id: Some(object.hubuum_class_id),
                        name: Some(object.name.clone()),
                        ..Default::default()
                    },
                },
            )
            .await?;
            total_count = known_count_or_skipped(&params, authorized.len() as i64);
            let enriched = enrich_objects_with_computed_sort_snapshot(
                pool.db_pool(),
                authorized,
                personal_owner,
                &computed_sort_snapshot,
            )
            .await?;
            let search_params = prepare_db_pagination::<HubuumObjectComputedResponse>(&params)?;
            let cursor_fields = object_cursor_sql_fields(&search_params.sort)?;
            paginate_in_memory_with_fields(enriched, &search_params, &cursor_fields)?
        };
        let page = finalize_page(enriched, &params)?;
        if include_computed {
            return serialized_object_page(page, total_count, effective_page_limit(&params)?, true);
        }
        return serialized_object_page(
            Page {
                items: page
                    .items
                    .into_iter()
                    .map(|response| response.object)
                    .collect(),
                next_cursor: page.next_cursor,
            },
            total_count,
            effective_page_limit(&params)?,
            true,
        );
    }

    let (objects, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let total_count = if params.include_total {
            user.count_objects(&pool, count_query_options(&params), requestor.scopes())
                .await?
        } else {
            SKIPPED_TOTAL_COUNT
        };
        let search_params = prepare_db_pagination::<HubuumObject>(&params)?;
        let objects = user
            .search_objects(&pool, search_params, requestor.scopes())
            .await?;
        (objects, total_count)
    } else {
        if !scope_allows(requestor.scopes(), &[Permissions::ReadObject]) {
            let page = finalize_page(Vec::<HubuumObject>::new(), &params)?;
            return serialized_object_page(
                page,
                0,
                effective_page_limit(&params)?,
                include_computed,
            );
        }
        let candidates = user
            .search_objects_from_backend_with_admin_status(
                &pool,
                count_query_options(&params),
                true,
                None,
            )
            .await?;
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumObject>(&params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
            vec![Permissions::ReadObject],
            &search_params,
            |object| ResourceRef {
                kind: ResourceKind::Object,
                id: object.id,
                attrs: ResourceAttrs {
                    collection_id: Some(object.collection_id),
                    class_id: Some(object.hubuum_class_id),
                    name: Some(object.name.clone()),
                    ..Default::default()
                },
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

    let page = finalize_page(objects, &params)?;
    if include_computed {
        let class = class.instance(&pool).await?;
        let personal_owner = computed_personal_owner(&pool, &requestor, &class).await?;
        let next_cursor = page.next_cursor;
        let enriched = enrich_objects_with_computed(&pool, page.items, personal_owner).await?;
        serialized_object_page(
            Page {
                items: enriched,
                next_cursor,
            },
            total_count,
            effective_page_limit(&params)?,
            true,
        )
    } else {
        serialized_object_page(page, total_count, effective_page_limit(&params)?, false)
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID")
    ),
    request_body = NewHubuumObjectRequest,
    responses(
        (status = 201, description = "Object created in class", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/")]
async fn create_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    object_data: web::Json<NewHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let object_data = object_data.into_inner().into_domain()?;

    debug!(
        message = "Creating object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_data = object_data.name,
    );

    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::CreateObject],
        class
    );
    ensure_new_object_matches_path_class(&object_data, &class)?;

    let event_context = requestor.event_context(&req);
    let object = object_data.save(&pool, &event_context).await?;

    let location = api_locations::class_object(class.id, object.id())?;
    Ok(ApiResponse::created(object, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("include" = Option<String>, Query, description = "Set to computed to enrich the object")
    ),
    responses(
        (status = 200, description = "Object in class, optionally enriched with computed fields", body = crate::models::HubuumObjectReadResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}")]
async fn get_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Getting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    let (_, include_computed) = parse_computed_include(req.query_string())?;
    if include_computed {
        let class = class_id.instance(&pool).await?;
        let personal_owner = computed_personal_owner(&pool, &requestor, &class).await?;
        let enriched = enrich_objects_with_computed(&pool, vec![object], personal_owner)
            .await?
            .pop()
            .ok_or_else(|| {
                ApiError::InternalServerError(
                    "Computed object enrichment returned no object".to_string(),
                )
            })?;
        Ok(ApiResponse::new_private_no_store(
            serde_json::to_value(enriched)?,
            StatusCode::OK,
        ))
    } else {
        Ok(ApiResponse::new(
            serde_json::to_value(object)?,
            StatusCode::OK,
        ))
    }
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    request_body = UpdateHubuumObjectRequest,
    responses(
        (status = 200, description = "Updated object", body = HubuumObject),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
#[patch("/{class_id}/{object_id}")]
async fn patch_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    object_data: web::Json<UpdateHubuumObjectRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();
    let object_data = object_data.into_inner().into_domain()?;

    debug!(
        message = "Updating object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::UpdateObject],
        object
    );
    ensure_object_update_stays_in_path_class(&object_data, &object)?;

    let event_context = requestor.event_context(&req);
    let object = object_data.update(&pool, object.id, &event_context).await?;
    Ok(ApiResponse::new(object, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/{object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 204, description = "Object deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Object not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/{object_id}")]
async fn delete_object_in_class(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    debug!(
        message = "Deleting object in class",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object_id.id()
    );

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::DeleteObject],
        object
    );

    let event_context = requestor.event_context(&req);
    object.delete(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/objects",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("ignore_classes" = Option<String>, Query, description = "Comma-separated class IDs to exclude from the returned connected objects"),
        ("ignore_self_class" = Option<bool>, Query, description = "Exclude connected objects in the same class as the root object. Defaults to true")
    ),
    responses(
        (status = 200, description = "Objects connected to the object", body = [HubuumObjectWithPath]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/objects")]
#[get("/{class_id}/objects/{object_id}/related/objects/")]
async fn get_related_objects(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();
    let query_string = req.query_string();

    let (mut params, related_options) = parse_related_objects_query(query_string)?;

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    if related_options.ignore_self_class {
        params.filters.add_filter(
            FilterField::ClassId,
            SearchOperator::Equals { is_negated: true },
            &object.hubuum_class_id.to_string(),
        );
    }

    if !related_options.ignore_classes.is_empty() {
        params.filters.add_filter(
            FilterField::ClassId,
            SearchOperator::Equals { is_negated: true },
            &related_options
                .ignore_classes
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }

    debug!(
        message = "Getting objects connected to object",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object.id(),
        query = query_string,
        ignore_classes = ?related_options.ignore_classes,
        ignore_self_class = related_options.ignore_self_class,
    );

    let search_params = prepare_db_pagination::<RelatedObjectGraphRow>(&params)?;
    let (hits, total_count) = user
        .objects_related_to_page(&pool, object, search_params, requestor.scopes())
        .await?;

    ApiResponse::mapped_paginated(hits, total_count, &params, |page| {
        page.to_descendant_objects_with_path()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/relations",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Direct relations touching the object", body = [HubuumObjectRelation]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/relations")]
#[get("/{class_id}/objects/{object_id}/related/relations/")]
async fn get_related_object_relations(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();
    let params = parse_query_parameter(req.query_string())?;

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    debug!(
        message = "Getting direct relations touching object",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object.id(),
        query = req.query_string(),
    );

    let (relations, total_count) = if pool.permission_backend().supports_sql_visibility_pushdown() {
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        user.object_relations_touching_page(&pool, object, search_params, requestor.scopes())
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
        let (candidates, _) = user
            .object_relations_touching_page_from_backend_with_admin_status(
                &pool,
                object,
                candidate_options,
                true,
                None,
            )
            .await?;
        let resources = object_relation_authorization_resources(&pool, &candidates)
            .await?
            .into_iter()
            .map(|resource| (resource.id, resource))
            .collect::<HashMap<_, _>>();
        let principal = PrincipalRef::load(&pool, user).await?;
        let search_params = prepare_db_pagination::<HubuumObjectRelation>(&params)?;
        let page = authorize_cursor_page(
            pool.permission_backend(),
            &principal,
            candidates,
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

    ApiResponse::paginated(relations, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/{object_id}/related/graph",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Neighborhood graph for the object", body = RelatedObjectGraph),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/objects/{object_id}/related/graph")]
#[get("/{class_id}/objects/{object_id}/related/graph/")]
async fn get_related_object_graph(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();
    let (params, graph_limit) =
        prepare_graph_query_options(parse_query_parameter(req.query_string())?)?;

    check_if_object_in_class(&pool, &class_id, &object_id).await?;
    let object = object_id.instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );

    debug!(
        message = "Getting related object graph",
        user_id = user.id(),
        class_id = class_id.id(),
        object_id = object.id(),
        query = req.query_string(),
    );

    let root_object = object_with_root_path(&object);
    let connected_objects = user
        .search_objects_related_to(&pool, object, params, requestor.scopes())
        .await?;
    ensure_graph_result_within_limit(&connected_objects, graph_limit, "objects")?;
    let mut objects = Vec::with_capacity(connected_objects.len() + 1);
    objects.push(root_object);
    objects.extend(connected_objects.to_descendant_objects_with_path());

    let object_ids = objects.iter().map(|item| item.id).collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(&pool, &object_ids, requestor.scopes())
        .await?;

    Ok(ApiResponse::new(
        RelatedObjectGraph { objects, relations },
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("from_object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 200, description = "Object relation", body = HubuumObjectRelation),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn get_object_relation_from_class_and_objects(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Getting object relation from class and objects",
        user_id = user.id(),
        class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_object_id = to_object.id()
    );

    check_if_object_in_class(&pool, &from_class, &from_object).await?;
    check_if_object_in_class(&pool, &to_class, &to_object).await?;

    let relation = from_object
        .object_relation(&pool, &from_class, &to_object)
        .await
        .map_err(|_| {
            ApiError::NotFound(format!(
                "Object {} of class {} is not related to object {}",
                from_object.id(),
                from_class.id(),
                to_object.id()
            ))
        })?;
    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::ReadObjectRelation],
            from_class,
            from_object,
            to_class,
            to_object
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::ReadObjectRelation],
            vec![resource],
        )
        .await?;
    }
    Ok(ApiResponse::new(relation, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("from_object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 204, description = "Object relation deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Relation not found", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn delete_object_relation(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    check_if_object_in_class(&pool, &from_class, &from_object).await?;
    check_if_object_in_class(&pool, &to_class, &to_object).await?;

    debug!(
        message = "Deleting object relation",
        user_id = user.id(),
        from_class_id = from_class.id(),
        from_object_id = from_object.id(),
        to_class_id = to_class.id(),
        to_object_id = to_object.id()
    );

    let relation = from_object
        .object_relation(&pool, &from_class, &to_object)
        .await;

    if relation.is_err() {
        debug!(
            message = "Relation does not exist",
            user_id = user.id(),
            from_class_id = from_class.id(),
            from_object_id = from_object.id(),
            to_class_id = to_class.id(),
            to_object_id = to_object.id()
        );
        return Err(ApiError::NotFound(format!(
            "Class {} is not related to class {}",
            from_class.id(),
            to_class.id()
        )));
    }

    let relation = relation.expect("Relation should exist after is_err check");

    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::DeleteObjectRelation],
            from_class,
            from_object,
            to_class,
            to_object
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::DeleteObjectRelation],
            vec![resource],
        )
        .await?;
    }

    debug!(
        message = "Relation ID found",
        user_id = user.id(),
        class_id = from_class.id(),
        object_id = from_object.id(),
        relation_id = relation.id(),
        relation_id_actual = relation.id()
    );

    let event_context = requestor.event_context(&req);
    relation.delete(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Source class ID"),
        ("from_object_id" = i32, Path, description = "Source object ID"),
        ("to_class_id" = i32, Path, description = "Target class ID"),
        ("to_object_id" = i32, Path, description = "Target object ID")
    ),
    responses(
        (status = 201, description = "Object relation created", body = HubuumObjectRelation),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}")]
async fn create_object_relation(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID, HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let (from_class, from_object, to_class, to_object) = paths.into_inner();

    debug!(
        message = "Creating object relation",
        user_id = user.id(),
        from_class = from_class.id(),
        from_object = from_object.id(),
        to_class = to_class.id(),
        to_object = to_object.id()
    );

    let is_related = from_class.direct_relation_to(&pool, &to_class).await?;

    if is_related.is_none() {
        debug!(
            message = "Relation does not exist",
            user_id = user.id(),
            from_class = from_class.id(),
            to_class = to_class.id()
        );
        return Err(ApiError::NotFound(format!(
            "Class {} is not related to class {}",
            from_class.id(),
            to_class.id()
        )));
    }

    let relation = is_related.expect("Relation should exist after is_none check");

    let relation = NewHubuumObjectRelation {
        class_relation_id: relation.id,
        from_hubuum_object_id: from_object.id(),
        to_hubuum_object_id: to_object.id(),
    };

    if pool.permission_backend().uses_sql_permission_store() {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateObjectRelation],
            from_class,
            to_class
        );
    } else {
        let resource = relation.to_resource_ref(&pool).await?;
        authorize_resources(
            pool.permission_backend(),
            &pool,
            user,
            requestor.scopes(),
            vec![Permissions::CreateObjectRelation],
            vec![resource],
        )
        .await?;
    }

    let event_context = requestor.event_context(&req);
    let relation = relation.save(&pool, &event_context).await?;

    let location = api_locations::object_relation(
        from_class.id(),
        from_object.id(),
        to_class.id(),
        to_object.id(),
    )?;
    Ok(ApiResponse::created(relation, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    responses(
        (status = 200, description = "Class history", body = [HistoryResponse<HubuumClassHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history")]
async fn get_class_history(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let (entity_id, require_history) = match class_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadClass],
                instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            (class_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<HubuumClassHistory>(&params)?;
    let (rows, total_count) =
        class_history_paginated_with_total_count(entity_id, &pool, &search_params).await?;
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!("class {entity_id} not found")));
    }

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Class version at timestamp", body = HistoryResponse<HubuumClassHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/history/as-of")]
async fn get_class_as_of(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let entity_id = match class_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadClass],
                instance
            );
            instance.id
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            class_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = class_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of class {entity_id} at {at}")))?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID")
    ),
    responses(
        (status = 200, description = "Object history", body = [HistoryResponse<HubuumObjectHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class or object not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history")]
async fn get_object_history(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    let (entity_id, require_history) =
        match check_if_object_in_class(&pool, &class_id, &object_id).await {
            Ok(()) => {
                let object = object_id.instance(&pool).await?;
                can!(
                    &pool,
                    user,
                    requestor.scopes(),
                    [Permissions::ReadObject],
                    object
                );
                (object.id, false)
            }
            Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
                (object_id.id(), true)
            }
            Err(err) => return Err(err),
        };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<HubuumObjectHistory>(&params)?;
    let (rows, total_count) =
        object_history_paginated_with_total_count(entity_id, class_id.id(), &pool, &search_params)
            .await?;
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!("object {entity_id} not found")));
    }

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/{object_id}/history/as-of",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("object_id" = i32, Path, description = "Object ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Object version at timestamp", body = HistoryResponse<HubuumObjectHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class, object, or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/{object_id}/history/as-of")]
async fn get_object_as_of(
    pool: AppContext,
    requestor: Authenticated,
    paths: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let (class_id, object_id) = paths.into_inner();

    let entity_id = match check_if_object_in_class(&pool, &class_id, &object_id).await {
        Ok(()) => {
            let object = object_id.instance(&pool).await?;
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadObject],
                object
            );
            object.id
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            object_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = object_as_of(entity_id, class_id.id(), at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of object {entity_id} at {at}")))?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}
