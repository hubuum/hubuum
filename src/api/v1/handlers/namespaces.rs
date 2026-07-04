use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess, Authenticated};
use crate::models::{
    Group, GroupID, GroupPermission, Namespace, NamespaceID, NewNamespaceWithAssignee, Permission,
    Permissions, PermissionsList, UpdateNamespace,
};

use crate::models::search::parse_query_parameter;
use crate::pagination::{count_query_options, prepare_db_pagination};

use crate::api::response::ApiResponse;
use actix_web::{
    HttpRequest, Responder, delete, get, http::StatusCode, patch, post, put, routes, web,
};
use tracing::{debug, info};

use crate::can;

use crate::db::traits::UserPermissions;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, NamespaceAccessors, PermissionController, Search, SelfAccessors,
};

crate::history_db_fns!(
    namespace_history_paginated_with_total_count,
    namespace_as_of,
    crate::schema::namespaces_history,
    crate::models::NamespaceHistory
);

#[utoipa::path(
    get,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Namespaces matching optional query filters", body = [Namespace]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_namespaces(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    debug!(message = "Namespace list requested", requestor = user.name);

    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    let total_count = user
        .count_namespaces(&pool, count_query_options(&params), requestor.scopes())
        .await?;
    let search_params = prepare_db_pagination::<Namespace>(&params)?;
    let result = user
        .search_namespaces(&pool, search_params, requestor.scopes())
        .await?;
    ApiResponse::paginated(result, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    request_body = NewNamespaceWithAssignee,
    responses(
        (status = 201, description = "Namespace created", body = Namespace),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_namespace(
    pool: web::Data<DbPool>,
    new_namespace_request: web::Json<NewNamespaceWithAssignee>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let new_namespace_request = new_namespace_request.into_inner();
    debug!(
        message = "Namespace create requested",
        requestor = requestor.user.id,
        new_namespace = new_namespace_request.name
    );

    let event_context = requestor.event_context(&req);
    let created_namespace = new_namespace_request.save(&pool, &event_context).await?;

    let location = api_locations::namespace(created_namespace.id)?;
    Ok(ApiResponse::created(created_namespace, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID")
    ),
    responses(
        (status = 200, description = "Namespace", body = Namespace),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}")]
pub async fn get_namespace(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace get requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    Ok(ApiResponse::new(namespace, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/namespaces/{namespace_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID")
    ),
    request_body = UpdateNamespace,
    responses(
        (status = 202, description = "Namespace updated", body = Namespace),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[patch("/{namespace_id}")]
pub async fn update_namespace(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    update_data: web::Json<UpdateNamespace>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace update requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    let updated_namespace = update_data
        .into_inner()
        .update(&pool, namespace.id, &event_context)
        .await?;
    Ok(ApiResponse::accepted(updated_namespace))
}

#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID")
    ),
    responses(
        (status = 204, description = "Namespace deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[delete("/{namespace_id}")]
pub async fn delete_namespace(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace delete requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DeleteCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    namespace.delete(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}

/// List all groups who have permissions for a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/permissions",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID")
    ),
    responses(
        (status = 200, description = "Group permissions on namespace", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/permissions")]
pub async fn get_namespace_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    info!(
        message = "Namespace permissions list requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id()
    );

    let params = parse_query_parameter(req.query_string())?;

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    let search_params = prepare_db_pagination::<GroupPermission>(&params)?;
    let (permissions, total_count) =
        crate::models::namespace::groups_on_paginated_with_total_count(
            &pool,
            namespace.clone(),
            vec![],
            &search_params,
        )
        .await?;
    ApiResponse::paginated(permissions, total_count, &params)
}

/// List all permissions for a given group on a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Permission record", body = Permission),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/permissions/group/{group_id}")]
pub async fn get_namespace_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::group_on;
    use crate::models::permissions::Permissions;

    let (namespace_id, group_id) = params.into_inner();

    info!(
        message = "Namespace group permissions list requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        group_id = group_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    let permissions = group_on(&pool, namespace.id, group_id.id()).await?;

    Ok(ApiResponse::new(permissions, StatusCode::OK))
}

/// Post a permission set to a group on a namespace
/// This will create a new entry if the group had no permissions,
/// or add to the existing entry if it did.
/// The body should be a JSON array of permissions:
/// ```json
/// [
///   "CreateObject",
///   "ReadCollection"
/// ]
/// ```
#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = Vec<Permissions>,
    responses(
        (status = 201, description = "Permissions set"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{namespace_id}/permissions/group/{group_id}")]
pub async fn grant_namespace_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Namespace group permissions grant requested",
        requestor = requestor.principal.id,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    namespace
        .grant(&pool, group_id.id(), permissions, Some(&event_context))
        .await?;

    Ok(ApiResponse::created_empty())
}

/// Replace all permissions for a group on a namespace
/// This removes any existing permissions and applies the new set.
#[utoipa::path(
    put,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = Vec<Permissions>,
    responses(
        (status = 200, description = "Permissions replaced"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[put("/{namespace_id}/permissions/group/{group_id}")]
pub async fn replace_namespace_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Namespace group permissions replace requested",
        requestor = requestor.principal.id,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        namespace
    );

    if permissions.iter().next().is_none() {
        return Err(ApiError::BadRequest(
            "Permissions list cannot be empty for replace operation, use DELETE endpoint instead"
                .to_string(),
        ));
    }

    let event_context = requestor.event_context(&req);
    namespace
        .set_permissions(&pool, group_id.id(), permissions, Some(&event_context))
        .await?;

    Ok(ApiResponse::ok_empty())
}

/// Revoke a permission set from a group on a namespace
#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 204, description = "Permissions revoked"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{namespace_id}/permissions/group/{group_id}")]
pub async fn revoke_namespace_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();

    info!(
        message = "Namespace group permissions revoke requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        group_id = group_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    namespace
        .revoke_all(&pool, group_id.id(), Some(&event_context))
        .await?;

    Ok(ApiResponse::no_content())
}

/// Check a specific permission for a group on a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}/{permission}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 204, description = "Group has permission"),
        (status = 404, description = "Group missing permission", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/permissions/group/{group_id}/{permission}")]
pub async fn get_namespace_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::group_can_on;

    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission check requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    if group_can_on(&pool, group_id.id(), namespace, permission).await? {
        return Ok(ApiResponse::no_content());
    }
    Ok(ApiResponse::not_found_empty())
}

/// Grant a specific permission to a group on a namespace
/// If the group previously had no permissions, a new entry is created
#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}/{permission}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 201, description = "Permission granted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{namespace_id}/permissions/group/{group_id}/{permission}")]
pub async fn grant_namespace_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission grant requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    namespace
        .grant(
            &pool,
            group_id.id(),
            PermissionsList::new([permission]),
            Some(&event_context),
        )
        .await?;

    Ok(ApiResponse::created_empty())
}

/// Revoke a specific permission from a group on a namespace
#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}/{permission}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 204, description = "Permission revoked"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{namespace_id}/permissions/group/{group_id}/{permission}")]
pub async fn revoke_namespace_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission revoke requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        namespace
    );

    let event_context = requestor.event_context(&req);
    namespace
        .revoke(
            &pool,
            group_id.id(),
            PermissionsList::new([permission]),
            Some(&event_context),
        )
        .await?;

    Ok(ApiResponse::no_content())
}

/// List all permissions for a principal on a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/permissions/principal/{principal_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 200, description = "Principal permissions via group memberships", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No permissions found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/permissions/principal/{principal_id}")]
pub async fn get_namespace_principal_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, crate::models::PrincipalID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, principal_id) = params.into_inner();
    let query_options = parse_query_parameter(req.query_string())?;

    info!(
        message = "Namespace principal permissions list requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        principal_id = principal_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    let search_params = prepare_db_pagination::<GroupPermission>(&query_options)?;
    let (permissions, total_count) =
        crate::models::namespace::principal_on_paginated_with_total_count(
            &pool,
            principal_id,
            namespace.clone(),
            &search_params,
        )
        .await?;

    if total_count == 0 {
        return Err(ApiError::NotFound("No permissions found".to_string()));
    }

    ApiResponse::paginated(permissions, total_count, &query_options)
}

/// List all groups that have any permissions on a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/has_permissions/{permission}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 200, description = "Groups with permission", body = [Group]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/has_permissions/{permission}")]
pub async fn get_namespace_groups_with_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(NamespaceID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, permission) = params.into_inner();
    let query_options = parse_query_parameter(req.query_string())?;

    info!(
        message = "Namespace groups with permission list requested",
        requestor = requestor.principal.name,
        namespace_id = namespace_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        namespace
    );

    let search_params = prepare_db_pagination::<Group>(&query_options)?;
    let (groups, total_count) = crate::models::namespace::groups_can_on_paginated_with_total_count(
        &pool,
        namespace.id,
        permission,
        &search_params,
    )
    .await?;

    ApiResponse::paginated(groups, total_count, &query_options)
}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/history",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(("namespace_id" = i32, Path, description = "Namespace ID")),
    responses(
        (status = 200, description = "Namespace history", body = [crate::api::v1::handlers::history::HistoryResponse<crate::models::NamespaceHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Namespace not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/history")]
pub async fn get_namespace_history(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let namespace_id = namespace_id.into_inner();
    let (entity_id, require_history) = match namespace_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadCollection],
                instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            (namespace_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::NamespaceHistory>(&params)?;
    let (rows, total_count) =
        namespace_history_paginated_with_total_count(entity_id, &pool, &search_params).await?;
    if require_history && total_count == 0 {
        return Err(ApiError::NotFound(format!(
            "namespace {entity_id} not found"
        )));
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
    path = "/api/v1/namespaces/{namespace_id}/history/as-of",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Namespace version at timestamp", body = crate::api::v1::handlers::history::HistoryResponse<crate::models::NamespaceHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Namespace or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/history/as-of")]
pub async fn get_namespace_as_of(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let namespace_id = namespace_id.into_inner();
    let entity_id = match namespace_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadCollection],
                instance
            );
            instance.id
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            namespace_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = namespace_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!("no version of namespace {entity_id} at {at}"))
        })?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}
