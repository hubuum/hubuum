use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::{
    Group, GroupID, GroupPermission, Namespace, NamespaceID, NewNamespaceWithAssignee, Permission,
    Permissions, PermissionsList, UpdateNamespace, UserID,
};

use crate::models::search::parse_query_parameter;

use crate::utilities::response::{json_response, json_response_created};
use actix_web::{
    delete, get, http::StatusCode, patch, post, put, routes, web, HttpRequest, Responder,
};
use serde_json::json;
use tracing::{debug, info};

use crate::can;

use crate::db::traits::UserPermissions;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, NamespaceAccessors, PermissionController, Search, SelfAccessors,
};

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
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    debug!(
        message = "Namespace list requested",
        requestor = user.username
    );

    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    let result = user.search_namespaces(&pool, params).await?;
    Ok(json_response(result, StatusCode::OK))
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
) -> Result<impl Responder, ApiError> {
    let new_namespace_request = new_namespace_request.into_inner();
    debug!(
        message = "Namespace create requested",
        requestor = requestor.user.id,
        new_namespace = new_namespace_request.name
    );

    let created_namespace = new_namespace_request.save(&pool).await?;

    Ok(json_response_created(
        &created_namespace,
        format!("/api/v1/namespaces/{}", created_namespace.id).as_str(),
    ))
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
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace get requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;

    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    Ok(json_response(namespace, StatusCode::OK))
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
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
    update_data: web::Json<UpdateNamespace>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace update requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;

    can!(
        &pool,
        requestor.user,
        [Permissions::UpdateCollection],
        namespace
    );

    let updated_namespace = update_data.into_inner().update(&pool, namespace.id).await?;
    Ok(json_response(updated_namespace, StatusCode::ACCEPTED))
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
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace delete requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DeleteCollection],
        namespace
    );

    namespace.delete(&pool).await?;
    Ok(json_response(json!(()), StatusCode::NO_CONTENT))
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
    requestor: UserAccess,
    namespace_id: web::Path<NamespaceID>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::groups_on;

    info!(
        message = "Namespace permissions list requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    let permissions = groups_on(&pool, namespace, vec![]).await?;
    Ok(json_response(permissions, StatusCode::OK))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::group_on;
    use crate::models::permissions::Permissions;

    let (namespace_id, group_id) = params.into_inner();

    info!(
        message = "Namespace group permissions list requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        group_id = group_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    let permissions = group_on(&pool, namespace.id, group_id.id()).await?;

    Ok(json_response(permissions, StatusCode::OK))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Namespace group permissions grant requested",
        requestor = requestor.user.id,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DelegateCollection],
        namespace
    );

    namespace.grant(&pool, group_id.id(), permissions).await?;

    Ok(json_response((), StatusCode::CREATED))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Namespace group permissions replace requested",
        requestor = requestor.user.id,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DelegateCollection],
        namespace
    );

    if !permissions.iter().next().is_some() {
        return Err(ApiError::BadRequest(
            "Permissions list cannot be empty for replace operation, use DELETE endpoint instead"
                .to_string(),
        ));
    }

    namespace
        .set_permissions(&pool, group_id.id(), permissions)
        .await?;

    Ok(json_response((), StatusCode::OK))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID)>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id) = params.into_inner();

    info!(
        message = "Namespace group permissions revoke requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        group_id = group_id.id()
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DelegateCollection],
        namespace
    );

    namespace.revoke_all(&pool, group_id.id()).await?;

    Ok(json_response((), StatusCode::NO_CONTENT))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::group_can_on;

    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission check requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    if group_can_on(&pool, group_id.id(), namespace, permission).await? {
        return Ok(json_response((), StatusCode::NO_CONTENT));
    }
    Ok(json_response((), StatusCode::NOT_FOUND))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission grant requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DelegateCollection],
        namespace
    );

    namespace
        .grant(&pool, group_id.id(), PermissionsList::new([permission]))
        .await?;

    Ok(json_response((), StatusCode::CREATED))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, GroupID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, group_id, permission) = params.into_inner();

    info!(
        message = "Namespace group permission revoke requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::DelegateCollection],
        namespace
    );

    namespace
        .revoke(&pool, group_id.id(), PermissionsList::new([permission]))
        .await?;

    Ok(json_response((), StatusCode::NO_CONTENT))
}

/// List all permissions for a user on a namespace
#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/permissions/user/{user_id}",
    tag = "namespaces",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 200, description = "User permissions via group memberships", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No permissions found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/permissions/user/{user_id}")]
pub async fn get_namespace_user_permissions(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    params: web::Path<(NamespaceID, UserID)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::user_on;

    let (namespace_id, user_id) = params.into_inner();

    info!(
        message = "Namespace user permissions list requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        user_id = user_id.0
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    let permissions = user_on(&pool, user_id, namespace).await?;

    if permissions.is_empty() {
        return Ok(json_response((), StatusCode::NOT_FOUND));
    }

    Ok(json_response(permissions, StatusCode::OK))
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
    requestor: UserAccess,
    params: web::Path<(NamespaceID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::namespace::groups_can_on;

    let (namespace_id, permission) = params.into_inner();

    info!(
        message = "Namespace groups with permission list requested",
        requestor = requestor.user.username,
        namespace_id = namespace_id.id(),
        permission = ?permission
    );

    let namespace = namespace_id.instance(&pool).await?;
    can!(
        &pool,
        requestor.user,
        [Permissions::ReadCollection],
        namespace
    );

    let groups = groups_can_on(&pool, namespace.id, permission).await?;

    Ok(json_response(groups, StatusCode::OK))
}
