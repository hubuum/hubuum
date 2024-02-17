use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::{
    user_can_on_any, GroupID, NamespaceID, NewNamespaceWithAssignee, Permissions, PermissionsList,
    UpdateNamespace, UserID,
};

use crate::utilities::response::{json_response, json_response_created};
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};
use serde_json::json;
use tracing::{debug, info};

use crate::traits::{CanDelete, CanSave, CanUpdate, PermissionController, SelfAccessors};

#[get("")]
pub async fn get_namespaces(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Namespace list requested",
        requestor = requestor.user.username
    );

    let result = user_can_on_any(
        &pool,
        UserID(requestor.user.id),
        Permissions::ReadCollection,
    )
    .await?;
    Ok(json_response(result, StatusCode::OK))
}

#[post("")]
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    Ok(json_response(namespace, StatusCode::OK))
}

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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::UpdateCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    let updated_namespace = update_data.into_inner().update(&pool, namespace.id).await?;
    Ok(json_response(updated_namespace, StatusCode::ACCEPTED))
}

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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::DeleteCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    namespace.delete(&pool).await?;
    Ok(json_response(json!(()), StatusCode::NO_CONTENT))
}

/// List all groups who have permissions for a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    let permissions = groups_on(&pool, namespace).await?;
    Ok(json_response(permissions, StatusCode::OK))
}

/// List all permissions for a given group on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    let permissions = group_on(&pool, namespace.id, group_id.id()).await?;

    Ok(json_response(permissions, StatusCode::OK))
}

/// Post a permission set to a group on a namespace
/// This will create a new entry if the group had no permissions,
/// or update the existing entry if it did.
/// The body should be a JSON array of permissions:
/// ```json
/// [
///   "CreateObject",
///   "ReadCollection"
/// ]
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::DelegateCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    namespace.grant(&pool, group_id.id(), permissions).await?;

    Ok(json_response((), StatusCode::CREATED))
}

/// Revoke a permission set from a group on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::DelegateCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    namespace.revoke_all(&pool, group_id.id()).await?;

    Ok(json_response((), StatusCode::NO_CONTENT))
}

/// Check a specific permission for a group on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    if group_can_on(&pool, group_id.id(), namespace, permission).await? {
        return Ok(json_response((), StatusCode::NO_CONTENT));
    }
    Ok(json_response((), StatusCode::NOT_FOUND))
}

/// Grant a specific permission to a group on a namespace
/// If the group previously had no permissions, a new entry is created
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::DelegateCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    namespace
        .grant(&pool, group_id.id(), PermissionsList::new([permission]))
        .await?;

    Ok(json_response((), StatusCode::CREATED))
}

/// Revoke a specific permission from a group on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::DelegateCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    namespace
        .revoke(&pool, group_id.id(), PermissionsList::new([permission]))
        .await?;

    Ok(json_response((), StatusCode::NO_CONTENT))
}

/// List all permissions for a user on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    let permissions = user_on(&pool, user_id, namespace).await?;

    if permissions.is_empty() {
        return Ok(json_response((), StatusCode::NOT_FOUND));
    }

    Ok(json_response(permissions, StatusCode::OK))
}

/// List all groups that have any permissions on a namespace
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
    if !namespace
        .user_can(
            &pool,
            UserID(requestor.user.id),
            Permissions::ReadCollection,
        )
        .await?
    {
        return Ok(json_response(json!(()), StatusCode::FORBIDDEN));
    }

    let groups = groups_can_on(&pool, namespace.id, permission).await?;

    Ok(json_response(groups, StatusCode::OK))
}
