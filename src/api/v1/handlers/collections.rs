use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess, Authenticated};
use crate::models::{
    Collection, CollectionID, EffectiveGroupPermission, Group, GroupID, GroupPermission,
    GroupResponse, NewCollectionWithAssignee, Permission, Permissions, PermissionsList,
    UpdateCollection, UpdateCollectionParent,
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
use crate::db::traits::history::{collection_as_of, collection_history_paginated_with_total_count};
use crate::traits::{
    CanDelete, CanSave, CanUpdate, CollectionAccessors, PermissionController, Search, SelfAccessors,
};

#[utoipa::path(
    get,
    path = "/api/v1/collections",
    tag = "collections",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Collections matching optional query filters", body = [Collection]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_collections(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    debug!(message = "Collection list requested", requestor = user.name);

    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    let total_count = user
        .count_collections(&pool, count_query_options(&params), requestor.scopes())
        .await?;
    let search_params = prepare_db_pagination::<Collection>(&params)?;
    let result = user
        .search_collections(&pool, search_params, requestor.scopes())
        .await?;
    ApiResponse::paginated(result, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/collections",
    tag = "collections",
    security(("bearer_auth" = [])),
    request_body = NewCollectionWithAssignee,
    responses(
        (status = 201, description = "Collection created", body = Collection),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_collection(
    pool: web::Data<DbPool>,
    new_collection_request: web::Json<NewCollectionWithAssignee>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let new_collection_request = new_collection_request.into_inner();
    debug!(
        message = "Collection create requested",
        requestor = requestor.user.id,
        new_collection = new_collection_request.name
    );

    let event_context = requestor.event_context(&req);
    let created_collection = new_collection_request.save(&pool, &event_context).await?;

    let location = api_locations::collection(created_collection.id)?;
    Ok(ApiResponse::created(created_collection, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    responses(
        (status = 200, description = "Collection", body = Collection),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}")]
pub async fn get_collection(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Collection get requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id()
    );

    let collection = collection_id.instance(&pool).await?;

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    Ok(ApiResponse::new(collection, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/collections/{collection_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    request_body = UpdateCollection,
    responses(
        (status = 202, description = "Collection updated", body = Collection),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[patch("/{collection_id}")]
pub async fn update_collection(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    update_data: web::Json<UpdateCollection>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Collection update requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id()
    );

    let collection = collection_id.instance(&pool).await?;

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    let updated_collection = update_data
        .into_inner()
        .update(&pool, collection.id, &event_context)
        .await?;
    Ok(ApiResponse::accepted(updated_collection))
}

#[utoipa::path(
    delete,
    path = "/api/v1/collections/{collection_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    responses(
        (status = 204, description = "Collection deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[delete("/{collection_id}")]
pub async fn delete_collection(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Collection delete requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id()
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DeleteCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    collection.delete(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/children",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    responses(
        (status = 200, description = "Direct child collections", body = [Collection]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/children")]
pub async fn get_collection_children(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
) -> Result<impl Responder, ApiError> {
    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection.clone()
    );

    let children = crate::models::collection::collection_children(&pool, collection).await?;
    Ok(ApiResponse::new(children, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/ancestors",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    responses(
        (status = 200, description = "Ancestor collections, nearest parent first", body = [Collection]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/ancestors")]
pub async fn get_collection_ancestors(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
) -> Result<impl Responder, ApiError> {
    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection.clone()
    );

    let ancestors = crate::models::collection::collection_ancestors(&pool, collection).await?;
    Ok(ApiResponse::new(ancestors, StatusCode::OK))
}

#[utoipa::path(
    put,
    path = "/api/v1/collections/{collection_id}/parent",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    request_body = UpdateCollectionParent,
    responses(
        (status = 202, description = "Collection moved", body = Collection),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[put("/{collection_id}/parent")]
pub async fn move_collection_parent(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    update_parent: web::Json<UpdateCollectionParent>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let collection = collection_id.instance(&pool).await?;
    let new_parent_id = update_parent.into_inner().parent_collection_id;
    let new_parent = new_parent_id.instance(&pool).await?;

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection.clone()
    );

    if let Some(old_parent_id) = collection.parent_collection_id {
        let old_parent = CollectionID::new(old_parent_id)?.instance(&pool).await?;
        can!(
            &pool,
            &requestor.principal,
            requestor.scopes(),
            [Permissions::DelegateCollection],
            old_parent
        );
    }

    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        new_parent
    );

    let event_context = requestor.event_context(&req);
    let updated = crate::models::collection::move_collection(
        &pool,
        collection.id,
        new_parent_id.id(),
        Some(&event_context),
    )
    .await?;

    Ok(ApiResponse::accepted(updated))
}

/// List all groups who have permissions for a collection
#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID")
    ),
    responses(
        (status = 200, description = "Group permissions on collection", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions")]
pub async fn get_collection_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    info!(
        message = "Collection permissions list requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id()
    );

    let params = parse_query_parameter(req.query_string())?;

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    let search_params = prepare_db_pagination::<GroupPermission>(&params)?;
    let (permissions, total_count) =
        crate::models::collection::groups_on_paginated_with_total_count(
            &pool,
            collection.clone(),
            vec![],
            &search_params,
        )
        .await?;
    ApiResponse::paginated(permissions, total_count, &params)
}

/// List all permissions for a given group on a collection
#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Permission record", body = Permission),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions/group/{group_id}")]
pub async fn get_collection_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::collection::group_on;
    use crate::models::permissions::Permissions;

    let (collection_id, group_id) = params.into_inner();

    info!(
        message = "Collection group permissions list requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        group_id = group_id.id()
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    let permissions = group_on(&pool, collection.id, group_id.id()).await?;

    Ok(ApiResponse::new(permissions, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions/effective/group/{group_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Effective group permissions, including inherited ancestor grants", body = [EffectiveGroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions/effective/group/{group_id}")]
pub async fn get_collection_effective_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id) = params.into_inner();
    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    let permissions =
        crate::models::collection::effective_group_on(&pool, collection_id.id(), group_id.id())
            .await?;

    Ok(ApiResponse::new(permissions, StatusCode::OK))
}

/// Post a permission set to a group on a collection
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
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = Vec<Permissions>,
    responses(
        (status = 201, description = "Permissions set"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{collection_id}/permissions/group/{group_id}")]
pub async fn grant_collection_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Collection group permissions grant requested",
        requestor = requestor.principal.id,
        collection_id = collection_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    collection
        .grant(&pool, group_id.id(), permissions, Some(&event_context))
        .await?;

    Ok(ApiResponse::created_empty())
}

/// Replace all permissions for a group on a collection
/// This removes any existing permissions and applies the new set.
#[utoipa::path(
    put,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = Vec<Permissions>,
    responses(
        (status = 200, description = "Permissions replaced"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[put("/{collection_id}/permissions/group/{group_id}")]
pub async fn replace_collection_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID)>,
    permissions: web::Json<Vec<Permissions>>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id) = params.into_inner();
    let permissions = PermissionsList::new(permissions.into_inner());

    info!(
        message = "Collection group permissions replace requested",
        requestor = requestor.principal.id,
        collection_id = collection_id.id(),
        group_id = group_id.id(),
        permissions = ?permissions
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        collection
    );

    if permissions.iter().next().is_none() {
        return Err(ApiError::BadRequest(
            "Permissions list cannot be empty for replace operation, use DELETE endpoint instead"
                .to_string(),
        ));
    }

    let event_context = requestor.event_context(&req);
    collection
        .set_permissions(&pool, group_id.id(), permissions, Some(&event_context))
        .await?;

    Ok(ApiResponse::ok_empty())
}

/// Revoke a permission set from a group on a collection
#[utoipa::path(
    delete,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 204, description = "Permissions revoked"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{collection_id}/permissions/group/{group_id}")]
pub async fn revoke_collection_group_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id) = params.into_inner();

    info!(
        message = "Collection group permissions revoke requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        group_id = group_id.id()
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    collection
        .revoke_all(&pool, group_id.id(), Some(&event_context))
        .await?;

    Ok(ApiResponse::no_content())
}

/// Check a specific permission for a group on a collection
#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 204, description = "Group has permission"),
        (status = 404, description = "Group missing permission", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions/group/{group_id}/{permission}")]
pub async fn get_collection_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID, Permissions)>,
) -> Result<impl Responder, ApiError> {
    use crate::models::collection::group_can_on;

    let (collection_id, group_id, permission) = params.into_inner();

    info!(
        message = "Collection group permission check requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    if group_can_on(&pool, group_id.id(), collection, permission).await? {
        return Ok(ApiResponse::no_content());
    }
    Ok(ApiResponse::not_found_empty())
}

/// Grant a specific permission to a group on a collection
/// If the group previously had no permissions, a new entry is created
#[utoipa::path(
    post,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 201, description = "Permission granted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{collection_id}/permissions/group/{group_id}/{permission}")]
pub async fn grant_collection_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id, permission) = params.into_inner();

    info!(
        message = "Collection group permission grant requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    collection
        .grant(
            &pool,
            group_id.id(),
            PermissionsList::new([permission]),
            Some(&event_context),
        )
        .await?;

    Ok(ApiResponse::created_empty())
}

/// Revoke a specific permission from a group on a collection
#[utoipa::path(
    delete,
    path = "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("group_id" = i32, Path, description = "Group ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 204, description = "Permission revoked"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{collection_id}/permissions/group/{group_id}/{permission}")]
pub async fn revoke_collection_group_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, GroupID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, group_id, permission) = params.into_inner();

    info!(
        message = "Collection group permission revoke requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        group_id = group_id.id(),
        permission = ?permission
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::DelegateCollection],
        collection
    );

    let event_context = requestor.event_context(&req);
    collection
        .revoke(
            &pool,
            group_id.id(),
            PermissionsList::new([permission]),
            Some(&event_context),
        )
        .await?;

    Ok(ApiResponse::no_content())
}

/// List all permissions for a principal on a collection
#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions/principal/{principal_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 200, description = "Principal permissions via group memberships", body = [GroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "No permissions found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions/principal/{principal_id}")]
pub async fn get_collection_principal_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, crate::models::PrincipalID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, principal_id) = params.into_inner();
    let query_options = parse_query_parameter(req.query_string())?;

    info!(
        message = "Collection principal permissions list requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        principal_id = principal_id.id()
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    let search_params = prepare_db_pagination::<GroupPermission>(&query_options)?;
    let (permissions, total_count) =
        crate::models::collection::principal_on_paginated_with_total_count(
            &pool,
            principal_id,
            collection.clone(),
            &search_params,
        )
        .await?;

    if total_count == 0 {
        return Err(ApiError::NotFound("No permissions found".to_string()));
    }

    ApiResponse::paginated(permissions, total_count, &query_options)
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/permissions/effective/principal/{principal_id}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 200, description = "Effective principal permissions via group memberships, including inherited ancestor grants", body = [EffectiveGroupPermission]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/permissions/effective/principal/{principal_id}")]
pub async fn get_collection_effective_principal_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, crate::models::PrincipalID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, principal_id) = params.into_inner();
    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection.clone()
    );

    let permissions =
        crate::models::collection::effective_principal_on(&pool, principal_id, collection).await?;

    Ok(ApiResponse::new(permissions, StatusCode::OK))
}

/// List all groups that have any permissions on a collection
#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/has_permissions/{permission}",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("permission" = Permissions, Path, description = "Permission value")
    ),
    responses(
        (status = 200, description = "Groups with permission", body = [GroupResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/has_permissions/{permission}")]
pub async fn get_collection_groups_with_permission(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    params: web::Path<(CollectionID, Permissions)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (collection_id, permission) = params.into_inner();
    let query_options = parse_query_parameter(req.query_string())?;

    info!(
        message = "Collection groups with permission list requested",
        requestor = requestor.principal.name,
        collection_id = collection_id.id(),
        permission = ?permission
    );

    let collection = collection_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadCollection],
        collection
    );

    let search_params = prepare_db_pagination::<Group>(&query_options)?;
    let (groups, total_count) =
        crate::models::collection::groups_can_on_paginated_with_total_count(
            &pool,
            collection.id,
            permission,
            &search_params,
        )
        .await?;

    let response = GroupResponse::from_groups(&pool, groups).await?;

    ApiResponse::paginated(response, total_count, &query_options)
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/history",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID")),
    responses(
        (status = 200, description = "Collection history", body = [crate::api::v1::handlers::history::HistoryResponse<crate::models::CollectionHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Collection not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/history")]
pub async fn get_collection_history(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let collection_id = collection_id.into_inner();
    let (entity_id, require_history) = match collection_id.instance(&pool).await {
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
            (collection_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::CollectionHistory>(&params)?;
    let (rows, total_count) =
        collection_history_paginated_with_total_count(entity_id, &pool, &search_params).await?;
    if require_history && total_count == 0 {
        return Err(ApiError::NotFound(format!(
            "collection {entity_id} not found"
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
    path = "/api/v1/collections/{collection_id}/history/as-of",
    tag = "collections",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Collection version at timestamp", body = crate::api::v1::handlers::history::HistoryResponse<crate::models::CollectionHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Collection or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/history/as-of")]
pub async fn get_collection_as_of(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let collection_id = collection_id.into_inner();
    let entity_id = match collection_id.instance(&pool).await {
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
            collection_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = collection_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!("no version of collection {entity_id} at {at}"))
        })?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}
