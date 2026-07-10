use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, put, web};
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::ToSchema;

use crate::api::openapi::{ApiErrorResponse, LoginResponse};
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::db::traits::ActiveTokens;
use crate::db::traits::service_account::{
    is_human_owner_group_member, load_service_account_by_id, principal_is_disabled,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated, ManagementAccess};
use crate::models::collection::principal_all_permissions;
use crate::models::principal::{Principal, PrincipalKind, PrincipalSettings};
use crate::models::search::parse_query_parameter;
use crate::models::token::{create_principal_token, revoke_token_by_id_for_principal};
use crate::models::{
    Group, GroupResponse, Permissions, PrincipalID, PrincipalToken, PrincipalTokenMetadata,
};
use crate::pagination::prepare_db_pagination;
use crate::traits::{AuthzSubject, GroupAccessors};
use std::collections::BTreeMap;
use std::collections::HashSet;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_token)
        .service(list_tokens)
        .service(revoke_token)
        .service(list_principal_groups)
        .service(list_principal_permissions)
        .service(get_principal_settings)
        .service(put_principal_settings)
        .service(patch_principal_settings)
        .service(delete_principal_settings);
}

async fn ensure_can_manage_principal_settings(
    pool: &DbPool,
    requestor: &Authenticated,
    target_principal_id: i32,
) -> Result<(), ApiError> {
    if requestor.principal.id == target_principal_id {
        return Ok(());
    }

    if requestor.scopes().is_none()
        && requestor.principal.is_human()
        && requestor.principal.is_admin(pool).await?
    {
        return Ok(());
    }

    Err(ApiError::NotFound("Principal not found".to_string()))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct NewTokenRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub expires_at: Option<chrono::NaiveDateTime>,
    /// Optional scope set. Omit for an unscoped token; an **empty** array is
    /// rejected (almost certainly a client bug, not "grant nothing").
    pub scopes: Option<Vec<Permissions>>,
}

#[derive(Debug, Deserialize)]
struct TokenPath {
    principal_id: PrincipalID,
    token_id: i32,
}

/// Management authz for a principal's credentials/membership:
/// * human principal — self or admin;
/// * service account — admin or a **human** member of its owner group.
async fn ensure_can_manage_principal(
    pool: &DbPool,
    requestor: &ManagementAccess,
    principal: &Principal,
) -> Result<(), ApiError> {
    if requestor.user.is_admin(pool).await? {
        return Ok(());
    }
    let permitted = match principal.principal_kind()? {
        PrincipalKind::Human => requestor.user.id == principal.id,
        PrincipalKind::ServiceAccount => {
            let sa = load_service_account_by_id(pool, principal.id).await?;
            is_human_owner_group_member(pool, requestor.user.id, sa.owner_group_id).await?
        }
    };
    if permitted {
        Ok(())
    } else {
        // Avoid leaking whether a target principal exists via 403 vs 404.
        Err(ApiError::NotFound("Principal not found".to_string()))
    }
}

pub(crate) async fn principal_permissions_response(
    pool: &DbPool,
    principal: &impl AuthzSubject,
) -> Result<Vec<PrincipalCollectionPermissions>, ApiError> {
    let rows = principal_all_permissions(pool, principal).await?;

    // Fold (collection, group, permission-row) tuples into a per-collection,
    // per-group export. BTreeMap keeps collections in a stable id order; groups
    // with no granted flags are dropped.
    let mut by_collection: BTreeMap<i32, PrincipalCollectionPermissions> = BTreeMap::new();
    for (collection, group, permission) in rows {
        let permissions = permission.granted();
        if permissions.is_empty() {
            continue;
        }
        by_collection
            .entry(collection.id)
            .or_insert_with(|| PrincipalCollectionPermissions {
                collection_id: collection.id,
                collection_name: collection.name.clone(),
                grants: Vec::new(),
            })
            .grants
            .push(GroupGrant {
                group_id: group.id,
                groupname: group.groupname.clone(),
                permissions,
            });
    }

    Ok(by_collection.into_values().collect())
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/principals/{principal_id}/tokens",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    request_body = NewTokenRequest,
    responses(
        (status = 201, description = "Raw token (shown once)", body = LoginResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Service account disabled", body = ApiErrorResponse)
    )
)]
#[post("/{principal_id}/tokens")]
pub async fn create_token(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    body: web::Json<NewTokenRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal = principal_id.into_inner().principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    // A disabled service account cannot mint credentials.
    if principal_is_disabled(&pool, &principal).await? {
        return Err(ApiError::Conflict(
            "Service account is disabled".to_string(),
        ));
    }

    let body = body.into_inner();
    if let Some(scopes) = &body.scopes
        && scopes.is_empty()
    {
        return Err(ApiError::BadRequest(
            "scopes must be non-empty when provided".to_string(),
        ));
    }

    if let Some(scopes) = &body.scopes {
        let unique: HashSet<Permissions> = scopes.iter().copied().collect();
        if unique.len() != scopes.len() {
            return Err(ApiError::BadRequest(
                "scopes must not contain duplicates".to_string(),
            ));
        }
    }

    debug!(
        message = "Token mint requested",
        principal = principal.id,
        requestor = requestor.user.id,
        scoped = body.scopes.is_some()
    );

    let event_context = requestor.event_context(&req);
    let raw = create_principal_token(
        &pool,
        principal.id,
        body.name.as_deref(),
        body.description.as_deref(),
        body.expires_at,
        body.scopes.as_deref(),
        Some(&event_context),
    )
    .await?;

    Ok(ApiResponse::new(
        LoginResponse::new(raw.get_token()),
        StatusCode::CREATED,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/tokens",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Active token metadata", body = [PrincipalTokenMetadata]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/tokens")]
pub async fn list_tokens(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<PrincipalToken>(&params)?;
    let (tokens, total_count) = pid
        .tokens_paginated_with_total_count(&pool, &search_params)
        .await?;

    ApiResponse::mapped_paginated(tokens, total_count, &params, |tokens| {
        tokens
            .into_iter()
            .map(PrincipalTokenMetadata::from)
            .collect()
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/revoke",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(
        ("principal_id" = i32, Path, description = "Principal id"),
        ("token_id" = i32, Path, description = "Token id")
    ),
    responses(
        (status = 204, description = "Token revoked"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Token not found for this principal", body = ApiErrorResponse)
    )
)]
#[post("/{principal_id}/tokens/{token_id}/revoke")]
pub async fn revoke_token(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    path: web::Path<TokenPath>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let path = path.into_inner();
    let principal = path.principal_id.principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    let event_context = requestor.event_context(&req);
    let revoked =
        revoke_token_by_id_for_principal(&pool, path.token_id, principal.id, Some(&event_context))
            .await?;
    if revoked == 0 {
        return Err(ApiError::NotFound(
            "Token not found for this principal".to_string(),
        ));
    }
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/groups",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Groups the principal belongs to", body = [GroupResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/groups")]
pub async fn list_principal_groups(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<Group>(&params)?;
    let (groups, total_count) = pid
        .groups_paginated_with_total_count(&pool, &search_params)
        .await?;
    let mut response = Vec::with_capacity(groups.len());
    for group in groups {
        response.push(group.to_response(&pool).await?);
    }
    ApiResponse::paginated(response, total_count, &params)
}

/// One group's direct permission row contribution on a collection.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupGrant {
    pub group_id: i32,
    pub groupname: String,
    pub permissions: Vec<Permissions>,
}

/// A principal's direct permission rows on a single collection, broken down by the
/// group that grants them.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PrincipalCollectionPermissions {
    pub collection_id: i32,
    pub collection_name: String,
    pub grants: Vec<GroupGrant>,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/permissions",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Effective permissions per collection, grouped by granting group", body = [PrincipalCollectionPermissions]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/permissions")]
pub async fn list_principal_permissions(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    let export = principal_permissions_response(&pool, &pid).await?;
    Ok(ApiResponse::new(export, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Principal settings", body = PrincipalSettings),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/settings")]
pub async fn get_principal_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&pool, &requestor, principal_id.id()).await?;
    Ok(ApiResponse::ok(principal_id.settings(&pool).await?))
}

#[utoipa::path(
    put,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    request_body = PrincipalSettings,
    responses(
        (status = 200, description = "Replaced principal settings", body = PrincipalSettings),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[put("/{principal_id}/settings")]
pub async fn put_principal_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    settings: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&pool, &requestor, principal_id.id()).await?;
    let event_context = requestor.event_context(&req);
    let settings = principal_id
        .replace_settings(&pool, settings.into_inner(), &event_context)
        .await?;
    Ok(ApiResponse::ok(settings))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    description = "Applies an object-only JSON Merge Patch to the target principal settings. Object values merge recursively; a `null` value removes its key; arrays, strings, numbers, and booleans replace the existing value. An object patch applied to a missing or non-object value starts from an empty object. The document root must be an object. Use PUT, rather than PATCH, when a setting itself must retain a null value.",
    request_body(
        content = PrincipalSettings,
        description = "The settings patch object.",
        example = json!({
            "theme": "dark",
            "layout": { "sidebar": null, "columns": 2 }
        })
    ),
    responses(
        (status = 200, description = "Merged principal settings", body = PrincipalSettings, example = json!({
            "theme": "dark",
            "layout": { "density": "normal", "columns": 2 }
        })),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[patch("/{principal_id}/settings")]
pub async fn patch_principal_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    patch: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&pool, &requestor, principal_id.id()).await?;
    let event_context = requestor.event_context(&req);
    let settings = principal_id
        .patch_settings(&pool, patch.into_inner(), &event_context)
        .await?;
    Ok(ApiResponse::ok(settings))
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 204, description = "Principal settings reset"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[delete("/{principal_id}/settings")]
pub async fn delete_principal_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&pool, &requestor, principal_id.id()).await?;
    let event_context = requestor.event_context(&req);
    principal_id.reset_settings(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}
