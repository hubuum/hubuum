use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;
use utoipa::ToSchema;

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::ActiveTokens;
use crate::errors::ApiError;
use crate::extractors::ManagementAccess;
use crate::models::namespace::principal_all_permissions;
use crate::models::principal::{Principal, PrincipalKind};
use crate::models::search::parse_query_parameter;
use crate::models::service_account::{
    is_human_owner_group_member, load_service_account_by_id, principal_is_disabled,
};
use crate::models::token::{create_principal_token, revoke_token_by_id_for_principal};
use crate::models::{Group, Permissions, PrincipalID, PrincipalToken, PrincipalTokenMetadata};
use crate::pagination::prepare_db_pagination;
use crate::traits::{AuthzSubject, GroupAccessors};
use crate::utilities::response::{
    json_response, paginated_json_mapped_response, paginated_json_response,
};
use std::collections::BTreeMap;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_token)
        .service(list_tokens)
        .service(revoke_token)
        .service(list_principal_groups)
        .service(list_principal_permissions);
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
        Err(ApiError::Forbidden(
            "Not permitted to manage this principal".to_string(),
        ))
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/principals/{principal_id}/tokens",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    request_body = NewTokenRequest,
    responses(
        (status = 201, description = "Raw token (shown once)"),
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

    debug!(
        message = "Token mint requested",
        principal = principal.id,
        requestor = requestor.user.id,
        scoped = body.scopes.is_some()
    );

    let raw = create_principal_token(
        &pool,
        principal.id,
        body.name.as_deref(),
        body.description.as_deref(),
        body.expires_at,
        body.scopes.as_deref(),
    )
    .await?;

    Ok(json_response(
        json!({ "token": raw.get_token() }),
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

    paginated_json_mapped_response(tokens, total_count, StatusCode::OK, &params, |tokens| {
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
) -> Result<impl Responder, ApiError> {
    let path = path.into_inner();
    let principal = path.principal_id.principal(&pool).await?;
    ensure_can_manage_principal(&pool, &requestor, &principal).await?;

    let revoked = revoke_token_by_id_for_principal(&pool, path.token_id, principal.id).await?;
    if revoked == 0 {
        return Err(ApiError::NotFound(
            "Token not found for this principal".to_string(),
        ));
    }
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/groups",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Groups the principal belongs to", body = [Group]),
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
    paginated_json_response(groups, total_count, StatusCode::OK, &params)
}

/// One group's contribution to a principal's effective permissions on a namespace.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GroupGrant {
    pub group_id: i32,
    pub groupname: String,
    pub permissions: Vec<Permissions>,
}

/// A principal's effective permissions on a single namespace, broken down by the
/// group that grants them.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PrincipalNamespacePermissions {
    pub namespace_id: i32,
    pub namespace_name: String,
    pub grants: Vec<GroupGrant>,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/permissions",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Effective permissions per namespace, grouped by granting group", body = [PrincipalNamespacePermissions]),
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

    let rows = principal_all_permissions(&pool, pid).await?;

    // Fold (namespace, group, permission-row) tuples into a per-namespace,
    // per-group report. BTreeMap keeps namespaces in a stable id order; groups
    // with no granted flags are dropped.
    let mut by_namespace: BTreeMap<i32, PrincipalNamespacePermissions> = BTreeMap::new();
    for (namespace, group, permission) in rows {
        let permissions = permission.granted();
        if permissions.is_empty() {
            continue;
        }
        by_namespace
            .entry(namespace.id)
            .or_insert_with(|| PrincipalNamespacePermissions {
                namespace_id: namespace.id,
                namespace_name: namespace.name.clone(),
                grants: Vec::new(),
            })
            .grants
            .push(GroupGrant {
                group_id: group.id,
                groupname: group.groupname.clone(),
                permissions,
            });
    }

    let report: Vec<PrincipalNamespacePermissions> = by_namespace.into_values().collect();
    Ok(json_response(report, StatusCode::OK))
}
