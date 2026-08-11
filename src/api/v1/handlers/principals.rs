use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, put, web};
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::ToSchema;

use crate::api::etag::{RevisionedResource, revision_precondition};
use crate::api::openapi::{ApiErrorResponse, LoginResponse};
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{
    AccessEventContext, Authenticated, ManagementAccess, PrincipalSettingsPatchPayload,
};
use crate::models::collection::principal_all_permissions;
use crate::models::principal::{
    Principal, PrincipalKind, PrincipalSettings, PrincipalSettingsPatchDocument,
};
use crate::models::search::{
    QueryOptions, parse_query_parameter, parse_query_parameter_with_passthrough,
};
use crate::models::token::{renew_token_by_id_for_principal, revoke_token_by_id_for_principal};
use crate::models::{
    Group, GroupResponse, Permissions, PrincipalID, PrincipalToken, PrincipalTokenCreateRequest,
    PrincipalTokenMetadata, PrincipalTokenPointResponse, TokenID, TokenListState,
    TokenScopeDetails,
};
use crate::pagination::{effective_page_limit, finalize_page, prepare_db_pagination};
use crate::permissions::AppContext;
use crate::services::identity::{
    is_human_owner_group_member, list_retained_tokens, load_service_account, principal_is_disabled,
};
use crate::storage::StorageContext;
use crate::storage::with_revision_precondition;
use crate::traits::{AuthzSubject, GroupAccessors};
use std::collections::BTreeMap;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_token)
        .service(list_tokens)
        .service(get_token)
        .service(renew_token)
        .service(revoke_token)
        .service(list_principal_groups)
        .service(list_principal_permissions)
        .service(get_principal_settings)
        .service(put_principal_settings)
        .service(patch_principal_settings)
        .service(delete_principal_settings);
}

async fn ensure_can_manage_principal_settings(
    context: &impl StorageContext,
    requestor: &Authenticated,
    target_principal_id: i32,
) -> Result<(), ApiError> {
    if requestor.principal.id() == target_principal_id {
        return Ok(());
    }

    if requestor.scopes().is_none()
        && requestor.principal.is_human()
        && requestor.principal.is_admin(context).await?
    {
        return Ok(());
    }

    Err(ApiError::NotFound("Principal not found".to_string()))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewTokenRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    /// Requested expiry. It must be in the future and no farther from issuance
    /// than the server's public maximum token lifetime. When omitted, the
    /// server applies the public default lifetime.
    pub expires_at: Option<chrono::NaiveDateTime>,
    /// Optional permission and resource boundaries. Omit or send `null` for an
    /// unscoped token.
    pub scope: Option<TokenScopeDetails>,
}

impl NewTokenRequest {
    fn into_create_request(
        self,
        principal_id: PrincipalID,
    ) -> Result<PrincipalTokenCreateRequest, ApiError> {
        let scope = self
            .scope
            .map(TokenScopeDetails::into_request_scope)
            .transpose()?;
        Ok(PrincipalTokenCreateRequest::new(principal_id)
            .name(self.name)
            .description(self.description)
            .expires_at(self.expires_at)
            .scope(scope))
    }
}

#[derive(Debug, Deserialize)]
struct TokenPath {
    principal_id: PrincipalID,
    token_id: TokenID,
}

#[derive(Debug, Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RenewTokenRequest {
    /// Optional expiry for the new token. When omitted, the server applies its
    /// public default token lifetime. The source token's expiry is never
    /// copied.
    pub expires_at: Option<chrono::NaiveDateTime>,
}

pub(crate) fn parse_token_list_query(
    query_string: &str,
) -> Result<(QueryOptions, TokenListState), ApiError> {
    let (params, mut passthrough) =
        parse_query_parameter_with_passthrough(query_string, &["state"])?;
    let state = match passthrough.remove("state") {
        None => TokenListState::default(),
        Some(values) if values.len() == 1 => values[0].parse()?,
        Some(_) => {
            return Err(ApiError::BadRequest(
                "Query parameter 'state' must be provided at most once".to_string(),
            ));
        }
    };
    Ok((params, state))
}

/// Management authz for a principal's credentials/membership:
/// * human principal — self or admin;
/// * service account — admin or a **human** member of its owner group.
async fn ensure_can_manage_principal(
    context: &impl StorageContext,
    requestor: &ManagementAccess,
    principal: &Principal,
) -> Result<(), ApiError> {
    if requestor.user.is_admin(context).await? {
        return Ok(());
    }
    let permitted = match principal.principal_kind()? {
        PrincipalKind::Human => requestor.user.id == principal.id,
        PrincipalKind::ServiceAccount => {
            let sa = load_service_account(context, principal.id).await?;
            is_human_owner_group_member(context, requestor.user.id, sa.owner_group_id).await?
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
    context: &impl StorageContext,
    principal: &impl AuthzSubject,
) -> Result<Vec<PrincipalCollectionPermissions>, ApiError> {
    let rows = principal_all_permissions(context, principal).await?;

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
        (status = 201, description = "Raw token and authoritative expiry (shown once)", body = LoginResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Service account disabled", body = ApiErrorResponse)
    )
)]
#[post("/{principal_id}/tokens")]
pub async fn create_token(
    context: AppContext,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    body: web::Json<NewTokenRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    let principal = principal_id.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    // A disabled service account cannot mint credentials.
    if principal_is_disabled(&context, principal.id).await? {
        return Err(ApiError::Conflict(
            "Service account is disabled".to_string(),
        ));
    }

    let token_request = body.into_inner().into_create_request(principal_id)?;

    debug!(
        message = "Token mint requested",
        principal = principal.id,
        requestor = requestor.user.id,
        scoped = token_request.is_scoped()
    );

    let event_context = requestor.event_context(&req);
    let issued = token_request
        .create_issued(&context, Some(&event_context))
        .await?;

    Ok(ApiResponse::new(
        LoginResponse::from_issued(&issued),
        StatusCode::CREATED,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/tokens",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(
        ("principal_id" = i32, Path, description = "Principal id"),
        ("state" = Option<TokenListState>, Query, description = "Retained-token lifecycle subset. Defaults to active. Expired and revoked subsets may overlap.")
    ),
    responses(
        (status = 200, description = "Selected retained token metadata; active tokens by default", body = [PrincipalTokenMetadata]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/tokens")]
pub async fn list_tokens(
    context: AppContext,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    let (params, state) = parse_token_list_query(req.query_string())?;
    let search_params = prepare_db_pagination::<PrincipalToken>(&params)?;
    let (metadata, total_count) =
        list_retained_tokens(&context, pid.id(), search_params, state).await?;
    let page = finalize_page(metadata, &params)?;

    Ok(ApiResponse::paginated_items(
        page.items,
        &page.next_cursor,
        total_count,
        effective_page_limit(&params)?,
        false,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/tokens/{token_id}",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(
        ("principal_id" = i32, Path, description = "Principal id"),
        ("token_id" = i32, Path, description = "Token id")
    ),
    responses(
        (status = 200, description = "Retained token metadata", body = PrincipalTokenPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Token not found, not owned by this principal, or already purged", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/tokens/{token_id}")]
pub async fn get_token(
    context: AppContext,
    requestor: ManagementAccess,
    path: web::Path<TokenPath>,
) -> Result<impl Responder, ApiError> {
    let path = path.into_inner();
    let principal = path.principal_id.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;
    let token = PrincipalTokenMetadata::load_for_principal_token(
        &context,
        path.principal_id,
        path.token_id,
    )
    .await?;
    ApiResponse::ok_revisioned(PrincipalTokenPointResponse::from(token))
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/renew",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(
        ("principal_id" = i32, Path, description = "Principal id"),
        ("token_id" = i32, Path, description = "Source token id")
    ),
    request_body = RenewTokenRequest,
    responses(
        (status = 201, description = "Fresh raw token and authoritative expiry (shown once)", body = LoginResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Source token not found, not owned by this principal, or already purged", body = ApiErrorResponse),
        (status = 409, description = "Source token revoked or service account disabled", body = ApiErrorResponse)
    )
)]
#[post("/{principal_id}/tokens/{token_id}/renew")]
pub async fn renew_token(
    context: AppContext,
    requestor: ManagementAccess,
    path: web::Path<TokenPath>,
    body: web::Json<RenewTokenRequest>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let path = path.into_inner();
    let principal = path.principal_id.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    let event_context = requestor.event_context(&req);
    let issued = renew_token_by_id_for_principal(
        &context,
        path.token_id,
        path.principal_id,
        body.into_inner().expires_at,
        Some(&event_context),
    )
    .await?;

    Ok(ApiResponse::new(
        LoginResponse::from_issued(&issued),
        StatusCode::CREATED,
    ))
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
    context: AppContext,
    requestor: ManagementAccess,
    path: web::Path<TokenPath>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let path = path.into_inner();
    let principal = path.principal_id.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    let current = PrincipalTokenMetadata::load_for_principal_token(
        &context,
        path.principal_id,
        path.token_id,
    )
    .await?;
    let current = PrincipalTokenPointResponse::from(current);
    let precondition = revision_precondition(&req, &current)?;

    let event_context = requestor.event_context(&req);
    let revoked = with_revision_precondition(
        &context,
        precondition,
        revoke_token_by_id_for_principal(
            &context,
            path.token_id,
            path.principal_id,
            Some(&event_context),
        ),
    )
    .await?;
    if revoked == 0 {
        return Err(ApiError::NotFound(
            "Token not found for this principal".to_string(),
        ));
    }
    let updated = PrincipalTokenMetadata::load_for_principal_token(
        &context,
        path.principal_id,
        path.token_id,
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(
        PrincipalTokenPointResponse::from(updated).entity_tag()?,
    ))
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
    context: AppContext,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<Group>(&params)?;
    let (groups, total_count) = pid
        .groups_paginated_with_total_count(&context, &search_params)
        .await?;
    let response = GroupResponse::from_groups(&context, groups).await?;
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
    context: AppContext,
    requestor: ManagementAccess,
    principal_id: web::Path<PrincipalID>,
) -> Result<impl Responder, ApiError> {
    let pid = principal_id.into_inner();
    let principal = pid.principal(&context).await?;
    ensure_can_manage_principal(&context, &requestor, &principal).await?;

    let export = principal_permissions_response(&context, &pid).await?;
    Ok(ApiResponse::new(export, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    responses(
        (status = 200, description = "Principal settings", body = crate::models::PrincipalSettingsResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[get("/{principal_id}/settings")]
pub async fn get_principal_settings(
    context: AppContext,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&context, &requestor, principal_id.id()).await?;
    ApiResponse::ok_revisioned(principal_id.settings(&context).await?)
}

#[utoipa::path(
    put,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    request_body = PrincipalSettings,
    responses(
        (status = 200, description = "Replaced principal settings", body = crate::models::PrincipalSettingsResponse),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse)
    )
)]
#[put("/{principal_id}/settings")]
pub async fn put_principal_settings(
    context: AppContext,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    settings: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&context, &requestor, principal_id.id()).await?;
    let current = principal_id.settings(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let settings = with_revision_precondition(
        &context,
        precondition,
        principal_id.replace_settings(&context, settings.into_inner(), &event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(settings)
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/principals/{principal_id}/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(("principal_id" = i32, Path, description = "Principal id")),
    summary = "Patch principal settings",
    description = "Selects patch semantics from Content-Type and applies the complete patch to the latest row-locked settings document. `application/json` and `application/merge-patch+json` use object-only JSON Merge Patch: object values merge recursively, `null` removes a key, and other values replace it. `application/json-patch+json` uses bounded RFC 6902 add, remove, replace, move, copy, and test operations. The final document root must remain an object. A no-op returns the unchanged settings without advancing the revision or emitting an event.",
    request_body(
        description = "An object-only JSON Merge Patch or RFC 6902 operation array, selected by Content-Type.",
        content(
            (PrincipalSettings = "application/json", example = json!({
                "theme": "dark",
                "layout": { "sidebar": null, "columns": 2 }
            })),
            (PrincipalSettings = "application/merge-patch+json", example = json!({
                "theme": "dark",
                "layout": { "sidebar": null, "columns": 2 }
            })),
            (PrincipalSettingsPatchDocument = "application/json-patch+json", example = json!([
                { "op": "test", "path": "/theme", "value": "light" },
                { "op": "replace", "path": "/theme", "value": "dark" }
            ]))
        )
    ),
    responses(
        (status = 200, description = "Patched principal settings, or the unchanged settings for a no-op", body = crate::models::PrincipalSettingsResponse),
        (status = 400, description = "Malformed patch, invalid patch bounds, an invalid final root, or a result PostgreSQL JSONB cannot represent", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Principal not found", body = ApiErrorResponse),
        (status = 409, description = "A JSON Patch operation failed, including a failed test; nothing was persisted", body = ApiErrorResponse),
        (status = 413, description = "The patch request, result, nesting, or cumulative application work exceeds its limit", body = ApiErrorResponse),
        (status = 415, description = "Content-Type is not application/json, application/merge-patch+json, or application/json-patch+json", body = ApiErrorResponse),
        (status = 500, description = "Persistence or event emission failed and the transaction was rolled back", body = ApiErrorResponse)
    )
)]
#[patch("/{principal_id}/settings")]
pub async fn patch_principal_settings(
    context: AppContext,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    patch: PrincipalSettingsPatchPayload,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&context, &requestor, principal_id.id()).await?;
    let current = principal_id.settings(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let settings = with_revision_precondition(
        &context,
        precondition,
        principal_id.apply_settings_patch(&context, patch.into_inner(), &event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(settings)
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
    context: AppContext,
    requestor: Authenticated,
    principal_id: web::Path<PrincipalID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = principal_id.into_inner();
    ensure_can_manage_principal_settings(&context, &requestor, principal_id.id()).await?;
    let current = principal_id.settings(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let reset = with_revision_precondition(
        &context,
        precondition,
        principal_id.reset_settings(&context, &event_context),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(reset.entity_tag()?))
}
