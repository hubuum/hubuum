use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, put, routes, web};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::etag::{RevisionedResource, revision_precondition};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::principals::{
    PrincipalCollectionPermissions, parse_token_list_query, principal_permissions_response,
};
use crate::errors::ApiError;
use crate::extractors::{
    AccessEventContext, Authenticated, ManagementAccess, PrincipalSettingsPatchPayload,
};
use crate::models::principal::{apply_principal_settings_patch, load_principal_by_id};
use crate::models::search::parse_query_parameter;
use crate::models::{
    Group, GroupResponse, PrincipalID, PrincipalSettings, PrincipalSettingsPatchDocument,
    PrincipalToken, TokenListState,
};
use crate::pagination::{effective_page_limit, finalize_page, prepare_db_pagination};
use crate::permissions::AppContext;
use crate::services::identity::list_retained_tokens;
use crate::storage::with_revision_precondition;
use crate::traits::{GroupAccessors, PrincipalIdApplicationExt};

pub use crate::models::CurrentTokenMetadata;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_me)
        .service(crate::api::v1::handlers::computed_fields::get_personal_computed_fields)
        .service(crate::api::v1::handlers::computed_fields::get_personal_computed_field)
        .service(crate::api::v1::handlers::computed_fields::create_personal_computed_field)
        .service(crate::api::v1::handlers::computed_fields::patch_personal_computed_field)
        .service(crate::api::v1::handlers::computed_fields::delete_personal_computed_field)
        .service(crate::api::v1::handlers::computed_fields::preview_personal_computed_field)
        .service(list_my_tokens)
        .service(list_my_groups)
        .service(list_my_permissions)
        .service(get_my_settings)
        .service(put_my_settings)
        .service(patch_my_settings)
        .service(delete_my_settings);
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MeResponse {
    pub principal: crate::models::MembershipPrincipalResponse,
    pub token: CurrentTokenMetadata,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current authenticated principal and token, including permission and resource scope dimensions", body = MeResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_me(
    context: AppContext,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let principal = load_principal_by_id(&context, requestor.principal.id().id()).await?;
    let token =
        CurrentTokenMetadata::from_authenticated_token(&requestor.token_meta, requestor.scope)?;

    Ok(ApiResponse::new(
        MeResponse {
            principal: crate::models::MembershipPrincipalResponse::from_principal(
                &context, principal,
            )
            .await?,
            token,
        },
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/tokens",
    tag = "principals",
    security(("bearer_auth" = [])),
    params(
        ("state" = Option<TokenListState>, Query, description = "Retained-token lifecycle subset. Defaults to active. Expired and revoked subsets may overlap.")
    ),
    responses(
        (status = 200, description = "Current human user's selected retained token metadata; active tokens by default", body = [crate::models::PrincipalTokenMetadata]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/tokens")]
pub async fn list_my_tokens(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (params, state) = parse_token_list_query(req.query_string())?;
    let search_params = prepare_db_pagination::<PrincipalToken>(&params)?;
    let (metadata, total_count) =
        list_retained_tokens(&context, requestor.user.id, search_params, state).await?;
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
    path = "/api/v1/iam/me/groups",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Groups the current principal belongs to", body = [GroupResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/groups")]
pub async fn list_my_groups(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<Group>(&params)?;
    let (groups, total_count) = requestor
        .principal
        .groups_paginated_with_total_count(&context, &search_params)
        .await?;
    let response = GroupResponse::from_groups(&context, groups).await?;
    ApiResponse::paginated(response, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/permissions",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current principal direct permission rows per collection, grouped by granting group", body = [PrincipalCollectionPermissions]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/permissions")]
pub async fn list_my_permissions(
    context: AppContext,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let export = principal_permissions_response(&context, &requestor.principal).await?;
    Ok(ApiResponse::new(export, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current principal settings", body = crate::models::PrincipalSettingsResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/settings")]
pub async fn get_my_settings(
    context: AppContext,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id().id())?;
    ApiResponse::ok_revisioned(principal_id.settings(&context).await?)
}

#[utoipa::path(
    put,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    request_body = PrincipalSettings,
    responses(
        (status = 200, description = "Replaced current principal settings", body = crate::models::PrincipalSettingsResponse),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[put("/settings")]
pub async fn put_my_settings(
    context: AppContext,
    requestor: Authenticated,
    settings: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id().id())?;
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
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    summary = "Patch current principal settings",
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
        (status = 200, description = "Patched current principal settings, or the unchanged settings for a no-op", body = crate::models::PrincipalSettingsResponse),
        (status = 400, description = "Malformed patch, invalid patch bounds, an invalid final root, or a result PostgreSQL JSONB cannot represent", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "A JSON Patch operation failed, including a failed test; nothing was persisted", body = ApiErrorResponse),
        (status = 413, description = "The patch request, result, nesting, or cumulative application work exceeds its limit", body = ApiErrorResponse),
        (status = 415, description = "Content-Type is not application/json, application/merge-patch+json, or application/json-patch+json", body = ApiErrorResponse),
        (status = 500, description = "Persistence or event emission failed and the transaction was rolled back", body = ApiErrorResponse)
    )
)]
#[patch("/settings")]
pub async fn patch_my_settings(
    context: AppContext,
    requestor: Authenticated,
    patch: PrincipalSettingsPatchPayload,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id().id())?;
    let current = principal_id.settings(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let settings = with_revision_precondition(
        &context,
        precondition,
        apply_principal_settings_patch(principal_id, &context, patch.into_inner(), &event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(settings)
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 204, description = "Current principal settings reset"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[delete("/settings")]
pub async fn delete_my_settings(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id().id())?;
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
