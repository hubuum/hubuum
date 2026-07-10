use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, put, routes, web};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::principals::{
    PrincipalCollectionPermissions, principal_permissions_response,
};
use crate::db::DbPool;
use crate::db::traits::ActiveTokens;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated, ManagementAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{
    Group, GroupResponse, Permissions, PrincipalID, PrincipalMemberResponse, PrincipalSettings,
    PrincipalToken,
};
use crate::pagination::prepare_db_pagination;
use crate::traits::GroupAccessors;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_me)
        .service(list_my_tokens)
        .service(list_my_groups)
        .service(list_my_permissions)
        .service(get_my_settings)
        .service(put_my_settings)
        .service(patch_my_settings)
        .service(delete_my_settings);
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CurrentTokenMetadata {
    pub id: i32,
    pub name: Option<String>,
    pub description: Option<String>,
    pub issued: chrono::NaiveDateTime,
    pub expires_at: Option<chrono::NaiveDateTime>,
    pub last_used_at: Option<chrono::NaiveDateTime>,
    pub scoped: bool,
    pub scopes: Option<Vec<Permissions>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MeResponse {
    pub principal: PrincipalMemberResponse,
    pub token: CurrentTokenMetadata,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current authenticated principal and token", body = MeResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_me(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let token = CurrentTokenMetadata {
        id: requestor.token_meta.id,
        name: requestor.token_meta.name,
        description: requestor.token_meta.description,
        issued: requestor.token_meta.issued,
        expires_at: requestor.token_meta.expires_at,
        last_used_at: requestor.token_meta.last_used_at,
        scoped: requestor.token_meta.scoped,
        scopes: requestor.scopes,
    };

    Ok(ApiResponse::new(
        MeResponse {
            principal: PrincipalMemberResponse::from_principal(&pool, requestor.principal).await?,
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
    responses(
        (status = 200, description = "Current human user's active token metadata", body = [crate::models::PrincipalTokenMetadata]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/tokens")]
pub async fn list_my_tokens(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<PrincipalToken>(&params)?;
    let (tokens, total_count) = requestor
        .user
        .tokens_paginated_with_total_count(&pool, &search_params)
        .await?;

    ApiResponse::mapped_paginated(tokens, total_count, &params, |tokens| {
        tokens
            .into_iter()
            .map(crate::models::PrincipalTokenMetadata::from)
            .collect()
    })
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
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<Group>(&params)?;
    let (groups, total_count) = requestor
        .principal
        .groups_paginated_with_total_count(&pool, &search_params)
        .await?;
    let response = GroupResponse::from_groups(&pool, groups).await?;
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
    pool: web::Data<DbPool>,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let export = principal_permissions_response(&pool, &requestor.principal).await?;
    Ok(ApiResponse::new(export, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current principal settings", body = PrincipalSettings),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/settings")]
pub async fn get_my_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id)?;
    Ok(ApiResponse::ok(principal_id.settings(&pool).await?))
}

#[utoipa::path(
    put,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    request_body = PrincipalSettings,
    responses(
        (status = 200, description = "Replaced current principal settings", body = PrincipalSettings),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[put("/settings")]
pub async fn put_my_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    settings: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id)?;
    let event_context = requestor.event_context(&req);
    let settings = principal_id
        .replace_settings(&pool, settings.into_inner(), &event_context)
        .await?;
    Ok(ApiResponse::ok(settings))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/me/settings",
    tag = "principals",
    security(("bearer_auth" = [])),
    description = "Applies an object-only JSON Merge Patch to the current principal settings. Object values merge recursively; a `null` value removes its key; arrays, strings, numbers, and booleans replace the existing value. An object patch applied to a missing or non-object value starts from an empty object. The document root must be an object. Use PUT, rather than PATCH, when a setting itself must retain a null value.",
    request_body(
        content = PrincipalSettings,
        description = "The settings patch object.",
        example = json!({
            "theme": "dark",
            "layout": { "sidebar": null, "columns": 2 }
        })
    ),
    responses(
        (status = 200, description = "Merged current principal settings", body = PrincipalSettings, example = json!({
            "theme": "dark",
            "layout": { "density": "normal", "columns": 2 }
        })),
        (status = 400, description = "Settings root is not an object", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[patch("/settings")]
pub async fn patch_my_settings(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    patch: web::Json<PrincipalSettings>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id)?;
    let event_context = requestor.event_context(&req);
    let settings = principal_id
        .patch_settings(&pool, patch.into_inner(), &event_context)
        .await?;
    Ok(ApiResponse::ok(settings))
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
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let principal_id = PrincipalID::new(requestor.principal.id)?;
    let event_context = requestor.event_context(&req);
    principal_id.reset_settings(&pool, &event_context).await?;
    Ok(ApiResponse::no_content())
}
