use actix_web::{HttpRequest, Responder, get, http::StatusCode, routes, web};
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
use crate::extractors::{Authenticated, ManagementAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{Group, Permissions, PrincipalMemberResponse, PrincipalToken};
use crate::pagination::prepare_db_pagination;
use crate::traits::GroupAccessors;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_me)
        .service(list_my_tokens)
        .service(list_my_groups)
        .service(list_my_permissions);
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
pub async fn get_me(requestor: Authenticated) -> Result<impl Responder, ApiError> {
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
            principal: requestor.principal.into(),
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
        (status = 200, description = "Groups the current principal belongs to", body = [Group]),
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
    ApiResponse::paginated(groups, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/permissions",
    tag = "principals",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Current principal effective permissions per collection, grouped by granting group", body = [PrincipalCollectionPermissions]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/permissions")]
pub async fn list_my_permissions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let report = principal_permissions_response(&pool, &requestor.principal).await?;
    Ok(ApiResponse::new(report, StatusCode::OK))
}
