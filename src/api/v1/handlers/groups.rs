use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::group::{GroupID, NewGroup, UpdateGroup};
use crate::models::search::parse_query_parameter;
use crate::models::service_account::service_accounts_owned_by_group;
use crate::models::{Group, Principal, PrincipalID, PrincipalMemberResponse};
use crate::pagination::{count_query_options, prepare_db_pagination};
use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Serialize, Deserialize)]
struct GroupMember {
    pub principal_id: PrincipalID,
    pub group_id: GroupID,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups",
    tag = "groups",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Groups matching optional query filters", body = [Group]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_groups(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user.clone();
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(
        message = "Group list requested",
        requestor = requestor.user.id,
        params = ?params
    );

    let total_count = user
        .count_groups(&pool, count_query_options(&params))
        .await?;
    let search_params = prepare_db_pagination::<Group>(&params)?;
    let result = user.search_groups(&pool, search_params).await?;

    ApiResponse::paginated(result, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/groups",
    tag = "groups",
    security(("bearer_auth" = [])),
    request_body = NewGroup,
    responses(
        (status = 201, description = "Group created", body = Group),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_group(
    pool: web::Data<DbPool>,
    new_group: web::Json<NewGroup>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group create requested",
        requestor = requestor.user.id,
        new_group = ?new_group
    );

    let event_context = requestor.event_context(&req);
    let group = new_group
        .save_with_context(&pool, Some(&event_context))
        .await?;

    let location = api_locations::group(group.id)?;
    Ok(ApiResponse::created(group, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Group", body = Group),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}")]
pub async fn get_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let group = group_id.group(&pool).await?;

    debug!(
        message = "Group get requested",
        target = group.id,
        requestor = requestor.user.id
    );

    Ok(ApiResponse::new(group, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = UpdateGroup,
    responses(
        (status = 200, description = "Updated group", body = Group),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[patch("/{group_id}")]
pub async fn update_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    updated_group: web::Json<UpdateGroup>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group = group_id.group(&pool).await?;

    debug!(
        message = "Group patch requested",
        target = group.id,
        requestor = requestor.user.id
    );

    let event_context = requestor.event_context(&req);
    let updated = updated_group
        .into_inner()
        .save_with_context(group.id, &pool, Some(&event_context))
        .await?;
    Ok(ApiResponse::new(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 204, description = "Group deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse),
        (status = 409, description = "Group still owns service accounts", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}")]
pub async fn delete_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group delete requested",
        target = group_id.id(),
        requestor = requestor.user.id
    );

    // owner_group_id is ON DELETE RESTRICT: surface a clear 409 listing the owned
    // service accounts instead of letting the FK violation become an opaque error.
    let owned = service_accounts_owned_by_group(&pool, group_id.id()).await?;
    if !owned.is_empty() {
        let list = owned
            .iter()
            .map(|(id, name)| format!("{name} (id {id})"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::Conflict(format!(
            "Group owns service accounts; reassign or delete them first: {list}"
        )));
    }

    let event_context = requestor.event_context(&req);
    group_id
        .delete_with_context(&pool, Some(&event_context))
        .await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}/members",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Members of group", body = [PrincipalMemberResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}/members")]
pub async fn get_group_members(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;

    let group = group_id.group(&pool).await?;

    debug!(
        message = "Group members requested",
        target = group.id,
        requestor = requestor.user.id
    );

    let count_params = count_query_options(&params);
    let total_count = group.count_members_paginated(&pool, &count_params).await?;
    let search_params = prepare_db_pagination::<Principal>(&params)?;
    let members = group.members_paginated(&pool, &search_params).await?;

    ApiResponse::mapped_paginated(members, total_count, &params, |members| {
        members
            .into_iter()
            .map(PrincipalMemberResponse::from)
            .collect()
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 204, description = "User added to group"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{group_id}/members/{principal_id}")]
pub async fn add_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&pool).await?;
    let principal = user_group_ids.principal_id.principal(&pool).await?;

    debug!(
        message = "Adding principal to group",
        principal = principal.id,
        group = group.id,
        requestor = requestor.user.id
    );

    let event_context = requestor.event_context(&req);
    group
        .add_member_with_context(&pool, &principal, Some(&event_context))
        .await?;

    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 204, description = "User removed from group"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}/members/{principal_id}")]
pub async fn delete_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&pool).await?;
    let principal = user_group_ids.principal_id.principal(&pool).await?;

    debug!(
        message = "Deleting principal from group",
        principal = principal.id,
        group = group.id,
        requestor = requestor.user.id
    );

    let event_context = requestor.event_context(&req);
    group
        .remove_member_with_context(&principal, &pool, Some(&event_context))
        .await?;
    Ok(ApiResponse::no_content())
}
