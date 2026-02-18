use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::group::{GroupID, NewGroup, UpdateGroup};
use crate::models::search::parse_query_parameter;
use crate::models::{Group, User, UserID};
use crate::utilities::response::{json_response, json_response_created};
use actix_web::{delete, get, http::StatusCode, patch, post, routes, web, HttpRequest, Responder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;

#[derive(Serialize, Deserialize)]
struct GroupMember {
    pub user_id: UserID,
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

    let result = user.search_groups(&pool, params).await?;

    Ok(json_response(result, StatusCode::OK))
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
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group create requested",
        requestor = requestor.user.id,
        new_group = ?new_group
    );

    let group = new_group.save(&pool).await?;

    Ok(json_response_created(
        &group,
        format!("/api/v1/iam/groups/{}", group.id).as_str(),
    ))
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

    Ok(json_response(group, StatusCode::OK))
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
) -> Result<impl Responder, ApiError> {
    let group = group_id.group(&pool).await?;

    debug!(
        message = "Group patch requested",
        target = group.id,
        requestor = requestor.user.id
    );

    let updated = updated_group.into_inner().save(group.id, &pool).await?;
    Ok(json_response(updated, StatusCode::OK))
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
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}")]
pub async fn delete_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group delete requested",
        target = group_id.0,
        requestor = requestor.user.id
    );

    group_id.delete(&pool).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
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
        (status = 200, description = "Members of group", body = [User]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}/members")]
pub async fn get_group_members(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let group = group_id.group(&pool).await?;

    debug!(
        message = "Group members requested",
        target = group.id,
        requestor = requestor.user.id
    );

    let members = group.members(&pool).await?;

    Ok(json_response(members, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/groups/{group_id}/members/{user_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 204, description = "User added to group"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{group_id}/members/{user_id}")]
pub async fn add_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&pool).await?;
    let user = user_group_ids.user_id.user(&pool).await?;

    debug!(
        message = "Adding user to group",
        user = user.id,
        group = group.id,
        requestor = requestor.user.id
    );

    group.add_member(&pool, &user).await?;

    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/groups/{group_id}/members/{user_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 204, description = "User removed from group"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}/members/{user_id}")]
pub async fn delete_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&pool).await?;
    let user = user_group_ids.user_id.user(&pool).await?;

    debug!(
        message = "Deleting user from group",
        user = user.id,
        group = group.id,
        requestor = requestor.user.id
    );

    group.remove_member(&user, &pool).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}
