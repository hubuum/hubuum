use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::group::{GroupID, NewGroup, UpdateGroup};
use crate::models::search::parse_query_parameter;
use crate::models::UserID;
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
