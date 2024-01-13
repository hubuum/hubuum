use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};
use crate::models::group::{Group, GroupID, NewGroup, UpdateGroup};
use crate::models::user::{NewUser, UpdateUser, User, UserID};
use crate::models::user_group::UserGroup;
use crate::utilities::response::{json_response, json_response_created};
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};
use serde_json::json;
use tracing::debug;

#[get("/users")]
pub async fn get_users(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    use crate::schema::users::dsl::users;
    use diesel::RunQueryDsl;
    let mut conn = pool.get()?;

    debug!(
        message = "User list requested",
        requestor = requestor.user.username
    );

    let result = users.load::<User>(&mut conn)?;
    Ok(json_response(result, StatusCode::OK))
}

#[post("/users")]
pub async fn create_user(
    pool: web::Data<DbPool>,
    new_user: web::Json<NewUser>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User create requested",
        requestor = requestor.user.id,
        new_user = new_user.username.as_str()
    );

    let result = new_user.into_inner().save(&pool).await?;

    Ok(json_response_created(
        format!("/api/v1/iam/users/{}", result.id).as_str(),
    ))
}

#[get("/users/{user_id}/tokens")]
pub async fn get_user_tokens(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User tokens requested",
        target = user.id,
        requestor = requestor.user.id
    );

    let valid_tokens = user.get_tokens(&pool).await?;
    Ok(json_response(valid_tokens, StatusCode::OK))
}

#[get("/users/{user_id}")]
pub async fn get_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User get requested",
        target = user.id,
        requestor = requestor.user.id
    );

    Ok(json_response(user, StatusCode::OK))
}

#[patch("/users/{user_id}")]
pub async fn update_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    updated_user: web::Json<UpdateUser>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User patch requested",
        target = user.id,
        requestor = requestor.user.id
    );

    let user = updated_user
        .into_inner()
        .hash_password()?
        .save(user.id, &pool)
        .await?;
    Ok(json_response(user, StatusCode::OK))
}

#[delete("/users/{user_id}")]
pub async fn delete_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User delete requested",
        target = user_id.0,
        requestor = requestor.user.id
    );

    let delete_result = user_id.delete(&pool).await;

    match delete_result {
        Ok(elements) => Ok(json_response(json!(elements), StatusCode::NO_CONTENT)),
        Err(e) => Err(e),
    }
}

#[post("/groups")]
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
        format!("/api/v1/iam/groups/{}", group.id).as_str(),
    ))
}

#[get("/groups/{group_id}")]
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

#[get("/groups")]
pub async fn get_groups(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    use crate::schema::groups::dsl::*;
    use diesel::RunQueryDsl;
    let mut conn = pool.get()?;

    debug!(
        message = "Group list requested",
        requestor = requestor.user.id
    );

    let result = groups.load::<Group>(&mut conn)?;

    Ok(json_response(result, StatusCode::OK))
}

#[patch("/groups/{group_id}")]
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

#[delete("/groups/{group_id}")]
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

#[get("/groups/{group_id}/members")]
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

#[post("/groups/{group_id}/members/{user_id}")]
pub async fn add_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<UserGroup>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group(&pool).await?;
    let user = user_group_ids.user(&pool).await?;

    debug!(
        message = "Adding user to group",
        user = user.id,
        group = group.id,
        requestor = requestor.user.id
    );

    group.add_member(&user, &pool).await?;

    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}

#[delete("/groups/{group_id}/members/{user_id}")]
pub async fn delete_group_member(
    pool: web::Data<DbPool>,
    user_group_ids: web::Path<UserGroup>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group(&pool).await?;
    let user = user_group_ids.user(&pool).await?;

    debug!(
        message = "Deleting user from group",
        user = user.id,
        group = group.id,
        requestor = requestor.user.id
    );

    user_group_ids.delete(&pool).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}
