use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};
use crate::models::user::{NewUser, UpdateUser, User, UserID};
use crate::utilities::response::{json_response, json_response_created};
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};
use serde_json::json;
use tracing::debug;

#[get("")]
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

#[post("")]
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

    let user = new_user.into_inner().save(&pool).await?;

    Ok(json_response_created(
        &user,
        format!("/api/v1/iam/users/{}", user.id).as_str(),
    ))
}

#[get("/{user_id}/tokens")]
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

#[get("/{user_id}")]
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

#[patch("/{user_id}")]
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

#[delete("/{user_id}")]
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
