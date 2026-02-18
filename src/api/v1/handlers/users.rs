use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};
use crate::models::search::parse_query_parameter;
use crate::models::user::{NewUser, UpdateUser, UserID};
use crate::models::{Group, User, UserToken};
use crate::utilities::response::{json_response, json_response_created};
use actix_web::{delete, get, http::StatusCode, patch, routes, web, HttpRequest, Responder};
use serde_json::json;
use tracing::debug;

#[utoipa::path(
    get,
    path = "/api/v1/iam/users",
    tag = "users",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Users matching optional query filters", body = [User]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_users(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "User list requested", requestor = user.username);

    let result = user.search_users(&pool, params).await?;

    Ok(json_response(result, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/users",
    tag = "users",
    security(("bearer_auth" = [])),
    request_body = NewUser,
    responses(
        (status = 201, description = "User created", body = User),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
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

#[utoipa::path(
    get,
    path = "/api/v1/iam/users/{user_id}/tokens",
    tag = "users",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 200, description = "Active tokens for user", body = [UserToken]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[get("/{user_id}/tokens")]
pub async fn get_user_tokens(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    use crate::db::traits::ActiveTokens;
    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User tokens requested",
        target = user.id,
        requestor = requestor.user.id
    );

    let valid_tokens = user.tokens(&pool).await?;
    Ok(json_response(valid_tokens, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/users/{user_id}",
    tag = "users",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 200, description = "User", body = User),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/api/v1/iam/users/{user_id}/groups",
    tag = "users",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 200, description = "Groups of user", body = [Group]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[get("/{user_id}/groups")]
pub async fn get_user_groups(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    use crate::models::traits::GroupAccessors;

    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User groups requested",
        target = user.id,
        requestor = requestor.user.id
    );

    let groups = user.groups(&pool).await?;
    Ok(json_response(groups, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/users/{user_id}",
    tag = "users",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID")
    ),
    request_body = UpdateUser,
    responses(
        (status = 200, description = "Updated user", body = User),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
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

#[utoipa::path(
    delete,
    path = "/api/v1/iam/users/{user_id}",
    tag = "users",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID")
    ),
    responses(
        (status = 204, description = "User deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
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
