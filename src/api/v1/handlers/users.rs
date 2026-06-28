use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, AdminOrSelfAccess};
use crate::models::search::parse_query_parameter;
use crate::models::user::{NewUser, UpdateUser, UserID, UserResponse, UserWithName};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::utilities::response::{
    json_response, json_response_created, paginated_json_mapped_response,
};
use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, routes, web};
use serde_json::json;
use tracing::debug;

#[utoipa::path(
    get,
    path = "/api/v1/iam/users",
    tag = "users",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Users matching optional query filters", body = [UserResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_users(
    pool: web::Data<DbPool>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "User list requested", requestor = user.id);

    let total_count = user
        .count_users(&pool, count_query_options(&params))
        .await?;
    let search_params = prepare_db_pagination::<UserWithName>(&params)?;
    let result = user.search_users(&pool, search_params).await?;

    paginated_json_mapped_response(result, total_count, StatusCode::OK, &params, |users| {
        users.into_iter().map(UserResponse::from).collect()
    })
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/users",
    tag = "users",
    security(("bearer_auth" = [])),
    request_body = NewUser,
    responses(
        (status = 201, description = "User created", body = UserResponse),
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
        new_user = new_user.name.as_str()
    );

    let user = new_user.into_inner().save(&pool).await?;
    let response = user.to_response(&pool).await?;

    Ok(json_response_created(
        response,
        format!("/api/v1/iam/users/{}", user.id).as_str(),
    ))
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
        (status = 200, description = "User", body = UserResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[get("/{user_id}")]
pub async fn get_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    let user = user_id.into_inner().user(&pool).await?;
    debug!(
        message = "User get requested",
        target = user.id,
        requestor = requestor.user.id
    );

    Ok(json_response(
        user.to_response(&pool).await?,
        StatusCode::OK,
    ))
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
        (status = 200, description = "Updated user", body = UserResponse),
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

    let user = updated_user.into_inner().save(user.id, &pool).await?;
    Ok(json_response(
        user.to_response(&pool).await?,
        StatusCode::OK,
    ))
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
        target = user_id.id(),
        requestor = requestor.user.id
    );

    let delete_result = user_id.delete(&pool).await;

    match delete_result {
        Ok(elements) => Ok(json_response(json!(elements), StatusCode::NO_CONTENT)),
        Err(e) => Err(e),
    }
}
