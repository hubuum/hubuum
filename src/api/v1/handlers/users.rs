use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess, AdminOrSelfAccess};
use crate::models::search::parse_query_parameter;
use crate::models::user::{
    NewUser, UpdateUser, UserID, UserPointResponse, UserResponse, UserWithName,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::storage::with_revision_precondition;
use crate::traits::UserIdApplicationExt;
use actix_web::{HttpRequest, Responder, delete, get, patch, post, routes, web};
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
    context: AppContext,
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

    let search_params = prepare_db_pagination::<UserWithName>(&params)?;
    let (result, total_count) =
        crate::services::identity::list_users(&context, search_params).await?;

    ApiResponse::mapped_paginated(result, total_count, &params, |users| {
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
        (status = 201, description = "User created", body = UserPointResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_user(
    context: AppContext,
    new_user: web::Json<NewUser>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User create requested",
        requestor = requestor.user.id,
        new_user = new_user.name.as_str()
    );

    let event_context = requestor.event_context(&req);
    let user = new_user
        .into_inner()
        .save(&context, Some(&event_context))
        .await?;
    let response = user.to_point_response(&context).await?;

    let location = api_locations::user(user.id)?;
    ApiResponse::created_revisioned(response, location)
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
        (status = 200, description = "User", body = UserPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[get("/{user_id}")]
pub async fn get_user(
    context: AppContext,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    let user = user_id.into_inner().user(&context).await?;
    debug!(
        message = "User get requested",
        target = user.id,
        requestor = requestor.user.id
    );

    ApiResponse::ok_revisioned(user.to_point_response(&context).await?)
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
        (status = 200, description = "Updated user", body = UserPointResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Provider-managed user is read-only", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[patch("/{user_id}")]
pub async fn update_user(
    context: AppContext,
    user_id: web::Path<UserID>,
    updated_user: web::Json<UpdateUser>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user_id = user_id.into_inner();
    let target_id = user_id.id();
    debug!(
        message = "User patch requested",
        target = target_id,
        requestor = requestor.user.id
    );

    let current = user_id
        .user(&context)
        .await?
        .to_point_response(&context)
        .await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let user = with_revision_precondition(
        &context,
        precondition,
        updated_user
            .into_inner()
            .save(user_id, &context, Some(&event_context)),
    )
    .await?;
    ApiResponse::ok_revisioned(user.to_point_response(&context).await?)
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
        (status = 403, description = "Provider-managed user is read-only", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[delete("/{user_id}")]
pub async fn delete_user(
    context: AppContext,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User delete requested",
        target = user_id.id(),
        requestor = requestor.user.id
    );

    let user_id = user_id.into_inner();
    let current = user_id
        .user(&context)
        .await?
        .to_point_response(&context)
        .await?;
    let etag = current.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;

    let event_context = requestor.event_context(&req);
    let delete_result = with_revision_precondition(
        &context,
        precondition,
        user_id.delete(&context, Some(&event_context)),
    )
    .await;

    match delete_result {
        Ok(_) => Ok(ApiResponse::no_content_with_etag(etag)),
        Err(e) => Err(e),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/users/{user_id}/anonymize",
    tag = "users",
    security(("bearer_auth" = [])),
    params(("user_id" = i32, Path, description = "User ID")),
    responses(
        (status = 204, description = "User anonymized"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[post("/{user_id}/anonymize")]
pub async fn anonymize_user(
    context: AppContext,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target_id = user_id.id();
    let user_id = user_id.into_inner();
    debug!(
        message = "User anonymize requested",
        target = target_id,
        requestor = requestor.user.id
    );
    let current = user_id
        .user(&context)
        .await?
        .to_point_response(&context)
        .await?;
    let precondition = revision_precondition(&req, &current)?;
    with_revision_precondition(&context, precondition, user_id.anonymize(&context)).await?;
    let updated = user_id
        .user(&context)
        .await?
        .to_point_response(&context)
        .await?;
    Ok(ApiResponse::no_content_with_etag(updated.entity_tag()?))
}
