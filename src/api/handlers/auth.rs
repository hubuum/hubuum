use crate::api::openapi::{ApiErrorResponse, LoginResponse, MessageResponse};
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::middlewares::rate_limit::{
    clear_login_failures, client_ip_for_request, login_is_rate_limited, record_login_failure,
};
use crate::models::{LoginUser, Token, UserID};
use crate::utilities::response::json_response;
use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, warn};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LogoutTokenRequest {
    pub token: String,
}

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.

#[utoipa::path(
    post,
    path = "/api/v0/auth/login",
    tag = "auth",
    request_body = LoginUser,
    responses(
        (status = 200, description = "Token issued", body = LoginResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Too many login attempts", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/login")]
pub async fn login(
    pool: web::Data<DbPool>,
    req: HttpRequest,
    req_input: web::Json<LoginUser>,
) -> Result<impl Responder, ApiError> {
    let login = req_input.into_inner();
    let username = login.username.clone();
    let client_ip = client_ip_for_request(&req);

    if login_is_rate_limited(&username, &client_ip).await {
        warn!(
            message = "Login throttled",
            user = username,
            client_ip = client_ip
        );
        return Ok(json_response(
            json!({ "error": "Too Many Requests", "message": "Too many login attempts. Please try again later." }),
            StatusCode::TOO_MANY_REQUESTS,
        ));
    }

    debug!(message = "Login started", user = username);
    let user = match login.login(&pool).await {
        Ok(user) => user,
        Err(e) => {
            if let ApiError::Unauthorized(_) = &e {
                record_login_failure(&username, &client_ip).await;
            }
            return Err(e);
        }
    };

    clear_login_failures(&username, &client_ip).await;

    let token_generation_result = user.create_token(&pool).await;

    let token = match token_generation_result {
        Ok(token) => token,
        Err(e) => {
            warn!(
                message = "Login failed (token generation failed)",
                user = user.username,
                error = e.to_string()
            );
            return Err(ApiError::InternalServerError(
                "Internal authentication failure".to_string(),
            ));
        }
    };

    debug!(
        message = "Login successful",
        username = user.username,
        user_id = user.id,
        token = token.obfuscate()
    );

    Ok(json_response(
        json!({"token": token.get_token()}),
        StatusCode::OK,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v0/auth/logout",
    tag = "auth",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Token revoked", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/logout")]
pub async fn logout(
    pool: web::Data<DbPool>,
    user_access: UserAccess,
) -> Result<impl Responder, ApiError> {
    let token = user_access.token;

    debug!(message = "Logging out token.", token = token.obfuscate());

    let result = token.delete(&pool).await;

    match result {
        Ok(_) => Ok(json_response(
            json!({ "message": "Logout successful."}),
            StatusCode::OK,
        )),
        Err(e) => {
            warn!(
                message = "Logout failed",
                token_used = token.obfuscate(),
                error = e.to_string()
            );
            Err(ApiError::InternalServerError(
                "Internal authentication failure".to_string(),
            ))
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v0/auth/logout_all",
    tag = "auth",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "All tokens revoked for current user", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/logout_all")]
pub async fn logout_all(
    pool: web::Data<DbPool>,
    user_access: UserAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Logging out all tokens for {}.",
        user_access.user.id
    );

    let delete_result = user_access.user.delete_all_tokens(&pool).await;

    match delete_result {
        Ok(_) => Ok(json_response(
            json!({ "message": "Logout of all tokens successful."}),
            StatusCode::OK,
        )),
        Err(e) => {
            warn!(
                message = "Logout of all tokens failed",
                token_used = user_access.token.obfuscate(),
                user_id = user_access.user.id,
                error = e.to_string()
            );
            Err(ApiError::InternalServerError(
                "Internal authentication failure.".to_string(),
            ))
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v0/auth/logout/token",
    tag = "auth",
    security(("bearer_auth" = [])),
    request_body = LogoutTokenRequest,
    responses(
        (status = 200, description = "Token revoked", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/logout/token")]
pub async fn logout_token(
    pool: web::Data<DbPool>,
    user_access: AdminAccess,
    token: web::Json<LogoutTokenRequest>,
) -> Result<impl Responder, ApiError> {
    let token = Token(token.into_inner().token);
    debug!(message = "Logging out token {}.", token = token.obfuscate());

    let result = token.delete(&pool).await;

    match result {
        Ok(_) => Ok(json_response(
            json!({ "message": "Logout of token successful."}),
            StatusCode::OK,
        )),
        Err(e) => {
            warn!(
                message = "Logout of token failed",
                token_used = token.obfuscate(),
                token_target = user_access.token.obfuscate(),
                user_id = user_access.user.id,
                error = e.to_string()
            );
            Err(ApiError::InternalServerError(
                "Internal authentication failure.".to_string(),
            ))
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v0/auth/logout/uid/{user_id}",
    tag = "auth",
    security(("bearer_auth" = [])),
    params(
        ("user_id" = i32, Path, description = "User ID to revoke all tokens for")
    ),
    responses(
        (status = 200, description = "All tokens revoked for user", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/logout/uid/{user_id}")]
pub async fn logout_other(
    pool: web::Data<DbPool>,
    admin_access: AdminAccess,
    user_id: web::Path<UserID>,
) -> Result<impl Responder, ApiError> {
    use crate::traits::SelfAccessors;

    debug!(
        message = "Logging out all tokens, {} on behalf of {}.",
        admin_access = admin_access.user.id,
        user_id = user_id.id()
    );

    let delete_result = user_id
        .instance(&pool)
        .await?
        .delete_all_tokens(&pool)
        .await;

    match delete_result {
        Ok(_) => Ok(json_response(
            json!({ "message": format!("Logout of tokens for {} successful.", user_id.id())}),
            StatusCode::OK,
        )),
        Err(e) => {
            warn!(
                message = "Logout of other tokens failed",
                token_used = admin_access.token.obfuscate(),
                user_id = admin_access.user.id,
                error = e.to_string()
            );
            Err(ApiError::InternalServerError(
                "Internal authentication failure.".to_string(),
            ))
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v0/auth/validate",
    tag = "auth",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Token is valid", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/validate")]
pub async fn validate_token(user_access: UserAccess) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Token validation successful",
        user_id = user_access.user.id,
        token = user_access.token.obfuscate()
    );

    Ok(json_response(
        json!({ "message": "Token is valid."}),
        StatusCode::OK,
    ))
}
