use crate::api::openapi::{ApiErrorResponse, LoginResponse, MessageResponse};
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, Authenticated, ManagementAccess};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, client_ip_for_request, finish_login_attempt,
};
use crate::models::{LOCAL_IDENTITY_SCOPE, LoginUser, Token, UserID};
use crate::observability::metrics;
use actix_web::{HttpRequest, Responder, get, post, web};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LogoutTokenRequest {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AuthProvidersResponse {
    pub providers: Vec<String>,
}

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.

#[utoipa::path(
    get,
    path = "/api/v0/auth/providers",
    tag = "auth",
    responses(
        (status = 200, description = "Configured authentication provider names", body = AuthProvidersResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("/providers")]
pub async fn get_auth_providers() -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::ok(AuthProvidersResponse {
        providers: crate::auth::auth_provider_names()?,
    }))
}

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
    let identity_scope = login
        .identity_scope
        .clone()
        .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
    let name = login.name.clone();
    let client_ip = client_ip_for_request(&req);
    let client_ip_log = client_ip.map(|ip| ip.to_string());

    let Some(login_permit) = begin_login_attempt(&identity_scope, &name, client_ip).await? else {
        metrics::login_attempt("rate_limited");
        warn!(
            message = "Login throttled",
            identity_scope = identity_scope,
            user = name,
            client_ip = client_ip_log.as_deref()
        );
        return Err(ApiError::TooManyRequests(
            "Too many login attempts. Please try again later.".to_string(),
        ));
    };

    debug!(message = "Login started", user = name);
    let user = match crate::auth::login(&pool, login).await {
        Ok(user) => user,
        Err(e) => {
            let (outcome, metric_outcome) = if matches!(e, ApiError::Unauthorized(_)) {
                (LoginAttemptOutcome::Failed, "bad_credentials")
            } else {
                (LoginAttemptOutcome::Aborted, "internal_error")
            };
            finish_login_attempt(login_permit, outcome).await?;
            metrics::login_attempt(metric_outcome);
            return Err(e);
        }
    };

    finish_login_attempt(login_permit, LoginAttemptOutcome::Succeeded).await?;

    let token_generation_result = user.create_token(&pool).await;

    let token = match token_generation_result {
        Ok(token) => token,
        Err(e) => {
            metrics::login_attempt("internal_error");
            warn!(
                message = "Login failed (token generation failed)",
                user = name,
                error = e.to_string()
            );
            return Err(ApiError::InternalServerError(
                "Internal authentication failure".to_string(),
            ));
        }
    };

    debug!(message = "Login successful", name = name, user_id = user.id,);
    metrics::login_attempt("success");

    Ok(ApiResponse::ok(LoginResponse::new(token.get_token())))
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
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    let token = requestor.token;

    debug!(message = "token logout requested");

    let result = token.delete(&pool).await;

    match result {
        Ok(_) => Ok(ApiResponse::message("Logout successful.")),
        Err(e) => {
            warn!(message = "Logout failed", error = e.to_string());
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
    user_access: ManagementAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Logging out all tokens for {}.",
        user_access.user.id
    );

    let delete_result = user_access.user.delete_all_tokens(&pool).await;

    match delete_result {
        Ok(_) => Ok(ApiResponse::message("Logout of all tokens successful.")),
        Err(e) => {
            warn!(
                message = "Logout of all tokens failed",
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
    debug!(message = "administrative token logout requested");

    let result = token.delete(&pool).await;

    match result {
        Ok(_) => Ok(ApiResponse::message("Logout of token successful.")),
        Err(e) => {
            warn!(
                message = "Logout of token failed",
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
        Ok(_) => Ok(ApiResponse::message(format!(
            "Logout of tokens for {} successful.",
            user_id.id()
        ))),
        Err(e) => {
            warn!(
                message = "Logout of other tokens failed",
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
pub async fn validate_token(user_access: Authenticated) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Token validation successful",
        principal_id = user_access.principal.id,
    );

    Ok(ApiResponse::message("Token is valid."))
}
