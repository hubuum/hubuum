use std::fmt;

use crate::api::openapi::{ApiErrorResponse, MessageResponse};
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::events::{EventContext, RequestProvenance};
use crate::extractors::AccessEventContext;
use crate::extractors::{AdminAccess, Authenticated, ManagementAccess};
use crate::middlewares::rate_limit::{
    LoginAttemptOutcome, begin_login_attempt, client_ip_for_request, finish_login_attempt,
};
use crate::models::auth_session::{SessionLoginRequest, SessionRefreshRequest, SessionResponse};
use crate::models::{REDACTED_DEBUG_VALUE, Token, UserID};
use crate::observability::metrics;
use crate::permissions::AppContext;
use crate::services::auth_sessions;
use crate::storage::{AuthenticationStorage, storage_handle};
use actix_web::http::StatusCode;
use actix_web::{HttpRequest, Responder, get, post, web};
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{SessionClientId, SessionRevocation};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct LogoutTokenRequest {
    pub token: String,
}

impl fmt::Debug for LogoutTokenRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogoutTokenRequest")
            .field("token", &REDACTED_DEBUG_VALUE)
            .finish()
    }
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
    path = "/api/v1/auth/providers",
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
    path = "/api/v1/auth/login",
    tag = "auth",
    request_body = SessionLoginRequest,
    responses(
        (status = 200, description = "Token and authoritative expiry issued", body = SessionResponse, headers(("Cache-Control" = String, description = "no-store"))),
        (status = 400, description = "Password is empty or login fields exceed their supported lengths", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 429, description = "Too many login attempts", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/login")]
pub async fn login(
    context: AppContext,
    req: HttpRequest,
    req_input: web::Json<SessionLoginRequest>,
) -> Result<impl Responder, ApiError> {
    let (client, login) = req_input.into_inner().into_login()?;
    let login = login.validate()?;
    let identity_scope = login.identity_scope().to_owned();
    let name = login.name().to_owned();
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
    let authenticated = match crate::auth::login(&context, login).await {
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

    let user = authenticated.user();
    let principal_id = PrincipalId::new(user.id)?;
    let event_context = RequestProvenance::from_request(&req)
        .map(|provenance| provenance.user_event_context(user.id))
        .unwrap_or_else(|| EventContext::user(principal_id, None, None));
    let token_generation_result = auth_sessions::issue(
        &context,
        &authenticated,
        client,
        event_context,
        crate::services::credential_activity::request_source(&req),
    )
    .await;

    let token = match token_generation_result {
        Ok(token) => token,
        Err(e @ ApiError::Unauthorized(_)) => {
            metrics::login_attempt("bad_credentials");
            return Err(e);
        }
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

    Ok(ApiResponse::new_no_store(token, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/logout",
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
    context: AppContext,
    requestor: Authenticated,
) -> Result<impl Responder, ApiError> {
    debug!(message = "token logout requested");

    let result = requestor.token.delete(&context).await;

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
    path = "/api/v1/auth/logout_all",
    tag = "auth",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "All interactive sessions revoked; durable credentials retained", body = MessageResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[post("/logout_all")]
pub async fn logout_all(
    context: AppContext,
    user_access: ManagementAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Logging out all tokens for {}.",
        user_access.user.id
    );

    let delete_result = storage_handle(&context)
        .revoke_auth_sessions(
            SessionRevocation::All(crate::models::PrincipalID::new(user_access.user.id)?),
            user_access.event_context(&req),
        )
        .await;

    match delete_result {
        Ok(_) => Ok(ApiResponse::message("Logout of all sessions successful.")),
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
    path = "/api/v1/auth/logout/token",
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
    context: AppContext,
    user_access: AdminAccess,
    token: web::Json<LogoutTokenRequest>,
) -> Result<impl Responder, ApiError> {
    let token = Token(token.into_inner().token);
    debug!(message = "administrative token logout requested");

    let result = token.delete(&context).await;

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
    path = "/api/v1/auth/logout/uid/{user_id}",
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
    context: AppContext,
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
        .instance(&context)
        .await?
        .delete_all_tokens(&context)
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
    path = "/api/v1/auth/validate",
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
        principal_id = user_access.principal.id().id(),
    );

    Ok(ApiResponse::message("Token is valid."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logout_token_request_debug_redacts_raw_bearer_token() {
        let raw_token = "raw-logout-bearer-token";
        let debug = format!(
            "{:?}",
            LogoutTokenRequest {
                token: raw_token.to_string(),
            }
        );

        assert!(!debug.contains(raw_token));
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
    }
}

#[utoipa::path(post, path = "/api/v1/auth/refresh", tag = "auth", request_body = SessionRefreshRequest,
    responses((status = 200, description = "Rotated access and refresh credentials; no-store", body = SessionResponse),
    (status = 401, description = "Authentication failed", body = ApiErrorResponse)))]
#[post("/refresh")]
pub async fn refresh(
    context: AppContext,
    req: HttpRequest,
    input: web::Json<SessionRefreshRequest>,
) -> Result<impl Responder, ApiError> {
    let input = input.into_inner();
    let client = SessionClientId::try_new(input.client_id)
        .map_err(|_| crate::models::auth_session::authentication_failed())?;
    let event_context = RequestProvenance::from_request(&req);
    let response = auth_sessions::refresh(
        &context,
        input.refresh_token,
        client,
        event_context,
        crate::services::credential_activity::request_source(&req),
    )
    .await?;
    Ok(ApiResponse::new_no_store(response, StatusCode::OK))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_auth_providers)
        .service(login)
        .service(logout)
        .service(logout_all)
        .service(logout_other)
        .service(logout_token)
        .service(validate_token)
        .service(refresh);
}

pub async fn no_store(
    req: actix_web::dev::ServiceRequest,
    next: actix_web::middleware::Next<impl actix_web::body::MessageBody>,
) -> Result<actix_web::dev::ServiceResponse<impl actix_web::body::MessageBody>, actix_web::Error> {
    let mut response = next.call(req).await?;
    response.headers_mut().insert(
        actix_web::http::header::CACHE_CONTROL,
        actix_web::http::header::HeaderValue::from_static("no-store"),
    );
    Ok(response)
}
