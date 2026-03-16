use crate::api::openapi::{ApiErrorResponse, LoginResponse, MessageResponse};
use crate::config::{AppConfig, login_rate_limit_max_attempts, login_rate_limit_window_seconds};
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::middlewares::client_allowlist::extract_client_ip_from_http_request;
use crate::models::{LoginUser, Token, UserID};
use crate::utilities::response::json_response;
use actix_web::{HttpRequest, Responder, get, http::StatusCode, post, web};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use utoipa::ToSchema;

static LOGIN_ATTEMPTS: LazyLock<Mutex<HashMap<String, VecDeque<Instant>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_LOGIN_ATTEMPT_KEYS: usize = 10_000;

fn login_rate_limit_key(username: &str, client_ip: &str) -> String {
    format!("{}|{}", username.trim().to_ascii_lowercase(), client_ip)
}

fn current_login_window() -> Duration {
    Duration::from_secs(login_rate_limit_window_seconds())
}

fn login_attempts_guard() -> MutexGuard<'static, HashMap<String, VecDeque<Instant>>> {
    match LOGIN_ATTEMPTS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            warn!(message = "Recovering poisoned login limiter state; clearing recorded attempts");
            guard.clear();
            guard
        }
    }
}

fn prune_attempts(attempts: &mut VecDeque<Instant>, now: Instant) {
    let window = current_login_window();
    while let Some(first) = attempts.front() {
        if now.duration_since(*first) > window {
            attempts.pop_front();
        } else {
            break;
        }
    }
}

fn prune_login_attempts_map(
    attempts_by_key: &mut HashMap<String, VecDeque<Instant>>,
    now: Instant,
) {
    attempts_by_key.retain(|_, attempts| {
        prune_attempts(attempts, now);
        !attempts.is_empty()
    });
}

fn evict_expired_or_stalest_login_attempt_key(
    attempts_by_key: &mut HashMap<String, VecDeque<Instant>>,
) {
    // First, try to evict any key whose VecDeque is empty (expired window)
    if let Some(empty_key) = attempts_by_key.iter().find_map(|(key, attempts)| {
        if attempts.is_empty() {
            Some(key.clone())
        } else {
            None
        }
    }) {
        attempts_by_key.remove(&empty_key);
        warn!(
            message = "Evicted expired login limiter entry to enforce key cap",
            max_tracked_keys = MAX_LOGIN_ATTEMPT_KEYS
        );
        return;
    }

    // If no empty keys, evict the stalest (oldest last attempt) key
    let stalest_key = attempts_by_key
        .iter()
        .filter_map(|(key, attempts)| {
            attempts
                .back()
                .copied()
                .map(|last_attempt| (key.clone(), last_attempt))
        })
        .min_by_key(|(_, last_attempt)| *last_attempt)
        .map(|(key, _)| key);

    if let Some(stalest_key) = stalest_key {
        attempts_by_key.remove(&stalest_key);
        warn!(
            message = "Evicted stalest login limiter entry to enforce key cap",
            max_tracked_keys = MAX_LOGIN_ATTEMPT_KEYS
        );
    }
}

fn client_ip_for_request(req: &HttpRequest) -> String {
    let trust_ip_headers = req
        .app_data::<web::Data<AppConfig>>()
        .map(|config| config.trust_ip_headers)
        .unwrap_or(false);

    extract_client_ip_from_http_request(req, trust_ip_headers)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn login_is_rate_limited(username: &str, client_ip: &str) -> bool {
    let key = login_rate_limit_key(username, client_ip);
    let now = Instant::now();
    let mut guard = login_attempts_guard();
    prune_login_attempts_map(&mut guard, now);

    if let Some(attempts) = guard.get_mut(&key) {
        attempts.len() >= login_rate_limit_max_attempts()
    } else {
        false
    }
}

fn record_login_failure(username: &str, client_ip: &str) {
    let key = login_rate_limit_key(username, client_ip);
    let now = Instant::now();
    let mut guard = login_attempts_guard();
    prune_login_attempts_map(&mut guard, now);

    if !guard.contains_key(&key) && guard.len() >= MAX_LOGIN_ATTEMPT_KEYS {
        // After pruning, only evict if still over capacity
        evict_expired_or_stalest_login_attempt_key(&mut guard);
    }

    let attempts = guard.entry(key).or_default();
    attempts.push_back(now);
}

fn clear_login_failures(username: &str, client_ip: &str) {
    let key = login_rate_limit_key(username, client_ip);
    let mut guard = login_attempts_guard();
    guard.remove(&key);
}

#[cfg(test)]
pub fn reset_login_rate_limit_for_tests() {
    login_attempts_guard().clear();
}

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

    if login_is_rate_limited(&username, &client_ip) {
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
                record_login_failure(&username, &client_ip);
            }
            return Err(e);
        }
    };

    clear_login_failures(&username, &client_ip);

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
