use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::{LoginUser, Token, UserID};
use crate::utilities::response::json_response;
use actix_web::{get, http::StatusCode, post, web, Responder};
use serde_json::json;
use tracing::{debug, warn};

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.

#[post("/login")]
pub async fn login(
    pool: web::Data<DbPool>,
    req_input: web::Json<LoginUser>,
) -> Result<impl Responder, ApiError> {
    debug!(message = "Login started", user = req_input.username);
    let user = req_input.into_inner().login(&pool).await?;

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

#[get("/logout")]
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

#[get("/logout_all")]
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

#[get("/logout/token/{token}")]
pub async fn logout_token(
    pool: web::Data<DbPool>,
    user_access: AdminAccess,
    token: web::Path<Token>,
) -> Result<impl Responder, ApiError> {
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
                token_target = user_access.token.get_token(),
                user_id = user_access.user.id,
                error = e.to_string()
            );
            Err(ApiError::InternalServerError(
                "Internal authentication failure.".to_string(),
            ))
        }
    }
}

#[get("/logout/uid/{user_id}")]
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
