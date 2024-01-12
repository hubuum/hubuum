use actix_web::{get, http::StatusCode, post, web, HttpRequest, Responder};

use crate::extractors::UserAccess;
use crate::models::user::LoginUser;
use crate::utilities::response::json_response;

use crate::db::get_db_pool;
use crate::errors::ApiError;
use serde_json::json;

use tracing::{debug, warn};

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.

#[post("/login")]
pub async fn login(
    req: HttpRequest,
    req_input: web::Json<LoginUser>,
) -> Result<impl Responder, ApiError> {
    let pool = get_db_pool(&req).await?;
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
pub async fn logout(req: HttpRequest, user_access: UserAccess) -> Result<impl Responder, ApiError> {
    let pool = get_db_pool(&req).await?;
    let token = user_access.token;

    debug!(message = "Logging out token.", token = token.obfuscate());

    let mut conn = pool.get()?;
    let result = token.delete(&mut conn).await;

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
    req: HttpRequest,
    user_access: UserAccess,
) -> Result<impl Responder, ApiError> {
    let pool = get_db_pool(&req).await?;
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
