use actix_web::{get, http::StatusCode, post, web, Responder, ResponseError};
use diesel::prelude::*;

use crate::extractors::BearerToken;
use crate::models::user::LoginUser;
use crate::utilities::response::json_response;

use crate::db::connection::DbPool;

use crate::errors::ApiError;
use serde_json::json;

use tracing::{debug, warn};

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.

#[post("/login")]
pub async fn login(
    pool: web::Data<DbPool>,
    req_input: web::Form<LoginUser>,
) -> Result<impl Responder, ApiError> {
    debug!(message = "Login started", user = req_input.username);

    let user = req_input.into_inner().login(&pool)?;

    let token_generation_result = user.add_token(&pool);

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
pub async fn logout(pool: web::Data<DbPool>, bearer_token: BearerToken) -> impl Responder {
    use crate::schema::tokens::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let token_value = &bearer_token.token;

    debug!(message = "Logging out token {}.", token_value);

    let result = diesel::delete(tokens.filter(token.eq(token_value))).execute(&mut conn);

    match result {
        Ok(_) => json_response(json!({ "message": "Logout successful."}), StatusCode::OK),
        Err(e) => {
            warn!(
                message = "Logout failed",
                token = token_value,
                error = e.to_string()
            );
            ApiError::InternalServerError("Internal authentication failure".to_string())
                .error_response()
        }
    }
}

#[get("/logout_all")]
pub async fn logout_all(pool: web::Data<DbPool>, bearer_token: BearerToken) -> impl Responder {
    use crate::schema::tokens::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let token_value = &bearer_token.token;
    let token_user_id = &bearer_token.user_id;

    debug!(message = "Logging out all tokens for {}.", token_user_id);

    let delete_result = diesel::delete(tokens.filter(user_id.eq(token_user_id))).execute(&mut conn);

    match delete_result {
        Ok(_) => json_response(
            json!({ "message": "Logout of all tokens successful."}),
            StatusCode::OK,
        ),
        Err(e) => {
            warn!(
                message = "Logout failed",
                token = token_value,
                user_id = token_user_id,
                error = e.to_string()
            );
            ApiError::InternalServerError("Internal authentication failure.".to_string())
                .error_response()
        }
    }
}
