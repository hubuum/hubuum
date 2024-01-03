use actix_web::HttpResponse;
use actix_web::{get, http::StatusCode, post, web, Responder, ResponseError};
use diesel::prelude::*;

use crate::extractors::BearerToken;
use crate::models::user::LoginUser;
use crate::utilities::response::json_response;

use crate::db::connection::DbPool;

use crate::errors::ApiError;
use crate::utilities::auth::verify_password;
use crate::utilities::iam::{add_token_for_user, get_user_by_username};
use serde_json::json;

use tracing::{debug, warn};

// During auth, no matter what the error is, we return a 401 Unauthorized
// with a generic message. This is to prevent leaking information about
// the existence of internal data.
pub fn auth_failure() -> HttpResponse {
    ApiError::Unauthorized("Authentication failure".to_string()).error_response()
}

#[post("/login")]
pub async fn login(pool: web::Data<DbPool>, req_input: web::Form<LoginUser>) -> impl Responder {
    debug!(message = "Login started", user = req_input.username);

    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let user_result = get_user_by_username(&mut conn, &req_input.username);

    // Check if user exists
    let db_user_data = match user_result {
        Ok(user_data) => user_data,
        Err(e) => {
            warn!(
                message = "Login failed (user not found)",
                user = req_input.username,
                error = e.to_string()
            );
            return auth_failure();
        }
    };

    // We have a user, now check the password
    let password_check = verify_password(&req_input.password, &db_user_data.password);

    match password_check {
        Ok(status) => {
            if !status {
                warn!(
                    message = "Login failed (password mismatch)",
                    user = req_input.username,
                    hash = db_user_data.password
                );
                return auth_failure();
            }
        }
        Err(e) => {
            warn!(
                message = "Login failed (password validation failure)",
                user = req_input.username,
                error = e.to_string()
            );
            return auth_failure();
        }
    }

    let token_generation_result = add_token_for_user(&mut conn, db_user_data.id);

    let token_value = match token_generation_result {
        Ok(token_value) => token_value,
        Err(e) => {
            warn!(
                message = "Login failed (token generation failed)",
                user = req_input.username,
                error = e.to_string()
            );
            return auth_failure();
        }
    };

    debug!(
        message = "Login successful",
        user = req_input.username,
        token = token_value
    );

    json_response(json!({"token": token_value}), StatusCode::OK)
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
