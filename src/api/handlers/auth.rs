use actix_web::{get, http::StatusCode, post, web, Responder};
use diesel::prelude::*;

use crate::extractors::BearerToken;
use crate::models::user::LoginUser;
use crate::utilities::response::json_response;

use crate::db::connection::DbPool;

use crate::models::user::User;
use crate::utilities::auth::{generate_token, verify_password};

use serde_json::json;

use tracing::{debug, warn};

#[post("/login")]
pub async fn login(pool: web::Data<DbPool>, req_input: web::Form<LoginUser>) -> impl Responder {
    use crate::schema::tokens::dsl::*;
    use crate::schema::users::dsl::*;

    debug!(message = "Login started", user = req_input.username);

    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let user_result = users
        .filter(username.eq(&req_input.username))
        .first::<User>(&mut conn);

    // Check if user exists
    let db_user_data = match user_result {
        Ok(user_data) => user_data,
        Err(e) => {
            warn!(
                message = "Login failed (user not found)",
                user = req_input.username,
                error = e.to_string()
            );
            return json_response(
                json!({ "message": "Authentication failure"}),
                StatusCode::UNAUTHORIZED,
            );
        }
    };

    // We have a user, now check the password
    let password_check = verify_password(&req_input.password, &db_user_data.password);

    if password_check.is_err() {
        warn!(
            message = "Login failed (password mismatch)",
            user = req_input.username,
            hash = db_user_data.password,
            error = password_check.err().unwrap().to_string()
        );
        return json_response(
            json!({ "message": "Authentication failure"}),
            StatusCode::UNAUTHORIZED,
        );
    }

    let generated_token = generate_token();

    // set expiration to 1 day
    let expire_when = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::days(1))
        .expect("valid timestamp")
        .naive_utc();

    let token_insert_result = diesel::insert_into(crate::schema::tokens::table)
        .values((
            user_id.eq(&db_user_data.id),
            token.eq(&generated_token),
            expires.eq(&expire_when),
        ))
        .execute(&mut conn);

    if token_insert_result.is_err() {
        return json_response(
            json!({ "message": "Internal authentication failure, please try again later."}),
            StatusCode::UNAUTHORIZED,
        );
    }

    debug!(
        message = "Login successful",
        user = req_input.username,
        token = generated_token
    );

    return json_response(json!({"token": generated_token}), StatusCode::OK);
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
        Ok(_) => {
            return json_response(json!({ "message": "Logout successful."}), StatusCode::OK);
        }
        Err(e) => {
            warn!(
                message = "Logout failed",
                token = token_value,
                error = e.to_string()
            );
            return json_response(
                json!({ "message": "Internal authentication failure, please try again later."}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
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
        Ok(_) => {
            return json_response(
                json!({ "message": "Logout of all tokens successful."}),
                StatusCode::OK,
            );
        }
        Err(e) => {
            warn!(
                message = "Logout failed",
                token = token_value,
                user_id = token_user_id,
                error = e.to_string()
            );
            return json_response(
                json!({ "message": "Internal authentication failure, please try again later."}),
                StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }
}
