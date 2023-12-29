use actix_web::dev::Payload;
use actix_web::FromRequest;
use actix_web::{web::Data, HttpRequest};
use futures_util::future::{ready, Ready};

use crate::db::connection::DbPool;

use crate::utilities::auth::validate_token;

use crate::errors::ApiError;

pub struct BearerToken {
    pub token: String,
    pub user_id: i32,
}

impl FromRequest for BearerToken {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match req.app_data::<Data<DbPool>>() {
            Some(pool) => pool,
            None => {
                return ready(Err(ApiError::InternalServerError(
                    "Pool not found".to_string(),
                )))
            }
        };

        if let Some(auth_header) = req.headers().get("Authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                if auth_str.starts_with("Bearer ") {
                    let token_string = auth_str[7..].to_string();

                    match validate_token(&token_string, &pool) {
                        Ok(Some(bearer_token)) => return ready(Ok(bearer_token)),
                        Ok(None) | Err(_) => {
                            return ready(Err(ApiError::Forbidden("Invalid token".to_string())))
                        }
                    }
                }
            }
        }
        ready(Err(ApiError::Unauthorized("No token provided".to_string())))
    }
}
