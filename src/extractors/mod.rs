use actix_web::dev::Payload;
use actix_web::FromRequest;
use actix_web::{web::Data, Error, HttpRequest};
use futures_util::future::{ready, Ready};

use crate::db::connection::DbPool;

use crate::utilities::auth::validate_token;

// Step 1: Custom Extractor
pub struct BearerToken(pub String);

impl FromRequest for BearerToken {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match req.app_data::<Data<DbPool>>() {
            Some(pool) => pool,
            None => {
                return ready(Err(actix_web::error::ErrorInternalServerError(
                    "Pool not found",
                )))
            }
        };

        if let Some(auth_header) = req.headers().get("Authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                if auth_str.starts_with("Bearer ") {
                    let token_string = auth_str[7..].to_string();

                    if validate_token(&token_string, &pool) {
                        return ready(Ok(BearerToken(token_string)));
                    }

                    return ready(Err(actix_web::error::ErrorUnauthorized("Unauthorized")));
                }
            }
        }
        ready(Err(actix_web::error::ErrorUnauthorized("Unauthorized")))
    }
}
