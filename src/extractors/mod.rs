use actix_web::dev::Payload;
use actix_web::FromRequest;
use actix_web::{web::Data, HttpRequest};
use futures_util::future::{ready, Ready};

use crate::db::connection::DbPool;
use crate::db::DatabaseOps;
use crate::errors::ApiError;
use crate::models::user::User;

pub struct BearerToken {
    pub token: String,
    pub user_id: i32,
}

pub struct AdminAccess {
    pub token: String,
    pub user: User,
}

/// A user with a valid token
///
/// This is a user that has a valid token, but is not necessarily an admin. In
/// the user variable, we have the entire user record.
pub struct UserAccess {
    pub token: String,
    pub user: User,
}

fn extract_token(req: &HttpRequest) -> Result<String, ApiError> {
    req.headers()
        .get("Authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|header_str| {
            if header_str.starts_with("Bearer ") {
                Some(header_str[7..].to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| ApiError::Unauthorized("No token provided".to_string()))
}

fn get_db_pool<'a>(req: &'a HttpRequest) -> Result<&'a Data<DbPool>, ApiError> {
    req.app_data::<Data<DbPool>>()
        .ok_or_else(|| ApiError::InternalServerError("Pool not found".to_string()))
}

impl FromRequest for BearerToken {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match get_db_pool(req) {
            Ok(pool) => pool,
            Err(e) => return ready(Err(e)),
        };

        let token_string = match extract_token(req) {
            Ok(token) => token,
            Err(e) => return ready(Err(e)),
        };

        match pool.get_valid_token(&token_string) {
            Ok(bearer_token) => ready(Ok(bearer_token)),
            Err(_) => ready(Err(ApiError::Forbidden("Invalid token".to_string()))),
        }
    }
}

impl FromRequest for UserAccess {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match get_db_pool(req) {
            Ok(pool) => pool,
            Err(e) => return ready(Err(e)),
        };

        let token_string = match extract_token(req) {
            Ok(token) => token,
            Err(e) => return ready(Err(e)),
        };

        match pool.get_valid_token(&token_string) {
            Ok(bearer_token) => {
                use crate::schema::users::dsl::*;
                use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

                let mut conn = pool.get().expect("couldn't get db connection from pool");
                let user = users
                    .filter(id.eq(bearer_token.user_id))
                    .first::<User>(&mut conn);

                match user {
                    Ok(user) => {
                        return ready(Ok(UserAccess {
                            token: token_string,
                            user,
                        }))
                    }
                    Err(_) => return ready(Err(ApiError::Forbidden("Invalid token".to_string()))),
                }
            }
            Err(_) => ready(Err(ApiError::Forbidden("Invalid token".to_string()))),
        }
    }
}

impl FromRequest for AdminAccess {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match get_db_pool(req) {
            Ok(pool) => pool,
            Err(e) => return ready(Err(e)),
        };

        let token_string = match extract_token(req) {
            Ok(token) => token,
            Err(e) => return ready(Err(e)),
        };

        match pool.get_valid_token(&token_string) {
            Ok(bearer_token) => {
                use crate::schema::users::dsl::*;
                use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

                let mut conn = pool.get().expect("couldn't get db connection from pool");
                let user = users
                    .filter(id.eq(bearer_token.user_id))
                    .first::<User>(&mut conn);

                match user {
                    Ok(user) => {
                        if !user.is_admin(pool.get_ref()) {
                            return ready(Err(ApiError::Forbidden(
                                "Permission denied".to_string(),
                            )));
                        }

                        return ready(Ok(AdminAccess {
                            token: token_string,
                            user,
                        }));
                    }
                    Err(_) => return ready(Err(ApiError::Forbidden("Invalid token".to_string()))),
                }
            }
            Err(_) => ready(Err(ApiError::Forbidden("Invalid token".to_string()))),
        }
    }
}
