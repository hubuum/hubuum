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

pub struct AdminOrSelfAccess {
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

fn extract_user_from_token(pool: &Data<DbPool>, token_string: &str) -> Result<User, ApiError> {
    let bearer_token = pool
        .get_valid_token(token_string)
        .map_err(|_| ApiError::Forbidden("Invalid token".to_string()))?;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    use crate::schema::users::dsl::*;
    use diesel::prelude::*;

    users
        .filter(id.eq(bearer_token.user_id))
        .first::<User>(&mut conn)
        .map_err(|_| ApiError::Forbidden("Invalid token".to_string()))
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

        match extract_token(req) {
            Ok(token_string) => match extract_user_from_token(&pool, &token_string) {
                Ok(user) => ready(Ok(UserAccess {
                    token: token_string,
                    user,
                })),
                Err(e) => ready(Err(e)),
            },
            Err(e) => ready(Err(e)),
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

        match extract_token(req) {
            Ok(token_string) => match extract_user_from_token(&pool, &token_string) {
                Ok(user) if user.is_admin(&pool) => ready(Ok(AdminAccess {
                    token: token_string,
                    user,
                })),
                Ok(_) => ready(Err(ApiError::Forbidden("Permission denied".to_string()))),
                Err(e) => ready(Err(e)),
            },
            Err(e) => ready(Err(e)),
        }
    }
}

impl FromRequest for AdminOrSelfAccess {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match get_db_pool(req) {
            Ok(pool) => pool,
            Err(e) => return ready(Err(e)),
        };

        match extract_token(req) {
            Ok(token_string) => match extract_user_from_token(&pool, &token_string) {
                Ok(user) => {
                    let path_user_id: i32 = req.match_info().query("user_id").parse().unwrap_or(-1);
                    if user.is_admin(&pool) || user.id == path_user_id {
                        ready(Ok(AdminOrSelfAccess {
                            token: token_string,
                            user,
                        }))
                    } else {
                        ready(Err(ApiError::Forbidden("Permission denied".to_string())))
                    }
                }
                Err(e) => ready(Err(e)),
            },
            Err(e) => ready(Err(e)),
        }
    }
}
