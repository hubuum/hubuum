use actix_web::dev::Payload;
use actix_web::FromRequest;
use actix_web::{web::Data, HttpRequest};
use futures_util::future::{ready, Ready};

use crate::db::connection::DbPool;
use crate::errors::ApiError;
use crate::models::token::Token;
use crate::models::user::User;
use crate::utilities::iam::get_user_by_id;

use tracing::debug;

pub struct AdminAccess {
    pub token: Token,
    pub user: User,
}

pub struct AdminOrSelfAccess {
    pub token: Token,
    pub user: User,
}

/// A user with a valid token
///
/// This is a user that has a valid token, but is not necessarily an admin. In
/// the user variable, we have the entire user record.
pub struct UserAccess {
    pub token: Token,
    pub user: User,
}

fn extract_token(req: &HttpRequest) -> Result<Token, ApiError> {
    req.headers()
        .get("Authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|header_str| {
            header_str
                .strip_prefix("Bearer ")
                .map(|header_str| header_str.to_string())
        })
        .map(Token)
        .ok_or_else(|| ApiError::Unauthorized("No token provided".to_string()))
}

fn get_db_pool(req: &HttpRequest) -> Result<&Data<DbPool>, ApiError> {
    req.app_data::<Data<DbPool>>()
        .ok_or_else(|| ApiError::InternalServerError("Pool not found".to_string()))
}

fn extract_user_from_token(pool: &Data<DbPool>, token: &Token) -> Result<User, ApiError> {
    let mut conn = pool.get()?;
    let user_token = token.is_valid(&mut conn)?;

    get_user_by_id(&mut conn, user_token.user_id)
        .map_err(|_| ApiError::Unauthorized("Invalid token".to_string()))
}

fn get_user_and_path(req: &HttpRequest, pool: &Data<DbPool>) -> Result<(User, String), ApiError> {
    let user_id = match req.match_info().query("user_id").parse::<i32>() {
        Ok(id) => id,
        Err(_) => {
            return Err(ApiError::InternalServerError(
                "Failed to parse user_id".into(),
            ))
        }
    };

    let path = req.path().to_string();

    let mut conn = pool.get()?;
    let user = get_user_by_id(&mut conn, user_id)?;

    Ok((user, path))
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
            Ok(token) => match extract_user_from_token(pool, &token) {
                Ok(user) => ready(Ok(UserAccess { token, user })),
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
            Ok(token) => match extract_user_from_token(pool, &token) {
                Ok(user) if user.is_admin(pool) => ready(Ok(AdminAccess { token, user })),
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
            Ok(token) => match extract_user_from_token(pool, &token) {
                Ok(user) => {
                    let (user_from_path, path) = match get_user_and_path(req, pool) {
                        Ok((user_from_path, path)) => (user_from_path, path),
                        Err(e) => return ready(Err(e)),
                    };

                    if user.is_admin(pool) || user.id == user_from_path.id {
                        ready(Ok(AdminOrSelfAccess { token, user }))
                    } else {
                        debug! {
                            message = "User attempted to access an admin-only resource.",
                            user_id = user.id,
                            path = path,
                        };
                        ready(Err(ApiError::Forbidden("Permission denied".to_string())))
                    }
                }
                Err(e) => ready(Err(e)),
            },
            Err(e) => ready(Err(e)),
        }
    }
}
