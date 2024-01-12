use actix_web::{dev::Payload, FromRequest, HttpMessage, HttpRequest};
use futures_util::future::{self, FutureExt};
use std::pin::Pin;

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

async fn extract_user_from_token(pool: &DbPool, token: &Token) -> Result<User, ApiError> {
    let mut conn = pool.get()?;
    let user_token = token.is_valid(&mut conn).await?;

    get_user_by_id(&mut conn, user_token.user_id)
        .map_err(|_| ApiError::Unauthorized("Invalid token".to_string()))
}

async fn get_user_and_path(
    path: &actix_web::dev::Path<actix_web::dev::Url>,
    pool: &DbPool,
) -> Result<(User, String), ApiError> {
    let user_id = match path.query("user_id").parse::<i32>() {
        Ok(id) => id,
        Err(_) => {
            return Err(ApiError::InternalServerError(
                "Failed to parse user_id".into(),
            ))
        }
    };

    let path = path.as_str().to_string();

    let mut conn = pool.get()?;
    let user = get_user_by_id(&mut conn, user_id)?;

    Ok((user, path))
}

impl FromRequest for UserAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match req.extensions().get::<DbPool>() {
            Some(pool) => pool.clone(),
            None => {
                return future::ready(Err(ApiError::InternalServerError(
                    "Pool not found".to_string(),
                )))
                .boxed_local()
            }
        };

        let token_result = extract_token(req);

        async move {
            let token = token_result?;
            let user = extract_user_from_token(&pool, &token).await?;

            Ok(UserAccess { token, user })
        }
        .boxed_local()
    }
}

impl FromRequest for AdminAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match req.extensions().get::<DbPool>() {
            Some(pool) => pool.clone(),
            None => {
                return future::ready(Err(ApiError::InternalServerError(
                    "Pool not found for admin request".to_string(),
                )))
                .boxed_local()
            }
        };

        let token_result = extract_token(req);

        async move {
            let token = token_result?;
            let user = extract_user_from_token(&pool, &token).await?;

            if user.is_admin(&pool).await {
                Ok(AdminAccess { token, user })
            } else {
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}

impl FromRequest for AdminOrSelfAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = match req.extensions().get::<DbPool>() {
            Some(pool) => pool.clone(),
            None => {
                return future::ready(Err(ApiError::InternalServerError(
                    "Pool not found".to_string(),
                )))
                .boxed_local()
            }
        };

        let token_result = extract_token(req);

        // Extract necessary information from `req` here
        let path_info = req.match_info().clone();

        async move {
            let token = token_result?;
            let user = extract_user_from_token(&pool, &token).await?;

            // Use the extracted information instead of `req`
            let (user_from_path, path) = get_user_and_path(&path_info, &pool).await?;

            if user.is_admin(&pool).await || user.id == user_from_path.id {
                Ok(AdminOrSelfAccess { token, user })
            } else {
                debug! {
                    message = "User attempted to access an admin-only resource.",
                    user_id = user.id,
                    path = path,
                };
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}
