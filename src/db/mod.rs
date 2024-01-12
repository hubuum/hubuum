pub mod connection;

use crate::{db::connection::DbPool, errors::ApiError};
use actix_web::{HttpMessage, HttpRequest};

pub async fn get_db_pool(req: &HttpRequest) -> Result<DbPool, ApiError> {
    match req.extensions().get::<DbPool>() {
        Some(pool) => Ok(pool.clone()),
        None => Err(ApiError::InternalServerError(
            "Failed to get database pool".into(),
        )),
    }
}
