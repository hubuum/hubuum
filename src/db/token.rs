use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};
use tracing::warn;

use crate::errors::ApiError;
use crate::models::UserToken;

pub async fn token_is_valid(
    conn: &mut PgConnection,
    token: &str,
    hours: i32,
) -> Result<UserToken, ApiError> {
    let token_result = diesel::sql_query("SELECT * FROM tokens WHERE token = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
        .bind::<Text, _>(token)
        .bind::<Integer, _>(hours)
        .load::<UserToken>(conn);

    match token_result {
        Ok(token_list) => {
            if let Some(token) = token_list.first() {
                Ok(token.clone())
            } else {
                warn!("Invalid token {}: Not found.", token);
                Err(ApiError::Unauthorized("Invalid token".to_string()))
            }
        }
        Err(e) => {
            warn!("Invalid token {}: {}", token, e);
            Err(ApiError::Unauthorized("Invalid token".to_string()))
        }
    }
}

pub async fn tokens_valid_for_user(
    conn: &mut PgConnection,
    user_id: i32,
    hours: i32,
) -> Result<Vec<UserToken>, ApiError> {
    diesel::sql_query("SELECT * FROM tokens WHERE user_id = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
        .bind::<Integer, _>(user_id)
        .bind::<Integer, _>(hours)
        .load::<UserToken>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
