use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};
use tracing::warn;

use crate::db::traits::Status;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{Token, UserToken};

impl Status<UserToken> for Token {
    async fn is_valid(&self, pool: &DbPool) -> Result<UserToken, ApiError> {
        let token = self.get_token();
        let hours = 24; // FIXME: Make this configurable

        let token_result = with_connection(pool, |conn| {
            Ok(diesel::sql_query("SELECT * FROM tokens WHERE token = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
                .bind::<Text, _>(&token)
                .bind::<Integer, _>(hours)
                .load::<UserToken>(conn))
        })?;

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
}
