use crate::config::{DEFAULT_TOKEN_LIFETIME_HOURS, get_config};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};
use tracing::warn;

use crate::db::traits::Status;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{Token, UserToken};

fn configured_token_lifetime_hours() -> i32 {
    let hours = get_config()
        .map(|config| config.token_lifetime_hours)
        .unwrap_or(DEFAULT_TOKEN_LIFETIME_HOURS);
    hours.clamp(1, i32::MAX as i64) as i32
}

impl Status<UserToken> for Token {
    async fn is_valid(&self, pool: &DbPool) -> Result<UserToken, ApiError> {
        let token = self.storage_hash();
        let token_preview = self.obfuscate();
        let hours = configured_token_lifetime_hours();

        let token_result = with_connection(pool, |conn| {
            diesel::sql_query("SELECT * FROM tokens WHERE token = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
                .bind::<Text, _>(&token)
                .bind::<Integer, _>(hours)
                .load::<UserToken>(conn)
        });

        match token_result {
            Ok(token_list) => {
                if let Some(token) = token_list.first() {
                    Ok(token.clone())
                } else {
                    warn!("Invalid token {}: Not found.", token_preview);
                    Err(ApiError::Unauthorized("Invalid token".to_string()))
                }
            }
            Err(e) => {
                warn!("Invalid token {}: {}", token_preview, e);
                Err(ApiError::Unauthorized("Invalid token".to_string()))
            }
        }
    }
}
