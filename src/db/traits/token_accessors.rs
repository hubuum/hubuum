use crate::db::traits::TokenAccessors;
use crate::errors::ApiError;
use crate::models::{Token, User, UserToken};
use crate::traits::SelfAccessors;
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};
use tracing::warn;

impl TokenAccessors for Token {
    async fn is_valid(&self, conn: &mut PgConnection) -> Result<UserToken, ApiError> {
        let token = self.get_token();
        let hours = 24;
        let token_result = diesel::sql_query("SELECT * FROM tokens WHERE token = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
            .bind::<Text, _>(&token)
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

    async fn tokens(&self, conn: &mut PgConnection) -> Result<Vec<UserToken>, ApiError> {
        // For a single token, we might want to return a vector with just this token if it's valid
        self.is_valid(conn).await.map(|token| vec![token])
    }
}

impl<S> TokenAccessors for S
where
    S: SelfAccessors<User>,
{
    async fn is_valid(&self, conn: &mut PgConnection) -> Result<UserToken, ApiError> {
        // For types implementing SelfAccessors<User>, we might want to check if the user is valid
        // and then return the most recent token, if any
        let tokens = self.tokens(conn).await?;
        tokens
            .first()
            .cloned()
            .ok_or_else(|| ApiError::Unauthorized("No valid token found".to_string()))
    }

    async fn tokens(&self, conn: &mut PgConnection) -> Result<Vec<UserToken>, ApiError> {
        active_tokens_by_user_id(self.id(), conn).await
    }
}

async fn active_tokens_by_user_id(
    user_id: i32,
    conn: &mut PgConnection,
) -> Result<Vec<UserToken>, ApiError> {
    let hours = 24;
    diesel::sql_query("SELECT * FROM tokens WHERE user_id = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
        .bind::<Integer, _>(user_id)
        .bind::<Integer, _>(hours)
        .load::<UserToken>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
