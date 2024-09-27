use crate::db::traits::ActiveTokens;
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{User, UserToken};
use crate::traits::SelfAccessors;
use diesel::prelude::*;
use diesel::sql_types::Integer;

impl<S> ActiveTokens for S
where
    S: SelfAccessors<User>,
{
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError> {
        active_tokens_by_user_id(self.id(), pool).await
    }
}

async fn active_tokens_by_user_id(user_id: i32, pool: &DbPool) -> Result<Vec<UserToken>, ApiError> {
    let hours = 24; // FIXME: Make this configurable

    with_connection(pool, |conn| {
        Ok(diesel::sql_query("SELECT * FROM tokens WHERE user_id = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
            .bind::<Integer, _>(user_id)
            .bind::<Integer, _>(hours)
            .load::<UserToken>(conn)
            .map_err(|e| ApiError::DatabaseError(e.to_string())))
    })?
}
