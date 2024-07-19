use crate::db::traits::ActiveTokens;
use crate::errors::ApiError;
use crate::models::{User, UserToken};
use crate::traits::SelfAccessors;
use diesel::prelude::*;
use diesel::sql_types::Integer;

impl<S> ActiveTokens for S
where
    S: SelfAccessors<User>,
{
    async fn tokens(&self, conn: &mut PgConnection) -> Result<Vec<UserToken>, ApiError> {
        active_tokens_by_user_id(self.id(), conn).await
    }
}

async fn active_tokens_by_user_id(
    user_id: i32,
    conn: &mut PgConnection,
) -> Result<Vec<UserToken>, ApiError> {
    let hours = 24; // FIXME: Make this configurable
    diesel::sql_query("SELECT * FROM tokens WHERE user_id = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
        .bind::<Integer, _>(user_id)
        .bind::<Integer, _>(hours)
        .load::<UserToken>(conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()))
}
