use crate::db::traits::ActiveTokens;
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions};
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

    async fn tokens_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<UserToken>, ApiError> {
        active_tokens_by_user_id_paginated(self.id(), pool, query_options).await
    }
}

#[allow(dead_code)]
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

async fn active_tokens_by_user_id_paginated(
    user_id: i32,
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<Vec<UserToken>, ApiError> {
    use crate::schema::tokens::dsl::{issued, token, tokens, user_id as token_user_id};
    use crate::{date_search, string_search};

    let hours = 24; // FIXME: Make this configurable
    let mut base_query = tokens
        .into_boxed()
        .filter(token_user_id.eq(user_id))
        .filter(issued.gt(chrono::Utc::now().naive_utc() - chrono::Duration::hours(hours)));

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::IssuedAt => date_search!(base_query, param, operator, issued),
            FilterField::Name => string_search!(base_query, param, operator, token),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for user tokens",
                    param.field
                )))
            }
        }
    }

    crate::apply_query_options!(base_query, query_options, UserToken);

    with_connection(pool, |conn| base_query.load::<UserToken>(conn))
}
