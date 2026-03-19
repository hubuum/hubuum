use crate::config::token_lifetime_hours_i32;
use crate::db::traits::ActiveTokens;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SearchOperator};
use crate::models::{Token, User, UserToken};
use crate::traits::SelfAccessors;
use diesel::prelude::*;
use diesel::sql_types::Integer;

fn hash_token_name_filters(query_options: &QueryOptions) -> Result<QueryOptions, ApiError> {
    let mut prepared = query_options.clone();
    for filter in &mut prepared.filters {
        if filter.field == FilterField::Name {
            match &filter.operator {
                SearchOperator::Equals { .. } => {
                    filter.value = Token::storage_hash_from_raw(&filter.value);
                }
                op => {
                    return Err(ApiError::BadRequest(format!(
                        "Token name only supports equality operators, got '{op}'"
                    )));
                }
            }
        }
    }
    Ok(prepared)
}

impl<S> ActiveTokens for S
where
    S: SelfAccessors<User>,
{
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError> {
        active_tokens_by_user_id(self.id(), pool).await
    }

    async fn tokens_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<UserToken>, i64), ApiError> {
        active_tokens_by_user_id_paginated_with_total_count(self.id(), pool, query_options).await
    }
}

#[allow(dead_code)]
async fn active_tokens_by_user_id(user_id: i32, pool: &DbPool) -> Result<Vec<UserToken>, ApiError> {
    let hours = token_lifetime_hours_i32();

    with_connection(pool, |conn| {
        diesel::sql_query("SELECT * FROM tokens WHERE user_id = $1 AND issued > (CURRENT_TIMESTAMP - ($2 || ' hours')::INTERVAL)")
            .bind::<Integer, _>(user_id)
                .bind::<Integer, _>(hours)
            .load::<UserToken>(conn)
            .map_err(|e| ApiError::DatabaseError(e.to_string()))
    })
}

fn active_tokens_cutoff() -> chrono::NaiveDateTime {
    let hours = token_lifetime_hours_i32() as i64;
    chrono::Utc::now().naive_utc() - chrono::Duration::hours(hours)
}

async fn active_tokens_by_user_id_paginated_with_total_count(
    user_id: i32,
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<UserToken>, i64), ApiError> {
    use crate::schema::tokens::dsl::{issued, token, tokens, user_id as token_user_id};
    use crate::{date_search, string_search};

    let prepared_query_options = hash_token_name_filters(query_options)?;
    let active_after = active_tokens_cutoff();
    let build_query = || -> Result<_, ApiError> {
        let mut base_query = tokens
            .into_boxed()
            .filter(token_user_id.eq(user_id))
            .filter(issued.gt(active_after));

        for param in &prepared_query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::IssuedAt => date_search!(base_query, param, operator, issued),
                FilterField::Name => string_search!(base_query, param, operator, token),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for user tokens",
                        param.field
                    )));
                }
            }
        }

        Ok(base_query)
    };

    let base_query = build_query()?;
    let total_count = with_connection(pool, |conn| base_query.count().get_result::<i64>(conn))?;

    let mut base_query = build_query()?;
    crate::apply_query_options!(base_query, &prepared_query_options, UserToken);
    let items = with_connection(pool, |conn| base_query.load::<UserToken>(conn))?;

    Ok((items, total_count))
}
