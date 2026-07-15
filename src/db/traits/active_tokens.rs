use crate::config::token_lifetime_hours_i32;
use crate::db::prelude::*;
use crate::db::traits::ActiveTokens;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::PrincipalToken;
use crate::models::search::{FilterField, QueryOptions};
use crate::traits::PrincipalIdAccessor;
use diesel::pg::Pg;
use diesel::sql_types::{Bool, Nullable};

impl<S> ActiveTokens for S
where
    S: PrincipalIdAccessor,
{
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<PrincipalToken>, ApiError> {
        active_tokens_by_principal_id(self.principal_id(), pool).await
    }

    async fn tokens_paginated_with_total_count(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<PrincipalToken>, i64), ApiError> {
        active_tokens_by_principal_id_paginated_with_total_count(
            self.principal_id(),
            pool,
            query_options,
        )
        .await
    }
}

pub(crate) fn active_tokens_cutoff() -> chrono::NaiveDateTime {
    let hours = token_lifetime_hours_i32() as i64;
    chrono::Utc::now().naive_utc() - chrono::Duration::hours(hours)
}

/// Boxed Diesel predicate for "token is active": not revoked, and not expired —
/// an explicit `expires_at` in the future, or (when null) issued within the
/// global lifetime window from `issued`.
///
/// Single source for the security-critical validity rule so token validation
/// ([`crate::db::traits::Status::is_valid`]) and active-token listing can never
/// drift apart.
///
/// Semantics note: an explicit `expires_at` is authoritative and overrides the
/// global `token_lifetime_hours` window — a token with a non-null `expires_at`
/// stays valid until that instant regardless of the global setting, and only
/// `expires_at IS NULL` tokens are bounded by `cutoff`. Lowering
/// `token_lifetime_hours` therefore does not shorten already-issued
/// explicit-expiry tokens; revoke them explicitly if that is required.
pub(crate) fn active_token_predicate(
    now: chrono::NaiveDateTime,
    cutoff: chrono::NaiveDateTime,
) -> Box<dyn BoxableExpression<crate::schema::tokens::table, Pg, SqlType = Nullable<Bool>>> {
    use crate::schema::tokens::dsl::{expires_at, issued, revoked_at};
    Box::new(
        revoked_at.is_null().and(
            expires_at
                .gt(now)
                .or(expires_at.is_null().and(issued.gt(cutoff))),
        ),
    )
}

/// A token is active when it is not revoked and not expired: an explicit
/// `expires_at` in the future, or (when null) within the global lifetime window
/// from `issued`.
async fn active_tokens_by_principal_id(
    principal: i32,
    pool: &DbPool,
) -> Result<Vec<PrincipalToken>, ApiError> {
    use crate::schema::tokens::dsl::*;
    let active_after = active_tokens_cutoff();
    let now = chrono::Utc::now().naive_utc();

    with_connection(pool, async |conn| {
        tokens
            .filter(principal_id.eq(principal))
            .filter(active_token_predicate(now, active_after))
            .load::<PrincipalToken>(conn)
            .await
    })
    .await
}

async fn active_tokens_by_principal_id_paginated_with_total_count(
    principal: i32,
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<PrincipalToken>, i64), ApiError> {
    use crate::schema::tokens::dsl::{
        expires_at, issued, last_used_at, name as token_name, principal_id as token_principal_id,
        tokens,
    };
    use crate::{date_search, string_search};

    let active_after = active_tokens_cutoff();
    let now = chrono::Utc::now().naive_utc();
    let build_query = || -> Result<_, ApiError> {
        let mut base_query = tokens
            .into_boxed()
            .filter(token_principal_id.eq(principal))
            .filter(active_token_predicate(now, active_after));

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::IssuedAt => date_search!(base_query, param, operator, issued),
                FilterField::ExpiresAt => date_search!(base_query, param, operator, expires_at),
                FilterField::LastUsedAt => date_search!(base_query, param, operator, last_used_at),
                FilterField::Name => string_search!(base_query, param, operator, token_name),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for tokens",
                        param.field
                    )));
                }
            }
        }

        Ok(base_query)
    };

    let base_query = build_query()?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            base_query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut base_query = build_query()?;
    crate::apply_query_options!(base_query, query_options, PrincipalToken);
    let items = with_connection(pool, async |conn| {
        base_query.load::<PrincipalToken>(conn).await
    })
    .await?;

    Ok((items, total_count))
}
