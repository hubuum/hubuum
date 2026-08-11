use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions};
use crate::models::{
    PrincipalToken, PrincipalTokenMetadata, TokenListState, configured_token_lifetime,
};
use crate::storage::postgres::operations::token::principal_token_metadata_conn;
use crate::storage::postgres::operations::{ActiveTokens, RetainedTokens};
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::traits::PrincipalIdAccessor;
use diesel::pg::Pg;
use diesel::sql_types::{Bool, Nullable};

impl<S> ActiveTokens for S
where
    S: PrincipalIdAccessor,
{
    async fn tokens(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<PrincipalToken>, ApiError> {
        active_tokens_by_principal_id(self.principal_id(), pool).await
    }

    async fn tokens_paginated_with_total_count(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: &QueryOptions,
    ) -> Result<(Vec<PrincipalToken>, i64), ApiError> {
        tokens_by_principal_id_paginated_with_total_count(
            self.principal_id(),
            pool,
            query_options,
            TokenListState::Active,
        )
        .await
    }
}

impl<S> RetainedTokens for S
where
    S: PrincipalIdAccessor,
{
    async fn tokens_paginated_with_total_count_for_state(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: &QueryOptions,
        state: TokenListState,
    ) -> Result<(Vec<PrincipalToken>, i64), ApiError> {
        tokens_by_principal_id_paginated_with_total_count(
            self.principal_id(),
            pool,
            query_options,
            state,
        )
        .await
    }
}

pub(crate) fn active_tokens_cutoff() -> Result<chrono::NaiveDateTime, ApiError> {
    Ok(configured_token_lifetime()?.cutoff_from(chrono::Utc::now().naive_utc())?)
}

/// Boxed Diesel predicate for "token is active": not revoked, and not expired —
/// an explicit `expires_at` in the future, or, for a legacy null expiry, issued
/// within the global lifetime window.
///
/// Single source for the security-critical validity rule so bearer validation
/// and active-token listing can never drift apart.
///
/// Semantics note: an explicit `expires_at` is authoritative and overrides the
/// global `token_lifetime_hours` window — a token with a non-null `expires_at`
/// stays valid until that instant regardless of the global setting, and only
/// legacy `expires_at IS NULL` tokens are bounded by `cutoff`. Lowering
/// `token_lifetime_hours` therefore does not shorten newly issued tokens;
/// revoke them explicitly if that is required.
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
/// `expires_at` in the future, or, for a legacy null expiry, within the global
/// lifetime window from `issued`.
async fn active_tokens_by_principal_id(
    principal: i32,
    pool: &impl crate::storage::StorageContext,
) -> Result<Vec<PrincipalToken>, ApiError> {
    use crate::schema::tokens::dsl::*;
    let active_after = active_tokens_cutoff()?;
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

fn build_tokens_by_principal_query<'a>(
    principal: i32,
    query_options: &'a QueryOptions,
    state: TokenListState,
    now: chrono::NaiveDateTime,
    active_after: chrono::NaiveDateTime,
) -> Result<crate::schema::tokens::BoxedQuery<'a, Pg>, ApiError> {
    use crate::schema::tokens::dsl::{
        expires_at, issued, last_used_at, name as token_name, principal_id as token_principal_id,
        revision, revoked_at, tokens,
    };
    use crate::{date_search, string_search};

    let mut base_query = tokens.into_boxed().filter(token_principal_id.eq(principal));

    base_query = match state {
        TokenListState::Active => base_query.filter(active_token_predicate(now, active_after)),
        TokenListState::Expired => base_query.filter(
            expires_at
                .le(now)
                .or(expires_at.is_null().and(issued.le(active_after))),
        ),
        TokenListState::Revoked => base_query.filter(revoked_at.is_not_null()),
        TokenListState::All => base_query,
    };

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::IssuedAt => date_search!(base_query, param, operator, issued),
            FilterField::ExpiresAt => date_search!(base_query, param, operator, expires_at),
            FilterField::LastUsedAt => date_search!(base_query, param, operator, last_used_at),
            FilterField::Name => string_search!(base_query, param, operator, token_name),
            FilterField::Revision => {
                crate::revision_search!(base_query, param, operator, revision)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for tokens",
                    param.field
                )));
            }
        }
    }

    Ok(base_query)
}

async fn tokens_by_principal_id_paginated_with_total_count(
    principal: i32,
    pool: &impl crate::storage::StorageContext,
    query_options: &QueryOptions,
    state: TokenListState,
) -> Result<(Vec<PrincipalToken>, i64), ApiError> {
    let active_after = active_tokens_cutoff()?;
    let now = chrono::Utc::now().naive_utc();

    let base_query =
        build_tokens_by_principal_query(principal, query_options, state, now, active_after)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            base_query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut base_query =
        build_tokens_by_principal_query(principal, query_options, state, now, active_after)?;
    crate::apply_query_options!(base_query, query_options, PrincipalToken);
    let items = with_connection(pool, async |conn| {
        base_query.load::<PrincipalToken>(conn).await
    })
    .await?;

    Ok((items, total_count))
}

pub(crate) async fn retained_token_metadata_by_principal_id_paginated_with_total_count(
    principal: crate::models::PrincipalID,
    pool: &impl crate::storage::StorageContext,
    query_options: &QueryOptions,
    state: TokenListState,
) -> Result<(Vec<PrincipalTokenMetadata>, i64), ApiError> {
    let active_after = active_tokens_cutoff()?;
    let now = chrono::Utc::now().naive_utc();

    let base_query =
        build_tokens_by_principal_query(principal.id(), query_options, state, now, active_after)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            base_query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut base_query =
        build_tokens_by_principal_query(principal.id(), query_options, state, now, active_after)?;
    crate::apply_query_options!(base_query, query_options, PrincipalToken);
    let metadata = with_transaction(pool, async |conn| -> Result<_, ApiError> {
        let selected = base_query.load::<PrincipalToken>(conn).await?;
        let selected_ids = selected.iter().map(|token| token.id).collect::<Vec<_>>();
        let locked = if selected_ids.is_empty() {
            Vec::new()
        } else {
            crate::schema::tokens::table
                .filter(crate::schema::tokens::id.eq_any(&selected_ids))
                .for_update()
                .load::<PrincipalToken>(conn)
                .await?
        };
        let mut locked_by_id = locked
            .into_iter()
            .map(|token| (token.id, token))
            .collect::<std::collections::HashMap<_, _>>();
        let items = selected_ids
            .into_iter()
            .filter_map(|id| locked_by_id.remove(&id))
            .collect::<Vec<_>>();
        principal_token_metadata_conn(conn, &items).await
    })
    .await?;

    Ok((metadata, total_count))
}
