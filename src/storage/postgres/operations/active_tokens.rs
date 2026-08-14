//! Transitional raw-token test fixture queries.
//!
//! Application token management uses the backend-neutral token contract. This
//! helper remains only for integration tests that assert stored HMAC state.

use crate::errors::ApiError;
use crate::models::{PrincipalToken, configured_token_lifetime};
use crate::storage::postgres::operations::ActiveTokens;
use crate::storage::postgres::operations::token::PrincipalTokenRow;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::with_connection;
use crate::traits::PrincipalIdAccessor;
use hubuum_storage_postgres::operations::authentication::active_token_predicate;

impl<S> ActiveTokens for S
where
    S: PrincipalIdAccessor,
{
    async fn tokens(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<PrincipalToken>, ApiError> {
        let legacy_valid_after =
            configured_token_lifetime()?.cutoff_from(chrono::Utc::now().naive_utc())?;
        let observed_at = chrono::Utc::now().naive_utc();
        let principal_id = self.principal_id();
        with_connection(pool, async move |connection| {
            crate::schema::tokens::table
                .filter(crate::schema::tokens::principal_id.eq(principal_id))
                .filter(active_token_predicate(observed_at, legacy_valid_after))
                .load::<PrincipalTokenRow>(connection)
                .await
        })
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
