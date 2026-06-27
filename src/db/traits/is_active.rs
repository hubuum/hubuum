use diesel::prelude::*;
use tracing::warn;

use crate::db::traits::Status;
use crate::db::traits::active_tokens::{active_token_predicate, active_tokens_cutoff};
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{PrincipalToken, Token};

/// Advance `last_used_at` at most this often. Routine authenticated requests
/// then stay read-only on the hot auth path instead of writing the token row on
/// every call; the field is telemetry, so coarse granularity is fine.
const LAST_USED_AT_THROTTLE_SECS: i64 = 60;

impl Status<PrincipalToken> for Token {
    /// Validate a bearer token: a single SELECT returns the row only if it is
    /// simultaneously unrevoked, unexpired, and not owned by a disabled service
    /// account (the validity predicate is shared with active-token listing via
    /// [`active_token_predicate`]).
    ///
    /// `last_used_at` is advanced best-effort and only when stale (see
    /// [`LAST_USED_AT_THROTTLE_SECS`]); the telemetry write is intentionally
    /// decoupled from the validity check so the common case touches no rows.
    async fn is_valid(&self, pool: &DbPool) -> Result<PrincipalToken, ApiError> {
        use crate::schema::service_accounts;
        use crate::schema::tokens::dsl::{
            id as token_id, last_used_at, principal_id, token, tokens,
        };

        let token_hash = self.storage_hash();
        let token_preview = self.obfuscate();
        let now = chrono::Utc::now().naive_utc();
        let cutoff = active_tokens_cutoff();

        let result = with_connection(pool, |conn| {
            tokens
                .filter(token.eq(&token_hash))
                .filter(active_token_predicate(now, cutoff))
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    service_accounts::table
                        .filter(service_accounts::id.eq(principal_id))
                        .filter(service_accounts::disabled_at.is_not_null()),
                )))
                .first::<PrincipalToken>(conn)
                .optional()
        });

        let mut valid_token = match result {
            Ok(Some(valid_token)) => valid_token,
            Ok(None) => {
                warn!(
                    "Invalid token {}: not found, revoked, expired, or disabled.",
                    token_preview
                );
                return Err(ApiError::Unauthorized("Invalid token".to_string()));
            }
            Err(e) => {
                warn!("Invalid token {}: {}", token_preview, e);
                return Err(ApiError::Unauthorized("Invalid token".to_string()));
            }
        };

        let throttle = chrono::Duration::seconds(LAST_USED_AT_THROTTLE_SECS);
        let last_used_is_stale = valid_token
            .last_used_at
            .map(|previous| now - previous >= throttle)
            .unwrap_or(true);
        if last_used_is_stale {
            // Best-effort telemetry: a failure to advance `last_used_at` must
            // never fail an otherwise-valid request.
            let token_id_value = valid_token.id;
            let updated = with_connection(pool, |conn| {
                diesel::update(tokens.filter(token_id.eq(token_id_value)))
                    .set(last_used_at.eq(now))
                    .execute(conn)
            });
            // Reflect the advance in the returned row (the SELECT above read the
            // prior value), so callers observe an accurate `last_used_at`.
            if updated.is_ok() {
                valid_token.last_used_at = Some(now);
            }
        }

        Ok(valid_token)
    }
}
