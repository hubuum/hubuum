use chrono::SubsecRound;
use tracing::{Instrument, field, info_span};

use crate::errors::ApiError;
use crate::models::{Token, configured_token_lifetime};
use crate::storage::{
    AuthenticationStorage, StorageAuthenticatedToken, StorageAuthenticationAttempt, StorageContext,
    StorageTokenFormat, StorageTokenMigrationOutcome, storage_handle,
};

/// Validate one presented bearer token through the selected complete storage
/// backend.
///
/// Raw bearer material stays in the application authentication layer. The
/// storage contract receives only a redacted opaque lookup value and returns a
/// hash-free backend-neutral authentication projection.
pub async fn authenticate_bearer_token(
    context: &impl StorageContext,
    token: &Token,
) -> Result<StorageAuthenticatedToken, ApiError> {
    let plan = match token.credentials() {
        Ok(plan) => plan,
        Err(error) => {
            crate::observability::metrics::token_authentication(
                if token.0.starts_with("hbt") {
                    "versioned_unknown"
                } else {
                    "legacy"
                },
                "unknown",
                "rejected",
            );
            return Err(error);
        }
    };
    let format = match plan.format {
        StorageTokenFormat::Legacy => "legacy",
        StorageTokenFormat::Version1 => "version1",
    };
    let key_state = plan.key_state;
    let observed_at = chrono::Utc::now().naive_utc().trunc_subsecs(6);
    let legacy_valid_after = configured_token_lifetime()?.cutoff_from(observed_at)?;
    let attempt = StorageAuthenticationAttempt::try_candidates(
        plan.credentials,
        plan.migration_target,
        observed_at.and_utc(),
        legacy_valid_after.and_utc(),
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let span = info_span!(
        "auth.token_validation",
        auth.token.format = format,
        auth.token.key_state = key_state,
        auth.result = field::Empty,
    );
    match storage_handle(context)
        .authenticate_bearer_token(attempt)
        .instrument(span.clone())
        .await
    {
        Ok(authenticated) => {
            let outcome = match authenticated.migration_outcome() {
                StorageTokenMigrationOutcome::NotNeeded => "success",
                StorageTokenMigrationOutcome::Migrated => "migrated",
                StorageTokenMigrationOutcome::Conflict => "migration_conflict",
            };
            span.record("auth.result", outcome);
            crate::observability::metrics::token_authentication(format, key_state, outcome);
            Ok(authenticated)
        }
        Err(error) => {
            span.record("auth.result", "rejected");
            crate::observability::metrics::token_authentication(format, key_state, "rejected");
            Err(error.into())
        }
    }
}
