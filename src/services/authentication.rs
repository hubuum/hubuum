use chrono::SubsecRound;

use crate::errors::ApiError;
use crate::models::{Token, configured_token_lifetime};
use crate::storage::{
    AuthenticatedToken, AuthenticationAttempt, AuthenticationCredential, AuthenticationStorage,
    StorageContext, storage_handle,
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
) -> Result<AuthenticatedToken, ApiError> {
    let credential = AuthenticationCredential::new(token.storage_hash());
    let observed_at = chrono::Utc::now().naive_utc().trunc_subsecs(6);
    let legacy_valid_after = configured_token_lifetime()?.cutoff_from(observed_at)?;
    let attempt = AuthenticationAttempt::new(
        credential,
        observed_at.and_utc(),
        legacy_valid_after.and_utc(),
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    Ok(storage_handle(context)
        .authenticate_bearer_token(attempt)
        .await?)
}
