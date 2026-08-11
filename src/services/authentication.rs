use crate::errors::ApiError;
use crate::models::Token;
use crate::storage::{
    AuthenticatedToken, AuthenticationCredential, AuthenticationStorage, StorageContext,
    storage_handle,
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
    Ok(storage_handle(context)
        .authenticate_bearer_token(credential)
        .await?)
}
