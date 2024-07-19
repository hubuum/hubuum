mod active_tokens;
mod is_active;

use diesel::PgConnection;

use crate::errors::ApiError;
use crate::models::token::UserToken;

/// Trait for checking if a structure is valid/active/etc in the database.
///
/// What the different traits imply may vary depending on the structure. For example, a user simply has to
/// exist in the database to be valid, while a token has to be valid and not expired.
pub trait Status<T> {
    /// Check that a structure is active.
    ///
    /// Validity implies that the structure exists in the database and that it is not expired, disabled,
    /// or otherwise inactive.
    async fn is_valid(&self, conn: &mut PgConnection) -> Result<T, ApiError>;
}

/// Trait for getting all active tokens for a given structure.
///
/// This trait is used to get all active tokens for a given structure. For example, a user may have multiple
/// active tokens, and this trait would allow us to get all of them.
pub trait ActiveTokens {
    /// Get all active tokens for a given structure.
    async fn tokens(&self, conn: &mut PgConnection) -> Result<Vec<UserToken>, ApiError>;
}
