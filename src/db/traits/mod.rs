mod active_tokens;
mod class;
mod is_active;
mod namespace;

use crate::errors::ApiError;
use crate::models::{HubuumClass, Namespace, UserToken};

use super::DbPool;

/// Trait for checking if a structure is valid/active/etc in the database.
///
/// What the different traits imply may vary depending on the structure. For example, a user simply has to
/// exist in the database to be valid, while a token has to be valid and not expired.
pub trait Status<T> {
    /// Check that a structure is active.
    ///
    /// Validity implies that the structure exists in the database and that it is not expired, disabled,
    /// or otherwise inactive.
    async fn is_valid(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting all active tokens for a given structure.
///
/// This trait is used to get all active tokens for a given structure. For example, a user may have multiple
/// active tokens, and this trait would allow us to get all of them.
pub trait ActiveTokens {
    /// Get all active tokens for a given structure.
    async fn tokens(&self, pool: &DbPool) -> Result<Vec<UserToken>, ApiError>;
}

/// Trait for getting the namespace(s) of a structure from the backend database.
///
/// By default, this returns the singular namespace of the structure in question.
/// For relations, where we have two namespaces (one for each class or object),
/// the trait is implemented to return a tuple of the two namespaces.
pub trait GetNamespace<T = Namespace> {
    async fn namespace_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}

/// Trait for getting the classes(s) of a structure from the backend database.
///
/// By default, this returns the singular class of the structure in question.
/// For relations, where we have two classes (one for each structure), the
/// trait is implemented to return a tuple of the two namespaces.
pub trait GetClass<T = HubuumClass> {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<T, ApiError>;
}
