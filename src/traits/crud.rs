use crate::db::DbPool;
use crate::errors::ApiError;

use super::context::BackendContext;

/// Delete the value represented by `self`.
///
/// This is the public model-facing delete API. The actual backend-specific work is delegated to
/// hidden adapter traits so implementations can stay thin.
pub trait CanDelete {
    async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;
}

/// Persist `self` and return the saved representation.
///
/// `Output` is usually the persisted model type. For example, saving a `NewNamespace` returns a
/// `Namespace`, while saving an existing value may also return the updated persisted value.
pub trait CanSave {
    type Output;
    async fn save<C>(&self, backend: &C) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;
}

/// Update an existing persisted value and return the updated representation.
///
/// `entry_id` identifies the stored record that should be updated. `Output` is the persisted type
/// returned after the update completes.
pub trait CanUpdate {
    type Output;
    async fn update<C>(&self, backend: &C, entry_id: i32) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;
}

#[doc(hidden)]
pub trait DeleteAdapter {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl<T> CanDelete for T
where
    T: DeleteAdapter,
{
    async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_adapter(backend.db_pool()).await
    }
}

#[doc(hidden)]
pub trait SaveAdapter {
    type Output;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError>;
}

impl<T> CanSave for T
where
    T: SaveAdapter,
{
    type Output = T::Output;

    async fn save<C>(&self, backend: &C) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_adapter(backend.db_pool()).await
    }
}

#[doc(hidden)]
pub trait UpdateAdapter {
    type Output;

    async fn update_adapter(&self, pool: &DbPool, entry_id: i32) -> Result<Self::Output, ApiError>;
}

impl<T> CanUpdate for T
where
    T: UpdateAdapter,
{
    type Output = T::Output;

    async fn update<C>(&self, backend: &C, entry_id: i32) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_adapter(backend.db_pool(), entry_id).await
    }
}

#[allow(dead_code)]
/// Validate a value in its full domain context.
///
/// Unlike purely local validation, implementations may consult the backend when validation depends
/// on related persisted state, such as a class schema or permissions.
pub trait Validate {
    /// Complete validation of the object.
    ///
    /// This returns errors if:
    /// - If the object's class requires validation (validate_schema), and the object's data
    ///   fails validation against the class's JSON schema (json_schema).
    async fn validate<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;
}

#[allow(dead_code)]
/// Validate a value against a supplied schema without loading additional backend state.
pub trait ValidateAgainstSchema {
    /// Validate the object's data against the class's JSON schema.
    ///
    /// This does not check if the class requires validation (validate_schema), it
    /// only checks if the data is valid against the schema.
    ///
    /// Returns OK() if any of the following are true:
    /// - The class does not have a schema (json_schema).
    /// - The object data is valid against the schema.
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError>;
}
