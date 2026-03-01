use crate::db::DbPool;
use crate::errors::ApiError;

use super::context::BackendContext;

pub trait CanDelete {
    async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;
}

pub trait CanSave {
    type Output;
    async fn save<C>(&self, backend: &C) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;
}

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
