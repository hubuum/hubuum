use crate::db::DbPool;
use crate::errors::ApiError;
use crate::events::EventContext;

use super::context::BackendContext;

/// Delete the value represented by `self`.
///
/// This is the public model-facing delete API. The actual backend-specific work is delegated to
/// hidden adapter traits so implementations can stay thin.
pub trait CanDelete {
    /// Delete without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture cleanup, and event-system tests. Normal application code should
    /// use [`CanDelete::delete`] so event subscribers observe the change.
    #[cfg_attr(not(test), allow(dead_code))]
    async fn delete_without_events<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized;
}

/// Persist `self` and return the saved representation.
///
/// `Output` is usually the persisted model type. For example, saving a `NewNamespace` returns a
/// `Namespace`, while saving an existing value may also return the updated persisted value.
pub trait CanSave {
    type Output;
    /// Persist without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`CanSave::save`] so event subscribers observe the change.
    #[cfg_attr(not(test), allow(dead_code))]
    async fn save_without_events<C>(&self, backend: &C) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;

    async fn save<C>(&self, backend: &C, context: &EventContext) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;
}

/// Update an existing persisted value and return the updated representation.
///
/// `entry_id` identifies the stored record that should be updated. `Output` is the persisted type
/// returned after the update completes.
pub trait CanUpdate {
    type Output;
    /// Update without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`CanUpdate::update`] so event subscribers observe the change.
    async fn update_without_events<C>(
        &self,
        backend: &C,
        entry_id: i32,
    ) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;

    async fn update<C>(
        &self,
        backend: &C,
        entry_id: i32,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized;
}

#[doc(hidden)]
pub trait DeleteAdapter {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError>;

    async fn delete_adapter(&self, pool: &DbPool, _context: &EventContext) -> Result<(), ApiError> {
        self.delete_adapter_without_events(pool).await
    }
}

impl<T> CanDelete for T
where
    T: DeleteAdapter,
{
    async fn delete_without_events<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_adapter_without_events(backend.db_pool()).await
    }

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_adapter(backend.db_pool(), context).await
    }
}

#[doc(hidden)]
pub trait SaveAdapter {
    type Output;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Self::Output, ApiError>;

    async fn save_adapter(
        &self,
        pool: &DbPool,
        _context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.save_adapter_without_events(pool).await
    }
}

impl<T> CanSave for T
where
    T: SaveAdapter,
{
    type Output = T::Output;

    async fn save_without_events<C>(&self, backend: &C) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_adapter_without_events(backend.db_pool()).await
    }

    async fn save<C>(&self, backend: &C, context: &EventContext) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_adapter(backend.db_pool(), context).await
    }
}

#[doc(hidden)]
pub trait UpdateAdapter {
    type Output;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        entry_id: i32,
    ) -> Result<Self::Output, ApiError>;

    async fn update_adapter(
        &self,
        pool: &DbPool,
        entry_id: i32,
        _context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.update_adapter_without_events(pool, entry_id).await
    }
}

impl<T> CanUpdate for T
where
    T: UpdateAdapter,
{
    type Output = T::Output;

    async fn update_without_events<C>(
        &self,
        backend: &C,
        entry_id: i32,
    ) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_adapter_without_events(backend.db_pool(), entry_id)
            .await
    }

    async fn update<C>(
        &self,
        backend: &C,
        entry_id: i32,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_adapter(backend.db_pool(), entry_id, context)
            .await
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
