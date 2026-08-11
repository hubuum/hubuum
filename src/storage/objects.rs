use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    Collection, HubuumClass, HubuumObject, NewHubuumObject, ObjectDataPatchDocument,
    ObjectSelector, ResolvedClassTarget, ResolvedObjectTarget, UpdateHubuumObject,
};

use super::StorageError;

/// Persistence capability for object resolution and lifecycle mutations.
///
/// Resolved class and object targets preserve the route-selected address so
/// implementations can recheck ID- and name-based mutations atomically before
/// writing.
#[async_trait]
pub(crate) trait ObjectStore: Send + Sync {
    async fn resolve_object(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, StorageError>;

    async fn create_object(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError>;

    async fn update_object(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError>;

    async fn patch_object_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError>;

    async fn delete_object(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), StorageError>;
}

/// Backend-neutral compatibility contract for point object persistence.
///
/// Application services use [`ObjectStore`] for ordinary lifecycle behavior.
/// Older domain adapters still need event-suppressed fixture writes, direct
/// point loads, and validation entrypoints; keeping those operations behind
/// this mandatory trait prevents their callers from depending on a database
/// adapter while that compatibility surface is retired.
#[async_trait]
pub(crate) trait ObjectRecordStorage: Send + Sync {
    async fn validate_object(&self, object: &HubuumObject) -> Result<(), StorageError>;

    async fn validate_new_object(&self, object: &NewHubuumObject) -> Result<(), StorageError>;

    async fn validate_object_update(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
    ) -> Result<(), StorageError>;

    async fn save_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError>;

    async fn create_object_record(
        &self,
        object: &NewHubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError>;

    async fn update_object_record(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError>;

    async fn delete_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn load_object_record(&self, object_id: i32) -> Result<HubuumObject, StorageError>;

    async fn object_collection(&self, object_id: i32) -> Result<Collection, StorageError>;

    async fn object_class(&self, object_id: i32) -> Result<HubuumClass, StorageError>;
}
