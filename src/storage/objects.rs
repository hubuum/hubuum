use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    HubuumObject, NewHubuumObject, ObjectDataPatchDocument, ObjectSelector, ResolvedClassTarget,
    ResolvedObjectTarget, UpdateHubuumObject,
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
