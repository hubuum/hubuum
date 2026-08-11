use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    ClassIdSet, ClassSelector, Collection, HubuumClass, NewHubuumClass, ResolvedClassTarget,
    UpdateHubuumClass,
};

use super::StorageError;

/// Persistence capability for class resolution and lifecycle mutations.
///
/// Resolved targets preserve the route-selected address so implementations can
/// recheck ID- and name-based mutations atomically before writing.
#[async_trait]
pub(crate) trait ClassStore: Send + Sync {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError>;

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError>;

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError>;

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError>;
}

/// Backend-neutral compatibility contract for point class persistence.
///
/// Application services use [`ClassStore`] for ordinary lifecycle behavior.
/// Legacy domain adapters additionally require event-suppressed fixture writes,
/// direct point loads, and collection access; selectable backends must provide
/// those operations without exposing an adapter or database type to callers.
#[async_trait]
pub(crate) trait ClassRecordStorage: Send + Sync {
    async fn create_class_record(
        &self,
        class: &NewHubuumClass,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError>;

    async fn update_class_record(
        &self,
        update: &UpdateHubuumClass,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError>;

    async fn delete_class_record(
        &self,
        class: &HubuumClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn load_class_record(&self, class_id: i32) -> Result<HubuumClass, StorageError>;

    async fn class_collection(&self, class_id: i32) -> Result<Collection, StorageError>;

    async fn class_names(&self, class_ids: &ClassIdSet)
    -> Result<Vec<(i32, String)>, StorageError>;
}
