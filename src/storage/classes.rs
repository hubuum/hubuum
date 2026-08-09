use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    ClassSelector, HubuumClass, NewHubuumClass, ResolvedClassTarget, UpdateHubuumClass,
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
