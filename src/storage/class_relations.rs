use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    HubuumClassRelationID, NewHubuumClassRelation, PreparedClassRelation,
    ResolvedClassRelationTarget,
};

use super::StorageError;

/// Persistence capability for class-relation resolution and lifecycle writes.
///
/// Prepared and resolved aggregates include both endpoint classes so callers
/// can authorize without depending on persistence-specific lookups.
#[async_trait]
pub trait ClassRelationStore: Send + Sync {
    async fn prepare_class_relation(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, StorageError>;

    async fn resolve_class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, StorageError>;

    async fn create_class_relation(
        &self,
        prepared: &PreparedClassRelation,
        context: &EventContext,
    ) -> Result<ResolvedClassRelationTarget, StorageError>;

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError>;
}
