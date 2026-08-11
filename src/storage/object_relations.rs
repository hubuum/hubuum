use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    HubuumObjectRelation, HubuumObjectRelationID, NewHubuumObjectRelation,
    ObjectRelationCreateSelector, ObjectRelationSelector, PreparedObjectRelation,
    ResolvedObjectRelationTarget,
};

use super::StorageError;

/// Persistence capability for object-relation resolution and lifecycle writes.
///
/// A missing event context deliberately suppresses audit-event emission for
/// compatibility operations; selectable backends must still perform the same
/// validation, persistence, and observation behavior.
#[async_trait]
pub(crate) trait ObjectRelationStore: Send + Sync {
    async fn prepare_object_relation(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, StorageError>;

    async fn resolve_object_relation(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, StorageError>;

    async fn create_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<ResolvedObjectRelationTarget, StorageError>;

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    /// Create directly from a command for compatibility callers that do not
    /// require a separately authorized prepared aggregate.
    async fn create_object_relation_from_command(
        &self,
        command: NewHubuumObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumObjectRelation, StorageError>;

    /// Delete directly by identifier for compatibility callers that do not
    /// require a separately authorized resolved aggregate.
    async fn delete_object_relation_by_id(
        &self,
        id: HubuumObjectRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}
