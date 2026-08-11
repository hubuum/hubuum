use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    HubuumClassRelation, HubuumClassRelationID, NewHubuumClassRelation, PreparedClassRelation,
    ResolvedClassRelationTarget,
};

use super::StorageError;

/// Persistence capability for class-relation resolution and lifecycle writes.
///
/// Prepared and resolved aggregates include both endpoint classes so callers
/// can authorize without depending on persistence-specific lookups.
/// A missing event context deliberately suppresses audit-event emission for
/// compatibility operations; selectable backends must still perform the same
/// validation, persistence, and observation behavior.
#[async_trait]
pub(crate) trait ClassRelationStore: Send + Sync {
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
        context: Option<&EventContext>,
    ) -> Result<ResolvedClassRelationTarget, StorageError>;

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    /// Create directly from a command for compatibility callers that do not
    /// require a separately authorized prepared aggregate.
    async fn create_class_relation_from_command(
        &self,
        command: NewHubuumClassRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumClassRelation, StorageError>;

    /// Delete directly by identifier for compatibility callers that do not
    /// require a separately authorized resolved aggregate.
    async fn delete_class_relation_by_id(
        &self,
        id: HubuumClassRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}
