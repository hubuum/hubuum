use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{
    ObjectRelationCreateSelector, ObjectRelationSelector, PreparedObjectRelation,
    ResolvedObjectRelationTarget,
};

use super::StorageError;

/// Persistence capability for object-relation resolution and lifecycle writes.
#[async_trait]
pub trait ObjectRelationStore: Send + Sync {
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
        context: &EventContext,
    ) -> Result<ResolvedObjectRelationTarget, StorageError>;

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError>;
}
