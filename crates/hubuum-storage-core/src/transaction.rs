use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use hubuum_domain::{ClassId, ClassRelationId, CollectionId, ObjectId};
use hubuum_events_core::EventContext;

use crate::{
    ClassRelationStorage, ClassStorage, CollectionStorage, ObjectRelationStorage, ObjectStorage,
    StorageClass, StorageClassCreate, StorageClassRelationCreate, StorageClassSelector,
    StorageClassUpdate, StorageCollection, StorageCollectionCreate, StorageCollectionUpdate,
    StorageError, StorageMutationOutcome, StorageObject, StorageObjectCreate,
    StorageObjectDataPatch, StorageObjectRelationCreateSelector, StorageObjectRelationSelector,
    StorageObjectSelector, StorageObjectUpdate, StoragePreparedClassRelation,
    StoragePreparedObjectRelation, StorageResolvedClass, StorageResolvedClassRelation,
    StorageResolvedObject, StorageResolvedObjectRelation,
};

/// Discoverable collection operations bound to one audited transaction.
pub struct TransactionalCollections<'transaction> {
    storage: &'transaction dyn CollectionStorage,
    event_context: &'transaction EventContext,
}

impl<'transaction> TransactionalCollections<'transaction> {
    /// Bind collection operations to an adapter-owned transaction and its
    /// required audit context.
    ///
    /// Storage adapters call this from their [`StorageTransaction`]
    /// implementation; application code normally obtains this value through
    /// [`StorageTransaction::collections`].
    #[must_use]
    pub const fn new(
        storage: &'transaction dyn CollectionStorage,
        event_context: &'transaction EventContext,
    ) -> Self {
        Self {
            storage,
            event_context,
        }
    }

    pub async fn get(
        &self,
        collection_id: CollectionId,
    ) -> Result<StorageCollection, StorageError> {
        self.storage.get_collection(collection_id).await
    }

    pub async fn create(
        &self,
        command: StorageCollectionCreate,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        self.storage
            .create_collection(command, self.event_context)
            .await
    }

    pub async fn update(
        &self,
        collection_id: CollectionId,
        changes: StorageCollectionUpdate,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        self.storage
            .update_collection(collection_id, changes, self.event_context)
            .await
    }

    pub async fn delete(
        &self,
        collection_id: CollectionId,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.storage
            .delete_collection(collection_id, self.event_context)
            .await
    }

    pub async fn list_children(
        &self,
        collection_id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.storage.list_collection_children(collection_id).await
    }

    pub async fn list_ancestors(
        &self,
        collection_id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.storage.list_collection_ancestors(collection_id).await
    }

    pub async fn move_to(
        &self,
        collection_id: CollectionId,
        new_parent_id: CollectionId,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        self.storage
            .move_collection(collection_id, new_parent_id, self.event_context)
            .await
    }
}

/// Discoverable class operations bound to one audited transaction.
pub struct TransactionalClasses<'transaction> {
    storage: &'transaction dyn ClassStorage,
    event_context: &'transaction EventContext,
}

impl<'transaction> TransactionalClasses<'transaction> {
    /// Bind class operations to an adapter-owned transaction and its required
    /// audit context.
    ///
    /// Storage adapters call this from their [`StorageTransaction`]
    /// implementation; application code normally obtains this value through
    /// [`StorageTransaction::classes`].
    #[must_use]
    pub const fn new(
        storage: &'transaction dyn ClassStorage,
        event_context: &'transaction EventContext,
    ) -> Self {
        Self {
            storage,
            event_context,
        }
    }

    pub async fn resolve(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        self.storage.resolve_class(selector).await
    }

    pub async fn create(
        &self,
        command: StorageClassCreate,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        self.storage.create_class(command, self.event_context).await
    }

    pub async fn update(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        self.storage
            .update_class(target, changes, self.event_context)
            .await
    }

    pub async fn delete(
        &self,
        target: &StorageResolvedClass,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.storage.delete_class(target, self.event_context).await
    }

    pub async fn resolve_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        self.storage.resolve_class_names(class_ids).await
    }
}

/// Discoverable class-relation operations bound to one audited transaction.
pub struct TransactionalClassRelations<'transaction> {
    storage: &'transaction dyn ClassRelationStorage,
    event_context: &'transaction EventContext,
}

impl<'transaction> TransactionalClassRelations<'transaction> {
    /// Bind class-relation operations to an adapter-owned transaction and its
    /// required audit context.
    ///
    /// Storage adapters call this from their [`StorageTransaction`]
    /// implementation; application code normally obtains this value through
    /// [`StorageTransaction::class_relations`].
    #[must_use]
    pub const fn new(
        storage: &'transaction dyn ClassRelationStorage,
        event_context: &'transaction EventContext,
    ) -> Self {
        Self {
            storage,
            event_context,
        }
    }

    pub async fn prepare(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        self.storage.prepare_class_relation(command).await
    }

    pub async fn resolve(
        &self,
        relation_id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        self.storage.resolve_class_relation(relation_id).await
    }

    pub async fn create(
        &self,
        prepared: &StoragePreparedClassRelation,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError> {
        self.storage
            .create_class_relation(prepared, self.event_context)
            .await
    }

    pub async fn delete(
        &self,
        target: &StorageResolvedClassRelation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.storage
            .delete_class_relation(target, self.event_context)
            .await
    }
}

/// Discoverable object operations bound to one audited transaction.
///
/// Mutating methods always forward the transaction's [`EventContext`] and
/// return the backend's durable [`StorageMutationOutcome`].
pub struct TransactionalObjects<'transaction> {
    storage: &'transaction dyn ObjectStorage,
    event_context: &'transaction EventContext,
}

impl<'transaction> TransactionalObjects<'transaction> {
    /// Bind object operations to an adapter-owned transaction and its required
    /// audit context.
    ///
    /// Storage adapters call this from their [`StorageTransaction`]
    /// implementation; application code normally obtains this value through
    /// [`StorageTransaction::objects`].
    #[must_use]
    pub const fn new(
        storage: &'transaction dyn ObjectStorage,
        event_context: &'transaction EventContext,
    ) -> Self {
        Self {
            storage,
            event_context,
        }
    }

    pub async fn get(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        self.storage.get_object(object_id).await
    }

    pub async fn resolve(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        self.storage.resolve_object(selector).await
    }

    pub async fn create(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        self.storage
            .create_object(class, command, self.event_context)
            .await
    }

    pub async fn update(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        self.storage
            .update_object(target, changes, self.event_context)
            .await
    }

    pub async fn patch_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        self.storage
            .patch_object_data(target, patch, self.event_context)
            .await
    }

    pub async fn delete(
        &self,
        target: &StorageResolvedObject,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.storage.delete_object(target, self.event_context).await
    }

    pub async fn validate(&self, object: StorageObject) -> Result<(), StorageError> {
        self.storage.validate_object(object).await
    }

    pub async fn validate_create(&self, command: StorageObjectCreate) -> Result<(), StorageError> {
        self.storage.validate_object_create(command).await
    }

    pub async fn validate_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        self.storage
            .validate_object_update(object_id, changes)
            .await
    }
}

/// Discoverable object-relation operations bound to one audited transaction.
pub struct TransactionalObjectRelations<'transaction> {
    storage: &'transaction dyn ObjectRelationStorage,
    event_context: &'transaction EventContext,
}

impl<'transaction> TransactionalObjectRelations<'transaction> {
    /// Bind object-relation operations to an adapter-owned transaction and its
    /// required audit context.
    ///
    /// Storage adapters call this from their [`StorageTransaction`]
    /// implementation; application code normally obtains this value through
    /// [`StorageTransaction::object_relations`].
    #[must_use]
    pub const fn new(
        storage: &'transaction dyn ObjectRelationStorage,
        event_context: &'transaction EventContext,
    ) -> Self {
        Self {
            storage,
            event_context,
        }
    }

    pub async fn prepare(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        self.storage.prepare_object_relation(selector).await
    }

    pub async fn resolve(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        self.storage.resolve_object_relation(selector).await
    }

    pub async fn create(
        &self,
        prepared: &StoragePreparedObjectRelation,
    ) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        self.storage
            .create_object_relation(prepared, self.event_context)
            .await
    }

    pub async fn delete(
        &self,
        target: &StorageResolvedObjectRelation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        self.storage
            .delete_object_relation(target, self.event_context)
            .await
    }
}

/// Backend-neutral capabilities available inside one atomic unit of work.
///
/// The accessors return crate-owned operation types with inherent methods for
/// discoverability. Those types delegate to the ordinary storage capability
/// traits, avoiding a second mirrored `Tx*` contract. Native connections,
/// transactions, query builders, and driver errors never cross this boundary.
///
/// Only capabilities whose operations are safe to compose are exposed here.
/// Backend-owned state machines and operations with stronger invariants remain
/// atomic methods on their existing capability traits.
pub trait StorageTransaction: Send + Sync {
    fn collections(&self) -> TransactionalCollections<'_>;

    fn classes(&self) -> TransactionalClasses<'_>;

    fn class_relations(&self) -> TransactionalClassRelations<'_>;

    fn objects(&self) -> TransactionalObjects<'_>;

    fn object_relations(&self) -> TransactionalObjectRelations<'_>;
}

/// Send-capable work performed against one backend-neutral transaction.
///
/// The single boxed future keeps the public callback lifetime expressible on
/// the workspace MSRV. Individual storage calls continue to use their normal
/// capability traits, so this does not introduce per-operation type erasure.
pub type StorageTransactionFuture<'transaction, R> =
    Pin<Box<dyn Future<Output = Result<R, StorageError>> + Send + 'transaction>>;

/// Mandatory atomic-composition capability for a selectable storage backend.
///
/// A backend commits when `operation` returns `Ok` and rolls back when it
/// returns `Err`. Every composable mutation receives `event_context`, so the
/// resulting state change and durable audit event are one semantic operation.
/// The transaction also inherits the surrounding [`crate::ExecutionStorage`]
/// context, including call-site attribution and revision preconditions.
///
/// This method is intentionally generic and therefore not object-safe. Hubuum
/// selects backends through static Rust composition rather than dynamic plugin
/// discovery. Keeping the result typed avoids the `Any` downcasts required by
/// object-safe transaction runners.
#[async_trait]
pub trait TransactionStorage: Send + Sync {
    async fn with_transaction<F, R>(
        &self,
        event_context: EventContext,
        operation: F,
    ) -> Result<R, StorageError>
    where
        F: for<'transaction> FnOnce(
                &'transaction dyn StorageTransaction,
            ) -> StorageTransactionFuture<'transaction, R>
            + Send,
        R: Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_transaction_is_object_safe(_: &dyn StorageTransaction) {}

    #[test]
    fn transaction_port_remains_object_safe() {
        let assertion: fn(&dyn StorageTransaction) = assert_transaction_is_object_safe;
        let _ = assertion;
    }
}
