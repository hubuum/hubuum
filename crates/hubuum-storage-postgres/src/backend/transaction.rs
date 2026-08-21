use async_trait::async_trait;
use hubuum_domain::{ClassId, ClassRelationId, CollectionId, ObjectId};
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    ClassRelationStorage, ClassStorage, CollectionStorage, MutationOutcome, ObjectRelationStorage,
    ObjectStorage, StorageClassCreate, StorageClassRecord, StorageClassRelationCreate,
    StorageClassSelector, StorageClassUpdate, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageError, StorageObject, StorageObjectCreate,
    StorageObjectDataPatch, StorageObjectRelationCreateSelector, StorageObjectRelationSelector,
    StorageObjectSelector, StorageObjectUpdate, StoragePreparedClassRelation,
    StoragePreparedObjectRelation, StorageResolvedClass, StorageResolvedClassRelation,
    StorageResolvedObject, StorageResolvedObjectRelation, StorageTransaction,
    StorageTransactionFuture, TransactionStorage, TransactionalClassRelations,
    TransactionalClasses, TransactionalCollections, TransactionalObjectRelations,
    TransactionalObjects,
};
use tokio::sync::Mutex;

use super::PostgresStorage;
use crate::{PostgresConnection, PostgresRuntime};

/// Transaction-scoped PostgreSQL implementation of the composable storage
/// ports. The native connection remains private and every operation serializes
/// access to it, matching the sequential semantics of a database transaction.
struct PostgresTransaction<'connection> {
    runtime: PostgresRuntime,
    connection: Mutex<&'connection mut PostgresConnection>,
    event_context: EventContext,
}

impl<'connection> PostgresTransaction<'connection> {
    fn new(
        runtime: PostgresRuntime,
        connection: &'connection mut PostgresConnection,
        event_context: EventContext,
    ) -> Self {
        Self {
            runtime,
            connection: Mutex::new(connection),
            event_context,
        }
    }
}

impl StorageTransaction for PostgresTransaction<'_> {
    fn collections(&self) -> TransactionalCollections<'_> {
        TransactionalCollections::new(self, &self.event_context)
    }

    fn classes(&self) -> TransactionalClasses<'_> {
        TransactionalClasses::new(self, &self.event_context)
    }

    fn class_relations(&self) -> TransactionalClassRelations<'_> {
        TransactionalClassRelations::new(self, &self.event_context)
    }

    fn objects(&self) -> TransactionalObjects<'_> {
        TransactionalObjects::new(self, &self.event_context)
    }

    fn object_relations(&self) -> TransactionalObjectRelations<'_> {
        TransactionalObjectRelations::new(self, &self.event_context)
    }
}

#[async_trait]
impl CollectionStorage for PostgresTransaction<'_> {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::get_collection_on(&mut connection, id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::create_collection_on(&mut connection, command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::update_collection_on(
            &mut connection,
            id.id(),
            changes,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::delete_collection_on(&mut connection, id.id(), context)
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::collection_children_on(&mut connection, id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::collection_ancestors_on(&mut connection, id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::collection::move_collection_on(
            &mut connection,
            id.id(),
            new_parent_id.id(),
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ClassStorage for PostgresTransaction<'_> {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::class::resolve_class_on(&mut connection, selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::class::create_class_on(&mut connection, command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::class::update_class_on(&mut connection, target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::class::delete_class_on(&mut connection, target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let mut connection = self.connection.lock().await;
        let class_ids = class_ids.into_iter().map(ClassId::id).collect();
        crate::operations::class::class_names_on(&mut connection, class_ids)
            .await
            .map_err(StorageError::from)
            .and_then(|rows| {
                rows.into_iter()
                    .map(|(id, name)| {
                        ClassId::new(id)
                            .map(|id| (id, name))
                            .map_err(crate::PostgresStorageError::from)
                            .map_err(StorageError::from)
                    })
                    .collect()
            })
    }
}

#[async_trait]
impl ClassRelationStorage for PostgresTransaction<'_> {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::prepare_class_relation_on(&mut connection, command)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::resolve_class_relation_on(&mut connection, id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedClassRelation>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::create_class_relation_on(&mut connection, prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::delete_class_relation_on(&mut connection, target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectStorage for PostgresTransaction<'_> {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::get_object_on(&mut connection, object_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::resolve_object_on(&mut connection, selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::create_object_on(
            &self.runtime,
            &mut connection,
            class,
            command,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::update_object_on(
            &self.runtime,
            &mut connection,
            target,
            changes,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::patch_object_data_on(
            &self.runtime,
            &mut connection,
            target,
            patch,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::delete_object_on(&mut connection, target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::validate_object_on(&mut connection, object)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::validate_object_create_command_on(&mut connection, command)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::object::validate_object_update_command_on(
            &mut connection,
            object_id.id(),
            changes,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectRelationStorage for PostgresTransaction<'_> {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::prepare_object_relation_on(&mut connection, selector)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::resolve_object_relation_on(&mut connection, selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::create_object_relation_on(&mut connection, prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        let mut connection = self.connection.lock().await;
        crate::operations::relation::delete_object_relation_on(&mut connection, target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl TransactionStorage for PostgresStorage {
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
        R: Send,
    {
        let executor = self.runtime().clone();
        let transaction_runtime = executor.clone();
        executor
            .with_transaction(async move |connection| {
                let transaction =
                    PostgresTransaction::new(transaction_runtime, connection, event_context);
                operation(&transaction).await
            })
            .await
            .map_err(StorageError::from)
    }
}
