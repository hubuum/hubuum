use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use hubuum_domain::{ClassId, ClassRelationId, CollectionId, ObjectId};
use tracing::{Instrument, debug, debug_span, warn};

use super::{
    ClassRelationStorage, ClassStorage, CollectionStorage, MutationOutcome, ObjectRelationStorage,
    ObjectStorage, StorageClassCreate, StorageClassRecord, StorageClassRelationCreate,
    StorageClassSelector, StorageClassUpdate, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageError, StorageObject, StorageObjectCreate,
    StorageObjectDataPatch, StorageObjectRelationCreateSelector, StorageObjectRelationSelector,
    StorageObjectSelector, StorageObjectUpdate, StorageObservation, StorageObserver,
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StorageResolvedClass,
    StorageResolvedClassRelation, StorageResolvedObject, StorageResolvedObjectRelation,
};
use crate::events::EventContext;

/// Uniform diagnostics around whichever storage capabilities `S` implements.
///
/// The wrapper deliberately has no aggregate capability bound. A focused test
/// adapter can implement one storage family and is observed only for that
/// family; complete backend selection is enforced separately at composition.
pub(crate) struct ObservedStorage<S> {
    backend: &'static str,
    inner: Arc<S>,
    observer: Arc<dyn StorageObserver>,
}

#[derive(Debug)]
pub(crate) struct ApplicationStorageObserver;

impl StorageObserver for ApplicationStorageObserver {
    fn operation_finished(&self, observation: &StorageObservation) {
        crate::observability::metrics::storage_operation_finished(
            observation.backend(),
            observation.capability(),
            observation.operation(),
            observation.result(),
            observation.duration(),
        );
    }
}

impl<S> ObservedStorage<S> {
    pub(crate) fn new(
        storage: S,
        backend: &'static str,
        observer: Arc<dyn StorageObserver>,
    ) -> Self {
        Self {
            backend,
            inner: Arc::new(storage),
            observer,
        }
    }

    async fn call<T>(
        &self,
        capability: &'static str,
        operation: &'static str,
        future: impl Future<Output = Result<T, StorageError>>,
    ) -> Result<T, StorageError> {
        observe_storage_call_with(
            self.observer.as_ref(),
            self.backend,
            capability,
            operation,
            future,
        )
        .await
    }
}

pub(super) async fn observe_storage_call_with<T>(
    observer: &dyn StorageObserver,
    backend: &'static str,
    capability: &'static str,
    operation: &'static str,
    future: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    let span = debug_span!("storage_operation", backend, capability, operation,);
    async move {
        let started_at = Instant::now();
        let result = future.await;
        let duration = started_at.elapsed();
        let result_kind = result
            .as_ref()
            .map(|_| "ok")
            .unwrap_or_else(|error| error.kind().as_str());
        observer.operation_finished(&StorageObservation::new(
            backend,
            capability,
            operation,
            result_kind,
            duration,
        ));

        match &result {
            Ok(_) => debug!(
                message = "storage operation complete",
                elapsed_ms = duration.as_millis(),
            ),
            Err(error) if error.kind().is_backend_failure() => warn!(
                message = "storage operation failed",
                error_kind = error.kind().as_str(),
                elapsed_ms = duration.as_millis(),
                error = %error,
            ),
            Err(error) => debug!(
                message = "storage operation rejected",
                error_kind = error.kind().as_str(),
                elapsed_ms = duration.as_millis(),
                error = %error,
            ),
        }
        result
    }
    .instrument(span)
    .await
}

#[async_trait]
impl<S> CollectionStorage for ObservedStorage<S>
where
    S: CollectionStorage,
{
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        self.call("collections", "get", self.inner.get_collection(id))
            .await
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        self.call(
            "collections",
            "create",
            self.inner.create_collection(command, context),
        )
        .await
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        self.call(
            "collections",
            "update",
            self.inner.update_collection(id, changes, context),
        )
        .await
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.call(
            "collections",
            "delete",
            self.inner.delete_collection(id, context),
        )
        .await
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.call(
            "collections",
            "children",
            self.inner.list_collection_children(id),
        )
        .await
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.call(
            "collections",
            "ancestors",
            self.inner.list_collection_ancestors(id),
        )
        .await
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        self.call(
            "collections",
            "move",
            self.inner.move_collection(id, new_parent_id, context),
        )
        .await
    }
}

#[async_trait]
impl<S> ClassStorage for ObservedStorage<S>
where
    S: ClassStorage,
{
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        self.call("classes", "resolve", self.inner.resolve_class(selector))
            .await
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        self.call(
            "classes",
            "create",
            self.inner.create_class(command, context),
        )
        .await
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        self.call(
            "classes",
            "update",
            self.inner.update_class(target, changes, context),
        )
        .await
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.call(
            "classes",
            "delete",
            self.inner.delete_class(target, context),
        )
        .await
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        self.call(
            "classes",
            "names",
            self.inner.resolve_class_names(class_ids),
        )
        .await
    }
}

#[async_trait]
impl<S> ObjectStorage for ObservedStorage<S>
where
    S: ObjectStorage,
{
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        self.call("objects", "get", self.inner.get_object(object_id))
            .await
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        self.call("objects", "resolve", self.inner.resolve_object(selector))
            .await
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        self.call(
            "objects",
            "create",
            self.inner.create_object(class, command, context),
        )
        .await
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        self.call(
            "objects",
            "update",
            self.inner.update_object(target, changes, context),
        )
        .await
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        self.call(
            "objects",
            "patch_data",
            self.inner.patch_object_data(target, patch, context),
        )
        .await
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.call(
            "objects",
            "delete",
            self.inner.delete_object(target, context),
        )
        .await
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        self.call("objects", "validate", self.inner.validate_object(object))
            .await
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        self.call(
            "objects",
            "validate_create",
            self.inner.validate_object_create(command),
        )
        .await
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        self.call(
            "objects",
            "validate_update",
            self.inner.validate_object_update(object_id, changes),
        )
        .await
    }
}

#[async_trait]
impl<S> ClassRelationStorage for ObservedStorage<S>
where
    S: ClassRelationStorage,
{
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        self.call(
            "class_relations",
            "prepare_create",
            self.inner.prepare_class_relation(command),
        )
        .await
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        self.call(
            "class_relations",
            "resolve",
            self.inner.resolve_class_relation(id),
        )
        .await
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedClassRelation>, StorageError> {
        self.call(
            "class_relations",
            "create",
            self.inner.create_class_relation(prepared, context),
        )
        .await
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.call(
            "class_relations",
            "delete",
            self.inner.delete_class_relation(target, context),
        )
        .await
    }
}

#[async_trait]
impl<S> ObjectRelationStorage for ObservedStorage<S>
where
    S: ObjectRelationStorage,
{
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        self.call(
            "object_relations",
            "prepare_create",
            self.inner.prepare_object_relation(selector),
        )
        .await
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        self.call(
            "object_relations",
            "resolve",
            self.inner.resolve_object_relation(selector),
        )
        .await
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        self.call(
            "object_relations",
            "create",
            self.inner.create_object_relation(prepared, context),
        )
        .await
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.call(
            "object_relations",
            "delete",
            self.inner.delete_object_relation(target, context),
        )
        .await
    }
}
