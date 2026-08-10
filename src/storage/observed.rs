use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{Instrument, debug, debug_span, warn};

use crate::events::EventContext;
use crate::models::{
    ClassSelector, Collection, CollectionID, HubuumClass, HubuumClassRelationID, HubuumObject,
    NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationSelector, ObjectSelector,
    PreparedClassRelation, PreparedObjectRelation, ResolvedClassRelationTarget,
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget, UpdateCollection,
    UpdateHubuumClass, UpdateHubuumObject,
};

use super::{
    ClassRelationStore, ClassStore, CollectionStore, LifecycleStorage, ObjectRelationStore,
    ObjectStore, StorageError, StorageIdentity,
};

/// Uniform diagnostics around every lifecycle storage entrypoint.
pub(super) struct ObservedLifecycleStorage {
    backend: &'static str,
    inner: Arc<dyn LifecycleStorage>,
}

impl ObservedLifecycleStorage {
    pub(super) fn new(inner: Arc<dyn LifecycleStorage>) -> Self {
        Self {
            backend: inner.storage_name(),
            inner,
        }
    }

    async fn call<T>(
        &self,
        capability: &'static str,
        operation: &'static str,
        future: impl Future<Output = Result<T, StorageError>>,
    ) -> Result<T, StorageError> {
        observe_storage_call(self.backend, capability, operation, future).await
    }
}

/// Apply the common logical-storage diagnostics to any capability entrypoint.
pub(super) async fn observe_storage_call<T>(
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
        crate::observability::metrics::storage_operation_finished(
            backend,
            capability,
            operation,
            result_kind,
            duration,
        );

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

impl StorageIdentity for ObservedLifecycleStorage {
    fn storage_name(&self) -> &'static str {
        self.backend
    }
}

#[async_trait]
impl CollectionStore for ObservedLifecycleStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        self.call("collections", "get", self.inner.get_collection(id))
            .await
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        self.call(
            "collections",
            "create",
            self.inner.create_collection(command, context),
        )
        .await
    }

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        self.call(
            "collections",
            "update",
            self.inner.update_collection(id, changes, context),
        )
        .await
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.call(
            "collections",
            "delete",
            self.inner.delete_collection(id, context),
        )
        .await
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        self.call(
            "collections",
            "children",
            self.inner.collection_children(id),
        )
        .await
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        self.call(
            "collections",
            "ancestors",
            self.inner.collection_ancestors(id),
        )
        .await
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        self.call(
            "collections",
            "move",
            self.inner.move_collection(id, new_parent_id, context),
        )
        .await
    }
}

#[async_trait]
impl ClassStore for ObservedLifecycleStorage {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError> {
        self.call("classes", "resolve", self.inner.resolve_class(selector))
            .await
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        self.call(
            "classes",
            "create",
            self.inner.create_class(command, context),
        )
        .await
    }

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        self.call(
            "classes",
            "update",
            self.inner.update_class(target, changes, context),
        )
        .await
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.call(
            "classes",
            "delete",
            self.inner.delete_class(target, context),
        )
        .await
    }
}

#[async_trait]
impl ObjectStore for ObservedLifecycleStorage {
    async fn resolve_object(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, StorageError> {
        self.call("objects", "resolve", self.inner.resolve_object(selector))
            .await
    }

    async fn create_object(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        self.call(
            "objects",
            "create",
            self.inner.create_object(class, command, context),
        )
        .await
    }

    async fn update_object(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        self.call(
            "objects",
            "update",
            self.inner.update_object(target, changes, context),
        )
        .await
    }

    async fn patch_object_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        self.call(
            "objects",
            "patch_data",
            self.inner.patch_object_data(target, patch, context),
        )
        .await
    }

    async fn delete_object(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.call(
            "objects",
            "delete",
            self.inner.delete_object(target, context),
        )
        .await
    }
}

#[async_trait]
impl ClassRelationStore for ObservedLifecycleStorage {
    async fn prepare_class_relation(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, StorageError> {
        self.call(
            "class_relations",
            "prepare_create",
            self.inner.prepare_class_relation(command),
        )
        .await
    }

    async fn resolve_class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        self.call(
            "class_relations",
            "resolve",
            self.inner.resolve_class_relation(id),
        )
        .await
    }

    async fn create_class_relation(
        &self,
        prepared: &PreparedClassRelation,
        context: &EventContext,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        self.call(
            "class_relations",
            "create",
            self.inner.create_class_relation(prepared, context),
        )
        .await
    }

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.call(
            "class_relations",
            "delete",
            self.inner.delete_class_relation(target, context),
        )
        .await
    }
}

#[async_trait]
impl ObjectRelationStore for ObservedLifecycleStorage {
    async fn prepare_object_relation(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, StorageError> {
        self.call(
            "object_relations",
            "prepare_create",
            self.inner.prepare_object_relation(selector),
        )
        .await
    }

    async fn resolve_object_relation(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        self.call(
            "object_relations",
            "resolve",
            self.inner.resolve_object_relation(selector),
        )
        .await
    }

    async fn create_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
        context: &EventContext,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        self.call(
            "object_relations",
            "create",
            self.inner.create_object_relation(prepared, context),
        )
        .await
    }

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.call(
            "object_relations",
            "delete",
            self.inner.delete_object_relation(target, context),
        )
        .await
    }
}
