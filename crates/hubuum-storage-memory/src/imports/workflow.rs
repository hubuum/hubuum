use super::*;
use hubuum_storage_core::{FencedImportItem, FencedImportPlan, FencedImportResults};

#[async_trait]
impl ImportStorage for MemoryStorage {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        self.get_collection(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"))
            .await
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .get(&collection_id.id())
            .cloned())
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        let parts = key.clone().into_parts();
        let state = self.state.read().await;
        let mut candidates = state
            .collections
            .values()
            .filter(|collection| collection.name() == parts.name)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(path) = parts.path {
            candidates.retain(|collection| {
                if collection.id().id() == ROOT_COLLECTION_ID {
                    return path.is_empty();
                }
                // Import paths include the selected collection and omit root.
                let mut names = vec![collection.name().to_string()];
                let mut parent = collection.parent_collection_id();
                while let Some(parent_id) = parent {
                    if parent_id.id() == ROOT_COLLECTION_ID {
                        break;
                    }
                    let Some(ancestor) = state.collections.get(&parent_id.id()) else {
                        return false;
                    };
                    names.push(ancestor.name().to_string());
                    parent = ancestor.parent_collection_id();
                }
                names.reverse();
                names == path
            });
        }
        match candidates.as_slice() {
            [] => Ok(None),
            [collection] => Ok(Some(collection.clone())),
            _ => Err(StorageError::conflict(format!(
                "Import collection key '{}' is ambiguous",
                parts.name
            ))),
        }
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .filter(|collection| collection.name() == name)
            .cloned()
            .collect())
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .find(|collection| {
                collection.parent_collection_id() == Some(parent_collection_id)
                    && collection.name() == name
            })
            .cloned())
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .find(|class| class.collection_id() == collection_id && class.name() == name)
            .cloned())
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .filter(|class| {
                class.collection_id() == collection_id
                    && names.iter().any(|name| name == class.name())
            })
            .cloned()
            .collect())
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .find(|object| object.class_id() == class_id && object.name() == name)
            .cloned())
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .filter(|object| {
                object.class_id() == class_id && names.iter().any(|name| name == object.name())
            })
            .cloned()
            .collect())
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .any(|relation| {
                relation.from_class_id() == left_class_id
                    && relation.to_class_id() == right_class_id
            }))
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .any(|relation| {
                relation.from_object_id() == left_object_id
                    && relation.to_object_id() == right_object_id
            }))
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        let scope_ids = state
            .identity_scopes
            .values()
            .filter(|scope| scope.name() == identity_scope)
            .map(|scope| scope.id())
            .collect::<BTreeSet<_>>();
        Ok(state.groups.values().any(|group| {
            group.name() == group_name && scope_ids.contains(&group.identity_scope_id())
        }))
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        let scratch = Self {
            schema_limits: self.schema_limits,
            state: Arc::new(MemoryStateLock::new(self.state.read().await.clone())),
        };
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        let mut aborted = false;
        for item in plan.into_items() {
            crate::execution::task_execution_checkpoint()?;
            let (index, operation) = item.into_parts();
            match scratch
                .commit_import_operation(operation, &mut references)
                .await
            {
                Ok(revision) => items.push(StorageImportPreflightItem::success(index, revision)),
                Err(error) => {
                    items.push(StorageImportPreflightItem::failure(index, None, error));
                    if mode.atomicity() == StorageImportAtomicity::Strict {
                        aborted = true;
                        break;
                    }
                }
            }
        }
        Ok(StorageImportPreflight::new(items, aborted))
    }

    async fn apply_claimed_import_strict(
        &self,
        plan: FencedImportPlan,
    ) -> Result<(), StorageError> {
        self.apply_claimed_import(plan, None).await.map(|_| ())
    }
    async fn apply_claimed_import_best_effort(
        &self,
        plan: FencedImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        self.apply_claimed_import(plan, Some(mode)).await
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        let mut committed = self.state.write().await;
        let scratch = Self {
            schema_limits: self.schema_limits,
            state: Arc::new(MemoryStateLock::new(committed.clone())),
        };
        let mut references = BTreeMap::new();
        for item in plan.into_items() {
            scratch
                .apply_import_operation(item.into_parts().1, &mut references)
                .await?;
        }
        *committed = scratch.state.read().await.clone();
        Ok(())
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        _mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        for item in plan.into_items() {
            crate::execution::task_execution_checkpoint()?;
            let (index, operation) = item.into_parts();
            match self
                .commit_import_operation(operation, &mut references)
                .await
            {
                Ok(_) => items.push(StorageImportApplyItem::success(index)),
                Err(error) => items.push(StorageImportApplyItem::failure(index, error)),
            }
        }
        Ok(StorageImportApply::new(items, false))
    }

    async fn record_claimed_import_results(
        &self,
        results: FencedImportResults,
    ) -> Result<(), StorageError> {
        let (lease, results) = results.into_parts();
        self.check_import_claim(&lease).await?;
        let snapshot = self.state.read().await.clone();
        let generation = snapshot.generation.clone();
        let scratch = Self {
            schema_limits: self.schema_limits,
            state: Arc::new(MemoryStateLock::new(snapshot)),
        };
        scratch.record_import_results(results).await?;
        let committed_state = scratch.state.read().await.clone();
        self.state
            .commit_import(&generation, committed_state, &lease)
            .await
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let mut staged = Vec::with_capacity(results.len());
        let mut ids = state.import_task_results.id_cursor();
        for result in results {
            crate::execution::task_execution_checkpoint()?;
            let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
                result.into_parts();
            let task = state.tasks.get(&task_id.id()).ok_or_else(|| {
                StorageError::not_found(format!("Task {} was not found", task_id.id()))
            })?;
            if task.kind != StorageTaskKind::Import {
                return Err(StorageError::invalid_input(format!(
                    "Task {} is not an import task",
                    task_id.id()
                )));
            }
            let stored = StorageImportTaskResult::builder(
                ids.allocate()?,
                task_id,
                entity_kind,
                action,
                outcome,
                Utc::now(),
            )
            .item_ref(item_ref)
            .identifier(identifier)
            .error(error)
            .details(details)
            .build();
            staged.push(stored);
        }
        for result in staged {
            state.import_task_results.insert(result);
        }
        Ok(())
    }
}

impl MemoryStorage {
    async fn apply_claimed_import(
        &self,
        plan: FencedImportPlan,
        mode: Option<StorageImportMode>,
    ) -> Result<StorageImportApply, StorageError> {
        let (lease, items) = plan.into_parts();
        let mut references = BTreeMap::new();
        let Some(mode) = mode else {
            let indices = items
                .iter()
                .cloned()
                .map(|item| item.into_parts().0)
                .collect::<Vec<_>>();
            self.commit_import_receipts(&lease, items, &mut references, true)
                .await?;
            return Ok(StorageImportApply::new(
                indices
                    .into_iter()
                    .map(StorageImportApplyItem::success)
                    .collect(),
                false,
            ));
        };
        let mut outcomes = Vec::new();
        let mut aborted = false;
        for item in items {
            let (index, _, result) = item.clone().into_parts();
            match self
                .commit_import_receipts(&lease, vec![item], &mut references, false)
                .await
            {
                Ok(()) => outcomes.push(StorageImportApplyItem::success(index)),
                Err(error) => {
                    if matches!(
                        error.kind(),
                        StorageErrorKind::TaskCancelled | StorageErrorKind::TaskDeadlineExceeded
                    ) {
                        return Err(error);
                    }
                    aborted = match error.kind() {
                        StorageErrorKind::PermissionDenied
                        | StorageErrorKind::AuthenticationRequired => {
                            mode.permission_policy() == StorageImportPermissionPolicy::Abort
                        }
                        StorageErrorKind::Conflict => {
                            mode.collision_policy() == StorageImportCollisionPolicy::Abort
                        }
                        _ => false,
                    };
                    self.commit_import_receipts(
                        &lease,
                        vec![FencedImportItem::new(index, None, result.failed(&error))],
                        &mut references,
                        false,
                    )
                    .await?;
                    outcomes.push(StorageImportApplyItem::failure(index, error));
                    if aborted {
                        break;
                    }
                }
            }
        }
        Ok(StorageImportApply::new(outcomes, aborted))
    }

    async fn commit_import_receipts(
        &self,
        lease: &StorageTaskLease,
        items: Vec<FencedImportItem>,
        references: &mut BTreeMap<String, MemoryImportReference>,
        complete_import: bool,
    ) -> Result<(), StorageError> {
        let snapshot = self.state.read().await.clone();
        let generation = snapshot.generation.clone();
        let scratch = Self {
            schema_limits: self.schema_limits,
            state: Arc::new(MemoryStateLock::new(snapshot)),
        };
        let mut next_references = references.clone();
        for item in items {
            crate::execution::task_execution_checkpoint()?;
            self.check_import_claim(lease).await?;
            let (index, operation, result) = item.into_parts();
            if !scratch
                .state
                .write()
                .await
                .import_execution_receipts
                .insert((lease.task_id().id(), index))
            {
                return Err(StorageError::conflict(
                    "import item already has an execution receipt",
                ));
            }
            if let Some(operation) = operation {
                scratch
                    .apply_import_operation(operation, &mut next_references)
                    .await?;
            }
            scratch.record_import_results(vec![result]).await?;
            tokio::task::yield_now().await;
        }
        let mut committed_state = scratch.state.read().await.clone();
        if complete_import {
            committed_state
                .tasks
                .get_mut(&lease.task_id().id())
                .ok_or_else(invalid_task_lease)?
                .control
                .record_import_commit(Utc::now())
                .map_err(invalid_contract_value)?;
        }
        self.state
            .commit_import(&generation, committed_state, lease)
            .await?;
        *references = next_references;
        Ok(())
    }

    async fn check_import_claim(&self, lease: &StorageTaskLease) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| {
                task.kind == StorageTaskKind::Import
                    && task.status.is_active()
                    && task.lease_matches(lease)
            })
            .ok_or_else(invalid_task_lease)?;
        if let Some(reason) = task.control.stop_reason(Utc::now()) {
            return Err(StorageError::task_stopped(reason));
        }
        Ok(())
    }
}
