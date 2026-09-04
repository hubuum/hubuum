use super::*;
use crate::execution::enforce_memory_revision_precondition;

#[async_trait]
impl CollectionStorage for MemoryStorage {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        self.state
            .read()
            .await
            .collections
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("Collection {} was not found", id.id())))
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let parent_id = command
            .parent_collection_id()
            .unwrap_or(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"));
        if !state.collections.contains_key(&parent_id.id()) {
            return Err(StorageError::not_found(format!(
                "Parent collection {} was not found",
                parent_id.id()
            )));
        }
        if state.collections.values().any(|collection| {
            collection.parent_collection_id() == Some(parent_id)
                && collection.name() == command.name()
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {}",
                command.name(),
                parent_id.id()
            )));
        }
        let id = state.next_collection_id;
        state.next_collection_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let collection = StorageCollection::try_new(
            metadata,
            command.name(),
            command.description(),
            Some(parent_id),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document =
            AuditDocument::builder(format!("Created collection '{}'", collection.name()))
                .after(collection.audit_snapshot())
                .metadata(serde_json::json!({"owner_group_id": command.owner_group_id().id()}))
                .try_build()
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id,
            Some(collection.name()),
            Some(collection.id()),
            Action::Created,
            context,
            document,
            None,
            Some(collection.revision()),
        )?;
        state.collections.insert(id, collection.clone());
        state.append_history(
            MemoryHistoryValue::Collection(collection.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(collection, receipt))
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        enforce_memory_revision_precondition(
            StorageRevisionTarget::Collection(id),
            current.revision(),
        )?;
        let name = changes.name().unwrap_or(current.name());
        let description = changes.description().unwrap_or(current.description());
        if name == current.name() && description == current.description() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state.collections.values().any(|collection| {
            collection.id() != id
                && collection.parent_collection_id() == current.parent_collection_id()
                && collection.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{name}' already exists under the same parent"
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated =
            StorageCollection::try_new(metadata, name, description, current.parent_collection_id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!("Updated collection '{}'", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(updated.name()),
            Some(updated.id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.collections.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Collection(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        if id.id() == ROOT_COLLECTION_ID {
            return Err(StorageError::conflict(
                "The root collection cannot be deleted",
            ));
        }
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        if state
            .collections
            .values()
            .any(|collection| collection.parent_collection_id() == Some(id))
        {
            return Err(StorageError::conflict(
                "Collections with child collections cannot be deleted",
            ));
        }
        let document = AuditDocument::builder(format!("Deleted collection '{}'", current.name()))
            .before(current.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(current.name()),
            Some(id),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Collection(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.collections.remove(&id.id());
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let state = self.state.read().await;
        if !state.collections.contains_key(&id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                id.id()
            )));
        }
        let mut children = state
            .collections
            .values()
            .filter(|collection| collection.parent_collection_id() == Some(id))
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(children)
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let state = self.state.read().await;
        let mut current = state
            .collections
            .get(&id.id())
            .ok_or_else(|| {
                StorageError::not_found(format!("Collection {} was not found", id.id()))
            })?
            .parent_collection_id();
        let mut ancestors = Vec::new();
        while let Some(parent_id) = current {
            let parent = state.collections.get(&parent_id.id()).ok_or_else(|| {
                StorageError::backend_failure(format!(
                    "Collection {} references a missing parent {}",
                    id.id(),
                    parent_id.id()
                ))
            })?;
            ancestors.push(parent.clone());
            current = parent.parent_collection_id();
        }
        Ok(ancestors)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        if id.id() == ROOT_COLLECTION_ID {
            return Err(StorageError::conflict(
                "The root collection cannot be moved",
            ));
        }
        if !state.collections.contains_key(&new_parent_id.id()) {
            return Err(StorageError::not_found(format!(
                "Parent collection {} was not found",
                new_parent_id.id()
            )));
        }
        if current.parent_collection_id() == Some(new_parent_id) {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let mut ancestor_id = Some(new_parent_id);
        while let Some(candidate) = ancestor_id {
            if candidate == id {
                return Err(StorageError::invalid_input(
                    "A collection cannot be moved under itself or a descendant",
                ));
            }
            ancestor_id = state
                .collections
                .get(&candidate.id())
                .and_then(StorageCollection::parent_collection_id);
        }
        if state.collections.values().any(|collection| {
            collection.id() != id
                && collection.parent_collection_id() == Some(new_parent_id)
                && collection.name() == current.name()
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {}",
                current.name(),
                new_parent_id.id()
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let moved = StorageCollection::try_new(
            metadata,
            current.name(),
            current.description(),
            Some(new_parent_id),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!("Moved collection '{}'", moved.name()))
            .before(current.audit_snapshot())
            .after(moved.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(moved.name()),
            Some(id),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(moved.revision()),
        )?;
        state.collections.insert(id.id(), moved.clone());
        state.append_history(
            MemoryHistoryValue::Collection(moved.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(moved, receipt))
    }
}

#[async_trait]
impl ClassStorage for MemoryStorage {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        let state = self.state.read().await;
        let class = match &selector {
            StorageClassSelector::Id(id) => state.classes.get(&id.id()),
            StorageClassSelector::Name(name) => {
                state.classes.values().find(|class| class.name() == name)
            }
        }
        .cloned()
        .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        StorageResolvedClass::try_new(selector, class)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        let mut state = self.state.write().await;
        if !state
            .collections
            .contains_key(&command.collection_id().id())
        {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                command.collection_id().id()
            )));
        }
        if state
            .classes
            .values()
            .any(|class| class.name() == command.name())
        {
            return Err(StorageError::conflict(format!(
                "A class named '{}' already exists",
                command.name()
            )));
        }
        let id = state.next_class_id;
        state.next_class_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let class = StorageClass::builder(
            metadata,
            command.name(),
            command.collection_id(),
            command.description(),
        )
        .schema_policy(command.schema_policy().clone())
        .build();
        let document = AuditDocument::builder(format!("Class '{}' created", class.name()))
            .after(class.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id,
            Some(class.name()),
            Some(class.collection_id()),
            Action::Created,
            context,
            document,
            None,
            Some(class.revision()),
        )?;
        state.classes.insert(id, class.clone());
        state.append_history(
            MemoryHistoryValue::Class(class.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(class, receipt))
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.class().id();
        let current = state
            .classes
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        if &current != target.class() {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        let name = changes.name().unwrap_or(current.name());
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let schema_policy = changes
            .resolve_schema_policy(current.schema_policy())
            .map_err(StorageValidationError::into_request_error)?;
        let description = changes.description().unwrap_or(current.description());
        if name == current.name()
            && collection_id == current.collection_id()
            && &schema_policy == current.schema_policy()
            && description == current.description()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state
            .classes
            .values()
            .any(|class| class.id() != id && class.name() == name)
        {
            return Err(StorageError::conflict(format!(
                "A class named '{name}' already exists"
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated = StorageClass::builder(metadata, name, collection_id, description)
            .schema_policy(schema_policy)
            .build();
        let document = AuditDocument::builder(format!("Class '{}' updated", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id.id(),
            Some(updated.name()),
            Some(updated.collection_id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.classes.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Class(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.class().id();
        let current = state
            .classes
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        if &current != target.class() {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        let document = AuditDocument::builder(format!("Class '{}' deleted", current.name()))
            .before(current.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id.id(),
            Some(current.name()),
            Some(current.collection_id()),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Class(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.classes.remove(&id.id());
        state.objects.retain(|_, object| object.class_id() != id);
        state
            .class_relations
            .retain(|_, relation| relation.from_class_id() != id && relation.to_class_id() != id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let state = self.state.read().await;
        let class_ids = class_ids.into_iter().collect::<BTreeSet<_>>();
        let rows = class_ids
            .iter()
            .filter_map(|id| {
                state
                    .classes
                    .get(&id.id())
                    .map(|class| (*id, class.name().to_string()))
            })
            .collect::<Vec<_>>();
        if rows.len() != class_ids.len() {
            return Err(StorageError::not_found(
                "One or more requested classes were not found",
            ));
        }
        Ok(rows)
    }
}

#[async_trait]
impl ObjectStorage for MemoryStorage {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        let state = self.state.read().await;
        let object = state
            .objects
            .get(&object_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let class = state
            .classes
            .get(&object.class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object references a missing class"))?;
        StorageResolvedObject::try_new(
            StorageObjectSelector::Ids {
                class_id: class.id(),
                object_id,
            },
            class,
            object,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        let state = self.state.read().await;
        let (class, object) = match &selector {
            StorageObjectSelector::Ids {
                class_id,
                object_id,
            } => {
                let class = state.classes.get(&class_id.id());
                let object = state
                    .objects
                    .get(&object_id.id())
                    .filter(|object| object.class_id() == *class_id);
                (class, object)
            }
            StorageObjectSelector::Names {
                class_name,
                object_name,
            } => {
                let class = state
                    .classes
                    .values()
                    .find(|class| class.name() == class_name);
                let object = class.and_then(|class| {
                    state.objects.values().find(|object| {
                        object.class_id() == class.id() && object.name() == object_name
                    })
                });
                (class, object)
            }
        };
        StorageResolvedObject::try_new(
            selector,
            class
                .cloned()
                .ok_or_else(|| StorageError::not_found("Object class was not found"))?,
            object
                .cloned()
                .ok_or_else(|| StorageError::not_found("Object was not found"))?,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        if class.class().id() != command.class_id()
            || class.class().collection_id() != command.collection_id()
        {
            return Err(StorageError::invalid_input(
                "Object class and collection must match the resolved class",
            ));
        }
        if class.class().validates_schema()
            && let Some(schema) = class.class().json_schema()
        {
            validate_json_value(schema, command.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        let mut state = self.state.write().await;
        if state.classes.get(&command.class_id().id()) != Some(class.class()) {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        if state.objects.values().any(|object| {
            object.class_id() == command.class_id() && object.name() == command.name()
        }) {
            return Err(StorageError::conflict(format!(
                "An object named '{}' already exists in class {}",
                command.name(),
                command.class_id().id()
            )));
        }
        let id = state.next_object_id;
        state.next_object_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let object = StorageObject::new(
            metadata,
            command.name(),
            command.collection_id(),
            command.class_id(),
            command.data().clone(),
            command.description(),
        );
        let document = AuditDocument::builder(format!("Object '{}' created", object.name()))
            .after(object.audit_snapshot())
            .metadata(serde_json::json!({"class_id": object.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id,
            Some(object.name()),
            Some(object.collection_id()),
            Action::Created,
            context,
            document,
            None,
            Some(object.revision()),
        )?;
        state.objects.insert(id, object.clone());
        state.append_history(
            MemoryHistoryValue::Object(object.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(object, receipt))
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.object().id();
        let current = state
            .objects
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        if &current != target.object() {
            return Err(StorageError::not_found(
                "Object no longer matches the resolved route target",
            ));
        }
        let name = changes.name().unwrap_or(current.name());
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let class_id = changes.class_id().unwrap_or(current.class_id());
        let data = changes
            .data()
            .cloned()
            .unwrap_or_else(|| current.data().clone());
        let description = changes.description().unwrap_or(current.description());
        if name == current.name()
            && collection_id == current.collection_id()
            && class_id == current.class_id()
            && data == *current.data()
            && description == current.description()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let class = state
            .classes
            .get(&class_id.id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != collection_id {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, &data)
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        if state.objects.values().any(|object| {
            object.id() != id && object.class_id() == class_id && object.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "An object named '{name}' already exists in class {}",
                class_id.id()
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated =
            StorageObject::new(metadata, name, collection_id, class_id, data, description);
        let document = AuditDocument::builder(format!("Object '{}' updated", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .metadata(serde_json::json!({"class_id": updated.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id.id(),
            Some(updated.name()),
            Some(updated.collection_id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.objects.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Object(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        let patched = patch.apply(target.object().data())?;
        self.update_object(
            target,
            StorageObjectUpdate::builder().data(Some(patched)).build(),
            context,
        )
        .await
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.object().id();
        let current = state
            .objects
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        if &current != target.object() {
            return Err(StorageError::not_found(
                "Object no longer matches the resolved route target",
            ));
        }
        let document = AuditDocument::builder(format!("Object '{}' deleted", current.name()))
            .before(current.audit_snapshot())
            .metadata(serde_json::json!({"class_id": current.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id.id(),
            Some(current.name()),
            Some(current.collection_id()),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Object(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.objects.remove(&id.id());
        state
            .object_relations
            .retain(|_, relation| relation.from_object_id() != id && relation.to_object_id() != id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&object.class_id().id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != object.collection_id() {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, object.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&command.class_id().id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != command.collection_id() {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, command.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let current = state
            .objects
            .get(&object_id.id())
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let class_id = changes.class_id().unwrap_or(current.class_id());
        let data = changes.data().unwrap_or(current.data());
        let class = state
            .classes
            .get(&class_id.id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != collection_id {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, data)
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClassRelationStorage for MemoryStorage {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        let state = self.state.read().await;
        let from = state
            .classes
            .get(&command.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to = state
            .classes
            .get(&command.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        StoragePreparedClassRelation::try_new(command, from, to)
            .map_err(|error| StorageError::invalid_input(error.to_string()))
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        let state = self.state.read().await;
        let relation = state
            .class_relations
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        let from = state
            .classes
            .get(&relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to = state
            .classes
            .get(&relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        StorageResolvedClassRelation::try_new(relation, from, to)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError> {
        let mut state = self.state.write().await;
        let command = prepared.command();
        if state.classes.get(&prepared.from_class().id().id()) != Some(prepared.from_class())
            || state.classes.get(&prepared.to_class().id().id()) != Some(prepared.to_class())
        {
            return Err(StorageError::not_found(
                "Class relation endpoint no longer matches its prepared value",
            ));
        }
        if state.class_relations.values().any(|relation| {
            relation.from_class_id() == command.from_class_id()
                && relation.to_class_id() == command.to_class_id()
        }) {
            return Err(StorageError::conflict(
                "A class relation already exists between these classes",
            ));
        }
        let id = state.next_class_relation_id;
        state.next_class_relation_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let relation =
            StorageClassRelation::try_new(metadata, command.from_class_id(), command.to_class_id())
                .and_then(|relation| {
                    relation.try_with_template_aliases(
                        command.forward_template_alias().map(ToOwned::to_owned),
                        command.reverse_template_alias().map(ToOwned::to_owned),
                    )
                })
                .and_then(|relation| {
                    relation.try_with_relation_limits(
                        command.from_max_relations(),
                        command.to_max_relations(),
                    )
                })
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let resolved = StorageResolvedClassRelation::try_new(
            relation.clone(),
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!(
            "Class relation {} -> {} created",
            relation.from_class_id().id(),
            relation.to_class_id().id()
        ))
        .after(relation.audit_snapshot())
        .metadata(serde_json::json!({
            "from_class_id": relation.from_class_id().id(),
            "to_class_id": relation.to_class_id().id(),
            "related_collection_ids": [
                prepared.from_class().collection_id().id(),
                prepared.to_class().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ClassRelation,
            id,
            None,
            None,
            Action::Created,
            context,
            document,
            None,
            Some(relation.metadata().revision()),
        )?;
        state.class_relations.insert(id, relation);
        Ok(StorageMutationOutcome::committed(resolved, receipt))
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let relation_id = ClassRelationId::from(target.relation().metadata().id());
        let current = state
            .class_relations
            .get(&relation_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        if &current != target.relation() {
            return Err(StorageError::not_found(
                "Class relation no longer matches the resolved target",
            ));
        }
        let document = AuditDocument::builder(format!(
            "Class relation {} -> {} deleted",
            current.from_class_id().id(),
            current.to_class_id().id()
        ))
        .before(current.audit_snapshot())
        .metadata(serde_json::json!({
            "from_class_id": current.from_class_id().id(),
            "to_class_id": current.to_class_id().id(),
            "related_collection_ids": [
                target.from_class().collection_id().id(),
                target.to_class().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ClassRelation,
            relation_id.id(),
            None,
            None,
            Action::Deleted,
            context,
            document,
            Some(current.metadata().revision()),
            None,
        )?;
        state.class_relations.remove(&relation_id.id());
        state
            .object_relations
            .retain(|_, relation| relation.class_relation_id() != relation_id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ObjectRelationStorage for MemoryStorage {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        let state = self.state.read().await;
        let command = match selector {
            StorageObjectRelationCreateSelector::Explicit(command) => command,
            StorageObjectRelationCreateSelector::Between { from, to } => {
                let (from_object_id, to_object_id) = if from.object_id() < to.object_id() {
                    (from.object_id(), to.object_id())
                } else {
                    (to.object_id(), from.object_id())
                };
                let class_relation = state
                    .class_relations
                    .values()
                    .find(|relation| {
                        (relation.from_class_id() == from.class_id()
                            && relation.to_class_id() == to.class_id())
                            || (relation.from_class_id() == to.class_id()
                                && relation.to_class_id() == from.class_id())
                    })
                    .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
                StorageObjectRelationCreate::new(
                    from_object_id,
                    to_object_id,
                    ClassRelationId::from(class_relation.metadata().id()),
                )
            }
        };
        let from_object = state
            .objects
            .get(&command.from_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("From object was not found"))?;
        let to_object = state
            .objects
            .get(&command.to_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("To object was not found"))?;
        let relation = state
            .class_relations
            .get(&command.class_relation_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        let from_class = state
            .classes
            .get(&relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to_class = state
            .classes
            .get(&relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let class_relation = StorageResolvedClassRelation::try_new(relation, from_class, to_class)
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        StoragePreparedObjectRelation::try_new(command, from_object, to_object, class_relation)
            .map_err(|error| StorageError::invalid_input(error.to_string()))
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        let state = self.state.read().await;
        let relation = match selector {
            StorageObjectRelationSelector::Id(id) => state.object_relations.get(&id.id()),
            StorageObjectRelationSelector::Between { from, to } => {
                let (from_id, to_id) = if from.object_id() < to.object_id() {
                    (from.object_id(), to.object_id())
                } else {
                    (to.object_id(), from.object_id())
                };
                state.object_relations.values().find(|relation| {
                    relation.from_object_id() == from_id && relation.to_object_id() == to_id
                })
            }
        }
        .cloned()
        .ok_or_else(|| StorageError::not_found("Object relation was not found"))?;
        let from_object = state
            .objects
            .get(&relation.from_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object relation endpoint is missing"))?;
        let to_object = state
            .objects
            .get(&relation.to_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object relation endpoint is missing"))?;
        let class_relation = state
            .class_relations
            .get(&relation.class_relation_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation is missing"))?;
        let from_class = state
            .classes
            .get(&class_relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to_class = state
            .classes
            .get(&class_relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let class_relation =
            StorageResolvedClassRelation::try_new(class_relation, from_class, to_class)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        StorageResolvedObjectRelation::try_new(relation, from_object, to_object, class_relation)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        let mut state = self.state.write().await;
        let command = *prepared.command();
        if state.objects.get(&prepared.from_object().id().id()) != Some(prepared.from_object())
            || state.objects.get(&prepared.to_object().id().id()) != Some(prepared.to_object())
            || state.class_relations.get(&command.class_relation_id().id())
                != Some(prepared.class_relation().relation())
        {
            return Err(StorageError::not_found(
                "Object relation aggregate no longer matches its prepared value",
            ));
        }
        if state.object_relations.values().any(|relation| {
            relation.from_object_id() == command.from_object_id()
                && relation.to_object_id() == command.to_object_id()
        }) {
            return Err(StorageError::conflict(
                "An object relation already exists between these objects",
            ));
        }
        let id = state.next_object_relation_id;
        state.next_object_relation_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let relation = StorageObjectRelation::try_new(
            metadata,
            command.from_object_id(),
            command.to_object_id(),
            command.class_relation_id(),
        )
        .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let resolved = StorageResolvedObjectRelation::try_new(
            relation.clone(),
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!(
            "Object relation {} -> {} created",
            relation.from_object_id().id(),
            relation.to_object_id().id()
        ))
        .after(relation.audit_snapshot())
        .metadata(serde_json::json!({
            "class_relation_id": relation.class_relation_id().id(),
            "from_object_id": prepared.from_object().id().id(),
            "to_object_id": prepared.to_object().id().id(),
            "from_class_id": prepared.from_object().class_id().id(),
            "to_class_id": prepared.to_object().class_id().id(),
            "related_collection_ids": [
                prepared.from_object().collection_id().id(),
                prepared.to_object().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ObjectRelation,
            id,
            None,
            None,
            Action::Created,
            context,
            document,
            None,
            Some(relation.metadata().revision()),
        )?;
        state.object_relations.insert(id, relation);
        Ok(StorageMutationOutcome::committed(resolved, receipt))
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let relation_id = ObjectRelationId::from(target.relation().metadata().id());
        let current = state
            .object_relations
            .get(&relation_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object relation was not found"))?;
        if &current != target.relation() {
            return Err(StorageError::not_found(
                "Object relation no longer matches the resolved target",
            ));
        }
        let document = AuditDocument::builder(format!(
            "Object relation {} -> {} deleted",
            current.from_object_id().id(),
            current.to_object_id().id()
        ))
        .before(current.audit_snapshot())
        .metadata(serde_json::json!({
            "class_relation_id": current.class_relation_id().id(),
            "from_object_id": target.from_object().id().id(),
            "to_object_id": target.to_object().id().id(),
            "from_class_id": target.from_object().class_id().id(),
            "to_class_id": target.to_object().class_id().id(),
            "related_collection_ids": [
                target.from_object().collection_id().id(),
                target.to_object().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ObjectRelation,
            relation_id.id(),
            None,
            None,
            Action::Deleted,
            context,
            document,
            Some(current.metadata().revision()),
            None,
        )?;
        state.object_relations.remove(&relation_id.id());
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}
