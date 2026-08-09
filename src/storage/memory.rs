use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use crate::events::{Action, EventContext};
use crate::models::{
    ClassSelector, ClassSelectorKind, Collection, CollectionID, HubuumClass,
    NewCollectionWithAssignee, NewHubuumClass, ResolvedClassTarget, ResourceRevision,
    UpdateCollection, UpdateHubuumClass,
};

use super::{ClassStore, CollectionStore, StorageError};

const ROOT_COLLECTION_ID: i32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryCollectionEvent {
    pub(crate) collection_id: i32,
    pub(crate) action: Action,
    pub(crate) context: EventContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryClassEvent {
    pub(crate) class_id: i32,
    pub(crate) action: Action,
    pub(crate) context: EventContext,
}

struct MemoryState {
    next_collection_id: i32,
    next_class_id: i32,
    collections: BTreeMap<i32, Collection>,
    classes: BTreeMap<i32, HubuumClass>,
    events: Vec<MemoryCollectionEvent>,
    class_events: Vec<MemoryClassEvent>,
}

impl MemoryState {
    fn new() -> Self {
        let now = Utc::now().naive_utc();
        let root = Collection {
            id: ROOT_COLLECTION_ID,
            name: "root".to_string(),
            description: "Root collection".to_string(),
            created_at: now,
            updated_at: now,
            parent_collection_id: None,
            revision: ResourceRevision::INITIAL,
        };
        Self {
            next_collection_id: ROOT_COLLECTION_ID + 1,
            next_class_id: 1,
            collections: BTreeMap::from([(root.id, root)]),
            classes: BTreeMap::new(),
            events: Vec::new(),
            class_events: Vec::new(),
        }
    }

    fn collection(&self, id: CollectionID) -> Result<&Collection, StorageError> {
        self.collections
            .get(&id.id())
            .ok_or_else(|| StorageError::not_found(format!("Collection {} was not found", id.id())))
    }

    fn name_in_use(&self, name: &str, except_id: Option<i32>) -> bool {
        self.collections
            .values()
            .any(|collection| collection.name == name && Some(collection.id) != except_id)
    }

    fn class_name_in_use(&self, name: &str, except_id: Option<i32>) -> bool {
        self.classes
            .values()
            .any(|class| class.name == name && Some(class.id) != except_id)
    }

    fn class_for_selector(&self, selector: &ClassSelector) -> Result<&HubuumClass, StorageError> {
        match selector.kind() {
            ClassSelectorKind::ById(class_id) => self.classes.get(&class_id.id()),
            ClassSelectorKind::ByName(class_name) => self
                .classes
                .values()
                .find(|class| class.name == *class_name),
        }
        .ok_or_else(|| StorageError::not_found("Class was not found"))
    }

    fn class_target(&self, target: &ResolvedClassTarget) -> Result<&HubuumClass, StorageError> {
        let current = self.class_for_selector(target.selector())?;
        let resolved = target.class();
        if current.id != resolved.id
            || current.name != resolved.name
            || current.collection_id != resolved.collection_id
        {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        Ok(current)
    }

    fn record_event(&mut self, collection_id: i32, action: Action, context: &EventContext) {
        self.events.push(MemoryCollectionEvent {
            collection_id,
            action,
            context: context.clone(),
        });
    }

    fn record_class_event(&mut self, class_id: i32, action: Action, context: &EventContext) {
        self.class_events.push(MemoryClassEvent {
            class_id,
            action,
            context: context.clone(),
        });
    }
}

/// Deterministic collection adapter used by shared storage contract tests.
#[derive(Clone)]
pub(crate) struct MemoryStorage {
    state: Arc<RwLock<MemoryState>>,
}

impl MemoryStorage {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::new())),
        }
    }

    pub(crate) async fn events(&self) -> Vec<MemoryCollectionEvent> {
        self.state.read().await.events.clone()
    }

    pub(crate) async fn class_events(&self) -> Vec<MemoryClassEvent> {
        self.state.read().await.class_events.clone()
    }
}

#[async_trait]
impl CollectionStore for MemoryStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        self.state.read().await.collection(id).cloned()
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        let mut state = self.state.write().await;
        if state.name_in_use(&command.name, None) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists",
                command.name
            )));
        }
        let parent_id = command
            .parent_collection_id
            .map(CollectionID::id)
            .unwrap_or(ROOT_COLLECTION_ID);
        if !state.collections.contains_key(&parent_id) {
            return Err(StorageError::not_found(format!(
                "Parent collection {parent_id} was not found"
            )));
        }

        let id = state.next_collection_id;
        state.next_collection_id += 1;
        let now = Utc::now().naive_utc();
        let collection = Collection {
            id,
            name: command.name,
            description: command.description,
            created_at: now,
            updated_at: now,
            parent_collection_id: Some(parent_id),
            revision: ResourceRevision::INITIAL,
        };
        state.collections.insert(id, collection.clone());
        state.record_event(id, Action::Created, context);
        Ok(collection)
    }

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        let mut state = self.state.write().await;
        let current = state.collection(id)?.clone();
        if !changes.has_changes(&current) {
            return Ok(current);
        }
        if let Some(name) = changes.name.as_deref()
            && state.name_in_use(name, Some(id.id()))
        {
            return Err(StorageError::conflict(format!(
                "A collection named '{name}' already exists"
            )));
        }

        let collection = state
            .collections
            .get_mut(&id.id())
            .expect("collection existence was checked");
        if let Some(name) = changes.name {
            collection.name = name;
        }
        if let Some(description) = changes.description {
            collection.description = description;
        }
        collection.updated_at = Utc::now().naive_utc();
        collection.revision = collection
            .revision
            .checked_advance()
            .map_err(StorageError::from)?;
        let updated = collection.clone();
        state.record_event(id.id(), Action::Updated, context);
        Ok(updated)
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let collection = state.collection(id)?.clone();
        if collection.parent_collection_id.is_none() {
            return Err(StorageError::conflict(
                "The root collection cannot be deleted",
            ));
        }
        if state
            .collections
            .values()
            .any(|candidate| candidate.parent_collection_id == Some(id.id()))
        {
            return Err(StorageError::conflict(
                "Collections with child collections cannot be deleted",
            ));
        }
        state.collections.remove(&id.id());
        state
            .classes
            .retain(|_, class| class.collection_id != id.id());
        state.record_event(id.id(), Action::Deleted, context);
        Ok(())
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        let state = self.state.read().await;
        state.collection(id)?;
        let mut children = state
            .collections
            .values()
            .filter(|collection| collection.parent_collection_id == Some(id.id()))
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(children)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        let state = self.state.read().await;
        let mut parent_id = state.collection(id)?.parent_collection_id;
        let mut ancestors = Vec::new();
        while let Some(current_id) = parent_id {
            let ancestor = state.collections.get(&current_id).ok_or_else(|| {
                StorageError::not_found(format!("Ancestor collection {current_id} was not found"))
            })?;
            ancestors.push(ancestor.clone());
            parent_id = ancestor.parent_collection_id;
        }
        Ok(ancestors)
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        let mut state = self.state.write().await;
        let collection = state.collection(id)?.clone();
        if collection.parent_collection_id.is_none() {
            return Err(StorageError::conflict(
                "The root collection cannot be moved",
            ));
        }
        if collection.parent_collection_id == Some(new_parent_id.id()) {
            return Ok(collection);
        }
        if id == new_parent_id {
            return Err(StorageError::bad_request(
                "A collection cannot be moved under itself",
            ));
        }
        state.collection(new_parent_id)?;

        let mut ancestor_id = Some(new_parent_id.id());
        while let Some(current_id) = ancestor_id {
            if current_id == id.id() {
                return Err(StorageError::bad_request(
                    "A collection cannot be moved under one of its descendants",
                ));
            }
            ancestor_id = state
                .collections
                .get(&current_id)
                .and_then(|current| current.parent_collection_id);
        }

        let collection = state
            .collections
            .get_mut(&id.id())
            .expect("collection existence was checked");
        collection.parent_collection_id = Some(new_parent_id.id());
        collection.updated_at = Utc::now().naive_utc();
        collection.revision = collection
            .revision
            .checked_advance()
            .map_err(StorageError::from)?;
        let moved = collection.clone();
        state.record_event(id.id(), Action::Updated, context);
        Ok(moved)
    }
}

#[async_trait]
impl ClassStore for MemoryStorage {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError> {
        let state = self.state.read().await;
        let class = state.class_for_selector(&selector)?.clone();
        Ok(ResolvedClassTarget::new(selector, class))
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        command.validate_schema().map_err(StorageError::from)?;
        let mut state = self.state.write().await;
        if state.class_name_in_use(&command.name, None) {
            return Err(StorageError::conflict(format!(
                "A class named '{}' already exists",
                command.name
            )));
        }
        if !state.collections.contains_key(&command.collection_id) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                command.collection_id
            )));
        }

        let id = state.next_class_id;
        state.next_class_id += 1;
        let now = Utc::now().naive_utc();
        let class = HubuumClass {
            id,
            name: command.name,
            collection_id: command.collection_id,
            json_schema: command.json_schema,
            validate_schema: command.validate_schema.unwrap_or(false),
            description: command.description,
            created_at: now,
            updated_at: now,
            revision: ResourceRevision::INITIAL,
        };
        state.classes.insert(id, class.clone());
        state.record_class_event(id, Action::Created, context);
        Ok(class)
    }

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        let mut state = self.state.write().await;
        let current = state.class_target(target)?.clone();
        changes
            .validate_schema_update(&current)
            .map_err(StorageError::from)?;
        if !changes.has_changes(&current) {
            return Ok(current);
        }
        if let Some(name) = changes.name.as_deref()
            && state.class_name_in_use(name, Some(current.id))
        {
            return Err(StorageError::conflict(format!(
                "A class named '{name}' already exists"
            )));
        }
        if let Some(collection_id) = changes.collection_id
            && !state.collections.contains_key(&collection_id)
        {
            return Err(StorageError::not_found(format!(
                "Collection {collection_id} was not found"
            )));
        }

        let class = state
            .classes
            .get_mut(&current.id)
            .expect("class existence was checked");
        if let Some(name) = changes.name {
            class.name = name;
        }
        if let Some(collection_id) = changes.collection_id {
            class.collection_id = collection_id;
        }
        if let Some(json_schema) = changes.json_schema {
            class.json_schema = Some(json_schema);
        }
        if let Some(validate_schema) = changes.validate_schema {
            class.validate_schema = validate_schema;
        }
        if let Some(description) = changes.description {
            class.description = description;
        }
        class.updated_at = Utc::now().naive_utc();
        class.revision = class
            .revision
            .checked_advance()
            .map_err(StorageError::from)?;
        let updated = class.clone();
        state.record_class_event(updated.id, Action::Updated, context);
        Ok(updated)
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let class = state.class_target(target)?.clone();
        state.classes.remove(&class.id);
        state.record_class_event(class.id, Action::Deleted, context);
        Ok(())
    }
}
