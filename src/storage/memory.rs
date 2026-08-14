mod error;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use crate::errors::ApiError;
use crate::events::{Action, EventContext};
use crate::models::{
    ClassSelector, ClassSelectorKind, Collection, CollectionID, HubuumClass, HubuumClassRelation,
    HubuumClassRelationID, HubuumObject, HubuumObjectRelation, HubuumObjectRelationID,
    NewHubuumClass, NewHubuumObject, NewHubuumObjectRelation, ObjectDataPatchDocument,
    ObjectRelationCreateSelectorKind, ObjectRelationSelectorKind, ObjectSelector,
    ObjectSelectorKind, PreparedClassRelation, PreparedObjectRelation, ResolvedClassRelationTarget,
    ResolvedClassTarget, ResolvedObjectRelationTarget, ResolvedObjectTarget, ResourceRevision,
    UpdateCollection, UpdateHubuumClass, UpdateHubuumObject,
};
use crate::services::storage_boundary::{
    class_record_to_storage, class_relation_create_from_storage, collection_to_storage,
    object_from_storage, object_relation_create_selector_from_storage,
    object_relation_selector_from_storage, object_to_storage, prepared_class_relation_from_storage,
    prepared_class_relation_to_storage, prepared_object_relation_from_storage,
    prepared_object_relation_to_storage, resolved_class_from_storage,
    resolved_class_relation_from_storage, resolved_class_relation_to_storage,
    resolved_object_from_storage, resolved_object_relation_from_storage,
    resolved_object_relation_to_storage,
};

use super::{
    ClassRelationStore, ClassStore, CollectionStore, ObjectRelationStore, ObjectStore,
    StorageClassCreate, StorageClassRecord, StorageClassRelation, StorageClassRelationCreate,
    StorageClassSelector, StorageClassUpdate, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageError, StorageIdentity, StorageObject, StorageObjectCreate,
    StorageObjectDataPatch, StorageObjectRelation, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationSelector, StorageObjectSelector,
    StorageObjectUpdate, StoragePreparedClassRelation, StoragePreparedObjectRelation,
    StorageResolvedClass, StorageResolvedClassRelation, StorageResolvedObject,
    StorageResolvedObjectRelation,
};
use error::map_memory_error;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryObjectEvent {
    pub(crate) object_id: i32,
    pub(crate) action: Action,
    pub(crate) context: EventContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryClassRelationEvent {
    pub(crate) class_relation_id: i32,
    pub(crate) action: Action,
    pub(crate) context: EventContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryObjectRelationEvent {
    pub(crate) object_relation_id: i32,
    pub(crate) action: Action,
    pub(crate) context: EventContext,
}

struct MemoryState {
    next_collection_id: i32,
    next_class_id: i32,
    next_object_id: i32,
    next_class_relation_id: i32,
    next_object_relation_id: i32,
    collections: BTreeMap<i32, Collection>,
    classes: BTreeMap<i32, HubuumClass>,
    objects: BTreeMap<i32, HubuumObject>,
    class_relations: BTreeMap<i32, HubuumClassRelation>,
    object_relations: BTreeMap<i32, HubuumObjectRelation>,
    events: Vec<MemoryCollectionEvent>,
    class_events: Vec<MemoryClassEvent>,
    object_events: Vec<MemoryObjectEvent>,
    class_relation_events: Vec<MemoryClassRelationEvent>,
    object_relation_events: Vec<MemoryObjectRelationEvent>,
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
            next_object_id: 1,
            next_class_relation_id: 1,
            next_object_relation_id: 1,
            collections: BTreeMap::from([(root.id, root)]),
            classes: BTreeMap::new(),
            objects: BTreeMap::new(),
            class_relations: BTreeMap::new(),
            object_relations: BTreeMap::new(),
            events: Vec::new(),
            class_events: Vec::new(),
            object_events: Vec::new(),
            class_relation_events: Vec::new(),
            object_relation_events: Vec::new(),
        }
    }

    fn collection(&self, id: CollectionID) -> Result<&Collection, StorageError> {
        self.collections
            .get(&id.id())
            .ok_or_else(|| StorageError::not_found(format!("Collection {} was not found", id.id())))
    }

    fn collection_name_in_use(
        &self,
        parent_collection_id: i32,
        name: &str,
        except_id: Option<i32>,
    ) -> bool {
        self.collections.values().any(|collection| {
            collection.parent_collection_id == Some(parent_collection_id)
                && collection.name == name
                && Some(collection.id) != except_id
        })
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

    fn object_for_selector(
        &self,
        selector: &ObjectSelector,
    ) -> Result<(&HubuumClass, &HubuumObject), StorageError> {
        let (class, object) = match selector.kind() {
            ObjectSelectorKind::ById {
                class_id,
                object_id,
            } => {
                let class = self.classes.get(&class_id.id());
                let object = self
                    .objects
                    .get(&object_id.id())
                    .filter(|object| object.hubuum_class_id == class_id.id());
                (class, object)
            }
            ObjectSelectorKind::ByName {
                class_name,
                object_name,
            } => {
                let class = self
                    .classes
                    .values()
                    .find(|class| class.name == *class_name);
                let object = class.and_then(|class| {
                    self.objects.values().find(|object| {
                        object.hubuum_class_id == class.id && object.name == *object_name
                    })
                });
                (class, object)
            }
        };
        match (class, object) {
            (Some(class), Some(object)) => Ok((class, object)),
            _ => Err(StorageError::not_found(
                "Object was not found in the selected class",
            )),
        }
    }

    fn object_target(
        &self,
        target: &ResolvedObjectTarget,
    ) -> Result<(&HubuumClass, &HubuumObject), StorageError> {
        let (class, object) = self.object_for_selector(target.selector())?;
        let resolved_class = target.class();
        let resolved_object = target.object();
        if class.id != resolved_class.id
            || class.name != resolved_class.name
            || class.collection_id != resolved_class.collection_id
            || object.id != resolved_object.id
            || object.name != resolved_object.name
            || object.collection_id != resolved_object.collection_id
            || object.hubuum_class_id != resolved_object.hubuum_class_id
        {
            return Err(StorageError::not_found(
                "Object no longer matches the resolved route target",
            ));
        }
        Ok((class, object))
    }

    fn object_name_in_use(&self, class_id: i32, name: &str, except_id: Option<i32>) -> bool {
        self.objects.values().any(|object| {
            object.hubuum_class_id == class_id
                && object.name == name
                && Some(object.id) != except_id
        })
    }

    fn class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<&HubuumClassRelation, StorageError> {
        self.class_relations.get(&id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Class relation {} was not found", id.id()))
        })
    }

    fn prepared_class_relation_endpoints(
        &self,
        prepared: &PreparedClassRelation,
    ) -> Result<(&HubuumClass, &HubuumClass), StorageError> {
        let command = prepared.command();
        let from_class = self
            .classes
            .get(&command.from_hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to_class = self
            .classes
            .get(&command.to_hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        if from_class != prepared.from_class() || to_class != prepared.to_class() {
            return Err(StorageError::not_found(
                "Class relation endpoints no longer match the prepared target",
            ));
        }
        Ok((from_class, to_class))
    }

    fn class_relation_target(
        &self,
        target: &ResolvedClassRelationTarget,
    ) -> Result<&HubuumClassRelation, StorageError> {
        let relation_id =
            HubuumClassRelationID::new(target.relation().id).map_err(map_memory_error)?;
        let current = self.class_relation(relation_id)?;
        let from_class = self
            .classes
            .get(&current.from_hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to_class = self
            .classes
            .get(&current.to_hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        if current != target.relation()
            || from_class != target.from_class()
            || to_class != target.to_class()
        {
            return Err(StorageError::not_found(
                "Class relation no longer matches the resolved target",
            ));
        }
        Ok(current)
    }

    fn class_relation_pair_in_use(&self, from_class_id: i32, to_class_id: i32) -> bool {
        self.class_relations.values().any(|relation| {
            relation.from_hubuum_class_id == from_class_id
                && relation.to_hubuum_class_id == to_class_id
        })
    }

    fn resolved_class_relation(
        &self,
        relation: &HubuumClassRelation,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        let from_class = self
            .classes
            .get(&relation.from_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to_class = self
            .classes
            .get(&relation.to_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        ResolvedClassRelationTarget::new(relation.clone(), from_class, to_class)
            .map_err(map_memory_error)
    }

    fn direct_class_relation(
        &self,
        first_class_id: i32,
        second_class_id: i32,
    ) -> Result<&HubuumClassRelation, StorageError> {
        let lower_class_id = first_class_id.min(second_class_id);
        let higher_class_id = first_class_id.max(second_class_id);
        self.class_relations
            .values()
            .find(|relation| {
                relation.from_hubuum_class_id == lower_class_id
                    && relation.to_hubuum_class_id == higher_class_id
            })
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Class {first_class_id} is not related to class {second_class_id}"
                ))
            })
    }

    fn object_relation(
        &self,
        id: HubuumObjectRelationID,
    ) -> Result<&HubuumObjectRelation, StorageError> {
        self.object_relations.get(&id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Object relation {} was not found", id.id()))
        })
    }

    fn object_relation_by_pair(
        &self,
        first_object_id: i32,
        second_object_id: i32,
    ) -> Result<&HubuumObjectRelation, StorageError> {
        let lower_object_id = first_object_id.min(second_object_id);
        let higher_object_id = first_object_id.max(second_object_id);
        self.object_relations
            .values()
            .find(|relation| {
                relation.from_hubuum_object_id == lower_object_id
                    && relation.to_hubuum_object_id == higher_object_id
            })
            .ok_or_else(|| StorageError::not_found("Object relation was not found"))
    }

    fn object_relation_endpoints(
        &self,
        command: &NewHubuumObjectRelation,
    ) -> Result<(&HubuumObject, &HubuumObject), StorageError> {
        let from_object = self
            .objects
            .get(&command.from_hubuum_object_id)
            .ok_or_else(|| StorageError::not_found("From object was not found"))?;
        let to_object = self
            .objects
            .get(&command.to_hubuum_object_id)
            .ok_or_else(|| StorageError::not_found("To object was not found"))?;
        Ok((from_object, to_object))
    }

    fn object_relation_scope_matches(current: &HubuumObject, expected: &HubuumObject) -> bool {
        current.id == expected.id
            && current.collection_id == expected.collection_id
            && current.hubuum_class_id == expected.hubuum_class_id
    }

    fn validate_prepared_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
    ) -> Result<(), StorageError> {
        let (from_object, to_object) = self.object_relation_endpoints(prepared.command())?;
        if !Self::object_relation_scope_matches(from_object, prepared.from_object())
            || !Self::object_relation_scope_matches(to_object, prepared.to_object())
        {
            return Err(StorageError::not_found(
                "Object relation endpoints no longer match the prepared target",
            ));
        }
        let class_relation_id = HubuumClassRelationID::new(prepared.command().class_relation_id)
            .map_err(map_memory_error)?;
        let class_relation = self.class_relation(class_relation_id)?;
        if class_relation != prepared.class_relation().relation() {
            return Err(StorageError::not_found(
                "Class relation no longer matches the prepared object relation",
            ));
        }
        Ok(())
    }

    fn validate_resolved_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
    ) -> Result<(), StorageError> {
        let relation_id =
            HubuumObjectRelationID::new(target.relation().id).map_err(map_memory_error)?;
        let relation = self.object_relation(relation_id)?;
        let command = NewHubuumObjectRelation {
            from_hubuum_object_id: relation.from_hubuum_object_id,
            to_hubuum_object_id: relation.to_hubuum_object_id,
            class_relation_id: relation.class_relation_id,
        };
        let (from_object, to_object) = self.object_relation_endpoints(&command)?;
        if relation != target.relation()
            || !Self::object_relation_scope_matches(from_object, target.from_object())
            || !Self::object_relation_scope_matches(to_object, target.to_object())
        {
            return Err(StorageError::not_found(
                "Object relation no longer matches the resolved target",
            ));
        }
        Ok(())
    }

    fn object_relation_count(&self, class_relation_id: i32, object_id: i32) -> usize {
        self.object_relations
            .values()
            .filter(|relation| {
                relation.class_relation_id == class_relation_id
                    && (relation.from_hubuum_object_id == object_id
                        || relation.to_hubuum_object_id == object_id)
            })
            .count()
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

    fn record_object_event(&mut self, object_id: i32, action: Action, context: &EventContext) {
        self.object_events.push(MemoryObjectEvent {
            object_id,
            action,
            context: context.clone(),
        });
    }

    fn record_class_relation_event(
        &mut self,
        class_relation_id: i32,
        action: Action,
        context: &EventContext,
    ) {
        self.class_relation_events.push(MemoryClassRelationEvent {
            class_relation_id,
            action,
            context: context.clone(),
        });
    }

    fn record_object_relation_event(
        &mut self,
        object_relation_id: i32,
        action: Action,
        context: &EventContext,
    ) {
        self.object_relation_events.push(MemoryObjectRelationEvent {
            object_relation_id,
            action,
            context: context.clone(),
        });
    }
}

/// Deterministic collection adapter used by shared storage contract tests.
#[derive(Clone)]
pub(crate) struct MemoryStorageModel {
    state: Arc<RwLock<MemoryState>>,
}

impl MemoryStorageModel {
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

    pub(crate) async fn object_events(&self) -> Vec<MemoryObjectEvent> {
        self.state.read().await.object_events.clone()
    }

    pub(crate) async fn class_relation_events(&self) -> Vec<MemoryClassRelationEvent> {
        self.state.read().await.class_relation_events.clone()
    }

    pub(crate) async fn object_relation_events(&self) -> Vec<MemoryObjectRelationEvent> {
        self.state.read().await.object_relation_events.clone()
    }
}

impl StorageIdentity for MemoryStorageModel {
    fn storage_name(&self) -> &'static str {
        "memory_contract_model"
    }
}

#[async_trait]
impl CollectionStore for MemoryStorageModel {
    async fn get_collection(&self, id: i32) -> Result<StorageCollection, StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
        self.state
            .read()
            .await
            .collection(id)
            .cloned()
            .map(collection_to_storage)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        let mut state = self.state.write().await;
        let parent_id = command.parent_collection_id().unwrap_or(ROOT_COLLECTION_ID);
        if !state.collections.contains_key(&parent_id) {
            return Err(StorageError::not_found(format!(
                "Parent collection {parent_id} was not found"
            )));
        }
        if state.collection_name_in_use(parent_id, command.name(), None) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {parent_id}",
                command.name()
            )));
        }

        let id = state.next_collection_id;
        state.next_collection_id += 1;
        let now = Utc::now().naive_utc();
        let collection = Collection {
            id,
            name: command.name().to_string(),
            description: command.description().to_string(),
            created_at: now,
            updated_at: now,
            parent_collection_id: Some(parent_id),
            revision: ResourceRevision::INITIAL,
        };
        state.collections.insert(id, collection.clone());
        if let Some(context) = context {
            state.record_event(id, Action::Created, context);
        }
        Ok(collection_to_storage(collection))
    }

    async fn update_collection(
        &self,
        id: i32,
        changes: StorageCollectionUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
        let changes = UpdateCollection {
            name: changes.name().map(str::to_string),
            description: changes.description().map(str::to_string),
        };
        let mut state = self.state.write().await;
        let current = state.collection(id)?.clone();
        if !changes.has_changes(&current) {
            return Ok(collection_to_storage(current));
        }
        if let (Some(name), Some(parent_id)) =
            (changes.name.as_deref(), current.parent_collection_id)
            && state.collection_name_in_use(parent_id, name, Some(id.id()))
        {
            return Err(StorageError::conflict(format!(
                "A collection named '{name}' already exists under the same parent"
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
            .map_err(map_memory_error)?;
        let updated = collection.clone();
        if let Some(context) = context {
            state.record_event(id.id(), Action::Updated, context);
        }
        Ok(collection_to_storage(updated))
    }

    async fn delete_collection(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
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
        let deleted_class_ids = state
            .classes
            .values()
            .filter(|class| class.collection_id == id.id())
            .map(|class| class.id)
            .collect::<Vec<_>>();
        state
            .classes
            .retain(|_, class| class.collection_id != id.id());
        state
            .objects
            .retain(|_, object| object.collection_id != id.id());
        state.class_relations.retain(|_, relation| {
            !deleted_class_ids.contains(&relation.from_hubuum_class_id)
                && !deleted_class_ids.contains(&relation.to_hubuum_class_id)
        });
        let remaining_object_ids = state.objects.keys().copied().collect::<Vec<_>>();
        let remaining_class_relation_ids =
            state.class_relations.keys().copied().collect::<Vec<_>>();
        state.object_relations.retain(|_, relation| {
            remaining_object_ids.contains(&relation.from_hubuum_object_id)
                && remaining_object_ids.contains(&relation.to_hubuum_object_id)
                && remaining_class_relation_ids.contains(&relation.class_relation_id)
        });
        if let Some(context) = context {
            state.record_event(id.id(), Action::Deleted, context);
        }
        Ok(())
    }

    async fn collection_children(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
        let state = self.state.read().await;
        state.collection(id)?;
        let mut children = state
            .collections
            .values()
            .filter(|collection| collection.parent_collection_id == Some(id.id()))
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(children.into_iter().map(collection_to_storage).collect())
    }

    async fn collection_ancestors(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
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
        Ok(ancestors.into_iter().map(collection_to_storage).collect())
    }

    async fn move_collection(
        &self,
        id: i32,
        new_parent_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        let id = CollectionID::new(id).map_err(map_memory_error)?;
        let new_parent_id = CollectionID::new(new_parent_id).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        let collection = state.collection(id)?.clone();
        if collection.parent_collection_id.is_none() {
            return Err(StorageError::conflict(
                "The root collection cannot be moved",
            ));
        }
        if collection.parent_collection_id == Some(new_parent_id.id()) {
            return Ok(collection_to_storage(collection));
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
        if state.collection_name_in_use(new_parent_id.id(), &collection.name, Some(id.id())) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {}",
                collection.name,
                new_parent_id.id()
            )));
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
            .map_err(map_memory_error)?;
        let moved = collection.clone();
        if let Some(context) = context {
            state.record_event(id.id(), Action::Updated, context);
        }
        Ok(collection_to_storage(moved))
    }
}

#[async_trait]
impl ClassStore for MemoryStorageModel {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        let selector = match selector {
            StorageClassSelector::Id(id) => ClassSelector::by_id(
                crate::models::HubuumClassID::new(id).map_err(map_memory_error)?,
            ),
            StorageClassSelector::Name(name) => ClassSelector::by_name(name),
        };
        let state = self.state.read().await;
        let class = state.class_for_selector(&selector)?.clone();
        Ok(StorageResolvedClass::new(
            match selector.kind() {
                ClassSelectorKind::ById(id) => StorageClassSelector::Id(id.id()),
                ClassSelectorKind::ByName(name) => StorageClassSelector::Name(name.clone()),
            },
            class_record_to_storage(class),
        ))
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        let command = NewHubuumClass {
            name: command.name().to_string(),
            collection_id: command.collection_id(),
            json_schema: command.json_schema().cloned(),
            validate_schema: Some(command.validates_schema()),
            description: command.description().to_string(),
        };
        command.validate_schema().map_err(map_memory_error)?;
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
        if let Some(context) = context {
            state.record_class_event(id, Action::Created, context);
        }
        Ok(class_record_to_storage(class))
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        let target = resolved_class_from_storage(target.clone()).map_err(map_memory_error)?;
        let changes = UpdateHubuumClass {
            name: changes.name().map(str::to_string),
            collection_id: changes.collection_id(),
            json_schema: changes.json_schema().cloned(),
            validate_schema: changes.validate_schema(),
            description: changes.description().map(str::to_string),
        };
        let mut state = self.state.write().await;
        let current = state.class_target(&target)?.clone();
        changes
            .validate_schema_update(&current)
            .map_err(map_memory_error)?;
        if !changes.has_changes(&current) {
            return Ok(class_record_to_storage(current));
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
        class.revision = class.revision.checked_advance().map_err(map_memory_error)?;
        let updated = class.clone();
        if let Some(context) = context {
            state.record_class_event(updated.id, Action::Updated, context);
        }
        Ok(class_record_to_storage(updated))
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target = resolved_class_from_storage(target.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        let class = state.class_target(&target)?.clone();
        state.classes.remove(&class.id);
        state
            .objects
            .retain(|_, object| object.hubuum_class_id != class.id);
        state.class_relations.retain(|_, relation| {
            relation.from_hubuum_class_id != class.id && relation.to_hubuum_class_id != class.id
        });
        let remaining_object_ids = state.objects.keys().copied().collect::<Vec<_>>();
        let remaining_class_relation_ids =
            state.class_relations.keys().copied().collect::<Vec<_>>();
        state.object_relations.retain(|_, relation| {
            remaining_object_ids.contains(&relation.from_hubuum_object_id)
                && remaining_object_ids.contains(&relation.to_hubuum_object_id)
                && remaining_class_relation_ids.contains(&relation.class_relation_id)
        });
        if let Some(context) = context {
            state.record_class_event(class.id, Action::Deleted, context);
        }
        Ok(())
    }

    async fn class_names(&self, class_ids: Vec<i32>) -> Result<Vec<(i32, String)>, StorageError> {
        let mut class_ids = class_ids;
        if class_ids.iter().any(|id| *id <= 0) {
            return Err(StorageError::bad_request(
                "class ids must be greater than zero",
            ));
        }
        class_ids.sort_unstable();
        class_ids.dedup();
        let state = self.state.read().await;
        class_ids
            .into_iter()
            .map(|id| {
                state
                    .classes
                    .get(&id)
                    .map(|class| (id, class.name.clone()))
                    .ok_or_else(|| StorageError::not_found(format!("Class {id} was not found")))
            })
            .collect()
    }
}

#[async_trait]
impl ClassRelationStore for MemoryStorageModel {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        let command = class_relation_create_from_storage(&command)
            .map_err(map_memory_error)?
            .normalized()
            .map_err(map_memory_error)?;
        let state = self.state.read().await;
        let from_class = state
            .classes
            .get(&command.from_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to_class = state
            .classes
            .get(&command.to_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        PreparedClassRelation::new(command, from_class, to_class)
            .map(|prepared| prepared_class_relation_to_storage(&prepared))
            .map_err(map_memory_error)
    }

    async fn resolve_class_relation(
        &self,
        id: i32,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        let id = HubuumClassRelationID::new(id).map_err(map_memory_error)?;
        let state = self.state.read().await;
        let relation = state.class_relation(id)?.clone();
        let from_class = state
            .classes
            .get(&relation.from_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to_class = state
            .classes
            .get(&relation.to_hubuum_class_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        ResolvedClassRelationTarget::new(relation, from_class, to_class)
            .map(|target| resolved_class_relation_to_storage(&target))
            .map_err(map_memory_error)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        let prepared =
            prepared_class_relation_from_storage(prepared.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        state.prepared_class_relation_endpoints(&prepared)?;
        let command = prepared.command();
        if state
            .class_relation_pair_in_use(command.from_hubuum_class_id, command.to_hubuum_class_id)
        {
            return Err(StorageError::conflict(format!(
                "A relation between classes {} and {} already exists",
                command.from_hubuum_class_id, command.to_hubuum_class_id
            )));
        }

        let id = state.next_class_relation_id;
        state.next_class_relation_id += 1;
        let now = Utc::now().naive_utc();
        let relation = HubuumClassRelation {
            id,
            from_hubuum_class_id: command.from_hubuum_class_id,
            to_hubuum_class_id: command.to_hubuum_class_id,
            forward_template_alias: command.forward_template_alias.clone(),
            reverse_template_alias: command.reverse_template_alias.clone(),
            created_at: now,
            updated_at: now,
            from_max_relations: command.from_max_relations,
            to_max_relations: command.to_max_relations,
            revision: ResourceRevision::INITIAL,
        };
        state.class_relations.insert(id, relation.clone());
        if let Some(context) = context {
            state.record_class_relation_event(id, Action::Created, context);
        }
        ResolvedClassRelationTarget::new(
            relation,
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map(|target| resolved_class_relation_to_storage(&target))
        .map_err(map_memory_error)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target =
            resolved_class_relation_from_storage(target.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        let relation_id = state.class_relation_target(&target)?.id;
        state.class_relations.remove(&relation_id);
        state
            .object_relations
            .retain(|_, relation| relation.class_relation_id != relation_id);
        if let Some(context) = context {
            state.record_class_relation_event(relation_id, Action::Deleted, context);
        }
        Ok(())
    }

    async fn create_class_relation_from_command(
        &self,
        command: StorageClassRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRelation, StorageError> {
        let prepared = self.prepare_class_relation(command).await?;
        Ok(self
            .create_class_relation(&prepared, context)
            .await?
            .relation()
            .clone())
    }

    async fn delete_class_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target = self.resolve_class_relation(id).await?;
        self.delete_class_relation(&target, context).await
    }
}

#[async_trait]
impl ObjectRelationStore for MemoryStorageModel {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        let selector =
            object_relation_create_selector_from_storage(selector).map_err(map_memory_error)?;
        let state = self.state.read().await;
        let prepared = match selector.kind() {
            ObjectRelationCreateSelectorKind::Explicit(command) => {
                let command = command.clone().normalized().map_err(map_memory_error)?;
                let (from_object, to_object) = state.object_relation_endpoints(&command)?;
                let class_relation_id = HubuumClassRelationID::new(command.class_relation_id)
                    .map_err(map_memory_error)?;
                let class_relation = state.class_relation(class_relation_id)?;
                let class_relation = state.resolved_class_relation(class_relation)?;
                PreparedObjectRelation::new(
                    command,
                    from_object.clone(),
                    to_object.clone(),
                    class_relation,
                )
                .map_err(map_memory_error)?
            }
            ObjectRelationCreateSelectorKind::Between { from, to } => {
                let route_from_object = state
                    .objects
                    .get(&from.object_id().id())
                    .ok_or_else(|| StorageError::not_found("From object was not found"))?;
                let route_to_object = state
                    .objects
                    .get(&to.object_id().id())
                    .ok_or_else(|| StorageError::not_found("To object was not found"))?;
                if route_from_object.hubuum_class_id != from.class_id().id()
                    || route_to_object.hubuum_class_id != to.class_id().id()
                {
                    return Err(StorageError::not_found(
                        "Object was not found in the selected class",
                    ));
                }
                let class_relation =
                    state.direct_class_relation(from.class_id().id(), to.class_id().id())?;
                let class_relation = state.resolved_class_relation(class_relation)?;
                let command = NewHubuumObjectRelation {
                    from_hubuum_object_id: route_from_object.id,
                    to_hubuum_object_id: route_to_object.id,
                    class_relation_id: class_relation.relation().id,
                }
                .normalized()
                .map_err(map_memory_error)?;
                let (from_object, to_object) =
                    if route_from_object.id == command.from_hubuum_object_id {
                        (route_from_object.clone(), route_to_object.clone())
                    } else {
                        (route_to_object.clone(), route_from_object.clone())
                    };
                PreparedObjectRelation::new(command, from_object, to_object, class_relation)
                    .map_err(map_memory_error)?
            }
        };
        Ok(prepared_object_relation_to_storage(&prepared))
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        let selector = object_relation_selector_from_storage(selector).map_err(map_memory_error)?;
        let state = self.state.read().await;
        let relation = match selector.kind() {
            ObjectRelationSelectorKind::ById(relation_id) => {
                state.object_relation(*relation_id)?.to_owned()
            }
            ObjectRelationSelectorKind::Between { from, to } => {
                let route_from_object = state
                    .objects
                    .get(&from.object_id().id())
                    .ok_or_else(|| StorageError::not_found("From object was not found"))?;
                let route_to_object = state
                    .objects
                    .get(&to.object_id().id())
                    .ok_or_else(|| StorageError::not_found("To object was not found"))?;
                if route_from_object.hubuum_class_id != from.class_id().id()
                    || route_to_object.hubuum_class_id != to.class_id().id()
                {
                    return Err(StorageError::not_found(
                        "Object relation was not found for the selected classes",
                    ));
                }
                state
                    .object_relation_by_pair(from.object_id().id(), to.object_id().id())?
                    .to_owned()
            }
        };
        let command = NewHubuumObjectRelation {
            from_hubuum_object_id: relation.from_hubuum_object_id,
            to_hubuum_object_id: relation.to_hubuum_object_id,
            class_relation_id: relation.class_relation_id,
        };
        let (from_object, to_object) = state.object_relation_endpoints(&command)?;
        let class_relation_id =
            HubuumClassRelationID::new(relation.class_relation_id).map_err(map_memory_error)?;
        let class_relation =
            state.resolved_class_relation(state.class_relation(class_relation_id)?)?;
        ResolvedObjectRelationTarget::new(
            relation,
            from_object.clone(),
            to_object.clone(),
            class_relation,
        )
        .map(|target| resolved_object_relation_to_storage(&target))
        .map_err(map_memory_error)
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        let prepared =
            prepared_object_relation_from_storage(prepared.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        state.validate_prepared_object_relation(&prepared)?;
        let command = prepared.command();
        if state
            .object_relation_by_pair(command.from_hubuum_object_id, command.to_hubuum_object_id)
            .is_ok()
        {
            return Err(StorageError::conflict(format!(
                "A relation between objects {} and {} already exists",
                command.from_hubuum_object_id, command.to_hubuum_object_id
            )));
        }

        let class_relation = prepared.class_relation().relation();
        for object in [prepared.from_object(), prepared.to_object()] {
            let limit = if object.hubuum_class_id == class_relation.from_hubuum_class_id {
                class_relation.from_max_relations
            } else {
                class_relation.to_max_relations
            };
            if let Some(limit) = limit
                && state.object_relation_count(class_relation.id, object.id)
                    >= limit.value() as usize
            {
                return Err(StorageError::conflict(format!(
                    "Object relation cardinality exceeded: object {} is limited to {} relations by class relation {}",
                    object.id,
                    limit.value(),
                    class_relation.id
                )));
            }
        }

        let id = state.next_object_relation_id;
        state.next_object_relation_id += 1;
        let now = Utc::now().naive_utc();
        let relation = HubuumObjectRelation {
            id,
            from_hubuum_object_id: command.from_hubuum_object_id,
            to_hubuum_object_id: command.to_hubuum_object_id,
            class_relation_id: command.class_relation_id,
            created_at: now,
            updated_at: now,
            revision: ResourceRevision::INITIAL,
        };
        state.object_relations.insert(id, relation);
        if let Some(context) = context {
            state.record_object_relation_event(id, Action::Created, context);
        }
        ResolvedObjectRelationTarget::new(
            relation,
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map(|target| resolved_object_relation_to_storage(&target))
        .map_err(map_memory_error)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target =
            resolved_object_relation_from_storage(target.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        state.validate_resolved_object_relation(&target)?;
        let relation_id = target.relation().id;
        state.object_relations.remove(&relation_id);
        if let Some(context) = context {
            state.record_object_relation_event(relation_id, Action::Deleted, context);
        }
        Ok(())
    }

    async fn create_object_relation_from_command(
        &self,
        command: StorageObjectRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObjectRelation, StorageError> {
        let prepared = self
            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(command))
            .await?;
        Ok(self
            .create_object_relation(&prepared, context)
            .await?
            .relation()
            .clone())
    }

    async fn delete_object_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target = self
            .resolve_object_relation(StorageObjectRelationSelector::Id(id))
            .await?;
        self.delete_object_relation(&target, context).await
    }
}

#[async_trait]
impl ObjectStore for MemoryStorageModel {
    async fn get_object(&self, object_id: i32) -> Result<StorageResolvedObject, StorageError> {
        let object_id = crate::models::HubuumObjectID::new(object_id).map_err(map_memory_error)?;
        let state = self.state.read().await;
        let object = state
            .objects
            .get(&object_id.id())
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let class = state
            .classes
            .get(&object.hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        Ok(StorageResolvedObject::new(
            StorageObjectSelector::Ids {
                class_id: class.id,
                object_id: object.id,
            },
            class_record_to_storage(class.clone()),
            object_to_storage(object.clone()),
        ))
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        let selector = match selector {
            StorageObjectSelector::Ids {
                class_id,
                object_id,
            } => ObjectSelector::by_id(
                crate::models::HubuumClassID::new(class_id).map_err(map_memory_error)?,
                crate::models::HubuumObjectID::new(object_id).map_err(map_memory_error)?,
            ),
            StorageObjectSelector::Names {
                class_name,
                object_name,
            } => ObjectSelector::by_name(class_name, object_name),
        };
        let state = self.state.read().await;
        let (class, object) = state.object_for_selector(&selector)?;
        let selector = match selector.kind() {
            ObjectSelectorKind::ById {
                class_id,
                object_id,
            } => StorageObjectSelector::Ids {
                class_id: class_id.id(),
                object_id: object_id.id(),
            },
            ObjectSelectorKind::ByName {
                class_name,
                object_name,
            } => StorageObjectSelector::Names {
                class_name: class_name.clone(),
                object_name: object_name.clone(),
            },
        };
        Ok(StorageResolvedObject::new(
            selector,
            class_record_to_storage(class.clone()),
            object_to_storage(object.clone()),
        ))
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        let class = resolved_class_from_storage(class.clone()).map_err(map_memory_error)?;
        let command = NewHubuumObject {
            name: command.name().to_string(),
            collection_id: command.collection_id(),
            hubuum_class_id: command.class_id(),
            data: command.data().clone(),
            description: command.description().to_string(),
        };
        let mut state = self.state.write().await;
        let current_class = state.class_target(&class)?.clone();
        command
            .validate_for_class(&current_class)
            .map_err(map_memory_error)?;
        if state.object_name_in_use(current_class.id, &command.name, None) {
            return Err(StorageError::conflict(format!(
                "An object named '{}' already exists in class {}",
                command.name, current_class.id
            )));
        }

        let id = state.next_object_id;
        state.next_object_id += 1;
        let now = Utc::now().naive_utc();
        let object = HubuumObject {
            id,
            name: command.name,
            collection_id: command.collection_id,
            hubuum_class_id: command.hubuum_class_id,
            data: command.data,
            description: command.description,
            created_at: now,
            updated_at: now,
            revision: ResourceRevision::INITIAL,
        };
        state.objects.insert(id, object.clone());
        if let Some(context) = context {
            state.record_object_event(id, Action::Created, context);
        }
        Ok(object_to_storage(object))
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        let target = resolved_object_from_storage(target.clone()).map_err(map_memory_error)?;
        let changes = UpdateHubuumObject {
            name: changes.name().map(str::to_string),
            collection_id: changes.collection_id(),
            hubuum_class_id: changes.class_id(),
            data: changes.data().cloned(),
            description: changes.description().map(str::to_string),
        };
        let mut state = self.state.write().await;
        let (class, current) = state.object_target(&target)?;
        let class = class.clone();
        let current = current.clone();
        changes
            .validate_for_class(&current, &class)
            .map_err(map_memory_error)?;
        if !changes.has_changes(&current) {
            return Ok(object_to_storage(current));
        }
        if let Some(name) = changes.name.as_deref()
            && state.object_name_in_use(class.id, name, Some(current.id))
        {
            return Err(StorageError::conflict(format!(
                "An object named '{name}' already exists in class {}",
                class.id
            )));
        }

        let mut updated = current.merge_update(&changes);
        updated.updated_at = Utc::now().naive_utc();
        updated.revision = current
            .revision
            .checked_advance()
            .map_err(map_memory_error)?;
        state.objects.insert(updated.id, updated.clone());
        if let Some(context) = context {
            state.record_object_event(updated.id, Action::Updated, context);
        }
        Ok(object_to_storage(updated))
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageObject, StorageError> {
        let target = resolved_object_from_storage(target.clone()).map_err(map_memory_error)?;
        let patch = serde_json::from_value::<ObjectDataPatchDocument>(patch.document().clone())
            .map_err(ApiError::from)
            .map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        let (class, current) = state.object_target(&target)?;
        let class = class.clone();
        let current = current.clone();
        let patched_data = patch.apply(&current.data).map_err(map_memory_error)?;
        if class.validate_schema
            && let Some(schema) = class.json_schema.as_ref()
        {
            crate::utilities::json_schema::validate_json_value(schema, &patched_data)
                .map_err(map_memory_error)?;
        }
        if patched_data == current.data {
            return Ok(object_to_storage(current));
        }

        let mut updated = current.clone();
        updated.data = patched_data;
        updated.updated_at = Utc::now().naive_utc();
        updated.revision = current
            .revision
            .checked_advance()
            .map_err(map_memory_error)?;
        state.objects.insert(updated.id, updated.clone());
        state.record_object_event(updated.id, Action::Updated, context);
        Ok(object_to_storage(updated))
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        let target = resolved_object_from_storage(target.clone()).map_err(map_memory_error)?;
        let mut state = self.state.write().await;
        let (_, object) = state.object_target(&target)?;
        let object_id = object.id;
        state.objects.remove(&object_id);
        state.object_relations.retain(|_, relation| {
            relation.from_hubuum_object_id != object_id && relation.to_hubuum_object_id != object_id
        });
        if let Some(context) = context {
            state.record_object_event(object_id, Action::Deleted, context);
        }
        Ok(())
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        let object = object_from_storage(object).map_err(map_memory_error)?;
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&object.hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        NewHubuumObject {
            name: object.name,
            collection_id: object.collection_id,
            hubuum_class_id: object.hubuum_class_id,
            data: object.data,
            description: object.description,
        }
        .validate_for_class(class)
        .map_err(map_memory_error)
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        let command = NewHubuumObject {
            name: command.name().to_string(),
            collection_id: command.collection_id(),
            hubuum_class_id: command.class_id(),
            data: command.data().clone(),
            description: command.description().to_string(),
        };
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&command.hubuum_class_id)
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        command.validate_for_class(class).map_err(map_memory_error)
    }

    async fn validate_object_update(
        &self,
        object_id: i32,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        let object_id = crate::models::HubuumObjectID::new(object_id).map_err(map_memory_error)?;
        let changes = UpdateHubuumObject {
            name: changes.name().map(str::to_string),
            collection_id: changes.collection_id(),
            hubuum_class_id: changes.class_id(),
            data: changes.data().cloned(),
            description: changes.description().map(str::to_string),
        };
        let state = self.state.read().await;
        let object = state
            .objects
            .get(&object_id.id())
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let class_id = changes.hubuum_class_id.unwrap_or(object.hubuum_class_id);
        let class = state
            .classes
            .get(&class_id)
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        changes
            .validate_for_class(object, class)
            .map_err(map_memory_error)
    }
}
