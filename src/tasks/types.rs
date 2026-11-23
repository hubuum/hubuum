use std::collections::{HashMap, HashSet};

use super::import_authorization::ImportAuthorization;
use crate::models::{GroupKey, TaskResultCounts, TaskStatus};
use crate::storage::StorageImportResult;

pub(super) use crate::storage::ApplicationImportOperation as PlannedExecution;

#[derive(Clone, Debug)]
pub(super) struct CollectionResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) description: String,
    pub(super) parent_collection_id: Option<i32>,
    pub(super) exists_in_db: bool,
}

#[derive(Clone, Debug)]
pub(super) struct ClassResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) collection_id: i32,
    pub(super) json_schema: Option<serde_json::Value>,
    pub(super) validate_schema: bool,
    pub(super) exists_in_db: bool,
}

#[derive(Clone, Debug)]
pub(super) struct ObjectResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) collection_id: i32,
    pub(super) class_id: i32,
    pub(super) exists_in_db: bool,
}

/// A prior database miss and an unqueried name require different planning decisions.
#[derive(Debug)]
pub(super) enum CacheLookup<T> {
    Found(T),
    Missing,
    Unknown,
}

pub(super) struct ResolutionCache<T> {
    by_key: HashMap<(i32, String), T>,
    by_ref: HashMap<String, T>,
    missing: HashSet<(i32, String)>,
    planned: HashSet<(i32, String)>,
}

impl<T> Default for ResolutionCache<T> {
    fn default() -> Self {
        Self {
            by_key: HashMap::new(),
            by_ref: HashMap::new(),
            missing: HashSet::new(),
            planned: HashSet::new(),
        }
    }
}

impl<T: Clone> ResolutionCache<T> {
    pub(super) fn reserve(&mut self, key: (i32, String)) -> bool {
        self.planned.insert(key)
    }

    pub(super) fn lookup(&self, key: &(i32, String)) -> CacheLookup<&T> {
        self.lookup_where(key, |_| true)
    }

    // A planned value can satisfy a reference even when storage has already reported a miss.
    pub(super) fn lookup_where(
        &self,
        key: &(i32, String),
        accepts: impl FnOnce(&T) -> bool,
    ) -> CacheLookup<&T> {
        if let Some(value) = self.by_key.get(key).filter(|value| accepts(value)) {
            CacheLookup::Found(value)
        } else if self.missing.contains(key) {
            CacheLookup::Missing
        } else {
            CacheLookup::Unknown
        }
    }

    pub(super) fn by_ref(&self, reference: &str) -> Option<&T> {
        self.by_ref.get(reference)
    }

    pub(super) fn record_missing(&mut self, key: (i32, String)) {
        self.missing.insert(key);
    }

    fn remember(&mut self, key: (i32, String), reference: Option<String>, value: T) {
        if let Some(reference) = reference {
            self.by_ref.insert(reference, value.clone());
        }
        self.by_key.insert(key, value);
    }
}

/// Planned creations supersede earlier database misses for the same normalized endpoints.
#[derive(Default)]
pub(super) struct RelationExistence(HashMap<(i32, i32), bool>);

impl RelationExistence {
    pub(super) async fn exists(
        &mut self,
        pair: (i32, i32),
        lookup: impl Future<Output = Result<bool, String>>,
    ) -> Result<bool, String> {
        if let Some(&exists) = self.0.get(&pair) {
            return Ok(exists);
        }
        if pair.0 <= 0 || pair.1 <= 0 {
            // Planner-local IDs have no corresponding storage row.
            return Ok(false);
        }
        let exists = lookup.await?;
        self.0.insert(pair, exists);
        Ok(exists)
    }

    pub(super) fn record_creation(&mut self, pair: (i32, i32)) {
        self.0.insert(pair, true);
    }
}

#[derive(Default)]
pub(super) struct PlanningState {
    last_virtual_id: i32,
    pub(super) authorization: ImportAuthorization,
    planned_collection_keys: HashSet<(i32, String)>,
    planned_groups: HashMap<String, HashSet<String>>,
    collections_by_ref: HashMap<String, CollectionResolution>,
    collections_by_name: HashMap<String, Vec<CollectionResolution>>,
    collections_by_parent_name: HashMap<(Option<i32>, String), CollectionResolution>,
    collections_by_id: HashMap<i32, CollectionResolution>,
    pub(super) classes: ResolutionCache<ClassResolution>,
    pub(super) objects: ResolutionCache<ObjectResolution>,
    pub(super) class_relations: RelationExistence,
    pub(super) object_relations: RelationExistence,
}

impl PlanningState {
    pub(super) fn new(authorization: ImportAuthorization) -> Self {
        Self {
            authorization,
            ..Self::default()
        }
    }

    pub(super) fn record_planned_group(&mut self, scope: &str, name: &str) {
        self.planned_groups
            .entry(scope.to_owned())
            .or_default()
            .insert(name.to_owned());
    }

    pub(super) fn has_planned_group(&self, key: &GroupKey) -> bool {
        self.planned_groups
            .get(key.resolve_identity_scope_name())
            .is_some_and(|groups| groups.contains(&key.groupname))
    }

    pub(super) fn reserve_collection_name(&mut self, parent_id: i32, name: &str) -> bool {
        self.planned_collection_keys
            .insert((parent_id, name.to_owned()))
    }

    pub(super) fn collection_by_ref(&self, reference: &str) -> Option<&CollectionResolution> {
        self.collections_by_ref.get(reference)
    }

    pub(super) fn collection_by_id(&self, id: i32) -> Option<&CollectionResolution> {
        self.collections_by_id.get(&id)
    }

    pub(super) fn collection_under_parent(
        &self,
        parent: Option<i32>,
        name: &str,
    ) -> Option<&CollectionResolution> {
        self.collections_by_parent_name
            .get(&(parent, name.to_owned()))
    }

    pub(super) fn collections_named(&self, name: &str) -> CacheLookup<&[CollectionResolution]> {
        match self.collections_by_name.get(name).map(Vec::as_slice) {
            Some([]) => CacheLookup::Missing,
            Some(collections) => CacheLookup::Found(collections),
            None => CacheLookup::Unknown,
        }
    }

    pub(super) fn record_missing_collection_name(&mut self, name: String) {
        self.collections_by_name.entry(name).or_default();
    }

    pub(super) fn remember_collection(
        &mut self,
        reference: Option<String>,
        collection: CollectionResolution,
    ) {
        self.collections_by_id
            .insert(collection.id, collection.clone());
        let same_name = self
            .collections_by_name
            .entry(collection.name.clone())
            .or_default();
        same_name.retain(|known| known.id != collection.id);
        same_name.push(collection.clone());
        self.collections_by_parent_name.insert(
            (collection.parent_collection_id, collection.name.clone()),
            collection.clone(),
        );
        if let Some(reference) = reference {
            self.collections_by_ref.insert(reference, collection);
        }
    }

    pub(super) fn remember_class(&mut self, reference: Option<String>, class: ClassResolution) {
        self.classes
            .remember((class.collection_id, class.name.clone()), reference, class);
    }

    pub(super) fn remember_object(&mut self, reference: Option<String>, object: ObjectResolution) {
        self.objects
            .remember((object.class_id, object.name.clone()), reference, object);
    }

    pub(super) fn next_virtual_id(&mut self) -> i32 {
        self.last_virtual_id -= 1;
        self.last_virtual_id
    }
}

pub(super) struct TerminalTaskUpdate {
    pub(super) status: TaskStatus,
    pub(super) summary: String,
    pub(super) counts: TaskResultCounts,
    pub(super) event_data: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorkerLoopAction {
    Continue,
    Sleep,
}

#[derive(Clone, Debug)]
pub(super) struct PlannedTaskResult {
    pub(super) item_ref: Option<String>,
    pub(super) entity_kind: String,
    pub(super) action: String,
    pub(super) identifier: Option<String>,
    pub(super) details: Option<serde_json::Value>,
}

impl PlannedTaskResult {
    pub(super) fn success_result(&self, task_id: hubuum_domain::TaskId) -> StorageImportResult {
        StorageImportResult::builder(
            task_id,
            self.entity_kind.clone(),
            self.action.clone(),
            "succeeded",
        )
        .item_ref(self.item_ref.clone())
        .identifier(self.identifier.clone())
        .details(self.details.clone())
        .build()
    }

    pub(super) fn set_observed_revision(
        &mut self,
        revision: Option<crate::models::ResourceRevision>,
    ) {
        let Some(revision) = revision else {
            return;
        };
        let details = self
            .details
            .get_or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        if let Some(details) = details.as_object_mut() {
            details.insert("observed_revision".to_string(), serde_json::json!(revision));
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PlannedItem {
    pub(super) result: PlannedTaskResult,
    pub(super) execution: Option<PlannedExecution>,
}

#[derive(Default)]
pub(super) struct ExecutionAccumulator {
    pub(super) results: Vec<StorageImportResult>,
    pub(super) processed: i32,
    pub(super) success: i32,
    pub(super) failed: i32,
}

pub(super) const IMPORT_RESULTS_BATCH_SIZE: usize = 1000;

impl ExecutionAccumulator {
    pub(super) fn push_success(
        &mut self,
        task_id: i32,
        planned: &PlannedTaskResult,
        outcome: &str,
    ) {
        self.processed += 1;
        self.success += 1;
        self.results.push(
            StorageImportResult::builder(
                crate::models::TaskID::new(task_id).expect("validated task id must be positive"),
                planned.entity_kind.clone(),
                planned.action.clone(),
                outcome,
            )
            .item_ref(planned.item_ref.clone())
            .identifier(planned.identifier.clone())
            .details(planned.details.clone())
            .build(),
        );
    }

    pub(super) fn push_failure(
        &mut self,
        task_id: i32,
        planned: &PlannedTaskResult,
        error: impl Into<String>,
        outcome: &str,
    ) {
        self.processed += 1;
        self.failed += 1;
        self.results.push(
            StorageImportResult::builder(
                crate::models::TaskID::new(task_id).expect("validated task id must be positive"),
                planned.entity_kind.clone(),
                planned.action.clone(),
                outcome,
            )
            .item_ref(planned.item_ref.clone())
            .identifier(planned.identifier.clone())
            .error(Some(error.into()))
            .details(planned.details.clone())
            .build(),
        );
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum FailureKind {
    Permission,
    Collision,
    Validation,
    Resolution,
    Runtime,
}

#[derive(Debug)]
pub(super) struct PlanningFailure {
    pub(super) kind: FailureKind,
    pub(super) item: PlannedTaskResult,
    pub(super) message: String,
}

#[derive(Default)]
pub(super) struct PlanningOutcome {
    pub(super) planned_items: Vec<PlannedItem>,
    pub(super) failures: Vec<PlanningFailure>,
    pub(super) aborted: bool,
}

impl PlanningFailure {
    pub(super) fn outcome(&self) -> &'static str {
        if self.message.starts_with("stale_revision") {
            "stale_revision"
        } else {
            "failed"
        }
    }

    pub(super) fn message_for_storage(&self) -> String {
        match self.kind {
            FailureKind::Runtime => "An internal error occurred".to_string(),
            _ => self.message.clone(),
        }
    }

    pub(super) fn into_result(self, task_id: i32) -> StorageImportResult {
        let error = self.message_for_storage();
        let outcome = self.outcome();
        StorageImportResult::builder(
            crate::models::TaskID::new(task_id).expect("validated task id must be positive"),
            self.item.entity_kind,
            self.item.action,
            outcome,
        )
        .item_ref(self.item.item_ref)
        .identifier(self.item.identifier)
        .error(Some(error))
        .details(self.item.details)
        .build()
    }
}
