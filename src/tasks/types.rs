use std::collections::{HashMap, HashSet};

use crate::models::{Permissions, TaskResultCounts, TaskStatus};
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

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum ImportAdminStatus {
    #[default]
    Unknown,
    Known(bool),
}

impl ImportAdminStatus {
    pub(super) fn known(self) -> Option<bool> {
        match self {
            Self::Unknown => None,
            Self::Known(is_admin) => Some(is_admin),
        }
    }
}

#[derive(Default)]
pub(super) struct PlanningState {
    pub(super) next_temp_id: i32,
    pub(super) admin_status: ImportAdminStatus,
    /// Submitting token's scope boundary (`None` = unscoped). Threaded into the
    /// per-collection permission checks so a scoped import cannot exceed it.
    pub(super) scopes: Option<crate::models::TokenScope>,
    pub(super) planned_collection_keys: HashSet<(Option<i32>, String)>,
    pub(super) planned_class_keys: HashSet<(i32, String)>,
    pub(super) planned_object_keys: HashSet<(i32, String)>,
    pub(super) planned_group_keys: HashSet<(String, String)>,
    pub(super) planned_identity_scope_names_by_ref: HashMap<String, String>,
    pub(super) missing_collection_names: HashSet<String>,
    pub(super) missing_class_keys: HashSet<(i32, String)>,
    pub(super) missing_object_keys: HashSet<(i32, String)>,
    pub(super) collections_by_ref: HashMap<String, CollectionResolution>,
    pub(super) collections_by_name: HashMap<String, Vec<CollectionResolution>>,
    pub(super) collections_by_parent_name: HashMap<(Option<i32>, String), CollectionResolution>,
    pub(super) collections_by_id: HashMap<i32, CollectionResolution>,
    pub(super) classes_by_ref: HashMap<String, ClassResolution>,
    pub(super) classes_by_key: HashMap<(i32, String), ClassResolution>,
    pub(super) objects_by_ref: HashMap<String, ObjectResolution>,
    pub(super) objects_by_key: HashMap<(i32, String), ObjectResolution>,
    pub(super) class_relations: HashSet<(i32, i32)>,
    pub(super) object_relations: HashSet<(i32, i32)>,
    pub(super) class_relation_exists_cache: HashMap<(i32, i32), bool>,
    pub(super) object_relation_exists_cache: HashMap<(i32, i32), bool>,
    pub(super) collection_permission_cache: HashMap<(i32, Permissions), Result<(), String>>,
}

impl PlanningState {
    pub(super) fn new() -> Self {
        Self {
            next_temp_id: -1,
            ..Self::default()
        }
    }

    pub(super) fn next_virtual_id(&mut self) -> i32 {
        let id = self.next_temp_id;
        self.next_temp_id -= 1;
        id
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
