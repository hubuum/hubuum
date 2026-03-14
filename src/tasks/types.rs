use std::collections::{HashMap, HashSet};

use crate::models::{
    HubuumClass, HubuumObject, ImportClassInput, ImportClassRelationInput, ImportNamespaceInput,
    ImportNamespacePermissionInput, ImportObjectInput, ImportObjectRelationInput, Namespace,
    NewImportTaskResultRecord, Permissions, TaskStatus,
};

#[derive(Clone)]
pub(super) struct NamespaceResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) description: String,
    pub(super) exists_in_db: bool,
}

#[derive(Clone)]
pub(super) struct ClassResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) namespace_id: i32,
    pub(super) json_schema: Option<serde_json::Value>,
    pub(super) validate_schema: bool,
    pub(super) exists_in_db: bool,
}

#[derive(Clone)]
pub(super) struct ObjectResolution {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) namespace_id: i32,
    pub(super) class_id: i32,
    pub(super) exists_in_db: bool,
}

#[derive(Default)]
pub(super) struct PlanningState {
    pub(super) next_temp_id: i32,
    pub(super) is_admin: Option<bool>,
    pub(super) planned_namespace_names: HashSet<String>,
    pub(super) planned_class_keys: HashSet<(i32, String)>,
    pub(super) planned_object_keys: HashSet<(i32, String)>,
    pub(super) missing_namespace_names: HashSet<String>,
    pub(super) missing_class_keys: HashSet<(i32, String)>,
    pub(super) missing_object_keys: HashSet<(i32, String)>,
    pub(super) namespaces_by_ref: HashMap<String, NamespaceResolution>,
    pub(super) namespaces_by_name: HashMap<String, NamespaceResolution>,
    pub(super) namespaces_by_id: HashMap<i32, NamespaceResolution>,
    pub(super) classes_by_ref: HashMap<String, ClassResolution>,
    pub(super) classes_by_key: HashMap<(i32, String), ClassResolution>,
    pub(super) objects_by_ref: HashMap<String, ObjectResolution>,
    pub(super) objects_by_key: HashMap<(i32, String), ObjectResolution>,
    pub(super) class_relations: HashSet<(i32, i32)>,
    pub(super) object_relations: HashSet<(i32, i32)>,
    pub(super) class_relation_exists_cache: HashMap<(i32, i32), bool>,
    pub(super) object_relation_exists_cache: HashMap<(i32, i32), bool>,
    pub(super) namespace_permission_cache: HashMap<(i32, Permissions), Result<(), String>>,
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

#[derive(Default)]
pub(super) struct RuntimeState {
    pub(super) namespaces_by_ref: HashMap<String, Namespace>,
    pub(super) classes_by_ref: HashMap<String, HubuumClass>,
    pub(super) objects_by_ref: HashMap<String, HubuumObject>,
}

pub(super) struct TerminalTaskUpdate {
    pub(super) status: TaskStatus,
    pub(super) summary: String,
    pub(super) processed_items: i32,
    pub(super) success_items: i32,
    pub(super) failed_items: i32,
    pub(super) event_data: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WorkerLoopAction {
    Continue,
    Sleep,
}

#[derive(Clone, Debug)]
pub(super) enum PlannedExecution {
    CreateNamespace(ImportNamespaceInput),
    UpdateNamespace {
        namespace_id: i32,
        input: ImportNamespaceInput,
    },
    CreateClass(ImportClassInput),
    UpdateClass {
        class_id: i32,
        input: ImportClassInput,
    },
    CreateObject(ImportObjectInput),
    UpdateObject {
        object_id: i32,
        input: ImportObjectInput,
    },
    CreateClassRelation(ImportClassRelationInput),
    CreateObjectRelation(ImportObjectRelationInput),
    ApplyNamespacePermissions(ImportNamespacePermissionInput),
}

#[derive(Clone, Debug)]
pub(super) struct PlannedTaskResult {
    pub(super) item_ref: Option<String>,
    pub(super) entity_kind: String,
    pub(super) action: String,
    pub(super) identifier: Option<String>,
    pub(super) details: Option<serde_json::Value>,
}

#[derive(Clone, Debug)]
pub(super) struct PlannedItem {
    pub(super) result: PlannedTaskResult,
    pub(super) execution: Option<PlannedExecution>,
}

#[derive(Default)]
pub(super) struct ExecutionAccumulator {
    pub(super) results: Vec<NewImportTaskResultRecord>,
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
        self.results.push(NewImportTaskResultRecord {
            task_id,
            item_ref: planned.item_ref.clone(),
            entity_kind: planned.entity_kind.clone(),
            action: planned.action.clone(),
            identifier: planned.identifier.clone(),
            outcome: outcome.to_string(),
            error: None,
            details: planned.details.clone(),
        });
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
        self.results.push(NewImportTaskResultRecord {
            task_id,
            item_ref: planned.item_ref.clone(),
            entity_kind: planned.entity_kind.clone(),
            action: planned.action.clone(),
            identifier: planned.identifier.clone(),
            outcome: outcome.to_string(),
            error: Some(error.into()),
            details: planned.details.clone(),
        });
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
    pub(super) fn message_for_storage(&self) -> String {
        match self.kind {
            FailureKind::Runtime => "An internal error occurred".to_string(),
            _ => self.message.clone(),
        }
    }

    pub(super) fn into_result(self, task_id: i32) -> NewImportTaskResultRecord {
        let error = self.message_for_storage();
        NewImportTaskResultRecord {
            task_id,
            item_ref: self.item.item_ref,
            entity_kind: self.item.entity_kind,
            action: self.item.action,
            identifier: self.item.identifier,
            outcome: "failed".to_string(),
            error: Some(error),
            details: self.item.details,
        }
    }
}
