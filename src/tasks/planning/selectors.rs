//! Carry the planner's visible name choices into backend preflight and execution.
use crate::models::{ClassKey, CollectionKey, ObjectKey};
use crate::tasks::types::{PlannedExecution, PlannedItem, PlanningState};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct PreviewClassNames(HashMap<(i32, String), String>);

impl PreviewClassNames {
    /// A rolled-back INSERT must not consult the uniqueness of a class name
    /// the caller cannot read. References within the same preview follow it.
    pub(super) fn for_creates(items: &[PlannedItem], state: &PlanningState) -> Self {
        let mut names = HashMap::new();
        for item in items {
            if let Some(PlannedExecution::CreateClass(input)) = &item.execution
                && let Some(id) = collection_id(
                    state,
                    input.collection_ref.as_deref(),
                    input.collection_key.as_ref(),
                )
            {
                names.insert(
                    (id, input.name.clone()),
                    format!("preview-{}", uuid::Uuid::new_v4().simple()),
                );
            }
        }
        Self(names)
    }

    pub(super) fn display_message(&self, mut message: String) -> String {
        for ((_, name), preview) in &self.0 {
            if message.contains(preview) {
                message = message.replace(preview, name);
            }
        }
        message
    }
}

fn collection_id(
    state: &PlanningState,
    reference: Option<&str>,
    key: Option<&CollectionKey>,
) -> Option<i32> {
    reference
        .and_then(|value| state.collection_by_ref(value).map(|row| row.id))
        .or_else(|| key.and_then(|key| state.resolved_keys.get(key).copied()))
}

pub(super) fn bind(
    operation: &mut PlannedExecution,
    state: &PlanningState,
    preview: &PreviewClassNames,
) {
    let bind_collection = |key: &mut Option<CollectionKey>| {
        if let Some(key) = key
            && key.path.is_none()
        {
            key.path = state.execution_paths.get(&key.name).cloned();
        }
    };
    let rename = |name: &mut String, reference: Option<&str>, key: Option<&CollectionKey>| {
        if !preview.0.is_empty()
            && let Some(id) = collection_id(state, reference, key)
            && let Some(alias) = preview.0.get(&(id, name.clone()))
        {
            name.clone_from(alias);
        }
    };
    let bind_class = |key: &mut Option<ClassKey>| {
        if let Some(key) = key {
            rename(
                &mut key.name,
                key.collection_ref.as_deref(),
                key.collection_key.as_ref(),
            );
            bind_collection(&mut key.collection_key);
        }
    };
    let bind_object = |key: &mut Option<ObjectKey>| {
        if let Some(key) = key {
            bind_class(&mut key.class_key);
        }
    };
    match operation {
        PlannedExecution::CreateCollection(input)
        | PlannedExecution::UpdateCollection { input, .. } => {
            bind_collection(&mut input.parent_collection_key)
        }
        PlannedExecution::CreateClass(input) => {
            rename(
                &mut input.name,
                input.collection_ref.as_deref(),
                input.collection_key.as_ref(),
            );
            bind_collection(&mut input.collection_key);
        }
        PlannedExecution::UpdateClass { input, .. } => bind_collection(&mut input.collection_key),
        PlannedExecution::CreateObject(input) | PlannedExecution::UpdateObject { input, .. } => {
            bind_class(&mut input.class_key)
        }
        PlannedExecution::CreateClassRelation(input)
        | PlannedExecution::CheckClassRelationCondition(input)
        | PlannedExecution::UpdateClassRelationTimestamps { input, .. } => {
            bind_class(&mut input.from_class_key);
            bind_class(&mut input.to_class_key);
        }
        PlannedExecution::CreateObjectRelation(input)
        | PlannedExecution::CheckObjectRelationCondition(input)
        | PlannedExecution::UpdateObjectRelationTimestamps { input, .. } => {
            bind_object(&mut input.from_object_key);
            bind_object(&mut input.to_object_key);
        }
        PlannedExecution::ApplyCollectionPermissions { input, .. } => {
            bind_collection(&mut input.collection_key)
        }
        // These operations already require unrestricted administrator authority.
        PlannedExecution::UpsertIdentityScope { .. }
        | PlannedExecution::UpsertGroup { .. }
        | PlannedExecution::UpsertPrincipal { .. }
        | PlannedExecution::UpsertGroupMembership { .. }
        | PlannedExecution::UpsertComputedField { .. }
        | PlannedExecution::UpsertExportTemplate { .. }
        | PlannedExecution::UpsertRemoteTarget { .. }
        | PlannedExecution::UpsertEventSink { .. }
        | PlannedExecution::UpsertEventSubscription { .. } => {}
    }
}
