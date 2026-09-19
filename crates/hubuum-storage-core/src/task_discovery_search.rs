//! Validated predicates over explicit operation targets and retained facts.
use crate::{
    StorageSchemaWorkKind, StorageSchemaWorkStatus, StorageTask, StorageTaskKind,
    StorageTaskMetadata, StorageValidationError, TaskExplicitTarget, TaskExportScopeKind,
    TaskImportAtomicity, TaskImportCollisionPolicy, TaskImportPermissionPolicy,
    TaskMetadataDetails, TaskOutputState,
};
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
    RemoteTargetId, SchemaRevision,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskRemoteSideEffectState {
    NotSent,
    PossiblySent,
    LegacyUnknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskDiscoveryPredicate {
    Class(ClassId),
    Object(ObjectId),
    Collection(CollectionId),
    ClassRelation(ClassRelationId),
    ObjectRelation(ObjectRelationId),
    SchemaRevision(SchemaRevision),
    SchemaWorkKind(StorageSchemaWorkKind),
    SchemaWorkStatus(StorageSchemaWorkStatus),
    ComputationRevision(i64),
    RemoteTarget(RemoteTargetId),
    RemoteSideEffect(TaskRemoteSideEffectState),
    ExportScope(TaskExportScopeKind),
    ExportTemplate(ExportTemplateId),
    ExportHasWarnings(bool),
    ExportTruncated(bool),
    ImportDryRun(bool),
    ImportAtomicity(TaskImportAtomicity),
    ImportCollisionPolicy(TaskImportCollisionPolicy),
    ImportPermissionPolicy(TaskImportPermissionPolicy),
    ImportHasFailedItems(bool),
    BackupIncludeHistory(bool),
    OutputState(TaskOutputState),
}
impl TaskDiscoveryPredicate {
    #[must_use]
    pub fn applicable_kinds(&self) -> &'static [StorageTaskKind] {
        use StorageTaskKind as K;
        match self {
            Self::SchemaRevision(_) | Self::SchemaWorkKind(_) | Self::SchemaWorkStatus(_) => {
                &[K::SchemaValidation]
            }
            Self::ComputationRevision(_) => &[K::Reindex],
            Self::RemoteTarget(_)
            | Self::RemoteSideEffect(_)
            | Self::Collection(_)
            | Self::ClassRelation(_)
            | Self::ObjectRelation(_) => &[K::RemoteCall],
            Self::ExportScope(_)
            | Self::ExportTemplate(_)
            | Self::ExportHasWarnings(_)
            | Self::ExportTruncated(_) => &[K::Export],
            Self::ImportDryRun(_)
            | Self::ImportAtomicity(_)
            | Self::ImportCollisionPolicy(_)
            | Self::ImportPermissionPolicy(_)
            | Self::ImportHasFailedItems(_) => &[K::Import],
            Self::BackupIncludeHistory(_) => &[K::Backup],
            Self::OutputState(_) => &[K::Export, K::Backup],
            Self::Class(_) => &[K::SchemaValidation, K::Reindex, K::Export, K::RemoteCall],
            Self::Object(_) => &[K::Export, K::RemoteCall],
        }
    }
    #[must_use]
    pub fn matches(&self, task: &StorageTask, now: DateTime<Utc>) -> bool {
        use TaskMetadataDetails as D;
        let data = task.metadata().map(StorageTaskMetadata::details);
        let target = data.and_then(D::target);
        match self {
            Self::Class(id) => target.and_then(TaskExplicitTarget::class_id) == Some(*id),
            Self::Object(id) => {
                matches!(target, Some(TaskExplicitTarget::Object { object_id, .. }) if object_id == *id)
            }
            Self::Collection(id) => {
                matches!(target, Some(TaskExplicitTarget::Collection { collection_id }) if collection_id == *id)
            }
            Self::ClassRelation(id) => {
                matches!(target, Some(TaskExplicitTarget::ClassRelation { relation_id }) if relation_id == *id)
            }
            Self::ObjectRelation(id) => {
                matches!(target, Some(TaskExplicitTarget::ObjectRelation { relation_id }) if relation_id == *id)
            }
            Self::SchemaRevision(v) => {
                matches!(data, Some(D::SchemaValidation { schema_revision: Some(x), .. }) if x == v)
            }
            Self::SchemaWorkKind(v) => task
                .discovery_state()
                .schema_work()
                .is_some_and(|(k, _)| k == *v),
            Self::SchemaWorkStatus(v) => task
                .discovery_state()
                .schema_work()
                .is_some_and(|(_, s)| s == *v),
            Self::ComputationRevision(v) => {
                matches!(data, Some(D::Reindex { computation_revision: Some(x), .. }) if x == v)
            }
            Self::RemoteTarget(v) => {
                matches!(data, Some(D::RemoteCall { remote_target_id: Some(x), .. }) if x == v)
            }
            Self::RemoteSideEffect(v) => {
                task.kind() == StorageTaskKind::RemoteCall && remote_side_effect(task) == *v
            }
            Self::ExportScope(v) => {
                matches!(data, Some(D::Export { scope_kind: Some(x), .. }) if x == v)
            }
            Self::ExportTemplate(v) => {
                matches!(data, Some(D::Export { template_id: Some(x), .. }) if x == v)
            }
            Self::ExportHasWarnings(v) => {
                matches!(data, Some(D::Export { warning_count: Some(x), .. }) if (*x > 0) == *v)
            }
            Self::ExportTruncated(v) => {
                matches!(data, Some(D::Export { truncated: Some(x), .. }) if x == v)
            }
            Self::ImportDryRun(v) => {
                matches!(data, Some(D::Import { dry_run: Some(x), .. }) if x == v)
            }
            Self::ImportAtomicity(v) => {
                matches!(data, Some(D::Import { atomicity: Some(x), .. }) if x == v)
            }
            Self::ImportCollisionPolicy(v) => {
                matches!(data, Some(D::Import { collision_policy: Some(x), .. }) if x == v)
            }
            Self::ImportPermissionPolicy(v) => {
                matches!(data, Some(D::Import { permission_policy: Some(x), .. }) if x == v)
            }
            Self::ImportHasFailedItems(v) => {
                matches!(data, Some(D::Import { has_failed_items: Some(x), .. }) if x == v)
            }
            Self::BackupIncludeHistory(v) => {
                matches!(data, Some(D::Backup { include_history: Some(x), .. }) if x == v)
            }
            Self::OutputState(v) => {
                matches!(
                    task.kind(),
                    StorageTaskKind::Export | StorageTaskKind::Backup
                ) && data
                    .and_then(D::output)
                    .map_or(TaskOutputState::Unknown, |o| {
                        o.state(now, task.discovery_state().output_present())
                    })
                    == *v
            }
        }
    }
}
fn remote_side_effect(task: &StorageTask) -> TaskRemoteSideEffectState {
    if matches!(
        task.control().phase(),
        crate::StorageTaskExecutionPhase::RemoteDispatched { .. }
    ) {
        TaskRemoteSideEffectState::PossiblySent
    } else if task.control().deadline().is_some()
        || (task.started_at().is_none() && task.attempt_count() == 0)
    {
        TaskRemoteSideEffectState::NotSent
    } else {
        TaskRemoteSideEffectState::LegacyUnknown
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskDiscoverySearch {
    predicates: Vec<TaskDiscoveryPredicate>,
    evaluated_at: DateTime<Utc>,
}
impl TaskDiscoverySearch {
    pub fn try_new(
        predicates: Vec<TaskDiscoveryPredicate>,
        evaluated_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        let class = predicates
            .iter()
            .any(|p| matches!(p, TaskDiscoveryPredicate::Class(_)));
        if predicates.iter().any(|p| {
            matches!(
                p,
                TaskDiscoveryPredicate::SchemaRevision(_)
                    | TaskDiscoveryPredicate::ComputationRevision(_)
            )
        }) && !class
        {
            return Err(StorageValidationError::invalid(
                "Revision filters require class_id",
            ));
        }
        if predicates
            .iter()
            .any(|p| matches!(p, TaskDiscoveryPredicate::ComputationRevision(v) if *v < 0))
        {
            return Err(StorageValidationError::invalid(
                "Computation revision must be nonnegative",
            ));
        }
        Ok(Self {
            predicates,
            evaluated_at,
        })
    }
    #[must_use]
    pub fn predicates(&self) -> &[TaskDiscoveryPredicate] {
        &self.predicates
    }
    #[must_use]
    pub const fn evaluated_at(&self) -> DateTime<Utc> {
        self.evaluated_at
    }
    #[must_use]
    pub fn matches(&self, task: &StorageTask) -> bool {
        self.predicates
            .iter()
            .all(|p| p.matches(task, self.evaluated_at))
    }
}
