//! Durable, non-secret task discovery facts. Payloads and resource membership are
//! deliberately not part of this contract.
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
    RemoteTargetId, SchemaRevision,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{StorageSchemaWorkKind, StorageTaskKind, StorageValidationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskExplicitTarget {
    Collection {
        collection_id: CollectionId,
    },
    Class {
        class_id: ClassId,
    },
    Object {
        class_id: Option<ClassId>,
        object_id: ObjectId,
    },
    ClassRelation {
        relation_id: ClassRelationId,
    },
    ObjectRelation {
        relation_id: ObjectRelationId,
    },
}

impl TaskExplicitTarget {
    #[must_use]
    pub const fn class_id(self) -> Option<ClassId> {
        match self {
            Self::Class { class_id } => Some(class_id),
            Self::Object { class_id, .. } => class_id,
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExportScopeKind {
    Collections,
    Classes,
    ObjectsInClass,
    ClassRelations,
    ObjectRelations,
    RelatedObjects,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskImportAtomicity {
    Strict,
    BestEffort,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskImportCollisionPolicy {
    Abort,
    Overwrite,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskImportPermissionPolicy {
    Abort,
    Continue,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExportMissingDataPolicy {
    Strict,
    Null,
    Omit,
}

/// Unknown is distinct from a known lack of output, including historical tasks.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOutputMetadata {
    #[default]
    Unknown,
    NotProduced,
    Produced {
        expires_at: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOutputState {
    Available,
    Expired,
    NotProduced,
    Unknown,
}
impl TaskOutputMetadata {
    #[must_use]
    pub fn state(&self, now: DateTime<Utc>, artifact_present: bool) -> TaskOutputState {
        match self {
            Self::Unknown => TaskOutputState::Unknown,
            Self::NotProduced => TaskOutputState::NotProduced,
            Self::Produced { expires_at } if *expires_at > now && artifact_present => {
                TaskOutputState::Available
            }
            Self::Produced { .. } => TaskOutputState::Expired,
        }
    }
}

/// A closed, typed allow-list. No variant can retain request bodies, query text,
/// remote invocation parameters, templates, or credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskMetadataDetails {
    Import {
        dry_run: Option<bool>,
        atomicity: Option<TaskImportAtomicity>,
        collision_policy: Option<TaskImportCollisionPolicy>,
        permission_policy: Option<TaskImportPermissionPolicy>,
        has_failed_items: Option<bool>,
    },
    Export {
        scope_kind: Option<TaskExportScopeKind>,
        target: Option<TaskExplicitTarget>,
        template_id: Option<ExportTemplateId>,
        missing_data_policy: Option<TaskExportMissingDataPolicy>,
        max_items: Option<u64>,
        max_output_bytes: Option<u64>,
        warning_count: Option<i32>,
        truncated: Option<bool>,
        #[serde(default)]
        output: TaskOutputMetadata,
    },
    Backup {
        include_history: Option<bool>,
        #[serde(default)]
        output: TaskOutputMetadata,
    },
    SchemaValidation {
        class_id: Option<ClassId>,
        schema_revision: Option<SchemaRevision>,
        work_kind: Option<StorageSchemaWorkKind>,
    },
    Reindex {
        class_id: Option<ClassId>,
        computation_revision: Option<i64>,
    },
    RemoteCall {
        remote_target_id: Option<RemoteTargetId>,
        target: Option<TaskExplicitTarget>,
    },
}
impl TaskMetadataDetails {
    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        match self {
            Self::Import { .. } => StorageTaskKind::Import,
            Self::Export { .. } => StorageTaskKind::Export,
            Self::Backup { .. } => StorageTaskKind::Backup,
            Self::SchemaValidation { .. } => StorageTaskKind::SchemaValidation,
            Self::Reindex { .. } => StorageTaskKind::Reindex,
            Self::RemoteCall { .. } => StorageTaskKind::RemoteCall,
        }
    }
    #[must_use]
    pub fn target(&self) -> Option<TaskExplicitTarget> {
        match self {
            Self::SchemaValidation { class_id, .. } | Self::Reindex { class_id, .. } => {
                class_id.map(|class_id| TaskExplicitTarget::Class { class_id })
            }
            Self::Export { target, .. } | Self::RemoteCall { target, .. } => *target,
            _ => None,
        }
    }
    #[must_use]
    pub const fn output(&self) -> Option<&TaskOutputMetadata> {
        match self {
            Self::Export { output, .. } | Self::Backup { output, .. } => Some(output),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MetadataWire")]
pub struct StorageTaskMetadata {
    version: u8,
    data: TaskMetadataDetails,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataWire {
    version: u8,
    data: TaskMetadataDetails,
}
impl TryFrom<MetadataWire> for StorageTaskMetadata {
    type Error = StorageValidationError;
    fn try_from(wire: MetadataWire) -> Result<Self, Self::Error> {
        if wire.version != 1 {
            return Err(invalid("Unsupported task metadata version"));
        }
        Self::new(wire.data)
    }
}
fn invalid(message: &str) -> StorageValidationError {
    StorageValidationError::invalid(message)
}
impl StorageTaskMetadata {
    /// No recoverable request facts; later artifact finalization can still add outcomes.
    #[must_use]
    pub fn unknown(kind: StorageTaskKind) -> Self {
        let data = match kind {
            StorageTaskKind::Import => TaskMetadataDetails::Import {
                dry_run: None,
                atomicity: None,
                collision_policy: None,
                permission_policy: None,
                has_failed_items: None,
            },
            StorageTaskKind::Export => TaskMetadataDetails::Export {
                scope_kind: None,
                target: None,
                template_id: None,
                missing_data_policy: None,
                max_items: None,
                max_output_bytes: None,
                warning_count: None,
                truncated: None,
                output: TaskOutputMetadata::Unknown,
            },
            StorageTaskKind::Backup => TaskMetadataDetails::Backup {
                include_history: None,
                output: TaskOutputMetadata::Unknown,
            },
            StorageTaskKind::SchemaValidation => TaskMetadataDetails::SchemaValidation {
                class_id: None,
                schema_revision: None,
                work_kind: None,
            },
            StorageTaskKind::Reindex => TaskMetadataDetails::Reindex {
                class_id: None,
                computation_revision: None,
            },
            StorageTaskKind::RemoteCall => TaskMetadataDetails::RemoteCall {
                remote_target_id: None,
                target: None,
            },
        };
        Self { version: 1, data }
    }

    pub fn new(data: TaskMetadataDetails) -> Result<Self, StorageValidationError> {
        match &data {
            TaskMetadataDetails::SchemaValidation {
                class_id,
                schema_revision,
                ..
            } if schema_revision.is_some() && class_id.is_none() => {
                return Err(invalid("Schema revision requires a class"));
            }
            TaskMetadataDetails::Reindex {
                class_id,
                computation_revision: Some(revision),
            } if class_id.is_none() || *revision < 0 => {
                return Err(invalid(
                    "Computation revision requires a class and must be nonnegative",
                ));
            }
            TaskMetadataDetails::Export {
                scope_kind,
                target,
                warning_count,
                max_items,
                max_output_bytes,
                ..
            } => {
                if warning_count.is_some_and(|n| n < 0)
                    || *max_items == Some(0)
                    || *max_output_bytes == Some(0)
                {
                    return Err(invalid("Invalid export discovery counts or limits"));
                }
                let valid_target = match (scope_kind, target) {
                    (
                        Some(TaskExportScopeKind::ObjectsInClass),
                        Some(TaskExplicitTarget::Class { .. }),
                    ) => true,
                    (
                        Some(TaskExportScopeKind::RelatedObjects),
                        Some(TaskExplicitTarget::Object {
                            class_id: Some(_), ..
                        }),
                    ) => true,
                    (_, None) => true, // Historical identity may be unavailable.
                    _ => false,
                };
                if !valid_target {
                    return Err(invalid("Export target does not agree with captured scope"));
                }
            }
            _ => {}
        }
        Ok(Self { version: 1, data })
    }
    pub fn from_persisted(
        kind: StorageTaskKind,
        value: Value,
    ) -> Result<Self, StorageValidationError> {
        let metadata: Self = serde_json::from_value(value)
            .map_err(|_| invalid("Invalid persisted task discovery metadata"))?;
        if metadata.kind() != kind {
            return Err(invalid("Task metadata kind differs from task kind"));
        }
        Ok(metadata)
    }
    #[must_use]
    pub const fn kind(&self) -> StorageTaskKind {
        self.data.kind()
    }
    #[must_use]
    pub const fn details(&self) -> &TaskMetadataDetails {
        &self.data
    }
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("task metadata contains only serializable values")
    }
    pub fn record_terminal(&mut self, failed_items: i32) {
        if let TaskMetadataDetails::Import {
            has_failed_items, ..
        } = &mut self.data
        {
            *has_failed_items = Some(failed_items > 0);
        }
    }
    pub fn record_export(
        &mut self,
        warning_count: i32,
        truncated: bool,
        expires_at: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        if warning_count < 0 {
            return Err(invalid("Warning count must not be negative"));
        }
        let TaskMetadataDetails::Export {
            warning_count: stored_count,
            truncated: stored_truncated,
            output,
            ..
        } = &mut self.data
        else {
            return Err(invalid("Export output requires export metadata"));
        };
        *stored_count = Some(warning_count);
        *stored_truncated = Some(truncated);
        *output = TaskOutputMetadata::Produced { expires_at };
        Ok(())
    }
    pub fn record_backup(
        &mut self,
        expires_at: DateTime<Utc>,
    ) -> Result<(), StorageValidationError> {
        let TaskMetadataDetails::Backup { output, .. } = &mut self.data else {
            return Err(invalid("Backup output requires backup metadata"));
        };
        *output = TaskOutputMetadata::Produced { expires_at };
        Ok(())
    }
}

/// Lightweight retained-work and artifact presence, loaded together for a page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageTaskDiscoveryState {
    schema_work: Option<(StorageSchemaWorkKind, crate::StorageSchemaWorkStatus)>,
    output_present: bool,
}
impl StorageTaskDiscoveryState {
    #[must_use]
    pub const fn new(
        schema_work: Option<(StorageSchemaWorkKind, crate::StorageSchemaWorkStatus)>,
        output_present: bool,
    ) -> Self {
        Self {
            schema_work,
            output_present,
        }
    }
    #[must_use]
    pub const fn schema_work(
        &self,
    ) -> Option<(StorageSchemaWorkKind, crate::StorageSchemaWorkStatus)> {
        self.schema_work
    }
    #[must_use]
    pub const fn output_present(&self) -> bool {
        self.output_present
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(json!({"version":2,"data":{"kind":"backup"}}))]
    #[case(json!({"version":1,"data":{"kind":"import"}}))]
    #[case(json!({"version":1,"data":{"kind":"backup","credentials":"secret"}}))]
    #[case(json!({"version":1,"data":{"kind":"backup","include_history":"false"}}))]
    #[case(json!({"version":1,"data":{"kind":"backup","output":{"state":"produced"}}}))]
    fn rejects_invalid_persisted_metadata(#[case] raw: Value) {
        assert!(StorageTaskMetadata::from_persisted(StorageTaskKind::Backup, raw).is_err());
    }
    #[rstest]
    #[case(StorageTaskKind::Import)]
    #[case(StorageTaskKind::Export)]
    #[case(StorageTaskKind::Backup)]
    #[case(StorageTaskKind::SchemaValidation)]
    #[case(StorageTaskKind::Reindex)]
    #[case(StorageTaskKind::RemoteCall)]
    fn unknown_facts_roundtrip(#[case] kind: StorageTaskKind) {
        let metadata = StorageTaskMetadata::unknown(kind);
        assert_eq!(
            StorageTaskMetadata::from_persisted(kind, metadata.to_value()).unwrap(),
            metadata
        );
    }
    #[rstest]
    #[case(99, true, TaskOutputState::Available)]
    #[case(100, true, TaskOutputState::Expired)]
    #[case(99, false, TaskOutputState::Expired)]
    fn output_expiry_boundary(
        #[case] now: i64,
        #[case] present: bool,
        #[case] expected: TaskOutputState,
    ) {
        let output = TaskOutputMetadata::Produced {
            expires_at: DateTime::from_timestamp(100, 0).unwrap(),
        };
        assert_eq!(
            output.state(DateTime::from_timestamp(now, 0).unwrap(), present),
            expected
        );
    }
}
