use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{StorageError, StorageValidationError};

macro_rules! backup_sections {
    (
        $(#[$meta:meta])*
        $name:ident {
            $($variant:ident => $serialized:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        pub enum $name {
            $(#[serde(rename = $serialized)] $variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $serialized),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

backup_sections! {
    /// Stable logical current-state sections in Hubuum's versioned backup format.
    ///
    /// These names describe Hubuum resources and relationships. They are not
    /// database table identifiers and adapters must map them to their own
    /// persistence layout explicitly.
    StorageBackupStateSection {
        IdentityScopes => "identity_scopes",
        Groups => "groups",
        Principals => "principals",
        Users => "users",
        ServiceAccounts => "service_accounts",
        GroupMemberships => "group_memberships",
        GroupMembershipSources => "group_membership_sources",
        Collections => "collections",
        CollectionAuthorization => "collection_authorization",
        CollectionHierarchy => "collection_hierarchy",
        CollectionPermissionGrants => "collection_permission_grants",
        Classes => "classes",
        ComputedFieldDefinitions => "computed_field_definitions",
        ClassRelations => "class_relations",
        Objects => "objects",
        ObjectRelations => "object_relations",
        ExportTemplates => "export_templates",
        RemoteTargets => "remote_targets",
        EventSinks => "event_sinks",
        EventSubscriptions => "event_subscriptions",
    }
}

backup_sections! {
    /// Stable logical history sections in Hubuum's versioned backup format.
    StorageBackupHistorySection {
        CollectionHistory => "collection_history",
        ClassHistory => "class_history",
        ClassRelationHistory => "class_relation_history",
        ObjectHistory => "object_history",
        ObjectRelationHistory => "object_relation_history",
        ExportTemplateHistory => "export_template_history",
        RemoteTargetHistory => "remote_target_history",
        TerminalTasks => "terminal_tasks",
        ImportResults => "import_results",
        ExportOutputs => "export_outputs",
        RemoteCallResults => "remote_call_results",
        AuditEvents => "audit_events",
        TerminalEventDeliveries => "terminal_event_deliveries",
    }
}

/// One object in a logical backup section.
///
/// The object shape belongs to the versioned Hubuum backup contract. Keeping
/// the representation behind this type prevents adapters from passing an
/// arbitrary JSON scalar or array as a database row.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(transparent)]
pub struct StorageBackupRow(Map<String, Value>);

impl StorageBackupRow {
    pub fn try_from_value(value: Value) -> Result<Self, StorageValidationError> {
        value.as_object().cloned().map(Self).ok_or_else(|| {
            StorageValidationError::invalid("A backup section item must be a JSON object")
        })
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    #[must_use]
    pub fn fields(&self) -> &Map<String, Value> {
        &self.0
    }

    #[must_use]
    pub fn into_value(self) -> Value {
        Value::Object(self.0)
    }
}

impl fmt::Debug for StorageBackupRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBackupRow")
            .field("field_count", &self.0.len())
            .finish()
    }
}

pub type StorageBackupStateSections = BTreeMap<StorageBackupStateSection, Vec<StorageBackupRow>>;
pub type StorageBackupHistorySections =
    BTreeMap<StorageBackupHistorySection, Vec<StorageBackupRow>>;

/// Canonical logical sections used to construct a full-system backup document.
///
/// Section identities belong to the Hubuum backup format. Every selectable
/// backend explicitly projects its durable state into these resource-oriented
/// sections instead of exposing table names or native row values.
#[derive(Clone, PartialEq)]
pub struct StorageBackupSnapshot {
    state_sections: StorageBackupStateSections,
    history_sections: Option<StorageBackupHistorySections>,
}

impl StorageBackupSnapshot {
    pub fn try_new(
        state_sections: StorageBackupStateSections,
        history_sections: Option<StorageBackupHistorySections>,
    ) -> Result<Self, StorageValidationError> {
        let missing_state = StorageBackupStateSection::ALL
            .iter()
            .find(|section| !state_sections.contains_key(section));
        if let Some(section) = missing_state {
            return Err(StorageValidationError::invalid(format!(
                "Backup snapshot is missing required state section '{section}'"
            )));
        }

        if let Some(history) = &history_sections {
            let missing_history = StorageBackupHistorySection::ALL
                .iter()
                .find(|section| !history.contains_key(section));
            if let Some(section) = missing_history {
                return Err(StorageValidationError::invalid(format!(
                    "Backup snapshot is missing required history section '{section}'"
                )));
            }
        }

        Ok(Self {
            state_sections,
            history_sections,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageBackupStateSections,
        Option<StorageBackupHistorySections>,
    ) {
        (self.state_sections, self.history_sections)
    }
}

impl fmt::Debug for StorageBackupSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBackupSnapshot")
            .field("state_section_count", &self.state_sections.len())
            .field(
                "state_row_count",
                &self.state_sections.values().map(Vec::len).sum::<usize>(),
            )
            .field(
                "history_section_count",
                &self.history_sections.as_ref().map(BTreeMap::len),
            )
            .field(
                "history_row_count",
                &self
                    .history_sections
                    .as_ref()
                    .map(|sections| sections.values().map(Vec::len).sum::<usize>()),
            )
            .finish()
    }
}

/// Mandatory full-system snapshot behavior for every selectable backend.
#[async_trait]
pub trait BackupSnapshotStorage: Send + Sync {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_state() -> StorageBackupStateSections {
        StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect()
    }

    fn complete_history() -> StorageBackupHistorySections {
        StorageBackupHistorySection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect()
    }

    #[test]
    fn backup_rows_reject_non_object_json() {
        assert!(StorageBackupRow::try_from_value(Value::Null).is_err());
    }

    #[test]
    fn snapshots_require_every_logical_section() {
        let mut state = complete_state();
        state.remove(&StorageBackupStateSection::Classes);

        assert!(StorageBackupSnapshot::try_new(state, None).is_err());
    }

    #[test]
    fn snapshot_debug_reports_shape_without_row_content() {
        let mut state = complete_state();
        state.insert(
            StorageBackupStateSection::Classes,
            vec![StorageBackupRow::try_from_value(serde_json::json!({"secret": "state"})).unwrap()],
        );
        let mut history = complete_history();
        history.insert(
            StorageBackupHistorySection::ClassHistory,
            vec![
                StorageBackupRow::try_from_value(serde_json::json!({"secret": "history"})).unwrap(),
            ],
        );
        let snapshot = StorageBackupSnapshot::try_new(state, Some(history)).unwrap();

        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("secret"));
        assert!(debug.contains("state_section_count: 20"));
        assert!(debug.contains("history_row_count: Some(1)"));
    }
}
