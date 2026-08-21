use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use serde_json::Value;

use crate::StorageError;

/// Current-state sections in Hubuum's versioned full-system backup format.
pub const BACKUP_STATE_SECTIONS: &[&str] = &[
    "identity_scopes",
    "groups",
    "principals",
    "users",
    "service_accounts",
    "group_memberships",
    "group_membership_sources",
    "collections",
    "collection_authorization_state",
    "collection_closure",
    "permissions",
    "hubuumclass",
    "computed_field_definitions",
    "hubuumclass_relation",
    "hubuumobject",
    "hubuumobject_relation",
    "export_templates",
    "remote_targets",
    "event_sinks",
    "event_subscriptions",
];

/// Temporal history sections in Hubuum's versioned full-system backup format.
pub const BACKUP_TEMPORAL_HISTORY_SECTIONS: &[&str] = &[
    "collections_history",
    "hubuumclass_history",
    "hubuumclass_relation_history",
    "hubuumobject_history",
    "hubuumobject_relation_history",
    "export_templates_history",
    "remote_targets_history",
];

/// Operational history sections in Hubuum's versioned full-system backup format.
pub const BACKUP_AUXILIARY_HISTORY_SECTIONS: &[&str] = &[
    "tasks",
    "import_task_results",
    "export_task_outputs",
    "remote_call_results",
    "events",
    "event_deliveries",
];

/// Named logical sections in Hubuum's versioned full-system backup format.
pub type StorageBackupSections = BTreeMap<String, Vec<Value>>;

/// Canonical logical sections used to construct a full-system backup document.
///
/// Section names and row shapes belong to the versioned Hubuum backup format,
/// not to the application consumer or a database driver. Every selectable
/// backend must project its durable state into this representation.
#[derive(Clone, PartialEq)]
pub struct StorageBackupSnapshot {
    state_sections: StorageBackupSections,
    history_sections: Option<StorageBackupSections>,
}

impl StorageBackupSnapshot {
    #[must_use]
    pub const fn new(
        state_sections: StorageBackupSections,
        history_sections: Option<StorageBackupSections>,
    ) -> Self {
        Self {
            state_sections,
            history_sections,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageBackupSections, Option<StorageBackupSections>) {
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
    async fn create_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_debug_reports_shape_without_row_content() {
        let snapshot = StorageBackupSnapshot::new(
            BTreeMap::from([(
                "secret-state-section".to_string(),
                vec![serde_json::json!({"secret": "state"})],
            )]),
            Some(BTreeMap::from([(
                "secret-history-section".to_string(),
                vec![serde_json::json!({"secret": "history"})],
            )])),
        );

        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("secret"));
        assert!(debug.contains("state_section_count: 1"));
        assert!(debug.contains("history_row_count: Some(1)"));
    }
}
