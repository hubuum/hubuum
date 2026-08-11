use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use serde_json::Value;

use crate::StorageError;

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
    async fn snapshot_backup(
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
