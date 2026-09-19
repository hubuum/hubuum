use hubuum_domain::JsonSchemaLimits;
mod baseline;
mod budget;
pub use budget::{StorageBackupBudget, StorageBackupCaptureProgress};
mod revisions;
mod schemas;
mod task_discovery;

use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use hubuum_events_core::CorrelationId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::task_control::SNAPSHOT_FIELDS;
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
        ClassSchemaRevisions => "class_schema_revisions",
        ClassSchemaState => "class_schema_state",
        ComputedFieldDefinitions => "computed_field_definitions",
        ClassRelations => "class_relations",
        Objects => "objects",
        ObjectSchemaEvidence => "object_schema_evidence",
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
        ClassSchemaHistory => "class_schema_history",
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
        match value {
            Value::Object(fields) => Ok(Self(fields)),
            _ => Err(StorageValidationError::invalid(
                "A backup section item must be a JSON object",
            )),
        }
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    #[must_use]
    pub fn fields(&self) -> &Map<String, Value> {
        &self.0
    }

    /// Apply version-5 legacy repairs shared by restore and source comparison.
    /// Clear previously accepted invalid correlation strings and supply the
    /// conservative default for remote results without dispatch evidence.
    /// Other field types remain intact so malformed rows and unrelated drift fail.
    pub fn normalize_legacy_history(&mut self, section: StorageBackupHistorySection) {
        if section == StorageBackupHistorySection::RemoteCallResults {
            self.0
                .entry("side_effect_state".to_string())
                .or_insert_with(|| Value::String("legacy_unknown".to_string()));
        }
        if section == StorageBackupHistorySection::AuditEvents
            && self
                .0
                .get("correlation_id")
                .and_then(Value::as_str)
                .is_some_and(|value| CorrelationId::new(value).is_err())
        {
            self.0.insert("correlation_id".to_string(), Value::Null);
        }
    }

    /// Canonicalize optional history fields added within backup version 5.
    /// An absent trace link and an all-null link carry the same information;
    /// populated or partial links must remain intact for validation/comparison.
    /// Empty task control and legacy-unknown remote evidence likewise carry no
    /// additional information; populated values must never disappear.
    pub fn canonicalize_history(&mut self, section: StorageBackupHistorySection) {
        if section == StorageBackupHistorySection::TerminalTasks {
            if self.0.get("discovery_metadata").is_some_and(Value::is_null) {
                self.0.remove("discovery_metadata");
            }
            for field in SNAPSHOT_FIELDS {
                if self.0.get(*field).is_some_and(Value::is_null) {
                    self.0.remove(*field);
                }
            }
        }
        if section == StorageBackupHistorySection::RemoteCallResults
            && self.0.get("side_effect_state").and_then(Value::as_str) == Some("legacy_unknown")
        {
            self.0.remove("side_effect_state");
        }
        if !matches!(
            section,
            StorageBackupHistorySection::TerminalTasks | StorageBackupHistorySection::AuditEvents
        ) {
            return;
        }
        const TRACE_FIELDS: [&str; 4] = [
            "trace_id",
            "trace_span_id",
            "trace_flags",
            "trace_context_version",
        ];
        if TRACE_FIELDS
            .iter()
            .all(|field| self.0.get(*field).is_none_or(Value::is_null))
        {
            for field in TRACE_FIELDS {
                self.0.remove(field);
            }
        }
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
#[derive(Clone)]
pub struct StorageBackupSnapshot {
    schema_limits: JsonSchemaLimits,
    state_sections: StorageBackupStateSections,
    history_sections: Option<StorageBackupHistorySections>,
}

impl PartialEq for StorageBackupSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.state_sections == other.state_sections
            && self.history_sections == other.history_sections
    }
}

impl StorageBackupSnapshot {
    #[must_use]
    pub const fn schema_limits(&self) -> JsonSchemaLimits {
        self.schema_limits
    }

    /// Validate the schema proof under a restore destination's deployment budgets.
    pub fn with_schema_limits(
        mut self,
        limits: JsonSchemaLimits,
    ) -> Result<Self, StorageValidationError> {
        if self.schema_limits != limits {
            self.schema_limits = limits;
            schemas::validate(&self)?;
        }
        Ok(self)
    }
    #[must_use]
    pub const fn includes_history(&self) -> bool {
        self.history_sections.is_some()
    }

    pub fn try_new(
        state_sections: StorageBackupStateSections,
        history_sections: Option<StorageBackupHistorySections>,
    ) -> Result<Self, StorageValidationError> {
        Self::try_new_with_limits(
            state_sections,
            history_sections,
            JsonSchemaLimits::default(),
        )
    }

    pub fn try_new_with_limits(
        state_sections: StorageBackupStateSections,
        mut history_sections: Option<StorageBackupHistorySections>,
        schema_limits: JsonSchemaLimits,
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

        if let Some(history) = &mut history_sections {
            Self::canonicalize_discovery_history(history)?;
            for (section, rows) in history {
                for row in rows {
                    row.canonicalize_history(*section);
                    if *section == StorageBackupHistorySection::TerminalTasks {
                        crate::task_control::validate_control_snapshot(row)?;
                        if let Some(value) = row.0.get("discovery_metadata") {
                            let kind = row
                                .0
                                .get("kind")
                                .and_then(Value::as_str)
                                .and_then(crate::StorageTaskKind::from_persisted)
                                .ok_or_else(|| {
                                    StorageValidationError::invalid("Invalid task metadata kind")
                                })?;
                            let metadata =
                                crate::StorageTaskMetadata::from_persisted(kind, value.clone())?;
                            row.0
                                .insert("discovery_metadata".to_string(), metadata.to_value());
                        }
                    }
                }
            }
        }

        let snapshot = Self {
            schema_limits,
            state_sections,
            history_sections,
        };
        revisions::validate_backup_revisions(&snapshot)?;
        schemas::validate(&snapshot)?;
        Ok(snapshot)
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
/// Capture every section from one consistent view. When history is requested,
/// every live temporal resource must have exactly one matching open snapshot,
/// including resources restored from a history-free backup. Missing or
/// contradictory history is a contract failure, never synthesized on capture.
/// The explicit budget applies during enumeration and retention, including
/// excluded rows inspected to select terminal history. Abort with InputTooLarge
/// on exhaustion; capturing everything before checking the budget is invalid.
#[async_trait]
pub trait BackupSnapshotStorage: Send + Sync {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
        budget: StorageBackupBudget,
    ) -> Result<StorageBackupSnapshot, StorageError>;
}

#[cfg(test)]
pub(crate) fn with_test_schema_sections(
    mut state: StorageBackupStateSections,
) -> StorageBackupStateSections {
    for class in state.get_mut(&StorageBackupStateSection::Classes).unwrap() {
        class.0.insert("validate_schema".into(), Value::Bool(false));
        class.0.insert("json_schema".into(), Value::Null);
    }
    let ids = state[&StorageBackupStateSection::Classes]
        .iter()
        .map(|class| class.get("id").unwrap().clone())
        .collect::<Vec<_>>();
    for id in ids {
        state.get_mut(&StorageBackupStateSection::ClassSchemaRevisions).unwrap().push(StorageBackupRow::try_from_value(serde_json::json!({
            "class_id": id, "revision": 1, "json_schema": null, "validate_schema": false,
            "status": "active", "created_at": "2026-01-01T00:00:00Z", "created_by": null,
            "activated_at": "2026-01-01T00:00:00Z", "activation_policy": "reject_incompatible"
        })).unwrap());
        state.get_mut(&StorageBackupStateSection::ClassSchemaState).unwrap().push(StorageBackupRow::try_from_value(serde_json::json!({
            "class_id": id, "active_revision": 1, "last_revision": 1, "object_epoch": 0, "object_count": 0
        })).unwrap());
    }
    state
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case::tasks(StorageBackupHistorySection::TerminalTasks)]
    #[case::events(StorageBackupHistorySection::AuditEvents)]
    fn snapshots_omit_empty_trace_links(#[case] section: StorageBackupHistorySection) {
        let mut history = complete_history();
        history.insert(
            section,
            vec![
                StorageBackupRow::try_from_value(json!({
                    "id": 1, "trace_id": null, "trace_span_id": null,
                    "trace_flags": null, "trace_context_version": null,
                }))
                .unwrap(),
            ],
        );
        let (_, history) = StorageBackupSnapshot::try_new(complete_state(), Some(history))
            .unwrap()
            .into_parts();
        assert_eq!(
            history.unwrap()[&section][0].fields(),
            json!({"id": 1}).as_object().unwrap()
        );
    }

    #[rstest]
    #[case::populated(json!({
        "id": 1, "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "trace_span_id": "00f067aa0ba902b7", "trace_flags": 1, "trace_context_version": 0,
    }))]
    #[case::partial(json!({"id": 1, "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "trace_span_id": null}))]
    fn canonicalization_preserves_nonempty_trace_links(#[case] value: Value) {
        let mut row = StorageBackupRow::try_from_value(value.clone()).unwrap();
        row.canonicalize_history(StorageBackupHistorySection::AuditEvents);
        assert_eq!(row.into_value(), value);
    }

    #[rstest]
    fn canonicalization_omits_only_empty_task_control(
        #[values(
            "cancel_requested_at",
            "cancel_requested_by",
            "cancel_reason",
            "execution_deadline_at",
            "import_effects_committed_at",
            "remote_dispatched_at",
            "terminal_reason"
        )]
        field: &str,
        #[values(Value::Null, json!("retained"), json!(42))] value: Value,
    ) {
        let original = json!({"id": 1, field: value});
        let mut row = StorageBackupRow::try_from_value(original.clone()).unwrap();
        row.canonicalize_history(StorageBackupHistorySection::TerminalTasks);
        let expected = if value.is_null() {
            json!({"id": 1})
        } else {
            original
        };
        assert_eq!(row.into_value(), expected);
    }

    #[rstest]
    #[case::absent(json!({"id": 1}), json!({"id": 1, "side_effect_state": "legacy_unknown"}))]
    #[case::not_sent(json!({"side_effect_state": "not_sent"}), json!({"side_effect_state": "not_sent"}))]
    #[case::possibly_sent(json!({"side_effect_state": "possibly_sent"}), json!({"side_effect_state": "possibly_sent"}))]
    #[case::response(json!({"side_effect_state": "response_received"}), json!({"side_effect_state": "response_received"}))]
    #[case::null(json!({"side_effect_state": null}), json!({"side_effect_state": null}))]
    #[case::malformed(json!({"side_effect_state": 42}), json!({"side_effect_state": 42}))]
    fn legacy_remote_history_defaults_only_missing_evidence(
        #[case] original: Value,
        #[case] expected: Value,
    ) {
        let mut row = StorageBackupRow::try_from_value(original).unwrap();
        row.normalize_legacy_history(StorageBackupHistorySection::RemoteCallResults);
        assert_eq!(row.into_value(), expected);
    }

    #[rstest]
    #[case::legacy(json!("legacy_unknown"), json!({"id": 1}))]
    #[case::not_sent(json!("not_sent"), json!({"id": 1, "side_effect_state": "not_sent"}))]
    #[case::possibly_sent(json!("possibly_sent"), json!({"id": 1, "side_effect_state": "possibly_sent"}))]
    #[case::response(json!("response_received"), json!({"id": 1, "side_effect_state": "response_received"}))]
    #[case::null(Value::Null, json!({"id": 1, "side_effect_state": null}))]
    #[case::malformed(json!(42), json!({"id": 1, "side_effect_state": 42}))]
    fn canonicalization_preserves_remote_side_effect_evidence(
        #[case] value: Value,
        #[case] expected: Value,
    ) {
        let mut row =
            StorageBackupRow::try_from_value(json!({"id": 1, "side_effect_state": value})).unwrap();
        row.canonicalize_history(StorageBackupHistorySection::RemoteCallResults);
        assert_eq!(row.into_value(), expected);
    }

    #[rstest]
    #[case::whitespace(json!("legacy correlation"), Value::Null)]
    #[case::overlong(json!("x".repeat(129)), Value::Null)]
    #[case::empty(json!(""), Value::Null)]
    #[case::valid(json!("valid-correlation"), json!("valid-correlation"))]
    #[case::null(Value::Null, Value::Null)]
    #[case::malformed(json!(42), json!(42))]
    fn legacy_history_normalization_is_limited_to_invalid_audit_correlation_strings(
        #[case] original: Value,
        #[case] normalized: Value,
        #[values(
            StorageBackupHistorySection::AuditEvents,
            StorageBackupHistorySection::TerminalTasks
        )]
        section: StorageBackupHistorySection,
    ) {
        let mut row =
            StorageBackupRow::try_from_value(json!({"correlation_id": original})).unwrap();
        row.normalize_legacy_history(section);
        let expected = if section == StorageBackupHistorySection::AuditEvents {
            normalized
        } else {
            original
        };
        assert_eq!(row.get("correlation_id"), Some(&expected));
    }

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
            vec![
                StorageBackupRow::try_from_value(
                    serde_json::json!({"id": 1, "revision": 7, "secret": "state"}),
                )
                .unwrap(),
            ],
        );
        let mut history = complete_history();
        history.insert(
            StorageBackupHistorySection::ClassHistory,
            vec![
                StorageBackupRow::try_from_value(serde_json::json!({"id": 1, "revision": 7, "valid_to": null, "operation": "create", "secret": "history"})).unwrap(),
            ],
        );
        let snapshot =
            StorageBackupSnapshot::try_new(with_test_schema_sections(state), Some(history))
                .unwrap();

        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("secret"));
        assert!(debug.contains("state_section_count: 23"));
        assert!(debug.contains("history_row_count: Some(1)"));
    }
}
