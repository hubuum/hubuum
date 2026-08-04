use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::NaiveDateTime;
use diesel::{Insertable, Queryable, Selectable};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::models::{REDACTED_DEBUG_VALUE, redacted_debug_option};
use crate::schema::{backup_task_outputs, restore_jobs, server_instances};

use super::principal::Principal;

pub const CURRENT_BACKUP_VERSION: i32 = 4;

pub(crate) const BACKUP_STATE_SECTIONS: &[&str] = &[
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

pub(crate) const BACKUP_TEMPORAL_HISTORY_SECTIONS: &[&str] = &[
    "collections_history",
    "hubuumclass_history",
    "hubuumclass_relation_history",
    "hubuumobject_history",
    "hubuumobject_relation_history",
    "export_templates_history",
    "remote_targets_history",
];

pub(crate) const BACKUP_AUXILIARY_HISTORY_SECTIONS: &[&str] = &[
    "tasks",
    "import_task_results",
    "export_task_outputs",
    "remote_call_results",
    "events",
    "event_deliveries",
];

pub(crate) fn backup_history_sections() -> impl Iterator<Item = &'static str> {
    BACKUP_TEMPORAL_HISTORY_SECTIONS
        .iter()
        .chain(BACKUP_AUXILIARY_HISTORY_SECTIONS)
        .copied()
}

pub(crate) fn is_backup_history_section(name: &str) -> bool {
    backup_history_sections().any(|known| known == name)
}

/// Immutable identity snapshot recorded with a restore stage and its provenance event.
pub struct RestoreInitiator {
    principal_id: Option<i32>,
    identity_scope: String,
    name: String,
}

impl RestoreInitiator {
    pub fn principal(
        principal: &Principal,
        identity_scope: impl Into<String>,
    ) -> Result<Self, ApiError> {
        Self::new(
            Some(principal.id),
            identity_scope.into(),
            principal.name.clone(),
        )
    }

    pub fn new(
        principal_id: Option<i32>,
        identity_scope: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, ApiError> {
        if principal_id.is_some_and(|id| id <= 0) {
            return Err(ApiError::BadRequest(
                "Restore initiator principal id must be greater than zero".to_string(),
            ));
        }
        let identity_scope = identity_scope.into();
        if identity_scope.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "Restore initiator identity scope must not be empty".to_string(),
            ));
        }
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "Restore initiator name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            principal_id,
            identity_scope,
            name,
        })
    }

    pub(crate) fn into_parts(self) -> (Option<i32>, String, String) {
        (self.principal_id, self.identity_scope, self.name)
    }
}

/// Typed input for staging an exact backup artifact under an initiator snapshot.
pub struct RestoreStageRequest {
    initiator: RestoreInitiator,
    document_bytes: Vec<u8>,
}

impl RestoreStageRequest {
    pub fn new(initiator: RestoreInitiator, document_bytes: Vec<u8>) -> Result<Self, ApiError> {
        if document_bytes.is_empty() {
            return Err(ApiError::BadRequest(
                "Restore upload must contain a backup document".to_string(),
            ));
        }
        Ok(Self {
            initiator,
            document_bytes,
        })
    }

    pub(crate) fn into_parts(self) -> (RestoreInitiator, Vec<u8>) {
        (self.initiator, self.document_bytes)
    }
}

const fn include_history_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BackupRequest {
    /// Full-system backups include durable audit, task, delivery, and temporal
    /// history by default. Set this explicitly to false only when the eventual
    /// restore is intended to reset that history.
    #[serde(default = "include_history_by_default")]
    pub include_history: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, Default)]
pub struct BackupManifest {
    #[schema(value_type = Object)]
    pub item_counts: BTreeMap<String, i64>,
    pub exclusions: Vec<String>,
}

/// Privileged, restore-only table snapshots. In backup version 4, each section
/// name and row shape corresponds to the PostgreSQL table restored from it.
/// These are versioned disaster-recovery internals, not portable import data.
#[derive(Clone, Serialize, Deserialize, PartialEq, ToSchema, Default)]
pub struct BackupHistory {
    #[schema(value_type = Object)]
    pub sections: BTreeMap<String, Vec<serde_json::Value>>,
}

impl fmt::Debug for BackupHistory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupHistory")
            .field("section_count", &self.sections.len())
            .field(
                "row_count",
                &self.sections.values().map(Vec::len).sum::<usize>(),
            )
            .finish()
    }
}

/// Exact logical rows used by the destructive full-system restore path.
#[derive(Clone, Serialize, Deserialize, PartialEq, ToSchema, Default)]
pub struct BackupState {
    #[schema(value_type = Object)]
    pub sections: BTreeMap<String, Vec<serde_json::Value>>,
}

impl fmt::Debug for BackupState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupState")
            .field("section_count", &self.sections.len())
            .field(
                "row_count",
                &self.sections.values().map(Vec::len).sum::<usize>(),
            )
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BackupDocument {
    pub backup_version: i32,
    pub created_at: NaiveDateTime,
    pub source_version: String,
    pub state: BackupState,
    pub history: Option<BackupHistory>,
    pub manifest: BackupManifest,
}

impl fmt::Debug for BackupDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackupDocument")
            .field("backup_version", &self.backup_version)
            .field("created_at", &self.created_at)
            .field("source_version", &self.source_version)
            .field("state", &self.state)
            .field("history", &self.history)
            .field("manifest", &self.manifest)
            .finish()
    }
}

impl BackupDocument {
    pub fn validate_version(&self) -> Result<(), ApiError> {
        if self.backup_version != CURRENT_BACKUP_VERSION {
            return Err(ApiError::BadRequest(format!(
                "Unsupported backup version '{}'; expected {}",
                self.backup_version, CURRENT_BACKUP_VERSION
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{BackupRequest, RestoreInitiator, RestoreJobID, RestoreStageRequest};

    #[test]
    fn backup_requests_include_history_by_default() {
        let request: BackupRequest = serde_json::from_str("{}").unwrap();

        assert!(request.include_history);
    }

    #[rstest]
    #[case::scope(r#"{"scope":{"kind":"collections","collection_ids":[1]}}"#)]
    #[case::embedded_import(r#"{"import_request":{"version":1,"graph":{}}}"#)]
    fn backup_requests_reject_non_backup_fields(#[case] payload: &str) {
        let request = serde_json::from_str::<BackupRequest>(payload);

        assert!(request.is_err());
    }

    #[rstest]
    #[case::non_positive_principal(Some(0), "local", "admin")]
    #[case::empty_scope(None, "", "hubuum-admin")]
    #[case::empty_name(None, "system", "  ")]
    fn restore_initiator_rejects_invalid_snapshots(
        #[case] principal_id: Option<i32>,
        #[case] identity_scope: &str,
        #[case] name: &str,
    ) {
        assert!(RestoreInitiator::new(principal_id, identity_scope, name).is_err());
    }

    #[test]
    fn restore_stage_request_rejects_an_empty_document() {
        let initiator =
            RestoreInitiator::new(None, "system", "hubuum-admin").expect("restore initiator");

        assert!(RestoreStageRequest::new(initiator, Vec::new()).is_err());
    }

    #[test]
    fn restore_job_id_rejects_non_positive_values_at_deserialization() {
        assert_eq!(RestoreJobID::new(1).unwrap().id(), 1);
        assert!(serde_json::from_str::<RestoreJobID>("0").is_err());
        assert!(serde_json::from_str::<RestoreJobID>("-1").is_err());
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = backup_task_outputs)]
pub struct BackupTaskOutputRecord {
    pub id: i32,
    pub task_id: i32,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = backup_task_outputs)]
pub struct NewBackupTaskOutputRecord {
    pub task_id: i32,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
}

/// Identifier wrapper for a staged restore job.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, ToSchema)]
#[schema(value_type = i64)]
pub struct RestoreJobID(i64);

impl RestoreJobID {
    pub fn new(id: i64) -> Result<Self, ApiError> {
        if id <= 0 {
            return Err(ApiError::BadRequest(format!(
                "Invalid restore job id '{id}': must be a positive integer"
            )));
        }
        Ok(Self(id))
    }

    pub fn id(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for RestoreJobID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id = <i64 as Deserialize>::deserialize(deserializer)?;
        Self::new(id).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone)]
pub enum BackupOutputLookup<T> {
    Available(T),
    Expired { expires_at: NaiveDateTime },
    Missing,
}

impl<T> BackupOutputLookup<T> {
    pub fn as_ref(&self) -> BackupOutputLookup<&T> {
        match self {
            Self::Available(value) => BackupOutputLookup::Available(value),
            Self::Expired { expires_at } => BackupOutputLookup::Expired {
                expires_at: *expires_at,
            },
            Self::Missing => BackupOutputLookup::Missing,
        }
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = restore_jobs)]
pub struct RestoreJobRecord {
    pub id: i64,
    pub status: String,
    pub requested_by: Option<i32>,
    pub requested_by_identity_scope: String,
    pub requested_by_name: String,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub capability_hash: String,
    pub error: Option<String>,
    pub expires_at: NaiveDateTime,
    pub confirmed_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = restore_jobs)]
pub struct NewRestoreJobRecord {
    pub status: String,
    pub requested_by: Option<i32>,
    pub requested_by_identity_scope: String,
    pub requested_by_name: String,
    pub document: Vec<u8>,
    pub byte_size: i64,
    pub sha256: String,
    pub capability_hash: String,
    pub validation_summary: serde_json::Value,
    pub expires_at: NaiveDateTime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RestoreJobStatus {
    Validated,
    Confirmed,
    Succeeded,
    Failed,
    Expired,
}

impl RestoreJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Confirmed => "confirmed",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }
}

impl FromStr for RestoreJobStatus {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "validated" => Ok(Self::Validated),
            "confirmed" => Ok(Self::Confirmed),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown restore job status '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RestoreValidationSummary {
    pub backup_version: i32,
    pub source_version: String,
    pub includes_history: bool,
    pub total_items: i64,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RestoreStageResponse {
    pub id: i64,
    pub status: RestoreJobStatus,
    pub requested_by: Option<i32>,
    pub requested_by_identity_scope: String,
    pub requested_by_name: String,
    pub sha256: String,
    pub byte_size: i64,
    pub expires_at: NaiveDateTime,
    pub error: Option<String>,
    pub confirmed_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub validation: RestoreValidationSummary,
    /// Returned only when a stage is created. It is stored only as a hash and
    /// must be supplied to confirm or inspect the restore while its staging
    /// record exists. A successful restore deletes every staging record.
    pub restore_capability: Option<String>,
}

impl fmt::Debug for RestoreStageResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestoreStageResponse")
            .field("id", &self.id)
            .field("status", &self.status)
            .field("requested_by", &self.requested_by)
            .field(
                "requested_by_identity_scope",
                &self.requested_by_identity_scope,
            )
            .field("requested_by_name", &self.requested_by_name)
            .field("sha256", &self.sha256)
            .field("byte_size", &self.byte_size)
            .field("expires_at", &self.expires_at)
            .field("error", &redacted_debug_option(&self.error))
            .field("confirmed_at", &self.confirmed_at)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("validation", &self.validation)
            .field(
                "restore_capability",
                &redacted_debug_option(&self.restore_capability),
            )
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RestoreConfirmRequest {
    pub restore_capability: String,
    pub sha256: String,
    pub confirmation: String,
}

impl fmt::Debug for RestoreConfirmRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestoreConfirmRequest")
            .field("restore_capability", &REDACTED_DEBUG_VALUE)
            .field("sha256", &self.sha256)
            .field("confirmation", &self.confirmation)
            .finish()
    }
}

pub const RESTORE_CONFIRMATION_PHRASE: &str = "REPLACE ALL HUBUUM DATA";

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = server_instances)]
pub struct ServerInstanceRecord {
    pub instance_id: Uuid,
    pub maintenance_generation: i64,
    pub drained: bool,
    pub last_heartbeat_at: NaiveDateTime,
    pub started_at: NaiveDateTime,
}

#[cfg(test)]
mod sensitive_debug_tests {
    use super::*;

    fn timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn backup_debug_reports_shape_without_snapshot_rows() {
        let document = BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: timestamp(),
            source_version: "test".to_string(),
            state: BackupState {
                sections: BTreeMap::from([(
                    "users".to_string(),
                    vec![serde_json::json!({"password": "backup-state-secret"})],
                )]),
            },
            history: Some(BackupHistory {
                sections: BTreeMap::from([(
                    "remote_call_results".to_string(),
                    vec![serde_json::json!({"body": "backup-history-secret"})],
                )]),
            }),
            manifest: BackupManifest::default(),
        };

        let debug = format!("{document:?}");

        assert!(debug.contains("section_count: 1"));
        assert!(debug.contains("row_count: 1"));
        assert!(!debug.contains("backup-state-secret"));
        assert!(!debug.contains("backup-history-secret"));
    }

    #[test]
    fn restore_debug_redacts_capabilities_and_persisted_errors() {
        let response = RestoreStageResponse {
            id: 1,
            status: RestoreJobStatus::Validated,
            requested_by: Some(2),
            requested_by_identity_scope: "local".to_string(),
            requested_by_name: "admin".to_string(),
            sha256: "document-sha".to_string(),
            byte_size: 42,
            expires_at: timestamp(),
            error: Some("restore-internal-secret".to_string()),
            confirmed_at: None,
            started_at: None,
            finished_at: None,
            created_at: timestamp(),
            updated_at: timestamp(),
            validation: RestoreValidationSummary {
                backup_version: CURRENT_BACKUP_VERSION,
                source_version: "test".to_string(),
                includes_history: true,
                total_items: 1,
            },
            restore_capability: Some("restore-capability-secret".to_string()),
        };
        let confirmation = RestoreConfirmRequest {
            restore_capability: "confirm-capability-secret".to_string(),
            sha256: "document-sha".to_string(),
            confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
        };

        let response_debug = format!("{response:?}");
        let confirmation_debug = format!("{confirmation:?}");

        assert!(response_debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!response_debug.contains("restore-internal-secret"));
        assert!(!response_debug.contains("restore-capability-secret"));
        assert!(confirmation_debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!confirmation_debug.contains("confirm-capability-secret"));
    }
}
