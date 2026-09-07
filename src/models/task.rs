use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;
use crate::events::{
    Event, MutationProvenance, PrincipalNames, Provenance, StoredProvenance, TraceLink,
};
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    BackupOutputLookup, REDACTED_DEBUG_VALUE, ResourceRevision, redacted_debug_option,
};
use crate::permissions::{AuthzTarget, ResourceRef};
use crate::traits::SelfAccessors;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Import,
    Export,
    Backup,
    Reindex,
    RemoteCall,
}

impl TaskKind {
    pub const ALL: [Self; 5] = [
        Self::Import,
        Self::Export,
        Self::Backup,
        Self::Reindex,
        Self::RemoteCall,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::Import => "import",
            TaskKind::Export => "export",
            TaskKind::Backup => "backup",
            TaskKind::Reindex => "reindex",
            TaskKind::RemoteCall => "remote_call",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "import" => Ok(TaskKind::Import),
            "export" => Ok(TaskKind::Export),
            "backup" => Ok(TaskKind::Backup),
            "reindex" => Ok(TaskKind::Reindex),
            "remote_call" => Ok(TaskKind::RemoteCall),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown task kind '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Validating,
    Running,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
}

impl TaskStatus {
    pub const ALL: [Self; 7] = [
        Self::Queued,
        Self::Validating,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];

    pub const ACTIVE: [Self; 2] = [Self::Validating, Self::Running];
    pub const NON_TERMINAL: [Self; 3] = [Self::Queued, Self::Validating, Self::Running];

    pub const TERMINAL: [Self; 4] = [
        Self::Succeeded,
        Self::Failed,
        Self::PartiallySucceeded,
        Self::Cancelled,
    ];

    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Queued | Self::Validating | Self::Running => false,
            Self::Succeeded | Self::Failed | Self::PartiallySucceeded | Self::Cancelled => true,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Queued => "queued",
            TaskStatus::Validating => "validating",
            TaskStatus::Running => "running",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
            TaskStatus::PartiallySucceeded => "partially_succeeded",
            TaskStatus::Cancelled => "cancelled",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "queued" => Ok(TaskStatus::Queued),
            "validating" => Ok(TaskStatus::Validating),
            "running" => Ok(TaskStatus::Running),
            "succeeded" => Ok(TaskStatus::Succeeded),
            "failed" => Ok(TaskStatus::Failed),
            "partially_succeeded" => Ok(TaskStatus::PartiallySucceeded),
            "cancelled" => Ok(TaskStatus::Cancelled),
            _ => Err(ApiError::InternalServerError(format!(
                "Unknown task status '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskResultCounts {
    processed: i32,
    success: i32,
    failed: i32,
}

impl TaskResultCounts {
    /// Build counts loaded from durable task state, where `processed` may not
    /// equal `success + failed` while a task is still running or recovering.
    pub fn from_stored<T, U, V>(processed: T, success: U, failed: V) -> Result<Self, ApiError>
    where
        T: TryInto<i32>,
        U: TryInto<i32>,
        V: TryInto<i32>,
    {
        Ok(Self {
            processed: task_result_count("processed", processed)?,
            success: task_result_count("success", success)?,
            failed: task_result_count("failed", failed)?,
        })
    }

    /// Build terminal counts and derive the processed total from the two
    /// mutually exclusive outcomes.
    pub fn from_outcomes<U, V>(success: U, failed: V) -> Result<Self, ApiError>
    where
        U: TryInto<i32>,
        V: TryInto<i32>,
    {
        let success = task_result_count("success", success)?;
        let failed = task_result_count("failed", failed)?;
        let processed = success.checked_add(failed).ok_or_else(|| {
            ApiError::InternalServerError("processed count is out of range".to_string())
        })?;
        Ok(Self {
            processed,
            success,
            failed,
        })
    }

    pub fn processed(self) -> i32 {
        self.processed
    }

    pub fn success(self) -> i32 {
        self.success
    }

    pub fn failed(self) -> i32 {
        self.failed
    }
}

fn task_result_count<T>(name: &str, value: T) -> Result<i32, ApiError>
where
    T: TryInto<i32>,
{
    let value = value
        .try_into()
        .map_err(|_| ApiError::InternalServerError(format!("{name} count is out of range")))?;
    if value < 0 {
        return Err(ApiError::InternalServerError(format!(
            "{name} count must not be negative"
        )));
    }
    Ok(value)
}

impl From<TaskResultCounts> for (i32, i32, i32) {
    fn from(value: TaskResultCounts) -> Self {
        (value.processed, value.success, value.failed)
    }
}

pub use hubuum_domain::TaskId as TaskID;

#[derive(Clone)]
pub struct TaskRecord {
    pub id: i32,
    pub kind: String,
    pub status: String,
    pub submitted_by: Option<i32>,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
    pub request_payload: Option<serde_json::Value>,
    pub summary: Option<String>,
    pub total_items: i32,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
    /// Scope snapshot of the submitting token (principal-centric). Captured at
    /// enqueue time so async execution can never exceed the token's scope.
    pub submitted_token_id: Option<i32>,
    pub submitted_token_scoped: bool,
    pub submitted_token_scopes: serde_json::Value,
    pub request_redacted_at: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub deleted_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub lease_expires_at: Option<NaiveDateTime>,
    pub attempt_count: i32,
    pub initiator_user_id: Option<i32>,
    pub(crate) trace_link: Option<TraceLink>,
}

impl fmt::Debug for TaskRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskRecord")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("submitted_by", &self.submitted_by)
            .field(
                "idempotency_key",
                &redacted_debug_option(&self.idempotency_key),
            )
            .field("request_hash", &redacted_debug_option(&self.request_hash))
            .field(
                "request_payload",
                &redacted_debug_option(&self.request_payload),
            )
            .field("summary", &self.summary)
            .field("total_items", &self.total_items)
            .field("processed_items", &self.processed_items)
            .field("success_items", &self.success_items)
            .field("failed_items", &self.failed_items)
            .field("submitted_token_id", &self.submitted_token_id)
            .field("submitted_token_scoped", &self.submitted_token_scoped)
            .field("submitted_token_scopes", &REDACTED_DEBUG_VALUE)
            .field("request_redacted_at", &self.request_redacted_at)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("deleted_at", &self.deleted_at)
            .field("deleted_by", &self.deleted_by)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("has_lease", &self.lease_expires_at.is_some())
            .field("lease_expires_at", &self.lease_expires_at)
            .field("attempt_count", &self.attempt_count)
            .field("initiator_user_id", &self.initiator_user_id)
            .field("has_trace_link", &self.trace_link.is_some())
            .finish()
    }
}

impl TaskRecord {
    pub(crate) fn worker_provenance(&self) -> MutationProvenance {
        MutationProvenance::worker(
            self.initiator_user_id.map(|initiator_user_id| {
                hubuum_domain::PrincipalId::new(initiator_user_id)
                    .expect("persisted task initiator id must be positive")
            }),
            hubuum_domain::TaskId::new(self.id).expect("persisted task id must be positive"),
        )
    }
}

#[derive(Clone)]
pub struct TaskEventRecord {
    pub id: i64,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub actor_user_id: Option<i32>,
    pub actor_kind: String,
    pub initiator_user_id: Option<i32>,
    pub provenance_task_id: Option<i32>,
}

pub struct NewTaskEventRecord {
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// Backend-neutral application projection of one persisted import result.
#[derive(Clone)]
pub struct ImportTaskResultRecord {
    pub id: i32,
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    pub details: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskProgress {
    pub total_items: i32,
    pub processed_items: i32,
    pub success_items: i32,
    pub failed_items: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskLinks {
    pub task: String,
    pub events: String,
    pub import: Option<String>,
    pub import_results: Option<String>,
    pub export: Option<String>,
    pub export_output: Option<String>,
    pub backup: Option<String>,
    pub backup_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ImportTaskDetails {
    pub results_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportTaskDetails {
    output_url: String,
    state: ExportTaskOutputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExportTaskOutputState {
    Missing,
    Expired {
        expires_at: NaiveDateTime,
    },
    Available {
        expires_at: NaiveDateTime,
        template_name: Option<String>,
        content_type: String,
        warning_count: i32,
        truncated: bool,
        total_duration_ms: i32,
        query_duration_ms: i32,
        hydration_duration_ms: i32,
        render_duration_ms: i32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
struct ExportTaskDetailsWire {
    output_url: String,
    output_available: bool,
    /// True when output was produced but has since passed its retention window. Distinguishes an
    /// expired export from one that was never generated (both have `output_available = false`).
    output_expired: bool,
    output_expires_at: Option<NaiveDateTime>,
    template_name: Option<String>,
    output_content_type: Option<String>,
    warning_count: Option<i32>,
    truncated: Option<bool>,
    total_duration_ms: Option<i32>,
    query_duration_ms: Option<i32>,
    hydration_duration_ms: Option<i32>,
    render_duration_ms: Option<i32>,
}

impl ExportTaskDetails {
    fn from_lookup(
        output_url: String,
        output: ExportOutputLookup<&ExportTaskOutputSummary>,
    ) -> Self {
        let state = match output {
            ExportOutputLookup::Available(summary) => ExportTaskOutputState::Available {
                expires_at: summary.output_expires_at,
                template_name: summary.template_name.clone(),
                content_type: summary.content_type.clone(),
                warning_count: summary.warning_count,
                truncated: summary.truncated,
                total_duration_ms: summary.total_duration_ms,
                query_duration_ms: summary.query_duration_ms,
                hydration_duration_ms: summary.hydration_duration_ms,
                render_duration_ms: summary.render_duration_ms,
            },
            ExportOutputLookup::Expired { expires_at } => {
                ExportTaskOutputState::Expired { expires_at }
            }
            ExportOutputLookup::Missing => ExportTaskOutputState::Missing,
        };
        Self { output_url, state }
    }

    #[must_use]
    pub fn state(&self) -> ExportTaskOutputStatus<'_> {
        match &self.state {
            ExportTaskOutputState::Missing => ExportTaskOutputStatus::Missing,
            ExportTaskOutputState::Expired { expires_at } => {
                ExportTaskOutputStatus::Expired { expires_at }
            }
            ExportTaskOutputState::Available {
                expires_at,
                template_name,
                content_type,
                warning_count,
                truncated,
                total_duration_ms,
                query_duration_ms,
                hydration_duration_ms,
                render_duration_ms,
            } => ExportTaskOutputStatus::Available {
                expires_at,
                template_name: template_name.as_deref(),
                content_type,
                warning_count: *warning_count,
                truncated: *truncated,
                total_duration_ms: *total_duration_ms,
                query_duration_ms: *query_duration_ms,
                hydration_duration_ms: *hydration_duration_ms,
                render_duration_ms: *render_duration_ms,
            },
        }
    }

    fn into_wire(self) -> ExportTaskDetailsWire {
        let mut wire = ExportTaskDetailsWire {
            output_url: self.output_url,
            output_available: false,
            output_expired: false,
            output_expires_at: None,
            template_name: None,
            output_content_type: None,
            warning_count: None,
            truncated: None,
            total_duration_ms: None,
            query_duration_ms: None,
            hydration_duration_ms: None,
            render_duration_ms: None,
        };
        match self.state {
            ExportTaskOutputState::Missing => {}
            ExportTaskOutputState::Expired { expires_at } => {
                wire.output_expired = true;
                wire.output_expires_at = Some(expires_at);
            }
            ExportTaskOutputState::Available {
                expires_at,
                template_name,
                content_type,
                warning_count,
                truncated,
                total_duration_ms,
                query_duration_ms,
                hydration_duration_ms,
                render_duration_ms,
            } => {
                wire.output_available = true;
                wire.output_expires_at = Some(expires_at);
                wire.template_name = template_name;
                wire.output_content_type = Some(content_type);
                wire.warning_count = Some(warning_count);
                wire.truncated = Some(truncated);
                wire.total_duration_ms = Some(total_duration_ms);
                wire.query_duration_ms = Some(query_duration_ms);
                wire.hydration_duration_ms = Some(hydration_duration_ms);
                wire.render_duration_ms = Some(render_duration_ms);
            }
        }
        wire
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTaskOutputStatus<'a> {
    Missing,
    Expired {
        expires_at: &'a NaiveDateTime,
    },
    Available {
        expires_at: &'a NaiveDateTime,
        template_name: Option<&'a str>,
        content_type: &'a str,
        warning_count: i32,
        truncated: bool,
        total_duration_ms: i32,
        query_duration_ms: i32,
        hydration_duration_ms: i32,
        render_duration_ms: i32,
    },
}

impl Serialize for ExportTaskDetails {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExportTaskDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExportTaskDetailsWire::deserialize(deserializer)?;
        let state = match (wire.output_available, wire.output_expired) {
            (false, false)
                if wire.output_expires_at.is_none()
                    && wire.template_name.is_none()
                    && wire.output_content_type.is_none()
                    && wire.warning_count.is_none()
                    && wire.truncated.is_none()
                    && wire.total_duration_ms.is_none()
                    && wire.query_duration_ms.is_none()
                    && wire.hydration_duration_ms.is_none()
                    && wire.render_duration_ms.is_none() =>
            {
                ExportTaskOutputState::Missing
            }
            (false, true)
                if wire.template_name.is_none()
                    && wire.output_content_type.is_none()
                    && wire.warning_count.is_none()
                    && wire.truncated.is_none()
                    && wire.total_duration_ms.is_none()
                    && wire.query_duration_ms.is_none()
                    && wire.hydration_duration_ms.is_none()
                    && wire.render_duration_ms.is_none() =>
            {
                ExportTaskOutputState::Expired {
                    expires_at: wire.output_expires_at.ok_or_else(|| {
                        serde::de::Error::custom("expired export output requires an expiry")
                    })?,
                }
            }
            (true, false) => ExportTaskOutputState::Available {
                expires_at: wire.output_expires_at.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires an expiry")
                })?,
                template_name: wire.template_name,
                content_type: wire.output_content_type.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires a content type")
                })?,
                warning_count: wire.warning_count.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires a warning count")
                })?,
                truncated: wire.truncated.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires truncation state")
                })?,
                total_duration_ms: wire.total_duration_ms.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires total duration")
                })?,
                query_duration_ms: wire.query_duration_ms.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires query duration")
                })?,
                hydration_duration_ms: wire.hydration_duration_ms.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires hydration duration")
                })?,
                render_duration_ms: wire.render_duration_ms.ok_or_else(|| {
                    serde::de::Error::custom("available export output requires render duration")
                })?,
            },
            _ => {
                return Err(serde::de::Error::custom(
                    "export output state and metadata are inconsistent",
                ));
            }
        };
        Ok(Self {
            output_url: wire.output_url,
            state,
        })
    }
}

impl PartialSchema for ExportTaskDetails {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        ExportTaskDetailsWire::schema()
    }
}

impl ToSchema for ExportTaskDetails {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupTaskDetails {
    output_url: String,
    state: BackupTaskOutputState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackupTaskOutputState {
    Missing,
    Expired {
        expires_at: NaiveDateTime,
    },
    Available {
        expires_at: NaiveDateTime,
        byte_size: i64,
        sha256: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
struct BackupTaskDetailsWire {
    output_url: String,
    output_available: bool,
    output_expired: bool,
    output_expires_at: Option<NaiveDateTime>,
    byte_size: Option<i64>,
    sha256: Option<String>,
}

impl BackupTaskDetails {
    fn from_lookup(
        output_url: String,
        output: BackupOutputLookup<&BackupTaskOutputSummary>,
    ) -> Self {
        let state = match output {
            BackupOutputLookup::Available(summary) => BackupTaskOutputState::Available {
                expires_at: summary.output_expires_at,
                byte_size: summary.byte_size,
                sha256: summary.sha256.clone(),
            },
            BackupOutputLookup::Expired { expires_at } => {
                BackupTaskOutputState::Expired { expires_at }
            }
            BackupOutputLookup::Missing => BackupTaskOutputState::Missing,
        };
        Self { output_url, state }
    }

    #[must_use]
    pub fn state(&self) -> BackupTaskOutputStatus<'_> {
        match &self.state {
            BackupTaskOutputState::Missing => BackupTaskOutputStatus::Missing,
            BackupTaskOutputState::Expired { expires_at } => {
                BackupTaskOutputStatus::Expired { expires_at }
            }
            BackupTaskOutputState::Available {
                expires_at,
                byte_size,
                sha256,
            } => BackupTaskOutputStatus::Available {
                expires_at,
                byte_size: *byte_size,
                sha256,
            },
        }
    }

    fn into_wire(self) -> BackupTaskDetailsWire {
        let mut wire = BackupTaskDetailsWire {
            output_url: self.output_url,
            output_available: false,
            output_expired: false,
            output_expires_at: None,
            byte_size: None,
            sha256: None,
        };
        match self.state {
            BackupTaskOutputState::Missing => {}
            BackupTaskOutputState::Expired { expires_at } => {
                wire.output_expired = true;
                wire.output_expires_at = Some(expires_at);
            }
            BackupTaskOutputState::Available {
                expires_at,
                byte_size,
                sha256,
            } => {
                wire.output_available = true;
                wire.output_expires_at = Some(expires_at);
                wire.byte_size = Some(byte_size);
                wire.sha256 = Some(sha256);
            }
        }
        wire
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupTaskOutputStatus<'a> {
    Missing,
    Expired {
        expires_at: &'a NaiveDateTime,
    },
    Available {
        expires_at: &'a NaiveDateTime,
        byte_size: i64,
        sha256: &'a str,
    },
}

impl Serialize for BackupTaskDetails {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BackupTaskDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackupTaskDetailsWire::deserialize(deserializer)?;
        let state = match (wire.output_available, wire.output_expired) {
            (false, false)
                if wire.output_expires_at.is_none()
                    && wire.byte_size.is_none()
                    && wire.sha256.is_none() =>
            {
                BackupTaskOutputState::Missing
            }
            (false, true) if wire.byte_size.is_none() && wire.sha256.is_none() => {
                BackupTaskOutputState::Expired {
                    expires_at: wire.output_expires_at.ok_or_else(|| {
                        serde::de::Error::custom("expired backup output requires an expiry")
                    })?,
                }
            }
            (true, false) => BackupTaskOutputState::Available {
                expires_at: wire.output_expires_at.ok_or_else(|| {
                    serde::de::Error::custom("available backup output requires an expiry")
                })?,
                byte_size: wire.byte_size.ok_or_else(|| {
                    serde::de::Error::custom("available backup output requires a byte size")
                })?,
                sha256: wire.sha256.ok_or_else(|| {
                    serde::de::Error::custom("available backup output requires a digest")
                })?,
            },
            _ => {
                return Err(serde::de::Error::custom(
                    "backup output state and metadata are inconsistent",
                ));
            }
        };
        Ok(Self {
            output_url: wire.output_url,
            state,
        })
    }
}

impl PartialSchema for BackupTaskDetails {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        BackupTaskDetailsWire::schema()
    }
}

impl ToSchema for BackupTaskDetails {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskDetails {
    Import(ImportTaskDetails),
    Export(ExportTaskDetails),
    Backup(BackupTaskDetails),
}

impl TaskDetails {
    #[must_use]
    pub const fn as_import(&self) -> Option<&ImportTaskDetails> {
        match self {
            Self::Import(details) => Some(details),
            Self::Export(_) | Self::Backup(_) => None,
        }
    }

    #[must_use]
    pub const fn as_export(&self) -> Option<&ExportTaskDetails> {
        match self {
            Self::Export(details) => Some(details),
            Self::Import(_) | Self::Backup(_) => None,
        }
    }

    #[must_use]
    pub const fn as_backup(&self) -> Option<&BackupTaskDetails> {
        match self {
            Self::Backup(details) => Some(details),
            Self::Import(_) | Self::Export(_) => None,
        }
    }

    #[must_use]
    pub fn into_import(self) -> Option<ImportTaskDetails> {
        match self {
            Self::Import(details) => Some(details),
            Self::Export(_) | Self::Backup(_) => None,
        }
    }

    #[must_use]
    pub fn into_export(self) -> Option<ExportTaskDetails> {
        match self {
            Self::Export(details) => Some(details),
            Self::Import(_) | Self::Backup(_) => None,
        }
    }

    #[must_use]
    pub fn into_backup(self) -> Option<BackupTaskDetails> {
        match self {
            Self::Backup(details) => Some(details),
            Self::Import(_) | Self::Export(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
struct TaskDetailsWire {
    import: Option<ImportTaskDetails>,
    export: Option<ExportTaskDetails>,
    backup: Option<BackupTaskDetails>,
}

impl Serialize for TaskDetails {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let wire = match self {
            Self::Import(details) => TaskDetailsWire {
                import: Some(details.clone()),
                export: None,
                backup: None,
            },
            Self::Export(details) => TaskDetailsWire {
                import: None,
                export: Some(details.clone()),
                backup: None,
            },
            Self::Backup(details) => TaskDetailsWire {
                import: None,
                export: None,
                backup: Some(details.clone()),
            },
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TaskDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TaskDetailsWire::deserialize(deserializer)?;
        match (wire.import, wire.export, wire.backup) {
            (Some(details), None, None) => Ok(Self::Import(details)),
            (None, Some(details), None) => Ok(Self::Export(details)),
            (None, None, Some(details)) => Ok(Self::Backup(details)),
            _ => Err(serde::de::Error::custom(
                "task details must contain exactly one detail kind",
            )),
        }
    }
}

impl PartialSchema for TaskDetails {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        TaskDetailsWire::schema()
    }
}

impl ToSchema for TaskDetails {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct TaskResponse {
    pub id: i32,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub submitted_by: Option<i32>,
    pub created_at: NaiveDateTime,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub progress: TaskProgress,
    pub summary: Option<String>,
    pub request_redacted_at: Option<NaiveDateTime>,
    pub links: TaskLinks,
    pub details: Option<TaskDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct TaskEventResponse {
    pub id: i64,
    pub task_id: i32,
    pub event_type: String,
    pub message: String,
    pub data: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ImportTaskResultResponse {
    pub id: i32,
    pub task_id: i32,
    pub item_ref: Option<String>,
    pub entity_kind: String,
    pub action: String,
    pub identifier: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_revision: Option<ResourceRevision>,
    pub details: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

/// Backend-neutral application projection of a retained export output.
pub struct ExportTaskOutput {
    pub content_type: String,
    pub json_output: Option<serde_json::Value>,
    pub text_output: Option<String>,
    pub meta_json: serde_json::Value,
    pub warnings_json: serde_json::Value,
    pub truncated: bool,
}

/// Outcome of looking up an export task's stored output.
///
/// Retention can lapse before the cleanup job purges the row, so a present-but-expired output is
/// distinct from one that never existed. Callers map `Expired` to `410 Gone` and `Missing` to
/// `404 Not Found` rather than collapsing both into a 404 that looks like data loss.
#[derive(Debug, Clone)]
pub enum ExportOutputLookup<T> {
    Available(T),
    Expired { expires_at: NaiveDateTime },
    Missing,
}

impl<T> ExportOutputLookup<T> {
    /// Borrow the contained value, mirroring `Option::as_ref`.
    pub fn as_ref(&self) -> ExportOutputLookup<&T> {
        match self {
            ExportOutputLookup::Available(value) => ExportOutputLookup::Available(value),
            ExportOutputLookup::Expired { expires_at } => ExportOutputLookup::Expired {
                expires_at: *expires_at,
            },
            ExportOutputLookup::Missing => ExportOutputLookup::Missing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExportTaskOutputSummary {
    pub task_id: i32,
    pub template_name: Option<String>,
    pub content_type: String,
    pub warning_count: i32,
    pub truncated: bool,
    pub output_expires_at: NaiveDateTime,
    pub total_duration_ms: i32,
    pub query_duration_ms: i32,
    pub hydration_duration_ms: i32,
    pub render_duration_ms: i32,
}

#[derive(Debug, Clone)]
pub struct BackupTaskOutputSummary {
    pub task_id: i32,
    pub byte_size: i64,
    pub sha256: String,
    pub output_expires_at: NaiveDateTime,
}

pub struct BackupTaskOutput {
    pub document: Vec<u8>,
    pub sha256: String,
}

impl TaskRecord {
    pub fn to_response(&self) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(ExportOutputLookup::Missing, BackupOutputLookup::Missing)
    }

    pub fn to_response_with_export_output(
        &self,
        export_output: ExportOutputLookup<&ExportTaskOutputSummary>,
    ) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(export_output, BackupOutputLookup::Missing)
    }

    pub fn to_response_with_backup_output(
        &self,
        backup_output: BackupOutputLookup<&BackupTaskOutputSummary>,
    ) -> Result<TaskResponse, ApiError> {
        self.to_response_with_outputs(ExportOutputLookup::Missing, backup_output)
    }

    pub(crate) fn to_response_with_outputs(
        &self,
        export_output: ExportOutputLookup<&ExportTaskOutputSummary>,
        backup_output: BackupOutputLookup<&BackupTaskOutputSummary>,
    ) -> Result<TaskResponse, ApiError> {
        let kind = TaskKind::from_db(&self.kind)?;
        let status = TaskStatus::from_db(&self.status)?;
        let task_url = format!("/api/v1/tasks/{}", self.id);
        let import_url = (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}", self.id));
        let import_results =
            (kind == TaskKind::Import).then(|| format!("/api/v1/imports/{}/results", self.id));
        let export_url = (kind == TaskKind::Export).then(|| format!("/api/v1/exports/{}", self.id));
        let export_output_url =
            (kind == TaskKind::Export).then(|| format!("/api/v1/exports/{}/output", self.id));
        let details = match kind {
            TaskKind::Import => import_results
                .clone()
                .map(|results_url| TaskDetails::Import(ImportTaskDetails { results_url })),
            TaskKind::Export => export_output_url.clone().map(|output_url| {
                TaskDetails::Export(ExportTaskDetails::from_lookup(output_url, export_output))
            }),
            TaskKind::Backup => {
                let output_url = format!("/api/v1/backups/{}/output", self.id);
                Some(TaskDetails::Backup(BackupTaskDetails::from_lookup(
                    output_url,
                    backup_output,
                )))
            }
            _ => None,
        };

        Ok(TaskResponse {
            id: self.id,
            kind,
            status,
            submitted_by: self.submitted_by,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            progress: TaskProgress {
                total_items: self.total_items,
                processed_items: self.processed_items,
                success_items: self.success_items,
                failed_items: self.failed_items,
            },
            summary: self.summary.clone(),
            request_redacted_at: self.request_redacted_at,
            links: TaskLinks {
                task: task_url.clone(),
                events: format!("{task_url}/events"),
                import: import_url.clone(),
                import_results: import_results.clone(),
                export: export_url,
                export_output: export_output_url,
                backup: (kind == TaskKind::Backup).then(|| format!("/api/v1/backups/{}", self.id)),
                backup_output: (kind == TaskKind::Backup)
                    .then(|| format!("/api/v1/backups/{}/output", self.id)),
            },
            details,
        })
    }
}

impl From<TaskEventRecord> for TaskEventResponse {
    fn from(value: TaskEventRecord) -> Self {
        Self::from_record_with_names(value, &PrincipalNames::default())
    }
}

impl TaskEventResponse {
    pub(crate) fn from_record_with_names(
        value: TaskEventRecord,
        principal_names: &PrincipalNames,
    ) -> Self {
        let provenance = StoredProvenance::from_actor_kind(Some(&value.actor_kind))
            .with_actor_user_id(value.actor_user_id)
            .with_initiator_user_id(value.initiator_user_id)
            .with_task_id(value.provenance_task_id)
            .resolve(principal_names);
        Self {
            id: value.id,
            task_id: value.task_id,
            event_type: value.event_type,
            message: value.message,
            data: value.data,
            created_at: value.created_at,
            provenance,
        }
    }
}

impl TryFrom<Event> for TaskEventRecord {
    type Error = ApiError;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        let Some(task_id) = value.entity_id else {
            return Err(ApiError::InternalServerError(
                "Task event is missing task id".to_string(),
            ));
        };
        let data = match value.metadata.get("data") {
            Some(serde_json::Value::Null) | None => None,
            Some(data) => Some(data.clone()),
        };

        Ok(Self {
            id: value.id,
            task_id,
            event_type: value.action,
            message: value.summary,
            data,
            created_at: value.occurred_at,
            actor_user_id: value.actor_user_id,
            actor_kind: value.actor_kind,
            initiator_user_id: value.initiator_user_id,
            provenance_task_id: value.task_id.or(Some(task_id)),
        })
    }
}

impl From<ImportTaskResultRecord> for ImportTaskResultResponse {
    fn from(value: ImportTaskResultRecord) -> Self {
        let mut details = value.details;
        let observed_revision = details
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|details| details.remove("observed_revision"))
            .and_then(|revision| serde_json::from_value(revision).ok());
        if details
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .is_some_and(serde_json::Map::is_empty)
        {
            details = None;
        }
        Self {
            id: value.id,
            task_id: value.task_id,
            item_ref: value.item_ref,
            entity_kind: value.entity_kind,
            action: value.action,
            identifier: value.identifier,
            outcome: value.outcome,
            error: value.error,
            observed_revision,
            details,
            created_at: value.created_at,
        }
    }
}

impl CursorPaginated for TaskResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Kind
                | FilterField::Status
                | FilterField::SubmittedBy
                | FilterField::CreatedAt
                | FilterField::StartedAt
                | FilterField::FinishedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            FilterField::Kind => Ok(CursorValue::String(self.kind.as_str().to_string())),
            FilterField::Status => Ok(CursorValue::String(self.status.as_str().to_string())),
            FilterField::SubmittedBy => Ok(match self.submitted_by {
                Some(value) => CursorValue::Integer(value as i64),
                None => CursorValue::Null,
            }),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::StartedAt => Ok(match self.started_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            }),
            FilterField::FinishedAt => Ok(match self.finished_at {
                Some(value) => CursorValue::DateTime(value),
                None => CursorValue::Null,
            }),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for tasks",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorPaginated for TaskRecord {
    fn supports_sort(field: &FilterField) -> bool {
        TaskResponse::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Kind => CursorValue::String(self.kind.clone()),
            FilterField::Status => CursorValue::String(self.status.clone()),
            FilterField::SubmittedBy => self
                .submitted_by
                .map(|value| CursorValue::Integer(value as i64))
                .unwrap_or(CursorValue::Null),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::StartedAt => self
                .started_at
                .map(CursorValue::DateTime)
                .unwrap_or(CursorValue::Null),
            FilterField::FinishedAt => self
                .finished_at
                .map(CursorValue::DateTime)
                .unwrap_or(CursorValue::Null),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Unsupported sort field '{}' for tasks",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        TaskResponse::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        TaskResponse::tie_breaker_sort()
    }
}

impl CursorPaginated for TaskEventResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for task events",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorPaginated for ImportTaskResultResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::Id)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for import results",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl IdAccessor for TaskRecord {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl IdAccessor for TaskID {
    fn accessor_id(&self) -> i32 {
        self.id()
    }
}

impl InstanceAdapter<TaskRecord> for TaskRecord {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskRecord, ApiError> {
        Ok(self.clone())
    }
}

impl InstanceAdapter<TaskRecord> for TaskID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<TaskRecord, ApiError> {
        crate::services::tasks::find_task(pool, *self).await
    }
}

#[async_trait]
impl AuthzTarget for TaskRecord {
    async fn to_resource_ref(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef::task(self.id, self.submitted_by))
    }
}

#[async_trait]
impl AuthzTarget for TaskID {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn generated_task_inventory_matches_runtime_kinds() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../../docs/generated/project_inventory.json"))
                .unwrap();
        assert_eq!(
            inventory["task_kinds"],
            serde_json::to_value(super::TaskKind::ALL).unwrap()
        );
    }

    use super::*;

    fn test_timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn task_result_counts_derive_processed_from_terminal_outcomes() {
        let counts = TaskResultCounts::from_outcomes(2, 1).unwrap();

        assert_eq!(
            (counts.processed(), counts.success(), counts.failed()),
            (3, 2, 1)
        );
    }

    #[test]
    fn task_result_counts_reject_negative_stored_values() {
        assert!(TaskResultCounts::from_stored(-1, 0, 0).is_err());
    }

    #[test]
    fn task_result_counts_reject_processed_overflow() {
        assert!(TaskResultCounts::from_outcomes(i32::MAX, 1).is_err());
    }

    #[test]
    fn terminal_task_statuses_are_stable() {
        assert_eq!(
            TaskStatus::TERMINAL.map(TaskStatus::as_str),
            ["succeeded", "failed", "partially_succeeded", "cancelled"]
        );
        assert_eq!(
            TaskStatus::ALL.map(TaskStatus::is_terminal),
            [false, false, false, true, true, true, true]
        );
    }

    #[test]
    fn active_task_statuses_are_stable() {
        assert_eq!(
            TaskStatus::ACTIVE.map(TaskStatus::as_str),
            ["validating", "running"]
        );
        assert_eq!(
            TaskStatus::NON_TERMINAL.map(TaskStatus::as_str),
            ["queued", "validating", "running"]
        );
    }

    #[test]
    fn task_details_deserialization_rejects_multiple_kinds() {
        let invalid = serde_json::json!({
            "import": {"results_url": "/imports/1/results"},
            "export": {
                "output_url": "/exports/1/output",
                "output_available": false,
                "output_expired": false,
                "output_expires_at": null,
                "template_name": null,
                "output_content_type": null,
                "warning_count": null,
                "truncated": null,
                "total_duration_ms": null,
                "query_duration_ms": null,
                "hydration_duration_ms": null,
                "render_duration_ms": null
            },
            "backup": null
        });

        let error = serde_json::from_value::<TaskDetails>(invalid).unwrap_err();

        assert!(error.to_string().contains("exactly one detail kind"));
    }

    #[test]
    fn export_details_deserialization_rejects_contradictory_output_state() {
        let invalid = serde_json::json!({
            "output_url": "/exports/1/output",
            "output_available": true,
            "output_expired": true,
            "output_expires_at": test_timestamp(),
            "template_name": null,
            "output_content_type": "application/json",
            "warning_count": 0,
            "truncated": false,
            "total_duration_ms": 1,
            "query_duration_ms": 1,
            "hydration_duration_ms": 0,
            "render_duration_ms": 0
        });

        let error = serde_json::from_value::<ExportTaskDetails>(invalid).unwrap_err();

        assert!(error.to_string().contains("inconsistent"));
    }

    #[test]
    fn task_id_new_accepts_positive() {
        assert_eq!(TaskID::new(1).unwrap().id(), 1);
        assert_eq!(TaskID::new(i32::MAX).unwrap().id(), i32::MAX);
    }

    #[test]
    fn task_id_new_rejects_non_positive() {
        for invalid in [0, -1, i32::MIN] {
            let err: ApiError = TaskID::new(invalid).unwrap_err().into();
            assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
        }
    }

    #[test]
    fn task_id_deserialize_routes_through_new() {
        // A valid id deserializes; a non-positive id is rejected by the validating constructor,
        // so an invalid path segment never produces a TaskID.
        assert_eq!(serde_json::from_str::<TaskID>("7").unwrap().id(), 7);
        assert!(serde_json::from_str::<TaskID>("0").is_err());
        assert!(serde_json::from_str::<TaskID>("-3").is_err());
    }

    #[test]
    fn task_record_debug_redacts_request_and_scope_material() {
        let timestamp = test_timestamp();
        let task = TaskRecord {
            id: 7,
            kind: TaskKind::Import.as_str().to_string(),
            status: TaskStatus::Running.as_str().to_string(),
            submitted_by: Some(1),
            idempotency_key: Some("idempotency-secret".to_string()),
            request_hash: Some("request-hash-secret".to_string()),
            request_payload: Some(serde_json::json!({"password": "payload-secret"})),
            summary: Some("safe summary".to_string()),
            total_items: 1,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            submitted_token_id: Some(3),
            submitted_token_scoped: true,
            submitted_token_scopes: serde_json::json!({"token": "scope-secret"}),
            request_redacted_at: None,
            started_at: Some(timestamp),
            finished_at: None,
            deleted_at: None,
            deleted_by: None,
            created_at: timestamp,
            updated_at: timestamp,
            lease_expires_at: Some(timestamp),
            attempt_count: 1,
            initiator_user_id: Some(1),
            trace_link: None,
        };

        let debug = format!("{task:?}");

        assert!(debug.contains("safe summary"));
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        for secret in [
            "idempotency-secret",
            "request-hash-secret",
            "payload-secret",
            "scope-secret",
        ] {
            assert!(!debug.contains(secret), "debug output exposed {secret}");
        }
    }

    #[test]
    fn export_task_details_include_persisted_phase_timings() {
        let timestamp = test_timestamp();
        let task = TaskRecord {
            id: 7,
            kind: TaskKind::Export.as_str().to_string(),
            status: TaskStatus::Succeeded.as_str().to_string(),
            submitted_by: Some(1),
            idempotency_key: None,
            request_hash: None,
            request_payload: None,
            summary: None,
            total_items: 1,
            processed_items: 1,
            success_items: 1,
            failed_items: 0,
            submitted_token_id: None,
            submitted_token_scoped: false,
            submitted_token_scopes: serde_json::json!([]),
            request_redacted_at: Some(timestamp),
            started_at: Some(timestamp),
            finished_at: Some(timestamp),
            deleted_at: None,
            deleted_by: None,
            created_at: timestamp,
            updated_at: timestamp,
            lease_expires_at: None,
            attempt_count: 1,
            initiator_user_id: Some(1),
            trace_link: None,
        };
        let output = ExportTaskOutputSummary {
            task_id: task.id,
            template_name: Some("inventory".to_string()),
            content_type: "text/plain".to_string(),
            warning_count: 0,
            truncated: false,
            output_expires_at: timestamp,
            total_duration_ms: 150,
            query_duration_ms: 40,
            hydration_duration_ms: 30,
            render_duration_ms: 70,
        };

        let response = task
            .to_response_with_export_output(ExportOutputLookup::Available(&output))
            .unwrap();
        let TaskDetails::Export(details) = response.details.unwrap() else {
            panic!("expected export task details");
        };
        let ExportTaskOutputStatus::Available {
            total_duration_ms,
            query_duration_ms,
            hydration_duration_ms,
            render_duration_ms,
            ..
        } = details.state()
        else {
            panic!("expected available export output");
        };

        assert_eq!(
            (
                total_duration_ms,
                query_duration_ms,
                hydration_duration_ms,
                render_duration_ms,
            ),
            (150, 40, 30, 70)
        );
    }
}
