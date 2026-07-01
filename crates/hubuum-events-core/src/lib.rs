//! Backend-agnostic event catalog and provenance types for Hubuum's unified
//! event & audit stream (issue #70).
//!
//! This crate owns the **authoritative** `EntityType` / `Action` catalog: the
//! set of valid event kinds and which actions are legal for each entity type.
//! It is intentionally free of Diesel, Actix, app configuration, and Hubuum's
//! `ApiError` so it can be shared by the producer (`emit_event`), the audit
//! read API (filter validation), and the fan-out worker (subscription
//! matching) without leaking backend concerns.
//!
//! The catalog mirrors the "Entity types & actions" table in the epic (#70):
//! `entity_type` is the API/concept name, **not** the table name (`class`, not
//! `hubuumclass`), and actions are **non-uniform** per entity type.

use std::fmt;

use chrono::NaiveDateTime;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The kind of actor that originated an event.
///
/// Stored as text on the `events.actor_kind` column. System actors cover
/// maintenance/migration paths; worker actors carry task causation in event
/// `metadata` (see #72/#87).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    User,
    System,
    Worker,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::User => "user",
            ActorKind::System => "system",
            ActorKind::Worker => "worker",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "user" => Ok(ActorKind::User),
            "system" => Ok(ActorKind::System),
            "worker" => Ok(ActorKind::Worker),
            other => Err(EventCatalogError::UnknownActorKind(other.to_string())),
        }
    }
}

/// Actor + request provenance attached to an event-producing mutation.
///
/// This type intentionally has no Actix, Diesel, or application-model
/// dependencies so producer code can pass it across future crate boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventContext {
    actor_kind: ActorKind,
    actor_user_id: Option<i32>,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
}

impl EventContext {
    pub fn user(
        actor_user_id: i32,
        request_id: Option<Uuid>,
        correlation_id: Option<String>,
    ) -> Self {
        Self::new(
            ActorKind::User,
            Some(actor_user_id),
            request_id,
            correlation_id,
        )
    }

    pub fn system() -> Self {
        Self::new(ActorKind::System, None, None, None)
    }

    pub fn worker(request_id: Option<Uuid>, correlation_id: Option<String>) -> Self {
        Self::new(ActorKind::Worker, None, request_id, correlation_id)
    }

    pub fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    pub fn actor_user_id(&self) -> Option<i32> {
        self.actor_user_id
    }

    pub fn request_id(&self) -> Option<Uuid> {
        self.request_id
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    fn new(
        actor_kind: ActorKind,
        actor_user_id: Option<i32>,
        request_id: Option<Uuid>,
        correlation_id: Option<String>,
    ) -> Self {
        Self {
            actor_kind,
            actor_user_id,
            request_id,
            correlation_id,
        }
    }
}

/// The conceptual entity type an event is about.
///
/// This is the API/concept name, **not** the table name (`class`, not
/// `hubuumclass`). Stored as text on `events.entity_type` and validated
/// against the catalog at emit time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Namespace,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    User,
    Group,
    UserGroup,
    Permission,
    Token,
    RemoteTarget,
    ReportTemplate,
    Task,
    ServiceAccount,
    EventSink,
    EventSubscription,
}

impl EntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            EntityType::Namespace => "namespace",
            EntityType::Class => "class",
            EntityType::Object => "object",
            EntityType::ClassRelation => "class_relation",
            EntityType::ObjectRelation => "object_relation",
            EntityType::User => "user",
            EntityType::Group => "group",
            EntityType::UserGroup => "user_group",
            EntityType::Permission => "permission",
            EntityType::Token => "token",
            EntityType::RemoteTarget => "remote_target",
            EntityType::ReportTemplate => "report_template",
            EntityType::Task => "task",
            EntityType::ServiceAccount => "service_account",
            EntityType::EventSink => "event_sink",
            EntityType::EventSubscription => "event_subscription",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "namespace" => Ok(EntityType::Namespace),
            "class" => Ok(EntityType::Class),
            "object" => Ok(EntityType::Object),
            "class_relation" => Ok(EntityType::ClassRelation),
            "object_relation" => Ok(EntityType::ObjectRelation),
            "user" => Ok(EntityType::User),
            "group" => Ok(EntityType::Group),
            "user_group" => Ok(EntityType::UserGroup),
            "permission" => Ok(EntityType::Permission),
            "token" => Ok(EntityType::Token),
            "remote_target" => Ok(EntityType::RemoteTarget),
            "report_template" => Ok(EntityType::ReportTemplate),
            "task" => Ok(EntityType::Task),
            "service_account" => Ok(EntityType::ServiceAccount),
            "event_sink" => Ok(EntityType::EventSink),
            "event_subscription" => Ok(EntityType::EventSubscription),
            other => Err(EventCatalogError::UnknownEntityType(other.to_string())),
        }
    }
}

/// The action an event records. Actions are **non-uniform** per entity type:
/// relations have no `Updated`; `permission` is grant/revoke; `user_group` is
/// add/remove; `token` is created/revoked; `remote_target` adds `Invoked`;
/// `task` is lifecycle-only (see #87).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Created,
    Updated,
    Deleted,
    Added,
    Removed,
    Granted,
    Revoked,
    Invoked,
    // task lifecycle (#87)
    Queued,
    Started,
    Validating,
    Running,
    Succeeded,
    Failed,
    PartiallySucceeded,
    Cancelled,
    Cleanup,
    Disabled,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Created => "created",
            Action::Updated => "updated",
            Action::Deleted => "deleted",
            Action::Added => "added",
            Action::Removed => "removed",
            Action::Granted => "granted",
            Action::Revoked => "revoked",
            Action::Invoked => "invoked",
            Action::Queued => "queued",
            Action::Started => "started",
            Action::Validating => "validating",
            Action::Running => "running",
            Action::Succeeded => "succeeded",
            Action::Failed => "failed",
            Action::PartiallySucceeded => "partially_succeeded",
            Action::Cancelled => "cancelled",
            Action::Cleanup => "cleanup",
            Action::Disabled => "disabled",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, EventCatalogError> {
        match value {
            "created" => Ok(Action::Created),
            "updated" => Ok(Action::Updated),
            "deleted" => Ok(Action::Deleted),
            "added" => Ok(Action::Added),
            "removed" => Ok(Action::Removed),
            "granted" => Ok(Action::Granted),
            "revoked" => Ok(Action::Revoked),
            "invoked" => Ok(Action::Invoked),
            "queued" => Ok(Action::Queued),
            "started" => Ok(Action::Started),
            "validating" => Ok(Action::Validating),
            "running" => Ok(Action::Running),
            "succeeded" => Ok(Action::Succeeded),
            "failed" => Ok(Action::Failed),
            "partially_succeeded" => Ok(Action::PartiallySucceeded),
            "cancelled" => Ok(Action::Cancelled),
            "cleanup" => Ok(Action::Cleanup),
            "disabled" => Ok(Action::Disabled),
            other => Err(EventCatalogError::UnknownAction(other.to_string())),
        }
    }
}

/// Returns the actions valid for `entity_type`, per the authoritative catalog.
///
/// This drives both audit-row emission (#73) and subscription/filter validation
/// (#74/#75): an `(entity_type, action)` pair outside this mapping is invalid.
pub fn valid_actions(entity_type: EntityType) -> &'static [Action] {
    use Action as A;
    use EntityType as E;
    match entity_type {
        E::Namespace | E::Class | E::Object | E::User | E::Group | E::ReportTemplate => {
            &[A::Created, A::Updated, A::Deleted]
        }
        E::ServiceAccount => &[A::Created, A::Updated, A::Disabled, A::Deleted],
        E::EventSink | E::EventSubscription => &[A::Created, A::Updated, A::Deleted],
        E::RemoteTarget => &[A::Created, A::Updated, A::Deleted, A::Invoked],
        E::ClassRelation | E::ObjectRelation => &[A::Created, A::Deleted],
        E::UserGroup => &[A::Added, A::Removed],
        E::Permission => &[A::Granted, A::Revoked],
        E::Token => &[A::Created, A::Revoked],
        E::Task => &[
            A::Queued,
            A::Started,
            A::Validating,
            A::Running,
            A::Succeeded,
            A::Failed,
            A::PartiallySucceeded,
            A::Cancelled,
            A::Cleanup,
        ],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventEnvelope {
    pub id: i64,
    pub event_id: Uuid,
    pub occurred_at: NaiveDateTime,
    pub entity_type: String,
    pub entity_id: Option<i32>,
    pub entity_name: Option<String>,
    pub namespace_id: Option<i32>,
    pub action: String,
    pub actor_user_id: Option<i32>,
    pub actor_kind: String,
    pub request_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub summary: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub metadata: serde_json::Value,
    pub schema_version: i32,
}

pub fn resolve_event_sink_secret(secret_ref: &str) -> Result<String, EventSinkSecretError> {
    let key = format!(
        "HUBUUM_EVENT_SINK_SECRET_{}",
        secret_ref.to_ascii_uppercase()
    );
    std::env::var(&key).map_err(|_| EventSinkSecretError::MissingSecret {
        secret_ref: secret_ref.to_string(),
    })
}

pub fn resolve_event_sink_secret_uri(
    uri: &str,
    secret_ref: Option<&str>,
    sink_label: &str,
) -> Result<String, EventSinkSecretError> {
    let contains_secret_placeholder = uri.contains("{secret}");
    match secret_ref {
        Some(secret_ref) => {
            if !contains_secret_placeholder {
                return Err(EventSinkSecretError::MissingSecretPlaceholder {
                    sink_label: sink_label.to_string(),
                });
            }
            let secret = resolve_event_sink_secret(secret_ref)?;
            let encoded = utf8_percent_encode(&secret, NON_ALPHANUMERIC).to_string();
            Ok(uri.replace("{secret}", &encoded))
        }
        None if contains_secret_placeholder => {
            Err(EventSinkSecretError::UnexpectedSecretPlaceholder {
                sink_label: sink_label.to_string(),
            })
        }
        None => Ok(uri.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventSinkSecretError {
    MissingSecret { secret_ref: String },
    MissingSecretPlaceholder { sink_label: String },
    UnexpectedSecretPlaceholder { sink_label: String },
}

impl fmt::Display for EventSinkSecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSecret { secret_ref } => write!(
                f,
                "Event sink secret reference '{secret_ref}' is not configured"
            ),
            Self::MissingSecretPlaceholder { sink_label } => write!(
                f,
                "Invalid {sink_label} config: uri must include {{secret}} when secret_ref is set"
            ),
            Self::UnexpectedSecretPlaceholder { sink_label } => write!(
                f,
                "Invalid {sink_label} config: uri includes {{secret}} without secret_ref"
            ),
        }
    }
}

impl std::error::Error for EventSinkSecretError {}

/// Validates that `action` is legal for `entity_type`.
pub fn is_valid_pair(entity_type: EntityType, action: Action) -> bool {
    valid_actions(entity_type).contains(&action)
}

/// Catalog-level validation errors. Callers map these into their public error
/// surface (e.g. Hubuum's `ApiError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventCatalogError {
    UnknownEntityType(String),
    UnknownAction(String),
    InvalidActionForType {
        entity_type: EntityType,
        action: Action,
    },
    UnknownActorKind(String),
}

impl fmt::Display for EventCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownEntityType(value) => {
                write!(f, "unknown event entity_type '{value}'")
            }
            Self::UnknownAction(value) => write!(f, "unknown event action '{value}'"),
            Self::InvalidActionForType {
                entity_type,
                action,
            } => write!(
                f,
                "action '{}' is not valid for entity_type '{}'",
                action.as_str(),
                entity_type.as_str()
            ),
            Self::UnknownActorKind(value) => write!(f, "unknown event actor_kind '{value}'"),
        }
    }
}

impl std::error::Error for EventCatalogError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_type_round_trips() {
        let all = [
            EntityType::Namespace,
            EntityType::Class,
            EntityType::Object,
            EntityType::ClassRelation,
            EntityType::ObjectRelation,
            EntityType::User,
            EntityType::Group,
            EntityType::UserGroup,
            EntityType::Permission,
            EntityType::Token,
            EntityType::RemoteTarget,
            EntityType::ReportTemplate,
            EntityType::Task,
            EntityType::ServiceAccount,
            EntityType::EventSink,
            EntityType::EventSubscription,
        ];
        for t in all {
            assert_eq!(EntityType::from_db(t.as_str()).unwrap(), t);
        }
        assert!(EntityType::from_db("hubuumclass").is_err());
    }

    #[test]
    fn action_round_trips() {
        let all = [
            Action::Created,
            Action::Updated,
            Action::Deleted,
            Action::Added,
            Action::Removed,
            Action::Granted,
            Action::Revoked,
            Action::Invoked,
            Action::Queued,
            Action::Started,
            Action::Validating,
            Action::Running,
            Action::Succeeded,
            Action::Failed,
            Action::PartiallySucceeded,
            Action::Cancelled,
            Action::Cleanup,
            Action::Disabled,
        ];
        for a in all {
            assert_eq!(Action::from_db(a.as_str()).unwrap(), a);
        }
        assert!(Action::from_db("patched").is_err());
    }

    #[test]
    fn relations_have_no_updated() {
        assert!(is_valid_pair(EntityType::ObjectRelation, Action::Created));
        assert!(is_valid_pair(EntityType::ObjectRelation, Action::Deleted));
        assert!(!is_valid_pair(EntityType::ObjectRelation, Action::Updated));
        assert!(!is_valid_pair(EntityType::ClassRelation, Action::Updated));
    }

    #[test]
    fn permission_is_grant_revoke() {
        assert!(is_valid_pair(EntityType::Permission, Action::Granted));
        assert!(is_valid_pair(EntityType::Permission, Action::Revoked));
        assert!(!is_valid_pair(EntityType::Permission, Action::Created));
        assert!(!is_valid_pair(EntityType::Permission, Action::Updated));
    }

    #[test]
    fn token_has_no_updated_or_deleted() {
        assert!(is_valid_pair(EntityType::Token, Action::Created));
        assert!(is_valid_pair(EntityType::Token, Action::Revoked));
        assert!(!is_valid_pair(EntityType::Token, Action::Updated));
        assert!(!is_valid_pair(EntityType::Token, Action::Deleted));
    }

    #[test]
    fn remote_target_has_invoked() {
        assert!(is_valid_pair(EntityType::RemoteTarget, Action::Invoked));
        assert!(!is_valid_pair(EntityType::Object, Action::Invoked));
    }

    #[test]
    fn user_group_is_add_remove() {
        assert!(is_valid_pair(EntityType::UserGroup, Action::Added));
        assert!(is_valid_pair(EntityType::UserGroup, Action::Removed));
        assert!(!is_valid_pair(EntityType::UserGroup, Action::Created));
    }

    #[test]
    fn task_is_lifecycle_only() {
        assert!(is_valid_pair(EntityType::Task, Action::Queued));
        assert!(is_valid_pair(EntityType::Task, Action::Succeeded));
        assert!(is_valid_pair(EntityType::Task, Action::Cleanup));
        assert!(!is_valid_pair(EntityType::Task, Action::Created));
        assert!(!is_valid_pair(EntityType::Task, Action::Updated));
    }

    #[test]
    fn actor_kind_round_trips() {
        for k in [ActorKind::User, ActorKind::System, ActorKind::Worker] {
            assert_eq!(ActorKind::from_db(k.as_str()).unwrap(), k);
        }
        assert!(ActorKind::from_db("anonymous").is_err());
    }
}
