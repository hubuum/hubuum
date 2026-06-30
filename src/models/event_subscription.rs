use std::str::FromStr;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::NamespaceID;
use crate::models::search::{FilterField, SortParam};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::{event_sinks, event_subscriptions};

crate::int_id_newtype! {
    /// Identifier wrapper for an event sink.
    pub struct EventSinkID;
    noun = "event sink id";
}

crate::int_id_newtype! {
    /// Identifier wrapper for an event subscription.
    pub struct EventSubscriptionID;
    noun = "event subscription id";
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventSinkKind {
    Webhook,
    Amqp,
    ValkeyStream,
    Email,
}

impl EventSinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Webhook => "webhook",
            Self::Amqp => "amqp",
            Self::ValkeyStream => "valkey_stream",
            Self::Email => "email",
        }
    }

    pub fn ensure_enabled(self) -> Result<(), ApiError> {
        match self {
            Self::Webhook => Ok(()),
            Self::Amqp if cfg!(feature = "amqp") => Ok(()),
            Self::ValkeyStream if cfg!(feature = "valkey") => Ok(()),
            Self::Email if cfg!(feature = "email") => Ok(()),
            _ => Err(ApiError::BadRequest(format!(
                "Event sink kind '{}' is not enabled on this server",
                self.as_str()
            ))),
        }
    }
}

impl FromStr for EventSinkKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "webhook" => Ok(Self::Webhook),
            "amqp" => Ok(Self::Amqp),
            "valkey_stream" => Ok(Self::ValkeyStream),
            "email" => Ok(Self::Email),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported event sink kind: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = event_sinks)]
pub(crate) struct EventSinkRow {
    pub id: i32,
    pub name: String,
    pub kind: String,
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSink {
    pub id: i32,
    pub name: String,
    pub kind: EventSinkKind,
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewEventSink {
    pub name: String,
    pub kind: EventSinkKind,
    #[serde(default = "empty_json_object")]
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateEventSink {
    pub name: Option<String>,
    pub kind: Option<EventSinkKind>,
    pub config: Option<serde_json::Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<String>)]
    pub secret_ref: Option<Option<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = event_sinks)]
pub(crate) struct NewEventSinkRow {
    pub name: String,
    pub kind: String,
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = event_sinks)]
pub(crate) struct UpdateEventSinkRow {
    pub name: Option<String>,
    pub kind: Option<String>,
    pub config: Option<serde_json::Value>,
    pub secret_ref: Option<Option<String>>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct EventSubscriptionRow {
    pub id: i32,
    pub namespace_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: serde_json::Value,
    pub actions: serde_json::Value,
    pub routing: serde_json::Value,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSubscription {
    pub id: i32,
    pub namespace_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: Vec<String>,
    pub actions: Vec<String>,
    pub routing: serde_json::Value,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewEventSubscription {
    pub sink_id: EventSinkID,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub entity_types: Vec<String>,
    pub actions: Vec<String>,
    #[serde(default = "empty_json_object")]
    pub routing: serde_json::Value,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateEventSubscription {
    pub sink_id: Option<EventSinkID>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub entity_types: Option<Vec<String>>,
    pub actions: Option<Vec<String>>,
    pub routing: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct NewEventSubscriptionRow {
    pub namespace_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: serde_json::Value,
    pub actions: serde_json::Value,
    pub routing: serde_json::Value,
    pub enabled: bool,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct UpdateEventSubscriptionRow {
    pub sink_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub entity_types: Option<serde_json::Value>,
    pub actions: Option<serde_json::Value>,
    pub routing: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

impl TryFrom<EventSinkRow> for EventSink {
    type Error = ApiError;

    fn try_from(row: EventSinkRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            name: row.name,
            kind: EventSinkKind::from_str(&row.kind)?,
            config: row.config,
            secret_ref: row.secret_ref,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<EventSubscriptionRow> for EventSubscription {
    type Error = ApiError;

    fn try_from(row: EventSubscriptionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            namespace_id: row.namespace_id,
            sink_id: row.sink_id,
            name: row.name,
            description: row.description,
            entity_types: serde_json::from_value(row.entity_types)?,
            actions: serde_json::from_value(row.actions)?,
            routing: row.routing,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl NewEventSink {
    pub(crate) fn into_row(self) -> Result<NewEventSinkRow, ApiError> {
        validate_sink_parts(self.kind, &self.config, self.secret_ref.as_deref())?;
        Ok(NewEventSinkRow {
            name: self.name,
            kind: self.kind.as_str().to_string(),
            config: self.config,
            secret_ref: normalize_optional_string(self.secret_ref),
            enabled: self.enabled,
        })
    }
}

impl UpdateEventSink {
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.kind.is_none()
            && self.config.is_none()
            && self.secret_ref.is_none()
            && self.enabled.is_none()
    }

    pub(crate) fn into_row(self, existing: &EventSink) -> Result<UpdateEventSinkRow, ApiError> {
        let kind = self.kind.unwrap_or(existing.kind);
        let config = self
            .config
            .clone()
            .unwrap_or_else(|| existing.config.clone());
        let secret_ref = match self.secret_ref.clone() {
            Some(value) => value,
            None => existing.secret_ref.clone(),
        };
        validate_sink_parts(kind, &config, secret_ref.as_deref())?;
        Ok(UpdateEventSinkRow {
            name: self.name,
            kind: self.kind.map(|kind| kind.as_str().to_string()),
            config: self.config,
            secret_ref: self.secret_ref.map(normalize_optional_string),
            enabled: self.enabled,
        })
    }
}

impl NewEventSubscription {
    pub(crate) fn into_row(
        self,
        namespace_id: NamespaceID,
    ) -> Result<NewEventSubscriptionRow, ApiError> {
        validate_subscription_parts(&self.entity_types, &self.actions, &self.routing)?;
        Ok(NewEventSubscriptionRow {
            namespace_id: namespace_id.id(),
            sink_id: self.sink_id.id(),
            name: self.name,
            description: self.description,
            entity_types: serde_json::to_value(self.entity_types)?,
            actions: serde_json::to_value(self.actions)?,
            routing: self.routing,
            enabled: self.enabled,
        })
    }
}

impl UpdateEventSubscription {
    pub fn is_empty(&self) -> bool {
        self.sink_id.is_none()
            && self.name.is_none()
            && self.description.is_none()
            && self.entity_types.is_none()
            && self.actions.is_none()
            && self.routing.is_none()
            && self.enabled.is_none()
    }

    pub(crate) fn into_row(
        self,
        existing: &EventSubscription,
    ) -> Result<UpdateEventSubscriptionRow, ApiError> {
        let entity_types = self
            .entity_types
            .clone()
            .unwrap_or_else(|| existing.entity_types.clone());
        let actions = self
            .actions
            .clone()
            .unwrap_or_else(|| existing.actions.clone());
        let routing = self
            .routing
            .clone()
            .unwrap_or_else(|| existing.routing.clone());
        validate_subscription_parts(&entity_types, &actions, &routing)?;
        Ok(UpdateEventSubscriptionRow {
            sink_id: self.sink_id.map(EventSinkID::id),
            name: self.name,
            description: self.description,
            entity_types: self.entity_types.map(serde_json::to_value).transpose()?,
            actions: self.actions.map(serde_json::to_value).transpose()?,
            routing: self.routing,
            enabled: self.enabled,
        })
    }
}

fn validate_sink_parts(
    kind: EventSinkKind,
    config: &serde_json::Value,
    secret_ref: Option<&str>,
) -> Result<(), ApiError> {
    kind.ensure_enabled()?;
    if !config.is_object() {
        return Err(ApiError::BadRequest(
            "config must be a JSON object".to_string(),
        ));
    }
    if let Some(secret_ref) = secret_ref
        && secret_ref.trim().is_empty()
    {
        return Err(ApiError::BadRequest(
            "secret_ref must not be empty".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_subscription_parts(
    entity_types: &[String],
    actions: &[String],
    routing: &serde_json::Value,
) -> Result<(), ApiError> {
    if !routing.is_object() {
        return Err(ApiError::BadRequest(
            "routing must be a JSON object".to_string(),
        ));
    }
    if entity_types.is_empty() {
        return Err(ApiError::BadRequest(
            "entity_types must include at least one value".to_string(),
        ));
    }
    if actions.is_empty() {
        return Err(ApiError::BadRequest(
            "actions must include at least one value".to_string(),
        ));
    }

    let mut parsed_entity_types = Vec::with_capacity(entity_types.len());
    let mut seen_entity_types = std::collections::HashSet::new();
    for value in entity_types {
        if !seen_entity_types.insert(value) {
            return Err(ApiError::BadRequest(format!(
                "entity_types contains duplicate '{value}'"
            )));
        }
        parsed_entity_types.push(
            hubuum_events_core::EntityType::from_db(value)
                .map_err(|error| ApiError::BadRequest(format!("bad entity_type: {error}")))?,
        );
    }

    let mut parsed_actions = Vec::with_capacity(actions.len());
    let mut seen_actions = std::collections::HashSet::new();
    for value in actions {
        if !seen_actions.insert(value) {
            return Err(ApiError::BadRequest(format!(
                "actions contains duplicate '{value}'"
            )));
        }
        parsed_actions.push(
            hubuum_events_core::Action::from_db(value)
                .map_err(|error| ApiError::BadRequest(format!("bad action: {error}")))?,
        );
    }

    for entity_type in parsed_entity_types {
        for action in &parsed_actions {
            if !hubuum_events_core::is_valid_pair(entity_type, *action) {
                return Err(ApiError::BadRequest(format!(
                    "action '{}' is not valid for entity_type '{}'",
                    action.as_str(),
                    entity_type.as_str()
                )));
            }
        }
    }

    Ok(())
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

fn default_enabled() -> bool {
    true
}

fn deserialize_double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

impl CursorPaginated for EventSink {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::Kind | FilterField::CreatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::Kind => Ok(CursorValue::String(self.kind.as_str().to_string())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event sinks",
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
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }
}

impl CursorSqlMapping for EventSink {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_sinks.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "event_sinks.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Kind => CursorSqlField {
                column: "event_sinks.kind",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_sinks.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event sinks",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for EventSubscription {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::CreatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event subscriptions",
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
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }
}

impl CursorSqlMapping for EventSubscription {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_subscriptions.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "event_subscriptions.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_subscriptions.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event subscriptions",
                    field
                )));
            }
        })
    }
}
