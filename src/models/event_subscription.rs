use std::{fmt, str::FromStr};

use crate::db::prelude::*;
use chrono::NaiveDateTime;
use hubuum_events_core::EventSubscriptionFilter;
use serde::{Deserialize, Serialize, Serializer};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{CollectionID, REDACTED_DEBUG_VALUE};
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

macro_rules! impl_redacted_event_sink_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("configuration", &REDACTED_DEBUG_VALUE)
                    .finish()
            }
        }
    };
}

macro_rules! impl_redacted_event_subscription_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("routing", &REDACTED_DEBUG_VALUE)
                    .finish()
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
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

impl_redacted_event_sink_debug!(
    EventSinkRow,
    id,
    name,
    kind,
    enabled,
    created_at,
    updated_at,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSink {
    pub id: i32,
    pub name: String,
    pub kind: EventSinkKind,
    #[serde(serialize_with = "serialize_redacted_event_sink_value")]
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl_redacted_event_sink_debug!(EventSink, id, name, kind, enabled, created_at, updated_at,);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewEventSink {
    pub name: String,
    pub kind: EventSinkKind,
    #[serde(default = "empty_json_object")]
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl_redacted_event_sink_debug!(NewEventSink, name, kind, enabled);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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

impl_redacted_event_sink_debug!(UpdateEventSink, name, kind, enabled);

#[derive(Clone, Insertable)]
#[diesel(table_name = event_sinks)]
pub(crate) struct NewEventSinkRow {
    pub name: String,
    pub kind: String,
    pub config: serde_json::Value,
    pub secret_ref: Option<String>,
    pub enabled: bool,
}

impl_redacted_event_sink_debug!(NewEventSinkRow, name, kind, enabled);

#[derive(Clone, AsChangeset)]
#[diesel(table_name = event_sinks)]
pub(crate) struct UpdateEventSinkRow {
    pub name: Option<String>,
    pub kind: Option<String>,
    pub config: Option<serde_json::Value>,
    pub secret_ref: Option<Option<String>>,
    pub enabled: Option<bool>,
}

impl_redacted_event_sink_debug!(UpdateEventSinkRow, name, kind, enabled);

impl UpdateEventSinkRow {
    pub(crate) fn has_changes(&self, current: &EventSinkRow) -> bool {
        self.name
            .as_ref()
            .is_some_and(|value| value != &current.name)
            || self
                .kind
                .as_ref()
                .is_some_and(|value| value != &current.kind)
            || self
                .config
                .as_ref()
                .is_some_and(|value| value != &current.config)
            || self
                .secret_ref
                .as_ref()
                .is_some_and(|value| value != &current.secret_ref)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct EventSubscriptionRow {
    pub id: i32,
    pub collection_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: serde_json::Value,
    pub actions: serde_json::Value,
    pub filter: serde_json::Value,
    pub routing: serde_json::Value,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl_redacted_event_subscription_debug!(
    EventSubscriptionRow,
    id,
    collection_id,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
    created_at,
    updated_at,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct EventSubscription {
    pub id: i32,
    pub collection_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: Vec<String>,
    pub actions: Vec<String>,
    #[serde(default)]
    pub filter: EventSubscriptionFilter,
    #[serde(serialize_with = "serialize_redacted_event_sink_value")]
    pub routing: serde_json::Value,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl_redacted_event_subscription_debug!(
    EventSubscription,
    id,
    collection_id,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
    created_at,
    updated_at,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewEventSubscription {
    pub sink_id: EventSinkID,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub entity_types: Vec<String>,
    pub actions: Vec<String>,
    #[serde(default)]
    pub filter: EventSubscriptionFilter,
    #[serde(default = "empty_json_object")]
    pub routing: serde_json::Value,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl_redacted_event_subscription_debug!(
    NewEventSubscription,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateEventSubscription {
    pub sink_id: Option<EventSinkID>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub entity_types: Option<Vec<String>>,
    pub actions: Option<Vec<String>>,
    pub filter: Option<EventSubscriptionFilter>,
    pub routing: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

impl_redacted_event_subscription_debug!(
    UpdateEventSubscription,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

#[derive(Clone, Insertable)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct NewEventSubscriptionRow {
    pub collection_id: i32,
    pub sink_id: i32,
    pub name: String,
    pub description: String,
    pub entity_types: serde_json::Value,
    pub actions: serde_json::Value,
    pub filter: serde_json::Value,
    pub routing: serde_json::Value,
    pub enabled: bool,
}

impl_redacted_event_subscription_debug!(
    NewEventSubscriptionRow,
    collection_id,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

#[derive(Clone, AsChangeset)]
#[diesel(table_name = event_subscriptions)]
pub(crate) struct UpdateEventSubscriptionRow {
    pub sink_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub entity_types: Option<serde_json::Value>,
    pub actions: Option<serde_json::Value>,
    pub filter: Option<serde_json::Value>,
    pub routing: Option<serde_json::Value>,
    pub enabled: Option<bool>,
}

impl_redacted_event_subscription_debug!(
    UpdateEventSubscriptionRow,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

impl UpdateEventSubscriptionRow {
    pub(crate) fn has_changes(&self, current: &EventSubscriptionRow) -> bool {
        self.sink_id.is_some_and(|value| value != current.sink_id)
            || self
                .name
                .as_ref()
                .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
            || self
                .entity_types
                .as_ref()
                .is_some_and(|value| value != &current.entity_types)
            || self
                .actions
                .as_ref()
                .is_some_and(|value| value != &current.actions)
            || self
                .filter
                .as_ref()
                .is_some_and(|value| value != &current.filter)
            || self
                .routing
                .as_ref()
                .is_some_and(|value| value != &current.routing)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
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
            collection_id: row.collection_id,
            sink_id: row.sink_id,
            name: row.name,
            description: row.description,
            entity_types: serde_json::from_value(row.entity_types)?,
            actions: serde_json::from_value(row.actions)?,
            filter: serde_json::from_value(row.filter)?,
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
        collection_id: CollectionID,
    ) -> Result<NewEventSubscriptionRow, ApiError> {
        validate_subscription_parts(
            &self.entity_types,
            &self.actions,
            &self.filter,
            &self.routing,
        )?;
        Ok(NewEventSubscriptionRow {
            collection_id: collection_id.id(),
            sink_id: self.sink_id.id(),
            name: self.name,
            description: self.description,
            entity_types: serde_json::to_value(self.entity_types)?,
            actions: serde_json::to_value(self.actions)?,
            filter: serde_json::to_value(self.filter)?,
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
            && self.filter.is_none()
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
        let filter = self
            .filter
            .clone()
            .unwrap_or_else(|| existing.filter.clone());
        validate_subscription_parts(&entity_types, &actions, &filter, &routing)?;
        Ok(UpdateEventSubscriptionRow {
            sink_id: self.sink_id.map(EventSinkID::id),
            name: self.name,
            description: self.description,
            entity_types: self.entity_types.map(serde_json::to_value).transpose()?,
            actions: self.actions.map(serde_json::to_value).transpose()?,
            filter: self.filter.map(serde_json::to_value).transpose()?,
            routing: self.routing,
            enabled: self.enabled,
        })
    }
}

pub(crate) fn validate_sink_parts(
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
    filter: &EventSubscriptionFilter,
    routing: &serde_json::Value,
) -> Result<(), ApiError> {
    filter
        .validate()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
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

pub fn redact_event_sink_config(config: &serde_json::Value) -> serde_json::Value {
    match config {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let redacted = if is_sensitive_config_key(key) {
                        serde_json::Value::String("[redacted]".to_string())
                    } else if key.eq_ignore_ascii_case("uri") || key.eq_ignore_ascii_case("url") {
                        redact_uri_value(value)
                    } else {
                        redact_event_sink_config(value)
                    };
                    (key.clone(), redacted)
                })
                .collect(),
        ),
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(redact_event_sink_config)
                .collect::<Vec<_>>(),
        ),
        value => value.clone(),
    }
}

fn serialize_redacted_event_sink_value<S>(
    value: &serde_json::Value,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    redact_event_sink_config(value).serialize(serializer)
}

fn is_sensitive_config_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let compact = lower
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    if [
        "password",
        "passwd",
        "token",
        "secret",
        "authorization",
        "auth",
        "credential",
        "credentials",
        "apikey",
        "privatekey",
        "accesskey",
    ]
    .iter()
    .any(|suffix| compact.ends_with(suffix))
    {
        return true;
    }

    let mut previous = None;
    for segment in lower
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
    {
        if matches!(
            segment,
            "password"
                | "passwd"
                | "token"
                | "secret"
                | "authorization"
                | "credential"
                | "credentials"
                | "apikey"
        ) || (segment == "key" && matches!(previous, Some("api" | "private" | "access")))
        {
            return true;
        }
        previous = Some(segment);
    }
    false
}

fn redact_uri_value(value: &serde_json::Value) -> serde_json::Value {
    let Some(uri) = value.as_str() else {
        return redact_event_sink_config(value);
    };
    serde_json::Value::String(redact_uri_userinfo(uri))
}

fn redact_uri_userinfo(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else {
        return uri.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some((_, host)) = authority.rsplit_once('@') else {
        return uri.to_string();
    };
    format!("{scheme}://[redacted]@{host}{}", &rest[authority_end..])
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::api_header("X-API-Key")]
    #[case::compact_api_header("XApiKey")]
    #[case::auth_token_header("X-Auth-Token")]
    #[case::client_secret("client-secret")]
    #[case::authorization_header("Proxy-Authorization")]
    #[case::credentials("database_credentials")]
    #[case::private_key("private_key")]
    fn common_secret_key_spellings_are_sensitive(#[case] key: &str) {
        assert!(is_sensitive_config_key(key));
    }

    #[rstest]
    #[case::routing_key("routing_key")]
    #[case::ordinary_key_suffix("monkey")]
    #[case::public_key("public_key")]
    fn non_secret_keys_remain_visible(#[case] key: &str) {
        assert!(!is_sensitive_config_key(key));
    }

    #[test]
    fn event_subscription_serialization_redacts_routing_credentials() {
        let credential = "routing-password";
        let api_key = "literal-api-key";
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let subscription = EventSubscription {
            id: 1,
            collection_id: 2,
            sink_id: 3,
            name: "webhook".to_string(),
            description: String::new(),
            entity_types: vec!["collection".to_string()],
            actions: vec!["created".to_string()],
            filter: EventSubscriptionFilter::default(),
            routing: serde_json::json!({
                "url": format!("https://user:{credential}@example.invalid/events"),
                "headers": {
                    "X-API-Key": api_key,
                    "routing_key": "events.created"
                }
            }),
            enabled: true,
            created_at: timestamp,
            updated_at: timestamp,
        };

        let serialized = serde_json::to_value(subscription).unwrap();

        assert_eq!(
            serialized["routing"]["url"],
            "https://[redacted]@example.invalid/events"
        );
        assert_eq!(serialized["routing"]["headers"]["X-API-Key"], "[redacted]");
        assert_eq!(
            serialized["routing"]["headers"]["routing_key"],
            "events.created"
        );
        assert!(!serialized.to_string().contains(credential));
        assert!(!serialized.to_string().contains(api_key));
    }

    fn timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    fn assert_omits(debug: &str, secrets: &[&str]) {
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        for secret in secrets {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn event_sink_debug_redacts_request_and_persisted_configuration() {
        let request = NewEventSink {
            name: "webhook".to_string(),
            kind: EventSinkKind::Webhook,
            config: serde_json::json!({
                "headers": {"authorization": "request-config-secret"}
            }),
            secret_ref: Some("request-secret-reference".to_string()),
            enabled: true,
        };
        let row = EventSinkRow {
            id: 1,
            name: "webhook".to_string(),
            kind: "webhook".to_string(),
            config: serde_json::json!({
                "headers": {"authorization": "stored-config-secret"}
            }),
            secret_ref: Some("stored-secret-reference".to_string()),
            enabled: true,
            created_at: timestamp(),
            updated_at: timestamp(),
        };

        assert_omits(
            &format!("{request:?}"),
            &["request-config-secret", "request-secret-reference"],
        );
        assert_omits(
            &format!("{row:?}"),
            &["stored-config-secret", "stored-secret-reference"],
        );
    }

    #[test]
    fn event_subscription_debug_redacts_request_and_persisted_routing() {
        let request = NewEventSubscription {
            sink_id: EventSinkID::new(1).unwrap(),
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["object".to_string()],
            actions: vec!["updated".to_string()],
            filter: EventSubscriptionFilter::default(),
            routing: serde_json::json!({
                "url": "https://example.invalid/hook?key=request-routing-secret"
            }),
            enabled: true,
        };
        let row = EventSubscriptionRow {
            id: 2,
            collection_id: 3,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: serde_json::json!(["object"]),
            actions: serde_json::json!(["updated"]),
            filter: serde_json::json!({}),
            routing: serde_json::json!({
                "url": "https://example.invalid/hook?key=stored-routing-secret"
            }),
            enabled: true,
            created_at: timestamp(),
            updated_at: timestamp(),
        };

        assert_omits(&format!("{request:?}"), &["request-routing-secret"]);
        assert_omits(&format!("{row:?}"), &["stored-routing-secret"]);
    }
}
