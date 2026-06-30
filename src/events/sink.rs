use std::fmt;

use chrono::NaiveDateTime;
use futures::future::BoxFuture;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::Event;
use crate::events::webhook::WebhookSink;
use crate::models::{EventSink, EventSinkKind, EventSubscription};

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

impl From<Event> for EventEnvelope {
    fn from(event: Event) -> Self {
        Self {
            id: event.id,
            event_id: event.event_id,
            occurred_at: event.occurred_at,
            entity_type: event.entity_type,
            entity_id: event.entity_id,
            entity_name: event.entity_name,
            namespace_id: event.namespace_id,
            action: event.action,
            actor_user_id: event.actor_user_id,
            actor_kind: event.actor_kind,
            request_id: event.request_id,
            correlation_id: event.correlation_id,
            summary: event.summary,
            before: event.before,
            after: event.after,
            metadata: event.metadata,
            schema_version: event.schema_version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkError {
    message: String,
}

impl SinkError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SinkError {}

pub trait Sink: Send + Sync {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), SinkError>>;
}

pub trait SinkResolver: Send + Sync {
    fn resolve(&self, kind: EventSinkKind) -> Option<&dyn Sink>;
}

#[derive(Debug, Default)]
pub struct NoopSinkResolver;

impl SinkResolver for NoopSinkResolver {
    fn resolve(&self, _kind: EventSinkKind) -> Option<&dyn Sink> {
        None
    }
}

#[derive(Debug, Default)]
pub struct DefaultSinkResolver {
    #[cfg(feature = "amqp")]
    amqp: crate::events::amqp::AmqpSink,
    #[cfg(feature = "email")]
    email: crate::events::email::EmailSink,
    #[cfg(feature = "valkey")]
    valkey: crate::events::valkey::ValkeySink,
    webhook: WebhookSink,
}

impl SinkResolver for DefaultSinkResolver {
    fn resolve(&self, kind: EventSinkKind) -> Option<&dyn Sink> {
        match kind {
            #[cfg(feature = "amqp")]
            EventSinkKind::Amqp => Some(&self.amqp),
            #[cfg(feature = "email")]
            EventSinkKind::Email => Some(&self.email),
            #[cfg(feature = "valkey")]
            EventSinkKind::ValkeyStream => Some(&self.valkey),
            EventSinkKind::Webhook => Some(&self.webhook),
            #[cfg(not(all(feature = "amqp", feature = "email", feature = "valkey")))]
            _ => None,
        }
    }
}

pub(crate) fn resolve_event_sink_secret(secret_ref: &str) -> Result<String, SinkError> {
    let key = format!(
        "HUBUUM_EVENT_SINK_SECRET_{}",
        secret_ref.to_ascii_uppercase()
    );
    std::env::var(&key).map_err(|_| {
        SinkError::new(format!(
            "Event sink secret reference '{secret_ref}' is not configured"
        ))
    })
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
pub(crate) fn resolve_event_sink_secret_uri(
    uri: &str,
    secret_ref: Option<&str>,
    sink_label: &str,
) -> Result<String, SinkError> {
    let contains_secret_placeholder = uri.contains("{secret}");
    match secret_ref {
        Some(secret_ref) => {
            if !contains_secret_placeholder {
                return Err(SinkError::new(format!(
                    "Invalid {sink_label} config: uri must include {{secret}} when secret_ref is set"
                )));
            }
            let secret = resolve_event_sink_secret(secret_ref)?;
            let encoded = utf8_percent_encode(&secret, NON_ALPHANUMERIC).to_string();
            Ok(uri.replace("{secret}", &encoded))
        }
        None if contains_secret_placeholder => Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri includes {{secret}} without secret_ref"
        ))),
        None => Ok(uri.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    struct RecordingSink;

    impl Sink for RecordingSink {
        fn deliver<'a>(
            &'a self,
            envelope: &'a EventEnvelope,
            subscription: &'a EventSubscription,
            sink: &'a EventSink,
        ) -> BoxFuture<'a, Result<(), SinkError>> {
            async move {
                assert_eq!(envelope.entity_type, "namespace");
                assert_eq!(subscription.name, "subscription");
                assert_eq!(sink.name, "sink");
                Ok(())
            }
            .boxed()
        }
    }

    #[actix_rt::test]
    async fn sink_trait_can_be_mocked_without_worker_storage() {
        let envelope = EventEnvelope {
            id: 1,
            event_id: Uuid::new_v4(),
            occurred_at: chrono::Utc::now().naive_utc(),
            entity_type: "namespace".to_string(),
            entity_id: Some(10),
            entity_name: Some("example".to_string()),
            namespace_id: Some(10),
            action: "created".to_string(),
            actor_user_id: None,
            actor_kind: "system".to_string(),
            request_id: None,
            correlation_id: None,
            summary: "created namespace".to_string(),
            before: None,
            after: None,
            metadata: serde_json::json!({}),
            schema_version: 1,
        };
        let subscription = EventSubscription {
            id: 1,
            namespace_id: 10,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            routing: serde_json::json!({}),
            enabled: true,
            created_at: envelope.occurred_at,
            updated_at: envelope.occurred_at,
        };
        let sink = EventSink {
            id: 1,
            name: "sink".to_string(),
            kind: EventSinkKind::Webhook,
            config: serde_json::json!({}),
            secret_ref: None,
            enabled: true,
            created_at: envelope.occurred_at,
            updated_at: envelope.occurred_at,
        };

        RecordingSink
            .deliver(&envelope, &subscription, &sink)
            .await
            .unwrap();
    }
}
