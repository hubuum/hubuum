use std::fmt;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
use std::future::Future;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
use std::hash::Hash;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
use tokio::sync::Mutex;

use crate::events::Event;
use crate::events::webhook::WebhookSink;
use crate::models::{EventSink, EventSinkKind, EventSubscription};

pub use hubuum_events_core::EventEnvelope;

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

impl From<hubuum_events_core::EventSinkSecretError> for SinkError {
    fn from(error: hubuum_events_core::EventSinkSecretError) -> Self {
        Self::new(error.to_string())
    }
}

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

pub(crate) use hubuum_events_core::resolve_event_sink_secret;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
pub(crate) use hubuum_events_core::resolve_event_sink_secret_uri;

pub(crate) fn parse_sink_config<T: DeserializeOwned>(
    sink: &EventSink,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(sink.config.clone())
        .map_err(|error| SinkError::new(format!("Invalid {sink_label} config: {error}")))
}

pub(crate) fn parse_sink_routing<T: DeserializeOwned>(
    subscription: &EventSubscription,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(subscription.routing.clone())
        .map_err(|error| SinkError::new(format!("Invalid {sink_label} routing: {error}")))
}

pub(crate) fn require_non_empty(value: &str, label: &str, field: &str) -> Result<(), SinkError> {
    if value.trim().is_empty() {
        return Err(SinkError::new(format!(
            "Invalid {label}: {field} is required"
        )));
    }
    Ok(())
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
pub(crate) fn require_tls_uri_scheme(
    uri: &str,
    sink_label: &str,
    tls_schemes: &[&str],
) -> Result<(), SinkError> {
    let Some((scheme, _)) = uri.split_once(':') else {
        return Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri must include a scheme"
        )));
    };
    if !tls_schemes
        .iter()
        .any(|allowed| scheme.eq_ignore_ascii_case(allowed))
    {
        return Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri must use a TLS scheme ({})",
            tls_schemes.join(", ")
        )));
    }
    Ok(())
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
#[derive(Debug)]
pub(crate) struct UriConnectionPool<K, V> {
    entries: Mutex<std::collections::HashMap<K, V>>,
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
impl<K, V> Default for UriConnectionPool<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
impl<K, V> UriConnectionPool<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub(crate) async fn get_or_try_insert_with<F, Fut>(
        &self,
        key: K,
        create: F,
    ) -> Result<V, SinkError>
    where
        F: FnOnce(K) -> Fut,
        Fut: Future<Output = Result<V, SinkError>>,
    {
        let mut entries = self.entries.lock().await;
        if let Some(value) = entries.get(&key) {
            return Ok(value.clone());
        }

        let value = create(key.clone()).await?;
        entries.insert(key, value.clone());
        Ok(value)
    }

    #[cfg(feature = "amqp")]
    pub(crate) async fn remove(&self, key: &K) {
        self.entries.lock().await.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use uuid::Uuid;

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

    #[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
    #[test]
    fn tls_scheme_validator_rejects_cleartext_uris() {
        let error =
            require_tls_uri_scheme("redis://localhost/0", "Valkey", &["rediss"]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: uri must use a TLS scheme (rediss)"
        );
        require_tls_uri_scheme("rediss://localhost/0", "Valkey", &["rediss"]).unwrap();
    }
}
