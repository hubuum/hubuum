use futures::FutureExt;
use futures::future::BoxFuture;
use hubuum_event_sink_webhook::WebhookSinkSettings;
use hubuum_event_sinks_common::SinkDelivery;

use crate::config::{
    DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS, DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
    DEFAULT_REMOTE_CALL_TIMEOUT_MS, get_config,
};
use crate::events::Event;
use crate::models::{EventSink, EventSinkKind, EventSubscription};

pub use hubuum_event_sinks_common::{EventEnvelope, SinkError};

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

#[derive(Debug)]
pub struct DefaultSinkResolver {
    #[cfg(feature = "amqp")]
    amqp: hubuum_event_sink_amqp::AmqpSink,
    #[cfg(feature = "email")]
    email: hubuum_event_sink_email::EmailSink,
    #[cfg(feature = "valkey")]
    valkey: hubuum_event_sink_valkey::ValkeySink,
    webhook: hubuum_event_sink_webhook::WebhookSink,
}

impl Default for DefaultSinkResolver {
    fn default() -> Self {
        Self {
            #[cfg(feature = "amqp")]
            amqp: hubuum_event_sink_amqp::AmqpSink::default(),
            #[cfg(feature = "email")]
            email: hubuum_event_sink_email::EmailSink::default(),
            #[cfg(feature = "valkey")]
            valkey: hubuum_event_sink_valkey::ValkeySink::default(),
            webhook: hubuum_event_sink_webhook::WebhookSink::new(webhook_settings()),
        }
    }
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

fn sink_delivery<'a>(subscription: &'a EventSubscription, sink: &'a EventSink) -> SinkDelivery<'a> {
    SinkDelivery::new(
        &sink.config,
        &subscription.routing,
        sink.secret_ref.as_deref(),
    )
}

fn webhook_settings() -> WebhookSinkSettings {
    let (max_timeout_ms, max_response_bytes, allow_private_targets) = get_config()
        .map(|config| {
            (
                config.remote_call_timeout_ms,
                config.remote_call_max_response_bytes,
                config.remote_call_allow_private_targets,
            )
        })
        .unwrap_or((
            DEFAULT_REMOTE_CALL_TIMEOUT_MS,
            DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
            DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS,
        ));
    WebhookSinkSettings {
        max_timeout_ms,
        max_response_bytes,
        max_request_bytes: max_response_bytes,
        allow_private_targets,
        dangerous_accept_invalid_certs: cfg!(test),
        dangerous_allow_localhost: cfg!(test),
    }
}

impl Sink for hubuum_event_sink_webhook::WebhookSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            self.deliver(envelope, sink_delivery(subscription, sink))
                .await
        }
        .boxed()
    }
}

#[cfg(feature = "amqp")]
impl Sink for hubuum_event_sink_amqp::AmqpSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            self.deliver(envelope, sink_delivery(subscription, sink))
                .await
        }
        .boxed()
    }
}

#[cfg(feature = "email")]
impl Sink for hubuum_event_sink_email::EmailSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            self.deliver(envelope, sink_delivery(subscription, sink))
                .await
        }
        .boxed()
    }
}

#[cfg(feature = "valkey")]
impl Sink for hubuum_event_sink_valkey::ValkeySink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            self.deliver(envelope, sink_delivery(subscription, sink))
                .await
        }
        .boxed()
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
        let error = hubuum_event_sinks_common::require_tls_uri_scheme(
            "redis://localhost/0",
            "Valkey",
            &["rediss"],
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: uri must use a TLS scheme (rediss)"
        );
        hubuum_event_sinks_common::require_tls_uri_scheme(
            "rediss://localhost/0",
            "Valkey",
            &["rediss"],
        )
        .unwrap();
    }
}
