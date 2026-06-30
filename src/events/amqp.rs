use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use futures::FutureExt;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind,
};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::warn;

use crate::events::sink::{EventEnvelope, Sink, SinkError, resolve_event_sink_secret};
use crate::models::{EventSink, EventSubscription};

#[derive(Default)]
pub struct AmqpSink {
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

impl fmt::Debug for AmqpSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmqpSink").finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
struct AmqpConfig {
    uri: String,
    exchange: String,
    #[serde(default = "default_exchange_type")]
    exchange_type: String,
    #[serde(default = "default_true")]
    declare_exchange: bool,
    #[serde(default = "default_true")]
    durable: bool,
    #[serde(default)]
    mandatory: bool,
}

impl Sink for AmqpSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        _subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> futures::future::BoxFuture<'a, Result<(), SinkError>> {
        async move { self.deliver_amqp(envelope, sink).await }.boxed()
    }
}

impl AmqpSink {
    async fn deliver_amqp(
        &self,
        envelope: &EventEnvelope,
        sink: &EventSink,
    ) -> Result<(), SinkError> {
        let config = parse_config(sink)?;
        let uri = resolve_amqp_uri(&config.uri, sink.secret_ref.as_deref())?;
        let channel = self.channel(&uri).await?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|error| SinkError::new(format!("AMQP confirm setup failed: {error}")))?;

        if config.declare_exchange {
            channel
                .exchange_declare(
                    config.exchange.clone().into(),
                    exchange_kind(&config.exchange_type)?,
                    ExchangeDeclareOptions {
                        durable: config.durable,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|error| {
                    SinkError::new(format!("AMQP exchange declaration failed: {error}"))
                })?;
        }

        let payload = serde_json::to_vec(envelope).map_err(|error| {
            SinkError::new(format!("Failed to serialize AMQP payload: {error}"))
        })?;
        let routing_key = routing_key(envelope);
        let properties = message_properties(envelope);
        let confirmation = channel
            .basic_publish(
                config.exchange.into(),
                routing_key.into(),
                BasicPublishOptions {
                    mandatory: config.mandatory,
                    ..Default::default()
                },
                &payload,
                properties,
            )
            .await
            .map_err(|error| SinkError::new(format!("AMQP publish failed: {error}")))?
            .await
            .map_err(|error| {
                SinkError::new(format!("AMQP publish confirmation failed: {error}"))
            })?;

        match confirmation {
            Confirmation::Ack(None) | Confirmation::NotRequested => Ok(()),
            Confirmation::Ack(Some(_)) => Err(SinkError::new(
                "AMQP publish was returned by the broker without a matching route",
            )),
            Confirmation::Nack(_) => Err(SinkError::new("AMQP publish was rejected by the broker")),
        }
    }

    async fn channel(&self, uri: &str) -> Result<Channel, SinkError> {
        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(uri) {
            match connection.create_channel().await {
                Ok(channel) => return Ok(channel),
                Err(error) => {
                    warn!(
                        message = "Dropping failed AMQP connection from sink pool",
                        error = %error
                    );
                    connections.remove(uri);
                }
            }
        }

        let connection = Arc::new(
            Connection::connect(uri, ConnectionProperties::default().enable_auto_recover())
                .await
                .map_err(|error| SinkError::new(format!("AMQP connection failed: {error}")))?,
        );
        let channel = connection
            .create_channel()
            .await
            .map_err(|error| SinkError::new(format!("AMQP channel creation failed: {error}")))?;
        connections.insert(uri.to_string(), connection);
        Ok(channel)
    }
}

fn parse_config(sink: &EventSink) -> Result<AmqpConfig, SinkError> {
    let config: AmqpConfig = serde_json::from_value(sink.config.clone())
        .map_err(|error| SinkError::new(format!("Invalid AMQP config: {error}")))?;
    if config.uri.trim().is_empty() {
        return Err(SinkError::new("Invalid AMQP config: uri is required"));
    }
    if config.exchange.trim().is_empty() {
        return Err(SinkError::new("Invalid AMQP config: exchange is required"));
    }
    Ok(config)
}

fn resolve_amqp_uri(uri: &str, secret_ref: Option<&str>) -> Result<String, SinkError> {
    let contains_secret_placeholder = uri.contains("{secret}");
    match secret_ref {
        Some(secret_ref) => {
            if !contains_secret_placeholder {
                return Err(SinkError::new(
                    "Invalid AMQP config: uri must include {secret} when secret_ref is set",
                ));
            }
            let secret = resolve_event_sink_secret(secret_ref)?;
            let encoded = utf8_percent_encode(&secret, NON_ALPHANUMERIC).to_string();
            Ok(uri.replace("{secret}", &encoded))
        }
        None if contains_secret_placeholder => Err(SinkError::new(
            "Invalid AMQP config: uri includes {secret} without secret_ref",
        )),
        None => Ok(uri.to_string()),
    }
}

fn routing_key(envelope: &EventEnvelope) -> String {
    format!("{}.{}", envelope.entity_type, envelope.action)
}

fn message_properties(envelope: &EventEnvelope) -> BasicProperties {
    BasicProperties::default()
        .with_content_type("application/json".into())
        .with_message_id(envelope.event_id.to_string().into())
        .with_delivery_mode(2)
}

fn exchange_kind(exchange_type: &str) -> Result<ExchangeKind, SinkError> {
    match exchange_type {
        "direct" => Ok(ExchangeKind::Direct),
        "fanout" => Ok(ExchangeKind::Fanout),
        "headers" => Ok(ExchangeKind::Headers),
        "topic" => Ok(ExchangeKind::Topic),
        custom if custom.trim().is_empty() => Err(SinkError::new(
            "Invalid AMQP config: exchange_type must not be empty",
        )),
        custom => Ok(ExchangeKind::Custom(custom.to_string())),
    }
}

fn default_exchange_type() -> String {
    "topic".to_string()
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::models::EventSinkKind;

    fn envelope() -> EventEnvelope {
        EventEnvelope {
            id: 42,
            event_id: Uuid::new_v4(),
            occurred_at: Utc::now().naive_utc(),
            entity_type: "namespace".to_string(),
            entity_id: Some(7),
            entity_name: Some("example".to_string()),
            namespace_id: Some(7),
            action: "created".to_string(),
            actor_user_id: Some(1),
            actor_kind: "user".to_string(),
            request_id: None,
            correlation_id: Some("corr-1".to_string()),
            summary: "namespace created".to_string(),
            before: None,
            after: Some(serde_json::json!({"name": "example"})),
            metadata: serde_json::json!({"source": "test"}),
            schema_version: 1,
        }
    }

    fn sink(config: serde_json::Value, secret_ref: Option<&str>) -> EventSink {
        let now = Utc::now().naive_utc();
        EventSink {
            id: 1,
            name: "amqp".to_string(),
            kind: EventSinkKind::Amqp,
            config,
            secret_ref: secret_ref.map(str::to_string),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn routing_key_uses_entity_type_and_action() {
        assert_eq!(routing_key(&envelope()), "namespace.created");
    }

    #[test]
    fn config_requires_uri_and_exchange() {
        let error = parse_config(&sink(
            serde_json::json!({
                "uri": "amqp://localhost",
                "exchange": ""
            }),
            None,
        ))
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: exchange is required"
        );
    }

    #[test]
    fn secret_ref_requires_uri_placeholder() {
        let error =
            resolve_amqp_uri("amqps://publisher@example/%2f", Some("rabbitmq")).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: uri must include {secret} when secret_ref is set"
        );
    }

    #[test]
    fn secret_placeholder_requires_secret_ref() {
        let error = resolve_amqp_uri("amqps://publisher:{secret}@example/%2f", None).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: uri includes {secret} without secret_ref"
        );
    }

    #[test]
    fn secret_ref_replaces_placeholder_with_encoded_secret() {
        let secret_ref = "amqp_sink_unit_test";
        unsafe {
            std::env::set_var("HUBUUM_EVENT_SINK_SECRET_AMQP_SINK_UNIT_TEST", "p@ss/w:rd");
        }

        let uri = resolve_amqp_uri(
            "amqps://publisher:{secret}@rabbitmq.example/%2f",
            Some(secret_ref),
        )
        .unwrap();

        assert_eq!(
            uri,
            "amqps://publisher:p%40ss%2Fw%3Ard@rabbitmq.example/%2f"
        );

        unsafe {
            std::env::remove_var("HUBUUM_EVENT_SINK_SECRET_AMQP_SINK_UNIT_TEST");
        }
    }

    #[test]
    fn default_exchange_type_is_topic() {
        let parsed = parse_config(&sink(
            serde_json::json!({
                "uri": "amqp://localhost/%2f",
                "exchange": "hubuum.events"
            }),
            None,
        ))
        .unwrap();

        assert_eq!(parsed.exchange_type, "topic");
        assert!(parsed.declare_exchange);
        assert!(parsed.durable);
    }
}
