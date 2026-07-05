use std::fmt;
use std::sync::Arc;

use hubuum_event_sinks_common::{
    DEFAULT_MAX_ENVELOPE_BYTES, EventEnvelope, SinkDelivery, SinkError, UriConnectionPool,
    parse_sink_config, reject_literal_uri_credentials, require_non_empty, require_tls_uri_scheme,
    resolve_event_sink_secret_uri, serialize_envelope_to_vec,
};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties, ExchangeKind,
};
use serde::Deserialize;
use tracing::warn;

#[derive(Default)]
pub struct AmqpSink {
    connections: UriConnectionPool<String, Arc<Connection>>,
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
    #[serde(default)]
    max_payload_bytes: Option<usize>,
}

impl AmqpSink {
    pub async fn deliver(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let config = parse_config(&delivery)?;
        let uri = resolve_event_sink_secret_uri(&config.uri, delivery.secret_ref, "AMQP")?;
        require_tls_uri_scheme(&uri, "AMQP", &["amqps"])?;
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

        let payload = serialize_envelope_to_vec(
            envelope,
            "AMQP",
            config
                .max_payload_bytes
                .unwrap_or(DEFAULT_MAX_ENVELOPE_BYTES),
        )?;
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
        let key = uri.to_string();
        let connection = self
            .connections
            .get_or_try_insert_with(key.clone(), |uri| async move {
                Connection::connect(&uri, ConnectionProperties::default().enable_auto_recover())
                    .await
                    .map(Arc::new)
                    .map_err(|error| SinkError::new(format!("AMQP connection failed: {error}")))
            })
            .await?;

        match connection.create_channel().await {
            Ok(channel) => Ok(channel),
            Err(error) => {
                warn!(
                    message = "Dropping failed AMQP connection from sink pool",
                    error = %error
                );
                self.connections.remove(&key).await;
                Err(SinkError::new(format!(
                    "AMQP channel creation failed: {error}"
                )))
            }
        }
    }
}

fn parse_config(delivery: &SinkDelivery<'_>) -> Result<AmqpConfig, SinkError> {
    let config: AmqpConfig = parse_sink_config(delivery, "AMQP")?;
    require_non_empty(&config.uri, "AMQP config", "uri")?;
    reject_literal_uri_credentials(&config.uri, "AMQP")?;
    require_non_empty(&config.exchange, "AMQP config", "exchange")?;
    Ok(config)
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
    fn envelope() -> EventEnvelope {
        EventEnvelope {
            id: 42,
            event_id: Uuid::new_v4(),
            occurred_at: Utc::now().naive_utc(),
            entity_type: "collection".to_string(),
            entity_id: Some(7),
            entity_name: Some("example".to_string()),
            collection_id: Some(7),
            action: "created".to_string(),
            actor_user_id: Some(1),
            actor_kind: "user".to_string(),
            request_id: None,
            correlation_id: Some("corr-1".to_string()),
            summary: "collection created".to_string(),
            before: None,
            after: Some(serde_json::json!({"name": "example"})),
            metadata: serde_json::json!({"source": "test"}),
            schema_version: 1,
        }
    }

    fn delivery<'a>(
        config: &'a serde_json::Value,
        routing: &'a serde_json::Value,
        secret_ref: Option<&'a str>,
    ) -> SinkDelivery<'a> {
        SinkDelivery::new(config, routing, secret_ref)
    }

    #[test]
    fn routing_key_uses_entity_type_and_action() {
        assert_eq!(routing_key(&envelope()), "collection.created");
    }

    #[test]
    fn config_requires_uri_and_exchange() {
        let config = serde_json::json!({
            "uri": "amqp://localhost",
            "exchange": ""
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: exchange is required"
        );
    }

    #[test]
    fn secret_ref_requires_uri_placeholder() {
        let error = resolve_event_sink_secret_uri(
            "amqps://publisher@example/%2f",
            Some("rabbitmq"),
            "AMQP",
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: uri must include {secret} when secret_ref is set"
        );
    }

    #[test]
    fn secret_placeholder_requires_secret_ref() {
        let error =
            resolve_event_sink_secret_uri("amqps://publisher:{secret}@example/%2f", None, "AMQP")
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: uri includes {secret} without secret_ref"
        );
    }

    #[test]
    fn config_rejects_literal_uri_credentials() {
        let config = serde_json::json!({
            "uri": "amqps://publisher:password@example/%2f",
            "exchange": "hubuum.events"
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid AMQP config: uri credentials must use {secret} with secret_ref"
        );
    }

    #[test]
    fn secret_ref_replaces_placeholder_with_encoded_secret() {
        let secret_ref = "amqp_sink_unit_test";
        unsafe {
            std::env::set_var("HUBUUM_EVENT_SINK_SECRET_AMQP_SINK_UNIT_TEST", "p@ss/w:rd");
        }

        let uri = resolve_event_sink_secret_uri(
            "amqps://publisher:{secret}@rabbitmq.example/%2f",
            Some(secret_ref),
            "AMQP",
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
        let config = serde_json::json!({
            "uri": "amqp://localhost/%2f",
            "exchange": "hubuum.events"
        });
        let routing = serde_json::json!({});
        let parsed = parse_config(&delivery(&config, &routing, None)).unwrap();

        assert_eq!(parsed.exchange_type, "topic");
        assert!(parsed.declare_exchange);
        assert!(parsed.durable);
    }
}
