use std::fmt;

use futures::FutureExt;
use redis::Client;
use serde::Deserialize;

use crate::events::sink::{
    EventEnvelope, Sink, SinkError, UriConnectionPool, parse_sink_config, parse_sink_routing,
    require_non_empty, require_tls_uri_scheme, resolve_event_sink_secret_uri,
};
use crate::models::{EventSink, EventSubscription};

#[derive(Default)]
pub struct ValkeySink {
    clients: UriConnectionPool<String, Client>,
}

impl fmt::Debug for ValkeySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValkeySink").finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
struct ValkeyConfig {
    uri: String,
    #[serde(default)]
    max_len: Option<usize>,
    #[serde(default = "default_true")]
    approximate_trim: bool,
}

#[derive(Debug, Deserialize)]
struct ValkeyRouting {
    stream: String,
}

#[derive(Debug)]
struct StreamEntry {
    stream: String,
    max_len: Option<usize>,
    approximate_trim: bool,
    fields: Vec<(&'static str, String)>,
}

impl Sink for ValkeySink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> futures::future::BoxFuture<'a, Result<(), SinkError>> {
        async move { self.deliver_valkey(envelope, subscription, sink).await }.boxed()
    }
}

impl ValkeySink {
    async fn deliver_valkey(
        &self,
        envelope: &EventEnvelope,
        subscription: &EventSubscription,
        sink: &EventSink,
    ) -> Result<(), SinkError> {
        let config = parse_config(sink)?;
        let routing = parse_routing(subscription)?;
        let uri = resolve_event_sink_secret_uri(&config.uri, sink.secret_ref.as_deref(), "Valkey")?;
        require_tls_uri_scheme(&uri, "Valkey", &["rediss"])?;
        let client = self.client(&uri).await?;
        let entry = stream_entry(envelope, routing, config)?;

        tokio::task::spawn_blocking(move || publish_stream_entry(client, entry))
            .await
            .map_err(|error| SinkError::new(format!("Valkey delivery task failed: {error}")))?
    }

    async fn client(&self, uri: &str) -> Result<Client, SinkError> {
        self.clients
            .get_or_try_insert_with(uri.to_string(), |uri| async move {
                Client::open(uri)
                    .map_err(|error| SinkError::new(format!("Invalid Valkey config: {error}")))
            })
            .await
    }
}

fn publish_stream_entry(client: Client, entry: StreamEntry) -> Result<(), SinkError> {
    let mut connection = client
        .get_connection()
        .map_err(|error| SinkError::new(format!("Valkey connection failed: {error}")))?;
    let command = xadd_command(&entry);
    let _: String = command
        .query(&mut connection)
        .map_err(|error| SinkError::new(format!("Valkey XADD failed: {error}")))?;
    Ok(())
}

fn parse_config(sink: &EventSink) -> Result<ValkeyConfig, SinkError> {
    let config: ValkeyConfig = parse_sink_config(sink, "Valkey")?;
    require_non_empty(&config.uri, "Valkey config", "uri")?;
    if matches!(config.max_len, Some(0)) {
        return Err(SinkError::new(
            "Invalid Valkey config: max_len must be greater than zero",
        ));
    }
    Ok(config)
}

fn parse_routing(subscription: &EventSubscription) -> Result<ValkeyRouting, SinkError> {
    let routing: ValkeyRouting = parse_sink_routing(subscription, "Valkey")?;
    require_non_empty(&routing.stream, "Valkey routing", "stream")?;
    Ok(routing)
}

fn stream_entry(
    envelope: &EventEnvelope,
    routing: ValkeyRouting,
    config: ValkeyConfig,
) -> Result<StreamEntry, SinkError> {
    let payload = serde_json::to_string(envelope)
        .map_err(|error| SinkError::new(format!("Failed to serialize Valkey payload: {error}")))?;
    Ok(StreamEntry {
        stream: routing.stream,
        max_len: config.max_len,
        approximate_trim: config.approximate_trim,
        fields: vec![
            ("event_id", envelope.event_id.to_string()),
            ("entity_type", envelope.entity_type.clone()),
            (
                "entity_name",
                envelope.entity_name.clone().unwrap_or_default(),
            ),
            ("action", envelope.action.clone()),
            ("actor_kind", envelope.actor_kind.clone()),
            ("payload", payload),
        ],
    })
}

fn xadd_command(entry: &StreamEntry) -> redis::Cmd {
    let mut command = redis::cmd("XADD");
    command.arg(&entry.stream);
    if let Some(max_len) = entry.max_len {
        command.arg("MAXLEN");
        if entry.approximate_trim {
            command.arg("~");
        } else {
            command.arg("=");
        }
        command.arg(max_len);
    }
    command.arg("*");
    for (name, value) in &entry.fields {
        command.arg(name).arg(value);
    }
    command
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
            name: "valkey".to_string(),
            kind: EventSinkKind::ValkeyStream,
            config,
            secret_ref: secret_ref.map(str::to_string),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn subscription(routing: serde_json::Value) -> EventSubscription {
        let now = Utc::now().naive_utc();
        EventSubscription {
            id: 1,
            namespace_id: 10,
            sink_id: 1,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            routing,
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn routing_requires_stream() {
        let error = parse_routing(&subscription(serde_json::json!({"stream": ""}))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey routing: stream is required"
        );
    }

    #[test]
    fn config_requires_uri_and_valid_max_len() {
        let error = parse_config(&sink(
            serde_json::json!({
                "uri": "redis://localhost/0",
                "max_len": 0
            }),
            None,
        ))
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: max_len must be greater than zero"
        );
    }

    #[test]
    fn stream_entry_contains_filter_fields_event_id_and_payload() {
        let envelope = envelope();
        let entry = stream_entry(
            &envelope,
            ValkeyRouting {
                stream: "hubuum:events".to_string(),
            },
            ValkeyConfig {
                uri: "redis://localhost/0".to_string(),
                max_len: Some(1000),
                approximate_trim: true,
            },
        )
        .unwrap();

        assert_eq!(entry.stream, "hubuum:events");
        assert_eq!(entry.max_len, Some(1000));
        assert_eq!(
            entry.fields,
            vec![
                ("event_id", envelope.event_id.to_string()),
                ("entity_type", "namespace".to_string()),
                ("entity_name", "example".to_string()),
                ("action", "created".to_string()),
                ("actor_kind", "user".to_string()),
                ("payload", serde_json::to_string(&envelope).unwrap()),
            ]
        );
    }

    #[test]
    fn secret_ref_replaces_uri_placeholder_with_encoded_secret() {
        let secret_ref = "valkey_sink_unit_test";
        unsafe {
            std::env::set_var(
                "HUBUUM_EVENT_SINK_SECRET_VALKEY_SINK_UNIT_TEST",
                "p@ss/w:rd",
            );
        }

        let uri = resolve_event_sink_secret_uri(
            "redis://default:{secret}@valkey.example/0",
            Some(secret_ref),
            "Valkey",
        )
        .unwrap();

        assert_eq!(uri, "redis://default:p%40ss%2Fw%3Ard@valkey.example/0");

        unsafe {
            std::env::remove_var("HUBUUM_EVENT_SINK_SECRET_VALKEY_SINK_UNIT_TEST");
        }
    }
}
