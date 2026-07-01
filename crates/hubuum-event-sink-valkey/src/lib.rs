use std::fmt;
use std::time::Duration;

use hubuum_event_sinks_common::{
    DEFAULT_MAX_ENVELOPE_BYTES, EventEnvelope, SinkDelivery, SinkError, UriConnectionPool,
    parse_sink_config, parse_sink_routing, reject_literal_uri_credentials, require_non_empty,
    require_tls_uri_scheme, resolve_event_sink_secret_uri, serialize_envelope_to_string,
};
use redis::Client;
use serde::Deserialize;

const DEFAULT_VALKEY_IO_TIMEOUT_MS: u64 = 25_000;

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
    #[serde(default)]
    max_payload_bytes: Option<usize>,
    #[serde(default)]
    io_timeout_ms: Option<u64>,
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
    io_timeout: Duration,
    fields: Vec<(&'static str, String)>,
}

impl ValkeySink {
    pub async fn deliver(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let config = parse_config(&delivery)?;
        let routing = parse_routing(&delivery)?;
        let uri = resolve_event_sink_secret_uri(&config.uri, delivery.secret_ref, "Valkey")?;
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
        .get_connection_with_timeout(entry.io_timeout)
        .map_err(|error| SinkError::new(format!("Valkey connection failed: {error}")))?;
    connection
        .set_read_timeout(Some(entry.io_timeout))
        .map_err(|error| SinkError::new(format!("Valkey read timeout setup failed: {error}")))?;
    connection
        .set_write_timeout(Some(entry.io_timeout))
        .map_err(|error| SinkError::new(format!("Valkey write timeout setup failed: {error}")))?;
    let command = xadd_command(&entry);
    let _: String = command
        .query(&mut connection)
        .map_err(|error| SinkError::new(format!("Valkey XADD failed: {error}")))?;
    Ok(())
}

fn parse_config(delivery: &SinkDelivery<'_>) -> Result<ValkeyConfig, SinkError> {
    let config: ValkeyConfig = parse_sink_config(delivery, "Valkey")?;
    require_non_empty(&config.uri, "Valkey config", "uri")?;
    reject_literal_uri_credentials(&config.uri, "Valkey")?;
    if matches!(config.max_len, Some(0)) {
        return Err(SinkError::new(
            "Invalid Valkey config: max_len must be greater than zero",
        ));
    }
    if matches!(config.io_timeout_ms, Some(0)) {
        return Err(SinkError::new(
            "Invalid Valkey config: io_timeout_ms must be greater than zero",
        ));
    }
    Ok(config)
}

fn parse_routing(delivery: &SinkDelivery<'_>) -> Result<ValkeyRouting, SinkError> {
    let routing: ValkeyRouting = parse_sink_routing(delivery, "Valkey")?;
    require_non_empty(&routing.stream, "Valkey routing", "stream")?;
    Ok(routing)
}

fn stream_entry(
    envelope: &EventEnvelope,
    routing: ValkeyRouting,
    config: ValkeyConfig,
) -> Result<StreamEntry, SinkError> {
    let payload = serialize_envelope_to_string(
        envelope,
        "Valkey",
        config
            .max_payload_bytes
            .unwrap_or(DEFAULT_MAX_ENVELOPE_BYTES),
    )?;
    Ok(StreamEntry {
        stream: routing.stream,
        max_len: config.max_len,
        approximate_trim: config.approximate_trim,
        io_timeout: valkey_io_timeout(&config)?,
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

fn valkey_io_timeout(config: &ValkeyConfig) -> Result<Duration, SinkError> {
    let timeout_ms = config.io_timeout_ms.unwrap_or(DEFAULT_VALKEY_IO_TIMEOUT_MS);
    if timeout_ms == 0 {
        return Err(SinkError::new(
            "Invalid Valkey config: io_timeout_ms must be greater than zero",
        ));
    }
    Ok(Duration::from_millis(timeout_ms))
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

    fn delivery<'a>(
        config: &'a serde_json::Value,
        routing: &'a serde_json::Value,
        secret_ref: Option<&'a str>,
    ) -> SinkDelivery<'a> {
        SinkDelivery::new(config, routing, secret_ref)
    }

    #[test]
    fn routing_requires_stream() {
        let config = serde_json::json!({});
        let routing = serde_json::json!({"stream": ""});
        let error = parse_routing(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey routing: stream is required"
        );
    }

    #[test]
    fn config_requires_uri_and_valid_max_len() {
        let config = serde_json::json!({
            "uri": "redis://localhost/0",
            "max_len": 0
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: max_len must be greater than zero"
        );
    }

    #[test]
    fn config_rejects_literal_uri_credentials() {
        let config = serde_json::json!({
            "uri": "rediss://:password@example/0"
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: uri credentials must use {secret} with secret_ref"
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
                max_payload_bytes: None,
                io_timeout_ms: None,
            },
        )
        .unwrap();

        assert_eq!(entry.stream, "hubuum:events");
        assert_eq!(entry.max_len, Some(1000));
        assert_eq!(
            entry.io_timeout,
            std::time::Duration::from_millis(DEFAULT_VALKEY_IO_TIMEOUT_MS)
        );
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
