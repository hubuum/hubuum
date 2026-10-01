//! Provider-specific chat notification configuration and acknowledgements.
use hubuum_event_rendering::{
    EventTemplate, MAX_MESSAGE_BYTES, default_text_template, render, rich_array, validate_text,
};
use hubuum_event_sinks_common::{EventEnvelope, SinkDelivery, SinkError};
use hubuum_event_sinks_http::{HttpSinkSettings, check_chat_status, json_headers, validate_url};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Transport {
    Webhook,
    Bot,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    transport: Transport,
    #[serde(default = "default_text_template")]
    text_template: String,
    #[serde(default)]
    attachments_template: Option<String>,
    #[serde(default)]
    server_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Routing {
    #[serde(default)]
    channel_id: Option<String>,
}

impl Configuration {
    pub fn parse(value: &Value) -> Result<Self, SinkError> {
        let config: Self = serde_json::from_value(value.clone())
            .map_err(|_| SinkError::permanent("Invalid Mattermost configuration"))?;
        EventTemplate::new("text_template", &config.text_template, MAX_MESSAGE_BYTES)?;
        if let Some(source) = &config.attachments_template {
            EventTemplate::new("attachments_template", source, MAX_MESSAGE_BYTES)?;
        }
        match (&config.transport, config.server_url.as_deref()) {
            (Transport::Bot, Some(url)) if !url.contains(['?', '#']) => validate_url(url)?,
            (Transport::Webhook, None) => {}
            _ => {
                return Err(SinkError::permanent(
                    "Mattermost bot configuration requires server_url; webhook configuration must omit it",
                ));
            }
        }
        Ok(config)
    }

    pub async fn validate(&self) -> Result<(), SinkError> {
        EventTemplate::new("text_template", &self.text_template, MAX_MESSAGE_BYTES)?
            .validate()
            .await?;
        if let Some(source) = &self.attachments_template {
            EventTemplate::new("attachments_template", source, MAX_MESSAGE_BYTES)?
                .validate()
                .await?;
        }
        Ok(())
    }

    fn routing(&self, value: &Value) -> Result<Routing, SinkError> {
        let routing: Routing = serde_json::from_value(value.clone())
            .map_err(|_| SinkError::permanent("Invalid Mattermost routing"))?;
        match (self.transport, routing.channel_id.as_deref()) {
            (Transport::Webhook, None) => {}
            (Transport::Bot, Some(id))
                if !id.is_empty()
                    && id.len() <= 128
                    && id.bytes().all(|byte| byte.is_ascii_alphanumeric()) => {}
            _ => {
                return Err(SinkError::permanent(
                    "Bot routing requires channel_id; webhook routing must not override the channel",
                ));
            }
        }
        Ok(routing)
    }

    pub fn validate_routing(&self, value: &Value) -> Result<(), SinkError> {
        self.routing(value).map(|_| ())
    }

    pub async fn preview(
        &self,
        envelope: &EventEnvelope,
        routing: &Value,
        test: bool,
    ) -> Result<Value, SinkError> {
        let routing = self.routing(routing)?;
        let mut templates = vec![EventTemplate::new(
            "text_template",
            &self.text_template,
            MAX_MESSAGE_BYTES,
        )?];
        if let Some(source) = &self.attachments_template {
            templates.push(EventTemplate::new(
                "attachments_template",
                source,
                MAX_MESSAGE_BYTES,
            )?);
        }
        let mut rendered = render(envelope, &templates, MAX_MESSAGE_BYTES)
            .await?
            .into_iter();
        let mut text = rendered
            .next()
            .ok_or_else(|| SinkError::permanent("Missing text template result"))?;
        if test {
            text = format!("[TEST] {text}");
        }
        validate_text(&text, 16000)?;
        let mut parts = rendered
            .next()
            .map(|value| rich_array(&value, 100))
            .transpose()?;
        if test && let Some(parts) = &mut parts {
            parts.insert(0, json!({"text":"TEST notification"}));
        }
        if parts.as_ref().is_some_and(|parts| parts.len() > 100) {
            return Err(SinkError::permanent(
                "Mattermost notification exceeds attachment limit including the test marker",
            ));
        }
        let mut payload = match self.transport {
            Transport::Webhook => json!({"text":text}),
            Transport::Bot => json!({"message":text,"channel_id":routing.channel_id}),
        };
        if let Some(parts) = parts {
            match self.transport {
                Transport::Webhook => payload["attachments"] = parts.into(),
                Transport::Bot => payload["props"] = json!({"attachments":parts}),
            }
        }
        if payload.to_string().len() > MAX_MESSAGE_BYTES {
            return Err(SinkError::permanent(
                "Rendered notification exceeds payload limit",
            ));
        }
        Ok(payload)
    }
}

/// Provider-validated payload, prepared before reserving a delivery slot.
pub struct PreparedNotification {
    configuration: Configuration,
    payload: Value,
}

#[derive(Debug)]
pub struct MattermostSink {
    settings: HttpSinkSettings,
}

impl MattermostSink {
    pub fn new(settings: HttpSinkSettings) -> Self {
        Self { settings }
    }

    pub async fn deliver(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let prepared = self.prepare(envelope, delivery).await?;
        self.send(&prepared, delivery).await
    }

    pub async fn prepare(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<PreparedNotification, SinkError> {
        let configuration = Configuration::parse(delivery.config())?;
        let payload = configuration
            .preview(envelope, delivery.routing(), delivery.is_test())
            .await?;
        Ok(PreparedNotification {
            configuration,
            payload,
        })
    }

    pub async fn send(
        &self,
        prepared: &PreparedNotification,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let config = &prepared.configuration;
        let payload = &prepared.payload;
        let secret = delivery
            .secret()
            .ok_or_else(|| SinkError::new("Mattermost sink requires secret_ref"))?
            .expose_utf8()?;
        let (url, token) = match config.transport {
            Transport::Webhook => (secret.to_string(), None),
            Transport::Bot => (
                format!(
                    "{}/api/v4/posts",
                    config
                        .server_url
                        .as_deref()
                        .ok_or_else(|| SinkError::permanent("Missing Mattermost server URL"))?
                        .trim_end_matches('/')
                ),
                Some(secret),
            ),
        };
        validate_url(&url)?;
        let response = self
            .settings
            .post(url, json_headers(token)?, payload.to_string())
            .await?;
        check_chat_status(&response)?;
        acknowledge(
            config.transport,
            response.status_code(),
            response.body_preview(),
        )
    }
}

fn acknowledge(transport: Transport, status: u16, body: &str) -> Result<(), SinkError> {
    match transport {
        Transport::Webhook if body.trim() == "ok" => Ok(()),
        Transport::Webhook => Err(SinkError::new(
            "Unexpected Mattermost webhook acknowledgement",
        )),
        Transport::Bot => {
            let body: Value = serde_json::from_str(body)
                .map_err(|_| SinkError::new("Invalid Mattermost API acknowledgement"))?;
            if status == 201
                && body
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| !id.is_empty())
            {
                Ok(())
            } else {
                Err(SinkError::new(
                    "Mattermost API did not acknowledge the notification",
                ))
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(json!({}))]
    #[case(json!({"transport":"other"}))]
    #[case(json!({"transport":"webhook","text_template":""}))]
    #[case(json!({"transport":"webhook","token":"literal-secret"}))]
    fn rejects_invalid_configuration(#[case] value: Value) {
        assert!(Configuration::parse(&value).is_err());
    }

    #[rstest]
    #[case("webhook", json!({}), true)]
    #[case("webhook", json!({"channel_id":"C123"}), false)]
    #[case("bot", json!({}), false)]
    #[case("bot", json!({"channel_id":"C123"}), true)]
    #[case("bot", json!({"channel_id":"bad channel"}), false)]
    fn routing_matches_transport(
        #[case] transport: &str,
        #[case] routing: Value,
        #[case] valid: bool,
    ) {
        let config = Configuration::parse(&if transport == "bot" {
            json!({"transport":transport,"server_url":"https://chat.example.com"})
        } else {
            json!({"transport":transport})
        })
        .unwrap();
        assert_eq!(config.validate_routing(&routing).is_ok(), valid);
    }
    fn event() -> EventEnvelope {
        use hubuum_events_core::{Action, ActorKind, EntityType, EventSequence, Provenance};
        EventEnvelope::builder()
            .id(EventSequence::new(1).unwrap())
            .event_id(uuid::Uuid::new_v4())
            .occurred_at(chrono::Utc::now())
            .entity_type(EntityType::Task)
            .action(Action::Failed)
            .actor_kind(ActorKind::System)
            .provenance(Provenance::default())
            .summary("Failed \"backup\"\ncheck".to_string())
            .metadata(json!({"task_kind":"backup"}))
            .schema_version(1)
            .try_build()
            .unwrap()
    }

    #[tokio::test]
    async fn preview_renders_json_escaped_event_values_and_marks_tests() {
        let config = Configuration::parse(&json!({"transport":"webhook", "attachments_template":"[{\"type\":\"section\",\"text\":{{ summary | tojson }}}]"})).unwrap();
        let event = event();
        let payload = config.preview(&event, &json!({}), true).await.unwrap();
        assert!(payload["text"].as_str().unwrap().starts_with("[TEST]"));
        assert_eq!(payload["attachments"][1]["text"], event.summary());
    }

    #[tokio::test]
    async fn rejects_malformed_template_without_exposing_source() {
        let config = Configuration::parse(
            &json!({"transport":"webhook", "text_template":"private-value {{ broken"}),
        )
        .unwrap();
        let error = config.validate().await.unwrap_err();
        assert!(!error.to_string().contains("private-value"));
    }

    #[rstest]
    #[case(201, r#"{"id":"post123"}"#, true)]
    #[case(200, r#"{"id":"post123"}"#, false)]
    #[case(201, r#"{"id":""}"#, false)]
    #[case(201, "not json", false)]
    fn bot_acknowledgement_requires_created_post(
        #[case] status: u16,
        #[case] body: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(acknowledge(Transport::Bot, status, body).is_ok(), expected);
    }
}
