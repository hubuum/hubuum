use std::fmt;

use hubuum_event_sinks_common::{
    DEFAULT_MAX_ENVELOPE_BYTES, EventEnvelope, SinkDelivery, SinkError, UriConnectionPool,
    ensure_payload_within_limit, parse_sink_config, parse_sink_routing,
    reject_literal_uri_credentials, require_non_empty, require_tls_uri_scheme,
    resolve_event_sink_secret_uri,
};
use hubuum_templates::{TemplateLimits, prepare_template};
use lettre::message::Mailbox;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Default)]
pub struct EmailSink {
    transports: UriConnectionPool<String, AsyncSmtpTransport<Tokio1Executor>>,
}

impl fmt::Debug for EmailSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmailSink").finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
struct EmailConfig {
    uri: String,
    from: String,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default = "default_subject_template")]
    subject_template: String,
    #[serde(default = "default_body_template")]
    body_template: String,
    #[serde(default)]
    max_payload_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct EmailRouting {
    #[serde(default, alias = "to")]
    recipients: Vec<String>,
    #[serde(default)]
    cc: Vec<String>,
    #[serde(default)]
    bcc: Vec<String>,
}

#[derive(Debug)]
struct RenderedEmail {
    subject: String,
    body: String,
}

impl EmailSink {
    pub async fn deliver(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let config = parse_config(&delivery)?;
        let routing = parse_routing(&delivery)?;
        let uri = resolve_event_sink_secret_uri(&config.uri, delivery.secret_ref, "email")?;
        require_tls_uri_scheme(&uri, "email", &["smtps"])?;
        let rendered = render_email(envelope, &config)?;
        let message = build_message(&config, &routing, rendered)?;

        self.transport(&uri)
            .await?
            .send(message)
            .await
            .map_err(|error| SinkError::new(format!("Email SMTP delivery failed: {error}")))?;
        Ok(())
    }

    async fn transport(&self, uri: &str) -> Result<AsyncSmtpTransport<Tokio1Executor>, SinkError> {
        self.transports
            .get_or_try_insert_with(uri.to_string(), |uri| async move {
                AsyncSmtpTransport::<Tokio1Executor>::from_url(&uri)
                    .map_err(|error| SinkError::new(format!("Invalid email config: {error}")))
                    .map(|builder| builder.build())
            })
            .await
    }
}

fn parse_config(delivery: &SinkDelivery<'_>) -> Result<EmailConfig, SinkError> {
    let config: EmailConfig = parse_sink_config(delivery, "email")?;
    require_non_empty(&config.uri, "email config", "uri")?;
    reject_literal_uri_credentials(&config.uri, "email")?;
    require_non_empty(&config.from, "email config", "from")?;
    require_non_empty(&config.subject_template, "email config", "subject_template")?;
    require_non_empty(&config.body_template, "email config", "body_template")?;
    validate_template("subject_template", &config.subject_template)?;
    validate_template("body_template", &config.body_template)?;
    Ok(config)
}

fn parse_routing(delivery: &SinkDelivery<'_>) -> Result<EmailRouting, SinkError> {
    let routing: EmailRouting = parse_sink_routing(delivery, "email")?;
    if routing.recipients.is_empty() {
        return Err(SinkError::new(
            "Invalid email routing: recipients is required",
        ));
    }
    Ok(routing)
}

fn render_email(
    envelope: &EventEnvelope,
    config: &EmailConfig,
) -> Result<RenderedEmail, SinkError> {
    let context = template_context(
        envelope,
        config
            .max_payload_bytes
            .unwrap_or(DEFAULT_MAX_ENVELOPE_BYTES),
    )?;
    let subject = render_template("subject_template", &config.subject_template, &context)?;
    if subject.trim().is_empty() {
        return Err(SinkError::new(
            "Invalid email config: rendered subject is empty",
        ));
    }
    if subject.contains(['\r', '\n']) {
        return Err(SinkError::new(
            "Invalid email config: rendered subject must not contain line breaks",
        ));
    }
    let body = render_template("body_template", &config.body_template, &context)?;
    if body.trim().is_empty() {
        return Err(SinkError::new(
            "Invalid email config: rendered body is empty",
        ));
    }

    Ok(RenderedEmail { subject, body })
}

fn build_message(
    config: &EmailConfig,
    routing: &EmailRouting,
    rendered: RenderedEmail,
) -> Result<Message, SinkError> {
    let mut builder = Message::builder()
        .from(parse_mailbox("from", &config.from)?)
        .subject(rendered.subject);
    if let Some(reply_to) = &config.reply_to {
        builder = builder.reply_to(parse_mailbox("reply_to", reply_to)?);
    }
    for recipient in parse_mailboxes("recipients", &routing.recipients)? {
        builder = builder.to(recipient);
    }
    for recipient in parse_mailboxes("cc", &routing.cc)? {
        builder = builder.cc(recipient);
    }
    for recipient in parse_mailboxes("bcc", &routing.bcc)? {
        builder = builder.bcc(recipient);
    }
    builder
        .body(rendered.body)
        .map_err(|error| SinkError::new(format!("Invalid email message: {error}")))
}

fn template_context(
    envelope: &EventEnvelope,
    max_payload_bytes: usize,
) -> Result<Value, SinkError> {
    let event = serde_json::to_value(envelope).map_err(|error| {
        SinkError::new(format!(
            "Failed to serialize email template context: {error}"
        ))
    })?;
    ensure_payload_within_limit("email", event.to_string().len(), max_payload_bytes)?;
    let mut root = match event.clone() {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    root.insert("event".to_string(), event);
    root.insert(
        "event_id".to_string(),
        Value::String(envelope.event_id.to_string()),
    );
    root.insert(
        "occurred_at".to_string(),
        Value::String(envelope.occurred_at.to_string()),
    );
    Ok(Value::Object(root))
}

fn validate_template(name: &str, source: &str) -> Result<(), SinkError> {
    prepare_template(source)
        .limits(template_limits())
        .validate()
        .map_err(|error| SinkError::new(format!("Invalid email config: {name}: {error}")))
}

fn render_template(name: &str, source: &str, context: &Value) -> Result<String, SinkError> {
    prepare_template(source)
        .limits(template_limits())
        .context(context)
        .render()
        .map_err(|error| SinkError::new(format!("Invalid email config: {name}: {error}")))
}

fn template_limits() -> TemplateLimits {
    TemplateLimits::new(16, 50_000)
}

fn parse_mailboxes(label: &str, values: &[String]) -> Result<Vec<Mailbox>, SinkError> {
    values
        .iter()
        .map(|value| parse_mailbox(label, value))
        .collect()
}

fn parse_mailbox(label: &str, value: &str) -> Result<Mailbox, SinkError> {
    if value.trim().is_empty() {
        return Err(SinkError::new(format!(
            "Invalid email routing: {label} contains an empty address"
        )));
    }
    value.parse::<Mailbox>().map_err(|error| {
        SinkError::new(format!(
            "Invalid email routing: {label} contains an invalid address: {error}"
        ))
    })
}

fn default_subject_template() -> String {
    "Hubuum {{ entity_type }} {{ action }}: {{ entity_name | default_if_empty(summary) }}"
        .to_string()
}

fn default_body_template() -> String {
    concat!(
        "{{ summary }}\n\n",
        "Entity: {{ entity_type }}",
        "{% if entity_name %} {{ entity_name }}{% endif %}\n",
        "Action: {{ action }}\n",
        "Event: {{ event_id }}\n",
        "Occurred: {{ occurred_at }}\n"
    )
    .to_string()
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
        config: &'a Value,
        routing: &'a Value,
        secret_ref: Option<&'a str>,
    ) -> SinkDelivery<'a> {
        SinkDelivery::new(config, routing, secret_ref)
    }

    fn config() -> EmailConfig {
        let config = serde_json::json!({
            "uri": "smtp://localhost:2525",
            "from": "Hubuum <hubuum@example.invalid>"
        });
        let routing = serde_json::json!({});
        parse_config(&delivery(&config, &routing, None)).unwrap()
    }

    #[test]
    fn routing_requires_recipients() {
        let config = serde_json::json!({});
        let routing = serde_json::json!({"recipients": []});
        let error = parse_routing(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid email routing: recipients is required"
        );
    }

    #[test]
    fn routing_accepts_to_alias() {
        let config = serde_json::json!({});
        let routing = serde_json::json!({"to": ["ops@example.invalid"]});
        let routing = parse_routing(&delivery(&config, &routing, None)).unwrap();
        assert_eq!(routing.recipients, vec!["ops@example.invalid"]);
    }

    #[test]
    fn config_requires_uri_from_and_templates() {
        let config = serde_json::json!({
            "uri": "",
            "from": "hubuum@example.invalid"
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(error.to_string(), "Invalid email config: uri is required");

        let config = serde_json::json!({
            "uri": "smtp://localhost:2525",
            "from": ""
        });
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(error.to_string(), "Invalid email config: from is required");
    }

    #[test]
    fn default_templates_render_readable_event_email() {
        let envelope = envelope();
        let rendered = render_email(&envelope, &config()).unwrap();

        assert_eq!(rendered.subject, "Hubuum collection created: example");
        assert!(rendered.body.contains("collection created"));
        assert!(rendered.body.contains(&envelope.event_id.to_string()));
    }

    #[test]
    fn rendered_subject_must_not_contain_line_breaks() {
        let mut config = config();
        config.subject_template = "hello\n{{ summary }}".to_string();

        let error = render_email(&envelope(), &config).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Invalid email config: rendered subject must not contain line breaks"
        );
    }

    #[test]
    fn builds_message_with_recipients() {
        let config_json = serde_json::json!({});
        let routing = serde_json::json!({
            "recipients": ["Ops <ops@example.invalid>"],
            "cc": ["audit@example.invalid"],
            "bcc": ["archive@example.invalid"]
        });
        let routing = parse_routing(&delivery(&config_json, &routing, None)).unwrap();
        let message = build_message(
            &config(),
            &routing,
            RenderedEmail {
                subject: "Test".to_string(),
                body: "Body".to_string(),
            },
        )
        .unwrap();

        assert_eq!(message.envelope().to().len(), 3);
    }

    #[test]
    fn secret_ref_replaces_smtp_uri_placeholder_with_encoded_secret() {
        let secret_ref = "email_sink_unit_test";
        unsafe {
            std::env::set_var("HUBUUM_EVENT_SINK_SECRET_EMAIL_SINK_UNIT_TEST", "p@ss/w:rd");
        }

        let uri = resolve_event_sink_secret_uri(
            "smtps://publisher:{secret}@smtp.example",
            Some(secret_ref),
            "email",
        )
        .unwrap();

        assert_eq!(uri, "smtps://publisher:p%40ss%2Fw%3Ard@smtp.example");

        unsafe {
            std::env::remove_var("HUBUUM_EVENT_SINK_SECRET_EMAIL_SINK_UNIT_TEST");
        }
    }

    #[test]
    fn config_rejects_literal_uri_credentials() {
        let config = serde_json::json!({
            "uri": "smtps://user:password@smtp.example",
            "from": "Hubuum <hubuum@example.invalid>"
        });
        let routing = serde_json::json!({});
        let error = parse_config(&delivery(&config, &routing, None)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid email config: uri credentials must use {secret} with secret_ref"
        );
    }
}
