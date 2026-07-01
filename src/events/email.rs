use std::fmt;

use futures::FutureExt;
use hubuum_templates::{TemplateLimits, prepare_template};
use lettre::message::Mailbox;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::events::sink::{
    EventEnvelope, Sink, SinkError, UriConnectionPool, parse_sink_config, parse_sink_routing,
    require_non_empty, require_tls_uri_scheme, resolve_event_sink_secret_uri,
};
use crate::models::{EventSink, EventSubscription};

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

impl Sink for EmailSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> futures::future::BoxFuture<'a, Result<(), SinkError>> {
        async move { self.deliver_email(envelope, subscription, sink).await }.boxed()
    }
}

impl EmailSink {
    async fn deliver_email(
        &self,
        envelope: &EventEnvelope,
        subscription: &EventSubscription,
        sink: &EventSink,
    ) -> Result<(), SinkError> {
        let config = parse_config(sink)?;
        let routing = parse_routing(subscription)?;
        let uri = resolve_event_sink_secret_uri(&config.uri, sink.secret_ref.as_deref(), "email")?;
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

fn parse_config(sink: &EventSink) -> Result<EmailConfig, SinkError> {
    let config: EmailConfig = parse_sink_config(sink, "email")?;
    require_non_empty(&config.uri, "email config", "uri")?;
    require_non_empty(&config.from, "email config", "from")?;
    require_non_empty(&config.subject_template, "email config", "subject_template")?;
    require_non_empty(&config.body_template, "email config", "body_template")?;
    validate_template("subject_template", &config.subject_template)?;
    validate_template("body_template", &config.body_template)?;
    Ok(config)
}

fn parse_routing(subscription: &EventSubscription) -> Result<EmailRouting, SinkError> {
    let routing: EmailRouting = parse_sink_routing(subscription, "email")?;
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
    let context = template_context(envelope)?;
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

fn template_context(envelope: &EventEnvelope) -> Result<Value, SinkError> {
    let event = serde_json::to_value(envelope).map_err(|error| {
        SinkError::new(format!(
            "Failed to serialize email template context: {error}"
        ))
    })?;
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

    fn sink(config: Value, secret_ref: Option<&str>) -> EventSink {
        let now = Utc::now().naive_utc();
        EventSink {
            id: 1,
            name: "email".to_string(),
            kind: EventSinkKind::Email,
            config,
            secret_ref: secret_ref.map(str::to_string),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn subscription(routing: Value) -> EventSubscription {
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

    fn config() -> EmailConfig {
        parse_config(&sink(
            serde_json::json!({
                "uri": "smtp://localhost:2525",
                "from": "Hubuum <hubuum@example.invalid>"
            }),
            None,
        ))
        .unwrap()
    }

    #[test]
    fn routing_requires_recipients() {
        let error =
            parse_routing(&subscription(serde_json::json!({"recipients": []}))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid email routing: recipients is required"
        );
    }

    #[test]
    fn routing_accepts_to_alias() {
        let routing = parse_routing(&subscription(
            serde_json::json!({"to": ["ops@example.invalid"]}),
        ))
        .unwrap();
        assert_eq!(routing.recipients, vec!["ops@example.invalid"]);
    }

    #[test]
    fn config_requires_uri_from_and_templates() {
        let error = parse_config(&sink(
            serde_json::json!({
                "uri": "",
                "from": "hubuum@example.invalid"
            }),
            None,
        ))
        .unwrap_err();
        assert_eq!(error.to_string(), "Invalid email config: uri is required");

        let error = parse_config(&sink(
            serde_json::json!({
                "uri": "smtp://localhost:2525",
                "from": ""
            }),
            None,
        ))
        .unwrap_err();
        assert_eq!(error.to_string(), "Invalid email config: from is required");
    }

    #[test]
    fn default_templates_render_readable_event_email() {
        let envelope = envelope();
        let rendered = render_email(&envelope, &config()).unwrap();

        assert_eq!(rendered.subject, "Hubuum namespace created: example");
        assert!(rendered.body.contains("namespace created"));
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
        let routing = parse_routing(&subscription(serde_json::json!({
            "recipients": ["Ops <ops@example.invalid>"],
            "cc": ["audit@example.invalid"],
            "bcc": ["archive@example.invalid"]
        })))
        .unwrap();
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
}
