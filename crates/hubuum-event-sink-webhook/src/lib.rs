//! Generic JSON webhook delivery with optional bounded payload templates.
mod http;
mod response;

use std::fmt;

use hubuum_domain::EventDeliveryId;
use hubuum_event_rendering::{EventTemplate, MAX_MESSAGE_BYTES, event_context, render_context};
use hubuum_event_sinks_common::{
    EventEnvelope, SinkDelivery, SinkError, ensure_payload_within_limit,
};
use hubuum_outbound_http::OutboundHeaders;
use hubuum_secrets::{SecretName, SecretValue};
use serde::Deserialize;
use serde_json::Value;

pub use http::HttpSinkSettings as WebhookSinkSettings;
use http::validate_url;
use response::ResponsePolicy;

#[derive(Debug, Clone)]
pub struct WebhookSink {
    settings: WebhookSinkSettings,
}

#[derive(Default, Deserialize)]
struct WebhookRouting {
    #[serde(default)]
    url: Option<String>,
}

#[derive(Default, Deserialize)]
struct WebhookConfig {
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_response_bytes: Option<usize>,
    #[serde(default)]
    max_request_bytes: Option<usize>,
    #[serde(default)]
    headers: Option<serde_json::Map<String, Value>>,
    #[serde(default)]
    body_template: Option<String>,
    #[serde(default)]
    url_secret_ref: Option<String>,
    #[serde(default)]
    response: ResponsePolicy,
}

macro_rules! redacted_debug {
    ($name:ty) => {
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .finish_non_exhaustive()
            }
        }
    };
}
redacted_debug!(WebhookRouting);
redacted_debug!(WebhookConfig);

/// Configuration validated once before rendering or delivery.
pub struct Configuration {
    config: WebhookConfig,
    url_secret_ref: Option<SecretName>,
}

impl Configuration {
    pub fn parse(value: &Value) -> Result<Self, SinkError> {
        let mut config: WebhookConfig = serde_json::from_value(value.clone())
            .map_err(|_| SinkError::permanent("Invalid webhook configuration"))?;
        if config.timeout_ms == Some(0)
            || config.max_response_bytes == Some(0)
            || config.max_request_bytes == Some(0)
        {
            return Err(SinkError::permanent(
                "Webhook limits must be greater than zero",
            ));
        }
        let url_secret_ref = config.url_secret_ref.take().map(SecretName::new).transpose()
            .map_err(|_| SinkError::permanent("url_secret_ref must contain 1-128 ASCII letters, numbers, underscores, or hyphens"))?;
        if let Some(source) = &config.body_template {
            EventTemplate::new("body_template", source, MAX_MESSAGE_BYTES)?;
        }
        webhook_headers(&config, None)?;
        config.response.validate()?;
        Ok(Self {
            config,
            url_secret_ref,
        })
    }

    pub async fn validate(&self) -> Result<(), SinkError> {
        if let Some(source) = &self.config.body_template {
            EventTemplate::new("body_template", source, MAX_MESSAGE_BYTES)?
                .validate()
                .await?;
        }
        Ok(())
    }

    pub fn validate_routing(&self, routing: &Value) -> Result<(), SinkError> {
        self.routing(routing).map(|_| ())
    }

    fn routing(&self, value: &Value) -> Result<WebhookRouting, SinkError> {
        let routing: WebhookRouting = serde_json::from_value(value.clone())
            .map_err(|_| SinkError::permanent("Invalid webhook routing"))?;
        if self.url_secret_ref.is_some() && routing.url.is_some() {
            return Err(SinkError::permanent(
                "routing.url cannot override url_secret_ref",
            ));
        }
        if let Some(url) = &routing.url {
            validate_url(url)?;
        }
        // Legacy subscriptions may be saved before their routing URL is configured.
        // A send or preview requires a complete destination below.
        Ok(routing)
    }
}

enum Destination {
    Url(String),
    Secret(SecretName),
}

/// A rendered and size-checked request prepared before delivery admission.
/// Neither credentials nor destination URLs are exposed in previews or Debug.
pub struct PreparedWebhook {
    configuration: Configuration,
    destination: Destination,
    payload: Value,
    body: String,
    event_id: String,
    idempotency_key: Option<String>,
    test: bool,
}
redacted_debug!(PreparedWebhook);

impl PreparedWebhook {
    /// Give queued tests a stable identity distinct from normal event delivery.
    pub fn for_delivery(mut self, delivery_id: EventDeliveryId) -> Self {
        if self.test {
            self.idempotency_key = Some(format!("{}-test-{}", self.event_id, delivery_id.id()));
        }
        self
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn url_secret_ref(&self) -> Option<&str> {
        match &self.destination {
            Destination::Secret(alias) => Some(alias.as_str()),
            Destination::Url(_) => None,
        }
    }
}

impl WebhookSink {
    pub fn new(settings: WebhookSinkSettings) -> Self {
        Self { settings }
    }

    pub async fn prepare(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<PreparedWebhook, SinkError> {
        let configuration = Configuration::parse(delivery.config())?;
        let routing = configuration.routing(delivery.routing())?;
        let destination = match (&configuration.url_secret_ref, routing.url) {
            (Some(alias), None) => Destination::Secret(alias.clone()),
            (None, Some(url)) => Destination::Url(url),
            _ => {
                return Err(SinkError::permanent(
                    "Webhook requires routing.url or config.url_secret_ref",
                ));
            }
        };
        let max_request_bytes = self
            .settings
            .request_bytes(configuration.config.max_request_bytes);
        let (payload, body) = if let Some(source) = &configuration.config.body_template {
            let mut context = event_context(envelope, MAX_MESSAGE_BYTES)?;
            context["test"] = delivery.is_test().into();
            context["test_marker"] = if delivery.is_test() { "[TEST] " } else { "" }.into();
            let template = EventTemplate::new(
                "body_template",
                source,
                max_request_bytes.min(MAX_MESSAGE_BYTES),
            )?;
            let body = render_context(&context, &[template])
                .await?
                .pop()
                .ok_or_else(|| SinkError::permanent("Missing webhook template result"))?;
            let payload: Value = serde_json::from_str(&body).map_err(|_| {
                SinkError::permanent(
                    "Webhook body_template must render valid JSON; use tojson for event values",
                )
            })?;
            (payload, body)
        } else {
            let payload = serde_json::to_value(envelope)
                .map_err(|_| SinkError::permanent("Cannot serialize webhook event"))?;
            let body = payload.to_string();
            (payload, body)
        };
        ensure_payload_within_limit("webhook", body.len(), max_request_bytes)?;
        Ok(PreparedWebhook {
            configuration,
            destination,
            payload,
            body,
            event_id: envelope.event_id().to_string(),
            idempotency_key: (!delivery.is_test()).then(|| envelope.event_id().to_string()),
            test: delivery.is_test(),
        })
    }

    /// Send a prepared request. URL and bearer credentials are resolved independently.
    pub async fn send(
        &self,
        prepared: &PreparedWebhook,
        bearer: Option<&SecretValue>,
        url_secret: Option<&SecretValue>,
    ) -> Result<(), SinkError> {
        let config = &prepared.configuration.config;
        let url = match &prepared.destination {
            Destination::Url(url) => url.clone(),
            Destination::Secret(_) => url_secret
                .ok_or_else(|| SinkError::new("Webhook URL secret is unavailable"))?
                .expose_utf8()?
                .to_string(),
        };
        validate_url(&url)?;
        let mut headers = webhook_headers(config, bearer)?;
        for (name, value) in [
            ("content-type", "application/json"),
            ("accept", "application/json"),
            ("x-hubuum-event-id", prepared.event_id.as_str()),
        ] {
            headers
                .insert(name, value)
                .map_err(|_| SinkError::permanent("Invalid webhook header"))?;
        }
        if let Some(key) = &prepared.idempotency_key {
            headers
                .insert("idempotency-key", key)
                .map_err(|_| SinkError::permanent("Invalid webhook idempotency key"))?;
        }
        if prepared.test {
            headers
                .insert("x-hubuum-delivery-purpose", "test")
                .map_err(|_| SinkError::permanent("Invalid webhook purpose header"))?;
        }
        let settings = self.settings.bounded(
            config.timeout_ms,
            config.max_response_bytes,
            config.max_request_bytes,
        );
        let response = settings.post(url, headers, prepared.body.clone()).await?;
        config
            .response
            .check(&response, settings.response_bytes(None))
    }

    /// Legacy routing-URL delivery; secret-backed URLs use prepare and send.
    pub async fn deliver(
        &self,
        envelope: &EventEnvelope,
        delivery: SinkDelivery<'_>,
    ) -> Result<(), SinkError> {
        let prepared = self.prepare(envelope, delivery).await?;
        self.send(&prepared, delivery.secret(), None).await
    }
}

fn webhook_headers(
    config: &WebhookConfig,
    secret: Option<&SecretValue>,
) -> Result<OutboundHeaders, SinkError> {
    let mut headers = OutboundHeaders::new();
    if let Some(config_headers) = &config.headers {
        for (name, value) in config_headers {
            let value = value.as_str().ok_or_else(|| {
                SinkError::permanent("Invalid webhook config: headers values must be strings")
            })?;
            headers
                .insert(name, value)
                .map_err(|_| SinkError::permanent("Invalid webhook header"))?;
        }
    }
    if let Some(secret) = secret {
        headers
            .insert(
                "authorization",
                &format!("Bearer {}", secret.expose_utf8()?),
            )
            .map_err(|_| SinkError::permanent("Resolved webhook secret is not a valid header"))?;
    }
    Ok(headers)
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use chrono::Utc;
    use hubuum_events_core::CorrelationId;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn parsed_webhook_debug_omits_routing_and_header_values() {
        let routing = WebhookRouting {
            url: Some("https://example.invalid/hook?token=routing-secret".to_string()),
        };
        let config = WebhookConfig {
            headers: Some(serde_json::Map::from_iter([(
                "authorization".to_string(),
                serde_json::Value::String("header-secret".to_string()),
            )])),
            ..WebhookConfig::default()
        };

        let routing_debug = format!("{routing:?}");
        let config_debug = format!("{config:?}");

        assert_eq!(routing_debug, "WebhookRouting { .. }");
        assert_eq!(config_debug, "WebhookConfig { .. }");
        assert!(!routing_debug.contains("routing-secret"));
        assert!(!config_debug.contains("header-secret"));
    }

    const LOCALHOST_CERT_DER_B64: &str = "MIIDHzCCAgegAwIBAgIUT7YypqM2YgvdrXLHby8OFyeNEEIwDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYyMzA0MDEyMloXDTI2MDYyNDA0MDEyMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn3A378veyRzeP7MSS/S61EPpE+v9Z+fGlFC4qB8SOUHvO1D6+QZrqcKkUJZb/HKnQyDydMNMBJfjswid5l18ogPVFmfGInGp50T3ceH8i1DAnN1Bj6g6h/QgKe64elkYDukaoHkqLGiQ7Nwsllm8UqwdgFa+B1hYD6uoYAcd/4gv5ClxOx6bkwganvWas+PXyHEEdYW7YBRAyPrJHIInWjck5k5UJPn5Vy551ptGpurvUqf2M7VcmnxjHAldTnc9br+chIvLtyulWg8pBAdFwu+4ZM0jWQpTRhVi5lWB+q7mmI8Da4izV0/K2a1bDnSN6j4rmAzEknok0fMoGXzWjQIDAQABo2kwZzAdBgNVHQ4EFgQUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwHwYDVR0jBBgwFoAUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwDwYDVR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJFxe1GtT9g/PI0Ht912WKwCJc8Oj0U49zUK8TRe9VZHMaJriozeS+4P6I6RhmMR4RV2bPtvjQjzv9ZCHoGoiPUupHd+PUGn8oyezDWoGLuwlPE0dQyn3OAdV1no6q/HI6PFThHTd2o/cLl3nfyIu56sCRLiwrMg6xH3UZ6VJ4qjtxTuyYloMNrb09Uyo7G1Qpw7qfiOB8whyJcjC8Gx1H1JTmF/h/CU2u79yAcVIRA4N6zJLAdtsseUjyTb5CAagmvZ6wZBqB+XNCwXzV09+56zt5fFtopF7mBgQcE21wtlzoKKLUyivc5FzgOHPv3YDJiooYyFXcOOobY1B0k8ih8=";
    const LOCALHOST_KEY_DER_B64: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCfcDfvy97JHN4/sxJL9LrUQ+kT6/1n58aUULioHxI5Qe87UPr5BmupwqRQllv8cqdDIPJ0w0wEl+OzCJ3mXXyiA9UWZ8YicannRPdx4fyLUMCc3UGPqDqH9CAp7rh6WRgO6RqgeSosaJDs3CyWWbxSrB2AVr4HWFgPq6hgBx3/iC/kKXE7HpuTCBqe9Zqz49fIcQR1hbtgFEDI+skcgidaNyTmTlQk+flXLnnWm0am6u9Sp/YztVyafGMcCV1Odz1uv5yEi8u3K6VaDykEB0XC77hkzSNZClNGFWLmVYH6ruaYjwNriLNXT8rZrVsOdI3qPiuYDMSSeiTR8ygZfNaNAgMBAAECggEAAQH66ebA1Y9whamibqggtQiyrd6HAohCnR1CEhpOWCcaXPbuAtJNkUapRSf72gAAND4v3j2ikL1S+P9Yxhc7lBclbMoV+3uxk5+qFYVxzNlzsz1RoLUMs0IkCtEt6L/UyIaLDjLGUCavrIAKuxNKlM0/EOOgCcyljFuUUAIKIwOcOKv7rG/t7GC+wZMTT3oyICgihwsN7D527BTKRlk6zcSCj38B21drfgLAMreGRt8NGcByhzo3BuazRkYyEw8SP9LCEbDQKwWGR2xJtxwnSHcrvYvSklhDAB3EP29URstGUxapRg4re25e3MRVIjVdYtCeGt8Ie71UZgO/lgwYAQKBgQDPL192FKjTUwqfhjICpXYiNbbseXw7dvvNfLOZvuE20zPTkwwEWkpF2dxQX44RfYS625jzj9GHRijKwL6HlV89i+pNw+N2OWLUdWkkeMVqqknSPgJavZ4O3WKpk+cSgVm0VgaxNfvwoNi+TnLQblP6YFoXMG/luY3wYg0CviHzAQKBgQDFAPGIU/G6SYAnD5SJcojUXKzH3ivvciBYuLJt4FGUlfym9fnkQNbGNJAL4c3otPTcR/r0br2JIrxod5/w4c93Q4EKmXEwMdW26npxDR8uO/caSvFGZweikqxIj0Im5UlGV3cuanFb+u0jZWjCjFxMO2sWGRMdwrgQm+GyG7z/jQKBgA+vxIiKM+YcKXe+j1bH9FPOwVTSNefCsHn0cRy46RBfmVLxlT1XILx9LEMhmP4WBNCpA8GdJ/4X/8qqIULeumFMkKbmp/gxjBwN77IFOt1Cm2hBraf1J1x0wp2YRyyNgp82zDbqoXKsmvx9sA+76rvQQ8Hxtucrz2Vd5yJIBwYBAoGAaLd7q8+TKkZvjFPHzNfIy7kHTqZWDE1JzF9A2Q7nzmd7iPQvBJlCkNDX0LkSTqQBlCXey5chwIdqRs1vgwdE1ExZh1zQwaF7zGMO+pDTBixxyNQVNCsH7+6vDVK5AxvVu0I6471IzG+xJaN98AvT8+GRpollk+gxFwMFETuVVvECgYAJ8qBnL/YnusNmORCdItqG6adl+0H4ohikxNurIP8cBRjKGJ6XSC2Qs3BmljiqL9aLluKTcbhOBKlH6iq63vA8KxF7JjVBj2NXClDh6MO6hr/4gWTi7VMpC3CWT80IijoMAth37y+MImdaJhG2kut+XcT14KFakVJM1JCbe0Ygdw==";

    pub(super) fn envelope() -> EventEnvelope {
        EventEnvelope::builder()
            .id(hubuum_events_core::EventSequence::new(42).unwrap())
            .event_id(Uuid::new_v4())
            .occurred_at(Utc::now())
            .entity_type(hubuum_events_core::EntityType::Collection)
            .entity_id(Some(hubuum_events_core::EventEntityId::new(7).unwrap()))
            .entity_name(Some("example".to_string()))
            .collection_id(Some(hubuum_events_core::CollectionId::new(7).unwrap()))
            .action(hubuum_events_core::Action::Created)
            .actor_user_id(Some(hubuum_events_core::PrincipalId::new(1).unwrap()))
            .actor_kind(hubuum_events_core::ActorKind::User)
            .provenance(hubuum_events_core::Provenance {
                actor: hubuum_events_core::ProvenanceActor {
                    kind: Some("user".to_string()),
                    principal: Some(hubuum_events_core::ProvenancePrincipal {
                        principal_id: hubuum_events_core::PrincipalId::new(1).unwrap(),
                        name: Some("admin".to_string()),
                    }),
                },
                initiator: Some(hubuum_events_core::ProvenancePrincipal {
                    principal_id: hubuum_events_core::PrincipalId::new(1).unwrap(),
                    name: Some("admin".to_string()),
                }),
                task_id: Some(hubuum_events_core::TaskId::new(99).unwrap()),
            })
            .correlation_id(Some(CorrelationId::new("corr-1").unwrap()))
            .summary("collection created".to_string())
            .after(Some(serde_json::json!({"name": "example"})))
            .metadata(serde_json::json!({"source": "test"}))
            .schema_version(1)
            .try_build()
            .unwrap()
    }

    pub(super) fn settings() -> WebhookSinkSettings {
        WebhookSinkSettings::new(30_000, 1_000_000)
            .unwrap()
            .dangerous_accept_invalid_certs(true)
            .dangerous_allow_localhost(true)
    }

    #[test]
    fn webhook_sink_settings_reject_zero_timeout() {
        let error = WebhookSinkSettings::new(0, 1_000_000).unwrap_err();

        assert_eq!(
            error.to_string(),
            "HTTP sink maximum timeout must be greater than zero"
        );
    }

    #[test]
    fn webhook_sink_settings_reject_zero_response_limit() {
        let error = WebhookSinkSettings::new(30_000, 0).unwrap_err();

        assert_eq!(
            error.to_string(),
            "HTTP sink maximum response size must be greater than zero"
        );
    }

    #[test]
    fn webhook_sink_settings_reject_zero_request_limit() {
        let error = WebhookSinkSettings::new(30_000, 1_000_000)
            .unwrap()
            .with_max_request_bytes(0)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "HTTP sink maximum request size must be greater than zero"
        );
    }

    fn delivery(url: String) -> (serde_json::Value, serde_json::Value) {
        (
            serde_json::json!({
                "headers": {
                    "x-custom": "custom"
                }
            }),
            serde_json::json!({ "url": url }),
        )
    }

    pub(super) async fn spawn_https_server(
        status_line: &'static str,
    ) -> (u16, oneshot::Receiver<String>) {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cert_der = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_CERT_DER_B64)
            .unwrap();
        let key_der = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_KEY_DER_B64)
            .unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(cert_der)],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der)),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (request_tx, request_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = Vec::new();
            let header_end;
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "client closed before sending request headers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    header_end = index + 4;
                    break;
                }
            }

            let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "client closed before sending request body");
                request.extend_from_slice(&chunk[..read]);
            }

            request_tx
                .send(String::from_utf8_lossy(&request).into_owned())
                .unwrap();
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Length: 2\r\nContent-Type: text/plain\r\n\r\nok"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        (port, request_rx)
    }

    #[tokio::test]
    async fn webhook_sink_posts_event_payload_with_idempotency_header_and_secret() {
        let secret = hubuum_secrets::SecretValue::new(b"expected-token".to_vec()).unwrap();
        let (port, request_rx) = spawn_https_server("202 Accepted").await;
        let envelope = envelope();
        let (config, routing) = delivery(format!("https://localhost:{port}/events"));
        WebhookSink::new(settings())
            .deliver(
                &envelope,
                SinkDelivery::new(&config, &routing, Some(&secret)),
            )
            .await
            .unwrap();

        let request = request_rx.await.unwrap();
        assert!(request.starts_with("POST /events HTTP/1.1"));
        assert!(request.contains("authorization: Bearer expected-token"));
        assert!(request.contains("idempotency-key: "));
        assert!(request.contains(&envelope.event_id().to_string()));
        assert!(request.contains("x-hubuum-event-id: "));
        assert!(request.contains("x-custom: custom"));
        assert!(request.contains("\"entity_type\":\"collection\""));
        assert!(request.contains("\"event_id\""));
        assert!(request.contains("\"provenance\""));
        assert!(request.contains("\"task_id\":99"));
    }

    #[tokio::test]
    async fn webhook_sink_treats_non_success_status_as_delivery_error() {
        let secret = hubuum_secrets::SecretValue::new(b"expected-token".to_vec()).unwrap();
        let (port, request_rx) = spawn_https_server("500 Internal Server Error").await;
        let (config, routing) = delivery(format!("https://localhost:{port}/events"));
        let error = WebhookSink::new(settings())
            .deliver(
                &envelope(),
                SinkDelivery::new(&config, &routing, Some(&secret)),
            )
            .await
            .unwrap_err();

        let _ = request_rx.await.unwrap();
        assert!(error.to_string().contains("HTTP 500"));
    }

    #[tokio::test]
    async fn webhook_sink_rejects_url_credentials_before_delivery() {
        let (config, routing) =
            delivery("https://user:password@example.invalid/events".to_string());
        let error = WebhookSink::new(settings())
            .deliver(&envelope(), SinkDelivery::new(&config, &routing, None))
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Sink URL must be an HTTPS URL without embedded credentials"
        );
    }
}

#[cfg(test)]
mod configuration_tests;
