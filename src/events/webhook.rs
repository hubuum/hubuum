use std::time::Duration;

use futures::FutureExt;
use hubuum_outbound_http::{OutboundHeaders, OutboundMethod, OutboundRequest};
use serde::Deserialize;
use tracing::warn;

use crate::config::{
    DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS, DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
    DEFAULT_REMOTE_CALL_TIMEOUT_MS, get_config,
};
use crate::events::sink::{EventEnvelope, Sink, SinkError};
use crate::models::{EventSink, EventSubscription};

#[derive(Debug, Default)]
pub struct WebhookSink;

#[derive(Debug, Deserialize)]
struct WebhookRouting {
    url: String,
}

#[derive(Debug, Default, Deserialize)]
struct WebhookConfig {
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_response_bytes: Option<usize>,
    #[serde(default)]
    headers: Option<serde_json::Map<String, serde_json::Value>>,
}

impl Sink for WebhookSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSink,
    ) -> futures::future::BoxFuture<'a, Result<(), SinkError>> {
        async move { deliver_webhook(envelope, subscription, sink).await }.boxed()
    }
}

async fn deliver_webhook(
    envelope: &EventEnvelope,
    subscription: &EventSubscription,
    sink: &EventSink,
) -> Result<(), SinkError> {
    let routing: WebhookRouting = serde_json::from_value(subscription.routing.clone())
        .map_err(|error| SinkError::new(format!("Invalid webhook routing: {error}")))?;
    if routing.url.trim().is_empty() {
        return Err(SinkError::new("Invalid webhook routing: url is required"));
    }

    let config: WebhookConfig = serde_json::from_value(sink.config.clone())
        .map_err(|error| SinkError::new(format!("Invalid webhook config: {error}")))?;

    let mut headers = webhook_headers(&config, sink.secret_ref.as_deref())?;
    headers
        .insert("content-type", "application/json")
        .map_err(sink_error)?;
    headers
        .insert("accept", "application/json")
        .map_err(sink_error)?;
    headers
        .insert("idempotency-key", &envelope.event_id.to_string())
        .map_err(sink_error)?;
    headers
        .insert("x-hubuum-event-id", &envelope.event_id.to_string())
        .map_err(sink_error)?;

    let body = serde_json::to_string(envelope)
        .map_err(|error| SinkError::new(format!("Failed to serialize webhook payload: {error}")))?;

    #[cfg(test)]
    let dangerous_accept_invalid_certs = true;
    #[cfg(not(test))]
    let dangerous_accept_invalid_certs = false;
    #[cfg(test)]
    let dangerous_allow_localhost = true;
    #[cfg(not(test))]
    let dangerous_allow_localhost = false;

    let response = OutboundRequest::new(
        OutboundMethod::Post,
        routing.url,
        Duration::from_millis(bounded_timeout_ms(config.timeout_ms)),
    )
    .headers(headers)
    .body(Some(body))
    .max_response_bytes(bounded_response_bytes(config.max_response_bytes))
    .allow_private_targets(allow_private_targets())
    .dangerous_accept_invalid_certs(dangerous_accept_invalid_certs)
    .dangerous_allow_localhost(dangerous_allow_localhost)
    .send()
    .await
    .map_err(|error| SinkError::new(format!("Webhook delivery failed: {error}")))?;

    if !response.is_success() {
        return Err(SinkError::new(format!(
            "Webhook delivery failed with HTTP {}",
            response.status_display()
        )));
    }

    Ok(())
}

fn webhook_headers(
    config: &WebhookConfig,
    secret_ref: Option<&str>,
) -> Result<OutboundHeaders, SinkError> {
    let mut headers = OutboundHeaders::new();
    if let Some(config_headers) = &config.headers {
        for (name, value) in config_headers {
            let Some(value) = value.as_str() else {
                return Err(SinkError::new(
                    "Invalid webhook config: headers values must be strings",
                ));
            };
            headers.insert(name, value).map_err(sink_error)?;
        }
    }

    if let Some(secret_ref) = secret_ref {
        let secret = resolve_event_sink_secret(secret_ref)?;
        headers
            .insert("authorization", &format!("Bearer {secret}"))
            .map_err(|_| SinkError::new("Resolved webhook secret is not a valid header"))?;
    }

    Ok(headers)
}

fn resolve_event_sink_secret(secret_ref: &str) -> Result<String, SinkError> {
    let key = format!(
        "HUBUUM_EVENT_SINK_SECRET_{}",
        secret_ref.to_ascii_uppercase()
    );
    std::env::var(&key).map_err(|_| {
        SinkError::new(format!(
            "Event sink secret reference '{secret_ref}' is not configured"
        ))
    })
}

fn bounded_timeout_ms(requested: Option<u64>) -> u64 {
    let cap = get_config()
        .map(|config| config.remote_call_timeout_ms)
        .unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS);
    let requested = requested.unwrap_or(cap);
    if requested > cap {
        warn!(
            message = "Webhook timeout clamped to configured maximum",
            requested_timeout_ms = requested,
            configured_max_timeout_ms = cap
        );
    }
    requested.min(cap)
}

fn bounded_response_bytes(requested: Option<usize>) -> usize {
    let cap = get_config()
        .map(|config| config.remote_call_max_response_bytes)
        .unwrap_or(DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES);
    requested.unwrap_or(cap).min(cap)
}

fn allow_private_targets() -> bool {
    get_config()
        .map(|config| config.remote_call_allow_private_targets)
        .unwrap_or(DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS)
}

fn sink_error(error: hubuum_outbound_http::OutboundHttpError) -> SinkError {
    SinkError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::Engine;
    use chrono::Utc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use uuid::Uuid;

    use super::*;
    use crate::models::EventSinkKind;

    const LOCALHOST_CERT_DER_B64: &str = "MIIDHzCCAgegAwIBAgIUT7YypqM2YgvdrXLHby8OFyeNEEIwDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYyMzA0MDEyMloXDTI2MDYyNDA0MDEyMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn3A378veyRzeP7MSS/S61EPpE+v9Z+fGlFC4qB8SOUHvO1D6+QZrqcKkUJZb/HKnQyDydMNMBJfjswid5l18ogPVFmfGInGp50T3ceH8i1DAnN1Bj6g6h/QgKe64elkYDukaoHkqLGiQ7Nwsllm8UqwdgFa+B1hYD6uoYAcd/4gv5ClxOx6bkwganvWas+PXyHEEdYW7YBRAyPrJHIInWjck5k5UJPn5Vy551ptGpurvUqf2M7VcmnxjHAldTnc9br+chIvLtyulWg8pBAdFwu+4ZM0jWQpTRhVi5lWB+q7mmI8Da4izV0/K2a1bDnSN6j4rmAzEknok0fMoGXzWjQIDAQABo2kwZzAdBgNVHQ4EFgQUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwHwYDVR0jBBgwFoAUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwDwYDVR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJFxe1GtT9g/PI0Ht912WKwCJc8Oj0U49zUK8TRe9VZHMaJriozeS+4P6I6RhmMR4RV2bPtvjQjzv9ZCHoGoiPUupHd+PUGn8oyezDWoGLuwlPE0dQyn3OAdV1no6q/HI6PFThHTd2o/cLl3nfyIu56sCRLiwrMg6xH3UZ6VJ4qjtxTuyYloMNrb09Uyo7G1Qpw7qfiOB8whyJcjC8Gx1H1JTmF/h/CU2u79yAcVIRA4N6zJLAdtsseUjyTb5CAagmvZ6wZBqB+XNCwXzV09+56zt5fFtopF7mBgQcE21wtlzoKKLUyivc5FzgOHPv3YDJiooYyFXcOOobY1B0k8ih8=";
    const LOCALHOST_KEY_DER_B64: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCfcDfvy97JHN4/sxJL9LrUQ+kT6/1n58aUULioHxI5Qe87UPr5BmupwqRQllv8cqdDIPJ0w0wEl+OzCJ3mXXyiA9UWZ8YicannRPdx4fyLUMCc3UGPqDqH9CAp7rh6WRgO6RqgeSosaJDs3CyWWbxSrB2AVr4HWFgPq6hgBx3/iC/kKXE7HpuTCBqe9Zqz49fIcQR1hbtgFEDI+skcgidaNyTmTlQk+flXLnnWm0am6u9Sp/YztVyafGMcCV1Odz1uv5yEi8u3K6VaDykEB0XC77hkzSNZClNGFWLmVYH6ruaYjwNriLNXT8rZrVsOdI3qPiuYDMSSeiTR8ygZfNaNAgMBAAECggEAAQH66ebA1Y9whamibqggtQiyrd6HAohCnR1CEhpOWCcaXPbuAtJNkUapRSf72gAAND4v3j2ikL1S+P9Yxhc7lBclbMoV+3uxk5+qFYVxzNlzsz1RoLUMs0IkCtEt6L/UyIaLDjLGUCavrIAKuxNKlM0/EOOgCcyljFuUUAIKIwOcOKv7rG/t7GC+wZMTT3oyICgihwsN7D527BTKRlk6zcSCj38B21drfgLAMreGRt8NGcByhzo3BuazRkYyEw8SP9LCEbDQKwWGR2xJtxwnSHcrvYvSklhDAB3EP29URstGUxapRg4re25e3MRVIjVdYtCeGt8Ie71UZgO/lgwYAQKBgQDPL192FKjTUwqfhjICpXYiNbbseXw7dvvNfLOZvuE20zPTkwwEWkpF2dxQX44RfYS625jzj9GHRijKwL6HlV89i+pNw+N2OWLUdWkkeMVqqknSPgJavZ4O3WKpk+cSgVm0VgaxNfvwoNi+TnLQblP6YFoXMG/luY3wYg0CviHzAQKBgQDFAPGIU/G6SYAnD5SJcojUXKzH3ivvciBYuLJt4FGUlfym9fnkQNbGNJAL4c3otPTcR/r0br2JIrxod5/w4c93Q4EKmXEwMdW26npxDR8uO/caSvFGZweikqxIj0Im5UlGV3cuanFb+u0jZWjCjFxMO2sWGRMdwrgQm+GyG7z/jQKBgA+vxIiKM+YcKXe+j1bH9FPOwVTSNefCsHn0cRy46RBfmVLxlT1XILx9LEMhmP4WBNCpA8GdJ/4X/8qqIULeumFMkKbmp/gxjBwN77IFOt1Cm2hBraf1J1x0wp2YRyyNgp82zDbqoXKsmvx9sA+76rvQQ8Hxtucrz2Vd5yJIBwYBAoGAaLd7q8+TKkZvjFPHzNfIy7kHTqZWDE1JzF9A2Q7nzmd7iPQvBJlCkNDX0LkSTqQBlCXey5chwIdqRs1vgwdE1ExZh1zQwaF7zGMO+pDTBixxyNQVNCsH7+6vDVK5AxvVu0I6471IzG+xJaN98AvT8+GRpollk+gxFwMFETuVVvECgYAJ8qBnL/YnusNmORCdItqG6adl+0H4ohikxNurIP8cBRjKGJ6XSC2Qs3BmljiqL9aLluKTcbhOBKlH6iq63vA8KxF7JjVBj2NXClDh6MO6hr/4gWTi7VMpC3CWT80IijoMAth37y+MImdaJhG2kut+XcT14KFakVJM1JCbe0Ygdw==";

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

    fn subscription(url: String) -> EventSubscription {
        let now = Utc::now().naive_utc();
        EventSubscription {
            id: 10,
            namespace_id: 7,
            sink_id: 20,
            name: "subscription".to_string(),
            description: String::new(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            routing: serde_json::json!({ "url": url }),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn sink() -> EventSink {
        let now = Utc::now().naive_utc();
        EventSink {
            id: 20,
            name: "sink".to_string(),
            kind: EventSinkKind::Webhook,
            config: serde_json::json!({
                "headers": {
                    "x-custom": "custom"
                }
            }),
            secret_ref: Some("webhook_test_token".to_string()),
            enabled: true,
            created_at: now,
            updated_at: now,
        }
    }

    async fn spawn_https_server(status_line: &'static str) -> (u16, oneshot::Receiver<String>) {
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

        actix_rt::spawn(async move {
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

    #[actix_rt::test]
    async fn webhook_sink_posts_event_payload_with_idempotency_header_and_secret() {
        unsafe {
            std::env::set_var(
                "HUBUUM_EVENT_SINK_SECRET_WEBHOOK_TEST_TOKEN",
                "expected-token",
            );
        }
        let (port, request_rx) = spawn_https_server("202 Accepted").await;
        let envelope = envelope();
        let subscription = subscription(format!("https://localhost:{port}/events"));
        WebhookSink
            .deliver(&envelope, &subscription, &sink())
            .await
            .unwrap();

        let request = request_rx.await.unwrap();
        assert!(request.starts_with("POST /events HTTP/1.1"));
        assert!(request.contains("authorization: Bearer expected-token"));
        assert!(request.contains("idempotency-key: "));
        assert!(request.contains(&envelope.event_id.to_string()));
        assert!(request.contains("x-hubuum-event-id: "));
        assert!(request.contains("x-custom: custom"));
        assert!(request.contains("\"entity_type\":\"namespace\""));
        assert!(request.contains("\"event_id\""));
    }

    #[actix_rt::test]
    async fn webhook_sink_treats_non_success_status_as_delivery_error() {
        unsafe {
            std::env::set_var(
                "HUBUUM_EVENT_SINK_SECRET_WEBHOOK_TEST_TOKEN",
                "expected-token",
            );
        }
        let (port, request_rx) = spawn_https_server("500 Internal Server Error").await;
        let error = WebhookSink
            .deliver(
                &envelope(),
                &subscription(format!("https://localhost:{port}/events")),
                &sink(),
            )
            .await
            .unwrap_err();

        let _ = request_rx.await.unwrap();
        assert!(error.to_string().contains("HTTP 500 Internal Server Error"));
    }
}
