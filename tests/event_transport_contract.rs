#![cfg(all(feature = "amqp", feature = "valkey"))]

use std::collections::BTreeMap;
use std::future::Future;
use std::process::Command;
use std::time::Duration;

use chrono::Utc;
use hubuum::tls::install_default_crypto_provider;
use hubuum_event_sink_amqp::AmqpSink;
use hubuum_event_sink_valkey::ValkeySink;
use hubuum_event_sink_webhook::{WebhookSink, WebhookSinkSettings};
use hubuum_event_sinks_common::{SecretValue, SinkDelivery};
use hubuum_events_core::{Action, ActorKind, EntityType, EventEnvelope, EventSequence, Provenance};
use hubuum_outbound_http::{OutboundMethod, OutboundRequest};
use reqwest::{Client, Method, RequestBuilder};
use rstest::rstest;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

fn fixture(name: &str) -> String {
    // Match the production application's process-wide TLS initialization.
    install_default_crypto_provider().unwrap();
    std::env::var(format!("HUBUUM_CONTRACT_{name}"))
        .expect("run with python3 scripts/test-event-transports.py; fixtures are mandatory")
}

fn secret() -> SecretValue {
    SecretValue::new(fixture("PASSWORD").into_bytes()).unwrap()
}

async fn bounded<T>(operation: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(15), operation)
        .await
        .expect("transport operation exceeded the fixture deadline")
}

fn envelope() -> EventEnvelope {
    EventEnvelope::builder()
        .id(EventSequence::new(1).unwrap())
        .event_id(Uuid::new_v4())
        .occurred_at(Utc::now())
        .entity_type(EntityType::Collection)
        .action(Action::Created)
        .actor_kind(ActorKind::System)
        .provenance(Provenance::default())
        .summary("transport contract".to_string())
        .metadata(json!({"source": "isolated-fixture"}))
        .schema_version(1)
        .try_build()
        .unwrap()
}

fn http() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn broker_request(method: Method, path: &str) -> RequestBuilder {
    http()
        .request(method, format!("{}/api/{path}", fixture("AMQP_MANAGEMENT")))
        .basic_auth("contract", Some(fixture("PASSWORD")))
}

async fn broker_put(path: &str, body: Value) {
    broker_request(Method::PUT, path)
        .json(&body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

fn amqp_config(exchange: &str) -> Value {
    json!({"uri": fixture("AMQP_URI"), "exchange": exchange, "mandatory": true})
}

#[tokio::test]
#[ignore = "requires the pinned RabbitMQ fixture"]
async fn amqp_acknowledges_a_routed_event_with_its_identity_and_payload() {
    let event = envelope();
    let exchange = format!("contract-{}", event.event_id());
    broker_put(
        &format!("exchanges/%2F/{exchange}"),
        json!({"type":"topic", "durable":true}),
    )
    .await;
    broker_put(&format!("queues/%2F/{exchange}"), json!({"durable":true})).await;
    broker_request(
        Method::POST,
        &format!("bindings/%2F/e/{exchange}/q/{exchange}"),
    )
    .json(&json!({"routing_key":"collection.created"}))
    .send()
    .await
    .unwrap()
    .error_for_status()
    .unwrap();
    bounded(AmqpSink::default().deliver(
        &event,
        SinkDelivery::new(&amqp_config(&exchange), &json!({}), Some(&secret())),
    ))
    .await
    .unwrap();
    let messages: Value = broker_request(Method::POST, &format!("queues/%2F/{exchange}/get"))
        .json(&json!({"count":1, "ackmode":"ack_requeue_false", "encoding":"auto"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(messages.as_array().unwrap().len(), 1);
    assert_eq!(
        messages[0]["properties"]["message_id"],
        event.event_id().to_string()
    );
    let payload: Value = serde_json::from_str(messages[0]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload, serde_json::to_value(&event).unwrap());
}

#[tokio::test]
#[ignore = "requires the pinned RabbitMQ fixture"]
async fn amqp_rejects_a_confirmed_but_unroutable_mandatory_message() {
    let config = amqp_config(&format!("unroutable-{}", Uuid::new_v4()));
    let error = bounded(AmqpSink::default().deliver(
        &envelope(),
        SinkDelivery::new(&config, &json!({}), Some(&secret())),
    ))
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "AMQP publish was returned by the broker without a matching route"
    );
}

async fn restart_fixture(name: &str) {
    let container = fixture(&format!("{name}_CONTAINER"));
    assert!(container.starts_with("hubuum-transport-contract-"));
    let output = tokio::task::spawn_blocking(move || {
        Command::new("docker")
            .args(["restart", &container])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "fixture restart failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires the pinned RabbitMQ fixture"]
async fn amqp_cached_sink_recovers_on_retry_after_broker_restart() {
    let sink = AmqpSink::default();
    let mut config = amqp_config(&format!("reconnect-{}", Uuid::new_v4()));
    config["mandatory"] = json!(false);
    let routing = json!({});
    let secret = secret();
    let event = envelope();
    bounded(sink.deliver(&event, SinkDelivery::new(&config, &routing, Some(&secret))))
        .await
        .unwrap();
    restart_fixture("AMQP").await;
    timeout(Duration::from_secs(60), async {
        loop {
            if sink
                .deliver(&event, SinkDelivery::new(&config, &routing, Some(&secret)))
                .await
                .is_ok()
            {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("cached AMQP sink must recover after broker restart");
}

fn valkey_config() -> Value {
    json!({"uri": fixture("VALKEY_URI"), "io_timeout_ms": 2000})
}

async fn stream_entries(stream: &str) -> Vec<(String, BTreeMap<String, String>)> {
    let mut connection = redis::Client::open(fixture("VALKEY_INSPECT_URI"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(&mut connection)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the pinned Valkey fixture"]
async fn valkey_persists_the_event_identity_and_payload_over_verified_tls() {
    let event = envelope();
    let stream = format!("contract:{}", event.event_id());
    ValkeySink::default()
        .deliver(
            &event,
            SinkDelivery::new(&valkey_config(), &json!({"stream":stream}), Some(&secret())),
        )
        .await
        .unwrap();
    let entries = stream_entries(&stream).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1["event_id"], event.event_id().to_string());
    let payload: Value = serde_json::from_str(&entries[0].1["payload"]).unwrap();
    assert_eq!(payload, serde_json::to_value(&event).unwrap());
}

#[tokio::test]
#[ignore = "requires the pinned Valkey fixture"]
async fn valkey_exact_trimming_retains_only_the_requested_tail() {
    let sink = ValkeySink::default();
    let stream = format!("trim:{}", Uuid::new_v4());
    let routing = json!({"stream": stream});
    let mut config = valkey_config();
    config["max_len"] = json!(2);
    config["approximate_trim"] = json!(false);
    let secret = secret();
    let mut expected = Vec::new();
    for _ in 0..3 {
        let event = envelope();
        expected.push(event.event_id().to_string());
        sink.deliver(&event, SinkDelivery::new(&config, &routing, Some(&secret)))
            .await
            .unwrap();
    }
    let actual = stream_entries(&stream)
        .await
        .into_iter()
        .map(|(_, fields)| fields["event_id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected[1..]);
}

#[tokio::test]
#[ignore = "requires the pinned Valkey fixture"]
async fn valkey_cached_sink_recovers_after_service_restart() {
    let sink = ValkeySink::default();
    let config = valkey_config();
    let routing = json!({"stream": format!("reconnect:{}", Uuid::new_v4())});
    let secret = secret();
    sink.deliver(
        &envelope(),
        SinkDelivery::new(&config, &routing, Some(&secret)),
    )
    .await
    .unwrap();
    restart_fixture("VALKEY").await;
    timeout(Duration::from_secs(30), async {
        loop {
            if sink
                .deliver(
                    &envelope(),
                    SinkDelivery::new(&config, &routing, Some(&secret)),
                )
                .await
                .is_ok()
            {
                break;
            }
            sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("cached Valkey sink must reconnect after service restart");
}

#[rstest]
#[case::success("HTTPS_URL", "ok", true)]
#[case::redirect("HTTPS_URL", "redirect", false)]
#[case::server_error("HTTPS_URL", "retry", false)]
#[case::capped_response("HTTPS_URL", "oversized", true)]
#[case::timeout("HTTPS_URL", "slow", false)]
#[case::untrusted_certificate("UNTRUSTED_HTTPS_URL", "ok", false)]
#[tokio::test]
#[ignore = "requires the private-CA HTTPS fixture"]
async fn webhook_enforces_the_https_delivery_contract(
    #[case] endpoint: &str,
    #[case] behavior: &str,
    #[case] succeeds: bool,
) {
    let event = envelope();
    let settings = WebhookSinkSettings::new(500, 1024)
        .unwrap()
        .allow_private_targets(true)
        .dangerous_allow_localhost(true);
    let routing = json!({"url":format!("{}/{behavior}/{}", fixture(endpoint), event.event_id())});
    let result = WebhookSink::new(settings)
        .deliver(&event, SinkDelivery::new(&json!({}), &routing, None))
        .await;
    assert_eq!(
        result.is_ok(),
        succeeds,
        "unexpected delivery outcome: {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires the private-CA HTTPS fixture"]
async fn webhook_redirects_do_not_contact_the_redirect_destination() {
    let event = envelope();
    let base = fixture("HTTPS_URL");
    let settings = WebhookSinkSettings::new(2000, 1024)
        .unwrap()
        .allow_private_targets(true)
        .dangerous_allow_localhost(true);
    let routing = json!({"url":format!("{base}/redirect/{}", event.event_id())});
    WebhookSink::new(settings)
        .deliver(&event, SinkDelivery::new(&json!({}), &routing, None))
        .await
        .unwrap_err();
    let observed: Value = http()
        .get(format!("{base}/observed/{}", event.event_id()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(observed, json!(["redirect"]));
}

#[tokio::test]
#[ignore = "requires the private-CA HTTPS fixture"]
async fn outbound_https_retains_only_the_bounded_response_preview() {
    let response = OutboundRequest::new(
        OutboundMethod::Post,
        format!("{}/oversized/{}", fixture("HTTPS_URL"), Uuid::new_v4()),
        Duration::from_secs(2),
    )
    .max_response_bytes(1024)
    .allow_private_targets(true)
    .dangerous_allow_localhost(true)
    .send()
    .await
    .unwrap();
    assert_eq!(response.body_preview(), "x".repeat(1024));
}
