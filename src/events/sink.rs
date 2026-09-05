use futures::FutureExt;
use futures::future::BoxFuture;
use hubuum_event_sink_webhook::WebhookSinkSettings;
#[cfg(any(feature = "amqp", feature = "email", feature = "valkey", test))]
use hubuum_event_sinks_common::SinkDelivery;

use crate::config::{
    DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS, DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
    DEFAULT_REMOTE_CALL_TIMEOUT_MS, get_config,
};
use crate::storage::{StorageEventDeliverySink, StorageEventDeliverySubscription};

pub use hubuum_event_sinks_common::{EventEnvelope, SinkError};

pub trait Sink: Send + Sync {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a StorageEventDeliverySubscription,
        sink: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>>;
}

pub trait SinkResolver: Send + Sync {
    fn resolve(&self, kind: &str) -> Option<&dyn Sink>;
}

#[derive(Debug, Default)]
pub struct NoopSinkResolver;

impl SinkResolver for NoopSinkResolver {
    fn resolve(&self, _kind: &str) -> Option<&dyn Sink> {
        None
    }
}

#[derive(Debug)]
pub struct DefaultSinkResolver {
    #[cfg(feature = "amqp")]
    amqp: hubuum_event_sink_amqp::AmqpSink,
    #[cfg(feature = "email")]
    email: hubuum_event_sink_email::EmailSink,
    #[cfg(feature = "valkey")]
    valkey: hubuum_event_sink_valkey::ValkeySink,
    webhook: hubuum_event_sink_webhook::WebhookSink,
}

impl Default for DefaultSinkResolver {
    fn default() -> Self {
        Self {
            #[cfg(feature = "amqp")]
            amqp: hubuum_event_sink_amqp::AmqpSink::default(),
            #[cfg(feature = "email")]
            email: hubuum_event_sink_email::EmailSink::default(),
            #[cfg(feature = "valkey")]
            valkey: hubuum_event_sink_valkey::ValkeySink::default(),
            webhook: hubuum_event_sink_webhook::WebhookSink::new(webhook_settings()),
        }
    }
}

impl SinkResolver for DefaultSinkResolver {
    fn resolve(&self, kind: &str) -> Option<&dyn Sink> {
        match kind {
            #[cfg(feature = "amqp")]
            "amqp" => Some(&self.amqp),
            #[cfg(feature = "email")]
            "email" => Some(&self.email),
            #[cfg(feature = "valkey")]
            "valkey_stream" => Some(&self.valkey),
            "webhook" => Some(&self.webhook),
            _ => None,
        }
    }
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
async fn sink_secret(
    sink: &StorageEventDeliverySink,
) -> Result<Option<hubuum_secrets::ResolvedSecret>, SinkError> {
    match sink.secret_ref() {
        Some(alias) => crate::secrets::resolve_event_sink_secret(alias)
            .await
            .map(Some)
            .map_err(SinkError::from),
        None => Ok(None),
    }
}

#[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
fn sink_delivery<'a>(
    subscription: &'a StorageEventDeliverySubscription,
    sink: &'a StorageEventDeliverySink,
    secret: Option<&'a hubuum_secrets::SecretValue>,
) -> SinkDelivery<'a> {
    SinkDelivery::new(sink.configuration(), subscription.routing(), secret)
}

fn webhook_settings() -> WebhookSinkSettings {
    let (max_timeout_ms, max_response_bytes, allow_private_targets) = get_config()
        .map(|config| {
            (
                config.remote_call_timeout_ms,
                config.remote_call_max_response_bytes,
                config.remote_call_allow_private_targets,
            )
        })
        .unwrap_or((
            DEFAULT_REMOTE_CALL_TIMEOUT_MS,
            DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
            DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS,
        ));
    WebhookSinkSettings::new(max_timeout_ms, max_response_bytes)
        .expect("remote call limits are validated before event sinks are initialized")
        .allow_private_targets(allow_private_targets)
        .dangerous_accept_invalid_certs(cfg!(test))
        .dangerous_allow_localhost(cfg!(test))
}

impl Sink for hubuum_event_sink_webhook::WebhookSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a StorageEventDeliverySubscription,
        sink: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            crate::models::credential::AuthorizedWebhookDelivery::authorize(
                sink.configuration(),
                subscription.routing(),
                sink.secret_ref(),
                subscription.collection_id(),
            )
            .map_err(|error| SinkError::new(error.to_string()))?
            .deliver(self, envelope)
            .await
        }
        .boxed()
    }
}

#[cfg(feature = "amqp")]
impl Sink for hubuum_event_sink_amqp::AmqpSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a StorageEventDeliverySubscription,
        sink: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            let secret = sink_secret(sink).await?;
            self.deliver(
                envelope,
                sink_delivery(
                    subscription,
                    sink,
                    secret.as_ref().map(|value| value.value()),
                ),
            )
            .await
        }
        .boxed()
    }
}

#[cfg(feature = "email")]
impl Sink for hubuum_event_sink_email::EmailSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a StorageEventDeliverySubscription,
        sink: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            let secret = sink_secret(sink).await?;
            self.deliver(
                envelope,
                sink_delivery(
                    subscription,
                    sink,
                    secret.as_ref().map(|value| value.value()),
                ),
            )
            .await
        }
        .boxed()
    }
}

#[cfg(feature = "valkey")]
impl Sink for hubuum_event_sink_valkey::ValkeySink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a EventEnvelope,
        subscription: &'a StorageEventDeliverySubscription,
        sink: &'a StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            let secret = sink_secret(sink).await?;
            self.deliver(
                envelope,
                sink_delivery(
                    subscription,
                    sink,
                    secret.as_ref().map(|value| value.value()),
                ),
            )
            .await
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::FutureExt;
    use futures::future::join_all;
    use hubuum_event_sinks_common::UriConnectionPool;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn default_resolver_rejects_unknown_sink_kinds() {
        assert!(DefaultSinkResolver::default().resolve("unknown").is_none());
    }

    struct RecordingSink;

    impl Sink for RecordingSink {
        fn deliver<'a>(
            &'a self,
            envelope: &'a EventEnvelope,
            subscription: &'a StorageEventDeliverySubscription,
            sink: &'a StorageEventDeliverySink,
        ) -> BoxFuture<'a, Result<(), SinkError>> {
            async move {
                assert_eq!(envelope.entity_type().as_str(), "collection");
                assert_eq!(subscription.name(), "subscription");
                assert_eq!(sink.name(), "sink");
                Ok(())
            }
            .boxed()
        }
    }

    #[actix_rt::test]
    async fn sink_trait_can_be_mocked_without_worker_storage() {
        let envelope = EventEnvelope::builder()
            .id(crate::events::EventSequence::new(1).unwrap())
            .event_id(Uuid::new_v4())
            .occurred_at(chrono::Utc::now())
            .entity_type(crate::events::EntityType::Collection)
            .entity_id(Some(crate::events::EventEntityId::new(10).unwrap()))
            .entity_name(Some("example".to_string()))
            .collection_id(Some(crate::events::CollectionId::new(10).unwrap()))
            .action(crate::events::Action::Created)
            .actor_kind(crate::events::ActorKind::System)
            .summary("created collection".to_string())
            .metadata(serde_json::json!({}))
            .schema_version(1)
            .try_build()
            .unwrap();
        let subscription = StorageEventDeliverySubscription::try_new(
            hubuum_domain::EventSubscriptionId::new(1).unwrap(),
            hubuum_domain::CollectionId::new(1).unwrap(),
            "subscription",
            serde_json::json!({}),
        )
        .unwrap();
        let sink = StorageEventDeliverySink::try_new(
            hubuum_domain::EventSinkId::new(1).unwrap(),
            "sink",
            "webhook",
            serde_json::json!({}),
            None,
        )
        .unwrap();

        RecordingSink
            .deliver(&envelope, &subscription, &sink)
            .await
            .unwrap();
    }

    #[actix_rt::test]
    async fn uri_connection_pool_debug_omits_credential_bearing_keys() {
        let pool = UriConnectionPool::<String, String>::default();
        pool.get_or_try_insert_with(
            "rediss://user:secret@example.invalid/0".to_string(),
            |_| async { Ok("client with secret state".to_string()) },
        )
        .await
        .unwrap();

        let debug = format!("{pool:?}");
        assert_eq!(debug, "UriConnectionPool { .. }");
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("example.invalid"));
    }

    #[test]
    fn sink_delivery_debug_omits_config_routing_and_secret_reference() {
        let config = serde_json::json!({
            "headers": {"authorization": "Bearer config-secret"}
        });
        let routing = serde_json::json!({
            "url": "https://user:routing-secret@example.invalid/events"
        });
        let secret = hubuum_secrets::SecretValue::new(b"secret-reference".to_vec()).unwrap();
        let delivery = SinkDelivery::new(&config, &routing, Some(&secret));

        let debug = format!("{delivery:?}");
        assert_eq!(debug, "SinkDelivery { .. }");
        assert!(!debug.contains("config-secret"));
        assert!(!debug.contains("routing-secret"));
        assert!(!debug.contains("secret-reference"));
    }

    #[actix_rt::test]
    async fn uri_connection_pool_initializes_each_key_once_under_concurrency() {
        let pool = UriConnectionPool::<String, usize>::default();
        let initializations = Arc::new(AtomicUsize::new(0));

        let results = join_all((0..16).map(|_| {
            let initializations = Arc::clone(&initializations);
            pool.get_or_try_insert_with("shared-uri".to_string(), move |_| async move {
                initializations.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(42)
            })
        }))
        .await;

        assert!(results.into_iter().all(|result| result == Ok(42)));
        assert_eq!(initializations.load(Ordering::SeqCst), 1);
    }

    #[actix_rt::test]
    async fn uri_connection_pool_retries_a_failed_initializer() {
        let pool = UriConnectionPool::<String, usize>::default();
        let initializations = Arc::new(AtomicUsize::new(0));

        let first = pool
            .get_or_try_insert_with("retry-uri".to_string(), {
                let initializations = Arc::clone(&initializations);
                move |_| async move {
                    initializations.fetch_add(1, Ordering::SeqCst);
                    Err(SinkError::new("initialization failed"))
                }
            })
            .await;
        let second = pool
            .get_or_try_insert_with("retry-uri".to_string(), {
                let initializations = Arc::clone(&initializations);
                move |_| async move {
                    initializations.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                }
            })
            .await;
        let cached = pool
            .get_or_try_insert_with("retry-uri".to_string(), |_| async move { Ok(99) })
            .await;

        assert_eq!(first.unwrap_err().to_string(), "initialization failed");
        assert_eq!(second, Ok(42));
        assert_eq!(cached, Ok(42));
        assert_eq!(initializations.load(Ordering::SeqCst), 2);
    }

    #[actix_rt::test]
    async fn uri_connection_pool_evicts_the_least_recently_used_key_at_capacity() {
        let pool = UriConnectionPool::<String, usize>::new(NonZeroUsize::new(2).unwrap());

        pool.get_or_try_insert_with("old".to_string(), |_| async { Ok(1) })
            .await
            .unwrap();
        pool.get_or_try_insert_with("recent".to_string(), |_| async { Ok(2) })
            .await
            .unwrap();
        pool.get_or_try_insert_with("old".to_string(), |_| async { Ok(10) })
            .await
            .unwrap();
        pool.get_or_try_insert_with("new".to_string(), |_| async { Ok(3) })
            .await
            .unwrap();

        let old = pool
            .get_or_try_insert_with("old".to_string(), |_| async { Ok(10) })
            .await;
        let recent = pool
            .get_or_try_insert_with("recent".to_string(), |_| async { Ok(20) })
            .await;

        assert_eq!(old, Ok(1));
        assert_eq!(recent, Ok(20));
    }

    #[actix_rt::test]
    async fn uri_connection_pool_does_not_evict_an_initializer_in_flight() {
        let pool = UriConnectionPool::<String, usize>::new(NonZeroUsize::new(1).unwrap());
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let initializations = Arc::new(AtomicUsize::new(0));

        let first = pool.get_or_try_insert_with("shared".to_string(), {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let initializations = Arc::clone(&initializations);
            move |_| async move {
                initializations.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                Ok(1)
            }
        });
        let overlap = async {
            started.notified().await;
            pool.get_or_try_insert_with("other".to_string(), |_| async { Ok(2) })
                .await
                .unwrap();

            let shared = pool.get_or_try_insert_with("shared".to_string(), {
                let initializations = Arc::clone(&initializations);
                move |_| async move {
                    initializations.fetch_add(1, Ordering::SeqCst);
                    Ok(99)
                }
            });
            tokio::pin!(shared);
            tokio::select! {
                unexpected = &mut shared => {
                    panic!("in-flight initializer was evicted: {unexpected:?}");
                }
                () = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
            release.notify_one();
            shared.await
        };

        let (first, shared) = tokio::join!(first, overlap);

        assert_eq!(first, Ok(1));
        assert_eq!(shared, Ok(1));
        assert_eq!(initializations.load(Ordering::SeqCst), 1);
    }

    #[cfg(any(feature = "amqp", feature = "email", feature = "valkey"))]
    #[test]
    fn tls_scheme_validator_rejects_cleartext_uris() {
        let error = hubuum_event_sinks_common::require_tls_uri_scheme(
            "redis://localhost/0",
            "Valkey",
            &["rediss"],
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid Valkey config: uri must use a TLS scheme (rediss)"
        );
        hubuum_event_sinks_common::require_tls_uri_scheme(
            "rediss://localhost/0",
            "Valkey",
            &["rediss"],
        )
        .unwrap();
    }
}
