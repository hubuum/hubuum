use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Weak};

use hubuum_secrets::SecretError;
pub use hubuum_secrets::SecretValue;
use percent_encoding::{NON_ALPHANUMERIC, percent_encode};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, OnceCell};

pub use hubuum_events_core::EventEnvelope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkError {
    message: String,
}

impl SinkError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SinkError {}

impl From<SecretError> for SinkError {
    fn from(error: SecretError) -> Self {
        Self::new(error.to_string())
    }
}

pub const DEFAULT_MAX_ENVELOPE_BYTES: usize = 1_000_000;
pub const DEFAULT_URI_CONNECTION_POOL_CAPACITY: usize = 64;

pub fn serialize_envelope_to_string(
    envelope: &EventEnvelope,
    sink_label: &str,
    max_bytes: usize,
) -> Result<String, SinkError> {
    let payload = serde_json::to_string(envelope).map_err(|error| {
        SinkError::new(format!("Failed to serialize {sink_label} payload: {error}"))
    })?;
    ensure_payload_within_limit(sink_label, payload.len(), max_bytes)?;
    Ok(payload)
}

pub fn serialize_envelope_to_vec(
    envelope: &EventEnvelope,
    sink_label: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, SinkError> {
    let payload = serde_json::to_vec(envelope).map_err(|error| {
        SinkError::new(format!("Failed to serialize {sink_label} payload: {error}"))
    })?;
    ensure_payload_within_limit(sink_label, payload.len(), max_bytes)?;
    Ok(payload)
}

pub fn ensure_payload_within_limit(
    sink_label: &str,
    payload_bytes: usize,
    max_bytes: usize,
) -> Result<(), SinkError> {
    if payload_bytes > max_bytes {
        return Err(SinkError::new(format!(
            "{sink_label} payload is {payload_bytes} bytes, exceeding the configured limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub struct SinkDelivery<'a> {
    config: &'a Value,
    routing: &'a Value,
    secret: Option<&'a SecretValue>,
}

impl fmt::Debug for SinkDelivery<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkDelivery").finish_non_exhaustive()
    }
}

impl<'a> SinkDelivery<'a> {
    pub fn new(config: &'a Value, routing: &'a Value, secret: Option<&'a SecretValue>) -> Self {
        Self {
            config,
            routing,
            secret,
        }
    }

    pub fn config(&self) -> &'a Value {
        self.config
    }

    pub fn routing(&self) -> &'a Value {
        self.routing
    }

    pub fn secret(&self) -> Option<&'a SecretValue> {
        self.secret
    }
}

pub fn resolve_secret_uri(
    uri: &str,
    secret: Option<&SecretValue>,
    sink_label: &str,
) -> Result<String, SinkError> {
    let contains_secret_placeholder = uri.contains("{secret}");
    match secret {
        Some(_) if !contains_secret_placeholder => Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri must include {{secret}} when secret_ref is set"
        ))),
        Some(secret) => {
            let encoded = percent_encode(secret.expose(), NON_ALPHANUMERIC).to_string();
            Ok(uri.replace("{secret}", &encoded))
        }
        None if contains_secret_placeholder => Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri includes {{secret}} without secret_ref"
        ))),
        None => Ok(uri.to_string()),
    }
}

pub fn parse_sink_config<T: DeserializeOwned>(
    delivery: &SinkDelivery<'_>,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(delivery.config().clone())
        .map_err(|error| SinkError::new(format!("Invalid {sink_label} config: {error}")))
}

pub fn parse_sink_routing<T: DeserializeOwned>(
    delivery: &SinkDelivery<'_>,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(delivery.routing().clone())
        .map_err(|error| SinkError::new(format!("Invalid {sink_label} routing: {error}")))
}

pub fn require_non_empty(value: &str, label: &str, field: &str) -> Result<(), SinkError> {
    if value.trim().is_empty() {
        return Err(SinkError::new(format!(
            "Invalid {label}: {field} is required"
        )));
    }
    Ok(())
}

pub fn require_tls_uri_scheme(
    uri: &str,
    sink_label: &str,
    tls_schemes: &[&str],
) -> Result<(), SinkError> {
    let Some((scheme, _)) = uri.split_once(':') else {
        return Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri must include a scheme"
        )));
    };
    if !tls_schemes
        .iter()
        .any(|allowed| scheme.eq_ignore_ascii_case(allowed))
    {
        return Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri must use a TLS scheme ({})",
            tls_schemes.join(", ")
        )));
    }
    Ok(())
}

pub fn reject_literal_uri_credentials(uri: &str, sink_label: &str) -> Result<(), SinkError> {
    if let Some(userinfo) = uri_userinfo(uri)
        && !userinfo.contains("{secret}")
    {
        return Err(SinkError::new(format!(
            "Invalid {sink_label} config: uri credentials must use {{secret}} with secret_ref"
        )));
    }
    Ok(())
}

fn uri_userinfo(uri: &str) -> Option<&str> {
    let (_, rest) = uri.split_once("://")?;
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let (userinfo, host) = authority.rsplit_once('@')?;
    (!userinfo.is_empty() && !host.is_empty()).then_some(userinfo)
}

pub struct UriConnectionPool<K, V> {
    capacity: NonZeroUsize,
    state: Mutex<UriConnectionPoolState<K, V>>,
}

#[derive(Debug)]
struct UriConnectionPoolState<K, V> {
    entries: HashMap<K, Arc<OnceCell<V>>>,
    recency: VecDeque<K>,
}

impl<K, V> fmt::Debug for UriConnectionPool<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UriConnectionPool").finish_non_exhaustive()
    }
}

impl<K, V> Default for UriConnectionPool<K, V> {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(DEFAULT_URI_CONNECTION_POOL_CAPACITY)
                .expect("the default URI connection pool capacity is non-zero"),
        )
    }
}

impl<K, V> UriConnectionPool<K, V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            state: Mutex::new(UriConnectionPoolState {
                entries: HashMap::new(),
                recency: VecDeque::new(),
            }),
        }
    }
}

impl<K, V> UriConnectionPool<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub async fn get_or_try_insert_with<F, Fut>(&self, key: K, create: F) -> Result<V, SinkError>
    where
        F: FnOnce(K) -> Fut,
        Fut: Future<Output = Result<V, SinkError>>,
    {
        let entry = {
            let mut state = self.state.lock().await;
            if let Some(entry) = state.entries.get(&key).cloned() {
                state.touch(&key);
                entry
            } else {
                state.evict_idle_to(self.capacity.get() - 1);
                let entry = Arc::new(OnceCell::new());
                state.entries.insert(key.clone(), Arc::clone(&entry));
                state.recency.push_back(key.clone());
                entry
            }
        };

        let entry_identity = Arc::downgrade(&entry);
        let result = entry.get_or_try_init(|| create(key.clone())).await.cloned();
        drop(entry);

        let mut state = self.state.lock().await;
        if result.is_err() {
            state.remove_failed(&key, &entry_identity);
        }
        state.evict_idle_to(self.capacity.get());
        result
    }

    pub async fn remove(&self, key: &K) {
        self.state.lock().await.remove(key);
    }
}

impl<K, V> UriConnectionPoolState<K, V>
where
    K: Eq + Hash + Clone,
{
    fn touch(&mut self, key: &K) {
        self.remove_recency(key);
        self.recency.push_back(key.clone());
    }

    fn evict_idle_to(&mut self, maximum_entries: usize) {
        let mut candidates = self.recency.len();
        while self.entries.len() > maximum_entries && candidates > 0 {
            candidates -= 1;
            let Some(key) = self.recency.pop_front() else {
                break;
            };
            let is_idle = self
                .entries
                .get(&key)
                .is_some_and(|entry| Arc::strong_count(entry) == 1);
            if is_idle {
                self.entries.remove(&key);
            } else {
                self.recency.push_back(key);
            }
        }
    }

    fn remove_failed(&mut self, key: &K, expected: &Weak<OnceCell<V>>) {
        let remove_failed_entry = self.entries.get(key).is_some_and(|current| {
            Weak::ptr_eq(&Arc::downgrade(current), expected)
                && current.get().is_none()
                && Arc::strong_count(current) == 1
        });
        if remove_failed_entry {
            self.remove(key);
        }
    }

    fn remove(&mut self, key: &K) {
        self.entries.remove(key);
        self.remove_recency(key);
    }

    fn remove_recency(&mut self, key: &K) {
        if let Some(position) = self.recency.iter().position(|candidate| candidate == key) {
            self.recency.remove(position);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use super::{SinkError, UriConnectionPool};

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = std::pin::pin!(future);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn failed_initializer_is_removed_from_the_pool() {
        let pool = UriConnectionPool::<String, usize>::default();

        let result = block_on(
            pool.get_or_try_insert_with("failed-uri".to_string(), |_| async move {
                Err(SinkError::new("initialization failed"))
            }),
        );

        assert_eq!(result.unwrap_err().to_string(), "initialization failed");
        assert!(block_on(pool.state.lock()).entries.is_empty());
    }
}
