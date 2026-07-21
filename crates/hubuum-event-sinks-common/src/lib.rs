use std::fmt;
use std::future::Future;
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::Mutex;

pub use hubuum_events_core::{
    EventEnvelope, EventSinkSecretError, resolve_event_sink_secret, resolve_event_sink_secret_uri,
};

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

impl From<EventSinkSecretError> for SinkError {
    fn from(error: EventSinkSecretError) -> Self {
        Self::new(error.to_string())
    }
}

pub const DEFAULT_MAX_ENVELOPE_BYTES: usize = 1_000_000;
/// Maximum number of resolved sink URIs retained by each transport instance.
pub const MAX_CACHED_SINK_URIS: usize = 128;

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

#[derive(Debug, Clone, Copy)]
pub struct SinkDelivery<'a> {
    pub config: &'a Value,
    pub routing: &'a Value,
    pub secret_ref: Option<&'a str>,
}

impl<'a> SinkDelivery<'a> {
    pub fn new(config: &'a Value, routing: &'a Value, secret_ref: Option<&'a str>) -> Self {
        Self {
            config,
            routing,
            secret_ref,
        }
    }
}

pub fn parse_sink_config<T: DeserializeOwned>(
    delivery: &SinkDelivery<'_>,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(delivery.config.clone())
        .map_err(|error| SinkError::new(format!("Invalid {sink_label} config: {error}")))
}

pub fn parse_sink_routing<T: DeserializeOwned>(
    delivery: &SinkDelivery<'_>,
    sink_label: &str,
) -> Result<T, SinkError> {
    serde_json::from_value(delivery.routing.clone())
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

/// A bounded client/connection cache keyed by a resolved transport URI.
#[derive(Debug)]
pub struct UriConnectionPool<K, V> {
    state: Mutex<UriConnectionPoolState<K, V>>,
}

impl<K, V> Default for UriConnectionPool<K, V> {
    fn default() -> Self {
        Self {
            state: Mutex::new(UriConnectionPoolState::default()),
        }
    }
}

#[derive(Debug)]
struct CachedUriConnection<V> {
    value: V,
    last_used: u64,
}

#[derive(Debug)]
struct UriConnectionPoolState<K, V> {
    entries: std::collections::HashMap<K, CachedUriConnection<V>>,
    clock: u64,
}

impl<K, V> Default for UriConnectionPoolState<K, V> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            clock: 0,
        }
    }
}

impl<K, V> UriConnectionPoolState<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn get(&mut self, key: &K) -> Option<V> {
        let last_used = self.next_usage();
        self.entries.get_mut(key).map(|entry| {
            entry.last_used = last_used;
            entry.value.clone()
        })
    }

    fn insert_or_get(&mut self, key: K, created: V, capacity: usize) -> V {
        let last_used = self.next_usage();
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.last_used = last_used;
            return entry.value.clone();
        }

        if self.entries.len() >= capacity
            && let Some(stalest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
        {
            self.entries.remove(&stalest);
        }
        self.entries.insert(
            key,
            CachedUriConnection {
                value: created.clone(),
                last_used,
            },
        );
        created
    }

    fn next_usage(&mut self) -> u64 {
        let current = self.clock;
        self.clock = self.clock.saturating_add(1);
        current
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
        {
            let mut state = self.state.lock().await;
            if let Some(value) = state.get(&key) {
                return Ok(value);
            }
        }

        let created = create(key.clone()).await?;

        let mut state = self.state.lock().await;
        Ok(state.insert_or_get(key, created, MAX_CACHED_SINK_URIS))
    }

    pub async fn remove(&self, key: &K) {
        self.state.lock().await.entries.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_connection_pool_state_evicts_the_least_recently_used_entry() {
        let mut state = UriConnectionPoolState::default();
        state.insert_or_get("first", 1, 2);
        state.insert_or_get("second", 2, 2);
        assert_eq!(state.get(&"first"), Some(1));

        state.insert_or_get("third", 3, 2);

        assert_eq!(state.get(&"second"), None);
        assert_eq!(state.get(&"first"), Some(1));
        assert_eq!(state.get(&"third"), Some(3));
    }

    #[test]
    fn uri_connection_pool_state_keeps_the_winner_of_a_racing_insert() {
        let mut state = UriConnectionPoolState::default();
        state.insert_or_get("same", 1, 2);

        let selected = state.insert_or_get("same", 2, 2);

        assert_eq!(selected, 1);
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn uri_connection_pool_state_never_exceeds_the_production_capacity() {
        let mut state = UriConnectionPoolState::default();
        for key in 0..=MAX_CACHED_SINK_URIS {
            state.insert_or_get(key, key, MAX_CACHED_SINK_URIS);
        }

        assert_eq!(state.entries.len(), MAX_CACHED_SINK_URIS);
        assert_eq!(state.get(&0), None);
    }
}
