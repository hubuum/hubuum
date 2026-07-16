#![cfg(feature = "integration-test-support")]

pub use hubuum::*;

#[path = "support/mod.rs"]
pub mod tests;

#[path = "api/v1/event_deliveries.rs"]
mod api_event_deliveries;
#[path = "api/v1/event_subscriptions.rs"]
mod api_event_subscriptions;
#[path = "api/v1/events.rs"]
mod api_events;
#[path = "api/meta.rs"]
mod api_meta;
#[path = "api/metrics.rs"]
mod api_metrics;
#[path = "api/probes.rs"]
mod api_probes;
#[path = "api/v1/request_and_correlation.rs"]
mod api_request_and_correlation;
#[path = "api/v1/runtime_config.rs"]
mod api_runtime_config;
