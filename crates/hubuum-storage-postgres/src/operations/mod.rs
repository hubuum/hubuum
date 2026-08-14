//! PostgreSQL implementations of backend-neutral storage operations.

pub mod authentication;
pub mod backup;
pub mod bootstrap;
pub mod event_delivery;
pub mod event_fanout;
pub mod event_observability;
pub mod event_retention;
pub mod identity_credentials;
pub mod identity_scope;
pub mod inventory;
pub mod maintenance;
pub mod meta;
pub mod metrics;
pub mod probe;
