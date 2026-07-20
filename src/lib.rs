#![allow(async_fn_in_trait)]

pub mod api;
pub mod auth;
pub mod backups;
pub mod config;
pub mod db;
pub mod errors;
pub mod events;
pub mod exports;
pub mod extractors;
#[doc(hidden)]
pub mod lifecycle;
pub mod logger;
pub mod macros;
pub mod middlewares;
pub mod models;
pub mod observability;
pub mod pagination;
pub mod permissions;
pub mod restores;
pub mod schema;
pub mod tasks;
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod test_support;
#[cfg(any(test, feature = "integration-test-support"))]
#[doc(hidden)]
pub mod tests;
pub mod tls;
pub mod traits;
pub mod utilities;
