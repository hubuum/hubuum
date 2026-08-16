//! Internal application implementation for Hubuum's binaries.
//!
//! This crate is not a supported third-party embedding interface. Hubuum's
//! supported application contract is the versioned HTTP API described by the
//! generated OpenAPI document. Rust API consumers should use the separately
//! maintained HTTP client. See `docs/rust_api_boundary.md` for the workspace
//! crate classification and promotion policy.

#![allow(async_fn_in_trait)]

mod administration;
mod application;

pub use administration::run_admin_from_environment;
pub use application::run_runtime_from_environment;

pub mod api;
pub mod auth;
pub mod backups;
#[doc(hidden)]
pub mod benchmark_support;
pub mod config;
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
#[doc(hidden)]
pub use hubuum_storage_postgres::schema;
pub mod services;
#[doc(hidden)]
pub mod storage;
pub mod tasks;
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod test_support;
#[cfg(any(test, feature = "integration-test-support"))]
#[doc(hidden)]
pub mod tests;
pub mod tls;
pub mod token_retention;
pub mod traits;
pub mod utilities;

/// Generate the canonical pretty-printed OpenAPI document.
///
/// This is the workspace-internal entrypoint used by `hubuum-openapi`.
#[must_use]
pub fn generate_openapi_json() -> String {
    use utoipa::OpenApi;

    let openapi = api::openapi::ApiDoc::openapi();
    serde_json::to_string_pretty(&openapi).expect("failed to serialize OpenAPI document")
}
