pub mod backend;
pub mod context;
#[cfg(feature = "permissions-local")]
pub mod export;
#[cfg(feature = "permissions-local")]
pub mod local;
pub mod observability;
#[cfg(feature = "permissions-treetop")]
pub mod treetop;
pub mod types;
pub mod visibility;

// Test support module provides mock backends for testing trait-level
// semantics without depending on SQL or external APIs. Always compiled
// because src/tests/ is a library submodule (not a separate integration
// test binary), but documented as test-only infrastructure — production
// code should never use MockTreetopBackend.
#[doc(hidden)]
pub mod test_support;

pub use backend::PermissionBackend;
pub use context::AppContext;
#[cfg(feature = "permissions-local")]
pub use local::LocalPermissionBackend;
pub use types::{
    AuthorizationResult, AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef,
    ResourceAttrs, ResourceKind, ResourceRef,
};

use std::sync::Arc;

use crate::config::{AppConfig, PermissionBackendKind};
use crate::db::DbPool;
use crate::errors::ApiError;

/// Construct the permission backend selected by the active config.
/// Called once at startup (from `main.rs` in Task 2.5) and once per test
/// fixture that needs an `AppContext`.
pub async fn build_permission_backend(
    cfg: &AppConfig,
    pool: DbPool,
) -> Result<Arc<dyn PermissionBackend>, ApiError> {
    match cfg.permission_backend {
        #[cfg(feature = "permissions-local")]
        PermissionBackendKind::Local => Ok(Arc::new(local::LocalPermissionBackend::new(
            pool,
            cfg.admin_groupname.clone(),
        ))),

        #[cfg(not(feature = "permissions-local"))]
        PermissionBackendKind::Local => Err(ApiError::BadRequest(
            "binary built without `permissions-local` feature".to_string(),
        )),

        #[cfg(feature = "permissions-treetop")]
        PermissionBackendKind::Treetop => {
            let url = cfg.treetop_url.as_deref().ok_or_else(|| {
                ApiError::BadRequest("HUBUUM_TREETOP_URL is required".to_string())
            })?;
            let backend = treetop::TreetopPermissionBackend::connect(url, cfg, pool).await?;
            Ok(Arc::new(backend))
        }

        #[cfg(not(feature = "permissions-treetop"))]
        PermissionBackendKind::Treetop => Err(ApiError::BadRequest(
            "binary built without `permissions-treetop` feature".to_string(),
        )),
    }
}

// Subsequent tasks add: treetop, test_support modules.
