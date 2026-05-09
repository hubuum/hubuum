pub mod backend;
pub mod context;
#[cfg(feature = "permissions-local")]
pub mod local;
pub mod types;
pub mod visibility;

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
            // Treetop backend lands in Phase 5 (Task 5.3). Until then this
            // arm is reachable only if an operator sets the env var with the
            // treetop feature compiled in; fail fast with a clear message
            // so it's obvious the feature isn't available yet.
            //
            // The unused params are intentional — they document the future
            // signature without dragging in a half-implementation.
            let _ = &cfg.treetop_url;
            let _ = pool;
            Err(ApiError::InternalServerError(
                "treetop backend not yet implemented (Phase 5 / Task 5.3)".to_string(),
            ))
        }

        #[cfg(not(feature = "permissions-treetop"))]
        PermissionBackendKind::Treetop => Err(ApiError::BadRequest(
            "binary built without `permissions-treetop` feature".to_string(),
        )),
    }
}

// Subsequent tasks add: treetop, test_support modules.
