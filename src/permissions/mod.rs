pub mod backend;
pub mod context;
pub mod export;
pub mod local;
pub mod observability;
mod storage;
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

pub use backend::{
    CompleteCollectionCandidateLimit, MAX_COMPLETE_COLLECTION_CANDIDATES, PermissionBackend,
};
pub use context::{AppContext, AuthorizationContext, AuthorizationMode};
pub use local::LocalPermissionBackend;
pub(crate) use storage::{
    collection_from_storage as authorization_collection_from_storage,
    effective_group_grant_from_storage as authorization_effective_group_grant_from_storage,
    grant_from_storage, group_from_storage as authorization_group_from_storage,
    group_grant_from_storage as authorization_group_grant_from_storage, permission_from_storage,
    permission_to_storage,
};
pub use types::{
    AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind,
    ResourceRef,
};

use std::sync::Arc;

use crate::config::{AppConfig, PermissionBackendKind};
use crate::errors::ApiError;
use crate::models::{Permissions, TokenScope};
use crate::storage::StorageHandle;
use crate::traits::{AuthzSubject, PrincipalIdAccessor, scope_allows, scope_allows_resources};

pub async fn authorize_resources<S>(
    backend: &dyn PermissionBackend,
    pool: &impl crate::storage::StorageContext,
    subject: S,
    scopes: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    resources: Vec<ResourceRef>,
) -> Result<(), ApiError>
where
    S: PrincipalIdAccessor,
{
    if !scope_allows(scopes, &permissions) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    if !scope_allows_resources(scopes, &resources) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    let principal = PrincipalRef::load(pool, &subject).await?;
    let requests = resources
        .iter()
        .flat_map(|resource| {
            permissions.iter().map(|permission| PermissionRequest {
                resource: resource.normalized_for_permission(*permission),
                permissions: vec![*permission],
            })
        })
        .collect();
    let decisions = backend.authorize_many(&principal, requests).await?;
    if decisions
        .iter()
        .all(|decision| *decision == PermissionDecision::Allow)
    {
        Ok(())
    } else {
        Err(ApiError::Forbidden("Permission denied".to_string()))
    }
}

/// Require runtime-wide administrator authority for a queued full-system task.
///
/// Unlike [`crate::extractors::AdminAccess`], this accepts both human principals
/// and service accounts. Tokens must still be unscoped because the operation
/// cannot be represented by a collection permission subset.
pub async fn require_unscoped_runtime_admin<C, S>(
    context: &C,
    subject: &S,
    token_scoped: bool,
) -> Result<(), ApiError>
where
    C: AuthorizationContext,
    S: AuthzSubject + ?Sized,
{
    if token_scoped {
        return Err(ApiError::Forbidden(
            "This operation requires an unscoped runtime administrator".to_string(),
        ));
    }
    let pool = context;
    let is_admin = match context.authorization_mode() {
        AuthorizationMode::Delegated(backend) => {
            let principal = PrincipalRef::load(pool, subject).await?;
            backend.is_admin(&principal).await?
        }
        AuthorizationMode::LocalStorage => subject.is_admin(pool).await?,
    };

    if is_admin {
        Ok(())
    } else {
        Err(ApiError::Forbidden(
            "This operation requires an unscoped runtime administrator".to_string(),
        ))
    }
}

/// Construct the permission backend selected by the active config.
/// Called once at startup (from `main.rs` in Task 2.5) and once per test
/// fixture that needs an `AppContext`.
pub(crate) async fn build_permission_backend(
    cfg: &AppConfig,
    storage: StorageHandle,
) -> Result<Arc<dyn PermissionBackend>, ApiError> {
    match cfg.permission_backend {
        PermissionBackendKind::Local => Ok(Arc::new(LocalPermissionBackend::new(
            storage,
            cfg.admin_groupname.clone(),
        ))),

        #[cfg(feature = "permissions-treetop")]
        PermissionBackendKind::Treetop => {
            let url = cfg.treetop_url.as_deref().ok_or_else(|| {
                ApiError::BadRequest("HUBUUM_TREETOP_URL is required".to_string())
            })?;
            let backend = treetop::TreetopPermissionBackend::connect(url, cfg, storage).await?;
            Ok(Arc::new(backend))
        }

        #[cfg(not(feature = "permissions-treetop"))]
        PermissionBackendKind::Treetop => Err(ApiError::BadRequest(
            "binary built without `permissions-treetop` feature".to_string(),
        )),
    }
}

// Subsequent tasks add: treetop, test_support modules.
