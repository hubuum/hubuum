pub mod backend;
pub mod context;
#[cfg(feature = "permissions-local")]
pub mod local;
pub mod types;

pub use backend::PermissionBackend;
pub use context::AppContext;
pub use types::{
    AuthorizationResult, AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef,
    ResourceAttrs, ResourceKind, ResourceRef,
};

// Subsequent tasks add: treetop, test_support modules.
