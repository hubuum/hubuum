//! Application-facing authorization subject facts.
//!
//! This contract deliberately has no Diesel types or query-construction
//! methods. Persistence is reached through the opaque storage handle and its
//! mandatory authorization capability.

use crate::errors::ApiError;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::{
    Permissions, Principal, PrincipalID, ServiceAccount, ServiceAccountID, TokenScope, User, UserID,
};
use crate::permissions::ResourceRef;
use crate::storage::{
    AuthenticationPrincipal, AuthorizationGroupMembershipQuery, AuthorizationStorage,
    StorageContext, storage_handle,
};

/// Cheap, local access to a subject's principal id (no backend round-trip).
pub trait PrincipalIdAccessor {
    fn principal_id(&self) -> i32;
}

impl PrincipalIdAccessor for User {
    fn principal_id(&self) -> i32 {
        self.id
    }
}

impl PrincipalIdAccessor for Principal {
    fn principal_id(&self) -> i32 {
        self.id
    }
}

impl PrincipalIdAccessor for AuthenticationPrincipal {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}

impl PrincipalIdAccessor for ServiceAccount {
    fn principal_id(&self) -> i32 {
        self.id
    }
}

impl PrincipalIdAccessor for UserID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}

impl PrincipalIdAccessor for PrincipalID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}

impl PrincipalIdAccessor for ServiceAccountID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}

impl<T: PrincipalIdAccessor + ?Sized> PrincipalIdAccessor for &T {
    fn principal_id(&self) -> i32 {
        (**self).principal_id()
    }
}

/// Identity-only authorization subject.
///
/// Token scope remains a separate request boundary and is never embedded in
/// this trait. Group membership is supplied by the selected storage backend.
#[allow(async_fn_in_trait)]
pub trait AuthzSubject: PrincipalIdAccessor {
    async fn admin_groupname(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?.admin_groupname.clone())
    }

    async fn admin_identity_scope(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?
            .admin_identity_scope
            .clone()
            .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string()))
    }

    async fn is_in_group_by_name(
        &self,
        group_name: &str,
        backend: &impl StorageContext,
    ) -> Result<bool, ApiError> {
        let query = AuthorizationGroupMembershipQuery::new(
            self.principal_id(),
            group_name,
            self.admin_identity_scope().await?,
        );
        Ok(storage_handle(backend)
            .authorization_principal_is_group_member(query)
            .await?)
    }

    /// Return whether the principal belongs to the configured admin group.
    ///
    /// This is a membership fact, not a human-IAM decision. Human-only API
    /// boundaries continue to enforce principal kind independently.
    async fn is_admin(&self, backend: &impl StorageContext) -> Result<bool, ApiError> {
        let group_name = self.admin_groupname().await?;
        self.is_in_group_by_name(&group_name, backend).await
    }
}

impl<T: PrincipalIdAccessor + ?Sized> AuthzSubject for T {}

/// Fail-closed token-scope permission pre-filter.
pub fn scope_allows(scopes: Option<&TokenScope>, requested: &[Permissions]) -> bool {
    match scopes {
        None => true,
        Some(scope) => scope.allows_permissions(requested),
    }
}

/// Fail-closed resource-identity pre-filter for a token scope.
pub fn scope_allows_resource(scope: Option<&TokenScope>, resource: &ResourceRef) -> bool {
    scope.is_none_or(|scope| scope.allows_resource(resource))
}

/// Require every resource touched by an operation to be inside the token's
/// resource boundary.
pub fn scope_allows_resources(scope: Option<&TokenScope>, resources: &[ResourceRef]) -> bool {
    resources
        .iter()
        .all(|resource| scope_allows_resource(scope, resource))
}
