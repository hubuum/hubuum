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
use crate::services::storage_boundary::{collection_id_to_storage, principal_id_to_storage};
use crate::storage::{
    AuthorizationDataStorage, StorageAuthenticationPrincipal,
    StorageAuthorizationCollectionsAccessQuery, StorageAuthorizationGroupMembershipQuery,
    StorageContext, storage_handle,
};

use super::CollectionAccessors;

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

impl PrincipalIdAccessor for StorageAuthenticationPrincipal {
    fn principal_id(&self) -> i32 {
        self.id().id()
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
        let query = StorageAuthorizationGroupMembershipQuery::new(
            principal_id_to_storage(self.principal_id()),
            group_name,
            self.admin_identity_scope().await?,
        );
        Ok(storage_handle(backend)
            .is_authorization_principal_group_member(query)
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

/// Application authorization behavior shared by every identity subject.
///
/// Scope and administrator policy stay in the application layer. The selected
/// storage backend supplies the mandatory batch permission lookup without
/// exposing its query language or persistence model.
pub trait UserPermissions: AuthzSubject {
    /// Require every requested permission on every resolved collection.
    async fn can<C, P, N, I>(
        &self,
        backend: &C,
        permissions: P,
        collections: I,
        scopes: Option<&TokenScope>,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: CollectionAccessors,
    {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let requested = permissions.into_iter().collect::<Vec<_>>();
        let principal_id = self.principal_id();

        if !scope_allows(scopes, &requested) {
            crate::logger::log_authorization_denial(
                principal_id,
                &requested,
                None,
                None,
                "token_scope",
            );
            return Err(ApiError::Forbidden(
                "Token scope does not permit the requested action".to_string(),
            ));
        }

        if self.is_admin(backend).await? {
            crate::logger::log_authorization_grant(principal_id, &requested, None, None, "admin");
            return Ok(());
        }

        let mut collection_ids = stream::iter(collections)
            .map(|collection| async move {
                collection
                    .collection_id(backend)
                    .await
                    .map(|collection_id| collection_id.id())
            })
            .buffered(5)
            .try_collect::<Vec<_>>()
            .await?;
        collection_ids.sort_unstable();
        collection_ids.dedup();

        let collection_count = collection_ids.len();
        let collection_id = (collection_count == 1).then(|| collection_ids[0]);
        let query = StorageAuthorizationCollectionsAccessQuery::new(
            principal_id_to_storage(principal_id),
            collection_ids.iter().copied().map(collection_id_to_storage),
            requested
                .iter()
                .copied()
                .map(crate::permissions::permission_to_storage),
        );
        let authorized = storage_handle(backend)
            .authorize_local_collections(query)
            .await?;

        if authorized {
            crate::logger::log_authorization_grant(
                principal_id,
                &requested,
                Some(collection_count),
                collection_id,
                "permissions",
            );
            Ok(())
        } else {
            crate::logger::log_authorization_denial(
                principal_id,
                &requested,
                Some(collection_count),
                collection_id,
                "permissions",
            );
            Err(ApiError::Forbidden(
                "User does not have the required permissions".to_string(),
            ))
        }
    }
}

impl<T: AuthzSubject + ?Sized> UserPermissions for T {}

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
