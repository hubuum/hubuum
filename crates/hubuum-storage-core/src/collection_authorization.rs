use std::fmt;

use async_trait::async_trait;
use hubuum_domain::{CollectionId, GroupId, PrincipalId};
use hubuum_query::QueryOptions;

use crate::{
    AuthenticationTokenScope, AuthorizationCollection, AuthorizationCollectionGrantListQuery,
    AuthorizationGrant, AuthorizationGroup, AuthorizationGroupGrant, AuthorizationGroupGrantPage,
    AuthorizationPermission, AuthorizationPolicySnapshotRow, StorageError,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationPrincipalCollectionQuery {
    principal_id: PrincipalId,
    collection_id: CollectionId,
}

impl AuthorizationPrincipalCollectionQuery {
    #[must_use]
    pub const fn new(principal_id: PrincipalId, collection_id: CollectionId) -> Self {
        Self {
            principal_id,
            collection_id,
        }
    }

    #[must_use]
    pub const fn principal_id(self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub const fn collection_id(self) -> CollectionId {
        self.collection_id
    }
}

impl fmt::Debug for AuthorizationPrincipalCollectionQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPrincipalCollectionQuery")
            .field("principal_id", &"<redacted>")
            .field("collection_id", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct AuthorizationPrincipalCollectionPageQuery {
    principal: AuthorizationPrincipalCollectionQuery,
    query_options: QueryOptions,
}

impl AuthorizationPrincipalCollectionPageQuery {
    #[must_use]
    pub const fn new(
        principal: AuthorizationPrincipalCollectionQuery,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            principal,
            query_options,
        }
    }

    #[must_use]
    pub const fn principal(&self) -> AuthorizationPrincipalCollectionQuery {
        self.principal
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

impl fmt::Debug for AuthorizationPrincipalCollectionPageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPrincipalCollectionPageQuery")
            .field("principal", &self.principal)
            .field("filter_count", &self.query_options.filters().len())
            .field("sort_count", &self.query_options.sort().len())
            .field("limit", &self.query_options.limit())
            .field("has_cursor", &self.query_options.cursor().is_some())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationCollectionVisibilityQuery {
    principal_id: PrincipalId,
    is_admin: bool,
    permission: AuthorizationPermission,
    scope: Option<AuthenticationTokenScope>,
}

impl AuthorizationCollectionVisibilityQuery {
    #[must_use]
    pub const fn new(
        principal_id: PrincipalId,
        is_admin: bool,
        permission: AuthorizationPermission,
        scope: Option<AuthenticationTokenScope>,
    ) -> Self {
        Self {
            principal_id,
            is_admin,
            permission,
            scope,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub const fn permission(&self) -> AuthorizationPermission {
        self.permission
    }

    #[must_use]
    pub const fn is_admin(&self) -> bool {
        self.is_admin
    }

    #[must_use]
    pub const fn scope(&self) -> Option<&AuthenticationTokenScope> {
        self.scope.as_ref()
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PrincipalId,
        bool,
        AuthorizationPermission,
        Option<AuthenticationTokenScope>,
    ) {
        (
            self.principal_id,
            self.is_admin,
            self.permission,
            self.scope,
        )
    }
}

impl fmt::Debug for AuthorizationCollectionVisibilityQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionVisibilityQuery")
            .field("principal_id", &"<redacted>")
            .field("is_admin", &self.is_admin)
            .field("permission", &self.permission)
            .field("has_scope", &self.scope.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorizationGroupCollectionQuery {
    collection_id: CollectionId,
    group_id: GroupId,
    permission: AuthorizationPermission,
}

impl AuthorizationGroupCollectionQuery {
    #[must_use]
    pub const fn new(
        collection_id: CollectionId,
        group_id: GroupId,
        permission: AuthorizationPermission,
    ) -> Self {
        Self {
            collection_id,
            group_id,
            permission,
        }
    }

    #[must_use]
    pub const fn collection_id(self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn group_id(self) -> GroupId {
        self.group_id
    }

    #[must_use]
    pub const fn permission(self) -> AuthorizationPermission {
        self.permission
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorizationCollectionGroupsQuery {
    collection_id: CollectionId,
    permission: AuthorizationPermission,
}

impl AuthorizationCollectionGroupsQuery {
    #[must_use]
    pub const fn new(collection_id: CollectionId, permission: AuthorizationPermission) -> Self {
        Self {
            collection_id,
            permission,
        }
    }

    #[must_use]
    pub const fn collection_id(self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn permission(self) -> AuthorizationPermission {
        self.permission
    }
}

#[derive(Clone, PartialEq)]
pub struct AuthorizationCollectionGroupsPageQuery {
    groups: AuthorizationCollectionGroupsQuery,
    query_options: QueryOptions,
}

impl AuthorizationCollectionGroupsPageQuery {
    #[must_use]
    pub const fn new(
        groups: AuthorizationCollectionGroupsQuery,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            groups,
            query_options,
        }
    }

    #[must_use]
    pub const fn groups(&self) -> AuthorizationCollectionGroupsQuery {
        self.groups
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

impl fmt::Debug for AuthorizationCollectionGroupsPageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionGroupsPageQuery")
            .field("groups", &self.groups)
            .field("filter_count", &self.query_options.filters().len())
            .field("sort_count", &self.query_options.sort().len())
            .field("limit", &self.query_options.limit())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationGroupPage {
    groups: Vec<AuthorizationGroup>,
    total_count: i64,
}

impl AuthorizationGroupPage {
    #[must_use]
    pub const fn new(groups: Vec<AuthorizationGroup>, total_count: i64) -> Self {
        Self {
            groups,
            total_count,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<AuthorizationGroup>, i64) {
        (self.groups, self.total_count)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationEffectiveGroupGrant {
    target_collection: AuthorizationCollection,
    source_collection: AuthorizationCollection,
    depth: i32,
    inherited: bool,
    group: AuthorizationGroup,
    grant: AuthorizationGrant,
}

impl AuthorizationEffectiveGroupGrant {
    #[must_use]
    pub const fn new(
        target_collection: AuthorizationCollection,
        source_collection: AuthorizationCollection,
        depth: i32,
        inherited: bool,
        group: AuthorizationGroup,
        grant: AuthorizationGrant,
    ) -> Self {
        Self {
            target_collection,
            source_collection,
            depth,
            inherited,
            group,
            grant,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        AuthorizationCollection,
        AuthorizationCollection,
        i32,
        bool,
        AuthorizationGroup,
        AuthorizationGrant,
    ) {
        (
            self.target_collection,
            self.source_collection,
            self.depth,
            self.inherited,
            self.group,
            self.grant,
        )
    }
}

/// Collection-oriented authorization projections required by legacy and
/// administration APIs. Policy decisions remain outside this data contract.
#[async_trait]
pub trait CollectionAuthorizationStorage: Send + Sync {
    async fn principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError>;

    async fn principal_all_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError>;

    async fn principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError>;

    async fn effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError>;

    async fn visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError>;

    async fn group_has_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError>;

    async fn effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError>;

    async fn groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError>;

    async fn groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<AuthorizationGroupPage, StorageError>;

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError>;

    async fn list_collection_group_permissions_page(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError>;

    async fn collection_group_permission(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<AuthorizationGrant, StorageError>;
}
