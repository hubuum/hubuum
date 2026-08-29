use std::fmt;

use async_trait::async_trait;
use hubuum_domain::{CollectionId, GroupId, PrincipalId};
use hubuum_query::QueryOptions;

use crate::{
    StorageAuthenticationTokenScope, StorageAuthorizationCollection, StorageAuthorizationGrant,
    StorageAuthorizationGroup, StorageAuthorizationGroupGrant, StorageAuthorizationPermission,
    StorageAuthorizationPolicySnapshotRow, StorageError, StoragePage,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageAuthorizationPrincipalCollectionQuery {
    principal_id: PrincipalId,
    collection_id: CollectionId,
}

impl StorageAuthorizationPrincipalCollectionQuery {
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

impl fmt::Debug for StorageAuthorizationPrincipalCollectionQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationPrincipalCollectionQuery")
            .field("principal_id", &"<redacted>")
            .field("collection_id", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageAuthorizationPrincipalCollectionPageQuery {
    principal: StorageAuthorizationPrincipalCollectionQuery,
    query_options: QueryOptions,
}

impl StorageAuthorizationPrincipalCollectionPageQuery {
    #[must_use]
    pub const fn new(
        principal: StorageAuthorizationPrincipalCollectionQuery,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            principal,
            query_options,
        }
    }

    #[must_use]
    pub const fn principal(&self) -> StorageAuthorizationPrincipalCollectionQuery {
        self.principal
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

impl fmt::Debug for StorageAuthorizationPrincipalCollectionPageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationPrincipalCollectionPageQuery")
            .field("principal", &self.principal)
            .field("filter_count", &self.query_options.filters().len())
            .field("sort_count", &self.query_options.sort().len())
            .field("limit", &self.query_options.limit())
            .field("has_cursor", &self.query_options.cursor().is_some())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationCollectionVisibilityQuery {
    principal_id: PrincipalId,
    is_admin: bool,
    permission: StorageAuthorizationPermission,
    scope: Option<StorageAuthenticationTokenScope>,
}

impl StorageAuthorizationCollectionVisibilityQuery {
    #[must_use]
    pub const fn new(
        principal_id: PrincipalId,
        is_admin: bool,
        permission: StorageAuthorizationPermission,
        scope: Option<StorageAuthenticationTokenScope>,
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
    pub const fn permission(&self) -> StorageAuthorizationPermission {
        self.permission
    }

    #[must_use]
    pub const fn is_admin(&self) -> bool {
        self.is_admin
    }

    #[must_use]
    pub const fn scope(&self) -> Option<&StorageAuthenticationTokenScope> {
        self.scope.as_ref()
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        PrincipalId,
        bool,
        StorageAuthorizationPermission,
        Option<StorageAuthenticationTokenScope>,
    ) {
        (
            self.principal_id,
            self.is_admin,
            self.permission,
            self.scope,
        )
    }
}

impl fmt::Debug for StorageAuthorizationCollectionVisibilityQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionVisibilityQuery")
            .field("principal_id", &"<redacted>")
            .field("is_admin", &self.is_admin)
            .field("permission", &self.permission)
            .field("has_scope", &self.scope.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationGroupCollectionQuery {
    collection_id: CollectionId,
    group_id: GroupId,
    permission: StorageAuthorizationPermission,
}

impl StorageAuthorizationGroupCollectionQuery {
    #[must_use]
    pub const fn new(
        collection_id: CollectionId,
        group_id: GroupId,
        permission: StorageAuthorizationPermission,
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
    pub const fn permission(self) -> StorageAuthorizationPermission {
        self.permission
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationCollectionGroupsQuery {
    collection_id: CollectionId,
    permission: StorageAuthorizationPermission,
}

impl StorageAuthorizationCollectionGroupsQuery {
    #[must_use]
    pub const fn new(
        collection_id: CollectionId,
        permission: StorageAuthorizationPermission,
    ) -> Self {
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
    pub const fn permission(self) -> StorageAuthorizationPermission {
        self.permission
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageAuthorizationCollectionGroupsPageQuery {
    groups: StorageAuthorizationCollectionGroupsQuery,
    query_options: QueryOptions,
}

impl StorageAuthorizationCollectionGroupsPageQuery {
    #[must_use]
    pub const fn new(
        groups: StorageAuthorizationCollectionGroupsQuery,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            groups,
            query_options,
        }
    }

    #[must_use]
    pub const fn groups(&self) -> StorageAuthorizationCollectionGroupsQuery {
        self.groups
    }

    #[must_use]
    pub const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

impl fmt::Debug for StorageAuthorizationCollectionGroupsPageQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionGroupsPageQuery")
            .field("groups", &self.groups)
            .field("filter_count", &self.query_options.filters().len())
            .field("sort_count", &self.query_options.sort().len())
            .field("limit", &self.query_options.limit())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationEffectiveGroupGrant {
    target_collection: StorageAuthorizationCollection,
    source_collection: StorageAuthorizationCollection,
    depth: i32,
    inherited: bool,
    group: StorageAuthorizationGroup,
    grant: StorageAuthorizationGrant,
}

impl StorageAuthorizationEffectiveGroupGrant {
    #[must_use]
    pub const fn new(
        target_collection: StorageAuthorizationCollection,
        source_collection: StorageAuthorizationCollection,
        depth: i32,
        inherited: bool,
        group: StorageAuthorizationGroup,
        grant: StorageAuthorizationGrant,
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
        StorageAuthorizationCollection,
        StorageAuthorizationCollection,
        i32,
        bool,
        StorageAuthorizationGroup,
        StorageAuthorizationGrant,
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
pub trait CollectionAuthorizationQueryStorage: Send + Sync {
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError>;

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError>;

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError>;

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError>;

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError>;

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError>;

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError>;

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError>;

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError>;
}
