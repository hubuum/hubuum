use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::group::Group;
use crate::models::output::{EffectiveGroupPermission, GroupPermission};
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionID, NewCollectionWithAssignee, Permission, Permissions, TokenScope,
    UpdateCollection,
};

use super::StorageError;

/// Persistence capability for the core collection lifecycle.
///
/// Methods are intentionally aggregate-shaped. Implementations retain control
/// over transactions, hierarchy maintenance, initial permission grants, and
/// atomic event persistence.
#[async_trait]
pub(crate) trait CollectionStore: Send + Sync {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError>;

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError>;

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError>;

    async fn collection_ancestors(&self, id: CollectionID)
    -> Result<Vec<Collection>, StorageError>;

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;
}

/// Backend-neutral compatibility contract for collection record mutations.
///
/// Application services use [`CollectionStore`] for ordinary lifecycle behavior.
/// Legacy domain adapters additionally require deliberately event-suppressed
/// writes; every selectable backend must provide those paths without exposing
/// adapter or database types to callers.
#[async_trait]
pub(crate) trait CollectionRecordStorage: Send + Sync {
    async fn create_collection_record(
        &self,
        command: &NewCollectionWithAssignee,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError>;

    async fn update_collection_record(
        &self,
        update: &UpdateCollection,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError>;

    async fn delete_collection_record(
        &self,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn move_collection_record(
        &self,
        collection_id: i32,
        new_parent_collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError>;
}

#[derive(Clone)]
pub(crate) struct CollectionPrincipalQuery {
    principal_id: i32,
    collection_id: i32,
}

impl CollectionPrincipalQuery {
    pub(crate) const fn new(principal_id: i32, collection_id: i32) -> Self {
        Self {
            principal_id,
            collection_id,
        }
    }

    pub(crate) const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    pub(crate) const fn collection_id(&self) -> i32 {
        self.collection_id
    }
}

#[derive(Clone)]
pub(crate) struct CollectionPrincipalPageQuery {
    principal: CollectionPrincipalQuery,
    query_options: QueryOptions,
}

impl CollectionPrincipalPageQuery {
    pub(crate) const fn new(
        principal: CollectionPrincipalQuery,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            principal,
            query_options,
        }
    }

    pub(crate) const fn principal(&self) -> &CollectionPrincipalQuery {
        &self.principal
    }

    pub(crate) const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

#[derive(Clone)]
pub(crate) struct CollectionVisibilityQuery {
    principal_id: i32,
    permission: Permissions,
    scopes: Option<TokenScope>,
}

impl CollectionVisibilityQuery {
    pub(crate) fn new(
        principal_id: i32,
        permission: Permissions,
        scopes: Option<&TokenScope>,
    ) -> Self {
        Self {
            principal_id,
            permission,
            scopes: scopes.cloned(),
        }
    }

    pub(crate) const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    pub(crate) const fn permission(&self) -> Permissions {
        self.permission
    }

    pub(crate) const fn scopes(&self) -> Option<&TokenScope> {
        self.scopes.as_ref()
    }
}

#[derive(Clone)]
pub(crate) struct CollectionGroupPermissionQuery {
    collection_id: i32,
    group_id: i32,
    permission: Permissions,
}

impl CollectionGroupPermissionQuery {
    pub(crate) const fn new(collection_id: i32, group_id: i32, permission: Permissions) -> Self {
        Self {
            collection_id,
            group_id,
            permission,
        }
    }

    pub(crate) const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    pub(crate) const fn group_id(&self) -> i32 {
        self.group_id
    }

    pub(crate) const fn permission(&self) -> Permissions {
        self.permission
    }
}

#[derive(Clone)]
pub(crate) struct CollectionGroupsQuery {
    collection_id: i32,
    permission: Permissions,
}

impl CollectionGroupsQuery {
    pub(crate) const fn new(collection_id: i32, permission: Permissions) -> Self {
        Self {
            collection_id,
            permission,
        }
    }

    pub(crate) const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    pub(crate) const fn permission(&self) -> Permissions {
        self.permission
    }
}

#[derive(Clone)]
pub(crate) struct CollectionGroupsPageQuery {
    groups: CollectionGroupsQuery,
    query_options: QueryOptions,
}

impl CollectionGroupsPageQuery {
    pub(crate) const fn new(groups: CollectionGroupsQuery, query_options: QueryOptions) -> Self {
        Self {
            groups,
            query_options,
        }
    }

    pub(crate) const fn groups(&self) -> &CollectionGroupsQuery {
        &self.groups
    }

    pub(crate) const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

#[derive(Clone)]
pub(crate) struct CollectionGrantListQuery {
    collection_id: i32,
    permissions: Vec<Permissions>,
    query_options: QueryOptions,
}

impl CollectionGrantListQuery {
    pub(crate) fn new(
        collection_id: i32,
        permissions: Vec<Permissions>,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            collection_id,
            permissions,
            query_options,
        }
    }

    pub(crate) const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    pub(crate) fn permissions(&self) -> &[Permissions] {
        &self.permissions
    }

    pub(crate) const fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

/// Operation-shaped collection permission queries required by every selectable backend.
///
/// Policy decisions still live in the application permission layer. These
/// methods expose the persisted grant projections needed by legacy endpoints
/// without allowing callers to depend on PostgreSQL query construction.
#[async_trait]
pub(crate) trait CollectionPermissionStorage: Send + Sync {
    async fn principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<GroupPermission>, StorageError>;

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<(Collection, Group, Permission)>, StorageError>;

    async fn principal_collection_permissions_page(
        &self,
        query: CollectionPrincipalPageQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError>;

    async fn effective_principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError>;

    async fn visible_collections(
        &self,
        query: CollectionVisibilityQuery,
    ) -> Result<Vec<Collection>, StorageError>;

    async fn group_has_collection_permission(
        &self,
        query: CollectionGroupPermissionQuery,
    ) -> Result<bool, StorageError>;

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError>;

    async fn groups_with_collection_permission(
        &self,
        query: CollectionGroupsQuery,
    ) -> Result<Vec<Group>, StorageError>;

    async fn groups_with_collection_permission_page(
        &self,
        query: CollectionGroupsPageQuery,
    ) -> Result<(Vec<Group>, i64), StorageError>;

    async fn list_collection_group_permissions(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<Vec<GroupPermission>, StorageError>;

    async fn list_collection_group_permissions_page(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError>;

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Permission, StorageError>;
}
