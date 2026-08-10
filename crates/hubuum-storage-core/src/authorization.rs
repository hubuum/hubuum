use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_query::QueryOptions;

use crate::StorageError;

/// Permission vocabulary persisted by a local authorization store.
///
/// Keeping this enum in the storage contract prevents adapters from accepting
/// application enums or unvalidated strings at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AuthorizationPermission {
    ReadCollection,
    UpdateCollection,
    DeleteCollection,
    DelegateCollection,
    CreateClass,
    ReadClass,
    UpdateClass,
    DeleteClass,
    CreateObject,
    ReadObject,
    UpdateObject,
    DeleteObject,
    CreateClassRelation,
    ReadClassRelation,
    UpdateClassRelation,
    DeleteClassRelation,
    CreateObjectRelation,
    ReadObjectRelation,
    UpdateObjectRelation,
    DeleteObjectRelation,
    ReadTemplate,
    CreateTemplate,
    UpdateTemplate,
    DeleteTemplate,
    ReadRemoteTarget,
    CreateRemoteTarget,
    UpdateRemoteTarget,
    DeleteRemoteTarget,
    ExecuteRemoteTarget,
    ReadAudit,
    ManageEventSubscription,
}

/// Principal facts required by policy engines.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationPrincipal {
    principal_id: i32,
    group_ids: Vec<i32>,
}

impl AuthorizationPrincipal {
    #[must_use]
    pub fn new(principal_id: i32, group_ids: impl IntoIterator<Item = i32>) -> Self {
        let mut group_ids = group_ids.into_iter().collect::<Vec<_>>();
        group_ids.sort_unstable();
        group_ids.dedup();
        Self {
            principal_id,
            group_ids,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub fn group_ids(&self) -> &[i32] {
        &self.group_ids
    }

    #[must_use]
    pub fn into_group_ids(self) -> Vec<i32> {
        self.group_ids
    }
}

impl fmt::Debug for AuthorizationPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPrincipal")
            .field("principal_id", &"[redacted]")
            .field("group_count", &self.group_ids.len())
            .finish()
    }
}

/// Membership lookup by stable principal id and configured group identity.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGroupMembershipQuery {
    principal_id: i32,
    group_name: String,
    identity_scope: String,
}

impl AuthorizationGroupMembershipQuery {
    #[must_use]
    pub fn new(
        principal_id: i32,
        group_name: impl Into<String>,
        identity_scope: impl Into<String>,
    ) -> Self {
        Self {
            principal_id,
            group_name: group_name.into(),
            identity_scope: identity_scope.into(),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub fn group_name(&self) -> &str {
        &self.group_name
    }

    #[must_use]
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }
}

impl fmt::Debug for AuthorizationGroupMembershipQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGroupMembershipQuery")
            .field("principal_id", &"[redacted]")
            .field("group_name", &"[redacted]")
            .field("identity_scope", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationCollectionAccessQuery {
    principal_id: i32,
    collection_id: i32,
    permissions: Vec<AuthorizationPermission>,
}

impl AuthorizationCollectionAccessQuery {
    #[must_use]
    pub fn new(
        principal_id: i32,
        collection_id: i32,
        permissions: impl IntoIterator<Item = AuthorizationPermission>,
    ) -> Self {
        Self {
            principal_id,
            collection_id,
            permissions: normalized_permissions(permissions),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub fn permissions(&self) -> &[AuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for AuthorizationCollectionAccessQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionAccessQuery")
            .field("principal_id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("permission_count", &self.permissions.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationCollectionsQuery {
    principal_id: i32,
    permissions: Vec<AuthorizationPermission>,
}

impl AuthorizationCollectionsQuery {
    #[must_use]
    pub fn new(
        principal_id: i32,
        permissions: impl IntoIterator<Item = AuthorizationPermission>,
    ) -> Self {
        Self {
            principal_id,
            permissions: normalized_permissions(permissions),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub fn permissions(&self) -> &[AuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for AuthorizationCollectionsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionsQuery")
            .field("principal_id", &"[redacted]")
            .field("permission_count", &self.permissions.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationCollection {
    id: i32,
    name: String,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_collection_id: Option<i32>,
    revision: i64,
}

impl AuthorizationCollection {
    #[must_use]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        description: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        parent_collection_id: Option<i32>,
        revision: i64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            description: description.into(),
            created_at,
            updated_at,
            parent_collection_id,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<i32> {
        self.parent_collection_id
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

impl fmt::Debug for AuthorizationCollection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollection")
            .field("id", &"[redacted]")
            .field("name", &"[redacted]")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field(
                "parent_collection_id",
                &self.parent_collection_id.map(|_| "[redacted]"),
            )
            .field("revision", &self.revision)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGroupIdentity {
    id: i32,
    group_name: String,
    identity_scope_id: i32,
    managed_by: String,
    external_key: Option<String>,
}

impl AuthorizationGroupIdentity {
    #[must_use]
    pub fn new(
        id: i32,
        group_name: impl Into<String>,
        identity_scope_id: i32,
        managed_by: impl Into<String>,
        external_key: Option<String>,
    ) -> Self {
        Self {
            id,
            group_name: group_name.into(),
            identity_scope_id,
            managed_by: managed_by.into(),
            external_key,
        }
    }
}

impl fmt::Debug for AuthorizationGroupIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGroupIdentity")
            .field("id", &"[redacted]")
            .field("group_name", &"[redacted]")
            .field("identity_scope_id", &"[redacted]")
            .field("managed_by", &self.managed_by)
            .field(
                "external_key",
                &self.external_key.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGroupProfile {
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl AuthorizationGroupProfile {
    #[must_use]
    pub fn new(
        description: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            description: description.into(),
            created_at,
            updated_at,
            revision,
        }
    }
}

impl fmt::Debug for AuthorizationGroupProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGroupProfile")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("revision", &self.revision)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorizationGroupSyncState {
    last_attempted_at: Option<NaiveDateTime>,
    last_succeeded_at: Option<NaiveDateTime>,
}

impl AuthorizationGroupSyncState {
    #[must_use]
    pub const fn new(
        last_attempted_at: Option<NaiveDateTime>,
        last_succeeded_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            last_attempted_at,
            last_succeeded_at,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGroup {
    identity: AuthorizationGroupIdentity,
    profile: AuthorizationGroupProfile,
    sync: AuthorizationGroupSyncState,
}

impl AuthorizationGroup {
    #[must_use]
    pub const fn new(
        identity: AuthorizationGroupIdentity,
        profile: AuthorizationGroupProfile,
        sync: AuthorizationGroupSyncState,
    ) -> Self {
        Self {
            identity,
            profile,
            sync,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.identity.id
    }
    #[must_use]
    pub fn group_name(&self) -> &str {
        &self.identity.group_name
    }
    #[must_use]
    pub fn description(&self) -> &str {
        &self.profile.description
    }
    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.profile.created_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.profile.updated_at
    }
    #[must_use]
    pub const fn identity_scope_id(&self) -> i32 {
        self.identity.identity_scope_id
    }
    #[must_use]
    pub fn managed_by(&self) -> &str {
        &self.identity.managed_by
    }
    #[must_use]
    pub fn external_key(&self) -> Option<&str> {
        self.identity.external_key.as_deref()
    }
    #[must_use]
    pub const fn last_sync_attempted_at(&self) -> Option<NaiveDateTime> {
        self.sync.last_attempted_at
    }
    #[must_use]
    pub const fn last_sync_success_at(&self) -> Option<NaiveDateTime> {
        self.sync.last_succeeded_at
    }
    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.profile.revision
    }
}

impl fmt::Debug for AuthorizationGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGroup")
            .field("identity", &self.identity)
            .field("profile", &self.profile)
            .field("sync", &self.sync)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGrant {
    id: i32,
    collection_id: i32,
    group_id: i32,
    permissions: Vec<AuthorizationPermission>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl fmt::Debug for AuthorizationGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGrant")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("group_id", &"[redacted]")
            .field("permission_count", &self.permissions.len())
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl AuthorizationGrant {
    #[must_use]
    pub fn new(
        id: i32,
        collection_id: i32,
        group_id: i32,
        permissions: impl IntoIterator<Item = AuthorizationPermission>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        Self {
            id,
            collection_id,
            group_id,
            permissions: normalized_permissions(permissions),
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }
    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }
    #[must_use]
    pub const fn group_id(&self) -> i32 {
        self.group_id
    }
    #[must_use]
    pub fn permissions(&self) -> &[AuthorizationPermission] {
        &self.permissions
    }
    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationGroupGrant {
    group: AuthorizationGroup,
    grant: AuthorizationGrant,
}

impl AuthorizationGroupGrant {
    #[must_use]
    pub const fn new(group: AuthorizationGroup, grant: AuthorizationGrant) -> Self {
        Self { group, grant }
    }

    #[must_use]
    pub fn into_parts(self) -> (AuthorizationGroup, AuthorizationGrant) {
        (self.group, self.grant)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationGroupGrantPage {
    items: Vec<AuthorizationGroupGrant>,
    total_count: i64,
}

impl AuthorizationGroupGrantPage {
    #[must_use]
    pub const fn new(items: Vec<AuthorizationGroupGrant>, total_count: i64) -> Self {
        Self { items, total_count }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<AuthorizationGroupGrant>, i64) {
        (self.items, self.total_count)
    }
}

#[derive(Clone, PartialEq)]
pub struct AuthorizationCollectionGrantListQuery {
    collection_id: i32,
    required_permissions: Vec<AuthorizationPermission>,
    query_options: QueryOptions,
}

impl fmt::Debug for AuthorizationCollectionGrantListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionGrantListQuery")
            .field("collection_id", &"[redacted]")
            .field(
                "required_permission_count",
                &self.required_permissions.len(),
            )
            .field("filter_count", &self.query_options.filters.len())
            .field("sort_count", &self.query_options.sort.len())
            .field("limit", &self.query_options.limit)
            .field("has_cursor", &self.query_options.cursor.is_some())
            .field("include_total", &self.query_options.include_total)
            .finish()
    }
}

impl AuthorizationCollectionGrantListQuery {
    #[must_use]
    pub fn new(
        collection_id: i32,
        required_permissions: impl IntoIterator<Item = AuthorizationPermission>,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            collection_id,
            required_permissions: normalized_permissions(required_permissions),
            query_options,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }
    #[must_use]
    pub fn required_permissions(&self) -> &[AuthorizationPermission] {
        &self.required_permissions
    }
    #[must_use]
    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationGrantKey {
    collection_id: i32,
    group_id: i32,
}

impl fmt::Debug for AuthorizationGrantKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGrantKey")
            .field("collection_id", &"[redacted]")
            .field("group_id", &"[redacted]")
            .finish()
    }
}

impl AuthorizationGrantKey {
    #[must_use]
    pub const fn new(collection_id: i32, group_id: i32) -> Self {
        Self {
            collection_id,
            group_id,
        }
    }
    #[must_use]
    pub const fn collection_id(self) -> i32 {
        self.collection_id
    }
    #[must_use]
    pub const fn group_id(self) -> i32 {
        self.group_id
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGrantMutation {
    key: AuthorizationGrantKey,
    permissions: Vec<AuthorizationPermission>,
    replace_existing: bool,
}

impl fmt::Debug for AuthorizationGrantMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGrantMutation")
            .field("key", &self.key)
            .field("permission_count", &self.permissions.len())
            .field("replace_existing", &self.replace_existing)
            .finish()
    }
}

impl AuthorizationGrantMutation {
    #[must_use]
    pub fn new(
        key: AuthorizationGrantKey,
        permissions: impl IntoIterator<Item = AuthorizationPermission>,
        replace_existing: bool,
    ) -> Self {
        Self {
            key,
            permissions: normalized_permissions(permissions),
            replace_existing,
        }
    }
    #[must_use]
    pub const fn key(&self) -> AuthorizationGrantKey {
        self.key
    }
    #[must_use]
    pub fn permissions(&self) -> &[AuthorizationPermission] {
        &self.permissions
    }
    #[must_use]
    pub const fn replace_existing(&self) -> bool {
        self.replace_existing
    }
}

fn normalized_permissions(
    permissions: impl IntoIterator<Item = AuthorizationPermission>,
) -> Vec<AuthorizationPermission> {
    let mut permissions = permissions.into_iter().collect::<Vec<_>>();
    permissions.sort_unstable();
    permissions.dedup();
    permissions
}

/// Mandatory authorization-data contract for every selectable storage backend.
///
/// Policy decisions remain the responsibility of the configured permission
/// backend. This trait supplies the backend-neutral identity facts and the
/// local policy-store operations needed by Hubuum's built-in permission
/// backend.
#[async_trait]
pub trait AuthorizationStorage: Send + Sync {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError>;

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError>;

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError>;

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError>;

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError>;

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError>;

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError>;

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError>;

    async fn revoke_all_local_collection_grants(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_groups_are_normalized() {
        let principal = AuthorizationPrincipal::new(7, [3, 1, 3, 2]);
        assert_eq!(principal.group_ids(), &[1, 2, 3]);
    }

    #[test]
    fn principal_debug_redacts_identity() {
        let debug = format!("{:?}", AuthorizationPrincipal::new(987_654, [123, 456]));
        assert!(!debug.contains("987654"));
        assert!(!debug.contains("123"));
        assert!(debug.contains("group_count"));
    }

    #[test]
    fn membership_debug_redacts_lookup_values() {
        let debug = format!(
            "{:?}",
            AuthorizationGroupMembershipQuery::new(987_654, "secret-admins", "private-scope")
        );
        assert!(!debug.contains("987654"));
        assert!(!debug.contains("secret-admins"));
        assert!(!debug.contains("private-scope"));
    }

    #[test]
    fn authorization_dto_debug_redacts_resource_identity() {
        let query = AuthorizationCollectionAccessQuery::new(
            987_654,
            876_543,
            [AuthorizationPermission::ReadCollection],
        );
        let grant = AuthorizationGrant::new(
            765_432,
            876_543,
            654_321,
            [AuthorizationPermission::ReadCollection],
            NaiveDateTime::default(),
            NaiveDateTime::default(),
        );
        let debug = format!("{query:?} {grant:?}");

        for sensitive in ["987654", "876543", "765432", "654321"] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("permission_count"));
    }

    #[test]
    fn permission_sets_are_normalized() {
        let query = AuthorizationCollectionAccessQuery::new(
            1,
            2,
            [
                AuthorizationPermission::UpdateCollection,
                AuthorizationPermission::ReadCollection,
                AuthorizationPermission::UpdateCollection,
            ],
        );
        assert_eq!(
            query.permissions(),
            &[
                AuthorizationPermission::ReadCollection,
                AuthorizationPermission::UpdateCollection,
            ]
        );
    }
}
