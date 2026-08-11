use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;
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

/// Batch permission lookup for a principal across a collection set.
///
/// The backend must return `true` only when every requested permission is
/// available on every requested collection. Collection identifiers and
/// permissions are normalized so adapters receive a deterministic query.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationCollectionsAccessQuery {
    principal_id: i32,
    collection_ids: Vec<i32>,
    permissions: Vec<AuthorizationPermission>,
}

impl AuthorizationCollectionsAccessQuery {
    #[must_use]
    pub fn new(
        principal_id: i32,
        collection_ids: impl IntoIterator<Item = i32>,
        permissions: impl IntoIterator<Item = AuthorizationPermission>,
    ) -> Self {
        let mut collection_ids = collection_ids.into_iter().collect::<Vec<_>>();
        collection_ids.sort_unstable();
        collection_ids.dedup();
        Self {
            principal_id,
            collection_ids,
            permissions: normalized_permissions(permissions),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub fn collection_ids(&self) -> &[i32] {
        &self.collection_ids
    }

    #[must_use]
    pub fn permissions(&self) -> &[AuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for AuthorizationCollectionsAccessQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationCollectionsAccessQuery")
            .field("principal_id", &"[redacted]")
            .field("collection_count", &self.collection_ids.len())
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

/// One complete local-policy row for backend-neutral policy export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationPolicySnapshotRow {
    grant: AuthorizationGrant,
    group: AuthorizationGroup,
    collection: AuthorizationCollection,
}

impl AuthorizationPolicySnapshotRow {
    #[must_use]
    pub const fn new(
        grant: AuthorizationGrant,
        group: AuthorizationGroup,
        collection: AuthorizationCollection,
    ) -> Self {
        Self {
            grant,
            group,
            collection,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        AuthorizationGrant,
        AuthorizationGroup,
        AuthorizationCollection,
    ) {
        (self.grant, self.group, self.collection)
    }
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
    event_context: Option<EventContext>,
}

impl fmt::Debug for AuthorizationGrantMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGrantMutation")
            .field("key", &self.key)
            .field("permission_count", &self.permissions.len())
            .field("replace_existing", &self.replace_existing)
            .field("has_event_context", &self.event_context.is_some())
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
            event_context: None,
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

    #[must_use]
    pub fn event_context(mut self, event_context: EventContext) -> Self {
        self.event_context = Some(event_context);
        self
    }

    #[must_use]
    pub const fn event_context_value(&self) -> Option<&EventContext> {
        self.event_context.as_ref()
    }
}

/// One collection permission-set snapshot query.
///
/// A group filter narrows the returned grants without changing the owner
/// revision, so conditional requests still describe the complete permission
/// set for the collection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationPermissionSetQuery {
    collection_id: i32,
    group_id: Option<i32>,
}

impl AuthorizationPermissionSetQuery {
    #[must_use]
    pub const fn new(collection_id: i32, group_id: Option<i32>) -> Self {
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
    pub const fn group_id(self) -> Option<i32> {
        self.group_id
    }
}

impl fmt::Debug for AuthorizationPermissionSetQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPermissionSetQuery")
            .field("collection_id", &"[redacted]")
            .field("has_group_filter", &self.group_id.is_some())
            .finish()
    }
}

/// Revisioned local permission set returned without database row types.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationPermissionSet {
    collection_id: i32,
    revision: i64,
    grants: Vec<AuthorizationGrant>,
}

impl AuthorizationPermissionSet {
    #[must_use]
    pub const fn new(collection_id: i32, revision: i64, grants: Vec<AuthorizationGrant>) -> Self {
        Self {
            collection_id,
            revision,
            grants,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, i64, Vec<AuthorizationGrant>) {
        (self.collection_id, self.revision, self.grants)
    }
}

impl fmt::Debug for AuthorizationPermissionSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPermissionSet")
            .field("collection_id", &"[redacted]")
            .field("revision", &self.revision)
            .field("grant_count", &self.grants.len())
            .finish()
    }
}

/// Delete-all request with optional atomic event provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationGrantDelete {
    key: AuthorizationGrantKey,
    event_context: Option<EventContext>,
}

impl AuthorizationGrantDelete {
    #[must_use]
    pub const fn new(key: AuthorizationGrantKey) -> Self {
        Self {
            key,
            event_context: None,
        }
    }

    #[must_use]
    pub fn event_context(mut self, event_context: EventContext) -> Self {
        self.event_context = Some(event_context);
        self
    }

    #[must_use]
    pub const fn key(&self) -> AuthorizationGrantKey {
        self.key
    }

    #[must_use]
    pub const fn event_context_value(&self) -> Option<&EventContext> {
        self.event_context.as_ref()
    }
}

impl fmt::Debug for AuthorizationGrantDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationGrantDelete")
            .field("key", &self.key)
            .field("has_event_context", &self.event_context.is_some())
            .finish()
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

/// Deduplicated resource identifiers requested for authorization enrichment.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationResourceIds {
    ids: Vec<i32>,
}

impl AuthorizationResourceIds {
    #[must_use]
    pub fn new(ids: impl IntoIterator<Item = i32>) -> Self {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
    }

    #[must_use]
    pub fn ids(&self) -> &[i32] {
        &self.ids
    }
}

impl fmt::Debug for AuthorizationResourceIds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationResourceIds")
            .field("resource_count", &self.ids.len())
            .finish()
    }
}

/// Class facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationClassResource {
    id: i32,
    collection_id: i32,
}

impl AuthorizationClassResource {
    #[must_use]
    pub const fn new(id: i32, collection_id: i32) -> Self {
        Self { id, collection_id }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }
}

impl fmt::Debug for AuthorizationClassResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationClassResource")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .finish()
    }
}

/// Object facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizationObjectResource {
    id: i32,
    collection_id: i32,
    class_id: i32,
    name: String,
}

impl AuthorizationObjectResource {
    #[must_use]
    pub fn new(id: i32, collection_id: i32, class_id: i32, name: impl Into<String>) -> Self {
        Self {
            id,
            collection_id,
            class_id,
            name: name.into(),
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
    pub const fn class_id(&self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for AuthorizationObjectResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationObjectResource")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("class_id", &"[redacted]")
            .field("name", &"[redacted]")
            .finish()
    }
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

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError>;

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError>;

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError>;

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError>;

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError>;

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError>;

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError>;

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError>;

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError>;

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError>;

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError>;

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
        request: AuthorizationGrantDelete,
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

    #[test]
    fn batch_access_query_normalizes_collection_and_permission_sets() {
        let query = AuthorizationCollectionsAccessQuery::new(
            1,
            [9, 3, 9, 5],
            [
                AuthorizationPermission::UpdateCollection,
                AuthorizationPermission::ReadCollection,
                AuthorizationPermission::UpdateCollection,
            ],
        );

        assert_eq!(query.collection_ids(), &[3, 5, 9]);
        assert_eq!(
            query.permissions(),
            &[
                AuthorizationPermission::ReadCollection,
                AuthorizationPermission::UpdateCollection,
            ]
        );
    }

    #[test]
    fn batch_access_query_debug_redacts_identity() {
        let debug = format!(
            "{:?}",
            AuthorizationCollectionsAccessQuery::new(
                987_654,
                [876_543, 765_432],
                [AuthorizationPermission::ReadCollection],
            )
        );

        for sensitive in ["987654", "876543", "765432"] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("collection_count"));
        assert!(debug.contains("permission_count"));
    }

    #[test]
    fn authorization_resource_debug_redacts_projected_values() {
        let class = AuthorizationClassResource::new(987_654, 876_543);
        let object =
            AuthorizationObjectResource::new(765_432, 654_321, 543_210, "sensitive-object-name");
        let ids = AuthorizationResourceIds::new([432_109, 321_098]);
        let debug = format!("{class:?} {object:?} {ids:?}");

        for sensitive in [
            "987654",
            "876543",
            "765432",
            "654321",
            "543210",
            "432109",
            "321098",
            "sensitive-object-name",
        ] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("resource_count"));
    }

    #[test]
    fn permission_set_and_mutation_debug_redact_identifiers() {
        let key = AuthorizationGrantKey::new(987_654, 876_543);
        let query = AuthorizationPermissionSetQuery::new(987_654, Some(876_543));
        let mutation =
            AuthorizationGrantMutation::new(key, [AuthorizationPermission::ReadCollection], false)
                .event_context(EventContext::system());
        let delete = AuthorizationGrantDelete::new(key).event_context(EventContext::system());
        let debug = format!("{query:?} {mutation:?} {delete:?}");

        for sensitive in ["987654", "876543"] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("has_event_context"));
        assert!(debug.contains("has_group_filter"));
    }
}
