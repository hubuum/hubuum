use std::fmt;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    AuthorizationGrantId, ClassId, CollectionId, GroupId, IdentityScopeId, ObjectId, PrincipalId,
    ResourceId, ResourceRevision,
};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;

use crate::validation::validate_sync_timestamps;
use crate::{
    StorageCandidatePage, StorageCandidatePageLimit, StorageError, StorageMutationOutcome,
    StoragePage, StorageValidationError,
};

/// Permission vocabulary persisted by a local authorization store.
///
/// Keeping this enum in the storage contract prevents adapters from accepting
/// application enums or unvalidated strings at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageAuthorizationPermission {
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

impl StorageAuthorizationPermission {
    pub const ALL: [Self; 31] = [
        Self::ReadCollection,
        Self::UpdateCollection,
        Self::DeleteCollection,
        Self::DelegateCollection,
        Self::CreateClass,
        Self::ReadClass,
        Self::UpdateClass,
        Self::DeleteClass,
        Self::CreateObject,
        Self::ReadObject,
        Self::UpdateObject,
        Self::DeleteObject,
        Self::CreateClassRelation,
        Self::ReadClassRelation,
        Self::UpdateClassRelation,
        Self::DeleteClassRelation,
        Self::CreateObjectRelation,
        Self::ReadObjectRelation,
        Self::UpdateObjectRelation,
        Self::DeleteObjectRelation,
        Self::ReadTemplate,
        Self::CreateTemplate,
        Self::UpdateTemplate,
        Self::DeleteTemplate,
        Self::ReadRemoteTarget,
        Self::CreateRemoteTarget,
        Self::UpdateRemoteTarget,
        Self::DeleteRemoteTarget,
        Self::ExecuteRemoteTarget,
        Self::ReadAudit,
        Self::ManageEventSubscription,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadCollection => "ReadCollection",
            Self::UpdateCollection => "UpdateCollection",
            Self::DeleteCollection => "DeleteCollection",
            Self::DelegateCollection => "DelegateCollection",
            Self::CreateClass => "CreateClass",
            Self::ReadClass => "ReadClass",
            Self::UpdateClass => "UpdateClass",
            Self::DeleteClass => "DeleteClass",
            Self::CreateObject => "CreateObject",
            Self::ReadObject => "ReadObject",
            Self::UpdateObject => "UpdateObject",
            Self::DeleteObject => "DeleteObject",
            Self::CreateClassRelation => "CreateClassRelation",
            Self::ReadClassRelation => "ReadClassRelation",
            Self::UpdateClassRelation => "UpdateClassRelation",
            Self::DeleteClassRelation => "DeleteClassRelation",
            Self::CreateObjectRelation => "CreateObjectRelation",
            Self::ReadObjectRelation => "ReadObjectRelation",
            Self::UpdateObjectRelation => "UpdateObjectRelation",
            Self::DeleteObjectRelation => "DeleteObjectRelation",
            Self::ReadTemplate => "ReadTemplate",
            Self::CreateTemplate => "CreateTemplate",
            Self::UpdateTemplate => "UpdateTemplate",
            Self::DeleteTemplate => "DeleteTemplate",
            Self::ReadRemoteTarget => "ReadRemoteTarget",
            Self::CreateRemoteTarget => "CreateRemoteTarget",
            Self::UpdateRemoteTarget => "UpdateRemoteTarget",
            Self::DeleteRemoteTarget => "DeleteRemoteTarget",
            Self::ExecuteRemoteTarget => "ExecuteRemoteTarget",
            Self::ReadAudit => "ReadAudit",
            Self::ManageEventSubscription => "ManageEventSubscription",
        }
    }

    pub fn from_name(value: &str) -> Result<Self, StorageError> {
        Self::ALL
            .into_iter()
            .find(|permission| permission.as_str() == value)
            .ok_or_else(|| StorageError::invalid_input(format!("Invalid permission: '{value}'")))
    }
}

/// Principal facts required by policy engines.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationPrincipal {
    principal_id: PrincipalId,
    group_ids: Vec<GroupId>,
}

impl StorageAuthorizationPrincipal {
    #[must_use]
    pub fn new(principal_id: PrincipalId, group_ids: impl IntoIterator<Item = GroupId>) -> Self {
        let mut group_ids = group_ids.into_iter().collect::<Vec<_>>();
        group_ids.sort_unstable();
        group_ids.dedup();
        Self {
            principal_id,
            group_ids,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub fn group_ids(&self) -> &[GroupId] {
        &self.group_ids
    }

    #[must_use]
    pub fn into_group_ids(self) -> Vec<GroupId> {
        self.group_ids
    }
}

impl fmt::Debug for StorageAuthorizationPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationPrincipal")
            .field("principal_id", &"[redacted]")
            .field("group_count", &self.group_ids.len())
            .finish()
    }
}

/// Membership lookup by stable principal id and configured group identity.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGroupMembershipQuery {
    principal_id: PrincipalId,
    group_name: String,
    identity_scope: String,
}

impl StorageAuthorizationGroupMembershipQuery {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
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
    pub const fn principal_id(&self) -> PrincipalId {
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

impl fmt::Debug for StorageAuthorizationGroupMembershipQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGroupMembershipQuery")
            .field("principal_id", &"[redacted]")
            .field("group_name", &"[redacted]")
            .field("identity_scope", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationCollectionAccessQuery {
    principal_id: PrincipalId,
    collection_id: CollectionId,
    permissions: Vec<StorageAuthorizationPermission>,
}

impl StorageAuthorizationCollectionAccessQuery {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        collection_id: CollectionId,
        permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
    ) -> Self {
        Self {
            principal_id,
            collection_id,
            permissions: normalized_permissions(permissions),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub fn permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for StorageAuthorizationCollectionAccessQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionAccessQuery")
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
pub struct StorageAuthorizationCollectionsAccessQuery {
    principal_id: PrincipalId,
    collection_ids: Vec<CollectionId>,
    permissions: Vec<StorageAuthorizationPermission>,
}

impl StorageAuthorizationCollectionsAccessQuery {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        collection_ids: impl IntoIterator<Item = CollectionId>,
        permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
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
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub fn collection_ids(&self) -> &[CollectionId] {
        &self.collection_ids
    }

    #[must_use]
    pub fn permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for StorageAuthorizationCollectionsAccessQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionsAccessQuery")
            .field("principal_id", &"[redacted]")
            .field("collection_count", &self.collection_ids.len())
            .field("permission_count", &self.permissions.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationCollectionsQuery {
    principal_id: PrincipalId,
    permissions: Vec<StorageAuthorizationPermission>,
}

impl StorageAuthorizationCollectionsQuery {
    #[must_use]
    pub fn new(
        principal_id: PrincipalId,
        permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
    ) -> Self {
        Self {
            principal_id,
            permissions: normalized_permissions(permissions),
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    #[must_use]
    pub fn permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.permissions
    }
}

impl fmt::Debug for StorageAuthorizationCollectionsQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionsQuery")
            .field("principal_id", &"[redacted]")
            .field("permission_count", &self.permissions.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationCollection {
    id: CollectionId,
    name: String,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    parent_collection_id: Option<CollectionId>,
    revision: ResourceRevision,
}

impl StorageAuthorizationCollection {
    pub fn try_new(
        id: CollectionId,
        name: impl Into<String>,
        description: impl Into<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        parent_collection_id: Option<CollectionId>,
        revision: ResourceRevision,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "authorization collection updated_at must not precede created_at",
            ));
        }
        if parent_collection_id == Some(id) {
            return Err(StorageValidationError::invalid(
                "authorization collection must not be its own parent",
            ));
        }
        Ok(Self {
            id,
            name: name.into(),
            description: description.into(),
            created_at,
            updated_at,
            parent_collection_id,
            revision,
        })
    }

    #[must_use]
    pub const fn id(&self) -> CollectionId {
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
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<CollectionId> {
        self.parent_collection_id
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

impl fmt::Debug for StorageAuthorizationCollection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollection")
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
pub struct StorageAuthorizationGroupIdentity {
    id: GroupId,
    group_name: String,
    identity_scope_id: IdentityScopeId,
    managed_by: String,
    external_key: Option<String>,
}

impl StorageAuthorizationGroupIdentity {
    #[must_use]
    pub fn new(
        id: GroupId,
        group_name: impl Into<String>,
        identity_scope_id: IdentityScopeId,
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

impl fmt::Debug for StorageAuthorizationGroupIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGroupIdentity")
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

/// One stable, id-ordered page request for authorization collection candidates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationCollectionCandidateQuery {
    after_id: Option<CollectionId>,
    page_limit: StorageCandidatePageLimit,
}

impl StorageAuthorizationCollectionCandidateQuery {
    #[must_use]
    pub const fn new(
        after_id: Option<CollectionId>,
        page_limit: StorageCandidatePageLimit,
    ) -> Self {
        Self {
            after_id,
            page_limit,
        }
    }

    #[must_use]
    pub const fn after_id(&self) -> Option<CollectionId> {
        self.after_id
    }

    #[must_use]
    pub const fn page_limit(&self) -> StorageCandidatePageLimit {
        self.page_limit
    }
}

/// Filtered, deterministically ordered authorization group candidate page.
///
/// `options` carries the application-normalized sort and cursor. The
/// constructor replaces its public page size with the validated internal
/// candidate look-ahead limit and disables count work. The policy backend
/// authorizes each returned page before deciding which rows belong to the
/// caller's public page.
#[derive(Clone, PartialEq)]
pub struct StorageAuthorizationGroupCandidateQuery {
    options: QueryOptions,
    page_limit: StorageCandidatePageLimit,
}

impl StorageAuthorizationGroupCandidateQuery {
    #[must_use]
    pub fn new(mut options: QueryOptions, page_limit: StorageCandidatePageLimit) -> Self {
        options.set_include_total(false);
        options
            .set_limit(Some(page_limit.get().saturating_add(1)))
            .expect("a validated storage candidate page limit plus look-ahead must be valid");
        Self {
            options,
            page_limit,
        }
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub const fn page_limit(&self) -> StorageCandidatePageLimit {
        self.page_limit
    }
}

impl fmt::Debug for StorageAuthorizationGroupCandidateQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGroupCandidateQuery")
            .field("filter_count", &self.options.filters().len())
            .field("sort_count", &self.options.sort().len())
            .field("has_cursor", &self.options.cursor().is_some())
            .field("page_limit", &self.page_limit)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGroupProfile {
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageAuthorizationGroupProfile {
    pub fn try_new(
        description: impl Into<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        revision: ResourceRevision,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "authorization group updated_at must not precede created_at",
            ));
        }
        Ok(Self {
            description: description.into(),
            created_at,
            updated_at,
            revision,
        })
    }
}

impl fmt::Debug for StorageAuthorizationGroupProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGroupProfile")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("revision", &self.revision)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationGroupSyncState {
    last_attempted_at: Option<DateTime<Utc>>,
    last_succeeded_at: Option<DateTime<Utc>>,
}

impl StorageAuthorizationGroupSyncState {
    pub fn try_new(
        last_attempted_at: Option<DateTime<Utc>>,
        last_succeeded_at: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        validate_sync_timestamps(last_attempted_at, last_succeeded_at)?;
        Ok(Self {
            last_attempted_at,
            last_succeeded_at,
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGroup {
    identity: StorageAuthorizationGroupIdentity,
    profile: StorageAuthorizationGroupProfile,
    sync: StorageAuthorizationGroupSyncState,
}

impl StorageAuthorizationGroup {
    #[must_use]
    pub const fn new(
        identity: StorageAuthorizationGroupIdentity,
        profile: StorageAuthorizationGroupProfile,
        sync: StorageAuthorizationGroupSyncState,
    ) -> Self {
        Self {
            identity,
            profile,
            sync,
        }
    }

    #[must_use]
    pub const fn id(&self) -> GroupId {
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
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.profile.created_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.profile.updated_at
    }
    #[must_use]
    pub const fn identity_scope_id(&self) -> IdentityScopeId {
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
    pub const fn last_sync_attempted_at(&self) -> Option<DateTime<Utc>> {
        self.sync.last_attempted_at
    }
    #[must_use]
    pub const fn last_sync_success_at(&self) -> Option<DateTime<Utc>> {
        self.sync.last_succeeded_at
    }
    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.profile.revision
    }
}

impl fmt::Debug for StorageAuthorizationGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGroup")
            .field("identity", &self.identity)
            .field("profile", &self.profile)
            .field("sync", &self.sync)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGrant {
    id: AuthorizationGrantId,
    collection_id: CollectionId,
    group_id: GroupId,
    permissions: Vec<StorageAuthorizationPermission>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl fmt::Debug for StorageAuthorizationGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGrant")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .field("group_id", &"[redacted]")
            .field("permission_count", &self.permissions.len())
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl StorageAuthorizationGrant {
    pub fn try_new(
        id: AuthorizationGrantId,
        collection_id: CollectionId,
        group_id: GroupId,
        permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "authorization grant updated_at must not precede created_at",
            ));
        }
        Ok(Self {
            id,
            collection_id,
            group_id,
            permissions: normalized_permissions(permissions),
            created_at,
            updated_at,
        })
    }

    #[must_use]
    pub const fn id(&self) -> AuthorizationGrantId {
        self.id
    }
    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }
    #[must_use]
    pub fn permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.permissions
    }
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationGroupGrant {
    group: StorageAuthorizationGroup,
    grant: StorageAuthorizationGrant,
}

impl StorageAuthorizationGroupGrant {
    pub fn try_new(
        group: StorageAuthorizationGroup,
        grant: StorageAuthorizationGrant,
    ) -> Result<Self, StorageValidationError> {
        if group.id() != grant.group_id() {
            return Err(StorageValidationError::invalid(
                "authorization group grant ids must match",
            ));
        }
        Ok(Self { group, grant })
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageAuthorizationGroup, StorageAuthorizationGrant) {
        (self.group, self.grant)
    }
}

/// One complete local-policy row for backend-neutral policy export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAuthorizationPolicySnapshotRow {
    grant: StorageAuthorizationGrant,
    group: StorageAuthorizationGroup,
    collection: StorageAuthorizationCollection,
}

impl StorageAuthorizationPolicySnapshotRow {
    pub fn try_new(
        grant: StorageAuthorizationGrant,
        group: StorageAuthorizationGroup,
        collection: StorageAuthorizationCollection,
    ) -> Result<Self, StorageValidationError> {
        if grant.group_id() != group.id() || grant.collection_id() != collection.id() {
            return Err(StorageValidationError::invalid(
                "authorization policy grant, group, and collection ids must match",
            ));
        }
        Ok(Self {
            grant,
            group,
            collection,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageAuthorizationGrant,
        StorageAuthorizationGroup,
        StorageAuthorizationCollection,
    ) {
        (self.grant, self.group, self.collection)
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageAuthorizationCollectionGrantListQuery {
    collection_id: CollectionId,
    required_permissions: Vec<StorageAuthorizationPermission>,
    query_options: QueryOptions,
}

impl fmt::Debug for StorageAuthorizationCollectionGrantListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationCollectionGrantListQuery")
            .field("collection_id", &"[redacted]")
            .field(
                "required_permission_count",
                &self.required_permissions.len(),
            )
            .field("filter_count", &self.query_options.filters().len())
            .field("sort_count", &self.query_options.sort().len())
            .field("limit", &self.query_options.limit())
            .field("has_cursor", &self.query_options.cursor().is_some())
            .field("include_total", &self.query_options.include_total())
            .finish()
    }
}

impl StorageAuthorizationCollectionGrantListQuery {
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        required_permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
        query_options: QueryOptions,
    ) -> Self {
        Self {
            collection_id,
            required_permissions: normalized_permissions(required_permissions),
            query_options,
        }
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    #[must_use]
    pub fn required_permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.required_permissions
    }
    #[must_use]
    pub fn query_options(&self) -> &QueryOptions {
        &self.query_options
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageAuthorizationGrantKey {
    collection_id: CollectionId,
    group_id: GroupId,
}

impl fmt::Debug for StorageAuthorizationGrantKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGrantKey")
            .field("collection_id", &"[redacted]")
            .field("group_id", &"[redacted]")
            .finish()
    }
}

impl StorageAuthorizationGrantKey {
    #[must_use]
    pub const fn new(collection_id: CollectionId, group_id: GroupId) -> Self {
        Self {
            collection_id,
            group_id,
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
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGrantMutation {
    key: StorageAuthorizationGrantKey,
    permissions: Vec<StorageAuthorizationPermission>,
    replace_existing: bool,
    event_context: EventContext,
}

impl fmt::Debug for StorageAuthorizationGrantMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGrantMutation")
            .field("key", &self.key)
            .field("permission_count", &self.permissions.len())
            .field("replace_existing", &self.replace_existing)
            .field("event_context", &"[redacted]")
            .finish()
    }
}

impl StorageAuthorizationGrantMutation {
    #[must_use]
    pub fn new(
        key: StorageAuthorizationGrantKey,
        permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
        replace_existing: bool,
        event_context: EventContext,
    ) -> Self {
        Self {
            key,
            permissions: normalized_permissions(permissions),
            replace_existing,
            event_context,
        }
    }
    #[must_use]
    pub const fn key(&self) -> StorageAuthorizationGrantKey {
        self.key
    }
    #[must_use]
    pub fn permissions(&self) -> &[StorageAuthorizationPermission] {
        &self.permissions
    }
    #[must_use]
    pub const fn replace_existing(&self) -> bool {
        self.replace_existing
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

/// One collection permission-set snapshot query.
///
/// A group filter narrows the returned grants without changing the owner
/// revision, so conditional requests still describe the complete permission
/// set for the collection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageAuthorizationPermissionSetQuery {
    collection_id: CollectionId,
    group_id: Option<GroupId>,
}

impl StorageAuthorizationPermissionSetQuery {
    #[must_use]
    pub const fn new(collection_id: CollectionId, group_id: Option<GroupId>) -> Self {
        Self {
            collection_id,
            group_id,
        }
    }

    #[must_use]
    pub const fn collection_id(self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn group_id(self) -> Option<GroupId> {
        self.group_id
    }
}

impl fmt::Debug for StorageAuthorizationPermissionSetQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationPermissionSetQuery")
            .field("collection_id", &"[redacted]")
            .field("has_group_filter", &self.group_id.is_some())
            .finish()
    }
}

/// Revisioned local permission set returned without database row types.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationPermissionSet {
    collection_id: CollectionId,
    revision: ResourceRevision,
    grants: Vec<StorageAuthorizationGrant>,
}

impl StorageAuthorizationPermissionSet {
    pub fn try_new(
        collection_id: CollectionId,
        revision: ResourceRevision,
        grants: Vec<StorageAuthorizationGrant>,
    ) -> Result<Self, StorageValidationError> {
        let mut group_ids = std::collections::HashSet::with_capacity(grants.len());
        if grants.iter().any(|grant| {
            grant.collection_id() != collection_id || !group_ids.insert(grant.group_id())
        }) {
            return Err(StorageValidationError::invalid(
                "authorization permission-set grants must have the owning collection and unique groups",
            ));
        }
        Ok(Self {
            collection_id,
            revision,
            grants,
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CollectionId,
        ResourceRevision,
        Vec<StorageAuthorizationGrant>,
    ) {
        (self.collection_id, self.revision, self.grants)
    }
}

impl fmt::Debug for StorageAuthorizationPermissionSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationPermissionSet")
            .field("collection_id", &"[redacted]")
            .field("revision", &self.revision)
            .field("grant_count", &self.grants.len())
            .finish()
    }
}

/// Delete-all request with mandatory atomic event provenance.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationGrantDelete {
    key: StorageAuthorizationGrantKey,
    event_context: EventContext,
}

impl StorageAuthorizationGrantDelete {
    #[must_use]
    pub const fn new(key: StorageAuthorizationGrantKey, event_context: EventContext) -> Self {
        Self { key, event_context }
    }

    #[must_use]
    pub const fn key(&self) -> StorageAuthorizationGrantKey {
        self.key
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }
}

impl fmt::Debug for StorageAuthorizationGrantDelete {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationGrantDelete")
            .field("key", &self.key)
            .field("event_context", &"[redacted]")
            .finish()
    }
}

fn normalized_permissions(
    permissions: impl IntoIterator<Item = StorageAuthorizationPermission>,
) -> Vec<StorageAuthorizationPermission> {
    let mut permissions = permissions.into_iter().collect::<Vec<_>>();
    permissions.sort_unstable();
    permissions.dedup();
    permissions
}

/// Deduplicated resource identifiers requested for authorization enrichment.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationResourceIds {
    ids: Vec<ResourceId>,
}

impl StorageAuthorizationResourceIds {
    #[must_use]
    pub fn new(ids: impl IntoIterator<Item = ResourceId>) -> Self {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
    }

    #[must_use]
    pub fn ids(&self) -> &[ResourceId] {
        &self.ids
    }
}

impl fmt::Debug for StorageAuthorizationResourceIds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationResourceIds")
            .field("resource_count", &self.ids.len())
            .finish()
    }
}

/// Class facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationClassResource {
    id: ClassId,
    collection_id: CollectionId,
}

impl StorageAuthorizationClassResource {
    #[must_use]
    pub const fn new(id: ClassId, collection_id: CollectionId) -> Self {
        Self { id, collection_id }
    }

    #[must_use]
    pub const fn id(&self) -> ClassId {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
}

impl fmt::Debug for StorageAuthorizationClassResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationClassResource")
            .field("id", &"[redacted]")
            .field("collection_id", &"[redacted]")
            .finish()
    }
}

/// Object facts needed to construct an authorization resource.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageAuthorizationObjectResource {
    id: ObjectId,
    collection_id: CollectionId,
    class_id: ClassId,
    name: String,
}

impl StorageAuthorizationObjectResource {
    #[must_use]
    pub fn new(
        id: ObjectId,
        collection_id: CollectionId,
        class_id: ClassId,
        name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            collection_id,
            class_id,
            name: name.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ObjectId {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for StorageAuthorizationObjectResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageAuthorizationObjectResource")
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
pub trait AuthorizationDataStorage: Send + Sync {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthorizationPrincipal, StorageError>;

    async fn is_authorization_principal_group_member(
        &self,
        query: StorageAuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError>;

    async fn list_authorization_classes(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationClassResource>, StorageError>;

    async fn list_authorization_objects(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationObjectResource>, StorageError>;

    async fn authorize_local_collection(
        &self,
        query: StorageAuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError>;

    async fn authorize_local_collections(
        &self,
        query: StorageAuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError>;

    async fn list_local_authorized_collections(
        &self,
        query: StorageAuthorizationCollectionsQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError>;

    async fn load_authorization_collection_candidates(
        &self,
        query: StorageAuthorizationCollectionCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationCollection>, StorageError>;

    async fn load_authorization_group_candidates(
        &self,
        query: StorageAuthorizationGroupCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationGroup>, StorageError>;

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError>;

    async fn list_local_collection_grants(
        &self,
        query: StorageAuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError>;

    async fn get_local_collection_grant(
        &self,
        key: StorageAuthorizationGrantKey,
    ) -> Result<Option<StorageAuthorizationGrant>, StorageError>;

    async fn get_local_collection_permission_set(
        &self,
        query: StorageAuthorizationPermissionSetQuery,
    ) -> Result<StorageAuthorizationPermissionSet, StorageError>;

    async fn apply_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError>;

    async fn revoke_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError>;

    async fn revoke_all_local_collection_grants(
        &self,
        request: StorageAuthorizationGrantDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(id: i32) -> PrincipalId {
        PrincipalId::new(id).unwrap()
    }

    fn group(id: i32) -> GroupId {
        GroupId::new(id).unwrap()
    }

    fn collection(id: i32) -> CollectionId {
        CollectionId::new(id).unwrap()
    }

    #[test]
    fn principal_groups_are_normalized() {
        let principal = StorageAuthorizationPrincipal::new(principal(7), [3, 1, 3, 2].map(group));
        assert_eq!(principal.group_ids(), &[group(1), group(2), group(3)]);
    }

    #[test]
    fn permission_names_round_trip_for_the_complete_contract_vocabulary() {
        for permission in StorageAuthorizationPermission::ALL {
            assert_eq!(
                StorageAuthorizationPermission::from_name(permission.as_str()),
                Ok(permission)
            );
        }
    }

    #[test]
    fn unknown_permission_names_are_rejected_at_the_contract_boundary() {
        let error = StorageAuthorizationPermission::from_name("read_collection")
            .expect_err("permission names are case-sensitive persisted vocabulary");
        assert_eq!(error.kind(), crate::StorageErrorKind::InvalidInput);
    }

    #[test]
    fn principal_debug_redacts_identity() {
        let debug = format!(
            "{:?}",
            StorageAuthorizationPrincipal::new(principal(987_654), [group(123), group(456)])
        );
        assert!(!debug.contains("987654"));
        assert!(!debug.contains("123"));
        assert!(debug.contains("group_count"));
    }

    #[test]
    fn membership_debug_redacts_lookup_values() {
        let debug = format!(
            "{:?}",
            StorageAuthorizationGroupMembershipQuery::new(
                principal(987_654),
                "secret-admins",
                "private-scope",
            )
        );
        assert!(!debug.contains("987654"));
        assert!(!debug.contains("secret-admins"));
        assert!(!debug.contains("private-scope"));
    }

    #[test]
    fn authorization_dto_debug_redacts_resource_identity() {
        let query = StorageAuthorizationCollectionAccessQuery::new(
            principal(987_654),
            collection(876_543),
            [StorageAuthorizationPermission::ReadCollection],
        );
        let grant = StorageAuthorizationGrant::try_new(
            AuthorizationGrantId::new(765_432).unwrap(),
            collection(876_543),
            group(654_321),
            [StorageAuthorizationPermission::ReadCollection],
            DateTime::<Utc>::default(),
            DateTime::<Utc>::default(),
        )
        .unwrap();
        let debug = format!("{query:?} {grant:?}");

        for sensitive in ["987654", "876543", "765432", "654321"] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("permission_count"));
    }

    #[test]
    fn permission_sets_are_normalized() {
        let query = StorageAuthorizationCollectionAccessQuery::new(
            principal(1),
            collection(2),
            [
                StorageAuthorizationPermission::UpdateCollection,
                StorageAuthorizationPermission::ReadCollection,
                StorageAuthorizationPermission::UpdateCollection,
            ],
        );
        assert_eq!(
            query.permissions(),
            &[
                StorageAuthorizationPermission::ReadCollection,
                StorageAuthorizationPermission::UpdateCollection,
            ]
        );
    }

    #[test]
    fn batch_access_query_normalizes_collection_and_permission_sets() {
        let query = StorageAuthorizationCollectionsAccessQuery::new(
            principal(1),
            [9, 3, 9, 5].map(collection),
            [
                StorageAuthorizationPermission::UpdateCollection,
                StorageAuthorizationPermission::ReadCollection,
                StorageAuthorizationPermission::UpdateCollection,
            ],
        );

        assert_eq!(
            query.collection_ids(),
            &[collection(3), collection(5), collection(9)]
        );
        assert_eq!(
            query.permissions(),
            &[
                StorageAuthorizationPermission::ReadCollection,
                StorageAuthorizationPermission::UpdateCollection,
            ]
        );
    }

    #[test]
    fn batch_access_query_debug_redacts_identity() {
        let debug = format!(
            "{:?}",
            StorageAuthorizationCollectionsAccessQuery::new(
                principal(987_654),
                [collection(876_543), collection(765_432)],
                [StorageAuthorizationPermission::ReadCollection],
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
        let class = StorageAuthorizationClassResource::new(
            ClassId::new(987_654).unwrap(),
            collection(876_543),
        );
        let object = StorageAuthorizationObjectResource::new(
            ObjectId::new(765_432).unwrap(),
            collection(654_321),
            ClassId::new(543_210).unwrap(),
            "sensitive-object-name",
        );
        let ids = StorageAuthorizationResourceIds::new([
            ResourceId::new(432_109).unwrap(),
            ResourceId::new(321_098).unwrap(),
        ]);
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
        let key = StorageAuthorizationGrantKey::new(collection(987_654), group(876_543));
        let query =
            StorageAuthorizationPermissionSetQuery::new(collection(987_654), Some(group(876_543)));
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            [StorageAuthorizationPermission::ReadCollection],
            false,
            EventContext::system(),
        );
        let delete = StorageAuthorizationGrantDelete::new(key, EventContext::system());
        let debug = format!("{query:?} {mutation:?} {delete:?}");

        for sensitive in ["987654", "876543"] {
            assert!(!debug.contains(sensitive));
        }
        assert!(debug.contains("event_context"));
        assert!(debug.contains("has_group_filter"));
    }
}
