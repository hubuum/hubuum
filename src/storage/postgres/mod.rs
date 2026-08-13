mod backup_snapshot;
mod capabilities;
mod computed_fields;
mod error;
mod export_templates;
mod failpoints;
mod imports;
#[cfg(feature = "embedded-migrations")]
mod migrations;
mod notifications;
#[cfg(test)]
pub(crate) use imports::{RuntimeState, execute_planned_item, resolve_object_runtime};
#[doc(hidden)]
pub mod operations;
mod remote_targets;
mod restores;
mod revision;
mod runtime;
mod task_execution;
mod task_queue;

#[cfg(test)]
pub(crate) use failpoints::{PostgresFailpoint, with_failpoint};
pub use runtime::*;

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::events::{
    EventContext, EventFanoutSettings, EventRetentionSettings, MutationProvenance,
};
use crate::models::output::{EffectiveGroupPermission, GroupPermission};
use crate::models::search::QueryOptions;
use crate::models::{
    ClassIdSet, ClassSelector, Collection, CollectionID, Group, GroupID, HubuumClass,
    HubuumClassID, HubuumClassRelation, HubuumClassRelationID, HubuumObject, HubuumObjectID,
    HubuumObjectRelation, HubuumObjectRelationID, MaintenanceState, NewCollectionWithAssignee,
    NewGroup, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    ObjectDataPatchDocument, ObjectRelationCreateSelector, ObjectRelationSelector, ObjectSelector,
    Permission, PreparedClassRelation, PreparedObjectRelation, Principal, PrincipalGroup,
    PrincipalID, PrincipalSettings, PrincipalSettingsPatch, PrincipalSettingsResponse,
    ResolvedClassRelationTarget, ResolvedClassTarget, ResolvedObjectRelationTarget,
    ResolvedObjectTarget, TokenRetentionSettings, UpdateCollection, UpdateGroup, UpdateHubuumClass,
    UpdateHubuumObject,
};
use crate::storage::postgres::operations::GetCollection;
use crate::storage::postgres::operations::class::{
    ClassCollectionLookup, CreateClassRecord, DeleteClassRecord, DeleteResolvedClassRecord,
    LoadClassRecord, ResolveClassSelectorRecord, UpdateClassRecord, UpdateResolvedClassRecord,
    load_class_names,
};
use crate::storage::postgres::operations::collection::{
    DeleteCollectionRecord, SaveCollectionWithAssigneeRecord, UpdateCollectionRecord,
    collection_ancestors_from_backend, collection_children_from_backend,
    effective_group_on_from_backend, effective_principal_on_from_backend,
    group_can_on_from_backend, group_on_from_backend, groups_can_on_from_backend,
    groups_can_on_paginated_with_total_count_from_backend, groups_on_from_backend,
    groups_on_paginated_with_total_count_from_backend, move_collection_record_from_backend,
    principal_all_permissions_from_backend, principal_on_from_backend,
    principal_on_paginated_with_total_count_from_backend, user_can_on_any_from_backend,
};
use crate::storage::postgres::operations::group::{
    DeleteGroupRecord, GroupMembersBackend, LoadGroupRecord, SaveGroupRecord, UpdateGroupRecord,
};
use crate::storage::postgres::operations::object::{
    CreateObjectInResolvedClassRecord, DeleteObjectRecord, DeleteResolvedObjectRecord,
    LoadObjectRecord, ObjectClassLookup, ObjectCollectionLookup, PatchObjectDataRecord,
    ResolveObjectSelectorRecord, SaveObjectRecord, UpdateObjectRecord, UpdateResolvedObjectRecord,
    ValidateObjectRecord,
};
use crate::storage::postgres::operations::relations::{
    CreatePreparedClassRelationRecord, CreatePreparedObjectRelationRecord,
    DeleteClassRelationRecord, DeleteObjectRelationRecord, DeleteResolvedClassRelationRecord,
    DeleteResolvedObjectRelationRecord, PrepareClassRelationRecord, PrepareObjectRelationRecord,
    ResolveClassRelationTargetRecord, ResolveObjectRelationTargetRecord, SaveClassRelationRecord,
    SaveObjectRelationRecord,
};

use super::{
    AuditEventStorage, AuthenticatedToken, AuthenticationCredential, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupGrantPage, AuthorizationGroupMembershipQuery, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationResourceIds, AuthorizationStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage, CatalogStorage,
    ClassRecordStorage, ClassRelationStore, ClassStore, CollectionGrantListQuery,
    CollectionGroupPermissionQuery, CollectionGroupsPageQuery, CollectionGroupsQuery,
    CollectionPermissionStorage, CollectionPrincipalPageQuery, CollectionPrincipalQuery,
    CollectionRecordStorage, CollectionStore, CollectionVisibilityQuery,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary,
    EventSubscriptionStorage, ExportQueryStorage, ExportTemplateHistoryRecord, GroupStorage,
    HistoryAsOfQuery, HistoryCollectionScope, HistoryListQuery, HistoryPage, HistoryPrincipalName,
    HistoryStorage, IdentityStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRecordStorage,
    ObjectRelationStore, ObjectRelationsTouchingIdsQuery, ObjectStore,
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth, OperationalStateStorage,
    OperationalStorageSnapshot, OperationalTaskQueueSnapshot, PrincipalStorage, ReadinessSnapshot,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    StorageAuditEvent, StorageAuditEventListQuery, StorageCallSite, StorageClass,
    StorageClassGraphRow, StorageClassRelation, StorageCollection, StorageComputedObject,
    StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecution,
    StorageExternalPrincipalState, StorageExternalUserSync, StorageGroupListQuery, StorageIdentity,
    StorageIdentityGroup, StorageIdentityPage, StorageIdentityScope, StorageIdentityScopeEnsure,
    StorageInventoryCounts, StorageLocalPasswordReset, StorageObject, StorageObjectAggregatePage,
    StorageObjectGraphRow, StorageObjectRelation, StoragePoolAcquisitionState, StoragePoolCapacity,
    StoragePoolConnectionState, StoragePoolState, StoragePrincipalGroup,
    StoragePrincipalGroupListQuery, StorageQueryBudget, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageRevisionPrecondition, StorageServiceAccount,
    StorageServiceAccountCreate, StorageServiceAccountListItem, StorageServiceAccountListQuery,
    StorageServiceAccountMutation, StorageServiceAccountPoint, StorageServiceAccountUpdate,
    StorageSyncedHuman, StorageTokenCreate, StorageTokenHashRevoke, StorageTokenListQuery,
    StorageTokenMetadata, StorageTokenRenew, StorageTokenRevoke, StorageUser, StorageUserCreate,
    StorageUserDelete, StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate,
    StorageUserPoint, StorageUserUpdate, TaskGaugeSnapshot, TokenRetentionStorage, TokenStorage,
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery,
    UnifiedSearchStorage, UserStorage,
};
use super::{ClassHistoryRecord, CollectionHistoryRecord};
use error::map_postgres_error;

/// Canonical production storage adapter.
#[derive(Clone)]
pub(crate) struct PostgresStorage {
    pool: PostgresPool,
    notification_pool_settings: Option<Arc<PostgresPoolSettings>>,
}

#[cfg(feature = "embedded-migrations")]
pub(in crate::storage) use migrations::run_embedded_migrations;

impl PostgresStorage {
    pub(crate) fn new(pool: PostgresPool) -> Self {
        Self {
            pool,
            notification_pool_settings: None,
        }
    }

    pub(crate) fn with_notification_pool_settings(
        pool: PostgresPool,
        notification_pool_settings: PostgresPoolSettings,
    ) -> Self {
        let mut backend = Self::new(pool);
        backend.notification_pool_settings = Some(Arc::new(notification_pool_settings));
        backend
    }

    pub(crate) fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    fn notification_listener_pool(&self) -> PostgresPool {
        self.notification_pool_settings
            .as_deref()
            .map(runtime::init_postgres_pool_with_settings)
            .unwrap_or_else(|| self.pool.clone())
    }
}
